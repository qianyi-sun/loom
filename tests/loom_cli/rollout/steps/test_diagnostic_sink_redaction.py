"""Sentinel coverage for rollout diagnostic persistence boundaries."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from loom_cli.rollout.base_context_fixture import make_ctx
from loom_cli.rollout.evidence import EvidenceDirectory, StepDir
from loom_cli.rollout.operator.redaction import rollout_redaction_scope
from loom_cli.rollout.steps.base import RunResult
from loom_cli.rollout.steps.s01_worktree import WorktreeStep
from loom_cli.rollout.steps.s02_build_images import BuildImagesStep
from loom_cli.rollout.steps.s07_render import RenderStep, rendered_yaml_path
from loom_cli.rollout.steps.s09_migrate import MigrateStep
from loom_cli.rollout.steps.s12_release_gate import ReleaseGateStep
from loom_cli.rollout.steps.subprocess_util import SubprocessResult

_SECRET = "opaque-diagnostic-sink-secret"


def _evidence(tmp_path: Path) -> EvidenceDirectory:
    evidence = EvidenceDirectory(tmp_path, "rid")
    evidence.ensure()
    return evidence


def _prepare_candidate(evidence: EvidenceDirectory) -> None:
    package = evidence.step_dir(1, "worktree").path / "src" / "src" / "loom_cli"
    package.mkdir(parents=True)
    (package / "__main__.py").write_text("raise SystemExit(0)\n", encoding="utf-8")


def _result(
    argv: list[str],
    *,
    returncode: int = 0,
    stdout: str = "",
    stderr: str = "",
) -> SubprocessResult:
    return SubprocessResult(
        argv=argv,
        returncode=returncode,
        stdout=stdout,
        stderr=stderr,
    )


def _assert_diagnostic_safe(path: Path) -> None:
    if path.exists():
        assert _SECRET not in path.read_text(encoding="utf-8")


def test_worktree_logs_are_redacted_before_abrupt_exit(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    evidence = _evidence(tmp_path)
    step_dir = evidence.step_dir(1, "worktree")
    ctx = make_ctx(tmp_path)
    monkeypatch.setattr(
        "loom_cli.rollout.steps.s01_worktree.run_captured",
        lambda argv, **_kwargs: _result(
            list(argv),
            stdout=f"stdout {_SECRET}\n",
            stderr=f"stderr {_SECRET}\n",
        ),
    )

    def abrupt_result(**_kwargs: object) -> None:
        raise KeyboardInterrupt("simulated termination after diagnostic write")

    monkeypatch.setattr("loom_cli.rollout.steps.s01_worktree.RunResult", abrupt_result)

    with rollout_redaction_scope((_SECRET,)), pytest.raises(KeyboardInterrupt):
        WorktreeStep().run(ctx, step_dir)

    _assert_diagnostic_safe(step_dir.stdout_path())
    _assert_diagnostic_safe(step_dir.stderr_path())


def test_build_failure_logs_redact_without_driver_cleanup(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    evidence = _evidence(tmp_path)
    worktree = evidence.step_dir(1, "worktree").path / "src"
    worktree.mkdir(parents=True)
    step_dir = evidence.step_dir(2, "build-images")
    ctx = make_ctx(tmp_path)
    calls = 0

    def fake_run(argv: list[str], **_kwargs: object) -> SubprocessResult:
        nonlocal calls
        calls += 1
        if calls == 1:
            return _result(list(argv), returncode=1)
        return _result(
            list(argv),
            returncode=1,
            stdout=f"build stdout {_SECRET}\n",
            stderr=f"build stderr {_SECRET}\n",
        )

    monkeypatch.setattr(
        "loom_cli.rollout.steps.s02_build_images.ROLLOUT_IMAGES",
        (("loom-service", "deploy/Dockerfile.service"),),
    )
    monkeypatch.setattr("loom_cli.rollout.steps.s02_build_images.run_captured", fake_run)

    with rollout_redaction_scope((_SECRET,)):
        result = BuildImagesStep().run(ctx, step_dir)

    assert result.exit_code == 1
    _assert_diagnostic_safe(step_dir.stdout_path())
    _assert_diagnostic_safe(step_dir.stderr_path())


def test_failed_render_stdout_redacts_without_corrupting_success_manifest(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    evidence = _evidence(tmp_path)
    _prepare_candidate(evidence)
    ctx = make_ctx(tmp_path)
    failed_dir = evidence.step_dir(7, "failed-render")
    monkeypatch.setattr(
        "loom_cli.rollout.steps.s07_render.run_captured",
        lambda argv, **_kwargs: _result(
            list(argv),
            returncode=1,
            stdout=f"partial yaml {_SECRET}\n",
            stderr=f"render failed {_SECRET}\n",
        ),
    )

    with rollout_redaction_scope((_SECRET,)):
        failed = RenderStep().run(ctx, failed_dir)

    assert failed.exit_code == 1
    _assert_diagnostic_safe(failed_dir.stdout_path())

    success_dir = evidence.step_dir(7, "render")
    functional_yaml = (
        "apiVersion: v1\nkind: ConfigMap\ndata:\n  endpoint: http://loom-control-plane:8080/api\n"
    )
    monkeypatch.setattr(
        "loom_cli.rollout.steps.s07_render.run_captured",
        lambda argv, **_kwargs: _result(list(argv), stdout=functional_yaml),
    )

    assert RenderStep().run(ctx, success_dir).exit_code == 0
    assert rendered_yaml_path(success_dir).read_text(encoding="utf-8") == functional_yaml


def _stateful_manifest() -> str:
    return """
apiVersion: apps/v1
kind: StatefulSet
metadata: {name: loom-postgres}
---
apiVersion: v1
kind: Service
metadata: {name: loom-postgres}
---
apiVersion: apps/v1
kind: StatefulSet
metadata: {name: loom-minio}
---
apiVersion: v1
kind: Service
metadata: {name: loom-minio}
"""


def _prepare_migrate(evidence: EvidenceDirectory) -> StepDir:
    _prepare_candidate(evidence)
    evidence.step_dir(7, "render").artifact_path("rendered.yaml").write_text(
        _stateful_manifest(),
        encoding="utf-8",
    )
    return evidence.step_dir(9, "migrate")


@pytest.mark.parametrize("failure_call", range(6))
def test_migrate_failure_diagnostics_redact_at_every_subprocess_sink(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    failure_call: int,
) -> None:
    evidence = _evidence(tmp_path)
    step_dir = _prepare_migrate(evidence)
    ctx = make_ctx(tmp_path)
    call_index = 0
    functional_manifest = f"apiVersion: batch/v1\nkind: Job\nmetadata: {{name: {_SECRET}}}\n"

    def fake_run(argv: list[str], **_kwargs: object) -> SubprocessResult:
        nonlocal call_index
        current = call_index
        call_index += 1
        if current == failure_call:
            return _result(
                list(argv),
                returncode=1,
                stdout=f"failed stdout {_SECRET}\n",
                stderr=f"failed stderr {_SECRET}\n",
            )
        return _result(
            list(argv),
            stdout=(functional_manifest if current == 0 else f"ok {_SECRET}\n"),
        )

    monkeypatch.setattr("loom_cli.rollout.steps.s09_migrate.run_captured", fake_run)

    with rollout_redaction_scope((_SECRET,)):
        result = MigrateStep().run(ctx, step_dir)

    assert result.exit_code == 1
    _assert_diagnostic_safe(step_dir.stdout_path())
    _assert_diagnostic_safe(step_dir.stderr_path())
    if failure_call > 0:
        assert step_dir.artifact_path("migration.yaml").read_text() == functional_manifest


def test_migrate_success_redacts_diagnostic_aggregate_but_keeps_manifest_raw(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    evidence = _evidence(tmp_path)
    step_dir = _prepare_migrate(evidence)
    ctx = make_ctx(tmp_path)
    call_index = 0
    manifest_prefix = "apiVersion: batch/v1\nkind: Job\n# "
    functional_manifest = manifest_prefix + ("x" * (1995 - len(manifest_prefix))) + _SECRET + "\n"

    def fake_run(argv: list[str], **_kwargs: object) -> SubprocessResult:
        nonlocal call_index
        current = call_index
        call_index += 1
        return _result(
            list(argv),
            stdout=(functional_manifest if current == 0 else f"ok {_SECRET}\n"),
        )

    monkeypatch.setattr("loom_cli.rollout.steps.s09_migrate.run_captured", fake_run)

    with rollout_redaction_scope((_SECRET,)):
        result = MigrateStep().run(ctx, step_dir)

    assert result.exit_code == 0
    _assert_diagnostic_safe(step_dir.stdout_path())
    assert _SECRET[:5] not in step_dir.stdout_path().read_text(encoding="utf-8")
    assert step_dir.artifact_path("migration.yaml").read_text() == functional_manifest


def test_migrate_error_excerpt_redacts_before_truncation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    evidence = _evidence(tmp_path)
    step_dir = _prepare_migrate(evidence)
    ctx = make_ctx(tmp_path)
    stderr = ("x" * 195) + _SECRET
    monkeypatch.setattr(
        "loom_cli.rollout.steps.s09_migrate.run_captured",
        lambda argv, **_kwargs: _result(list(argv), returncode=1, stderr=stderr),
    )

    with rollout_redaction_scope((_SECRET,)):
        result = MigrateStep().run(ctx, step_dir)

    assert result.exit_code == 1
    assert result.error is not None
    assert _SECRET[:5] not in result.error


def test_release_gate_nested_retry_artifact_redacts_raw_stderr(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    evidence = _evidence(tmp_path)
    step_dir = evidence.step_dir(14, "release-gate")
    ctx = make_ctx(tmp_path)
    step = ReleaseGateStep()
    calls = 0

    monkeypatch.setattr(ReleaseGateStep, "cwd", lambda *_args: tmp_path)
    monkeypatch.setattr(ReleaseGateStep, "env", lambda *_args: {})
    monkeypatch.setattr(
        ReleaseGateStep,
        "_write_expected_image_identities",
        lambda _self, _ctx, sd: sd.artifact_path("image-identities.json"),
    )
    monkeypatch.setattr(
        ReleaseGateStep,
        "_run_candidate_bound_hf_canary",
        lambda _self, _ctx, _step_dir: RunResult(
            exit_code=0,
            artifacts={"batch_id": "00000000-0000-4000-8000-000000000001"},
        ),
    )
    monkeypatch.setattr(ReleaseGateStep, "_gb10_desired_state_count", lambda *_args: 1)
    monkeypatch.setattr(
        "loom_cli.rollout.steps.s12_release_gate.environment_state_check_argv",
        lambda *_args: None,
    )
    monkeypatch.setattr(
        "loom_cli.rollout.steps.s12_release_gate._GB10_STATUS_MAX_ATTEMPTS",
        1,
    )
    monkeypatch.setattr(
        "loom_cli.rollout.steps.s12_release_gate._GB10_STATUS_RETRY_DELAY_SEC",
        0.0,
    )

    def fake_run(argv: list[str], **_kwargs: Any) -> SubprocessResult:
        nonlocal calls
        calls += 1
        if calls < 3:
            return _result(list(argv), stdout="ok\n")
        return _result(
            list(argv),
            returncode=1,
            stderr=(
                f"could not reach CP: {_SECRET}\nredis://:nested-retry-password@cache.example/0\n"
            ),
        )

    monkeypatch.setattr("loom_cli.rollout.steps.s12_release_gate.run_captured", fake_run)

    with rollout_redaction_scope((_SECRET,)):
        result = step.run(ctx, step_dir)

    assert result.exit_code == 1
    retry_log = step_dir.artifact_path("gb10-workers-status.retries.log")
    rendered = retry_log.read_text(encoding="utf-8")
    assert _SECRET not in rendered
    assert "nested-retry-password" not in rendered
