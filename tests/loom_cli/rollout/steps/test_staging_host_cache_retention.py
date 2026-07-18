"""Fail-closed staging host Docker-cache retention evidence."""

from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from loom_cli.rollout.base_context_fixture import make_ctx
from loom_cli.rollout.context import RolloutContext
from loom_cli.rollout.evidence import EvidenceDirectory, StepDir
from loom_cli.rollout.steps.s12_release_gate import ReleaseGateStep
from loom_cli.rollout.steps.subprocess_util import (
    SubprocessExecutionError,
    SubprocessResult,
)


def _step(tmp_path: Path) -> tuple[ReleaseGateStep, RolloutContext, StepDir]:
    ctx = make_ctx(tmp_path, rollout_root=Path("/data/loom-staging"))
    evidence = EvidenceDirectory(tmp_path, "test-rid")
    evidence.ensure()
    step_dir = evidence.step_dir(14, "release-gate")
    identity = step_dir.artifact_path("image-identities-staging-abc123.json")
    identity.write_text('{"loom-service":"sha256:exact"}\n', encoding="utf-8")
    return ReleaseGateStep(), ctx, step_dir


def test_staging_host_cache_retention_is_bounded_and_audited(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    step, ctx, step_dir = _step(tmp_path)
    calls: list[tuple[list[str], float | None]] = []
    free = iter((100, 250))

    monkeypatch.setattr(
        "loom_cli.rollout.steps.s12_release_gate.shutil.disk_usage",
        lambda _path: SimpleNamespace(free=next(free)),
    )
    monkeypatch.setattr(
        step,
        "_write_expected_image_identities",
        lambda _ctx, _step_dir: step.expected_image_identities_path(_ctx, _step_dir),
    )

    def fake_run(argv, **kwargs):
        calls.append((list(argv), kwargs.get("timeout_sec")))
        return SubprocessResult(list(argv), 0, "ok\n", "")

    monkeypatch.setattr(
        "loom_cli.rollout.steps.s12_release_gate.run_captured",
        fake_run,
    )

    result = step._reclaim_staging_host_cache(ctx, step_dir)

    assert result.exit_code == 0
    assert calls == [
        (
            [
                "docker",
                "image",
                "prune",
                "--all",
                "--force",
                "--filter",
                "until=24h",
            ],
            1800.0,
        ),
        (
            [
                "docker",
                "builder",
                "prune",
                "--all",
                "--force",
                "--filter",
                "until=24h",
            ],
            1800.0,
        ),
    ]
    record = json.loads(
        step_dir.artifact_path("staging-host-cache-retention.json").read_text(),
    )
    assert record["outcome"] == "passed"
    assert record["free_bytes_before"] == 100
    assert record["free_bytes_after"] == 250
    assert record["free_bytes_reclaimed"] == 150
    assert record["candidate_sha"] == ctx.resolved_sha
    assert record["candidate_image_tag"] == ctx.image_tag
    assert record["retention_hours"] == 24
    assert record["steps"] == [
        {"command": ["docker", "image", "prune"], "exit_code": 0},
        {"command": ["docker", "builder", "prune"], "exit_code": 0},
    ]


def test_staging_host_cache_retention_fails_closed_on_prune_error(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    step, ctx, step_dir = _step(tmp_path)
    calls: list[list[str]] = []
    monkeypatch.setattr(
        "loom_cli.rollout.steps.s12_release_gate.shutil.disk_usage",
        lambda _path: SimpleNamespace(free=100),
    )

    def fake_run(argv, **_kwargs):
        calls.append(list(argv))
        return SubprocessResult(list(argv), 7, "", "daemon unavailable")

    monkeypatch.setattr(
        "loom_cli.rollout.steps.s12_release_gate.run_captured",
        fake_run,
    )

    result = step._reclaim_staging_host_cache(ctx, step_dir)

    assert result.exit_code == 7
    assert len(calls) == 1
    record = json.loads(
        step_dir.artifact_path("staging-host-cache-retention.json").read_text(),
    )
    assert record["outcome"] == "failed"
    assert record["failed_command"] == ["docker", "image", "prune"]


def test_staging_host_cache_retention_rejects_candidate_identity_drift(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    step, ctx, step_dir = _step(tmp_path)
    free = iter((100, 200))
    monkeypatch.setattr(
        "loom_cli.rollout.steps.s12_release_gate.shutil.disk_usage",
        lambda _path: SimpleNamespace(free=next(free)),
    )
    monkeypatch.setattr(
        "loom_cli.rollout.steps.s12_release_gate.run_captured",
        lambda argv, **_kwargs: SubprocessResult(list(argv), 0, "", ""),
    )

    def drift(_ctx, _step_dir):
        path = step.expected_image_identities_path(_ctx, _step_dir)
        path.write_text('{"loom-service":"sha256:drift"}\n', encoding="utf-8")
        return path

    monkeypatch.setattr(step, "_write_expected_image_identities", drift)

    result = step._reclaim_staging_host_cache(ctx, step_dir)

    assert result.exit_code == 2
    assert result.error == "candidate image identities changed during retention"
    record = json.loads(
        step_dir.artifact_path("staging-host-cache-retention.json").read_text(),
    )
    assert record["outcome"] == "failed"


def test_staging_host_cache_retention_records_launch_or_timeout_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    step, ctx, step_dir = _step(tmp_path)
    monkeypatch.setattr(
        "loom_cli.rollout.steps.s12_release_gate.shutil.disk_usage",
        lambda _path: SimpleNamespace(free=100),
    )

    def fail_run(*_args, **_kwargs):
        raise SubprocessExecutionError("command timed out after 1800s: docker image prune")

    monkeypatch.setattr(
        "loom_cli.rollout.steps.s12_release_gate.run_captured",
        fail_run,
    )

    result = step._reclaim_staging_host_cache(ctx, step_dir)

    assert result.exit_code == 2
    assert result.error is not None and "timed out" in result.error
    record = json.loads(
        step_dir.artifact_path("staging-host-cache-retention.json").read_text(),
    )
    assert record["outcome"] == "failed"
    assert record["failed_command"] == ["docker", "image", "prune"]


def test_host_cache_retention_skips_outside_fixed_staging_root(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    step = ReleaseGateStep()
    ctx = make_ctx(tmp_path)
    evidence = EvidenceDirectory(tmp_path, "test-rid")
    evidence.ensure()
    step_dir = evidence.step_dir(14, "release-gate")
    monkeypatch.setattr(
        "loom_cli.rollout.steps.s12_release_gate.run_captured",
        lambda *_args, **_kwargs: pytest.fail("docker cleanup must not run"),
    )

    result = step._reclaim_staging_host_cache(ctx, step_dir)

    assert result.exit_code == 0
    record = json.loads(
        step_dir.artifact_path("staging-host-cache-retention.json").read_text(),
    )
    assert record["outcome"] == "skipped"
