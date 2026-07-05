"""Regression tests for candidate-source rollout command invocation (#441)."""

from __future__ import annotations

import json
import os
import sys
import tomllib
from pathlib import Path
from typing import Any

import pytest

from loom_cli.rollout.base_context_fixture import make_ctx
from loom_cli.rollout.evidence import EvidenceDirectory
from loom_cli.rollout.steps.s03_kind_load_images import KindLoadImagesStep
from loom_cli.rollout.steps.s05_backup import BackupStep
from loom_cli.rollout.steps.s06_audit import AuditStep
from loom_cli.rollout.steps.s07_render import RenderStep
from loom_cli.rollout.steps.s08_preflight import PreflightStep
from loom_cli.rollout.steps.s09_migrate import MigrateStep
from loom_cli.rollout.steps.s10_env_state import EnvStateStep
from loom_cli.rollout.steps.s11_cluster_up import ClusterUpStep
from loom_cli.rollout.steps.s12_release_gate import ReleaseGateStep
from loom_cli.rollout.steps.subprocess_util import SubprocessResult


def _prepare_candidate_worktree(ev: EvidenceDirectory) -> Path:
    worktree = ev.step_dir(1, "worktree").path / "src"
    package_dir = worktree / "src" / "loom_cli"
    package_dir.mkdir(parents=True)
    (package_dir / "__main__.py").write_text("raise SystemExit(0)\n")
    return worktree


def _assert_candidate_invocation(
    call: dict[str, Any],
    *,
    worktree: Path,
) -> None:
    assert call["argv"][:3] == [sys.executable, "-m", "loom_cli"]
    assert call["cwd"] == worktree
    pythonpath = call["env"].get("PYTHONPATH", "")
    assert pythonpath.split(os.pathsep)[0] == str(worktree / "src")


def _write_rendered_service(
    ev: EvidenceDirectory,
    *,
    image_tag: str = "public-beta-abc123",
) -> Path:
    rendered = ev.step_dir(7, "render").artifact_path("rendered.yaml")
    rendered.write_text(
        f"""
apiVersion: apps/v1
kind: Deployment
metadata:
  name: loom-service
spec:
  template:
    spec:
      containers:
        - name: loom-service
          image: loom-service:{image_tag}
""",
        encoding="utf-8",
    )
    return rendered


def _docker_inspect_success(argv: list[str]) -> SubprocessResult:
    docs = []
    for image in argv[3:]:
        docs.append(
            {
                "Id": "sha256:" + "1" * 64,
                "RepoTags": [image],
                "RepoDigests": [image.split(":", 1)[0] + "@sha256:" + "2" * 64],
            }
        )
    return SubprocessResult(
        argv=list(argv),
        returncode=0,
        stdout=json.dumps(docs),
        stderr="",
    )


def test_render_runs_loom_cli_from_candidate_worktree(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ctx = make_ctx(tmp_path)
    ev = EvidenceDirectory(tmp_path, "test-rid")
    ev.ensure()
    worktree = _prepare_candidate_worktree(ev)
    step_dir = ev.step_dir(7, "render")
    seen: dict[str, Any] = {}

    def fake_run(argv, **kwargs):
        seen.update(
            argv=list(argv),
            cwd=kwargs.get("cwd"),
            env=kwargs.get("env"),
        )
        return SubprocessResult(
            argv=list(argv),
            returncode=0,
            stdout="kind: List\nitems: []\n",
            stderr="",
        )

    monkeypatch.setattr("loom_cli.rollout.steps.s07_render.run_captured", fake_run)

    result = RenderStep().run(ctx, step_dir)

    assert result.exit_code == 0
    _assert_candidate_invocation(seen, worktree=worktree)


def test_kind_load_images_resolves_loom_cli_without_global_executable(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ctx = make_ctx(tmp_path)
    ev = EvidenceDirectory(tmp_path, "test-rid")
    ev.ensure()
    worktree = _prepare_candidate_worktree(ev)
    step_dir = ev.step_dir(3, "kind-load-images")
    calls: list[dict[str, Any]] = []

    def fake_run(argv, **kwargs):
        calls.append(
            {
                "argv": list(argv),
                "cwd": kwargs.get("cwd"),
                "env": kwargs.get("env"),
            }
        )
        return SubprocessResult(
            argv=list(argv),
            returncode=0,
            stdout="all images present\n",
            stderr="",
        )

    monkeypatch.setattr(
        "loom_cli.rollout.steps.s03_kind_load_images.run_captured",
        fake_run,
    )

    result = KindLoadImagesStep().run(ctx, step_dir)

    assert result.exit_code == 0
    assert calls, "expected step to invoke check-only subcommand"
    _assert_candidate_invocation(calls[0], worktree=worktree)
    assert calls[0]["argv"][3:6] == ["cluster", "load-images", "--cluster-name"]


def test_migrate_render_migration_resolves_loom_cli_without_global_executable(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ctx = make_ctx(tmp_path)
    ev = EvidenceDirectory(tmp_path, "test-rid")
    ev.ensure()
    worktree = _prepare_candidate_worktree(ev)
    step_dir = ev.step_dir(9, "migrate")
    calls: list[dict[str, Any]] = []

    def fake_run(argv, **kwargs):
        calls.append(
            {
                "argv": list(argv),
                "cwd": kwargs.get("cwd"),
                "env": kwargs.get("env"),
            }
        )
        if list(argv)[:3] == [sys.executable, "-m", "loom_cli"]:
            return SubprocessResult(
                argv=list(argv),
                returncode=0,
                stdout="apiVersion: batch/v1\nkind: Job\n",
                stderr="",
            )
        return SubprocessResult(
            argv=list(argv),
            returncode=0,
            stdout="ok\n",
            stderr="",
        )

    monkeypatch.setattr("loom_cli.rollout.steps.s09_migrate.run_captured", fake_run)

    result = MigrateStep().run(ctx, step_dir)

    assert result.exit_code == 0
    _assert_candidate_invocation(calls[0], worktree=worktree)
    assert calls[0]["argv"][3:5] == ["cluster", "render-migration"]


def test_env_state_resolves_loom_cli_without_global_executable(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ctx = make_ctx(tmp_path)
    ev = EvidenceDirectory(tmp_path, "test-rid")
    ev.ensure()
    worktree = _prepare_candidate_worktree(ev)
    step_dir = ev.step_dir(10, "env-state")
    profile = tmp_path / "staging.toml"
    profile.write_text("[worker_service]\n")
    calls: list[dict[str, Any]] = []

    def fake_run(argv, **kwargs):
        calls.append(
            {
                "argv": list(argv),
                "cwd": kwargs.get("cwd"),
                "env": kwargs.get("env"),
            }
        )
        return SubprocessResult(
            argv=list(argv),
            returncode=0,
            stdout="ok\n",
            stderr="",
        )

    monkeypatch.setattr(
        "loom_cli.rollout.steps.s10_env_state._profile_path_for",
        lambda ctx: str(profile),
    )
    monkeypatch.setattr("loom_cli.rollout.steps.s10_env_state.run_captured", fake_run)

    result = EnvStateStep().run(ctx, step_dir)

    assert result.exit_code == 0
    assert len(calls) == 2
    for call in calls:
        _assert_candidate_invocation(call, worktree=worktree)
    assert calls[0]["argv"][3:6] == ["admin", "environment-state", "apply"]
    assert calls[1]["argv"][3:6] == ["admin", "environment-state", "check"]
    for call in calls:
        assert "--var" in call["argv"]
        assert f"GIT_SHA={ctx.resolved_sha}" in call["argv"]


def test_subcommand_step_resolves_loom_cli_without_global_executable(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ctx = make_ctx(tmp_path)
    ev = EvidenceDirectory(tmp_path, "test-rid")
    ev.ensure()
    worktree = _prepare_candidate_worktree(ev)
    step_dir = ev.step_dir(6, "audit")
    seen: dict[str, Any] = {}

    def fake_run(argv, **kwargs):
        seen.update(
            argv=list(argv),
            cwd=kwargs.get("cwd"),
            env=kwargs.get("env"),
        )
        return SubprocessResult(
            argv=list(argv),
            returncode=0,
            stdout="audit clean\n",
            stderr="",
        )

    monkeypatch.setattr("loom_cli.rollout.steps.subcommand_step.run_captured", fake_run)

    result = AuditStep().run(ctx, step_dir)

    assert result.exit_code == 0
    _assert_candidate_invocation(seen, worktree=worktree)
    assert seen["argv"][3:6] == ["cluster", "audit", "--config"]


def test_backup_resolves_loom_cli_without_global_executable(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ctx = make_ctx(tmp_path)
    ev = EvidenceDirectory(tmp_path, "test-rid")
    ev.ensure()
    worktree = _prepare_candidate_worktree(ev)
    step_dir = ev.step_dir(5, "backup")
    seen: dict[str, Any] = {}

    def fake_run(argv, **kwargs):
        seen.update(
            argv=list(argv),
            cwd=kwargs.get("cwd"),
            env=kwargs.get("env"),
        )
        return SubprocessResult(
            argv=list(argv),
            returncode=0,
            stdout="backup ok\n",
            stderr="",
        )

    monkeypatch.setattr("loom_cli.rollout.steps.subcommand_step.run_captured", fake_run)

    result = BackupStep().run(ctx, step_dir)

    assert result.exit_code == 0
    _assert_candidate_invocation(seen, worktree=worktree)
    assert seen["argv"][3:6] == ["cluster", "backup", "check"]


def test_preflight_resolves_loom_cli_with_rollout_cluster_config(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ctx = make_ctx(tmp_path, image_tag="public-beta-new")
    ctx.cluster_config_path.write_text(
        'image_tag = "public-beta-old"\nnamespace = "loom-public-beta"\n',
    )
    ev = EvidenceDirectory(tmp_path, "test-rid")
    ev.ensure()
    worktree = _prepare_candidate_worktree(ev)
    step_dir = ev.step_dir(8, "preflight")
    seen: dict[str, Any] = {}

    def fake_run(argv, **kwargs):
        seen.update(
            argv=list(argv),
            cwd=kwargs.get("cwd"),
            env=kwargs.get("env"),
        )
        return SubprocessResult(
            argv=list(argv),
            returncode=0,
            stdout="preflight ok\n",
            stderr="",
        )

    monkeypatch.setattr("loom_cli.rollout.steps.subcommand_step.run_captured", fake_run)

    result = PreflightStep().run(ctx, step_dir)

    assert result.exit_code == 0
    _assert_candidate_invocation(seen, worktree=worktree)
    config_path = Path(seen["argv"][seen["argv"].index("--config") + 1])
    assert config_path != ctx.cluster_config_path
    assert tomllib.loads(config_path.read_text())["image_tag"] == ctx.image_tag


def test_cluster_up_runs_loom_cli_from_candidate_worktree(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ctx = make_ctx(tmp_path)
    ev = EvidenceDirectory(tmp_path, "test-rid")
    ev.ensure()
    worktree = _prepare_candidate_worktree(ev)
    step_dir = ev.step_dir(11, "cluster-up")
    seen: dict[str, Any] = {}

    def fake_run(argv, **kwargs):
        seen.update(
            argv=list(argv),
            cwd=kwargs.get("cwd"),
            env=kwargs.get("env"),
        )
        if kwargs.get("stdout_log"):
            kwargs["stdout_log"].write_text("cluster ready\n")
        return SubprocessResult(
            argv=list(argv),
            returncode=0,
            stdout="cluster ready\n",
            stderr="",
        )

    monkeypatch.setattr("loom_cli.rollout.steps.subcommand_step.run_captured", fake_run)

    result = ClusterUpStep().run(ctx, step_dir)

    assert result.exit_code == 0
    _assert_candidate_invocation(seen, worktree=worktree)


def test_release_gate_argv_passes_generated_release_manifest(tmp_path: Path) -> None:
    ctx = make_ctx(tmp_path, image_tag="public-beta-abc123")
    ev = EvidenceDirectory(tmp_path, "test-rid")
    ev.ensure()
    step_dir = ev.step_dir(12, "release-gate")

    argv = list(ReleaseGateStep().argv(ctx, step_dir))

    assert "--manifest" in argv
    manifest = Path(argv[argv.index("--manifest") + 1])
    assert manifest == step_dir.artifact_path("release-manifest-public-beta-abc123.json")


def test_rollout_cluster_commands_use_config_with_context_image_tag(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ctx = make_ctx(tmp_path, image_tag="public-beta-new")
    ctx.cluster_config_path.write_text(
        'image_tag = "public-beta-old"\nnamespace = "loom-public-beta"\n',
    )
    ev = EvidenceDirectory(tmp_path, "test-rid")
    ev.ensure()
    _prepare_candidate_worktree(ev)
    render_dir = ev.step_dir(7, "render")
    render_call: dict[str, Any] = {}

    def fake_render(argv, **kwargs):
        render_call.update(argv=list(argv))
        return SubprocessResult(
            argv=list(argv),
            returncode=0,
            stdout="kind: List\nitems: []\n",
            stderr="",
        )

    monkeypatch.setattr("loom_cli.rollout.steps.s07_render.run_captured", fake_render)

    result = RenderStep().run(ctx, render_dir)

    assert result.exit_code == 0
    rendered_config = Path(
        render_call["argv"][render_call["argv"].index("--config") + 1],
    )
    cluster_up_argv = list(ClusterUpStep().argv(ctx, ev.step_dir(11, "cluster-up")))
    cluster_up_config = Path(cluster_up_argv[cluster_up_argv.index("--config") + 1])
    manifest_argv = list(
        ReleaseGateStep().release_manifest_argv(ctx, ev.step_dir(12, "release-gate")),
    )
    release_manifest_config = Path(manifest_argv[manifest_argv.index("--config") + 1])
    gate_argv = list(ReleaseGateStep().argv(ctx, ev.step_dir(12, "release-gate")))
    release_gate_config = Path(gate_argv[gate_argv.index("--config") + 1])

    assert rendered_config != ctx.cluster_config_path
    assert {
        rendered_config,
        cluster_up_config,
        release_manifest_config,
        release_gate_config,
    } == {rendered_config}
    assert rendered_config.is_file()
    rendered_raw = tomllib.loads(rendered_config.read_text())
    original_raw = tomllib.loads(ctx.cluster_config_path.read_text())
    assert rendered_raw["image_tag"] == "public-beta-new"
    assert original_raw["image_tag"] == "public-beta-old"


def test_rollout_cluster_config_is_stable_after_first_synthesis(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ctx = make_ctx(tmp_path, image_tag="public-beta-new")
    ctx.cluster_config_path.write_text(
        'image_tag = "public-beta-old"\nnamespace = "loom-public-beta"\n',
    )
    ev = EvidenceDirectory(tmp_path, "test-rid")
    ev.ensure()
    _prepare_candidate_worktree(ev)
    render_call: dict[str, Any] = {}

    def fake_render(argv, **kwargs):
        render_call.update(argv=list(argv))
        return SubprocessResult(
            argv=list(argv),
            returncode=0,
            stdout="kind: List\nitems: []\n",
            stderr="",
        )

    monkeypatch.setattr("loom_cli.rollout.steps.s07_render.run_captured", fake_render)

    result = RenderStep().run(ctx, ev.step_dir(7, "render"))

    assert result.exit_code == 0
    rendered_config = Path(
        render_call["argv"][render_call["argv"].index("--config") + 1],
    )
    ctx.cluster_config_path.write_text(
        'image_tag = "public-beta-old"\nnamespace = "changed-after-render"\n',
    )

    cluster_up_argv = list(ClusterUpStep().argv(ctx, ev.step_dir(11, "cluster-up")))
    cluster_up_config = Path(cluster_up_argv[cluster_up_argv.index("--config") + 1])
    rendered_raw = tomllib.loads(cluster_up_config.read_text())

    assert cluster_up_config == rendered_config
    assert rendered_raw["image_tag"] == "public-beta-new"
    assert rendered_raw["namespace"] == "loom-public-beta"


def test_release_gate_run_generates_manifest_then_gates(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ctx = make_ctx(tmp_path, image_tag="public-beta-abc123")
    ev = EvidenceDirectory(tmp_path, "test-rid")
    ev.ensure()
    worktree = _prepare_candidate_worktree(ev)
    _write_rendered_service(ev)
    step_dir = ev.step_dir(12, "release-gate")
    calls: list[dict[str, Any]] = []

    def fake_run(argv, **kwargs):
        calls.append(
            {
                "argv": list(argv),
                "cwd": kwargs.get("cwd"),
                "env": kwargs.get("env"),
            }
        )
        if list(argv[:3]) == ["docker", "image", "inspect"]:
            return _docker_inspect_success(list(argv))
        if "release-manifest" in argv:
            output = Path(argv[argv.index("--output") + 1])
            output.write_text('{"schema_version": 1}\n')
        if kwargs.get("stdout_log"):
            kwargs["stdout_log"].write_text("ok\n")
        if kwargs.get("stderr_log"):
            kwargs["stderr_log"].write_text("")
        return SubprocessResult(
            argv=list(argv),
            returncode=0,
            stdout="ok\n",
            stderr="",
        )

    monkeypatch.setattr("loom_cli.rollout.steps.s12_release_gate.run_captured", fake_run)

    result = ReleaseGateStep().run(ctx, step_dir)

    assert result.exit_code == 0
    assert len(calls) == 4
    assert calls[0]["argv"][:3] == ["docker", "image", "inspect"]
    for call in calls[1:]:
        _assert_candidate_invocation(call, worktree=worktree)
    assert calls[1]["argv"][3:5] == ["cluster", "release-manifest"]
    assert calls[2]["argv"][3:6] == ["admin", "gb10-workers", "status"]
    assert calls[3]["argv"][3:5] == ["cluster", "release-gate"]
    manifest = step_dir.artifact_path("release-manifest-public-beta-abc123.json")
    assert calls[1]["argv"][calls[1]["argv"].index("--output") + 1] == str(manifest)
    assert calls[3]["argv"][calls[3]["argv"].index("--manifest") + 1] == str(manifest)
    assert "--gb10-workers-status" in calls[3]["argv"]


def test_release_gate_records_expected_image_identities_before_manifest(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ctx = make_ctx(tmp_path, image_tag="public-beta-abc123")
    ev = EvidenceDirectory(tmp_path, "test-rid")
    ev.ensure()
    _prepare_candidate_worktree(ev)
    rendered = ev.step_dir(7, "render").artifact_path("rendered.yaml")
    rendered.write_text(
        """
apiVersion: apps/v1
kind: Deployment
metadata:
  name: loom-service
spec:
  template:
    spec:
      containers:
        - name: loom-service
          image: loom-service:public-beta-abc123
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: loom-egress-proxy
spec:
  template:
    spec:
      containers:
        - name: envoy
          image: envoyproxy/envoy:v1.30-latest
""",
        encoding="utf-8",
    )
    step_dir = ev.step_dir(12, "release-gate")

    def fake_run(argv, **kwargs):
        if list(argv[:3]) == ["docker", "image", "inspect"]:
            assert "envoyproxy/envoy:v1.30-latest" not in argv
            docs = []
            for image in argv[3:]:
                docs.append(
                    {
                        "Id": "sha256:" + "1" * 64,
                        "RepoTags": [image],
                        "RepoDigests": [
                            image.split(":", 1)[0] + "@sha256:" + "2" * 64,
                        ],
                        "Config": {"Env": ["LOOM_ADMIN_TOKEN=do-not-write"]},
                    }
                )
            return SubprocessResult(
                argv=list(argv),
                returncode=0,
                stdout=json.dumps(docs),
                stderr="",
            )
        if "release-manifest" in argv:
            assert "--expected-image-identities-json" in argv
            identities_path = Path(
                argv[argv.index("--expected-image-identities-json") + 1],
            )
            assert identities_path == step_dir.artifact_path(
                "image-identities-public-beta-abc123.json",
            )
            body = json.loads(identities_path.read_text(encoding="utf-8"))
            assert body == {
                "loom-service": {
                    "loom-service": {
                        "image": "loom-service:public-beta-abc123",
                        "image_id": "sha256:" + "1" * 64,
                        "repo_digest": "loom-service@sha256:" + "2" * 64,
                    },
                },
            }
            assert "do-not-write" not in identities_path.read_text(encoding="utf-8")
            output = Path(argv[argv.index("--output") + 1])
            output.write_text('{"schema_version": 1}\n')
            return SubprocessResult(
                argv=list(argv),
                returncode=0,
                stdout="manifest\n",
                stderr="",
            )
        return SubprocessResult(
            argv=list(argv),
            returncode=0,
            stdout="ok\n",
            stderr="",
        )

    monkeypatch.setattr("loom_cli.rollout.steps.s12_release_gate.run_captured", fake_run)

    result = ReleaseGateStep().run(ctx, step_dir)

    assert result.exit_code == 0


def test_release_gate_run_fails_fast_when_gb10_status_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ctx = make_ctx(tmp_path, image_tag="public-beta-abc123")
    ev = EvidenceDirectory(tmp_path, "test-rid")
    ev.ensure()
    _prepare_candidate_worktree(ev)
    _write_rendered_service(ev)
    step_dir = ev.step_dir(12, "release-gate")
    calls: list[list[str]] = []

    def fake_run(argv, **kwargs):
        calls.append(list(argv))
        if list(argv[:3]) == ["docker", "image", "inspect"]:
            return _docker_inspect_success(list(argv))
        if "release-manifest" in argv:
            output = Path(argv[argv.index("--output") + 1])
            output.write_text('{"schema_version": 1}\n')
            return SubprocessResult(
                argv=list(argv),
                returncode=0,
                stdout="manifest\n",
                stderr="",
            )
        if "gb10-workers" in argv:
            if kwargs.get("stderr_log"):
                kwargs["stderr_log"].write_text("GB10 rollout target mismatch\n")
            return SubprocessResult(
                argv=list(argv),
                returncode=1,
                stdout="{}",
                stderr="GB10 rollout target mismatch\n",
            )
        raise AssertionError("release-gate should not run after GB10 status failure")

    monkeypatch.setattr("loom_cli.rollout.steps.s12_release_gate.run_captured", fake_run)

    result = ReleaseGateStep().run(ctx, step_dir)

    assert result.exit_code == 1
    assert result.error == "GB10 rollout target mismatch"
    non_docker_calls = [
        call for call in calls
        if call[:3] != ["docker", "image", "inspect"]
    ]
    assert [call[3:6] for call in non_docker_calls] == [
        ["cluster", "release-manifest", "--config"],
        ["admin", "gb10-workers", "status"],
    ]


def test_release_gate_retries_transient_gb10_status_cp_unreachable(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ctx = make_ctx(tmp_path, image_tag="public-beta-abc123")
    ev = EvidenceDirectory(tmp_path, "test-rid")
    ev.ensure()
    _prepare_candidate_worktree(ev)
    _write_rendered_service(ev)
    step_dir = ev.step_dir(12, "release-gate")
    calls: list[list[str]] = []
    gb10_attempts = 0

    def fake_run(argv, **kwargs):
        nonlocal gb10_attempts
        calls.append(list(argv))
        if list(argv[:3]) == ["docker", "image", "inspect"]:
            return _docker_inspect_success(list(argv))
        if "release-manifest" in argv:
            output = Path(argv[argv.index("--output") + 1])
            output.write_text('{"schema_version": 1}\n')
            return SubprocessResult(
                argv=list(argv),
                returncode=0,
                stdout="manifest\n",
                stderr="",
            )
        if "gb10-workers" in argv:
            gb10_attempts += 1
            if gb10_attempts < 3:
                return SubprocessResult(
                    argv=list(argv),
                    returncode=2,
                    stdout="",
                    stderr=(
                        "error: could not reach CP at "
                        "http://127.0.0.1:18081/admin/gb10-worker-pools/status: "
                        "[Errno 111] Connection refused\n"
                    ),
                )
            if kwargs.get("stdout_log"):
                kwargs["stdout_log"].write_text('{"nodes":[],"desired_states":[]}\n')
            if kwargs.get("stderr_log"):
                kwargs["stderr_log"].write_text("")
            return SubprocessResult(
                argv=list(argv),
                returncode=0,
                stdout='{"nodes":[],"desired_states":[]}\n',
                stderr="",
            )
        if "release-gate" in argv:
            return SubprocessResult(
                argv=list(argv),
                returncode=0,
                stdout="gate\n",
                stderr="",
            )
        raise AssertionError(f"unexpected argv: {argv}")

    monkeypatch.setattr("loom_cli.rollout.steps.s12_release_gate.run_captured", fake_run)
    monkeypatch.setattr(
        "loom_cli.rollout.steps.s12_release_gate._GB10_STATUS_RETRY_DELAY_SEC",
        0.0,
        raising=False,
    )

    result = ReleaseGateStep().run(ctx, step_dir)

    assert result.exit_code == 0
    assert gb10_attempts == 3
    non_docker_calls = [
        call for call in calls
        if call[:3] != ["docker", "image", "inspect"]
    ]
    assert [call[3:6] for call in non_docker_calls] == [
        ["cluster", "release-manifest", "--config"],
        ["admin", "gb10-workers", "status"],
        ["admin", "gb10-workers", "status"],
        ["admin", "gb10-workers", "status"],
        ["cluster", "release-gate", "--manifest"],
    ]


def test_release_gate_retries_gb10_convergence_until_node_agent_reports_current(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ctx = make_ctx(tmp_path, image_tag="public-beta-53897aa")
    ev = EvidenceDirectory(tmp_path, "test-rid")
    ev.ensure()
    _prepare_candidate_worktree(ev)
    _write_rendered_service(ev, image_tag="public-beta-53897aa")
    step_dir = ev.step_dir(12, "release-gate")
    status_attempts = 0
    gate_attempts = 0

    def fake_run(argv, **kwargs):
        nonlocal status_attempts, gate_attempts
        if list(argv[:3]) == ["docker", "image", "inspect"]:
            return _docker_inspect_success(list(argv))
        if "release-manifest" in argv:
            output = Path(argv[argv.index("--output") + 1])
            output.write_text('{"schema_version": 1}\n')
            return SubprocessResult(
                argv=list(argv),
                returncode=0,
                stdout="manifest\n",
                stderr="",
            )
        if "gb10-workers" in argv:
            status_attempts += 1
            body = (
                '{"nodes":[{"source_git_commit":"old"}],"desired_states":[]}\n'
                if status_attempts == 1
                else '{"nodes":[{"source_git_commit":"53897aa"}],"desired_states":[]}\n'
            )
            if kwargs.get("stdout_log"):
                kwargs["stdout_log"].write_text(body)
            return SubprocessResult(
                argv=list(argv),
                returncode=0,
                stdout=body,
                stderr="",
            )
        if "release-gate" in argv:
            gate_attempts += 1
            if gate_attempts == 1:
                return SubprocessResult(
                    argv=list(argv),
                    returncode=1,
                    stdout="gb10-worker-convergence fail stale source\n",
                    stderr="",
                )
            return SubprocessResult(
                argv=list(argv),
                returncode=0,
                stdout="gb10-worker-convergence pass\n",
                stderr="",
            )
        raise AssertionError(f"unexpected argv: {argv}")

    monkeypatch.setattr("loom_cli.rollout.steps.s12_release_gate.run_captured", fake_run)
    monkeypatch.setattr(
        "loom_cli.rollout.steps.s12_release_gate._GB10_STATUS_RETRY_DELAY_SEC",
        0.0,
        raising=False,
    )

    result = ReleaseGateStep().run(ctx, step_dir)

    assert result.exit_code == 0
    assert status_attempts == 2
    assert gate_attempts == 2
