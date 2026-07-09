"""Regression tests for candidate-source rollout command invocation (#441)."""

from __future__ import annotations

import json
import os
import sys
import threading
import time
import tomllib
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from loom_cli.rollout.base_context_fixture import make_ctx
from loom_cli.rollout.evidence import EvidenceDirectory
from loom_cli.rollout.steps.base import VerifyOutcome
from loom_cli.rollout.steps.s03_kind_load_images import KindLoadImagesStep
from loom_cli.rollout.steps.s04_gb10_prep import (
    GB10Host,
    GB10PrepStep,
    _ssh,
    gb10_hosts_for,
)
from loom_cli.rollout.steps.s05_backup import BackupStep
from loom_cli.rollout.steps.s06_audit import AuditStep
from loom_cli.rollout.steps.s07_render import RenderStep
from loom_cli.rollout.steps.s08_preflight import PreflightStep
from loom_cli.rollout.steps.s09_migrate import MigrateStep
from loom_cli.rollout.steps.s10_env_state import EnvStateStep, _profile_path_for
from loom_cli.rollout.steps.s11_cluster_up import ClusterUpStep
from loom_cli.rollout.steps.s12_production_defaults import ProductionDefaultsStep
from loom_cli.rollout.steps.s12_release_gate import (
    ReleaseGateStep,
    _is_gb10_convergence_failure,
)
from loom_cli.rollout.steps.subprocess_util import SubprocessResult


@pytest.fixture(autouse=True)
def _control_plane_ready(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "loom_cli.rollout.steps.s10_env_state._wait_for_control_plane",
        lambda *_args, **_kwargs: None,
    )


def _prepare_candidate_worktree(ev: EvidenceDirectory) -> Path:
    worktree = ev.step_dir(1, "worktree").path / "src"
    package_dir = worktree / "src" / "loom_cli"
    package_dir.mkdir(parents=True)
    (package_dir / "__main__.py").write_text("raise SystemExit(0)\n")
    return worktree


@pytest.fixture(autouse=True)
def _control_plane_readiness_is_out_of_scope(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "loom_cli.rollout.steps.s10_env_state._wait_for_control_plane",
        lambda *_args, **_kwargs: None,
    )


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
    image_tag: str = "staging-abc123",
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


def _write_rendered_stateful_substrate(ev: EvidenceDirectory) -> Path:
    rendered = ev.step_dir(7, "render").artifact_path("rendered.yaml")
    rendered.write_text(
        """
apiVersion: v1
kind: PersistentVolume
metadata: { name: loom-staging-postgres-data }
---
apiVersion: v1
kind: PersistentVolume
metadata: { name: loom-staging-minio-data }
---
apiVersion: apps/v1
kind: StatefulSet
metadata: { name: loom-postgres }
---
apiVersion: v1
kind: Service
metadata: { name: loom-postgres }
---
apiVersion: apps/v1
kind: StatefulSet
metadata: { name: loom-minio }
---
apiVersion: v1
kind: Service
metadata: { name: loom-minio }
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


def _release_manifest_with_gb10_contract() -> str:
    return (
        json.dumps(
            {
                "schema_version": 1,
                "external_workers": {
                    "control_plane_environment": "production",
                    "gb10_desired_states": [
                        {
                            "pool_name": "gb10-arm64",
                            "image_tag": "staging-abc123",
                            "env_config_version": "staging-abc123",
                            "source_git_commit": "a" * 40,
                        }
                    ],
                },
            }
        )
        + "\n"
    )


def _write_dummy_identity(path: Path) -> Path:
    path.write_text("not-a-real-key\n", encoding="utf-8")
    path.chmod(0o600)
    return path


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
    _write_rendered_stateful_substrate(ev)
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
    step_dir = ev.step_dir(11, "env-state")
    profile = tmp_path / "staging.toml"
    profile.write_text('environment = "staging"\n')
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
        assert call["argv"][call["argv"].index("--environment") + 1] == ctx.environment
        assert call["argv"][call["argv"].index("--admin-token") + 1] == ("env:LOOM_CP_ADMIN_TOKEN")
        assert "--var" in call["argv"]
        assert f"GIT_SHA={ctx.resolved_sha}" in call["argv"]


def test_env_state_passes_pinned_admin_token_source_and_fingerprint(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ctx = make_ctx(
        tmp_path,
        admin_token_source="file:/secure/path/staging-admin-token",
        expect_admin_token_fingerprint="sha256:abc123def456 len=64",
    )
    ev = EvidenceDirectory(tmp_path, "test-rid")
    ev.ensure()
    _prepare_candidate_worktree(ev)
    step_dir = ev.step_dir(11, "env-state")
    profile = tmp_path / "staging.toml"
    profile.write_text('environment = "staging"\n')
    calls: list[list[str]] = []

    def fake_run(argv, **kwargs):
        calls.append(list(argv))
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
    for argv in calls:
        assert argv[argv.index("--admin-token") + 1] == ("file:/secure/path/staging-admin-token")
        assert argv[argv.index("--expect-admin-token-fingerprint") + 1] == (
            "sha256:abc123def456 len=64"
        )
        assert "raw-secret-token" not in argv


def test_env_state_passes_worker_token_source_to_check_only(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ctx = make_ctx(
        tmp_path,
        admin_token_source="file:/secure/path/staging-admin-token",
        worker_token_source="file:/secure/path/staging-worker-token",
    )
    ev = EvidenceDirectory(tmp_path, "test-rid")
    ev.ensure()
    _prepare_candidate_worktree(ev)
    step_dir = ev.step_dir(11, "env-state")
    profile = tmp_path / "staging.toml"
    profile.write_text('environment = "staging"\n')
    calls: list[list[str]] = []

    def fake_run(argv, **kwargs):
        calls.append(list(argv))
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
    apply_argv, check_argv = calls
    assert apply_argv[3:6] == ["admin", "environment-state", "apply"]
    assert check_argv[3:6] == ["admin", "environment-state", "check"]
    assert "--worker-token" not in apply_argv
    assert check_argv[check_argv.index("--worker-token") + 1] == (
        "file:/secure/path/staging-worker-token"
    )
    assert "staging-worker-token-value" not in str(check_argv)


def test_env_state_materializes_current_profile_under_rollout_root(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ctx = make_ctx(tmp_path)
    ev = EvidenceDirectory(tmp_path, "test-rid")
    ev.ensure()
    _prepare_candidate_worktree(ev)
    step_dir = ev.step_dir(11, "env-state")
    source_profile = tmp_path / "candidate" / "deploy" / "environment-state" / "staging.toml"
    source_profile.parent.mkdir(parents=True)
    source_profile.write_text(
        'environment = "staging"\n\n[catalog_provisioning]\nrequired = false\n',
        encoding="utf-8",
    )
    physical_profile = tmp_path / "environment-state" / "staging.toml"
    physical_profile.parent.mkdir()
    physical_profile.write_text('environment = "stale"\n', encoding="utf-8")

    def fake_run(argv, **kwargs):
        return SubprocessResult(
            argv=list(argv),
            returncode=0,
            stdout=json.dumps({"ok": True, "drift": []}),
            stderr="",
        )

    monkeypatch.setattr(
        "loom_cli.rollout.steps.s10_env_state._profile_path_for",
        lambda ctx: str(source_profile),
    )
    monkeypatch.setattr("loom_cli.rollout.steps.s10_env_state.run_captured", fake_run)

    result = EnvStateStep().run(ctx, step_dir)

    assert result.exit_code == 0
    assert physical_profile.read_text(encoding="utf-8") == source_profile.read_text(
        encoding="utf-8",
    )
    assert oct(physical_profile.stat().st_mode & 0o777) == "0o600"
    evidence = json.loads(
        step_dir.artifact_path("environment-state-profile-materialization.json").read_text(
            encoding="utf-8",
        ),
    )
    assert evidence["source_path"] == str(source_profile)
    assert evidence["target_path"] == str(physical_profile)
    assert evidence["source_sha256"] == evidence["target_sha256"]
    assert evidence["mode"] == "0o600"


def test_env_state_runs_catalog_provisioning_between_apply_and_check(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    env_file = tmp_path / "catalog.env"
    hf_token = "hf_" + "a" * 32
    db_url = "postgresql://loom:catalog-secret@postgres/loom"
    env_file.write_text(
        f"HF_TOKEN={hf_token}\n"
        f"LOOM_SVC_DB_URL={db_url}\n"
        "LOOM_SVC_MINIO_ENDPOINT=http://minio:9000\n"
        "LOOM_SVC_MINIO_ACCESS_KEY=minio-access-secret\n"
        "LOOM_SVC_MINIO_SECRET_KEY=minio-secret-secret\n",
        encoding="utf-8",
    )
    env_file.chmod(0o600)
    ctx = make_ctx(tmp_path, image_tag="staging-abc123")
    ev = EvidenceDirectory(tmp_path, "test-rid")
    ev.ensure()
    worktree = _prepare_candidate_worktree(ev)
    step_dir = ev.step_dir(11, "env-state")
    profile = tmp_path / "staging.toml"
    profile.write_text(
        f"""
environment = "staging"

[catalog_provisioning]
required = true
command = "loom datasets provision-catalog && loom datasets publish-local deploy/catalog/gb10-smoke"
env_file = "{env_file}"
required_env = [
  "PUBLISHED_SHA",
  "HF_TOKEN",
  "LOOM_SVC_DB_URL",
  "LOOM_SVC_MINIO_ENDPOINT",
  "LOOM_SVC_MINIO_ACCESS_KEY",
  "LOOM_SVC_MINIO_SECRET_KEY",
]

[catalog_provisioning.env]
PUBLISHED_SHA = "79087002d62bb22169a704bc941c8d614082d880"
""".lstrip(),
        encoding="utf-8",
    )
    calls: list[dict[str, Any]] = []
    monkeypatch.setenv("PATH", "/usr/bin")

    def fake_run(argv, **kwargs):
        call = {
            "argv": list(argv),
            "cwd": kwargs.get("cwd"),
            "env": kwargs.get("env"),
        }
        calls.append(call)
        if list(argv)[3:6] == ["admin", "environment-state", "apply"]:
            return SubprocessResult(
                argv=list(argv),
                returncode=0,
                stdout="applied\n",
                stderr="",
            )
        if list(argv)[:2] == ["bash", "-euo"]:
            assert kwargs["cwd"] == worktree
            path_entries = kwargs["env"]["PATH"].split(os.pathsep)
            assert path_entries[0] == str(Path(sys.executable).parent)
            assert "/usr/bin" in path_entries[1:]
            assert kwargs["env"]["HF_TOKEN"] == hf_token
            assert kwargs["env"]["LOOM_SVC_DB_URL"] == db_url
            assert kwargs["env"]["PUBLISHED_SHA"] == ("79087002d62bb22169a704bc941c8d614082d880")
            return SubprocessResult(
                argv=list(argv),
                returncode=0,
                stdout=f"catalog ok {hf_token} {db_url}\n",
                stderr=f"warning access {kwargs['env']['LOOM_SVC_MINIO_ACCESS_KEY']}\n",
            )
        if list(argv)[3:6] == ["admin", "environment-state", "check"]:
            return SubprocessResult(
                argv=list(argv),
                returncode=0,
                stdout='{"ok": true, "drift": [], "autoscaler_blockers": []}\n',
                stderr="",
            )
        raise AssertionError(f"unexpected argv: {argv}")

    monkeypatch.setattr(
        "loom_cli.rollout.steps.s10_env_state._profile_path_for",
        lambda ctx: str(profile),
    )
    monkeypatch.setattr("loom_cli.rollout.steps.s10_env_state.run_captured", fake_run)

    result = EnvStateStep().run(ctx, step_dir)

    assert result.exit_code == 0
    assert [
        call["argv"][3:6]
        for call in calls
        if call["argv"][:3] == [sys.executable, "-m", "loom_cli"]
    ] == [
        ["admin", "environment-state", "apply"],
        ["admin", "environment-state", "check"],
    ]
    assert calls[1]["argv"] == [
        "bash",
        "-euo",
        "pipefail",
        "-c",
        "loom datasets provision-catalog && loom datasets publish-local deploy/catalog/gb10-smoke",
    ]
    for call in (calls[0], calls[2]):
        _assert_candidate_invocation(call, worktree=worktree)
    stdout = step_dir.stdout_path().read_text(encoding="utf-8")
    stderr = step_dir.stderr_path().read_text(encoding="utf-8")
    artifact = step_dir.artifact_path("catalog-provisioning.json").read_text(
        encoding="utf-8",
    )
    combined = stdout + stderr + artifact + json.dumps(result.artifacts)
    assert hf_token not in combined
    assert db_url not in combined
    assert "minio-access-secret" not in combined
    assert "[REDACTED:HF_TOKEN]" in combined
    assert "[REDACTED:LOOM_SVC_DB_URL]" in combined
    evidence = json.loads(artifact)
    assert evidence["returncode"] == 0
    assert evidence["env_file"]["path"] == str(env_file)
    assert evidence["required_env"] == [
        "PUBLISHED_SHA",
        "HF_TOKEN",
        "LOOM_SVC_DB_URL",
        "LOOM_SVC_MINIO_ENDPOINT",
        "LOOM_SVC_MINIO_ACCESS_KEY",
        "LOOM_SVC_MINIO_SECRET_KEY",
    ]


def test_env_state_catalog_provisioning_forwards_cluster_local_db_and_minio(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    env_file = tmp_path / "catalog.env"
    env_file.write_text(
        "HF_TOKEN=hf_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\n"
        "LOOM_SVC_DB_URL=postgresql+psycopg://loom:secret@loom-postgres:5432/loom\n"
        "LOOM_SVC_MINIO_ENDPOINT=http://loom-minio:9000\n"
        "LOOM_SVC_MINIO_ACCESS_KEY=minio-access-secret\n"
        "LOOM_SVC_MINIO_SECRET_KEY=minio-secret-secret\n",
        encoding="utf-8",
    )
    env_file.chmod(0o600)
    ctx = make_ctx(tmp_path, image_tag="staging-abc123")
    ev = EvidenceDirectory(tmp_path, "test-rid")
    ev.ensure()
    worktree = _prepare_candidate_worktree(ev)
    step_dir = ev.step_dir(11, "env-state")
    profile = tmp_path / "staging.toml"
    profile.write_text(
        f"""
environment = "staging"

[catalog_provisioning]
required = true
command = "loom datasets register skilllearnbench && loom datasets publish-local deploy/catalog/gb10-smoke"
env_file = "{env_file}"
required_env = [
  "PUBLISHED_SHA",
  "HF_TOKEN",
  "LOOM_SVC_DB_URL",
  "LOOM_SVC_MINIO_ENDPOINT",
  "LOOM_SVC_MINIO_ACCESS_KEY",
  "LOOM_SVC_MINIO_SECRET_KEY",
]

[catalog_provisioning.env]
PUBLISHED_SHA = "79087002d62bb22169a704bc941c8d614082d880"

[catalog_provisioning.kubernetes_port_forward]
enabled = true
postgres_service = "service/loom-postgres"
postgres_remote_port = 5432
minio_service = "service/loom-minio"
minio_remote_port = 9000
""".lstrip(),
        encoding="utf-8",
    )
    calls: list[dict[str, Any]] = []
    forwarded: list[dict[str, Any]] = []
    ports = iter([15432, 19000])

    def fake_reserve_port() -> int:
        return next(ports)

    def fake_start_forward(**kwargs: Any) -> SimpleNamespace:
        forwarded.append(kwargs)
        return SimpleNamespace(
            **kwargs,
            stdout_log=step_dir.artifact_path(
                f"fake-port-forward-{kwargs['name']}.stdout",
            ),
            stderr_log=step_dir.artifact_path(
                f"fake-port-forward-{kwargs['name']}.stderr",
            ),
            stdout_handle=None,
            stderr_handle=None,
            process=None,
        )

    def fake_stop_forward(_handle: object) -> None:
        return None

    def fake_run(argv, **kwargs):
        calls.append(
            {
                "argv": list(argv),
                "cwd": kwargs.get("cwd"),
                "env": kwargs.get("env"),
            }
        )
        if list(argv)[3:6] == ["admin", "environment-state", "apply"]:
            return SubprocessResult(
                argv=list(argv),
                returncode=0,
                stdout="applied\n",
                stderr="",
            )
        if list(argv)[:2] == ["bash", "-euo"]:
            assert kwargs["cwd"] == worktree
            assert kwargs["env"]["LOOM_SVC_DB_URL"] == (
                "postgresql+psycopg://loom:secret@127.0.0.1:15432/loom"
            )
            assert kwargs["env"]["LOOM_SVC_MINIO_ENDPOINT"] == "http://127.0.0.1:19000"
            return SubprocessResult(
                argv=list(argv),
                returncode=0,
                stdout="catalog ok\n",
                stderr="",
            )
        if list(argv)[3:6] == ["admin", "environment-state", "check"]:
            return SubprocessResult(
                argv=list(argv),
                returncode=0,
                stdout='{"ok": true, "drift": [], "autoscaler_blockers": []}\n',
                stderr="",
            )
        raise AssertionError(f"unexpected argv: {argv}")

    monkeypatch.setattr(
        "loom_cli.rollout.steps.s10_env_state._profile_path_for",
        lambda ctx: str(profile),
    )
    monkeypatch.setattr("loom_cli.rollout.steps.s10_env_state.run_captured", fake_run)
    monkeypatch.setattr(
        "loom_cli.rollout.steps.s10_env_state._reserve_local_port",
        fake_reserve_port,
        raising=False,
    )
    monkeypatch.setattr(
        "loom_cli.rollout.steps.s10_env_state._start_catalog_port_forward",
        fake_start_forward,
        raising=False,
    )
    monkeypatch.setattr(
        "loom_cli.rollout.steps.s10_env_state._stop_catalog_port_forward",
        fake_stop_forward,
        raising=False,
    )

    result = EnvStateStep().run(ctx, step_dir)

    assert result.exit_code == 0
    assert [item["local_port"] for item in forwarded] == [15432, 19000]
    assert [item["remote_port"] for item in forwarded] == [5432, 9000]
    assert [item["resource"] for item in forwarded] == [
        "service/loom-postgres",
        "service/loom-minio",
    ]
    assert {item["namespace"] for item in forwarded} == {ctx.namespace}
    artifact = json.loads(
        step_dir.artifact_path("catalog-provisioning.json").read_text(
            encoding="utf-8",
        )
    )
    assert artifact["kubernetes_port_forward"]["enabled"] is True
    assert "secret" not in json.dumps(artifact)


def test_env_state_fails_before_mutation_when_catalog_required_env_missing(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ctx = make_ctx(tmp_path)
    ev = EvidenceDirectory(tmp_path, "test-rid")
    ev.ensure()
    _prepare_candidate_worktree(ev)
    step_dir = ev.step_dir(11, "env-state")
    profile = tmp_path / "staging.toml"
    profile.write_text(
        """
environment = "staging"

[catalog_provisioning]
required = true
command = "loom datasets publish-local deploy/catalog/gb10-smoke"
required_env = ["HF_TOKEN"]
""".lstrip(),
        encoding="utf-8",
    )
    calls: list[list[str]] = []

    monkeypatch.delenv("HF_TOKEN", raising=False)
    monkeypatch.setattr(
        "loom_cli.rollout.steps.s10_env_state._profile_path_for",
        lambda ctx: str(profile),
    )
    monkeypatch.setattr(
        "loom_cli.rollout.steps.s10_env_state.run_captured",
        lambda argv, **kwargs: calls.append(list(argv)),
    )

    result = EnvStateStep().run(ctx, step_dir)

    assert result.exit_code == 2
    assert "catalog provisioning missing required env: HF_TOKEN" in (result.error or "")
    assert calls == []


def test_env_state_defers_gb10_node_status_drift_to_release_gate(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ctx = make_ctx(tmp_path, image_tag="staging-80f7e01")
    ev = EvidenceDirectory(tmp_path, "test-rid")
    ev.ensure()
    _prepare_candidate_worktree(ev)
    step_dir = ev.step_dir(11, "env-state")
    profile = tmp_path / "staging.toml"
    profile.write_text('environment = "staging"\n')
    calls: list[list[str]] = []
    check_attempts = 0

    def fake_run(argv, **kwargs):
        nonlocal check_attempts
        calls.append(list(argv))
        if "apply" in argv:
            return SubprocessResult(
                argv=list(argv),
                returncode=0,
                stdout="applied\n",
                stderr="",
            )
        if "check" in argv:
            check_attempts += 1
            return SubprocessResult(
                argv=list(argv),
                returncode=1,
                stdout=json.dumps(
                    {
                        "ok": False,
                        "drift": [
                            {
                                "path": (
                                    "gb10_worker_node_status"
                                    "[production/gb10-arm64/trt-gb10-1]"
                                    ".source_git_commit"
                                ),
                                "desired": "80f7e01",
                                "live": "2e6cf2f",
                            },
                        ],
                        "autoscaler_blockers": [],
                    }
                ),
                stderr="",
            )
        raise AssertionError(argv)

    monkeypatch.setattr(
        "loom_cli.rollout.steps.s10_env_state._profile_path_for",
        lambda ctx: str(profile),
    )
    monkeypatch.setattr("loom_cli.rollout.steps.s10_env_state.run_captured", fake_run)

    result = EnvStateStep().run(ctx, step_dir)

    assert result.exit_code == 0
    assert check_attempts == 1
    assert len([call for call in calls if "check" in call]) == 1
    retry_log = step_dir.artifact_path("environment-state-check.retries.log")
    assert "gb10 node-status drift deferred to release-gate" in retry_log.read_text()


def test_release_gate_convergence_retry_budget_covers_worker_image_builds() -> None:
    from loom_cli.rollout.steps import s12_release_gate

    min_budget_sec = 15 * 60
    assert (
        s12_release_gate._GB10_STATUS_MAX_ATTEMPTS * s12_release_gate._GB10_STATUS_RETRY_DELAY_SEC
    ) >= min_budget_sec


def test_env_state_profile_path_resolves_from_cluster_config_dir(
    tmp_path: Path,
) -> None:
    config_dir = tmp_path / "deploy" / "environments"
    config_dir.mkdir(parents=True)
    profile = tmp_path / "deploy" / "environment-state" / "staging.toml"
    profile.parent.mkdir()
    profile.write_text('environment = "staging"\n', encoding="utf-8")
    ctx = make_ctx(tmp_path)
    ctx.cluster_config_path.write_text(
        'env_state_profile = "../environment-state/staging.toml"\n',
        encoding="utf-8",
    )
    config_path = config_dir / "staging.cluster.toml"
    config_path.write_text(
        'env_state_profile = "../environment-state/staging.toml"\n',
        encoding="utf-8",
    )

    assert _profile_path_for(ctx, config_path) == str(profile)


def test_production_defaults_syncs_rate_card_and_verifies_hosted_provider(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    token_file = tmp_path / "service-token"
    token_file.write_text("super-secret-service-token\n", encoding="utf-8")
    ctx = make_ctx(
        tmp_path,
        service_token_source=f"file:{token_file}",
    )
    ctx.cluster_config_path.write_text(
        """
ingress_host = "yylx.world"
frontend_route_path = "/dev"
frontend_api_base_path = "/dev"
""".lstrip(),
        encoding="utf-8",
    )
    ev = EvidenceDirectory(tmp_path, "test-rid")
    ev.ensure()
    worktree = _prepare_candidate_worktree(ev)
    step_dir = ev.step_dir(13, "production-defaults")
    profile = tmp_path / "staging.toml"
    profile.write_text(
        """
environment = "staging"

[rate_card_sync.yibuapi]
enabled = true
group = "default"

[[hosted_provider_pricing_defaults]]
name = "mz_tn_canada_qianyi"
pricing_source = "rate-card"
rate_card_provider = "yibuapi"
""".strip()
        + "\n",
        encoding="utf-8",
    )
    calls: list[dict[str, Any]] = []
    xdg_dirs: list[Path] = []

    def fake_run(argv, **kwargs):
        xdg_config_home = Path(kwargs["env"]["XDG_CONFIG_HOME"])
        xdg_dirs.append(xdg_config_home)
        config_text = (xdg_config_home / "loom" / "config.toml").read_text(
            encoding="utf-8",
        )
        assert 'server_url = "https://yylx.world/dev"' in config_text
        assert 'auth_token = "super-secret-service-token"' in config_text
        calls.append(
            {
                "argv": list(argv),
                "cwd": kwargs.get("cwd"),
                "env": kwargs.get("env"),
            }
        )
        if list(argv)[3:6] == ["admin", "rate-cards", "sync-yibuapi"]:
            return SubprocessResult(
                argv=list(argv),
                returncode=0,
                stdout='{"id":"yibuapi-pricing-v1","entry_count":128}\n',
                stderr="",
            )
        if list(argv)[3:5] == ["providers", "update"]:
            return SubprocessResult(
                argv=list(argv),
                returncode=0,
                stdout="updated\n",
                stderr="",
            )
        if list(argv)[3:5] == ["providers", "show"]:
            return SubprocessResult(
                argv=list(argv),
                returncode=0,
                stdout=json.dumps(
                    {
                        "name": "mz_tn_canada_qianyi",
                        "pricing_source": "rate-card",
                        "rate_card_provider": "yibuapi",
                    }
                ),
                stderr="",
            )
        raise AssertionError(f"unexpected argv: {argv}")

    monkeypatch.setattr(
        "loom_cli.rollout.steps.s12_production_defaults._profile_path_for",
        lambda ctx: str(profile),
    )
    monkeypatch.setattr(
        "loom_cli.rollout.steps.s12_production_defaults.run_captured",
        fake_run,
    )

    result = ProductionDefaultsStep().run(ctx, step_dir)

    assert result.exit_code == 0
    assert len(calls) == 3
    for call in calls:
        _assert_candidate_invocation(call, worktree=worktree)
        assert call["env"]["XDG_CONFIG_HOME"] != os.environ.get("XDG_CONFIG_HOME")
    assert calls[0]["argv"][3:] == [
        "admin",
        "rate-cards",
        "sync-yibuapi",
        "--group",
        "default",
        "--format",
        "json",
    ]
    assert calls[1]["argv"][3:] == [
        "providers",
        "update",
        "mz_tn_canada_qianyi",
        "--pricing-source",
        "rate-card",
        "--rate-card-provider",
        "yibuapi",
        "--admin-actor",
        "rollout-production-defaults",
    ]
    assert calls[2]["argv"][3:] == [
        "providers",
        "show",
        "mz_tn_canada_qianyi",
        "--format",
        "json",
    ]
    assert step_dir.artifact_path("rate-card-sync-yibuapi.json").is_file()
    assert step_dir.artifact_path(
        "provider-mz_tn_canada_qianyi.json",
    ).is_file()
    assert xdg_dirs
    assert all(not path.exists() for path in xdg_dirs)
    stdout = step_dir.stdout_path().read_text(encoding="utf-8")
    stderr = step_dir.stderr_path().read_text(encoding="utf-8")
    assert "super-secret-service-token" not in stdout
    assert "super-secret-service-token" not in stderr
    assert "super-secret-service-token" not in json.dumps(result.artifacts)


def test_production_defaults_requires_service_token_source_for_mutations(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ctx = make_ctx(tmp_path)
    ev = EvidenceDirectory(tmp_path, "test-rid")
    ev.ensure()
    _prepare_candidate_worktree(ev)
    step_dir = ev.step_dir(13, "production-defaults")
    profile = tmp_path / "staging.toml"
    profile.write_text(
        """
environment = "staging"

[rate_card_sync.yibuapi]
enabled = true
group = "default"
""".strip()
        + "\n",
        encoding="utf-8",
    )
    calls: list[list[str]] = []

    monkeypatch.setattr(
        "loom_cli.rollout.steps.s12_production_defaults._profile_path_for",
        lambda ctx: str(profile),
    )
    monkeypatch.setattr(
        "loom_cli.rollout.steps.s12_production_defaults.run_captured",
        lambda argv, **kwargs: calls.append(list(argv)),
    )

    result = ProductionDefaultsStep().run(ctx, step_dir)

    assert result.exit_code == 2
    assert "--service-token" in (result.error or "")
    assert "env:VAR or file:PATH" in (result.error or "")
    assert calls == []


def test_production_defaults_redacts_service_token_from_failed_evidence(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    token_file = tmp_path / "service-token"
    token_file.write_text("super-secret-service-token\n", encoding="utf-8")
    ctx = make_ctx(
        tmp_path,
        service_token_source=f"file:{token_file}",
    )
    ctx.cluster_config_path.write_text(
        """
ingress_host = "yylx.world"
frontend_route_path = "/dev"
frontend_api_base_path = "/dev"
""".lstrip(),
        encoding="utf-8",
    )
    ev = EvidenceDirectory(tmp_path, "test-rid")
    ev.ensure()
    _prepare_candidate_worktree(ev)
    step_dir = ev.step_dir(13, "production-defaults")
    profile = tmp_path / "staging.toml"
    profile.write_text(
        """
environment = "staging"

[rate_card_sync.yibuapi]
enabled = true
group = "default"
""".strip()
        + "\n",
        encoding="utf-8",
    )

    def fake_run(argv, **kwargs):
        return SubprocessResult(
            argv=list(argv),
            returncode=1,
            stdout="debug token super-secret-service-token\n",
            stderr=(
                "failed Authorization: Bearer super-secret-service-token "
                "super-secret-service-token\n"
            ),
        )

    monkeypatch.setattr(
        "loom_cli.rollout.steps.s12_production_defaults._profile_path_for",
        lambda ctx: str(profile),
    )
    monkeypatch.setattr(
        "loom_cli.rollout.steps.s12_production_defaults.run_captured",
        fake_run,
    )

    result = ProductionDefaultsStep().run(ctx, step_dir)

    assert result.exit_code == 1
    assert "super-secret-service-token" not in (result.error or "")
    assert "[REDACTED:service-token]" in (result.error or "")
    stdout = step_dir.stdout_path().read_text(encoding="utf-8")
    stderr = step_dir.stderr_path().read_text(encoding="utf-8")
    artifact = step_dir.artifact_path("rate-card-sync-yibuapi.json").read_text(
        encoding="utf-8",
    )
    assert "super-secret-service-token" not in stdout
    assert "super-secret-service-token" not in stderr
    assert "super-secret-service-token" not in artifact
    assert "[REDACTED:service-token]" in stdout
    assert "[REDACTED:service-token]" in stderr
    assert "[REDACTED:service-token]" in artifact


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
    ctx = make_ctx(tmp_path, image_tag="staging-new")
    ctx.cluster_config_path.write_text(
        'image_tag = "staging-old"\nnamespace = "loom-staging"\n',
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
    assert seen["argv"][seen["argv"].index("--backup-manifest") + 1] == (
        str(ctx.backup_manifest_path)
    )


def test_cluster_up_runs_loom_cli_from_candidate_worktree(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ctx = make_ctx(tmp_path)
    ev = EvidenceDirectory(tmp_path, "test-rid")
    ev.ensure()
    worktree = _prepare_candidate_worktree(ev)
    step_dir = ev.step_dir(10, "cluster-up")
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
    ctx = make_ctx(tmp_path, image_tag="staging-abc123")
    ev = EvidenceDirectory(tmp_path, "test-rid")
    ev.ensure()
    step_dir = ev.step_dir(14, "release-gate")

    argv = list(ReleaseGateStep().argv(ctx, step_dir))

    assert "--manifest" in argv
    manifest = Path(argv[argv.index("--manifest") + 1])
    assert manifest == step_dir.artifact_path("release-manifest-staging-abc123.json")


def test_release_gate_argv_passes_hf_mirror_boundary_evidence_for_staging(
    tmp_path: Path,
) -> None:
    ctx = make_ctx(tmp_path, image_tag="staging-abc123", environment="staging")
    ev = EvidenceDirectory(tmp_path, "test-rid")
    ev.ensure()
    step_dir = ev.step_dir(14, "release-gate")

    argv = list(ReleaseGateStep().argv(ctx, step_dir))

    assert "--hf-mirror-boundary-evidence" in argv
    evidence = Path(argv[argv.index("--hf-mirror-boundary-evidence") + 1])
    assert evidence == step_dir.artifact_path(
        "hf-mirror-boundary-evidence-staging-abc123.json",
    )


def test_rollout_cluster_commands_use_config_with_context_image_tag(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ctx = make_ctx(tmp_path, image_tag="staging-new")
    ctx.cluster_config_path.write_text(
        'image_tag = "staging-old"\nnamespace = "loom-staging"\n',
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
    cluster_up_argv = list(ClusterUpStep().argv(ctx, ev.step_dir(10, "cluster-up")))
    cluster_up_config = Path(cluster_up_argv[cluster_up_argv.index("--config") + 1])
    manifest_argv = list(
        ReleaseGateStep().release_manifest_argv(ctx, ev.step_dir(14, "release-gate")),
    )
    release_manifest_config = Path(manifest_argv[manifest_argv.index("--config") + 1])
    gate_argv = list(ReleaseGateStep().argv(ctx, ev.step_dir(14, "release-gate")))
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
    assert rendered_raw["image_tag"] == "staging-new"
    assert original_raw["image_tag"] == "staging-old"


def test_rollout_cluster_config_is_stable_after_first_synthesis(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ctx = make_ctx(tmp_path, image_tag="staging-new")
    ctx.cluster_config_path.write_text(
        'image_tag = "staging-old"\nnamespace = "loom-staging"\n',
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
        'image_tag = "staging-old"\nnamespace = "changed-after-render"\n',
    )

    cluster_up_argv = list(ClusterUpStep().argv(ctx, ev.step_dir(10, "cluster-up")))
    cluster_up_config = Path(cluster_up_argv[cluster_up_argv.index("--config") + 1])
    rendered_raw = tomllib.loads(cluster_up_config.read_text())

    assert cluster_up_config == rendered_config
    assert rendered_raw["image_tag"] == "staging-new"
    assert rendered_raw["namespace"] == "loom-staging"


def test_gb10_prep_reads_hosts_from_cluster_config(tmp_path: Path) -> None:
    ctx = make_ctx(tmp_path)
    ssh_config = tmp_path / "deploy" / "worker-pools" / "gb10" / "ssh_config"
    ssh_config.parent.mkdir(parents=True)
    ssh_config.write_text("Host trt-gb10-1\n  HostName 203.0.113.1\n")
    identity = _write_dummy_identity(tmp_path / "gb10-rollout-ed25519")
    cert = tmp_path / "gb10-rollout-ed25519-cert.pub"
    cert.write_text("ssh-ed25519-cert-v01@openssh.com dummy\n", encoding="utf-8")
    ctx.cluster_config_path.write_text(
        "[gb10_pool]\n"
        f'ssh_config = "{ssh_config}"\n'
        f'ssh_identity_file = "{identity}"\n'
        f'ssh_certificate_file = "{cert}"\n'
        "hosts = [\n"
        '  { ssh_target = "trt-gb10-1", repo_path = "/srv/loom", '
        'env_file_path = "/srv/loom/.env", '
        'repo_url = "https://github.com/qianyi-sun/loom.git", '
        'node_agent_service = "loom-gb10-node-agent.service" },\n'
        "]\n",
        encoding="utf-8",
    )

    hosts = gb10_hosts_for(ctx)

    assert len(hosts) == 1
    assert hosts[0].ssh_target == "trt-gb10-1"
    assert hosts[0].repo_path == "/srv/loom"
    assert hosts[0].env_file_path == "/srv/loom/.env"
    assert hosts[0].repo_url == "https://github.com/qianyi-sun/loom.git"
    assert hosts[0].node_agent_service == "loom-gb10-node-agent.service"
    assert hosts[0].ssh_config_path == str(ssh_config)
    assert hosts[0].ssh_identity_file == str(identity)
    assert hosts[0].ssh_certificate_file == str(cert)


def test_gb10_prep_ssh_uses_declared_config(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ssh_config = tmp_path / "gb10_ssh_config"
    ssh_config.write_text("Host trt-gb10-1\n  HostName 203.0.113.1\n")
    identity = _write_dummy_identity(tmp_path / "gb10-rollout-ed25519")
    cert = tmp_path / "gb10-rollout-ed25519-cert.pub"
    cert.write_text("ssh-ed25519-cert-v01@openssh.com dummy\n", encoding="utf-8")
    host = GB10Host(
        ssh_target="trt-gb10-1",
        repo_path="/srv/loom",
        env_file_path="/srv/loom/.env",
        ssh_config_path=str(ssh_config),
        ssh_identity_file=str(identity),
        ssh_certificate_file=str(cert),
    )
    captured: dict[str, Any] = {}

    def fake_run(argv):
        captured["argv"] = list(argv)
        return SubprocessResult(
            argv=list(argv),
            returncode=0,
            stdout="",
            stderr="",
        )

    monkeypatch.setattr("loom_cli.rollout.steps.s04_gb10_prep.run_captured", fake_run)

    _ssh(host, "hostname >/dev/null")

    argv = captured["argv"]
    assert argv[:3] == ["ssh", "-F", str(ssh_config)]
    assert "-i" in argv
    assert str(identity) in argv
    assert "IdentitiesOnly=yes" in argv
    assert f"CertificateFile={cert}" in argv
    assert "-o" in argv
    assert "BatchMode=yes" in argv
    assert argv[-2:] == ["trt-gb10-1", "hostname >/dev/null"]


def test_gb10_prep_fails_when_current_gb10_profile_has_no_hosts(
    tmp_path: Path,
) -> None:
    ctx = make_ctx(tmp_path, image_tag="staging-abc123")
    state_dir = tmp_path / "environment-state"
    state_dir.mkdir()
    (state_dir / "staging.toml").write_text(
        """
environment = "staging"

[[gb10_worker_pool_desired_states]]
pool_name = "gb10-arm64"
image_tag = "${IMAGE_TAG}"
max_concurrent = 1
env_config_version = "${ENV_CONFIG_VERSION}"
source_git_commit = "${GIT_SHA}"
target_slots = 1

[gb10_worker_pool_desired_states.host_intents]
trt-gb10-1 = "active"
""".strip()
        + "\n",
        encoding="utf-8",
    )
    ctx.cluster_config_path.write_text(
        'env_state_profile = "environment-state/staging.toml"\n',
        encoding="utf-8",
    )
    ev = EvidenceDirectory(tmp_path, "test-rid")
    ev.ensure()

    result = GB10PrepStep().run(ctx, ev.step_dir(12, "gb10-prep"))

    assert result.exit_code == 1
    assert result.error is not None
    assert "declares GB10 desired state" in result.error
    assert "no [gb10_pool] hosts" in result.error


def test_gb10_prep_requires_platform_dev_identity_file(
    tmp_path: Path,
) -> None:
    ctx = make_ctx(tmp_path, image_tag="staging-abc123")
    ctx.cluster_config_path.write_text(
        "[gb10_pool]\n"
        "hosts = [\n"
        '  { ssh_target = "trt-gb10-1", repo_path = "/srv/loom-staging", '
        'env_file_path = "/srv/loom-staging/.env" },\n'
        "]\n",
        encoding="utf-8",
    )
    ev = EvidenceDirectory(tmp_path, "test-rid")
    ev.ensure()
    step_dir = ev.step_dir(12, "gb10-prep")

    result = GB10PrepStep().run(ctx, step_dir)

    assert result.exit_code == 1
    assert result.error is not None
    assert "[gb10_pool].ssh_identity_file" in result.error
    assert "ssh-agent forwarding" in step_dir.stderr_path().read_text()


def test_gb10_prep_rejects_group_readable_identity_file(
    tmp_path: Path,
) -> None:
    ctx = make_ctx(tmp_path, image_tag="staging-abc123")
    identity = tmp_path / "gb10-rollout-ed25519"
    identity.write_text("not-a-real-key\n", encoding="utf-8")
    identity.chmod(0o640)
    ctx.cluster_config_path.write_text(
        "[gb10_pool]\n"
        f'ssh_identity_file = "{identity}"\n'
        "hosts = [\n"
        '  { ssh_target = "trt-gb10-1", repo_path = "/srv/loom-staging", '
        'env_file_path = "/srv/loom-staging/.env" },\n'
        "]\n",
        encoding="utf-8",
    )
    ev = EvidenceDirectory(tmp_path, "test-rid")
    ev.ensure()

    result = GB10PrepStep().run(ctx, ev.step_dir(12, "gb10-prep"))

    assert result.exit_code == 1
    assert result.error is not None
    assert "must not be group/world accessible" in result.error
    assert "mode=640" in result.error


def test_gb10_prep_verify_treats_missing_target_env_as_retryable_mismatch(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ctx = make_ctx(tmp_path, image_tag="staging-abc123")
    identity = _write_dummy_identity(tmp_path / "gb10-rollout-ed25519")
    ctx.cluster_config_path.write_text(
        "[gb10_pool]\n"
        f'ssh_identity_file = "{identity}"\n'
        "hosts = [\n"
        '  { ssh_target = "trt-gb10-1", repo_path = "/srv/loom-staging", '
        'env_file_path = "/srv/loom-staging/.env" },\n'
        "]\n",
        encoding="utf-8",
    )
    ev = EvidenceDirectory(tmp_path, "test-rid")
    ev.ensure()

    def fake_ssh(host, remote_cmd):
        return SubprocessResult(
            argv=["ssh", host.ssh_target, remote_cmd],
            returncode=1,
            stdout="",
            stderr="",
        )

    monkeypatch.setattr("loom_cli.rollout.steps.s04_gb10_prep._ssh", fake_ssh)

    outcome = GB10PrepStep().verify(ctx, ev.step_dir(12, "gb10-prep"))

    assert outcome is VerifyOutcome.MISMATCH


def test_gb10_prep_verify_keeps_ssh_auth_failure_unknown(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ctx = make_ctx(tmp_path, image_tag="staging-abc123")
    identity = _write_dummy_identity(tmp_path / "gb10-rollout-ed25519")
    ctx.cluster_config_path.write_text(
        "[gb10_pool]\n"
        f'ssh_identity_file = "{identity}"\n'
        "hosts = [\n"
        '  { ssh_target = "trt-gb10-1", repo_path = "/srv/loom-staging", '
        'env_file_path = "/srv/loom-staging/.env" },\n'
        "]\n",
        encoding="utf-8",
    )
    ev = EvidenceDirectory(tmp_path, "test-rid")
    ev.ensure()

    def fake_ssh(host, remote_cmd):
        return SubprocessResult(
            argv=["ssh", host.ssh_target, remote_cmd],
            returncode=255,
            stdout="",
            stderr="Permission denied (publickey).",
        )

    monkeypatch.setattr("loom_cli.rollout.steps.s04_gb10_prep._ssh", fake_ssh)

    outcome = GB10PrepStep().verify(ctx, ev.step_dir(12, "gb10-prep"))

    assert outcome is VerifyOutcome.UNKNOWN


def test_gb10_prep_verify_checks_checkout_env_version_and_node_agent(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ctx = make_ctx(
        tmp_path,
        image_tag="staging-abc123",
        resolved_sha="d" * 40,
    )
    identity = _write_dummy_identity(tmp_path / "gb10-rollout-ed25519")
    ctx.cluster_config_path.write_text(
        "[gb10_pool]\n"
        f'ssh_identity_file = "{identity}"\n'
        "hosts = [\n"
        '  { ssh_target = "trt-gb10-1", repo_path = "/srv/loom-staging", '
        'env_file_path = "/srv/loom-staging/.env", '
        'node_agent_service = "loom-gb10-node-agent.service" },\n'
        "]\n",
        encoding="utf-8",
    )
    ev = EvidenceDirectory(tmp_path, "test-rid")
    ev.ensure()
    calls: list[str] = []

    def fake_ssh(host, remote_cmd):
        calls.append(remote_cmd)
        if "systemctl --user show" in remote_cmd:
            return SubprocessResult(
                argv=["ssh", host.ssh_target, remote_cmd],
                returncode=0,
                stdout=(
                    "Type=oneshot\n"
                    "Result=failed\n"
                    "ExecMainStatus=1\n"
                    "ActiveState=failed\n"
                    "SubState=failed\n"
                ),
                stderr="",
            )
        return SubprocessResult(
            argv=["ssh", host.ssh_target, remote_cmd],
            returncode=0,
            stdout="",
            stderr="",
        )

    monkeypatch.setattr("loom_cli.rollout.steps.s04_gb10_prep._ssh", fake_ssh)

    outcome = GB10PrepStep().verify(ctx, ev.step_dir(12, "gb10-prep"))

    assert outcome is VerifyOutcome.MISMATCH
    assert calls == [
        'test "$(cd /srv/loom-staging && git rev-parse HEAD)" = dddddddddddddddddddddddddddddddddddddddd',
        "grep -q '^LOOM_IMAGE_TAG=staging-abc123$' /srv/loom-staging/.env",
        "grep -q '^LOOM_WORKER_ENV_CONFIG_VERSION=staging-abc123$' /srv/loom-staging/.env",
        "systemctl --user is-enabled loom-gb10-worker.service",
        "systemctl --user show loom-gb10-worker.service -p ActiveState -p SubState",
        "systemctl --user show loom-gb10-node-agent.service -p Type -p Result -p ExecMainStatus -p ActiveState -p SubState",
    ]


def test_gb10_prep_verify_accepts_successful_oneshot_node_agent(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ctx = make_ctx(
        tmp_path,
        image_tag="staging-abc123",
        resolved_sha="e" * 40,
    )
    identity = _write_dummy_identity(tmp_path / "gb10-rollout-ed25519")
    ctx.cluster_config_path.write_text(
        "[gb10_pool]\n"
        f'ssh_identity_file = "{identity}"\n'
        "hosts = [\n"
        '  { ssh_target = "trt-gb10-1", repo_path = "/srv/loom-staging", '
        'env_file_path = "/srv/loom-staging/.env", '
        'node_agent_service = "loom-gb10-node-agent.service" },\n'
        "]\n",
        encoding="utf-8",
    )
    ev = EvidenceDirectory(tmp_path, "test-rid")
    ev.ensure()
    calls: list[str] = []

    def fake_ssh(host, remote_cmd):
        calls.append(remote_cmd)
        if "systemctl --user show" in remote_cmd:
            return SubprocessResult(
                argv=["ssh", host.ssh_target, remote_cmd],
                returncode=0,
                stdout=(
                    "Type=oneshot\n"
                    "Result=success\n"
                    "ExecMainStatus=0\n"
                    "ActiveState=inactive\n"
                    "SubState=dead\n"
                ),
                stderr="",
            )
        return SubprocessResult(
            argv=["ssh", host.ssh_target, remote_cmd],
            returncode=3 if "systemctl --user is-active" in remote_cmd else 0,
            stdout="",
            stderr="",
        )

    monkeypatch.setattr("loom_cli.rollout.steps.s04_gb10_prep._ssh", fake_ssh)

    outcome = GB10PrepStep().verify(ctx, ev.step_dir(12, "gb10-prep"))

    assert outcome is VerifyOutcome.MATCH
    assert any("systemctl --user show" in call for call in calls)


def test_gb10_prep_verify_retries_when_legacy_worker_unit_is_enabled(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ctx = make_ctx(
        tmp_path,
        image_tag="staging-abc123",
        resolved_sha="e" * 40,
    )
    identity = _write_dummy_identity(tmp_path / "gb10-rollout-ed25519")
    ctx.cluster_config_path.write_text(
        "[gb10_pool]\n"
        f'ssh_identity_file = "{identity}"\n'
        "hosts = [\n"
        '  { ssh_target = "trt-gb10-1", repo_path = "/srv/loom-staging", '
        'env_file_path = "/srv/loom-staging/.env", '
        'node_agent_service = "loom-gb10-node-agent.service" },\n'
        "]\n",
        encoding="utf-8",
    )
    ev = EvidenceDirectory(tmp_path, "test-rid")
    ev.ensure()
    calls: list[str] = []

    def fake_ssh(host, remote_cmd):
        calls.append(remote_cmd)
        if "systemctl --user show loom-gb10-node-agent.service" in remote_cmd:
            return SubprocessResult(
                argv=["ssh", host.ssh_target, remote_cmd],
                returncode=0,
                stdout=(
                    "Type=oneshot\n"
                    "Result=success\n"
                    "ExecMainStatus=0\n"
                    "ActiveState=inactive\n"
                    "SubState=dead\n"
                ),
                stderr="",
            )
        if "systemctl --user is-enabled loom-gb10-worker.service" in remote_cmd:
            return SubprocessResult(
                argv=["ssh", host.ssh_target, remote_cmd],
                returncode=0,
                stdout="enabled\n",
                stderr="",
            )
        return SubprocessResult(
            argv=["ssh", host.ssh_target, remote_cmd],
            returncode=0,
            stdout="",
            stderr="",
        )

    monkeypatch.setattr("loom_cli.rollout.steps.s04_gb10_prep._ssh", fake_ssh)
    step_dir = ev.step_dir(12, "gb10-prep")

    outcome = GB10PrepStep().verify(ctx, step_dir)

    assert outcome is VerifyOutcome.MISMATCH
    assert "legacy" in step_dir.stderr_path().read_text()
    assert any("loom-gb10-worker.service" in call for call in calls)


def test_gb10_prep_clones_preserves_env_and_starts_node_agent(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ctx = make_ctx(
        tmp_path,
        image_tag="staging-abc123",
        resolved_sha="c" * 40,
    )
    identity = _write_dummy_identity(tmp_path / "gb10-rollout-ed25519")
    ctx.cluster_config_path.write_text(
        "[gb10_pool]\n"
        f'ssh_identity_file = "{identity}"\n'
        "hosts = [\n"
        '  { ssh_target = "trt-gb10-1", repo_path = "/srv/loom-staging", '
        'env_file_path = "/srv/loom-staging/.env", '
        'repo_url = "https://github.com/qianyi-sun/loom.git", '
        'node_agent_service = "loom-gb10-node-agent.service" },\n'
        "]\n",
        encoding="utf-8",
    )
    ev = EvidenceDirectory(tmp_path, "test-rid")
    ev.ensure()
    calls: list[str] = []

    def fake_ssh(host, remote_cmd):
        calls.append(remote_cmd)
        return SubprocessResult(
            argv=["ssh", host.ssh_target, remote_cmd],
            returncode=0,
            stdout="",
            stderr="",
        )

    monkeypatch.setattr("loom_cli.rollout.steps.s04_gb10_prep._ssh", fake_ssh)

    result = GB10PrepStep().run(ctx, ev.step_dir(12, "gb10-prep"))

    assert result.exit_code == 0
    assert any(
        "git clone --quiet https://github.com/qianyi-sun/loom.git /srv/loom-staging" in call
        for call in calls
    )
    env_update = next(call for call in calls if "LOOM_IMAGE_TAG" in call)
    assert "LOOM_IMAGE_TAG" in env_update
    assert "LOOM_WORKER_ENV_CONFIG_VERSION" in env_update
    assert "LOOM_WORKER_TOKEN" not in env_update
    assert "> /srv/loom-staging/.env" not in env_update
    legacy_idx = next(
        idx for idx, call in enumerate(calls) if "disable --now loom-gb10-worker.service" in call
    )
    node_agent_idx = calls.index("systemctl --user start loom-gb10-node-agent.service")
    assert legacy_idx < node_agent_idx
    assert calls[-1] == "systemctl --user start loom-gb10-node-agent.service"


def test_gb10_prep_runs_hosts_with_bounded_parallelism(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ctx = make_ctx(tmp_path, image_tag="staging-abc123")
    identity = _write_dummy_identity(tmp_path / "gb10-rollout-ed25519")
    ctx.cluster_config_path.write_text(
        "[gb10_pool]\n"
        f'ssh_identity_file = "{identity}"\n'
        "hosts = [\n"
        '  { ssh_target = "trt-gb10-1", repo_path = "/srv/loom-staging", env_file_path = "/srv/loom-staging/.env" },\n'
        '  { ssh_target = "trt-gb10-2", repo_path = "/srv/loom-staging", env_file_path = "/srv/loom-staging/.env" },\n'
        '  { ssh_target = "trt-gb10-3", repo_path = "/srv/loom-staging", env_file_path = "/srv/loom-staging/.env" },\n'
        "]\n",
        encoding="utf-8",
    )
    ev = EvidenceDirectory(tmp_path, "test-rid")
    ev.ensure()
    lock = threading.Lock()
    active = 0
    max_active = 0

    def fake_prep_one_host(ctx, host, host_dir):
        nonlocal active, max_active
        with lock:
            active += 1
            max_active = max(max_active, active)
        time.sleep(0.05)
        (host_dir / "prep.log").write_text(f"{host.ssh_target}\n", encoding="utf-8")
        with lock:
            active -= 1
        return True, f"prepped {host.ssh_target}"

    monkeypatch.setattr(
        "loom_cli.rollout.steps.s04_gb10_prep._prep_one_host",
        fake_prep_one_host,
    )

    step = GB10PrepStep()
    step.host_concurrency = 2
    result = step.run(ctx, ev.step_dir(12, "gb10-prep"))

    assert result.exit_code == 0
    assert max_active == 2
    stdout = ev.step_dir(12, "gb10-prep").stdout_path().read_text()
    assert "started=3 succeeded=3 failed=0 retried=0 concurrency=2" in stdout


def test_gb10_prep_slow_failing_host_does_not_block_other_hosts(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ctx = make_ctx(tmp_path, image_tag="staging-abc123")
    identity = _write_dummy_identity(tmp_path / "gb10-rollout-ed25519")
    ctx.cluster_config_path.write_text(
        "[gb10_pool]\n"
        f'ssh_identity_file = "{identity}"\n'
        "hosts = [\n"
        '  { ssh_target = "trt-gb10-1", repo_path = "/srv/loom-staging", env_file_path = "/srv/loom-staging/.env" },\n'
        '  { ssh_target = "trt-gb10-2", repo_path = "/srv/loom-staging", env_file_path = "/srv/loom-staging/.env" },\n'
        '  { ssh_target = "trt-gb10-3", repo_path = "/srv/loom-staging", env_file_path = "/srv/loom-staging/.env" },\n'
        "]\n",
        encoding="utf-8",
    )
    ev = EvidenceDirectory(tmp_path, "test-rid")
    ev.ensure()
    host1_active = False
    progressed_while_host1_active: set[str] = set()
    lock = threading.Lock()

    def fake_prep_one_host(ctx, host, host_dir):
        nonlocal host1_active
        if host.ssh_target == "trt-gb10-1":
            with lock:
                host1_active = True
            time.sleep(0.08)
            (host_dir / "prep.log").write_text("failed\n", encoding="utf-8")
            with lock:
                host1_active = False
            return False, "checkout failed on trt-gb10-1: rc=1"
        with lock:
            if host1_active:
                progressed_while_host1_active.add(host.ssh_target)
        (host_dir / "prep.log").write_text("ok\n", encoding="utf-8")
        return True, f"prepped {host.ssh_target}"

    monkeypatch.setattr(
        "loom_cli.rollout.steps.s04_gb10_prep._prep_one_host",
        fake_prep_one_host,
    )

    step = GB10PrepStep()
    step.host_concurrency = 2
    step.max_retries = 1
    result = step.run(ctx, ev.step_dir(12, "gb10-prep"))

    assert result.exit_code == 1
    assert "trt-gb10-1" in str(result.error)
    assert progressed_while_host1_active
    assert (ev.step_dir(12, "gb10-prep").path / "host-trt-gb10-2" / "prep.log").is_file()
    assert (ev.step_dir(12, "gb10-prep").path / "host-trt-gb10-3" / "prep.log").is_file()
    stdout = ev.step_dir(12, "gb10-prep").stdout_path().read_text()
    assert "started=3 succeeded=2 failed=1 retried=0 concurrency=2" in stdout


def test_gb10_prep_retry_count_is_per_host_and_reported(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ctx = make_ctx(tmp_path, image_tag="staging-abc123")
    identity = _write_dummy_identity(tmp_path / "gb10-rollout-ed25519")
    ctx.cluster_config_path.write_text(
        "[gb10_pool]\n"
        f'ssh_identity_file = "{identity}"\n'
        "hosts = [\n"
        '  { ssh_target = "trt-gb10-1", repo_path = "/srv/loom-staging", env_file_path = "/srv/loom-staging/.env" },\n'
        '  { ssh_target = "trt-gb10-2", repo_path = "/srv/loom-staging", env_file_path = "/srv/loom-staging/.env" },\n'
        "]\n",
        encoding="utf-8",
    )
    ev = EvidenceDirectory(tmp_path, "test-rid")
    ev.ensure()
    attempts: dict[str, int] = {}

    def fake_prep_one_host(ctx, host, host_dir):
        attempts[host.ssh_target] = attempts.get(host.ssh_target, 0) + 1
        if host.ssh_target == "trt-gb10-1" and attempts[host.ssh_target] == 1:
            return False, "fetch failed on trt-gb10-1: rc=128"
        (host_dir / "prep.log").write_text("ok\n", encoding="utf-8")
        return True, f"prepped {host.ssh_target}"

    monkeypatch.setattr(
        "loom_cli.rollout.steps.s04_gb10_prep._prep_one_host",
        fake_prep_one_host,
    )

    step = GB10PrepStep()
    step.host_concurrency = 2
    step.backoff_sec = 0
    result = step.run(ctx, ev.step_dir(12, "gb10-prep"))

    assert result.exit_code == 0
    assert attempts == {"trt-gb10-1": 2, "trt-gb10-2": 1}
    stdout = ev.step_dir(12, "gb10-prep").stdout_path().read_text()
    assert "started=2 succeeded=2 failed=0 retried=1 concurrency=2" in stdout


def test_release_gate_run_generates_manifest_then_gates(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ctx = make_ctx(tmp_path, image_tag="staging-abc123")
    profile = tmp_path / "deploy" / "environment-state" / "staging.toml"
    profile.parent.mkdir(parents=True)
    profile.write_text('environment = "staging"\n', encoding="utf-8")
    ctx.cluster_config_path.write_text(
        f'env_state_profile = "{profile}"\n',
        encoding="utf-8",
    )
    ev = EvidenceDirectory(tmp_path, "test-rid")
    ev.ensure()
    worktree = _prepare_candidate_worktree(ev)
    _write_rendered_service(ev)
    step_dir = ev.step_dir(14, "release-gate")
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
        if "minio-storage-preflight" in argv:
            output = Path(argv[argv.index("--output") + 1])
            output.write_text(
                json.dumps(
                    {
                        "outcome": "pass",
                        "filesystem": {"free_percent": 42.0},
                        "thresholds": {"stop_free_percent": 15.0},
                        "checks": [],
                    }
                )
                + "\n",
                encoding="utf-8",
            )
        if "release-manifest" in argv:
            output = Path(argv[argv.index("--output") + 1])
            output.write_text(_release_manifest_with_gb10_contract())
        if argv[3:6] == ["admin", "environment-state", "check"]:
            stdout = json.dumps(
                {
                    "environment": "production",
                    "control_plane_environment": "production",
                    "ok": True,
                    "drift": [],
                    "autoscaler_blockers": [],
                }
            )
            if kwargs.get("stdout_log"):
                kwargs["stdout_log"].write_text(stdout)
            if kwargs.get("stderr_log"):
                kwargs["stderr_log"].write_text("")
            return SubprocessResult(
                argv=list(argv),
                returncode=0,
                stdout=stdout,
                stderr="",
            )
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
    assert len(calls) == 7
    assert calls[0]["argv"][:3] == ["docker", "image", "inspect"]
    for call in calls[1:]:
        _assert_candidate_invocation(call, worktree=worktree)
    assert calls[1]["argv"][3:5] == ["cluster", "release-manifest"]
    assert calls[2]["argv"][3:5] == ["cluster", "minio-storage-preflight"]
    assert calls[2]["argv"][calls[2]["argv"].index("--namespace") + 1] == ctx.namespace
    assert calls[3]["argv"][3:6] == ["admin", "gb10-workers", "status"]
    assert calls[3]["argv"][calls[3]["argv"].index("--environment") + 1] == "production"
    assert calls[4]["argv"][3:6] == ["admin", "environment-state", "check"]
    assert calls[5]["argv"][3:5] == ["datasets", "hf-boundary-evidence"]
    assert calls[6]["argv"][3:5] == ["cluster", "release-gate"]
    manifest = step_dir.artifact_path("release-manifest-staging-abc123.json")
    storage = step_dir.artifact_path("minio-storage-preflight-staging-abc123.json")
    env_state_check = step_dir.artifact_path("environment-state-check.json")
    hf_boundary = step_dir.artifact_path(
        "hf-mirror-boundary-evidence-staging-abc123.json",
    )
    assert calls[1]["argv"][calls[1]["argv"].index("--output") + 1] == str(manifest)
    assert calls[2]["argv"][calls[2]["argv"].index("--output") + 1] == str(storage)
    assert calls[5]["argv"][calls[5]["argv"].index("--environment") + 1] == ctx.environment
    assert calls[5]["argv"][calls[5]["argv"].index("--namespace") + 1] == ctx.namespace
    assert calls[5]["argv"][calls[5]["argv"].index("--gb10-workers-status") + 1] == (
        str(step_dir.artifact_path("gb10-workers-status-staging-abc123.json"))
    )
    assert calls[5]["argv"][calls[5]["argv"].index("--output") + 1] == str(hf_boundary)
    assert calls[6]["argv"][calls[6]["argv"].index("--manifest") + 1] == str(manifest)
    assert calls[6]["argv"][calls[6]["argv"].index("--minio-storage-preflight") + 1] == str(storage)
    assert calls[6]["argv"][calls[6]["argv"].index("--environment-state-check") + 1] == (
        str(env_state_check)
    )
    assert "--gb10-workers-status" in calls[6]["argv"]


def test_release_gate_records_expected_image_identities_before_manifest(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ctx = make_ctx(tmp_path, image_tag="staging-abc123")
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
          image: loom-service:staging-abc123
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
    step_dir = ev.step_dir(14, "release-gate")

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
                "image-identities-staging-abc123.json",
            )
            body = json.loads(identities_path.read_text(encoding="utf-8"))
            assert body == {
                "loom-service": {
                    "loom-service": {
                        "image": "loom-service:staging-abc123",
                        "image_id": "sha256:" + "1" * 64,
                        "repo_digest": "loom-service@sha256:" + "2" * 64,
                    },
                },
            }
            assert "do-not-write" not in identities_path.read_text(encoding="utf-8")
            output = Path(argv[argv.index("--output") + 1])
            output.write_text(_release_manifest_with_gb10_contract())
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


def test_release_gate_run_fails_fast_when_gb10_status_hard_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ctx = make_ctx(tmp_path, image_tag="staging-abc123")
    ev = EvidenceDirectory(tmp_path, "test-rid")
    ev.ensure()
    _prepare_candidate_worktree(ev)
    _write_rendered_service(ev)
    step_dir = ev.step_dir(14, "release-gate")
    calls: list[list[str]] = []

    def fake_run(argv, **kwargs):
        calls.append(list(argv))
        if list(argv[:3]) == ["docker", "image", "inspect"]:
            return _docker_inspect_success(list(argv))
        if "release-manifest" in argv:
            output = Path(argv[argv.index("--output") + 1])
            output.write_text(_release_manifest_with_gb10_contract())
            return SubprocessResult(
                argv=list(argv),
                returncode=0,
                stdout="manifest\n",
                stderr="",
            )
        if "minio-storage-preflight" in argv:
            output = Path(argv[argv.index("--output") + 1])
            output.write_text('{"outcome":"pass","checks":[]}\n', encoding="utf-8")
            return SubprocessResult(
                argv=list(argv),
                returncode=0,
                stdout='{"outcome":"pass","checks":[]}\n',
                stderr="",
            )
        if "gb10-workers" in argv:
            if kwargs.get("stderr_log"):
                kwargs["stderr_log"].write_text("error: CP returned 500: internal error\n")
            return SubprocessResult(
                argv=list(argv),
                returncode=1,
                stdout="{}",
                stderr="error: CP returned 500: internal error\n",
            )
        raise AssertionError("release-gate should not run after GB10 status failure")

    monkeypatch.setattr("loom_cli.rollout.steps.s12_release_gate.run_captured", fake_run)

    result = ReleaseGateStep().run(ctx, step_dir)

    assert result.exit_code == 1
    assert result.error == "error: CP returned 500: internal error"
    non_docker_calls = [call for call in calls if call[:3] != ["docker", "image", "inspect"]]
    assert [call[3:6] for call in non_docker_calls] == [
        ["cluster", "release-manifest", "--config"],
        ["cluster", "minio-storage-preflight", "--namespace"],
        ["admin", "gb10-workers", "status"],
    ]


def test_release_gate_current_gb10_requires_manifest_desired_state(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ctx = make_ctx(tmp_path, image_tag="staging-abc123", scope="current-gb10")
    ev = EvidenceDirectory(tmp_path, "test-rid")
    ev.ensure()
    _prepare_candidate_worktree(ev)
    _write_rendered_service(ev)
    step_dir = ev.step_dir(14, "release-gate")
    calls: list[list[str]] = []

    def fake_run(argv, **kwargs):
        calls.append(list(argv))
        if list(argv[:3]) == ["docker", "image", "inspect"]:
            return _docker_inspect_success(list(argv))
        if "release-manifest" in argv:
            output = Path(argv[argv.index("--output") + 1])
            output.write_text(
                json.dumps(
                    {
                        "schema_version": 1,
                        "external_workers": {
                            "environment_state_file": None,
                            "control_plane_environment": None,
                            "slurm_pools": [],
                            "gb10_desired_states": [],
                        },
                    }
                )
                + "\n"
            )
            return SubprocessResult(
                argv=list(argv),
                returncode=0,
                stdout="manifest\n",
                stderr="",
            )
        raise AssertionError("GB10 status should not run without a desired state")

    monkeypatch.setattr("loom_cli.rollout.steps.s12_release_gate.run_captured", fake_run)

    result = ReleaseGateStep().run(ctx, step_dir)

    assert result.exit_code == 2
    assert result.summary == "release manifest lacks GB10 desired state"
    assert "env_state_profile" in (result.error or "")
    non_docker_calls = [call for call in calls if call[:3] != ["docker", "image", "inspect"]]
    assert [call[3:6] for call in non_docker_calls] == [
        ["cluster", "release-manifest", "--config"],
    ]


def test_release_gate_retry_classifier_ignores_passing_gb10_check() -> None:
    result = SubprocessResult(
        argv=["loom", "cluster", "release-gate"],
        returncode=1,
        stdout=(
            "CHECK                                      OUTCOME  DETAIL\n"
            "gb10-worker-convergence                    pass     "
            "GB10 worker status matches release target\n"
            "hf-mirror-token-boundary                   fail     "
            "HF mirror/token boundary evidence artifact is unreadable\n"
        ),
        stderr="",
    )

    assert not _is_gb10_convergence_failure(result)


def test_release_gate_retry_classifier_accepts_failing_gb10_check() -> None:
    result = SubprocessResult(
        argv=["loom", "cluster", "release-gate"],
        returncode=1,
        stdout=(
            "CHECK                                      OUTCOME  DETAIL\n"
            "gb10-worker-convergence                    fail     "
            "GB10 worker status reports release-target drift\n"
        ),
        stderr="",
    )

    assert _is_gb10_convergence_failure(result)


def test_release_gate_retries_transient_gb10_status_cp_unreachable(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ctx = make_ctx(tmp_path, image_tag="staging-abc123")
    ev = EvidenceDirectory(tmp_path, "test-rid")
    ev.ensure()
    _prepare_candidate_worktree(ev)
    _write_rendered_service(ev)
    step_dir = ev.step_dir(14, "release-gate")
    calls: list[list[str]] = []
    gb10_attempts = 0

    def fake_run(argv, **kwargs):
        nonlocal gb10_attempts
        calls.append(list(argv))
        if list(argv[:3]) == ["docker", "image", "inspect"]:
            return _docker_inspect_success(list(argv))
        if "release-manifest" in argv:
            output = Path(argv[argv.index("--output") + 1])
            output.write_text(_release_manifest_with_gb10_contract())
            return SubprocessResult(
                argv=list(argv),
                returncode=0,
                stdout="manifest\n",
                stderr="",
            )
        if "minio-storage-preflight" in argv:
            output = Path(argv[argv.index("--output") + 1])
            output.write_text('{"outcome":"pass","checks":[]}\n', encoding="utf-8")
            return SubprocessResult(
                argv=list(argv),
                returncode=0,
                stdout='{"outcome":"pass","checks":[]}\n',
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
        if "hf-boundary-evidence" in argv:
            output = Path(argv[argv.index("--output") + 1])
            output.write_text('{"environment":"staging"}\n', encoding="utf-8")
            return SubprocessResult(
                argv=list(argv),
                returncode=0,
                stdout="wrote HF boundary evidence\n",
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
    non_docker_calls = [call for call in calls if call[:3] != ["docker", "image", "inspect"]]
    assert [call[3:6] for call in non_docker_calls] == [
        ["cluster", "release-manifest", "--config"],
        ["cluster", "minio-storage-preflight", "--namespace"],
        ["admin", "gb10-workers", "status"],
        ["admin", "gb10-workers", "status"],
        ["admin", "gb10-workers", "status"],
        ["datasets", "hf-boundary-evidence", "skilllearnbench"],
        ["cluster", "release-gate", "--manifest"],
    ]


def test_release_gate_retries_gb10_status_release_target_mismatch(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ctx = make_ctx(tmp_path, image_tag="staging-abc123")
    ev = EvidenceDirectory(tmp_path, "test-rid")
    ev.ensure()
    _prepare_candidate_worktree(ev)
    _write_rendered_service(ev)
    step_dir = ev.step_dir(14, "release-gate")
    calls: list[list[str]] = []
    gb10_attempts = 0

    def fake_run(argv, **kwargs):
        nonlocal gb10_attempts
        calls.append(list(argv))
        if list(argv[:3]) == ["docker", "image", "inspect"]:
            return _docker_inspect_success(list(argv))
        if "release-manifest" in argv:
            output = Path(argv[argv.index("--output") + 1])
            output.write_text(_release_manifest_with_gb10_contract())
            return SubprocessResult(
                argv=list(argv),
                returncode=0,
                stdout="manifest\n",
                stderr="",
            )
        if "minio-storage-preflight" in argv:
            output = Path(argv[argv.index("--output") + 1])
            output.write_text('{"outcome":"pass","checks":[]}\n', encoding="utf-8")
            return SubprocessResult(
                argv=list(argv),
                returncode=0,
                stdout='{"outcome":"pass","checks":[]}\n',
                stderr="",
            )
        if "gb10-workers" in argv:
            gb10_attempts += 1
            if gb10_attempts == 1:
                if kwargs.get("stdout_log"):
                    kwargs["stdout_log"].write_text(
                        '{"nodes":[{"hostname":"trt-gb10-9"}],"desired_states":[]}\n',
                    )
                if kwargs.get("stderr_log"):
                    kwargs["stderr_log"].write_text(
                        "GB10 rollout target mismatch:\n"
                        "  node trt-gb10-9 missing active/fresh docker worker "
                        "registration worker_fresh=False\n",
                    )
                return SubprocessResult(
                    argv=list(argv),
                    returncode=1,
                    stdout='{"nodes":[{"hostname":"trt-gb10-9"}],"desired_states":[]}\n',
                    stderr=(
                        "GB10 rollout target mismatch:\n"
                        "  node trt-gb10-9 missing active/fresh docker worker "
                        "registration worker_fresh=False\n"
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
        if "hf-boundary-evidence" in argv:
            output = Path(argv[argv.index("--output") + 1])
            output.write_text('{"environment":"staging"}\n', encoding="utf-8")
            return SubprocessResult(
                argv=list(argv),
                returncode=0,
                stdout="wrote HF boundary evidence\n",
                stderr="",
            )
        if "release-gate" in argv:
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
    assert gb10_attempts == 2
    non_docker_calls = [call for call in calls if call[:3] != ["docker", "image", "inspect"]]
    assert [call[3:6] for call in non_docker_calls] == [
        ["cluster", "release-manifest", "--config"],
        ["cluster", "minio-storage-preflight", "--namespace"],
        ["admin", "gb10-workers", "status"],
        ["admin", "gb10-workers", "status"],
        ["datasets", "hf-boundary-evidence", "skilllearnbench"],
        ["cluster", "release-gate", "--manifest"],
    ]


def test_release_gate_retries_gb10_convergence_until_node_agent_reports_current(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ctx = make_ctx(tmp_path, image_tag="staging-53897aa")
    ev = EvidenceDirectory(tmp_path, "test-rid")
    ev.ensure()
    _prepare_candidate_worktree(ev)
    _write_rendered_service(ev, image_tag="staging-53897aa")
    step_dir = ev.step_dir(14, "release-gate")
    status_attempts = 0
    gate_attempts = 0

    def fake_run(argv, **kwargs):
        nonlocal status_attempts, gate_attempts
        if list(argv[:3]) == ["docker", "image", "inspect"]:
            return _docker_inspect_success(list(argv))
        if "release-manifest" in argv:
            output = Path(argv[argv.index("--output") + 1])
            output.write_text(_release_manifest_with_gb10_contract())
            return SubprocessResult(
                argv=list(argv),
                returncode=0,
                stdout="manifest\n",
                stderr="",
            )
        if "minio-storage-preflight" in argv:
            output = Path(argv[argv.index("--output") + 1])
            output.write_text('{"outcome":"pass","checks":[]}\n', encoding="utf-8")
            return SubprocessResult(
                argv=list(argv),
                returncode=0,
                stdout='{"outcome":"pass","checks":[]}\n',
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
        if "hf-boundary-evidence" in argv:
            output = Path(argv[argv.index("--output") + 1])
            output.write_text('{"environment":"staging"}\n', encoding="utf-8")
            return SubprocessResult(
                argv=list(argv),
                returncode=0,
                stdout="wrote HF boundary evidence\n",
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
