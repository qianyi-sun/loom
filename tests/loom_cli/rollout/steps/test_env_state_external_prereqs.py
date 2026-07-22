"""Env-state external Slurm prerequisite materialization tests (#562)."""

from __future__ import annotations

import json
import os
import shutil
import stat
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from loom_cli.rollout.base_context_fixture import make_ctx
from loom_cli.rollout.context import RolloutContext
from loom_cli.rollout.evidence import EvidenceDirectory, StepDir
from loom_cli.rollout.operator.preflight import ACTIVE_GB10_HOSTS
from loom_cli.rollout.steps import s10_env_state as env_state_module
from loom_cli.rollout.steps.s04_gb10_prep import GB10Host
from loom_cli.rollout.steps.s10_env_state import (
    ControlPlaneReadinessError,
    EnvStateStep,
    ExternalSlurmPrereqMaterializationError,
    _control_plane_health_url,
    _materialize_external_slurm_runner_prerequisites,
    _materialize_repo_dir,
    _verify_external_slurm_runner_consumers,
    _wait_for_control_plane,
)
from loom_cli.rollout.steps.subprocess_util import SubprocessResult


@pytest.fixture(autouse=True)
def _exclusive_rename_backend(monkeypatch: pytest.MonkeyPatch) -> None:
    if sys.platform.startswith("linux"):
        return

    def rename_noreplace(
        source_fd: int,
        source_name: str,
        destination_fd: int,
        destination_name: str,
    ) -> None:
        try:
            os.stat(destination_name, dir_fd=destination_fd, follow_symlinks=False)
        except FileNotFoundError:
            pass
        else:
            raise ExternalSlurmPrereqMaterializationError(
                "external runner repository destination appeared during materialization",
            )
        os.rename(
            source_name,
            destination_name,
            src_dir_fd=source_fd,
            dst_dir_fd=destination_fd,
        )

    monkeypatch.setattr(
        env_state_module,
        "_TEST_RENAME_NOREPLACE_BACKEND",
        rename_noreplace,
    )


def _write_external_prereq_profile(
    tmp_path: Path,
    *,
    env_file: Path,
    repo_dir: Path,
    template_glob: str,
) -> Path:
    profile = tmp_path / "staging.toml"
    profile.write_text(
        f"""
environment = "staging"

[[worker_pool_autoscaler_policies]]
pool_name = "gb10"
actuator = "slurm"
enabled = true
max_slots = 150

[worker_pool_autoscaler_policies.actuator_config]
external_runner = true
env_file = "{env_file}"
repo_dir = "{repo_dir}"
requested_concurrency = 10

[external_slurm_runner_prerequisites]
pools = ["gb10"]
expected_repo_ref = "${{IMAGE_TAG}}"
require_clean_repo = true
require_worker_token_parity = true
materialize = true
env_template_glob = "{template_glob}"
""",
        encoding="utf-8",
    )
    return profile


def _complete_worker_env_text() -> str:
    return "\n".join(
        [
            "LOOM_WORKER_CONTROL_PLANE_URL=http://control.example:8080",
            "LOOM_WORKER_GATEWAY_URL=http://control.example:9100",
            "LOOM_WORKER_TOKEN=old-worker-token",
            "LOOM_WORKER_MINIO_ENDPOINT=http://control.example:9000",
            "LOOM_WORKER_MINIO_ACCESS_KEY=keep-access",
            "LOOM_WORKER_MINIO_SECRET_KEY=keep-secret",
            "",
        ]
    )


def _consumer_verification_fixture(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[RolloutContext, StepDir, list[dict[str, Any]], list[tuple[str, str, str]]]:
    ctx = make_ctx(tmp_path, resolved_sha="a" * 40, scope="current-gb10")
    evidence = EvidenceDirectory(tmp_path, "test-rid")
    evidence.ensure()
    step_dir = evidence.step_dir(11, "env-state")
    candidate = step_dir.path.parent / "01-worktree" / "src"
    marker = candidate / "src" / "loom_cli" / "__main__.py"
    marker.parent.mkdir(parents=True)
    marker.write_text("# candidate marker\n", encoding="utf-8")
    verifier = candidate / "scripts" / "ops" / "staging_rollout_shared_repo_consumer.py"
    verifier.parent.mkdir(parents=True)
    verifier.write_text("# trusted verifier bytes\n", encoding="utf-8")
    verifier.chmod(0o640)
    shared_root = tmp_path / "worker-repos"
    target = shared_root / "loom-remote-worker-test"
    malicious = target / "scripts" / "ops" / "staging_rollout_shared_repo_consumer.py"
    malicious.parent.mkdir(parents=True)
    malicious.write_text("print('forged')\n", encoding="utf-8")
    shared_gid = os.getegid()
    monkeypatch.setattr(env_state_module, "_SHARED_WORKER_REPO_ROOT", shared_root)
    monkeypatch.setattr(
        env_state_module.pwd,
        "getpwnam",
        lambda _name: SimpleNamespace(pw_name="qianyi", pw_uid=os.geteuid(), pw_gid=os.getegid()),
    )
    monkeypatch.setattr(
        env_state_module.grp,
        "getgrnam",
        lambda _name: SimpleNamespace(gr_gid=shared_gid),
    )
    monkeypatch.setattr(
        env_state_module.os,
        "getgrouplist",
        lambda _name, primary: [primary, shared_gid],
    )
    hosts = [
        GB10Host(
            ssh_target=host,
            repo_path="/unused",
            env_file_path="/unused/.env",
            ssh_config_path="/candidate/ssh_config",
            ssh_identity_file="/etc/loom/gb10-key",
        )
        for host in ACTIVE_GB10_HOSTS
    ]
    from loom_cli.rollout.steps import s04_gb10_prep

    monkeypatch.setattr(
        s04_gb10_prep,
        "_gb10_prep_config_paths",
        lambda _ctx, _step: (candidate / "cluster.toml", candidate / "materialized.toml"),
    )
    monkeypatch.setattr(
        s04_gb10_prep,
        "gb10_hosts_for",
        lambda _ctx, *, config_path: hosts,
    )
    calls: list[tuple[str, str, str]] = []

    def ssh(host: GB10Host, command: str, *, stdin_text: str | None = None) -> SubprocessResult:
        assert stdin_text is not None
        calls.append((host.ssh_target, command, stdin_text))
        position = hosts.index(host) + 1
        payload = {
            "head": ctx.resolved_sha,
            "index_sha256": "b" * 64,
            "probe_file_sha256": "c" * 64,
            "root_device": position,
            "root_inode": 100 + position,
            "target_device": 200 + position,
            "target_inode": 300 + position,
            "tree_content_sha256": "e" * 64,
            "tracked_entries": 10,
        }
        return SubprocessResult([], 0, json.dumps(payload) + "\n", "")

    monkeypatch.setattr(s04_gb10_prep, "_ssh", ssh)
    records = [
        {
            "repo_dir": str(target),
            "repo_head": ctx.resolved_sha,
            "repo_group_id": shared_gid,
        }
    ]
    return ctx, step_dir, records, calls


def test_post_publish_consumer_verification_streams_trusted_verifier_to_exact_14_hosts(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ctx, step_dir, records, calls = _consumer_verification_fixture(tmp_path, monkeypatch)

    evidence = _verify_external_slurm_runner_consumers(ctx, step_dir, records)

    assert evidence is not None
    assert evidence["passed"] is True
    assert evidence["host_count"] == 14
    assert evidence["expected_host_count"] == 14
    assert evidence["tracked_entries"] == 10
    assert evidence["tree_content_sha256"] == "e" * 64
    assert len(str(evidence["verifier_sha256"])) == 64
    assert len(calls) == 14
    assert [host for host, _, _ in calls] == list(ACTIVE_GB10_HOSTS)
    assert all(command.startswith("/usr/bin/python3 - --root ") for _, command, _ in calls)
    assert all(stdin_text == "# trusted verifier bytes\n" for _, _, stdin_text in calls)
    assert all("forged" not in stdin_text for _, _, stdin_text in calls)
    persisted = json.loads(
        step_dir.artifact_path("external-slurm-runner-consumer-verification.json").read_text(
            encoding="utf-8"
        )
    )
    assert persisted == evidence
    assert len({node["root_device"] for node in persisted["nodes"]}) == 14


def test_post_publish_consumer_verification_rejects_divergent_host_content_identity(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ctx, step_dir, records, _calls = _consumer_verification_fixture(tmp_path, monkeypatch)
    from loom_cli.rollout.steps import s04_gb10_prep

    original_ssh = s04_gb10_prep._ssh

    def divergent(
        host: GB10Host,
        command: str,
        *,
        stdin_text: str | None = None,
    ) -> SubprocessResult:
        result = original_ssh(host, command, stdin_text=stdin_text)
        if host.ssh_target == "trt-gb10-15":
            payload = json.loads(result.stdout)
            payload["index_sha256"] = "d" * 64
            return SubprocessResult([], 0, json.dumps(payload) + "\n", "")
        return result

    monkeypatch.setattr(s04_gb10_prep, "_ssh", divergent)

    with pytest.raises(
        ExternalSlurmPrereqMaterializationError,
        match="failed safely",
    ):
        _verify_external_slurm_runner_consumers(ctx, step_dir, records)

    persisted = json.loads(
        step_dir.artifact_path("external-slurm-runner-consumer-verification.json").read_text(
            encoding="utf-8"
        )
    )
    assert persisted["passed"] is False
    assert persisted["host_count"] == 13


def test_materializes_external_slurm_env_file_without_secret_evidence(
    tmp_path: Path,
    monkeypatch,
) -> None:
    worker_token = "sensitive-worker-token"
    monkeypatch.setenv("TEST_WORKER_TOKEN", worker_token)
    template = tmp_path / "capacity" / "staging-gb10-worker-staging-old.env"
    template.parent.mkdir()
    template.write_text(
        "\n".join(
            [
                "LOOM_WORKER_CONTROL_PLANE_URL=http://control.example:8080",
                "LOOM_WORKER_GATEWAY_URL=http://control.example:9100",
                "LOOM_IMAGE_TAG=staging-old",
                "LOOM_WORKER_ENV_CONFIG_VERSION=staging-old",
                "LOOM_WORKER_TOKEN=old-worker-token$literal",
                "LOOM_WORKER_MINIO_ENDPOINT=http://control.example:9000",
                "LOOM_WORKER_MINIO_ACCESS_KEY=keep-access",
                "LOOM_WORKER_MINIO_SECRET_KEY=keep-secret",
                'LOOM_WORKER_SUBPROCESS_GATEWAY_URL=""',
                "LOOM_WORKER_MAX_CONCURRENT=1",
                "LOOM_WORKER_POOL_NAME=old-pool",
                "",
            ]
        ),
        encoding="utf-8",
    )
    template.chmod(0o600)
    target_env = template.parent / "staging-gb10-worker-staging-abc123.env"
    target_repo = tmp_path / "repo" / "loom-remote-worker-staging-abc123"
    profile = _write_external_prereq_profile(
        tmp_path,
        env_file=target_env,
        repo_dir=target_repo,
        template_glob=str(template.parent / "staging-gb10-worker-staging-*.env"),
    )
    ctx = make_ctx(
        tmp_path,
        image_tag="staging-abc123",
        worker_token_source="env:TEST_WORKER_TOKEN",
    )
    ev = EvidenceDirectory(tmp_path, "test-rid")
    ev.ensure()
    step_dir = ev.step_dir(11, "env-state")

    def fake_repo_materializer(*, repo_dir: Path, **_: Any) -> dict[str, Any]:
        repo_dir.mkdir(parents=True, exist_ok=True)
        return {
            "repo_dir": str(repo_dir),
            "repo_action": "created",
            "repo_head": ctx.resolved_sha,
            "repo_status": "clean",
        }

    monkeypatch.setattr(
        "loom_cli.rollout.steps.s10_env_state._materialize_repo_dir",
        fake_repo_materializer,
    )

    records = _materialize_external_slurm_runner_prerequisites(
        ctx,
        profile,
        step_dir,
    )
    first_content = target_env.read_text(encoding="utf-8")
    second_records = _materialize_external_slurm_runner_prerequisites(
        ctx,
        profile,
        step_dir,
    )

    assert len(records) == 1
    assert len(second_records) == 1
    assert target_env.exists()
    assert stat.S_IMODE(target_env.stat().st_mode) == 0o600
    assert target_env.read_text(encoding="utf-8") == first_content
    assert "LOOM_IMAGE_TAG=staging-abc123" in first_content
    assert "LOOM_WORKER_ENV_CONFIG_VERSION=staging-abc123" in first_content
    assert f"LOOM_WORKER_TOKEN={worker_token}" in first_content
    assert "LOOM_WORKER_MINIO_SECRET_KEY=keep-secret" in first_content
    assert 'LOOM_WORKER_SUBPROCESS_GATEWAY_URL=""' in first_content
    assert "LOOM_WORKER_MAX_CONCURRENT=10" in first_content
    assert "LOOM_WORKER_POOL_NAME=gb10" in first_content

    evidence = step_dir.artifact_path(
        "external-slurm-runner-prerequisites.json",
    ).read_text(encoding="utf-8")
    assert str(target_env) in evidence
    assert "sensitive-worker-token" not in evidence
    assert "old-worker-token" not in evidence
    assert "keep-secret" not in evidence
    assert json.loads(evidence)["records"][0]["worker_token"] == "[REDACTED]"


def test_materialization_rejects_symlinked_private_env_template(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.setenv("TEST_WORKER_TOKEN", "sensitive-worker-token")
    capacity = tmp_path / "capacity"
    capacity.mkdir()
    referent = tmp_path / "outside.env"
    referent.write_text(
        "LOOM_WORKER_TOKEN=outside-secret\n",
        encoding="utf-8",
    )
    referent.chmod(0o600)
    template = capacity / "staging-gb10-worker-staging-old.env"
    template.symlink_to(referent)
    target_env = capacity / "staging-gb10-worker-staging-abc123.env"
    profile = _write_external_prereq_profile(
        tmp_path,
        env_file=target_env,
        repo_dir=tmp_path / "repo",
        template_glob=str(capacity / "staging-gb10-worker-staging-*.env"),
    )
    ctx = make_ctx(
        tmp_path,
        image_tag="staging-abc123",
        worker_token_source="env:TEST_WORKER_TOKEN",
    )
    ev = EvidenceDirectory(tmp_path, "test-rid")
    ev.ensure()

    with pytest.raises(
        ExternalSlurmPrereqMaterializationError,
        match="template match is unsafe",
    ):
        _materialize_external_slurm_runner_prerequisites(
            ctx,
            profile,
            ev.step_dir(11, "env-state"),
        )

    assert referent.read_text(encoding="utf-8") == "LOOM_WORKER_TOKEN=outside-secret\n"
    assert not target_env.exists()


def test_materialization_rejects_incomplete_private_env_template(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.setenv("TEST_WORKER_TOKEN", "sensitive-worker-token")
    template = tmp_path / "staging-gb10-worker-staging-old.env"
    template.write_text("LOOM_WORKER_TOKEN=old-token\n", encoding="utf-8")
    template.chmod(0o600)
    target_env = tmp_path / "staging-gb10-worker-staging-abc123.env"
    profile = _write_external_prereq_profile(
        tmp_path,
        env_file=target_env,
        repo_dir=tmp_path / "repo",
        template_glob=str(tmp_path / "staging-gb10-worker-staging-*.env"),
    )
    ctx = make_ctx(
        tmp_path,
        image_tag="staging-abc123",
        worker_token_source="env:TEST_WORKER_TOKEN",
    )
    ev = EvidenceDirectory(tmp_path, "test-rid")
    ev.ensure()

    with pytest.raises(
        ExternalSlurmPrereqMaterializationError,
        match="missing required settings",
    ):
        _materialize_external_slurm_runner_prerequisites(
            ctx,
            profile,
            ev.step_dir(11, "env-state"),
        )

    assert not target_env.exists()


def test_materialization_rejects_symlinked_existing_target_before_read(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.setenv("TEST_WORKER_TOKEN", "sensitive-worker-token")
    template = tmp_path / "staging-gb10-worker-staging-old.env"
    template.write_text(_complete_worker_env_text(), encoding="utf-8")
    template.chmod(0o600)
    referent = tmp_path / "outside.env"
    referent.write_text(_complete_worker_env_text(), encoding="utf-8")
    referent.chmod(0o600)
    target_env = tmp_path / "staging-gb10-worker-staging-abc123.env"
    target_env.symlink_to(referent)
    profile = _write_external_prereq_profile(
        tmp_path,
        env_file=target_env,
        repo_dir=tmp_path / "repo",
        template_glob=str(tmp_path / "staging-gb10-worker-staging-*.env"),
    )
    ctx = make_ctx(
        tmp_path,
        image_tag="staging-abc123",
        worker_token_source="env:TEST_WORKER_TOKEN",
    )
    ev = EvidenceDirectory(tmp_path, "test-rid")
    ev.ensure()

    with pytest.raises(
        ExternalSlurmPrereqMaterializationError,
        match="env file is unsafe",
    ):
        _materialize_external_slurm_runner_prerequisites(
            ctx,
            profile,
            ev.step_dir(11, "env-state"),
        )

    assert target_env.is_symlink()
    assert referent.read_text(encoding="utf-8") == _complete_worker_env_text()


@pytest.mark.parametrize(
    ("suffix", "message"),
    [
        ("LOOM_WORKER_TOKEN=duplicate-token\n", "invalid key"),
        ('BROKEN="first\nsecond"\n', "malformed entry"),
    ],
)
def test_materialization_rejects_ambiguous_dotenv_before_target_creation(
    tmp_path: Path,
    monkeypatch,
    suffix: str,
    message: str,
) -> None:
    monkeypatch.setenv("TEST_WORKER_TOKEN", "sensitive-worker-token")
    template = tmp_path / "staging-gb10-worker-staging-old.env"
    template.write_text(_complete_worker_env_text() + suffix, encoding="utf-8")
    template.chmod(0o600)
    target_env = tmp_path / "staging-gb10-worker-staging-abc123.env"
    profile = _write_external_prereq_profile(
        tmp_path,
        env_file=target_env,
        repo_dir=tmp_path / "repo",
        template_glob=str(tmp_path / "staging-gb10-worker-staging-*.env"),
    )
    ctx = make_ctx(
        tmp_path,
        image_tag="staging-abc123",
        worker_token_source="env:TEST_WORKER_TOKEN",
    )
    ev = EvidenceDirectory(tmp_path, "test-rid")
    ev.ensure()

    with pytest.raises(ExternalSlurmPrereqMaterializationError, match=message) as exc:
        _materialize_external_slurm_runner_prerequisites(
            ctx,
            profile,
            ev.step_dir(11, "env-state"),
        )

    assert "duplicate-token" not in str(exc.value)
    assert "sensitive-worker-token" not in str(exc.value)
    assert not target_env.exists()


@pytest.mark.parametrize("semantic_empty", ('""', "'   '"))
def test_materialization_rejects_semantically_empty_required_value(
    tmp_path: Path,
    monkeypatch,
    semantic_empty: str,
) -> None:
    monkeypatch.setenv("TEST_WORKER_TOKEN", "sensitive-worker-token")
    template = tmp_path / "staging-gb10-worker-staging-old.env"
    template.write_text(
        _complete_worker_env_text().replace(
            "LOOM_WORKER_TOKEN=old-worker-token",
            f"LOOM_WORKER_TOKEN={semantic_empty}",
        ),
        encoding="utf-8",
    )
    template.chmod(0o600)
    target_env = tmp_path / "staging-gb10-worker-staging-abc123.env"
    profile = _write_external_prereq_profile(
        tmp_path,
        env_file=target_env,
        repo_dir=tmp_path / "repo",
        template_glob=str(tmp_path / "staging-gb10-worker-staging-*.env"),
    )
    ctx = make_ctx(
        tmp_path,
        image_tag="staging-abc123",
        worker_token_source="env:TEST_WORKER_TOKEN",
    )
    ev = EvidenceDirectory(tmp_path, "test-rid")
    ev.ensure()

    with pytest.raises(
        ExternalSlurmPrereqMaterializationError,
        match="empty required value",
    ) as exc:
        _materialize_external_slurm_runner_prerequisites(
            ctx,
            profile,
            ev.step_dir(11, "env-state"),
        )

    assert "sensitive-worker-token" not in str(exc.value)
    assert not target_env.exists()


def test_materialization_rejects_required_interpolation(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.setenv("TEST_WORKER_TOKEN", "sensitive-worker-token")
    template = tmp_path / "staging-gb10-worker-staging-old.env"
    template.write_text(
        _complete_worker_env_text().replace(
            "LOOM_WORKER_TOKEN=old-worker-token",
            "LOOM_WORKER_TOKEN=${UNSET}",
        ),
        encoding="utf-8",
    )
    template.chmod(0o600)
    target_env = tmp_path / "staging-gb10-worker-staging-abc123.env"
    profile = _write_external_prereq_profile(
        tmp_path,
        env_file=target_env,
        repo_dir=tmp_path / "repo",
        template_glob=str(tmp_path / "staging-gb10-worker-staging-*.env"),
    )
    ctx = make_ctx(
        tmp_path,
        image_tag="staging-abc123",
        worker_token_source="env:TEST_WORKER_TOKEN",
    )
    ev = EvidenceDirectory(tmp_path, "test-rid")
    ev.ensure()

    with pytest.raises(
        ExternalSlurmPrereqMaterializationError,
        match="cannot use interpolation",
    ) as exc:
        _materialize_external_slurm_runner_prerequisites(
            ctx,
            profile,
            ev.step_dir(11, "env-state"),
        )

    assert "sensitive-worker-token" not in str(exc.value)
    assert not target_env.exists()


def test_env_state_materializes_external_prereqs_before_apply_check(
    tmp_path: Path,
    monkeypatch,
) -> None:
    ctx = make_ctx(tmp_path, worker_token_source="file:/secure/path/worker-token")
    ev = EvidenceDirectory(tmp_path, "test-rid")
    ev.ensure()
    worktree = ev.step_dir(1, "worktree").path / "src"
    package_dir = worktree / "src" / "loom_cli"
    package_dir.mkdir(parents=True)
    (package_dir / "__main__.py").write_text("raise SystemExit(0)\n")
    step_dir = ev.step_dir(11, "env-state")
    profile = tmp_path / "staging.toml"
    profile.write_text('environment = "staging"\n', encoding="utf-8")
    order: list[str] = []

    def fake_materialize(*_args, **_kwargs):
        order.append("materialize")
        return []

    def fake_wait_cp(*_args, **_kwargs):
        order.append("wait-cp")

    def fake_run(argv, **_kwargs):
        if "apply" in argv:
            order.append("apply")
        elif "check" in argv:
            order.append("check")
        return SubprocessResult(
            argv=list(argv),
            returncode=0,
            stdout="ok\n",
            stderr="",
        )

    monkeypatch.setattr(
        "loom_cli.rollout.steps.s10_env_state._candidate_profile_path",
        lambda _ctx, _step_dir: Path(profile),
    )
    monkeypatch.setattr(
        "loom_cli.rollout.steps.s10_env_state._materialize_external_slurm_runner_prerequisites",
        fake_materialize,
    )
    monkeypatch.setattr(
        "loom_cli.rollout.steps.s10_env_state._wait_for_control_plane",
        fake_wait_cp,
    )
    monkeypatch.setattr("loom_cli.rollout.steps.s10_env_state.run_captured", fake_run)

    result = EnvStateStep().run(ctx, step_dir)

    assert result.exit_code == 0
    assert order == ["materialize", "wait-cp", "apply", "check"]


def test_env_state_stops_before_apply_when_control_plane_is_not_ready(
    tmp_path: Path,
    monkeypatch,
) -> None:
    ctx = make_ctx(tmp_path, worker_token_source="file:/secure/path/worker-token")
    ev = EvidenceDirectory(tmp_path, "test-rid")
    ev.ensure()
    worktree = ev.step_dir(1, "worktree").path / "src"
    package_dir = worktree / "src" / "loom_cli"
    package_dir.mkdir(parents=True)
    (package_dir / "__main__.py").write_text("raise SystemExit(0)\n")
    step_dir = ev.step_dir(11, "env-state")
    profile = tmp_path / "staging.toml"
    profile.write_text('environment = "staging"\n', encoding="utf-8")
    order: list[str] = []

    def fake_materialize(*_args, **_kwargs):
        order.append("materialize")
        return []

    def fake_wait_cp(*_args, **_kwargs):
        order.append("wait-cp")
        raise ControlPlaneReadinessError("control-plane did not become ready")

    def fake_run(*_args, **_kwargs):
        raise AssertionError("env-state apply/check should wait for CP first")

    monkeypatch.setattr(
        "loom_cli.rollout.steps.s10_env_state._candidate_profile_path",
        lambda _ctx, _step_dir: Path(profile),
    )
    monkeypatch.setattr(
        "loom_cli.rollout.steps.s10_env_state._materialize_external_slurm_runner_prerequisites",
        fake_materialize,
    )
    monkeypatch.setattr(
        "loom_cli.rollout.steps.s10_env_state._wait_for_control_plane",
        fake_wait_cp,
    )
    monkeypatch.setattr("loom_cli.rollout.steps.s10_env_state.run_captured", fake_run)

    result = EnvStateStep().run(ctx, step_dir)

    assert result.exit_code == 2
    assert order == ["materialize", "wait-cp"]
    assert "control-plane readiness failed" in str(result.error)
    assert "# control-plane-readiness" in step_dir.stderr_path().read_text(
        encoding="utf-8",
    )


def test_wait_for_control_plane_uses_root_healthz_and_records_evidence(
    tmp_path: Path,
    monkeypatch,
) -> None:
    ctx = make_ctx(tmp_path, cp_url="http://127.0.0.1:18081/admin/path?x=1")
    ev = EvidenceDirectory(tmp_path, "test-rid")
    ev.ensure()
    step_dir = ev.step_dir(11, "env-state")
    seen: list[tuple[str, float]] = []

    class FakeResponse:
        status = 200

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return None

        def read(self, _size: int) -> bytes:
            return b"ok\n"

    def fake_urlopen(url: str, *, timeout: float):
        seen.append((url, timeout))
        return FakeResponse()

    monkeypatch.setattr(
        "loom_cli.rollout.steps.s10_env_state.urllib.request.urlopen",
        fake_urlopen,
    )

    _wait_for_control_plane(ctx, step_dir)

    assert seen == [("http://127.0.0.1:18081/healthz", 2.0)]
    assert _control_plane_health_url(ctx.cp_url) == "http://127.0.0.1:18081/healthz"
    evidence = json.loads(
        step_dir.artifact_path("control-plane-readiness.json").read_text(
            encoding="utf-8",
        )
    )
    assert evidence["ready"] is True
    assert evidence["status"] == 200
    assert evidence["health_url"] == "http://127.0.0.1:18081/healthz"


@pytest.mark.parametrize(
    "drift",
    ("modified", "untracked", "ignored", "empty-directory", "malicious-config"),
)
def test_materialize_repo_dir_rejects_any_worktree_drift_without_replacement(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    drift: str,
) -> None:
    source_repo = tmp_path / "source"
    source_repo.mkdir()
    subprocess.run(["git", "-C", str(source_repo), "init"], check=True)
    subprocess.run(
        ["git", "-C", str(source_repo), "config", "user.email", "test@example.com"],
        check=True,
    )
    subprocess.run(
        ["git", "-C", str(source_repo), "config", "user.name", "Test User"],
        check=True,
    )
    (source_repo / "README.md").write_text("target\n", encoding="utf-8")
    (source_repo / ".gitignore").write_text("ignored-output\n", encoding="utf-8")
    subprocess.run(["git", "-C", str(source_repo), "add", "."], check=True)
    subprocess.run(["git", "-C", str(source_repo), "commit", "-m", "init"], check=True)
    head = subprocess.run(
        ["git", "-C", str(source_repo), "rev-parse", "HEAD"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    repo_root = tmp_path / "worker-repos"
    repo_root.mkdir()
    repo_root.chmod(0o2750)
    monkeypatch.setattr(
        "loom_cli.rollout.steps.s10_env_state._SHARED_WORKER_REPO_ROOT",
        repo_root,
    )
    monkeypatch.setattr(
        "loom_cli.rollout.steps.s10_env_state.grp.getgrnam",
        lambda name: SimpleNamespace(gr_gid=os.getgid()) if name == "sharedwork" else None,
    )
    repo_dir = repo_root / "loom-remote-worker-staging-abc123"

    created = _materialize_repo_dir(
        repo_dir=repo_dir,
        source_repo=source_repo,
        resolved_sha=head,
        expected_ref="staging-abc123",
    )
    if drift == "modified":
        (repo_dir / "README.md").write_text("dirty half update\n", encoding="utf-8")
    elif drift == "untracked":
        (repo_dir / "untracked-output").write_text("unexpected\n", encoding="utf-8")
    elif drift == "empty-directory":
        (repo_dir / "untracked-empty").mkdir(mode=0o750)
    elif drift == "malicious-config":
        sentinel = tmp_path / "must-not-execute"
        (repo_dir / ".git" / "config").write_text(
            "[core]\n\tfsmonitor = !touch " + str(sentinel) + "\n"
            '[filter "sentinel"]\n\tsmudge = touch ' + str(sentinel) + "\n",
            encoding="utf-8",
        )
        (repo_dir / ".git" / "config").chmod(0o640)

        def forbidden_git(*_args: object, **_kwargs: object) -> str:
            raise AssertionError("Git must not run before canonical config validation")

        monkeypatch.setattr(env_state_module, "_shared_repo_git", forbidden_git)
    else:
        (repo_dir / "ignored-output").write_text("unexpected\n", encoding="utf-8")
    with pytest.raises(ExternalSlurmPrereqMaterializationError):
        _materialize_repo_dir(
            repo_dir=repo_dir,
            source_repo=source_repo,
            resolved_sha=head,
            expected_ref="staging-abc123",
        )

    assert created["repo_action"] == "created"
    assert (repo_dir / "README.md").read_text(encoding="utf-8") == (
        "dirty half update\n" if drift == "modified" else "target\n"
    )
    if drift in {"untracked", "ignored"}:
        assert (repo_dir / f"{drift}-output").read_text(encoding="utf-8") == "unexpected\n"
    if drift == "empty-directory":
        assert (repo_dir / "untracked-empty").is_dir()
    if drift == "malicious-config":
        assert not sentinel.exists()
    assert not list(repo_dir.parent.glob(f".{repo_dir.name}.previous-*"))


def test_materialize_repo_dir_rejects_fresh_clone_with_untracked_empty_directory(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source_repo = tmp_path / "source"
    source_repo.mkdir()
    subprocess.run(["git", "-C", str(source_repo), "init", "-q"], check=True)
    subprocess.run(
        ["git", "-C", str(source_repo), "config", "user.email", "test@example.com"],
        check=True,
    )
    subprocess.run(
        ["git", "-C", str(source_repo), "config", "user.name", "Test User"],
        check=True,
    )
    (source_repo / "README.md").write_text("target\n", encoding="utf-8")
    subprocess.run(["git", "-C", str(source_repo), "add", "."], check=True)
    subprocess.run(["git", "-C", str(source_repo), "commit", "-qm", "init"], check=True)
    head = subprocess.run(
        ["git", "-C", str(source_repo), "rev-parse", "HEAD"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    repo_root = tmp_path / "worker-repos"
    repo_root.mkdir(mode=0o2750)
    repo_root.chmod(0o2750)
    monkeypatch.setattr(env_state_module, "_SHARED_WORKER_REPO_ROOT", repo_root)
    monkeypatch.setattr(
        env_state_module.grp,
        "getgrnam",
        lambda _name: SimpleNamespace(gr_gid=os.getgid()),
    )
    original_clone = env_state_module._clone_repo_checkout

    def clone_with_empty_directory(**kwargs: Any) -> None:
        original_clone(**kwargs)
        (kwargs["tmp_dir"] / "untracked-empty").mkdir()

    monkeypatch.setattr(
        env_state_module,
        "_clone_repo_checkout",
        clone_with_empty_directory,
    )
    repo_dir = repo_root / "loom-remote-worker-staging-abc123"

    with pytest.raises(
        ExternalSlurmPrereqMaterializationError,
        match="untracked directory",
    ):
        _materialize_repo_dir(
            repo_dir=repo_dir,
            source_repo=source_repo,
            resolved_sha=head,
            expected_ref="staging-abc123",
        )

    assert not repo_dir.exists()
    assert not list(repo_root.glob(f".{repo_dir.name}.tmp-*"))


def test_materialize_repo_dir_uses_no_hardlinks_and_preserves_tracked_symlinks(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clone_commands: list[list[str]] = []
    original_run_captured = env_state_module.run_captured

    def record_git_commands(argv: list[str], **kwargs: Any) -> SubprocessResult:
        if argv[:3] == ["git", "--no-replace-objects", "clone"]:
            clone_commands.append(argv)
        return original_run_captured(argv, **kwargs)

    monkeypatch.setattr(env_state_module, "run_captured", record_git_commands)
    source_repo = tmp_path / "source"
    source_repo.mkdir()
    subprocess.run(["git", "-C", str(source_repo), "init"], check=True)
    subprocess.run(
        ["git", "-C", str(source_repo), "config", "user.email", "test@example.com"],
        check=True,
    )
    subprocess.run(
        ["git", "-C", str(source_repo), "config", "user.name", "Test User"],
        check=True,
    )
    (source_repo / "README.md").write_text("target\n", encoding="utf-8")
    (source_repo / "README.link").symlink_to("README.md")
    (source_repo / "run.sh").write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    (source_repo / "run.sh").chmod(0o755)
    subprocess.run(["git", "-C", str(source_repo), "add", "."], check=True)
    subprocess.run(["git", "-C", str(source_repo), "commit", "-m", "init"], check=True)
    head = subprocess.run(
        ["git", "-C", str(source_repo), "rev-parse", "HEAD"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    repo_root = tmp_path / "worker-repos"
    repo_root.mkdir()
    repo_root.chmod(0o2750)
    monkeypatch.setattr(
        "loom_cli.rollout.steps.s10_env_state._SHARED_WORKER_REPO_ROOT",
        repo_root,
    )
    monkeypatch.setattr(
        "loom_cli.rollout.steps.s10_env_state.grp.getgrnam",
        lambda name: SimpleNamespace(gr_gid=os.getgid()) if name == "sharedwork" else None,
    )
    repo_dir = repo_root / "loom-remote-worker-staging-abc123"

    result = _materialize_repo_dir(
        repo_dir=repo_dir,
        source_repo=source_repo,
        resolved_sha=head,
        expected_ref="staging-abc123",
    )

    assert result["repo_head"] == head
    assert (repo_dir / "README.link").is_symlink()
    assert (repo_dir / "README.link").readlink() == Path("README.md")
    assert (repo_dir / "README.md").stat().st_ino != (source_repo / "README.md").stat().st_ino
    assert stat.S_IMODE((repo_dir / "README.md").stat().st_mode) == 0o640
    assert stat.S_IMODE((repo_dir / "run.sh").stat().st_mode) == 0o750
    assert (repo_dir / ".git" / "config").read_bytes() == (
        env_state_module._CANONICAL_SHARED_REPO_GIT_CONFIG
    )
    assert clone_commands
    assert all("--no-hardlinks" in command for command in clone_commands)


def test_atomic_repo_publish_succeeds_once_and_rejects_existing_targets(
    tmp_path: Path,
) -> None:
    source = tmp_path / "source"
    destination = tmp_path / "destination"
    source.mkdir()
    destination.mkdir()
    source_fd = os.open(source, os.O_RDONLY | os.O_DIRECTORY)
    destination_fd = os.open(destination, os.O_RDONLY | os.O_DIRECTORY)
    try:
        (source / "checkout").mkdir()
        env_state_module._rename_directory_noreplace(
            source_fd,
            "checkout",
            destination_fd,
            "published",
        )
        assert (destination / "published").is_dir()
        assert not (source / "checkout").exists()

        (source / "empty-race").mkdir()
        (destination / "empty-target").mkdir()
        with pytest.raises(
            ExternalSlurmPrereqMaterializationError,
            match="destination appeared",
        ):
            env_state_module._rename_directory_noreplace(
                source_fd,
                "empty-race",
                destination_fd,
                "empty-target",
            )
        assert (source / "empty-race").is_dir()
        assert (destination / "empty-target").is_dir()

        (source / "nonempty-race").mkdir()
        (destination / "nonempty-target").mkdir()
        (destination / "nonempty-target" / "marker").write_text(
            "preserve\n",
            encoding="utf-8",
        )
        with pytest.raises(
            ExternalSlurmPrereqMaterializationError,
            match="destination appeared",
        ):
            env_state_module._rename_directory_noreplace(
                source_fd,
                "nonempty-race",
                destination_fd,
                "nonempty-target",
            )
        assert (source / "nonempty-race").is_dir()
        assert (destination / "nonempty-target" / "marker").read_text(
            encoding="utf-8"
        ) == "preserve\n"
    finally:
        os.close(source_fd)
        os.close(destination_fd)


def test_materialize_repo_dir_rejects_non_direct_or_symlinked_authority(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    real_root = tmp_path / "real" / "worker-repos"
    real_root.mkdir(parents=True)
    real_root.chmod(0o2750)
    monkeypatch.setattr(
        "loom_cli.rollout.steps.s10_env_state._SHARED_WORKER_REPO_ROOT",
        real_root,
    )
    monkeypatch.setattr(
        "loom_cli.rollout.steps.s10_env_state.grp.getgrnam",
        lambda name: SimpleNamespace(gr_gid=os.getgid()) if name == "sharedwork" else None,
    )
    nested = real_root / "nested" / "loom-remote-worker-staging-abc123"
    with pytest.raises(
        ExternalSlurmPrereqMaterializationError,
        match="candidate-bound root",
    ):
        _materialize_repo_dir(
            repo_dir=nested,
            source_repo=tmp_path,
            resolved_sha="a" * 40,
            expected_ref="staging-abc123",
        )

    linked_root = tmp_path / "linked" / "worker-repos"
    linked_root.parent.mkdir()
    linked_root.symlink_to(real_root, target_is_directory=True)
    monkeypatch.setattr(
        "loom_cli.rollout.steps.s10_env_state._SHARED_WORKER_REPO_ROOT",
        linked_root,
    )
    with pytest.raises(
        ExternalSlurmPrereqMaterializationError,
        match="authority is unavailable",
    ):
        _materialize_repo_dir(
            repo_dir=linked_root / "loom-remote-worker-staging-abc123",
            source_repo=tmp_path,
            resolved_sha="a" * 40,
            expected_ref="staging-abc123",
        )

    monkeypatch.setattr(env_state_module, "_SHARED_WORKER_REPO_ROOT", real_root)
    outside = tmp_path / "outside-checkout"
    outside.mkdir()
    target = real_root / "loom-remote-worker-staging-abc123"
    target.symlink_to(outside, target_is_directory=True)
    with pytest.raises(
        ExternalSlurmPrereqMaterializationError,
        match="unsafe authority",
    ):
        _materialize_repo_dir(
            repo_dir=target,
            source_repo=tmp_path,
            resolved_sha="a" * 40,
            expected_ref="staging-abc123",
        )


def test_materialize_repo_dir_does_not_clean_replaced_temp_inode(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo_root = tmp_path / "worker-repos"
    repo_root.mkdir()
    repo_root.chmod(0o2750)
    monkeypatch.setattr(
        "loom_cli.rollout.steps.s10_env_state._SHARED_WORKER_REPO_ROOT",
        repo_root,
    )
    monkeypatch.setattr(
        "loom_cli.rollout.steps.s10_env_state.grp.getgrnam",
        lambda name: SimpleNamespace(gr_gid=os.getgid()) if name == "sharedwork" else None,
    )
    repo_dir = repo_root / "loom-remote-worker-staging-abc123"

    def replace_temp_authority(*, tmp_dir: Path, **_: Any) -> None:
        temp_root = tmp_dir.parent
        original = temp_root.with_name(f"{temp_root.name}.original")
        temp_root.rename(original)
        temp_root.mkdir()
        (temp_root / "foreign-marker").write_text("preserve\n", encoding="utf-8")
        raise ExternalSlurmPrereqMaterializationError("injected clone failure")

    monkeypatch.setattr(
        "loom_cli.rollout.steps.s10_env_state._clone_repo_checkout",
        replace_temp_authority,
    )

    with pytest.raises(
        ExternalSlurmPrereqMaterializationError,
        match="authority changed",
    ):
        _materialize_repo_dir(
            repo_dir=repo_dir,
            source_repo=tmp_path,
            resolved_sha="a" * 40,
            expected_ref="staging-abc123",
        )

    replacement = next(
        path
        for path in repo_root.glob(f".{repo_dir.name}.tmp-*")
        if not path.name.endswith(".original")
    )
    assert (replacement / "foreign-marker").read_text(encoding="utf-8") == "preserve\n"


def test_temp_root_requires_linux_setgid_inheritance_for_nonmember_shared_group(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    inherited = SimpleNamespace(
        st_gid=2007,
        st_uid=995,
        st_mode=stat.S_IFDIR | 0o2700,
    )
    missing_setgid = SimpleNamespace(
        st_gid=2007,
        st_uid=995,
        st_mode=stat.S_IFDIR | 0o700,
    )
    root = SimpleNamespace(identity=SimpleNamespace(st_gid=2007))
    monkeypatch.setattr(env_state_module.os, "geteuid", lambda: 995)
    monkeypatch.setattr(env_state_module.os, "getgid", lambda: 982)
    monkeypatch.setattr(env_state_module.os, "getgroups", lambda: [])

    class FakeTempRoot:
        def __init__(self, metadata: SimpleNamespace) -> None:
            self.metadata = metadata

        def lstat(self) -> SimpleNamespace:
            return self.metadata

    assert (
        env_state_module._prepare_temp_root(  # type: ignore[arg-type]
            FakeTempRoot(inherited),
            root=root,  # type: ignore[arg-type]
        )
        is inherited
    )
    with pytest.raises(
        ExternalSlurmPrereqMaterializationError,
        match="did not inherit setgid",
    ):
        env_state_module._prepare_temp_root(  # type: ignore[arg-type]
            FakeTempRoot(missing_setgid),
            root=root,  # type: ignore[arg-type]
        )


@pytest.mark.parametrize("drift", ("owner", "group", "mode"))
def test_materialize_repo_dir_rejects_root_metadata_drift(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    drift: str,
) -> None:
    repo_root = tmp_path / "worker-repos"
    repo_root.mkdir()
    repo_root.chmod(0o2750 if drift != "mode" else 0o2770)
    monkeypatch.setattr(env_state_module, "_SHARED_WORKER_REPO_ROOT", repo_root)
    expected_gid = os.getgid() + (1 if drift == "group" else 0)
    monkeypatch.setattr(
        env_state_module.grp,
        "getgrnam",
        lambda _name: SimpleNamespace(gr_gid=expected_gid),
    )
    if drift == "owner":
        monkeypatch.setattr(env_state_module.os, "geteuid", lambda: os.getuid() + 1)

    with pytest.raises(
        ExternalSlurmPrereqMaterializationError,
        match="unsafe owner or mode",
    ):
        _materialize_repo_dir(
            repo_dir=repo_root / "loom-remote-worker-staging-abc123",
            source_repo=tmp_path,
            resolved_sha="a" * 40,
            expected_ref="staging-abc123",
        )


@pytest.mark.parametrize("unsafe", ("foreign-owner", "top-writable", "hardlink", "fifo"))
def test_repo_tree_rejects_unsafe_authority_or_entries(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    unsafe: str,
) -> None:
    repo_root = tmp_path / "worker-repos"
    repo_root.mkdir()
    repo_root.chmod(0o2750)
    source_repo = tmp_path / "source"
    source_repo.mkdir()
    subprocess.run(["git", "-C", str(source_repo), "init", "-q"], check=True)
    subprocess.run(["git", "-C", str(source_repo), "config", "user.email", "x@y"], check=True)
    subprocess.run(["git", "-C", str(source_repo), "config", "user.name", "x"], check=True)
    (source_repo / "object").write_text("payload\n", encoding="utf-8")
    subprocess.run(["git", "-C", str(source_repo), "add", "object"], check=True)
    subprocess.run(["git", "-C", str(source_repo), "commit", "-qm", "init"], check=True)
    head = subprocess.run(
        ["git", "-C", str(source_repo), "rev-parse", "HEAD"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    monkeypatch.setattr(env_state_module, "_SHARED_WORKER_REPO_ROOT", repo_root)
    monkeypatch.setattr(
        env_state_module.grp,
        "getgrnam",
        lambda _name: SimpleNamespace(gr_gid=os.getgid()),
    )
    repo_dir = repo_root / "loom-remote-worker-staging-abc123"
    _materialize_repo_dir(
        repo_dir=repo_dir,
        source_repo=source_repo,
        resolved_sha=head,
        expected_ref="staging-abc123",
    )

    if unsafe == "foreign-owner":
        monkeypatch.setattr(env_state_module.os, "geteuid", lambda: os.getuid() + 1)
    elif unsafe == "top-writable":
        repo_dir.chmod(0o770)
    elif unsafe == "hardlink":
        original = repo_dir / "object"
        os.link(original, repo_dir / "object-link")
    else:
        os.mkfifo(repo_dir / "unsafe-fifo")

    root = env_state_module._open_bound_directory(repo_root)
    try:
        with pytest.raises(ExternalSlurmPrereqMaterializationError):
            env_state_module._validate_repo_tree(repo_dir, root=root, resolved_sha=head)
    finally:
        os.close(root.fd)


def test_materialize_repo_dir_matches_exact_sha_without_reclone_and_rejects_wrong_sha(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source_repo = tmp_path / "source"
    source_repo.mkdir()
    subprocess.run(["git", "-C", str(source_repo), "init"], check=True)
    subprocess.run(
        ["git", "-C", str(source_repo), "config", "user.email", "test@example.com"],
        check=True,
    )
    subprocess.run(
        ["git", "-C", str(source_repo), "config", "user.name", "Test User"],
        check=True,
    )
    (source_repo / "README.md").write_text("one\n", encoding="utf-8")
    subprocess.run(["git", "-C", str(source_repo), "add", "README.md"], check=True)
    subprocess.run(["git", "-C", str(source_repo), "commit", "-m", "one"], check=True)
    first_sha = subprocess.run(
        ["git", "-C", str(source_repo), "rev-parse", "HEAD"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    (source_repo / "README.md").write_text("two\n", encoding="utf-8")
    subprocess.run(["git", "-C", str(source_repo), "commit", "-am", "two"], check=True)
    second_sha = subprocess.run(
        ["git", "-C", str(source_repo), "rev-parse", "HEAD"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    repo_root = tmp_path / "worker-repos"
    repo_root.mkdir()
    repo_root.chmod(0o2750)
    monkeypatch.setattr(env_state_module, "_SHARED_WORKER_REPO_ROOT", repo_root)
    monkeypatch.setattr(
        env_state_module.grp,
        "getgrnam",
        lambda _name: SimpleNamespace(gr_gid=os.getgid()),
    )
    repo_dir = repo_root / "loom-remote-worker-staging-abc123"

    created = _materialize_repo_dir(
        repo_dir=repo_dir,
        source_repo=source_repo,
        resolved_sha=first_sha,
        expected_ref="staging-abc123",
    )
    original_clone = env_state_module._clone_repo_checkout
    clone_calls = 0

    def count_clone(**kwargs: Any) -> None:
        nonlocal clone_calls
        clone_calls += 1
        original_clone(**kwargs)

    monkeypatch.setattr(env_state_module, "_clone_repo_checkout", count_clone)
    matched = _materialize_repo_dir(
        repo_dir=repo_dir,
        source_repo=source_repo,
        resolved_sha=first_sha,
        expected_ref="staging-abc123",
    )
    with pytest.raises(
        ExternalSlurmPrereqMaterializationError,
        match="exactly match",
    ):
        _materialize_repo_dir(
            repo_dir=repo_dir,
            source_repo=source_repo,
            resolved_sha=second_sha,
            expected_ref="staging-abc123",
        )

    assert created["repo_action"] == "created"
    assert matched["repo_action"] == "matched"
    assert clone_calls == 0
    assert (
        subprocess.run(
            ["git", "-C", str(repo_dir), "rev-parse", "HEAD"],
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()
        == first_sha
    )


@pytest.mark.parametrize("hidden_flag", ("--assume-unchanged", "--skip-worktree"))
def test_materialize_repo_dir_rejects_hidden_non_probe_content_drift(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    hidden_flag: str,
) -> None:
    source_repo = tmp_path / "source"
    source_repo.mkdir()
    subprocess.run(["git", "-C", str(source_repo), "init", "-q"], check=True)
    subprocess.run(
        ["git", "-C", str(source_repo), "config", "user.email", "test@example.com"],
        check=True,
    )
    subprocess.run(
        ["git", "-C", str(source_repo), "config", "user.name", "Test User"],
        check=True,
    )
    (source_repo / "README.md").write_text("probe\n", encoding="utf-8")
    (source_repo / "other.txt").write_text("other\n", encoding="utf-8")
    subprocess.run(["git", "-C", str(source_repo), "add", "."], check=True)
    subprocess.run(["git", "-C", str(source_repo), "commit", "-qm", "init"], check=True)
    head = subprocess.run(
        ["git", "-C", str(source_repo), "rev-parse", "HEAD"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    repo_root = tmp_path / "worker-repos"
    repo_root.mkdir(mode=0o2750)
    repo_root.chmod(0o2750)
    monkeypatch.setattr(env_state_module, "_SHARED_WORKER_REPO_ROOT", repo_root)
    monkeypatch.setattr(
        env_state_module.grp,
        "getgrnam",
        lambda _name: SimpleNamespace(gr_gid=os.getgid()),
    )
    repo_dir = repo_root / "loom-remote-worker-staging-abc123"
    _materialize_repo_dir(
        repo_dir=repo_dir,
        source_repo=source_repo,
        resolved_sha=head,
        expected_ref="staging-abc123",
    )
    subprocess.run(
        ["git", "-C", str(repo_dir), "update-index", hidden_flag, "other.txt"],
        check=True,
    )
    (repo_dir / ".git" / "index").chmod(0o640)
    (repo_dir / "other.txt").write_text("hidden drift\n", encoding="utf-8")
    (repo_dir / "other.txt").chmod(0o640)

    with pytest.raises(
        ExternalSlurmPrereqMaterializationError,
        match="content drifted",
    ):
        _materialize_repo_dir(
            repo_dir=repo_dir,
            source_repo=source_repo,
            resolved_sha=head,
            expected_ref="staging-abc123",
        )

    assert (repo_dir / "other.txt").read_text(encoding="utf-8") == "hidden drift\n"


def test_materialize_repo_dir_ignores_replace_ref_and_rejects_replacement_tree_checkout(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source_repo = tmp_path / "source"
    source_repo.mkdir()
    subprocess.run(["git", "-C", str(source_repo), "init", "-q"], check=True)
    subprocess.run(
        ["git", "-C", str(source_repo), "config", "user.email", "test@example.com"],
        check=True,
    )
    subprocess.run(
        ["git", "-C", str(source_repo), "config", "user.name", "Test User"],
        check=True,
    )
    (source_repo / "README.md").write_text("candidate\n", encoding="utf-8")
    (source_repo / "other.txt").write_text("original\n", encoding="utf-8")
    subprocess.run(["git", "-C", str(source_repo), "add", "."], check=True)
    subprocess.run(["git", "-C", str(source_repo), "commit", "-qm", "candidate"], check=True)
    candidate_sha = subprocess.run(
        ["git", "-C", str(source_repo), "rev-parse", "HEAD"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    (source_repo / "other.txt").write_text(
        "hostile replacement payload\n",
        encoding="utf-8",
    )
    subprocess.run(["git", "-C", str(source_repo), "commit", "-qam", "replacement"], check=True)
    replacement_sha = subprocess.run(
        ["git", "-C", str(source_repo), "rev-parse", "HEAD"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    repo_root = tmp_path / "worker-repos"
    repo_root.mkdir(mode=0o2750)
    repo_root.chmod(0o2750)
    monkeypatch.setattr(env_state_module, "_SHARED_WORKER_REPO_ROOT", repo_root)
    monkeypatch.setattr(
        env_state_module.grp,
        "getgrnam",
        lambda _name: SimpleNamespace(gr_gid=os.getgid()),
    )
    repo_dir = repo_root / "loom-remote-worker-staging-abc123"
    _materialize_repo_dir(
        repo_dir=repo_dir,
        source_repo=source_repo,
        resolved_sha=candidate_sha,
        expected_ref="staging-abc123",
    )
    subprocess.run(
        ["git", "-C", str(repo_dir), "replace", candidate_sha, replacement_sha],
        check=True,
    )
    subprocess.run(
        ["git", "-C", str(repo_dir), "reset", "--hard", candidate_sha],
        check=True,
        capture_output=True,
    )
    assert (
        subprocess.run(
            ["git", "-C", str(repo_dir), "rev-parse", "HEAD"],
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()
        == candidate_sha
    )
    assert (
        subprocess.run(
            ["git", "-C", str(repo_dir), "status", "--porcelain=v1"],
            check=True,
            capture_output=True,
            text=True,
        ).stdout
        == ""
    )
    assert (repo_dir / "other.txt").read_text(encoding="utf-8") == ("hostile replacement payload\n")
    git_fd = os.open(repo_dir / ".git", os.O_RDONLY | os.O_DIRECTORY)
    try:
        env_state_module._normalize_git_metadata(
            git_fd,
            uid=os.geteuid(),
            gid=os.getegid(),
        )
    finally:
        os.close(git_fd)
    for path in (repo_dir / "README.md", repo_dir / "other.txt"):
        path.chmod(0o640)

    with pytest.raises(
        ExternalSlurmPrereqMaterializationError,
        match="index does not match",
    ):
        _materialize_repo_dir(
            repo_dir=repo_dir,
            source_repo=source_repo,
            resolved_sha=candidate_sha,
            expected_ref="staging-abc123",
        )


def test_materialize_repo_dir_rejects_external_common_git_authority_before_git(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source_repo = tmp_path / "source"
    source_repo.mkdir()
    subprocess.run(["git", "-C", str(source_repo), "init", "-q"], check=True)
    subprocess.run(
        ["git", "-C", str(source_repo), "config", "user.email", "test@example.com"],
        check=True,
    )
    subprocess.run(
        ["git", "-C", str(source_repo), "config", "user.name", "Test User"],
        check=True,
    )
    (source_repo / "README.md").write_text("candidate\n", encoding="utf-8")
    subprocess.run(["git", "-C", str(source_repo), "add", "."], check=True)
    subprocess.run(["git", "-C", str(source_repo), "commit", "-qm", "candidate"], check=True)
    head = subprocess.run(
        ["git", "-C", str(source_repo), "rev-parse", "HEAD"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    repo_root = tmp_path / "worker-repos"
    repo_root.mkdir(mode=0o2750)
    repo_root.chmod(0o2750)
    monkeypatch.setattr(env_state_module, "_SHARED_WORKER_REPO_ROOT", repo_root)
    monkeypatch.setattr(
        env_state_module.grp,
        "getgrnam",
        lambda _name: SimpleNamespace(gr_gid=os.getgid()),
    )
    repo_dir = repo_root / "loom-remote-worker-staging-abc123"
    _materialize_repo_dir(
        repo_dir=repo_dir,
        source_repo=source_repo,
        resolved_sha=head,
        expected_ref="staging-abc123",
    )
    external_common = tmp_path / "external-common.git"
    shutil.copytree(repo_dir / ".git", external_common, symlinks=True)
    commondir = repo_dir / ".git" / "commondir"
    commondir.write_text(str(external_common) + "\n", encoding="utf-8")
    commondir.chmod(0o640)
    assert subprocess.run(
        ["git", "-C", str(repo_dir), "rev-parse", "--git-common-dir"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip() == str(external_common)
    sentinel = tmp_path / "must-not-execute"
    (external_common / "config").write_text(
        "[core]\n\tfsmonitor = !touch " + str(sentinel) + "\n",
        encoding="utf-8",
    )

    def forbidden_git(*_args: object, **_kwargs: object) -> str:
        raise AssertionError("Git must not run before common authority validation")

    monkeypatch.setattr(env_state_module, "_shared_repo_git", forbidden_git)

    with pytest.raises(
        ExternalSlurmPrereqMaterializationError,
        match="common authority",
    ):
        _materialize_repo_dir(
            repo_dir=repo_dir,
            source_repo=source_repo,
            resolved_sha=head,
            expected_ref="staging-abc123",
        )

    assert not sentinel.exists()


def test_fresh_clone_writes_canonical_git_config_before_shared_git_verification(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source_repo = tmp_path / "source"
    source_repo.mkdir()
    subprocess.run(["git", "-C", str(source_repo), "init", "-q"], check=True)
    subprocess.run(
        ["git", "-C", str(source_repo), "config", "user.email", "test@example.com"],
        check=True,
    )
    subprocess.run(
        ["git", "-C", str(source_repo), "config", "user.name", "Test User"],
        check=True,
    )
    (source_repo / "README.md").write_text("candidate\n", encoding="utf-8")
    subprocess.run(["git", "-C", str(source_repo), "add", "."], check=True)
    subprocess.run(["git", "-C", str(source_repo), "commit", "-qm", "init"], check=True)
    head = subprocess.run(
        ["git", "-C", str(source_repo), "rev-parse", "HEAD"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    repo_root = tmp_path / "worker-repos"
    repo_root.mkdir(mode=0o2750)
    repo_root.chmod(0o2750)
    monkeypatch.setattr(env_state_module, "_SHARED_WORKER_REPO_ROOT", repo_root)
    monkeypatch.setattr(
        env_state_module.grp,
        "getgrnam",
        lambda _name: SimpleNamespace(gr_gid=os.getgid()),
    )
    original = env_state_module._shared_repo_git
    observed_configs: list[bytes] = []

    def shared_repo_git(repo_dir: Path, *arguments: str) -> str:
        observed_configs.append((repo_dir / ".git" / "config").read_bytes())
        return original(repo_dir, *arguments)

    monkeypatch.setattr(env_state_module, "_shared_repo_git", shared_repo_git)
    repo_dir = repo_root / "loom-remote-worker-staging-abc123"

    _materialize_repo_dir(
        repo_dir=repo_dir,
        source_repo=source_repo,
        resolved_sha=head,
        expected_ref="staging-abc123",
    )

    assert observed_configs
    assert set(observed_configs) == {env_state_module._CANONICAL_SHARED_REPO_GIT_CONFIG}


def test_shared_repo_git_uses_exact_safe_directory_argv_and_environment(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo = tmp_path / "worker-repos" / "loom-remote-worker-test"
    captured: dict[str, object] = {}

    def run_captured(argv: list[str], *, env: dict[str, str]) -> SubprocessResult:
        captured["argv"] = argv
        captured["env"] = env
        return SubprocessResult(argv, 0, "sha1\n", "")

    monkeypatch.setattr(env_state_module, "run_captured", run_captured)

    assert (
        env_state_module._shared_repo_git(
            repo,
            "rev-parse",
            "--show-object-format",
        )
        == "sha1\n"
    )
    assert captured["argv"] == [
        "/usr/bin/git",
        "--git-dir",
        str(repo / ".git"),
        "--work-tree",
        str(repo),
        "-c",
        f"safe.directory={repo}",
        "-c",
        f"core.worktree={repo}",
        "-c",
        "core.bare=false",
        "-c",
        "core.fsmonitor=false",
        "-c",
        "core.hooksPath=/dev/null",
        "-c",
        "core.attributesFile=/dev/null",
        "-c",
        "core.excludesFile=/dev/null",
        "-c",
        "core.untrackedCache=false",
        "-c",
        "submodule.recurse=false",
        "-c",
        "fetch.recurseSubmodules=false",
        "-c",
        "protocol.file.allow=never",
        "-c",
        "credential.helper=",
        "-c",
        "core.sshCommand=/usr/bin/false",
        "rev-parse",
        "--show-object-format",
    ]
    assert captured["env"] == {
        "GIT_CONFIG_NOSYSTEM": "1",
        "GIT_NO_REPLACE_OBJECTS": "1",
        "GIT_CONFIG_SYSTEM": "/dev/null",
        "GIT_CONFIG_GLOBAL": "/dev/null",
        "GIT_ATTR_NOSYSTEM": "1",
        "GIT_OPTIONAL_LOCKS": "0",
        "GIT_TERMINAL_PROMPT": "0",
        "GIT_PROTOCOL_FROM_USER": "0",
        "GIT_PAGER": "cat",
        "GIT_EXTERNAL_DIFF": "/usr/bin/false",
        "GIT_SSH_COMMAND": "/usr/bin/false",
        "HOME": "/nonexistent",
        "XDG_CONFIG_HOME": "/nonexistent",
        "LANG": "C",
        "LC_ALL": "C",
        "PATH": "/usr/bin:/bin",
    }


def test_materialize_repo_dir_cleans_clone_failure_and_refuses_existing_drift(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo_root = tmp_path / "worker-repos"
    repo_root.mkdir()
    repo_root.chmod(0o2750)
    monkeypatch.setattr(env_state_module, "_SHARED_WORKER_REPO_ROOT", repo_root)
    monkeypatch.setattr(
        env_state_module.grp,
        "getgrnam",
        lambda _name: SimpleNamespace(gr_gid=os.getgid()),
    )
    repo_dir = repo_root / "loom-remote-worker-staging-abc123"

    def fail_clone(**_: Any) -> None:
        raise ExternalSlurmPrereqMaterializationError("injected clone failure")

    monkeypatch.setattr(env_state_module, "_clone_repo_checkout", fail_clone)
    with pytest.raises(ExternalSlurmPrereqMaterializationError, match="injected clone failure"):
        _materialize_repo_dir(
            repo_dir=repo_dir,
            source_repo=tmp_path,
            resolved_sha="a" * 40,
            expected_ref="staging-abc123",
        )
    assert not list(repo_root.glob(f".{repo_dir.name}.tmp-*"))

    repo_dir.mkdir(mode=0o750)
    (repo_dir / ".git").mkdir(mode=0o750)
    (repo_dir / "dirty").write_text("dirty\n", encoding="utf-8")
    (repo_dir / "dirty").chmod(0o640)
    with pytest.raises(
        ExternalSlurmPrereqMaterializationError,
        match=r"unsafe authority|exactly match",
    ):
        _materialize_repo_dir(
            repo_dir=repo_dir,
            source_repo=tmp_path,
            resolved_sha="a" * 40,
            expected_ref="staging-abc123",
        )
    assert (repo_dir / "dirty").read_text(encoding="utf-8") == "dirty\n"
    assert not list(repo_root.glob(f".{repo_dir.name}.previous-*"))
