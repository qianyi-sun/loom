"""Env-state external Slurm prerequisite materialization tests (#562)."""

from __future__ import annotations

import json
import stat
import subprocess
from pathlib import Path
from typing import Any

import pytest

from loom_cli.rollout.base_context_fixture import make_ctx
from loom_cli.rollout.evidence import EvidenceDirectory
from loom_cli.rollout.steps.s10_env_state import (
    ControlPlaneReadinessError,
    EnvStateStep,
    ExternalSlurmPrereqMaterializationError,
    _control_plane_health_url,
    _materialize_external_slurm_runner_prerequisites,
    _materialize_repo_dir,
    _wait_for_control_plane,
)
from loom_cli.rollout.steps.subprocess_util import SubprocessResult


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
pool_name = "gb10-arm64"
actuator = "slurm"
enabled = true
max_slots = 150

[worker_pool_autoscaler_policies.actuator_config]
external_runner = true
env_file = "{env_file}"
repo_dir = "{repo_dir}"
requested_concurrency = 10

[external_slurm_runner_prerequisites]
pools = ["gb10-arm64"]
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
    assert "LOOM_WORKER_POOL_NAME=gb10-arm64" in first_content

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


def test_materialize_repo_dir_replaces_dirty_half_updated_checkout(
    tmp_path: Path,
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
    subprocess.run(["git", "-C", str(source_repo), "add", "README.md"], check=True)
    subprocess.run(["git", "-C", str(source_repo), "commit", "-m", "init"], check=True)
    head = subprocess.run(
        ["git", "-C", str(source_repo), "rev-parse", "HEAD"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    repo_dir = tmp_path / "loom-remote-worker-staging-abc123"

    created = _materialize_repo_dir(
        repo_dir=repo_dir,
        source_repo=source_repo,
        resolved_sha=head,
        expected_ref=head[:7],
    )
    (repo_dir / "README.md").write_text("dirty half update\n", encoding="utf-8")
    replaced = _materialize_repo_dir(
        repo_dir=repo_dir,
        source_repo=source_repo,
        resolved_sha=head,
        expected_ref=head[:7],
    )

    assert created["repo_action"] == "created"
    assert replaced["repo_action"] == "replaced"
    assert (repo_dir / "README.md").read_text(encoding="utf-8") == "target\n"
    assert (
        subprocess.run(
            [
                "git",
                "-C",
                str(repo_dir),
                "status",
                "--short",
                "--untracked-files=no",
            ],
            check=True,
            capture_output=True,
            text=True,
        ).stdout
        == ""
    )
    assert list(repo_dir.parent.glob(f".{repo_dir.name}.previous-*"))
