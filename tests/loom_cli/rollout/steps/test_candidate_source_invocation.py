"""Regression tests for candidate-source rollout command invocation (#441)."""

from __future__ import annotations

import hashlib
import json
import os
import stat
import subprocess
import sys
import threading
import time
import tomllib
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from loom_cli.rollout.base_context_fixture import make_ctx
from loom_cli.rollout.context import RolloutContext
from loom_cli.rollout.evidence import EvidenceDirectory
from loom_cli.rollout.image_readiness import (
    AUXILIARY_ROLLOUT_IMAGES,
    DEFAULT_ROLLOUT_IMAGE_PLAN,
    ROLLOUT_IMAGES,
    image_plan_digest,
)
from loom_cli.rollout.steps import candidate_source
from loom_cli.rollout.steps.base import RunResult, VerifyOutcome
from loom_cli.rollout.steps.candidate_source import (
    candidate_loom_argv,
    candidate_loom_env,
    candidate_relative_path,
    rollout_cluster_config,
)
from loom_cli.rollout.steps.s04_gb10_prep import (
    _LEGACY_NODE_AGENT_TIMER_DROPIN,
    GB10Host,
    GB10PrepStep,
    _node_agent_timer_dropin_cleanup_command,
    _node_agent_timer_dropins_absent_command,
    _node_agent_timer_name,
    _prep_one_host,
    _ssh,
    gb10_hosts_for,
)
from loom_cli.rollout.steps.s05_backup import BackupStep
from loom_cli.rollout.steps.s06_audit import AuditStep
from loom_cli.rollout.steps.s07_render import RenderStep
from loom_cli.rollout.steps.s08_preflight import PreflightStep
from loom_cli.rollout.steps.s09_migrate import MigrateStep
from loom_cli.rollout.steps.s10_env_state import (
    EnvStateStep,
    _materialize_rollout_root_profile,
    _profile_path_for,
)
from loom_cli.rollout.steps.s11_cluster_up import ClusterUpStep
from loom_cli.rollout.steps.s12_production_defaults import ProductionDefaultsStep
from loom_cli.rollout.steps.s12_release_gate import (
    ReleaseGateStep,
    _current_candidate_worker_binding,
    _is_gb10_convergence_failure,
)
from loom_cli.rollout.steps.subprocess_util import SubprocessResult

_REPO_ROOT = Path(__file__).resolve().parents[4]
_RUN_CANDIDATE_BOUND_HF_CANARY = ReleaseGateStep._run_candidate_bound_hf_canary


@pytest.fixture(autouse=True)
def _stub_candidate_bound_hf_canary(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        ReleaseGateStep,
        "_run_candidate_bound_hf_canary",
        lambda _self, _ctx, _step_dir: RunResult(
            exit_code=0,
            artifacts={"batch_id": "00000000-0000-4000-8000-000000000001"},
        ),
    )


def test_sealed_gb10_prep_fetches_exact_shared_candidate(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sha = "a" * 40
    ctx = replace(
        make_ctx(tmp_path),
        image_tag="staging-aaaaaaa",
        resolved_sha=sha,
        source_mode="sealed-cumulative",
        resolved_tree="b" * 40,
        approved_base_sha="c" * 40,
    )
    host = GB10Host(
        ssh_target="trt-gb10-1",
        repo_path="/srv/loom/worker-build-staging",
        env_file_path="/srv/loom/worker-staging.env",
    )
    commands: list[str] = []

    def successful_ssh(
        _host: GB10Host,
        command: str,
        *,
        stdin_text: str | None = None,
    ) -> SubprocessResult:
        assert stdin_text is None
        commands.append(command)
        return SubprocessResult([], 0, "", "")

    monkeypatch.setattr("loom_cli.rollout.steps.s04_gb10_prep._ssh", successful_ssh)
    host_dir = tmp_path / "host"
    host_dir.mkdir()

    ok, _summary = _prep_one_host(ctx, host, host_dir)

    assert ok is True
    fetch = next(command for command in commands if " fetch " in command)
    assert "git fetch --quiet origin" not in fetch
    assert "-c protocol.file.allow=always" in fetch
    assert "-c fetch.fsckObjects=true" in fetch
    assert "--upload-pack='/usr/bin/git -c safe.directory=" in fetch
    assert "/loom-remote-worker-staging-aaaaaaa/.git upload-pack'" in fetch
    assert (
        "/shared_work2/loom-staging-rollout/worker-repos/loom-remote-worker-staging-aaaaaaa"
    ) in fetch
    assert fetch.endswith(sha)


def test_node_agent_timer_dropin_cleanup_removes_only_exact_legacy_payload(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("HOME", str(tmp_path))
    dropin_dir = (
        tmp_path
        / ".config/systemd/user/loom-gb10-node-agent.timer.d"
    )
    dropin_dir.mkdir(parents=True)
    dropin = dropin_dir / "deploy-window.conf"
    dropin.write_bytes(_LEGACY_NODE_AGENT_TIMER_DROPIN)
    dropin.chmod(0o664)

    result = subprocess.run(
        _node_agent_timer_dropin_cleanup_command("loom-gb10-node-agent.timer"),
        check=False,
        shell=True,
        executable="/bin/sh",
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0
    assert not dropin_dir.exists()


@pytest.mark.parametrize(
    "unsafe_setup",
    ["changed-payload", "extra-entry", "symlink"],
)
def test_node_agent_timer_dropin_cleanup_rejects_unknown_state(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    unsafe_setup: str,
) -> None:
    monkeypatch.setenv("HOME", str(tmp_path))
    dropin_dir = (
        tmp_path
        / ".config/systemd/user/loom-gb10-node-agent.timer.d"
    )
    dropin_dir.mkdir(parents=True)
    dropin = dropin_dir / "deploy-window.conf"
    if unsafe_setup == "symlink":
        target = tmp_path / "target"
        target.write_bytes(_LEGACY_NODE_AGENT_TIMER_DROPIN)
        dropin.symlink_to(target)
    else:
        dropin.write_bytes(
            b"[Timer]\nOnUnitActiveSec=30s\n"
            if unsafe_setup == "changed-payload"
            else _LEGACY_NODE_AGENT_TIMER_DROPIN
        )
    if unsafe_setup == "extra-entry":
        (dropin_dir / "unknown.conf").write_text("[Timer]\n", encoding="utf-8")

    result = subprocess.run(
        _node_agent_timer_dropin_cleanup_command("loom-gb10-node-agent.timer"),
        check=False,
        shell=True,
        executable="/bin/sh",
        capture_output=True,
        text=True,
    )

    assert result.returncode == 1
    assert dropin_dir.exists()


def test_node_agent_timer_dropin_verify_requires_no_effective_overrides() -> None:
    assert _node_agent_timer_dropins_absent_command(
        "loom-gb10-node-agent.timer"
    ) == (
        'test ! -e "$HOME/.config/systemd/user/'
        'loom-gb10-node-agent.timer.d"'
    )


def test_gb10_prep_rejects_unknown_dropin_before_unit_or_systemd_mutation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ctx = make_ctx(tmp_path)
    host = GB10Host(
        ssh_target="trt-gb10-1",
        repo_path="/srv/loom/worker-build-staging",
        env_file_path="/srv/loom/worker-staging.env",
        node_agent_service="loom-gb10-node-agent.service",
    )
    commands: list[str] = []

    def fake_ssh(
        _host: GB10Host,
        command: str,
        *,
        stdin_text: str | None = None,
    ) -> SubprocessResult:
        assert stdin_text is None
        commands.append(command)
        return SubprocessResult(
            [],
            1 if "deploy-window.conf" in command else 0,
            "",
            "",
        )

    monkeypatch.setattr("loom_cli.rollout.steps.s04_gb10_prep._ssh", fake_ssh)
    host_dir = tmp_path / "host"
    host_dir.mkdir()

    ok, summary = _prep_one_host(ctx, host, host_dir)

    assert not ok
    assert "node-agent-timer-dropin-cleanup failed" in summary
    assert not any("install -D" in command for command in commands)
    assert not any("systemctl --user" in command for command in commands)


def test_merged_dev_gb10_prep_keeps_origin_fetch(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ctx = make_ctx(tmp_path)
    host = GB10Host(
        ssh_target="trt-gb10-1",
        repo_path="/srv/loom/worker-build-staging",
        env_file_path="/srv/loom/worker-staging.env",
    )
    commands: list[str] = []

    def successful_ssh(
        _host: GB10Host,
        command: str,
        *,
        stdin_text: str | None = None,
    ) -> SubprocessResult:
        assert stdin_text is None
        commands.append(command)
        return SubprocessResult([], 0, "", "")

    monkeypatch.setattr("loom_cli.rollout.steps.s04_gb10_prep._ssh", successful_ssh)
    host_dir = tmp_path / "host"
    host_dir.mkdir()

    ok, _summary = _prep_one_host(ctx, host, host_dir)

    assert ok is True
    assert any(command.endswith("git fetch --quiet origin") for command in commands)


def test_sealed_context_binds_source_identities_into_inputs(tmp_path: Path) -> None:
    ctx = replace(
        make_ctx(tmp_path),
        request_id="req-1234567890abcdef",
        source_mode="sealed-cumulative",
        resolved_tree="b" * 40,
        approved_base_sha="c" * 40,
        preflight_attestation_sha256="d" * 64,
        preflight_registry_sha256="e" * 64,
        preflight_coverage_sha256="f" * 64,
    )

    inputs = ctx.to_inputs_dict()

    assert inputs["source_mode"] == "sealed-cumulative"
    assert inputs["resolved_tree"] == "b" * 40
    assert inputs["approved_base_sha"] == "c" * 40


def _is_candidate_invocation(argv: list[str]) -> bool:
    return argv[:3] == [sys.executable, "-I", "-c"]


def _candidate_args(argv: list[str]) -> list[str]:
    assert _is_candidate_invocation(argv)
    return argv[4:]


@pytest.fixture(autouse=True)
def _control_plane_ready(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "loom_cli.rollout.steps.s10_env_state._wait_for_control_plane",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(
        "loom_cli.rollout.steps.s02_build_images.validate_candidate_worktree_identity",
        lambda ctx: (
            ctx.rollout_root
            / "rollouts"
            / ctx.metadata["rollout_id"]
            / "01-worktree"
            / "src"
        ),
    )

    def fake_materialize(ctx, repo_path, target):
        worktree = (
            ctx.rollout_root
            / "rollouts"
            / ctx.metadata["rollout_id"]
            / "01-worktree"
            / "src"
        )
        data = (worktree / repo_path).read_bytes()
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(data)
        return candidate_source.MaterializedCandidateBlob(data, target.resolve())

    monkeypatch.setattr(
        "loom_cli.rollout.steps.s02_build_images.materialize_candidate_blob",
        fake_materialize,
    )


def _prepare_candidate_worktree(ev: EvidenceDirectory) -> Path:
    worktree = ev.step_dir(1, "worktree").path / "src"
    package_dir = worktree / "src" / "loom_cli"
    package_dir.mkdir(parents=True)
    (package_dir / "__main__.py").write_text("raise SystemExit(0)\n")
    ownership_script = worktree / "scripts" / "component_ownership.py"
    ownership_script.parent.mkdir(parents=True)
    primary = tuple((name, dockerfile, ".") for name, dockerfile in ROLLOUT_IMAGES)
    auxiliary = tuple(
        (name, dockerfile, ".") for name, dockerfile in AUXILIARY_ROLLOUT_IMAGES
    )
    primary_rows = [
        {"image_name": name, "dockerfile": dockerfile, "context": context}
        for name, dockerfile, context in primary
    ]
    auxiliary_rows = [
        {"image_name": name, "dockerfile": dockerfile, "context": context}
        for name, dockerfile, context in auxiliary
    ]
    role_payload = {
        "auxiliary_images": auxiliary_rows,
        "primary_images": primary_rows,
    }
    query_payload = {"primary": primary_rows, "auxiliary": auxiliary_rows}
    ownership_script.write_text(
        "import json\n"
        "import sys\n"
        f"payloads = {query_payload!r}\n"
        "role = sys.argv[sys.argv.index('--rollout-role') + 1]\n"
        "print(json.dumps(payloads[role]))\n",
        encoding="utf-8",
    )
    manifest_path = worktree / "config" / "component-ownership.toml"
    manifest_path.parent.mkdir(parents=True)
    manifest_data = b"schema_version = 2\n"
    manifest_path.write_bytes(manifest_data)
    matrix_sha256 = hashlib.sha256(
        json.dumps(role_payload, separators=(",", ":"), sort_keys=True).encode(),
    ).hexdigest()
    image_ids = {
        name: "sha256:" + f"{index:064x}"
        for index, (name, _dockerfile, _context) in enumerate(
            DEFAULT_ROLLOUT_IMAGE_PLAN,
            start=1,
        )
    }
    ev.step_dir(2, "build-images").artifact_path("image-matrix.json").write_text(
        json.dumps(
            {
                "schema_version": 2,
                "resolved_sha": "a" * 40,
                "component_manifest_sha256": hashlib.sha256(manifest_data).hexdigest(),
                "matrix_sha256": matrix_sha256,
                "image_plan_sha256": image_plan_digest(DEFAULT_ROLLOUT_IMAGE_PLAN),
                "image_artifact_sha256": "b" * 64,
                "primary_images": primary_rows,
                "auxiliary_images": auxiliary_rows,
                "image_ids": image_ids,
            }
        ),
        encoding="utf-8",
    )
    return worktree


def test_candidate_environment_is_fixed_and_contains_no_pythonpath_or_injection(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ev = EvidenceDirectory(tmp_path, "test-rid")
    ev.ensure()
    _prepare_candidate_worktree(ev)
    step_dir = ev.step_dir(7, "render")
    for name in (
        "PYTHONPATH",
        "PYTHONHOME",
        "GIT_CONFIG_GLOBAL",
        "LD_PRELOAD",
        "AWS_SECRET_ACCESS_KEY",
        "SSH_AUTH_SOCK",
        "HTTPS_PROXY",
        "DOCKER_CONFIG",
        "LOOM_CP_ADMIN_TOKEN",
    ):
        monkeypatch.setenv(name, f"unsafe-{name}")

    env = candidate_loom_env(step_dir)

    assert "PYTHONPATH" not in env
    assert "PYTHONHOME" not in env
    assert "GIT_CONFIG_GLOBAL" not in env
    assert "LD_PRELOAD" not in env
    assert "AWS_SECRET_ACCESS_KEY" not in env
    assert "SSH_AUTH_SOCK" not in env
    assert "HTTPS_PROXY" not in env
    assert "DOCKER_CONFIG" not in env
    assert "LOOM_CP_ADMIN_TOKEN" not in env
    assert env["PATH"] == f"{Path(sys.executable).parent}:/usr/local/bin:/usr/bin:/bin"


def test_candidate_argv_uses_isolated_fixed_runpy_launcher() -> None:
    argv = candidate_loom_argv("cluster", "status")

    assert argv[:3] == [sys.executable, "-I", "-c"]
    assert "run_module('loom_cli'" in argv[3]
    assert argv[4:] == ["cluster", "status"]
    assert "PYTHONPATH" not in " ".join(argv)


def test_candidate_path_git_probe_uses_fixed_environment(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ev = EvidenceDirectory(tmp_path, "test-rid")
    ev.ensure()
    candidate = _prepare_candidate_worktree(ev)
    step_dir = ev.step_dir(7, "render")
    repo = tmp_path / "source"
    source = repo / "deploy" / "config.toml"
    source.parent.mkdir(parents=True)
    source.write_text("x = 1\n", encoding="utf-8")
    mapped = candidate / "deploy" / "config.toml"
    mapped.parent.mkdir(parents=True)
    mapped.write_text("x = 1\n", encoding="utf-8")
    seen: dict[str, Any] = {}

    class Completed:
        returncode = 0
        stdout = str(repo) + "\n"
        stderr = ""

    def fake_run(argv, **kwargs):
        seen["env"] = kwargs.get("env")
        return Completed()

    monkeypatch.setenv("GIT_CONFIG_GLOBAL", "/tmp/evil")
    monkeypatch.setenv("LD_PRELOAD", "/tmp/evil.so")
    monkeypatch.setattr("subprocess.run", fake_run)

    assert candidate_relative_path(source, step_dir) == mapped
    assert "GIT_CONFIG_GLOBAL" not in seen["env"]
    assert "LD_PRELOAD" not in seen["env"]


def test_candidate_worktree_from_context_uses_rollout_evidence_identity(
    tmp_path: Path,
) -> None:
    rollout_root = tmp_path / "evidence"
    rollout_root.mkdir(mode=0o700)
    ctx = make_ctx(tmp_path, rollout_root=rollout_root)
    evidence = EvidenceDirectory(rollout_root, "test-rid")
    evidence.ensure()
    expected = _prepare_candidate_worktree(evidence)

    assert candidate_source.candidate_worktree_from_context(ctx) == expected


def test_candidate_worktree_from_context_requires_rollout_id(tmp_path: Path) -> None:
    ctx = replace(make_ctx(tmp_path), metadata={})

    with pytest.raises(candidate_source.CandidateToolingError, match="rollout_id"):
        candidate_source.candidate_worktree_from_context(ctx)


def test_candidate_worktree_from_context_requires_existing_candidate(
    tmp_path: Path,
) -> None:
    ctx = make_ctx(tmp_path)

    with pytest.raises(
        candidate_source.CandidateToolingError,
        match="candidate worktree does not exist",
    ):
        candidate_source.candidate_worktree_from_context(ctx)


def test_materialize_candidate_blob_is_commit_bound_and_fails_closed(
    tmp_path: Path,
) -> None:
    rollout_root = tmp_path / "evidence"
    rollout_root.mkdir(mode=0o700)
    ctx = make_ctx(tmp_path, rollout_root=rollout_root)
    evidence = EvidenceDirectory(rollout_root, "test-rid")
    evidence.ensure()
    worktree = evidence.step_dir(1, "worktree").path / "src"
    source = worktree / "deploy" / "k8s" / "ingress.yaml"
    source.parent.mkdir(parents=True)
    source.write_bytes(b"candidate-v1\n")
    subprocess.run(["git", "init", "--quiet"], cwd=worktree, check=True)
    subprocess.run(
        ["git", "config", "user.email", "rollout-tests@example.invalid"],
        cwd=worktree,
        check=True,
    )
    subprocess.run(
        ["git", "config", "user.name", "Rollout Tests"],
        cwd=worktree,
        check=True,
    )
    subprocess.run(["git", "add", "."], cwd=worktree, check=True)
    subprocess.run(
        ["git", "commit", "--quiet", "-m", "candidate v1"],
        cwd=worktree,
        check=True,
    )
    first_head = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=worktree,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    ctx = replace(ctx, resolved_sha=first_head)
    target = evidence.step_dir(3, "cluster-target").artifact_path("candidate.yaml")

    first_blob = candidate_source.materialize_candidate_blob(
        ctx,
        Path("deploy/k8s/ingress.yaml"),
        target,
    )
    assert first_blob.evidence_path == target.resolve()
    assert first_blob.data == b"candidate-v1\n"
    assert target.read_bytes() == b"candidate-v1\n"

    target.write_bytes(b"tampered evidence\n")
    candidate_source.materialize_candidate_blob(
        ctx,
        Path("deploy/k8s/ingress.yaml"),
        target,
    )
    assert target.read_bytes() == b"candidate-v1\n"

    source.write_bytes(b"dirty worktree\n")
    with pytest.raises(
        candidate_source.CandidateToolingError,
        match="candidate worktree is dirty",
    ):
        candidate_source.materialize_candidate_blob(
            ctx,
            Path("deploy/k8s/ingress.yaml"),
            target,
        )

    subprocess.run(["git", "add", "."], cwd=worktree, check=True)
    subprocess.run(
        ["git", "commit", "--quiet", "-m", "candidate v2"],
        cwd=worktree,
        check=True,
    )
    second_head = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=worktree,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    with pytest.raises(
        candidate_source.CandidateToolingError,
        match="HEAD does not match resolved rollout SHA",
    ):
        candidate_source.materialize_candidate_blob(
            ctx,
            Path("deploy/k8s/ingress.yaml"),
            target,
        )

    second_blob = candidate_source.materialize_candidate_blob(
        replace(ctx, resolved_sha=second_head),
        Path("deploy/k8s/ingress.yaml"),
        target,
    )
    assert second_blob.data == b"dirty worktree\n"
    assert target.read_bytes() == b"dirty worktree\n"


def test_materialize_candidate_blob_ignores_replace_refs_and_git_env_injection(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    rollout_root = tmp_path / "evidence"
    rollout_root.mkdir(mode=0o700)
    ctx = make_ctx(tmp_path, rollout_root=rollout_root)
    evidence = EvidenceDirectory(rollout_root, "test-rid")
    evidence.ensure()
    worktree = evidence.step_dir(1, "worktree").path / "src"
    source = worktree / "deploy" / "k8s" / "ingress.yaml"
    source.parent.mkdir(parents=True)
    source.write_bytes(b"trusted-v1\n")
    subprocess.run(["git", "init", "--quiet"], cwd=worktree, check=True)
    subprocess.run(
        ["git", "config", "user.email", "rollout-tests@example.invalid"],
        cwd=worktree,
        check=True,
    )
    subprocess.run(
        ["git", "config", "user.name", "Rollout Tests"],
        cwd=worktree,
        check=True,
    )
    subprocess.run(["git", "add", "."], cwd=worktree, check=True)
    subprocess.run(
        ["git", "commit", "--quiet", "-m", "trusted v1"],
        cwd=worktree,
        check=True,
    )
    first_head = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=worktree,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()

    source.write_bytes(b"replacement-v2\n")
    subprocess.run(["git", "add", "."], cwd=worktree, check=True)
    subprocess.run(
        ["git", "commit", "--quiet", "-m", "replacement v2"],
        cwd=worktree,
        check=True,
    )
    second_head = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=worktree,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    subprocess.run(
        ["git", "checkout", "--detach", "--quiet", first_head],
        cwd=worktree,
        check=True,
    )
    subprocess.run(
        ["git", "replace", first_head, second_head],
        cwd=worktree,
        check=True,
    )
    replaced = subprocess.run(
        ["git", "show", f"{first_head}:deploy/k8s/ingress.yaml"],
        cwd=worktree,
        check=True,
        capture_output=True,
    )
    assert replaced.stdout == b"replacement-v2\n"

    monkeypatch.setenv("GIT_DIR", str(tmp_path / "injected-git-dir"))
    monkeypatch.setenv("GIT_OBJECT_DIRECTORY", str(tmp_path / "injected-objects"))
    monkeypatch.setenv(
        "GIT_ALTERNATE_OBJECT_DIRECTORIES",
        str(tmp_path / "injected-alternates"),
    )
    monkeypatch.setenv("GIT_CONFIG_COUNT", "1")
    monkeypatch.setenv("GIT_CONFIG_KEY_0", "core.repositoryformatversion")
    monkeypatch.setenv("GIT_CONFIG_VALUE_0", "999")
    target = evidence.step_dir(3, "cluster-target").artifact_path("candidate.yaml")

    blob = candidate_source.materialize_candidate_blob(
        replace(ctx, resolved_sha=first_head),
        Path("deploy/k8s/ingress.yaml"),
        target,
    )

    assert blob.data == b"trusted-v1\n"
    assert target.read_bytes() == b"trusted-v1\n"


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
    assert call["argv"][:3] == [sys.executable, "-I", "-c"]
    assert "run_module('loom_cli'" in call["argv"][3]
    assert call["cwd"] == worktree
    assert "PYTHONPATH" not in call["env"]


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
                            "pool_name": "gb10",
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


def _write_single_node_agent_gb10_config(
    ctx: RolloutContext,
    tmp_path: Path,
) -> None:
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
        if _is_candidate_invocation(list(argv)):
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
    assert _candidate_args(calls[0]["argv"])[:2] == ["cluster", "render-migration"]


def test_migrate_registry_profile_uses_published_control_plane_digest(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ctx = make_ctx(tmp_path)
    ev = EvidenceDirectory(tmp_path, "test-rid")
    ev.ensure()
    _prepare_candidate_worktree(ev)
    _write_rendered_stateful_substrate(ev)
    step_dir = ev.step_dir(9, "migrate")
    digest = "sha256:" + "d" * 64
    calls: list[list[str]] = []
    monkeypatch.setattr(
        "loom_cli.rollout.steps.s09_migrate.load_cluster_config",
        lambda _path: object(),
    )
    monkeypatch.setattr(
        "loom_cli.rollout.steps.s09_migrate.validate_container_registry_publication",
        lambda _config: ("192.168.50.13:5000", "localhost:5000"),
    )
    monkeypatch.setattr(
        "loom_cli.rollout.steps.s09_migrate.registry_image_digests",
        lambda _ctx, _step_dir: {"loom-control-plane": digest},
    )

    def fake_run(argv, **kwargs):  # type: ignore[no-untyped-def]
        calls.append(list(argv))
        return SubprocessResult(
            argv=list(argv),
            returncode=0,
            stdout=(
                "apiVersion: batch/v1\nkind: Job\n"
                if _is_candidate_invocation(list(argv))
                else "ok\n"
            ),
            stderr="",
        )

    monkeypatch.setattr("loom_cli.rollout.steps.s09_migrate.run_captured", fake_run)

    result = MigrateStep().run(ctx, step_dir)

    assert result.exit_code == 0
    candidate_args = _candidate_args(calls[0])
    assert candidate_args[candidate_args.index("--container-registry") + 1] == (
        "192.168.50.13:5000"
    )
    assert candidate_args[candidate_args.index("--registry-digest") + 1] == digest


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
        "loom_cli.rollout.steps.s10_env_state._candidate_profile_path",
        lambda _ctx, _step_dir: Path(profile),
    )
    monkeypatch.setattr("loom_cli.rollout.steps.s10_env_state.run_captured", fake_run)

    result = EnvStateStep().run(ctx, step_dir)

    assert result.exit_code == 0
    assert len(calls) == 2
    for call in calls:
        _assert_candidate_invocation(call, worktree=worktree)
    assert _candidate_args(calls[0]["argv"])[:3] == [
        "admin",
        "environment-state",
        "apply",
    ]
    assert _candidate_args(calls[1]["argv"])[:3] == [
        "admin",
        "environment-state",
        "check",
    ]
    for call in calls:
        assert call["argv"][call["argv"].index("--environment") + 1] == ctx.environment
        assert call["argv"][call["argv"].index("--admin-token") + 1] == ("env:LOOM_CP_ADMIN_TOKEN")
        assert "--var" in call["argv"]
        assert f"GIT_SHA={ctx.resolved_sha}" in call["argv"]


def test_env_state_passes_pinned_admin_token_source_and_fingerprint(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    envelope = tmp_path / "requests" / "request-a" / "attempts" / "1" / "envelope.json"
    ctx = make_ctx(
        tmp_path,
        admin_token_source="file:/secure/path/staging-admin-token",
        expect_admin_token_fingerprint="sha256:abc123def456 len=64",
        request_envelope_path=envelope,
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
        "loom_cli.rollout.steps.s10_env_state._candidate_profile_path",
        lambda _ctx, _step_dir: Path(profile),
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
        assert argv[argv.index("--rollout-request-envelope") + 1] == str(envelope)
        assert "--rollout-id" not in argv
        assert "--rollout-lock-evidence" not in argv
        assert "--force-rollout-lock" not in argv
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
        "loom_cli.rollout.steps.s10_env_state._candidate_profile_path",
        lambda _ctx, _step_dir: Path(profile),
    )
    monkeypatch.setattr("loom_cli.rollout.steps.s10_env_state.run_captured", fake_run)

    result = EnvStateStep().run(ctx, step_dir)

    assert result.exit_code == 0
    assert len(calls) == 2
    apply_argv, check_argv = calls
    assert _candidate_args(apply_argv)[:3] == ["admin", "environment-state", "apply"]
    assert _candidate_args(check_argv)[:3] == ["admin", "environment-state", "check"]
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
        "loom_cli.rollout.steps.s10_env_state._candidate_profile_path",
        lambda _ctx, _step_dir: Path(source_profile),
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


def test_env_state_replaces_unreadable_operator_owned_profile(
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
    physical_profile.write_text('environment = "operator-owned"\n', encoding="utf-8")
    physical_profile.chmod(0o000)

    def fake_run(argv, **kwargs):
        return SubprocessResult(
            argv=list(argv),
            returncode=0,
            stdout=json.dumps({"ok": True, "drift": []}),
            stderr="",
        )

    monkeypatch.setattr(
        "loom_cli.rollout.steps.s10_env_state._candidate_profile_path",
        lambda _ctx, _step_dir: Path(source_profile),
    )
    monkeypatch.setattr("loom_cli.rollout.steps.s10_env_state.run_captured", fake_run)

    result = EnvStateStep().run(ctx, step_dir)

    assert result.exit_code == 0
    assert physical_profile.read_bytes() == source_profile.read_bytes()
    assert oct(physical_profile.stat().st_mode & 0o777) == "0o600"
    evidence = json.loads(
        step_dir.artifact_path("environment-state-profile-materialization.json").read_text(
            encoding="utf-8",
        ),
    )
    assert evidence["changed"] is True
    assert evidence["existing_profile_readable"] is False
    assert evidence["source_sha256"] == evidence["target_sha256"]


def test_env_state_rejects_symlink_profile_without_mutating_referent(tmp_path: Path) -> None:
    ctx = make_ctx(tmp_path)
    ev = EvidenceDirectory(tmp_path, "test-rid")
    ev.ensure()
    step_dir = ev.step_dir(11, "env-state")
    source_profile = tmp_path / "candidate-staging.toml"
    source_profile.write_text('environment = "staging"\n', encoding="utf-8")
    target = tmp_path / "environment-state" / "staging.toml"
    target.parent.mkdir()
    referent = tmp_path / "outside.toml"
    referent.write_bytes(source_profile.read_bytes())
    referent.chmod(0o644)
    target.symlink_to(referent)

    with pytest.raises(
        candidate_source.CandidateToolingError,
        match="must be a regular file",
    ):
        _materialize_rollout_root_profile(
            ctx,
            source_profile=source_profile,
            step_dir=step_dir,
        )

    assert target.is_symlink()
    assert referent.read_bytes() == source_profile.read_bytes()
    assert oct(referent.stat().st_mode & 0o777) == "0o644"
    assert not step_dir.artifact_path("environment-state-profile-materialization.json").exists()


def test_env_state_rejects_nonregular_profile_without_replacing_it(tmp_path: Path) -> None:
    ctx = make_ctx(tmp_path)
    ev = EvidenceDirectory(tmp_path, "test-rid")
    ev.ensure()
    step_dir = ev.step_dir(11, "env-state")
    source_profile = tmp_path / "candidate-staging.toml"
    source_profile.write_text('environment = "staging"\n', encoding="utf-8")
    target = tmp_path / "environment-state" / "staging.toml"
    target.mkdir(parents=True)
    marker = target / "preserved"
    marker.write_text("do not replace\n", encoding="utf-8")

    with pytest.raises(
        candidate_source.CandidateToolingError,
        match="must be a regular file",
    ):
        _materialize_rollout_root_profile(
            ctx,
            source_profile=source_profile,
            step_dir=step_dir,
        )

    assert target.is_dir()
    assert marker.read_text(encoding="utf-8") == "do not replace\n"
    assert not step_dir.artifact_path("environment-state-profile-materialization.json").exists()


def test_env_state_runs_catalog_provisioning_between_apply_and_check(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    env_file = tmp_path / "catalog.env"
    hf_token = "hf_" + "a" * 32
    db_url = "postgresql://loom:catalog-secret@postgres/loom"
    env_file.write_text(
        f"HF_TOKEN={hf_token}\n"
        "XDG_CACHE_HOME=/data/loom-staging/breakglass/stale/xdg\n"
        "HF_HOME=/data/loom-staging/breakglass/stale/huggingface\n"
        "HF_HUB_CACHE=/data/loom-staging/breakglass/stale/huggingface/hub\n"
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
        if _is_candidate_invocation(list(argv)) and _candidate_args(list(argv))[:3] == [
            "admin",
            "environment-state",
            "apply",
        ]:
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
            cache_root = step_dir.artifact_path("catalog-cache")
            assert kwargs["env"]["XDG_CACHE_HOME"] == str(cache_root / "xdg")
            assert kwargs["env"]["HF_HOME"] == str(cache_root / "huggingface")
            assert kwargs["env"]["HF_HUB_CACHE"] == str(cache_root / "huggingface/hub")
            for path in (
                cache_root,
                cache_root / "xdg",
                cache_root / "huggingface",
                cache_root / "huggingface/hub",
            ):
                assert stat.S_IMODE(path.stat().st_mode) == 0o700
            return SubprocessResult(
                argv=list(argv),
                returncode=0,
                stdout=f"catalog ok {hf_token} {db_url}\n",
                stderr=f"warning access {kwargs['env']['LOOM_SVC_MINIO_ACCESS_KEY']}\n",
            )
        if _is_candidate_invocation(list(argv)) and _candidate_args(list(argv))[:3] == [
            "admin",
            "environment-state",
            "check",
        ]:
            return SubprocessResult(
                argv=list(argv),
                returncode=0,
                stdout='{"ok": true, "drift": [], "autoscaler_blockers": []}\n',
                stderr="",
            )
        raise AssertionError(f"unexpected argv: {argv}")

    monkeypatch.setattr(
        "loom_cli.rollout.steps.s10_env_state._candidate_profile_path",
        lambda _ctx, _step_dir: Path(profile),
    )
    monkeypatch.setattr("loom_cli.rollout.steps.s10_env_state.run_captured", fake_run)

    result = EnvStateStep().run(ctx, step_dir)

    assert result.exit_code == 0
    assert [
        _candidate_args(call["argv"])[:3]
        for call in calls
        if _is_candidate_invocation(call["argv"])
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
    assert evidence["cache"] == {
        "environment_keys": ["HF_HOME", "HF_HUB_CACHE", "XDG_CACHE_HOME"],
        "mode": "0o700",
        "root": str(step_dir.artifact_path("catalog-cache")),
    }
    assert evidence["env_file"]["source_identity"].startswith("sha256:")
    assert str(env_file) not in artifact
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
        if _is_candidate_invocation(list(argv)) and _candidate_args(list(argv))[:3] == [
            "admin",
            "environment-state",
            "apply",
        ]:
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
        if _is_candidate_invocation(list(argv)) and _candidate_args(list(argv))[:3] == [
            "admin",
            "environment-state",
            "check",
        ]:
            return SubprocessResult(
                argv=list(argv),
                returncode=0,
                stdout='{"ok": true, "drift": [], "autoscaler_blockers": []}\n',
                stderr="",
            )
        raise AssertionError(f"unexpected argv: {argv}")

    monkeypatch.setattr(
        "loom_cli.rollout.steps.s10_env_state._candidate_profile_path",
        lambda _ctx, _step_dir: Path(profile),
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
        "loom_cli.rollout.steps.s10_env_state._candidate_profile_path",
        lambda _ctx, _step_dir: Path(profile),
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
                                    "[production/gb10/trt-gb10-1]"
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
        "loom_cli.rollout.steps.s10_env_state._candidate_profile_path",
        lambda _ctx, _step_dir: Path(profile),
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
        if _candidate_args(list(argv))[:3] == ["admin", "rate-cards", "sync-yibuapi"]:
            return SubprocessResult(
                argv=list(argv),
                returncode=0,
                stdout='{"id":"yibuapi-pricing-v1","entry_count":128}\n',
                stderr="",
            )
        if _candidate_args(list(argv))[:2] == ["providers", "update"]:
            return SubprocessResult(
                argv=list(argv),
                returncode=0,
                stdout="updated\n",
                stderr="",
            )
        if _candidate_args(list(argv))[:2] == ["providers", "show"]:
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
    assert _candidate_args(calls[0]["argv"]) == [
        "admin",
        "rate-cards",
        "sync-yibuapi",
        "--group",
        "default",
        "--format",
        "json",
    ]
    assert _candidate_args(calls[1]["argv"]) == [
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
    assert _candidate_args(calls[2]["argv"]) == [
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
    assert _candidate_args(seen["argv"])[:3] == ["cluster", "audit", "--config"]


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
    assert _candidate_args(seen["argv"])[:3] == ["cluster", "backup", "check"]


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


def test_rollout_cluster_config_uses_candidate_profile_when_runner_is_stale(
    tmp_path: Path,
) -> None:
    """Protected rollout config follows the resolved candidate, not the runner."""
    runner_root = tmp_path / "runner"
    runner_config = runner_root / "deploy" / "environments" / "staging.cluster.toml"
    runner_config.parent.mkdir(parents=True)
    runner_config.write_text(
        'image_tag = "staging-old"\nnamespace = "loom-staging"\n',
        encoding="utf-8",
    )
    subprocess.run(["git", "init", "--quiet", str(runner_root)], check=True)

    ctx = replace(
        make_ctx(tmp_path, image_tag="staging-new"),
        cluster_config_path=runner_config,
    )
    ev = EvidenceDirectory(tmp_path, "test-rid")
    ev.ensure()
    worktree = _prepare_candidate_worktree(ev)
    candidate_config = worktree / "deploy" / "environments" / "staging.cluster.toml"
    candidate_config.parent.mkdir(parents=True)
    candidate_config.write_text(
        'image_tag = "staging-candidate"\n'
        'namespace = "loom-staging"\n'
        "\n"
        "[workload_contract]\n"
        'workload_trust_mode = "internal_trusted"\n'
        "taskset_transforms_enabled = false\n"
        "taskset_transform_network_isolated = false\n"
        "untrusted_workload_isolation = false\n",
        encoding="utf-8",
    )

    rendered_config = rollout_cluster_config(ctx, ev.step_dir(8, "preflight"))
    rendered_raw = tomllib.loads(rendered_config.read_text(encoding="utf-8"))

    assert rendered_raw["image_tag"] == "staging-new"
    assert rendered_raw["workload_contract"] == {
        "workload_trust_mode": "internal_trusted",
        "taskset_transforms_enabled": False,
        "taskset_transform_network_isolated": False,
        "untrusted_workload_isolation": False,
    }


def test_rollout_cluster_config_rewrites_repo_relative_gb10_paths_to_candidate(
    tmp_path: Path,
) -> None:
    ctx = make_ctx(tmp_path, image_tag="staging-new")
    source_config = _REPO_ROOT / "deploy" / "environments" / "staging.cluster.toml"
    ctx = replace(ctx, cluster_config_path=source_config)
    ev = EvidenceDirectory(tmp_path, "test-rid")
    ev.ensure()
    worktree = _prepare_candidate_worktree(ev)
    candidate_ssh_config = worktree / "deploy" / "worker-pools" / "gb10" / "ssh_config"
    candidate_ssh_config.parent.mkdir(parents=True)
    candidate_ssh_config.write_text(
        "Host trt-gb10-1\n  HostName 203.0.113.1\n",
        encoding="utf-8",
    )

    rendered_config = rollout_cluster_config(ctx, ev.step_dir(14, "release-gate"))
    rendered_raw = tomllib.loads(rendered_config.read_text(encoding="utf-8"))

    assert Path(rendered_raw["gb10_pool"]["ssh_config"]) == candidate_ssh_config


def test_rollout_cluster_config_migrates_existing_relative_gb10_paths_on_resume(
    tmp_path: Path,
) -> None:
    ctx = make_ctx(tmp_path, image_tag="staging-new")
    source_config = _REPO_ROOT / "deploy" / "environments" / "staging.cluster.toml"
    ctx = replace(ctx, cluster_config_path=source_config)
    ev = EvidenceDirectory(tmp_path, "test-rid")
    ev.ensure()
    worktree = _prepare_candidate_worktree(ev)
    candidate_ssh_config = worktree / "deploy" / "worker-pools" / "gb10" / "ssh_config"
    candidate_ssh_config.parent.mkdir(parents=True)
    candidate_ssh_config.write_text(
        "Host trt-gb10-1\n  HostName 203.0.113.1\n",
        encoding="utf-8",
    )
    existing_config = ev.path / "rollout-cluster-config.toml"
    existing_config.write_text(
        'image_tag = "staging-new"\n'
        'namespace = "loom-staging"\n'
        "\n"
        "[gb10_pool]\n"
        'ssh_config = "../worker-pools/gb10/ssh_config"\n',
        encoding="utf-8",
    )

    rendered_config = rollout_cluster_config(ctx, ev.step_dir(14, "release-gate"))
    rendered_raw = tomllib.loads(rendered_config.read_text(encoding="utf-8"))

    assert rendered_config == existing_config
    assert rendered_raw["namespace"] == "loom-staging"
    assert Path(rendered_raw["gb10_pool"]["ssh_config"]) == candidate_ssh_config


def _gb10_candidate_config_fixture(
    tmp_path: Path,
) -> tuple[RolloutContext, EvidenceDirectory, Path, Path]:
    runner_root = tmp_path / "runner"
    runner_config = runner_root / "deploy" / "environments" / "staging.cluster.toml"
    runner_config.parent.mkdir(parents=True)
    subprocess.run(["git", "init", "--quiet", str(runner_root)], check=True)
    ctx = replace(make_ctx(tmp_path), cluster_config_path=runner_config)
    ev = EvidenceDirectory(tmp_path, "test-rid")
    ev.ensure()
    worktree = _prepare_candidate_worktree(ev)
    candidate_config = worktree / "deploy" / "environments" / "staging.cluster.toml"
    candidate_config.parent.mkdir(parents=True)
    return ctx, ev, runner_config, candidate_config


def test_gb10_prep_uses_candidate_hosts_and_ssh_when_runner_is_stale(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ctx, ev, runner_config, candidate_config = _gb10_candidate_config_fixture(tmp_path)
    identity = _write_dummy_identity(tmp_path / "gb10-rollout-ed25519")
    runner_ssh = tmp_path / "runner-ssh-config"
    runner_ssh.write_text("Host stale-runner\n", encoding="utf-8")
    runner_config.write_text(
        "[gb10_pool]\n"
        f'ssh_config = "{runner_ssh}"\n'
        f'ssh_identity_file = "{identity}"\n'
        "hosts = [\n"
        '  { ssh_target = "stale-runner", repo_path = "/srv/stale", '
        'env_file_path = "/srv/stale/.env" },\n'
        "]\n",
        encoding="utf-8",
    )
    candidate_ssh = candidate_config.parent.parent / "worker-pools" / "gb10" / "ssh_config"
    candidate_ssh.parent.mkdir(parents=True)
    candidate_ssh.write_text("Host candidate-host\n", encoding="utf-8")
    candidate_config.write_text(
        "[gb10_pool]\n"
        'ssh_config = "../worker-pools/gb10/ssh_config"\n'
        f'ssh_identity_file = "{identity}"\n'
        "hosts = [\n"
        '  { ssh_target = "candidate-host", repo_path = "/srv/candidate", '
        'env_file_path = "/srv/candidate/.env" },\n'
        "]\n",
        encoding="utf-8",
    )
    seen: list[GB10Host] = []

    def fake_prep(
        _ctx: RolloutContext,
        host: GB10Host,
        _host_dir: Path,
    ) -> tuple[bool, str]:
        seen.append(host)
        return True, f"prepped {host.ssh_target}"

    monkeypatch.setattr("loom_cli.rollout.steps.s04_gb10_prep._prep_one_host", fake_prep)

    result = GB10PrepStep().run(ctx, ev.step_dir(12, "gb10-prep"))

    assert result.exit_code == 0
    assert [host.ssh_target for host in seen] == ["candidate-host"]
    assert seen[0].repo_path == "/srv/candidate"
    assert seen[0].ssh_config_path == str(candidate_ssh)


def test_gb10_prep_fails_closed_when_candidate_config_is_missing(tmp_path: Path) -> None:
    ctx, ev, runner_config, _candidate_config = _gb10_candidate_config_fixture(tmp_path)
    runner_config.write_text("image_tag = 'stale-runner'\n", encoding="utf-8")

    result = GB10PrepStep().run(ctx, ev.step_dir(12, "gb10-prep"))

    assert result.exit_code == 2
    assert "candidate cluster config" in (result.error or "")


def test_gb10_prep_fails_closed_when_candidate_config_is_symlink(tmp_path: Path) -> None:
    ctx, ev, runner_config, candidate_config = _gb10_candidate_config_fixture(tmp_path)
    runner_config.write_text("image_tag = 'stale-runner'\n", encoding="utf-8")
    candidate_config.symlink_to(runner_config)

    result = GB10PrepStep().run(ctx, ev.step_dir(12, "gb10-prep"))

    assert result.exit_code == 2
    assert "candidate cluster config" in (result.error or "")
    assert "symlink" in (result.error or "")


def test_gb10_prep_fails_closed_when_candidate_config_mapping_escapes(
    tmp_path: Path,
) -> None:
    ctx = replace(make_ctx(tmp_path), cluster_config_path=Path("../escape.cluster.toml"))
    ev = EvidenceDirectory(tmp_path, "test-rid")
    ev.ensure()
    _prepare_candidate_worktree(ev)

    result = GB10PrepStep().run(ctx, ev.step_dir(12, "gb10-prep"))

    assert result.exit_code == 2
    assert "candidate cluster config" in (result.error or "")
    assert "outside" in (result.error or "")


def test_gb10_prep_fails_closed_when_candidate_ssh_config_is_missing(
    tmp_path: Path,
) -> None:
    ctx, ev, runner_config, candidate_config = _gb10_candidate_config_fixture(tmp_path)
    runner_config.write_text("image_tag = 'stale-runner'\n", encoding="utf-8")
    candidate_config.write_text(
        '[gb10_pool]\nssh_config = "../worker-pools/gb10/missing-ssh-config"\n',
        encoding="utf-8",
    )

    result = GB10PrepStep().run(ctx, ev.step_dir(12, "gb10-prep"))

    assert result.exit_code == 2
    assert "candidate GB10 SSH config" in (result.error or "")
    assert "unavailable" in (result.error or "")


def test_gb10_prep_fails_closed_when_candidate_profile_is_symlink(
    tmp_path: Path,
) -> None:
    ctx, ev, runner_config, candidate_config = _gb10_candidate_config_fixture(tmp_path)
    runner_config.write_text("image_tag = 'stale-runner'\n", encoding="utf-8")
    stale_profile = tmp_path / "stale-runner-profile.toml"
    stale_profile.write_text('environment = "staging"\n', encoding="utf-8")
    candidate_profile = candidate_config.parent.parent / "environment-state" / "staging.toml"
    candidate_profile.parent.mkdir(parents=True)
    candidate_profile.symlink_to(stale_profile)
    candidate_config.write_text(
        'env_state_profile = "../environment-state/staging.toml"\n',
        encoding="utf-8",
    )

    result = GB10PrepStep().run(ctx, ev.step_dir(12, "gb10-prep"))

    assert result.exit_code == 2
    assert "candidate environment-state profile" in (result.error or "")
    assert "symlink" in (result.error or "")


def test_gb10_prep_fails_closed_when_materialized_config_is_symlink(
    tmp_path: Path,
) -> None:
    ctx, ev, runner_config, candidate_config = _gb10_candidate_config_fixture(tmp_path)
    runner_config.write_text("image_tag = 'stale-runner'\n", encoding="utf-8")
    candidate_config.write_text("image_tag = 'candidate'\n", encoding="utf-8")
    (ev.path / "rollout-cluster-config.toml").symlink_to(runner_config)

    result = GB10PrepStep().run(ctx, ev.step_dir(12, "gb10-prep"))

    assert result.exit_code == 2
    assert "rollout-local cluster config" in (result.error or "")
    assert "symlink" in (result.error or "")


def test_gb10_prep_uses_candidate_profile_for_missing_host_failure(
    tmp_path: Path,
) -> None:
    ctx, ev, runner_config, candidate_config = _gb10_candidate_config_fixture(tmp_path)
    runner_config.write_text("image_tag = 'stale-runner'\n", encoding="utf-8")
    candidate_profile = candidate_config.parent.parent / "environment-state" / "staging.toml"
    candidate_profile.parent.mkdir(parents=True)
    candidate_profile.write_text(
        """
environment = "staging"

[[gb10_worker_pool_desired_states]]
pool_name = "gb10"
image_tag = "${IMAGE_TAG}"
max_concurrent = 1
env_config_version = "${ENV_CONFIG_VERSION}"
source_git_commit = "${GIT_SHA}"
target_slots = 1

[gb10_worker_pool_desired_states.host_intents]
candidate-host = "active"
""".strip()
        + "\n",
        encoding="utf-8",
    )
    candidate_config.write_text(
        'env_state_profile = "../environment-state/staging.toml"\n',
        encoding="utf-8",
    )

    result = GB10PrepStep().run(ctx, ev.step_dir(12, "gb10-prep"))

    assert result.exit_code == 1
    assert "declares GB10 desired state" in (result.error or "")


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

    def fake_run(argv, *, stdin_text=None):
        captured["argv"] = list(argv)
        captured["stdin_text"] = stdin_text
        return SubprocessResult(
            argv=list(argv),
            returncode=0,
            stdout="",
            stderr="",
        )

    monkeypatch.setattr("loom_cli.rollout.steps.s04_gb10_prep.run_captured", fake_run)

    _ssh(host, "hostname >/dev/null", stdin_text="trusted verifier\n")

    argv = captured["argv"]
    assert argv[:3] == ["/usr/bin/ssh", "-F", str(ssh_config)]
    assert "-i" in argv
    assert str(identity) in argv
    assert "IdentitiesOnly=yes" in argv
    assert f"CertificateFile={cert}" in argv
    assert "-o" in argv
    assert "BatchMode=yes" in argv
    assert "StrictHostKeyChecking=yes" in argv
    assert "UserKnownHostsFile=/etc/loom/staging-rollout-gb10-known-hosts" in argv
    assert "GlobalKnownHostsFile=/dev/null" in argv
    assert "UpdateHostKeys=no" in argv
    assert argv[-2:] == ["trt-gb10-1", "hostname >/dev/null"]
    assert captured["stdin_text"] == "trusted verifier\n"


@pytest.fixture
def _runner_backed_gb10_config(monkeypatch: pytest.MonkeyPatch) -> None:
    """Keep non-candidate unit cases focused on their original GB10 behavior."""
    monkeypatch.setattr(
        "loom_cli.rollout.steps.s04_gb10_prep._gb10_prep_config_paths",
        lambda ctx, _step_dir: (ctx.cluster_config_path, ctx.cluster_config_path),
    )


def test_gb10_prep_fails_when_current_gb10_profile_has_no_hosts(
    tmp_path: Path,
    _runner_backed_gb10_config: None,
) -> None:
    ctx = make_ctx(tmp_path, image_tag="staging-abc123")
    state_dir = tmp_path / "environment-state"
    state_dir.mkdir()
    (state_dir / "staging.toml").write_text(
        """
environment = "staging"

[[gb10_worker_pool_desired_states]]
pool_name = "gb10"
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


def test_gb10_prep_skips_direct_host_delivery_for_external_slurm_pool(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from loom_cli.rollout.steps import s04_gb10_prep

    ctx = make_ctx(tmp_path, image_tag="staging-abc123")
    monkeypatch.setattr(s04_gb10_prep, "_gb10_desired_state_declared", lambda *a, **k: True)
    monkeypatch.setattr(
        s04_gb10_prep,
        "_gb10_external_autoscaler_declared",
        lambda *a, **k: True,
    )

    assert s04_gb10_prep._no_gb10_hosts_error(ctx) is None


def test_gb10_prep_requires_platform_dev_identity_file(
    tmp_path: Path,
    _runner_backed_gb10_config: None,
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
    _runner_backed_gb10_config: None,
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
    _runner_backed_gb10_config: None,
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
    _runner_backed_gb10_config: None,
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
    _runner_backed_gb10_config: None,
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
                    "LoadState=loaded\n"
                    "Type=oneshot\n"
                    "Result=failed\n"
                    "ExecMainStatus=1\n"
                    "ActiveState=failed\n"
                    "SubState=failed\n"
                    "NeedDaemonReload=no\n"
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
    assert calls[:3] == [
        'test "$(cd /srv/loom-staging && git rev-parse HEAD)" = dddddddddddddddddddddddddddddddddddddddd',
        "grep -q '^LOOM_IMAGE_TAG=staging-abc123$' /srv/loom-staging/.env",
        "grep -q '^LOOM_WORKER_ENV_CONFIG_VERSION=staging-abc123$' /srv/loom-staging/.env",
    ]
    assert "git cat-file -e" in calls[3]
    assert "git diff --quiet" in calls[3]
    assert "loginctl show-user" in calls[4]
    assert calls[5].startswith(
        "cmp -s /srv/loom-staging/deploy/worker-pools/gb10/loom-gb10-node-agent.service"
    )
    assert calls[6].startswith(
        "cmp -s /srv/loom-staging/deploy/worker-pools/gb10/loom-gb10-node-agent.timer"
    )
    assert calls[7:] == [
        'test ! -e "$HOME/.config/systemd/user/'
        'loom-gb10-node-agent.timer.d"',
        "systemctl --user is-enabled loom-gb10-worker.service",
        "systemctl --user show loom-gb10-worker.service -p ActiveState -p SubState",
        "systemctl --user show loom-gb10-node-agent.service -p LoadState -p Type -p Result -p ExecMainStatus -p ActiveState -p SubState -p NeedDaemonReload",
    ]


def test_gb10_prep_verify_accepts_successful_oneshot_node_agent(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    _runner_backed_gb10_config: None,
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
        if "systemctl --user show loom-gb10-node-agent.timer" in remote_cmd:
            return SubprocessResult(
                argv=["ssh", host.ssh_target, remote_cmd],
                returncode=0,
                stdout=(
                    "LoadState=loaded\n"
                    "ActiveState=active\n"
                    "SubState=waiting\n"
                    "Unit=loom-gb10-node-agent.service\n"
                    "NeedDaemonReload=no\n"
                ),
                stderr="",
            )
        if "systemctl --user show loom-gb10-node-agent.service" in remote_cmd:
            return SubprocessResult(
                argv=["ssh", host.ssh_target, remote_cmd],
                returncode=0,
                stdout=(
                    "LoadState=loaded\n"
                    "Type=oneshot\n"
                    "Result=success\n"
                    "ExecMainStatus=0\n"
                    "ActiveState=inactive\n"
                    "SubState=dead\n"
                    "NeedDaemonReload=no\n"
                ),
                stderr="",
            )
        if "systemctl --user is-enabled loom-gb10-node-agent.timer" in remote_cmd:
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

    outcome = GB10PrepStep().verify(ctx, ev.step_dir(12, "gb10-prep"))

    assert outcome is VerifyOutcome.MATCH
    assert any("systemctl --user show" in call for call in calls)


def test_gb10_prep_verify_waits_for_transient_timer_run(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    _runner_backed_gb10_config: None,
) -> None:
    ctx = make_ctx(tmp_path, image_tag="staging-abc123", resolved_sha="e" * 40)
    _write_single_node_agent_gb10_config(ctx, tmp_path)
    ev = EvidenceDirectory(tmp_path, "test-rid")
    ev.ensure()
    timer_calls = 0
    service_calls = 0
    sleeps: list[float] = []

    def fake_ssh(host, remote_cmd):
        nonlocal timer_calls, service_calls
        if "systemctl --user show loom-gb10-node-agent.timer" in remote_cmd:
            timer_calls += 1
            substate = "running" if timer_calls == 1 else "waiting"
            stdout = (
                "LoadState=loaded\n"
                "ActiveState=active\n"
                f"SubState={substate}\n"
                "Unit=loom-gb10-node-agent.service\n"
                "NeedDaemonReload=no\n"
            )
        elif "systemctl --user show loom-gb10-node-agent.service" in remote_cmd:
            service_calls += 1
            stdout = (
                "LoadState=loaded\n"
                "Type=oneshot\n"
                "Result=success\n"
                "ExecMainStatus=0\n"
                f"ActiveState={'active' if service_calls == 1 else 'inactive'}\n"
                f"SubState={'running' if service_calls == 1 else 'dead'}\n"
                "NeedDaemonReload=no\n"
            )
        elif "systemctl --user is-enabled loom-gb10-node-agent.timer" in remote_cmd:
            stdout = "enabled\n"
        else:
            stdout = ""
        return SubprocessResult(
            argv=["ssh", host.ssh_target, remote_cmd],
            returncode=0,
            stdout=stdout,
            stderr="",
        )

    monkeypatch.setattr("loom_cli.rollout.steps.s04_gb10_prep._ssh", fake_ssh)
    monkeypatch.setattr(
        "loom_cli.rollout.steps.s04_gb10_prep.time.sleep",
        sleeps.append,
    )

    outcome = GB10PrepStep().verify(ctx, ev.step_dir(12, "gb10-prep"))

    assert outcome is VerifyOutcome.MATCH
    assert timer_calls == 2
    assert service_calls == 2
    assert sleeps == [2.0]


def test_gb10_prep_verify_fails_closed_when_timer_never_settles(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    _runner_backed_gb10_config: None,
) -> None:
    ctx = make_ctx(tmp_path, image_tag="staging-abc123", resolved_sha="e" * 40)
    _write_single_node_agent_gb10_config(ctx, tmp_path)
    ev = EvidenceDirectory(tmp_path, "test-rid")
    ev.ensure()
    timer_calls = 0
    sleeps: list[float] = []

    def fake_ssh(host, remote_cmd):
        nonlocal timer_calls
        if "systemctl --user show loom-gb10-node-agent.timer" in remote_cmd:
            timer_calls += 1
            stdout = (
                "LoadState=loaded\n"
                "ActiveState=active\n"
                "SubState=running\n"
                "Unit=loom-gb10-node-agent.service\n"
                "NeedDaemonReload=no\n"
            )
        elif "systemctl --user show loom-gb10-node-agent.service" in remote_cmd:
            stdout = (
                "LoadState=loaded\n"
                "Type=oneshot\n"
                "Result=success\n"
                "ExecMainStatus=0\n"
                "ActiveState=active\n"
                "SubState=running\n"
                "NeedDaemonReload=no\n"
            )
        elif "systemctl --user is-enabled loom-gb10-node-agent.timer" in remote_cmd:
            stdout = "enabled\n"
        else:
            stdout = ""
        return SubprocessResult(
            argv=["ssh", host.ssh_target, remote_cmd],
            returncode=0,
            stdout=stdout,
            stderr="",
        )

    monkeypatch.setattr("loom_cli.rollout.steps.s04_gb10_prep._ssh", fake_ssh)
    monkeypatch.setattr(
        "loom_cli.rollout.steps.s04_gb10_prep.time.sleep",
        sleeps.append,
    )
    step_dir = ev.step_dir(12, "gb10-prep")

    outcome = GB10PrepStep().verify(ctx, step_dir)

    assert outcome is VerifyOutcome.MISMATCH
    assert timer_calls == 16
    assert sleeps == [2.0] * 15
    assert "SubState=running" in step_dir.stderr_path().read_text()


@pytest.mark.parametrize(
    "service",
    (
        "../loom-gb10-node-agent.service",
        "/tmp/loom-gb10-node-agent.service",
        "loom gb10.service",
        "loom-gb10-node-agent.timer",
    ),
)
def test_gb10_prep_rejects_unsafe_node_agent_service_names(service: str) -> None:
    with pytest.raises(
        candidate_source.CandidateToolingError,
        match=r"simple \.service basename",
    ):
        _node_agent_timer_name(service)


def test_gb10_prep_refuses_dirty_candidate_units_before_unit_mutation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    _runner_backed_gb10_config: None,
) -> None:
    ctx = make_ctx(tmp_path, resolved_sha="a" * 40)
    _write_single_node_agent_gb10_config(ctx, tmp_path)
    ev = EvidenceDirectory(tmp_path, "test-rid")
    ev.ensure()
    calls: list[str] = []

    def fake_ssh(host, remote_cmd):
        calls.append(remote_cmd)
        return SubprocessResult(
            argv=["ssh", host.ssh_target, remote_cmd],
            returncode=1 if "git diff --quiet" in remote_cmd else 0,
            stdout="",
            stderr="dirty candidate unit" if "git diff --quiet" in remote_cmd else "",
        )

    monkeypatch.setattr("loom_cli.rollout.steps.s04_gb10_prep._ssh", fake_ssh)
    step = GB10PrepStep()
    step.max_retries = 1
    step_dir = ev.step_dir(12, "gb10-prep")

    result = step.run(ctx, step_dir)

    assert result.exit_code == 1
    assert result.error is not None
    assert "verify-node-agent-unit-source failed" in step_dir.stdout_path().read_text()
    clean_check = next(call for call in calls if "git diff --quiet" in call)
    assert "git cat-file -e" in clean_check
    assert f"{'a' * 40}:deploy/worker-pools/gb10/loom-gb10-node-agent.service" in clean_check
    assert f"{'a' * 40}:deploy/worker-pools/gb10/loom-gb10-node-agent.timer" in clean_check
    assert not any("install -D" in call for call in calls)
    assert not any("systemctl --user" in call for call in calls)


def test_gb10_prep_verify_rejects_dirty_candidate_units_at_same_sha(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    _runner_backed_gb10_config: None,
) -> None:
    ctx = make_ctx(tmp_path, resolved_sha="b" * 40)
    _write_single_node_agent_gb10_config(ctx, tmp_path)
    ev = EvidenceDirectory(tmp_path, "test-rid")
    ev.ensure()
    calls: list[str] = []

    def fake_ssh(host, remote_cmd):
        calls.append(remote_cmd)
        return SubprocessResult(
            argv=["ssh", host.ssh_target, remote_cmd],
            returncode=1 if "git diff --quiet" in remote_cmd else 0,
            stdout="",
            stderr="",
        )

    monkeypatch.setattr("loom_cli.rollout.steps.s04_gb10_prep._ssh", fake_ssh)
    step_dir = ev.step_dir(12, "gb10-prep")

    outcome = GB10PrepStep().verify(ctx, step_dir)

    assert outcome is VerifyOutcome.MISMATCH
    assert "node-agent-unit-source mismatch" in step_dir.stderr_path().read_text()
    assert any(f"git diff --quiet {'b' * 40}" in call for call in calls)
    assert not any("cmp -s" in call for call in calls)


def test_gb10_prep_verify_rejects_service_only_partial_install(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    _runner_backed_gb10_config: None,
) -> None:
    ctx = make_ctx(tmp_path, resolved_sha="c" * 40)
    _write_single_node_agent_gb10_config(ctx, tmp_path)
    ev = EvidenceDirectory(tmp_path, "test-rid")
    ev.ensure()
    calls: list[str] = []

    def fake_ssh(host, remote_cmd):
        calls.append(remote_cmd)
        timer_missing = (
            remote_cmd.startswith("cmp -s ") and "loom-gb10-node-agent.timer" in remote_cmd
        )
        return SubprocessResult(
            argv=["ssh", host.ssh_target, remote_cmd],
            returncode=1 if timer_missing else 0,
            stdout="",
            stderr="",
        )

    monkeypatch.setattr("loom_cli.rollout.steps.s04_gb10_prep._ssh", fake_ssh)
    step_dir = ev.step_dir(12, "gb10-prep")

    outcome = GB10PrepStep().verify(ctx, step_dir)

    assert outcome is VerifyOutcome.MISMATCH
    assert "node-agent-timer-unit mismatch" in step_dir.stderr_path().read_text()
    assert any("loom-gb10-node-agent.service" in call for call in calls if "cmp -s" in call)
    assert not any("systemctl --user" in call for call in calls)


def test_gb10_prep_done_resume_requires_fresh_live_verification(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ctx = make_ctx(tmp_path)
    ev = EvidenceDirectory(tmp_path, "test-rid")
    ev.ensure()
    step = GB10PrepStep()
    seen: list[tuple[RolloutContext, object]] = []

    def fake_verify(ctx_arg, step_dir_arg):
        seen.append((ctx_arg, step_dir_arg))
        return VerifyOutcome.MISMATCH

    monkeypatch.setattr(step, "_verify_impl", fake_verify)
    step_dir = ev.step_dir(12, "gb10-prep")

    assert step.requires_strict_live_verification()
    assert step.verify_done(ctx, step_dir) is VerifyOutcome.MISMATCH
    assert seen == [(ctx, step_dir)]


def test_gb10_prep_verify_retries_when_timer_needs_daemon_reload(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    _runner_backed_gb10_config: None,
) -> None:
    ctx = make_ctx(
        tmp_path,
        image_tag="staging-abc123",
        resolved_sha="f" * 40,
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

    def fake_ssh(host, remote_cmd):
        if "systemctl --user show loom-gb10-node-agent.timer" in remote_cmd:
            stdout = (
                "LoadState=loaded\n"
                "ActiveState=active\n"
                "SubState=waiting\n"
                "Unit=loom-gb10-node-agent.service\n"
                "NeedDaemonReload=yes\n"
            )
        elif "systemctl --user show loom-gb10-node-agent.service" in remote_cmd:
            stdout = (
                "LoadState=loaded\n"
                "Type=oneshot\n"
                "Result=success\n"
                "ExecMainStatus=0\n"
                "ActiveState=inactive\n"
                "SubState=dead\n"
                "NeedDaemonReload=no\n"
            )
        elif "systemctl --user is-enabled loom-gb10-node-agent.timer" in remote_cmd:
            stdout = "enabled\n"
        else:
            stdout = ""
        return SubprocessResult(
            argv=["ssh", host.ssh_target, remote_cmd],
            returncode=0,
            stdout=stdout,
            stderr="",
        )

    monkeypatch.setattr("loom_cli.rollout.steps.s04_gb10_prep._ssh", fake_ssh)
    step_dir = ev.step_dir(12, "gb10-prep")

    outcome = GB10PrepStep().verify(ctx, step_dir)

    assert outcome is VerifyOutcome.MISMATCH
    assert "NeedDaemonReload=yes" in step_dir.stderr_path().read_text()


def test_gb10_prep_verify_retries_when_legacy_worker_unit_is_enabled(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    _runner_backed_gb10_config: None,
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
    _runner_backed_gb10_config: None,
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
    source_clean_idx = next(idx for idx, call in enumerate(calls) if "git diff --quiet" in call)
    linger_idx = next(idx for idx, call in enumerate(calls) if "loginctl show-user" in call)
    service_install_idx = next(
        idx
        for idx, call in enumerate(calls)
        if "install -D -m 0644" in call and call.endswith('node-agent.service"')
    )
    timer_install_idx = next(
        idx
        for idx, call in enumerate(calls)
        if "install -D -m 0644" in call and call.endswith('node-agent.timer"')
    )
    daemon_reload_idx = calls.index("systemctl --user daemon-reload")
    node_agent_idx = calls.index("systemctl --user start loom-gb10-node-agent.service")
    timer_enable_idx = calls.index("systemctl --user enable --now loom-gb10-node-agent.timer")
    timer_restart_idx = calls.index("systemctl --user restart loom-gb10-node-agent.timer")
    assert (
        source_clean_idx
        < linger_idx
        < service_install_idx
        < timer_install_idx
        < legacy_idx
        < daemon_reload_idx
        < node_agent_idx
        < timer_enable_idx
        < timer_restart_idx
    )
    assert calls[-1] == "systemctl --user restart loom-gb10-node-agent.timer"


def test_gb10_prep_runs_hosts_with_bounded_parallelism(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    _runner_backed_gb10_config: None,
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
    _runner_backed_gb10_config: None,
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
    host1_started = threading.Event()
    other_host_progressed = threading.Event()

    def fake_prep_one_host(ctx, host, host_dir):
        nonlocal host1_active
        if host.ssh_target == "trt-gb10-1":
            with lock:
                host1_active = True
            host1_started.set()
            assert other_host_progressed.wait(timeout=1.0)
            (host_dir / "prep.log").write_text("failed\n", encoding="utf-8")
            with lock:
                host1_active = False
            return False, "checkout failed on trt-gb10-1: rc=1"
        assert host1_started.wait(timeout=1.0)
        with lock:
            if host1_active:
                progressed_while_host1_active.add(host.ssh_target)
        other_host_progressed.set()
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
    _runner_backed_gb10_config: None,
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
                "timeout_sec": kwargs.get("timeout_sec"),
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
        if _candidate_args(argv)[:3] == ["admin", "environment-state", "check"]:
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
    assert _candidate_args(calls[1]["argv"])[:2] == ["cluster", "release-manifest"]
    assert _candidate_args(calls[2]["argv"])[:2] == [
        "cluster",
        "minio-storage-preflight",
    ]
    assert calls[2]["argv"][calls[2]["argv"].index("--namespace") + 1] == ctx.namespace
    assert _candidate_args(calls[3]["argv"])[:3] == [
        "admin",
        "gb10-workers",
        "status",
    ]
    assert calls[3]["argv"][calls[3]["argv"].index("--environment") + 1] == "production"
    assert _candidate_args(calls[4]["argv"])[:3] == [
        "admin",
        "environment-state",
        "check",
    ]
    assert calls[3]["timeout_sec"] == 180.0
    assert calls[4]["timeout_sec"] == 180.0
    assert _candidate_args(calls[5]["argv"])[:2] == [
        "datasets",
        "hf-boundary-evidence",
    ]
    assert _candidate_args(calls[6]["argv"])[:2] == ["cluster", "release-gate"]
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
    assert calls[5]["argv"][calls[5]["argv"].index("--canary-batch-id") + 1] == (
        "00000000-0000-4000-8000-000000000001"
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


def test_release_gate_binds_pinned_registry_reference_to_local_candidate_image(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ctx = make_ctx(tmp_path, image_tag="staging-abc123")
    ev = EvidenceDirectory(tmp_path, "test-rid")
    ev.ensure()
    _prepare_candidate_worktree(ev)
    digest = "sha256:" + "2" * 64
    rendered = ev.step_dir(7, "render").artifact_path("rendered.yaml")
    rendered.write_text(
        f"""apiVersion: apps/v1
kind: Deployment
metadata:
  name: loom-service
spec:
  template:
    spec:
      containers:
        - name: loom-service
          image: 192.168.50.13:5000/loom-service@{digest}
""",
        encoding="utf-8",
    )
    monkeypatch.setattr(
        "loom_cli.rollout.steps.s12_release_gate.rollout_images",
        lambda _ctx, _step_dir: (("loom-service", "Dockerfile.service", "."),),
    )
    monkeypatch.setattr(
        "loom_cli.rollout.steps.s12_release_gate._registry_publication",
        lambda _ctx: ("192.168.50.13:5000", "localhost:5000"),
    )
    monkeypatch.setattr(
        "loom_cli.rollout.steps.s12_release_gate.registry_image_digests",
        lambda _ctx, _step_dir: {"loom-service": digest},
    )
    inspected: list[str] = []

    def fake_run(argv, **kwargs):  # type: ignore[no-untyped-def]
        inspected.extend(argv[3:])
        return SubprocessResult(
            argv=list(argv),
            returncode=0,
            stdout=json.dumps(
                [
                    {
                        "Id": "sha256:" + "1" * 64,
                        "RepoTags": ["loom-service:staging-abc123"],
                        "RepoDigests": [],
                    }
                ]
            ),
            stderr="",
        )

    monkeypatch.setattr("loom_cli.rollout.steps.s12_release_gate.run_captured", fake_run)

    output = ReleaseGateStep()._write_expected_image_identities(
        ctx,
        ev.step_dir(14, "release-gate"),
    )

    assert inspected == ["loom-service:staging-abc123"]
    identity = json.loads(output.read_text(encoding="utf-8"))["loom-service"][
        "loom-service"
    ]
    assert identity == {
        "image": f"192.168.50.13:5000/loom-service@{digest}",
        "image_id": "sha256:" + "1" * 64,
        "repo_digest": f"192.168.50.13:5000/loom-service@{digest}",
    }


def test_release_gate_rejects_rendered_image_absent_from_candidate_matrix(
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
  name: rendered-service
spec:
  template:
    spec:
      containers:
        - name: rendered-service
          image: loom-agent-sandbox:staging-abc123
""",
        encoding="utf-8",
    )
    monkeypatch.setattr(
        "loom_cli.rollout.steps.s12_release_gate.run_captured",
        lambda *_args, **_kwargs: pytest.fail("docker inspect must not run"),
    )

    with pytest.raises(
        ValueError,
        match="contains images absent from the candidate rollout matrix",
    ):
        ReleaseGateStep()._write_expected_image_identities(
            ctx,
            ev.step_dir(14, "release-gate"),
        )


def test_release_gate_rejects_render_without_release_managed_images(
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
  name: envoy
spec:
  template:
    spec:
      containers:
        - name: envoy
          image: envoyproxy/envoy:v1.30-latest
""",
        encoding="utf-8",
    )
    monkeypatch.setattr(
        "loom_cli.rollout.steps.s12_release_gate.run_captured",
        lambda *_args, **_kwargs: pytest.fail("docker inspect must not run"),
    )

    with pytest.raises(ValueError, match="does not reference any release-managed images"):
        ReleaseGateStep()._write_expected_image_identities(
            ctx,
            ev.step_dir(14, "release-gate"),
        )


def test_release_gate_allows_profile_disabled_candidate_images(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ctx = make_ctx(tmp_path, image_tag="staging-abc123")
    ev = EvidenceDirectory(tmp_path, "test-rid")
    ev.ensure()
    _prepare_candidate_worktree(ev)
    _write_rendered_service(ev)
    monkeypatch.setattr(
        "loom_cli.rollout.steps.s12_release_gate.rollout_images",
        lambda _ctx, _step_dir: (
            ("loom-service", "deploy/Dockerfile.service", "."),
            ("loom-worker", "deploy/Dockerfile.worker", "."),
        ),
    )
    monkeypatch.setattr(
        "loom_cli.rollout.steps.s12_release_gate.run_captured",
        lambda argv, **_kwargs: _docker_inspect_success(list(argv)),
    )

    output = ReleaseGateStep()._write_expected_image_identities(
        ctx,
        ev.step_dir(14, "release-gate"),
    )

    identities = json.loads(output.read_text(encoding="utf-8"))
    assert set(identities) == {"loom-service"}


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
    assert [_candidate_args(call)[:3] for call in non_docker_calls] == [
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
    assert [_candidate_args(call)[:3] for call in non_docker_calls] == [
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


def test_current_candidate_worker_binding_is_stable_across_heartbeat_timestamps(
    tmp_path: Path,
) -> None:
    ctx = make_ctx(
        tmp_path,
        image_tag="staging-abc123",
        resolved_sha="a" * 40,
    )
    status_path = tmp_path / "status.json"

    def write_status(timestamp: str) -> None:
        status_path.write_text(
            json.dumps(
                {
                    "desired_states": [
                        {
                            "environment": "staging",
                            "pool_name": "gb10",
                            "image_tag": "staging-abc123",
                            "source_git_commit": "a" * 40,
                            "updated_at": timestamp,
                            "host_intents": {
                                "trt-gb10-1": "active",
                                "trt-gb10-2": "stopped",
                            },
                        }
                    ],
                    "nodes": [
                        {
                            "environment": "staging",
                            "pool_name": "gb10",
                            "hostname": "trt-gb10-1",
                            "worker_id": "worker-current",
                            "worker_status": "active",
                            "worker_fresh": True,
                            "worker_last_seen_at": timestamp,
                        }
                    ],
                }
            )
            + "\n",
            encoding="utf-8",
        )

    write_status("2026-07-18T00:00:00Z")
    first = _current_candidate_worker_binding(ctx=ctx, status_path=status_path)
    write_status("2026-07-18T00:01:00Z")
    second = _current_candidate_worker_binding(ctx=ctx, status_path=status_path)

    assert first == second


def test_current_candidate_worker_binding_rejects_stale_registration(
    tmp_path: Path,
) -> None:
    ctx = make_ctx(
        tmp_path,
        image_tag="staging-abc123",
        resolved_sha="a" * 40,
    )
    status_path = tmp_path / "status.json"
    status_path.write_text(
        json.dumps(
            {
                "desired_states": [
                    {
                        "environment": "staging",
                        "pool_name": "gb10",
                        "image_tag": "staging-abc123",
                        "source_git_commit": "a" * 40,
                        "host_intents": {"trt-gb10-1": "active"},
                    }
                ],
                "nodes": [
                    {
                        "environment": "staging",
                        "pool_name": "gb10",
                        "hostname": "trt-gb10-1",
                        "worker_id": "worker-stale",
                        "worker_status": "active",
                        "worker_fresh": False,
                    }
                ],
            }
        )
        + "\n",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="no active fresh worker registration"):
        _current_candidate_worker_binding(ctx=ctx, status_path=status_path)


def test_release_gate_runs_exact_candidate_bound_canary_before_evidence(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ctx = replace(
        make_ctx(
            tmp_path,
            image_tag="staging-abc123",
            resolved_sha="a" * 40,
        ),
        smoke_task_id="loom-smoke/gb10-oracle-hello-world",
        smoke_required_worker_pool="gb10",
        smoke_agent="oracle",
        smoke_on_behalf_username="devansh",
        smoke_on_behalf_team_id="11111111-1111-4111-8111-111111111111",
        smoke_admin_actor="qianyi",
    )
    ev = EvidenceDirectory(tmp_path, "test-rid")
    ev.ensure()
    step_dir = ev.step_dir(14, "release-gate")
    status_path = ReleaseGateStep().gb10_status_path(ctx, step_dir)
    status_path.write_text(
        json.dumps(
            {
                "desired_states": [
                    {
                        "environment": "staging",
                        "pool_name": "gb10",
                        "image_tag": "staging-abc123",
                        "source_git_commit": "a" * 40,
                        "host_intents": {"trt-gb10-1": "active"},
                    }
                ],
                "nodes": [
                    {
                        "environment": "staging",
                        "pool_name": "gb10",
                        "hostname": "trt-gb10-1",
                        "worker_id": "worker-current",
                        "worker_status": "active",
                        "worker_fresh": True,
                    }
                ],
            }
        )
        + "\n",
        encoding="utf-8",
    )
    captured: dict[str, object] = {}

    def fake_canary(canary_ctx, _step_dir, **kwargs):
        captured["ctx"] = canary_ctx
        captured.update(kwargs)
        return RunResult(exit_code=0, artifacts={"batch_id": "batch-current"})

    monkeypatch.setattr(
        "loom_cli.rollout.steps.s12_release_gate.run_admin_on_behalf_smoke",
        fake_canary,
    )

    result = _RUN_CANDIDATE_BOUND_HF_CANARY(ReleaseGateStep(), ctx, step_dir)

    assert result.artifacts == {"batch_id": "batch-current"}
    canary_ctx = captured["ctx"]
    assert isinstance(canary_ctx, RolloutContext)
    assert canary_ctx.smoke_task_id == ("skilllearnbench/fix-security-bug/fix-security-bug-1")
    assert captured["n_per_task"] == 1
    assert captured["artifact_prefix"] == "hf-canary-"
    assert str(captured["batch_name"]).startswith("rollout-hf-boundary-staging-abc123-")


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
    assert [_candidate_args(call)[:3] for call in non_docker_calls] == [
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
    assert [_candidate_args(call)[:3] for call in non_docker_calls] == [
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
