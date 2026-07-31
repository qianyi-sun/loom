from __future__ import annotations

import base64
import hashlib
import io
import json
import os
import stat
import subprocess
import sys
import tarfile
import threading
from contextlib import contextmanager, nullcontext
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest
from scripts.ops import developer_environment_deploy as environment_deploy
from scripts.ops import developer_sandbox_host as host
from scripts.plan_ci_validations import HEAVY_CHECKS, plan_validations

SHA = "a" * 40


def test_dynamic_environment_commands_delegate_only_identity_bindings(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[list[str]] = []
    monkeypatch.setattr(
        environment_deploy,
        "main",
        lambda argv: calls.append(list(argv or ())) or 0,
    )

    assert (
        host.main(
            (
                "environment-create",
                "--env-id",
                "denv-0123456789abcdef",
                "--candidate-id",
                "cand-" + "a" * 40,
                "--idempotency-key",
                "developer-deploy-0001",
                "--execute",
            )
        )
        == 0
    )

    assert calls == [
        [
            "create",
            "--env-id",
            "denv-0123456789abcdef",
            "--candidate-id",
            "cand-" + "a" * 40,
            "--idempotency-key",
            "developer-deploy-0001",
            "--execute",
        ]
    ]


def _temporary_profile(tmp_path: Path, sandbox: str = "qianyi") -> host.Profile:
    checked_in = next(profile for profile in host.load_profiles() if profile.sandbox == sandbox)
    state = tmp_path / "state" / sandbox
    return replace(
        checked_in,
        candidate_root=tmp_path / "candidates" / sandbox,
        state_root=state,
        cache_root=state / "cache",
        evidence_root=state / "evidence",
        runtime_root=state / "runtime",
    )


def _dynamic_profile(tmp_path: Path, sandbox: str = "qianyi") -> host.Profile:
    return replace(
        _temporary_profile(tmp_path, sandbox),
        env_id=f"env-{sandbox}-000000000001",
        resource_generation=7,
        registry_generation=11,
        registry_payload_sha256="c" * 64,
        candidate_id=f"cand-{sandbox}-000000000001",
        candidate_tree="b" * 40,
        service_user=f"loom-dev-{sandbox}",
        worker_image_ids={
            "oldlab": "sha256:" + "d" * 64,
            "gb10": "sha256:" + "e" * 64,
        },
    )


def test_worker_runtime_env_uses_profile_root_for_both_domains(tmp_path: Path) -> None:
    profile = replace(
        _dynamic_profile(tmp_path),
        runtime_root=tmp_path / "shared/runtime/environments/denv-test",
    )

    assert profile.worker_runtime_env(SHA, "oldlab") == (
        profile.runtime_root / SHA / "worker-oldlab.env"
    )
    assert profile.worker_runtime_env(SHA, "gb10") == (
        profile.runtime_root / SHA / "worker-gb10.env"
    )
    with pytest.raises(host.HostConvergeError, match="runtime identity"):
        profile.worker_runtime_env(SHA, "arbitrary")


def test_legacy_profile_separates_shared_and_private_runtime_roots() -> None:
    profile = next(item for item in host.load_profiles() if item.sandbox == "qianyi")

    assert profile.runtime_root == host.NFS_RUNTIME_ROOT / "qianyi"
    assert profile.private_runtime_root == profile.state_root / "runtime"
    assert profile.worker_runtime_env(SHA) == (
        host.NFS_RUNTIME_ROOT / "qianyi" / SHA / "worker-oldlab.env"
    )


def _current_identity(user: str) -> host.Identity:
    return host.Identity(
        user=user,
        group="test-group",
        uid=os.getuid(),
        gid=os.getgid(),
    )


def _receipt(
    profile: host.Profile,
    sha: str,
    *,
    expires_at: datetime | None = None,
) -> host.ActivationReceipt:
    return host.ActivationReceipt(
        path=host.combined_receipt_path(profile, sha),
        payload_sha256="d" * 64,
        fleet_payload_sha256="sha256:" + "e" * 64,
        expires_at=expires_at or datetime.now(UTC) + timedelta(minutes=10),
    )


def _git(candidate: Path, *args: str) -> str:
    result = subprocess.run(
        ["git", "-C", str(candidate), *args],
        check=True,
        capture_output=True,
        text=True,
    )
    return result.stdout.strip()


def test_plan_covers_all_fixed_paths_ports_and_nfs_readbacks_without_secrets() -> None:
    profiles = host.load_profiles()
    plan = host.plan_document(profiles, SHA, "install")
    encoded = json.dumps(plan, sort_keys=True)

    assert plan["mutation_authorized"] is False
    assert "remote_url" not in plan
    assert {row["sandbox"] for row in plan["sandboxes"]} == set(host.LEGACY_SEED_RUNTIME_IDS)
    assert sum(len(row["ports"]) for row in plan["sandboxes"]) == 30
    assert (
        len(
            {port for row in plan["sandboxes"] for port in row["ports"].values()},
        )
        == 30
    )
    assert all(len(row["nfs_readback_commands"]) == 5 for row in plan["sandboxes"])
    assert all(row["candidate"].endswith(SHA) for row in plan["sandboxes"])
    assert plan["node_authority"] == {
        "program": "/usr/local/libexec/loom-developer-sandbox-node-authority",
        "runtime_verbs": ["transact", "check"],
        "external_root_bootstrap_required": True,
        "candidate_tree_pinned": True,
        "nodes": list(host.ELIGIBLE_LINK_NODES),
        "raw_remote_sudo_allowed": False,
    }
    assert "LOOM_DEV_POSTGRES_PASSWORD" not in encoded
    assert "loom_admin_" not in encoded


def test_slurm_maintenance_inventory_is_controller_last_and_includes_node7() -> None:
    assert host._slurm_node_order("oldlab") == (
        "oldlab-2",
        "oldlab-3",
        "oldlab-4",
        "oldlab-5",
        "oldlab-1",
    )
    assert host._slurm_node_order("gb10")[-1] == "trt-gb10-1"
    assert set(host._slurm_node_order("gb10")) == {f"trt-gb10-{index}" for index in range(1, 16)}


def test_slurm_maintenance_journal_rejects_symlink_and_hardlink(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    profile = next(item for item in host.load_profiles() if item.sandbox == "qianyi")
    monkeypatch.setattr(host, "SLURM_MAINTENANCE_ROOT", tmp_path / "slurm")
    path = host._slurm_maintenance_file("oldlab", "qianyi", SHA)
    state = host._new_slurm_maintenance_state(
        profile,
        domain="oldlab",
        sha=SHA,
        tree="b" * 40,
    )
    host._write_slurm_maintenance_state(path, state)
    os.link(path, path.with_suffix(".hardlink"))
    with pytest.raises(host.HostConvergeError, match="metadata is unsafe"):
        host._load_slurm_maintenance_file(path)

    path.with_suffix(".hardlink").unlink()
    target = path.with_suffix(".target")
    path.rename(target)
    path.symlink_to(target)
    with pytest.raises(host.HostConvergeError, match="unavailable"):
        host._load_slurm_maintenance_file(path)


def test_slurm_maintenance_journal_rejects_foreign_root_and_read_race(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    profile = next(item for item in host.load_profiles() if item.sandbox == "qianyi")
    monkeypatch.setattr(host, "SLURM_MAINTENANCE_ROOT", tmp_path / "slurm")
    path = host._slurm_maintenance_file("oldlab", "qianyi", SHA)
    state = host._new_slurm_maintenance_state(
        profile,
        domain="oldlab",
        sha=SHA,
        tree="b" * 40,
    )
    host._write_slurm_maintenance_state(path, state)

    original_lseek = host.os.lseek
    seeks = 0

    def raced_lseek(descriptor: int, offset: int, whence: int) -> int:
        nonlocal seeks
        seeks += 1
        if seeks == 2:
            path.write_bytes(path.read_bytes() + b" ")
        return original_lseek(descriptor, offset, whence)

    monkeypatch.setattr(host.os, "lseek", raced_lseek)
    with pytest.raises(host.HostConvergeError, match="changed during read"):
        host._load_slurm_maintenance_file(path)

    monkeypatch.setattr(host.os, "lseek", original_lseek)
    path.write_bytes(
        json.dumps(state, sort_keys=True, separators=(",", ":")).encode("ascii") + b"\n",
    )
    current_uid = os.geteuid()
    monkeypatch.setattr(host.os, "geteuid", lambda: current_uid + 1)
    with pytest.raises(host.HostConvergeError, match="state root is unsafe"):
        host._load_slurm_maintenance_file(path)


def test_slurm_maintenance_busy_resume_reuses_receipts_and_keeps_controller_last(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    profile = next(item for item in host.load_profiles() if item.sandbox == "qianyi")
    monkeypatch.setattr(host, "SLURM_MAINTENANCE_ROOT", tmp_path / "slurm")
    monkeypatch.setattr(host, "_require_live_host", lambda: None)
    monkeypatch.setattr(host, "_slurm_maintenance_tree", lambda _profile, _sha: "b" * 40)
    install_lock_held = False

    @contextmanager
    def install_lock() -> object:
        nonlocal install_lock_held
        assert install_lock_held is False
        install_lock_held = True
        try:
            yield
        finally:
            install_lock_held = False

    monkeypatch.setattr(host, "_install_lock", install_lock)
    candidate_bindings = {
        f"loom-dev-{sandbox}": {
            "sandbox": sandbox,
            "service_user": f"loom-sandbox-{sandbox}",
            "candidate_sha": candidate,
            "candidate_tree": "b" * 40,
        }
        for sandbox, candidate in zip(
            host.LEGACY_SEED_RUNTIME_IDS,
            (SHA, "c" * 40, "d" * 40),
            strict=True,
        )
    }

    def candidate_set(
        *,
        generation: int = 1,
        convergence_id: str | None = None,
    ) -> tuple[dict[str, object], bytes]:
        assert install_lock_held is True
        payload: dict[str, object] = {
            "schema_version": 2,
            "kind": "loom.developer-sandbox.slurm-candidate-set",
            "candidate_set_sha256": hashlib.sha256(
                json.dumps(
                    candidate_bindings,
                    sort_keys=True,
                    separators=(",", ":"),
                ).encode("ascii"),
            ).hexdigest(),
            "candidate_bindings": candidate_bindings,
            "generation": generation,
            "convergence_id": convergence_id or "e" * 64,
        }
        return (
            payload,
            (json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n").encode("ascii"),
        )

    monkeypatch.setattr(host, "_slurm_candidate_set", candidate_set)
    monkeypatch.setattr(host, "_slurm_maintenance_lock", lambda _domain: nullcontext())
    monkeypatch.setattr(
        host,
        "_ensure_root_private_directory",
        lambda path: path.mkdir(mode=0o700, parents=True, exist_ok=True),
    )
    calls: list[tuple[str, str, str | None]] = []
    busy = {"enabled": True}
    rollback_busy = {"enabled": False}

    def authority_request(
        _profile: host.Profile,
        *,
        domain: str,
        node: str,
        action: str,
        sha: str,
        tree: str,
        candidate_set_bytes: bytes | None = None,
        prior_request_id: str | None = None,
    ) -> tuple[dict[str, object], str]:
        del domain, sha, tree
        assert install_lock_held is True
        assert candidate_set_bytes is not None
        calls.append((node, action, prior_request_id))
        if busy["enabled"] and node == "oldlab-3" and action == "slurm-node-converge":
            raise host.HostConvergeError("node still has Slurm jobs; drain and retry")
        if rollback_busy["enabled"] and node == "oldlab-3" and action == "slurm-rollback":
            raise host.HostConvergeError("pending recovery timer has not completed")
        request_id = hashlib.sha256(
            b":".join(
                (
                    node.encode(),
                    action.encode(),
                    str(prior_request_id).encode(),
                    candidate_set_bytes,
                ),
            ),
        ).hexdigest()
        if action == "slurm-check":
            return (
                {
                    "status": "succeeded",
                    "request_id": request_id,
                    "result": {
                        "cluster": "trt-oldlab",
                        "candidate_sha": SHA,
                        "file_plan": {"converged": True},
                    },
                },
                request_id,
            )
        return (
            {
                "schema_version": 1,
                "status": "succeeded",
                "request_id": request_id,
                "action": action,
                "node": node,
                "domain": "oldlab",
                "sandbox": "qianyi",
                "candidate_sha": SHA,
                "candidate_tree": "b" * 40,
                "payload_sha256": hashlib.sha256(b"").hexdigest(),
                "result_sha256": "e" * 64,
                "inner_receipt": "slurm-policy-v1:trt-oldlab:" + "c" * 64 + ":" + "d" * 64,
                "completed_at": "2026-07-28T00:00:00+00:00",
            },
            request_id,
        )

    monkeypatch.setattr(host, "_slurm_authority_request", authority_request)
    with pytest.raises(host.HostConvergeError, match="Slurm jobs"):
        host.slurm_maintenance_converge(profile, SHA, "oldlab")

    path = host._slurm_maintenance_file("oldlab", "qianyi", SHA)
    blocked = json.loads(path.read_text(encoding="ascii"))
    assert blocked["phase"] == "blocked"
    first_receipt = blocked["nodes"]["oldlab-2"]["converge_receipt"]
    assert first_receipt is not None
    assert blocked["nodes"]["oldlab-3"]["converge_receipt"] is None
    assert not any(node == "oldlab-1" for node, _action, _prior in calls)

    calls.clear()
    with pytest.raises(host.HostConvergeError, match="completed candidate-set"):
        host.slurm_maintenance_rollback(profile, SHA, "oldlab")
    assert calls == []

    calls.clear()
    busy["enabled"] = False
    completed = host.slurm_maintenance_converge(profile, SHA, "oldlab")
    assert completed["phase"] == "completed"
    assert completed["nodes"]["oldlab-2"]["converge_receipt"] == first_receipt
    controller_index = calls.index(("oldlab-1", "slurm-controller-converge", None))
    compute_converge_indexes = [
        index
        for index, (_node, action, _prior) in enumerate(calls)
        if action == "slurm-node-converge"
    ]
    assert controller_index > max(compute_converge_indexes)
    assert all(
        completed["nodes"][node]["check_request_id"] is not None
        for node in host._slurm_node_order("oldlab")
    )

    calls.clear()
    checked = host.slurm_maintenance_check(profile, SHA, "oldlab")
    assert checked["status"] == "succeeded"
    assert checked["generation"] == completed["generation"]
    assert checked["convergence_id"] == completed["convergence_id"]
    assert checked["candidate_set_sha256"] == completed["candidate_set_sha256"]
    assert [node for node, action, _prior in calls if action == "slurm-check"] == list(
        host._slurm_node_order("oldlab"),
    )

    converge_receipts = {
        node: completed["nodes"][node]["converge_receipt"]
        for node in host._slurm_node_order("oldlab")
    }
    repeated = host.slurm_maintenance_converge(profile, SHA, "oldlab")
    assert repeated["generation"] == completed["generation"] + 1
    assert repeated["convergence_id"] != completed["convergence_id"]
    assert {
        node: repeated["nodes"][node]["converge_receipt"]
        for node in host._slurm_node_order("oldlab")
    } != converge_receipts

    calls.clear()
    incomplete = json.loads(json.dumps(repeated))
    incomplete["nodes"]["oldlab-3"]["converge_receipt"] = None
    host._write_slurm_maintenance_state(path, incomplete)
    with pytest.raises(host.HostConvergeError, match="every converge authority receipt"):
        host.slurm_maintenance_rollback(profile, SHA, "oldlab")
    assert calls == []
    host._write_slurm_maintenance_state(path, repeated)

    rollback_busy["enabled"] = True
    with pytest.raises(host.HostConvergeError, match="pending recovery timer"):
        host.slurm_maintenance_rollback(profile, SHA, "oldlab")
    interrupted = json.loads(path.read_text(encoding="ascii"))
    assert interrupted["phase"] == "blocked"
    assert interrupted["last_failure"]["action"] == "slurm-rollback"
    assert interrupted["nodes"]["oldlab-2"]["rollback_receipt"] is not None
    assert interrupted["nodes"]["oldlab-3"]["rollback_receipt"] is None
    assert not any(node == "oldlab-1" for node, _action, _prior in calls)

    calls.clear()
    rollback_busy["enabled"] = False
    rolled_back = host.slurm_maintenance_rollback(profile, SHA, "oldlab")
    assert rolled_back["phase"] == "rolled-back"
    rollback_calls = [row for row in calls if row[1] == "slurm-rollback"]
    assert [node for node, _action, _prior in rollback_calls] == list(
        host._slurm_node_order("oldlab"),
    )
    assert all(prior is not None for _node, _action, prior in rollback_calls)

    calls.clear()
    assert host.slurm_maintenance_rollback(profile, SHA, "oldlab") == rolled_back
    assert calls == []


def test_sandbox_batch_identity_is_fixed_nonlogin_service(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        host,
        "_identity",
        lambda user, group: host.Identity(user=user, group=group, uid=31023, gid=31023),
    )
    monkeypatch.setattr(
        host.pwd,
        "getpwnam",
        lambda _user: SimpleNamespace(
            pw_gid=31023,
            pw_dir="/nonexistent",
            pw_shell="/usr/sbin/nologin",
        ),
    )

    identity = host._sandbox_batch_identity("devansh")

    assert identity == host.Identity(
        user="loom-sandbox-devansh",
        group="loom-sandbox-devansh",
        uid=31023,
        gid=31023,
    )


def test_sandbox_batch_identity_rejects_login_capable_drift(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        host,
        "_identity",
        lambda user, group: host.Identity(user=user, group=group, uid=31021, gid=31021),
    )
    monkeypatch.setattr(
        host.pwd,
        "getpwnam",
        lambda _user: SimpleNamespace(
            pw_gid=31021,
            pw_dir="/home/loom-sandbox-qianyi",
            pw_shell="/bin/bash",
        ),
    )

    with pytest.raises(host.HostConvergeError, match="metadata drifted"):
        host._sandbox_batch_identity("qianyi")


def test_secret_initialization_is_private_idempotent_and_unique(tmp_path: Path) -> None:
    qianyi = _temporary_profile(tmp_path, "qianyi")
    hongjian = _temporary_profile(tmp_path, "hongjian")
    qianyi_identity = _current_identity("qianyi")
    hongjian_identity = _current_identity("hongjian")

    host.ensure_secret_files(qianyi, qianyi_identity)
    first_env = qianyi.secrets_env.read_bytes()
    first_admin = qianyi.admin_secret.read_bytes()
    host.ensure_secret_files(qianyi, qianyi_identity)
    host.ensure_secret_files(hongjian, hongjian_identity)

    assert qianyi.secrets_env.read_bytes() == first_env
    assert qianyi.admin_secret.read_bytes() == first_admin
    for path in (qianyi.secrets_env, qianyi.admin_secret, hongjian.secrets_env):
        assert stat.S_IMODE(path.stat().st_mode) == 0o600
        assert (path.stat().st_uid, path.stat().st_gid) == (os.getuid(), os.getgid())
    qianyi_values = host._parse_env_file(qianyi.secrets_env)
    hongjian_values = host._parse_env_file(hongjian.secrets_env)
    for key in (
        "LOOM_DEV_POSTGRES_PASSWORD",
        "LOOM_DEV_MINIO_ROOT_PASSWORD",
        "LOOM_CP_STEP_JWT_SIGNING_KEY",
        "LOOM_SECRET_STORE_MASTER_KEY",
        "LOOM_WORKER_TOKEN",
    ):
        assert qianyi_values[key] != hongjian_values[key]


def test_secret_initialization_rejects_permissive_existing_file(tmp_path: Path) -> None:
    profile = _temporary_profile(tmp_path)
    identity = _current_identity("qianyi")
    host.ensure_secret_files(profile, identity)
    profile.secrets_env.chmod(0o640)

    with pytest.raises(host.HostConvergeError, match="mode 0600"):
        host.ensure_secret_files(profile, identity)


def test_private_root_convergence_rejects_symlink(tmp_path: Path) -> None:
    profile = _temporary_profile(tmp_path)
    target = tmp_path / "unrelated"
    target.mkdir()
    profile.state_root.parent.mkdir(parents=True)
    profile.state_root.symlink_to(target, target_is_directory=True)

    with pytest.raises(host.HostConvergeError, match="root is unsafe"):
        host.ensure_private_roots(profile, _current_identity("qianyi"))

    assert stat.S_IMODE(target.stat().st_mode) != 0o700


def test_combined_receipt_reader_does_not_follow_symlink(tmp_path: Path) -> None:
    target = tmp_path / "combined-target.json"
    target.write_text("{}\n", encoding="utf-8")
    receipt = tmp_path / "combined.json"
    receipt.symlink_to(target)

    with pytest.raises(host.HostConvergeError, match="unavailable"):
        host._read_combined_receipt_bytes(receipt)


def test_private_root_convergence_does_not_follow_raced_child_symlink(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    profile = _temporary_profile(tmp_path)
    identity = _current_identity("qianyi")
    target = tmp_path / "unrelated"
    target.mkdir(mode=0o755)
    target_before = target.stat()
    mkdir_private_dir_at = host._mkdir_private_dir_at

    def replace_created_cache_with_symlink(parent_fd: int, name: str) -> None:
        mkdir_private_dir_at(parent_fd, name)
        if name == profile.cache_root.name:
            profile.cache_root.rmdir()
            profile.cache_root.symlink_to(target, target_is_directory=True)

    monkeypatch.setattr(
        host,
        "_mkdir_private_dir_at",
        replace_created_cache_with_symlink,
    )

    with pytest.raises(host.HostConvergeError, match="root is unsafe"):
        host.ensure_private_roots(profile, identity)

    target_after = target.stat()
    assert (target_after.st_dev, target_after.st_ino) == (
        target_before.st_dev,
        target_before.st_ino,
    )
    assert stat.S_IMODE(target_after.st_mode) == stat.S_IMODE(target_before.st_mode)
    state_metadata = profile.state_root.stat()
    assert stat.S_IMODE(state_metadata.st_mode) == 0o700
    assert (state_metadata.st_uid, state_metadata.st_gid) == (
        identity.uid,
        identity.gid,
    )


def test_atomic_write_does_not_follow_target_replaced_after_rename(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    profile = _temporary_profile(tmp_path)
    identity = _current_identity("qianyi")
    host.ensure_private_roots(profile, identity)
    parent_before = profile.secrets_root.stat()
    target = tmp_path / "unrelated"
    target.mkdir(mode=0o755)
    target_before = target.stat()
    replace_file_at = host._replace_file_at

    def replace_then_swap_target(
        parent_fd: int,
        source: str,
        destination: str,
    ) -> None:
        replace_file_at(parent_fd, source, destination)
        os.unlink(destination, dir_fd=parent_fd)
        os.symlink(target, destination, dir_fd=parent_fd)

    monkeypatch.setattr(host, "_replace_file_at", replace_then_swap_target)

    with pytest.raises(host.HostConvergeError, match="target binding changed"):
        host._atomic_write(
            profile.secrets_env,
            b"private\n",
            mode=0o600,
            identity=identity,
        )

    target_after = target.stat()
    assert (target_after.st_dev, target_after.st_ino) == (
        target_before.st_dev,
        target_before.st_ino,
    )
    assert stat.S_IMODE(target_after.st_mode) == stat.S_IMODE(target_before.st_mode)
    parent_after = profile.secrets_root.stat()
    assert stat.S_IMODE(parent_after.st_mode) == stat.S_IMODE(parent_before.st_mode)
    assert (parent_after.st_uid, parent_after.st_gid) == (
        parent_before.st_uid,
        parent_before.st_gid,
    )


def test_exact_candidate_verifier_requires_clean_immutable_tree(tmp_path: Path) -> None:
    profile = _temporary_profile(tmp_path)
    candidate = profile.candidate_root / SHA
    candidate.mkdir(parents=True)
    _git(candidate, "init")
    _git(candidate, "config", "user.email", "sandbox-test@example.invalid")
    _git(candidate, "config", "user.name", "Sandbox Test")
    (candidate / "README.md").write_text("exact candidate\n", encoding="utf-8")
    _git(candidate, "add", "README.md")
    _git(candidate, "commit", "-m", "candidate")
    actual_sha = _git(candidate, "rev-parse", "HEAD")
    immutable = profile.candidate_root / actual_sha
    candidate.rename(immutable)
    for root, directories, files in os.walk(immutable):
        for entry in [Path(root), *(Path(root) / name for name in directories + files)]:
            if not entry.is_symlink():
                entry.chmod(0o2750 if entry.is_dir() else 0o640)

    tree = host.verify_candidate(
        profile,
        immutable,
        actual_sha,
        _current_identity("publisher"),
    )

    assert len(tree) == 40
    readme = immutable / "README.md"
    readme.chmod(0o660)
    with pytest.raises(host.HostConvergeError, match="group/world-writable"):
        host.verify_candidate(
            profile,
            immutable,
            actual_sha,
            _current_identity("publisher"),
        )


def test_desired_state_records_only_one_safe_rollback_target(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(host, "DESIRED_ROOT", tmp_path / "desired")
    profile = _temporary_profile(tmp_path)
    first = "1" * 40
    second = "2" * 40

    assert host.write_desired(profile, first, "3" * 40, _receipt(profile, first)) is None
    previous = host.write_desired(profile, second, "4" * 40, _receipt(profile, second))
    desired = json.loads(profile.desired_file.read_text(encoding="utf-8"))

    assert previous is not None
    assert desired["candidate_sha"] == second
    assert desired["previous_sha"] == first
    assert stat.S_IMODE(profile.desired_file.stat().st_mode) == 0o600


def test_desired_state_binding_rejects_tree_or_secret_path_drift(tmp_path: Path) -> None:
    profile = _temporary_profile(tmp_path)
    receipt = _receipt(profile, SHA)
    payload = host._desired_payload(
        profile,
        SHA,
        "b" * 40,
        previous_sha="c" * 40,
        receipt=receipt,
    )
    host._validate_desired_binding(
        profile,
        payload,
        sha=SHA,
        tree="b" * 40,
        receipt=receipt,
    )

    payload["candidate_tree"] = "d" * 40
    with pytest.raises(host.HostConvergeError, match="desired state binding"):
        host._validate_desired_binding(
            profile,
            payload,
            sha=SHA,
            tree="b" * 40,
            receipt=receipt,
        )


def test_update_refuses_migration_tree_change_before_activation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(host, "DESIRED_ROOT", tmp_path / "desired")
    profile = _temporary_profile(tmp_path)
    current = "1" * 40
    target = "2" * 40
    host.write_desired(profile, current, "3" * 40, _receipt(profile, current))
    monkeypatch.setattr(host, "verify_candidate", lambda *args, **kwargs: "3" * 40)
    monkeypatch.setattr(
        host,
        "_migration_tree",
        lambda candidate, publisher: "4" * 40 if candidate.name == current else "5" * 40,
    )

    with pytest.raises(host.HostConvergeError, match="migration-tree change"):
        host.require_migration_compatible_update(
            profile,
            target,
            _current_identity("publisher"),
        )


def test_fresh_database_tokens_are_minted_into_private_file_without_output(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    profile = _temporary_profile(tmp_path)
    identity = _current_identity("qianyi")
    host.ensure_secret_files(profile, identity)
    monkeypatch.setattr(host, "_wait_for_control_plane", lambda selected: None)
    monkeypatch.setattr(host, "_read_admin_token", lambda path: "private-admin")

    def request(
        url: str,
        *,
        token: str | None,
        expected: set[int],
    ) -> tuple[int, dict[str, str]]:
        if url.endswith("/workers/register"):
            return 401, {}
        if url.endswith("/worker-tokens"):
            assert token == "private-admin"
            return 201, {"token": "loom_w_private-worker"}
        assert url.endswith("/batch-runner-tokens")
        assert token == "private-admin"
        return 201, {"token": "loom_br_private-batch"}

    monkeypatch.setattr(host, "_request_json", request)

    assert host.bootstrap_runtime_tokens(profile, identity) is True
    values = host._parse_env_file(profile.secrets_env)
    captured = capsys.readouterr()
    assert values["LOOM_WORKER_TOKEN"] == "loom_w_private-worker"
    assert values["LOOM_SVC_BATCH_RUNNER_CP_TOKEN"] == "loom_br_private-batch"
    assert stat.S_IMODE(profile.secrets_env.stat().st_mode) == 0o600
    assert "private-worker" not in captured.out + captured.err
    assert "private-batch" not in captured.out + captured.err


def test_candidate_child_environment_drops_ambient_secret(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    profile = _temporary_profile(tmp_path)
    monkeypatch.setenv("DO_NOT_FORWARD_PRIVATE_VALUE", "private")

    environment = host._candidate_environment(profile, profile.candidate_root / SHA)

    assert "DO_NOT_FORWARD_PRIVATE_VALUE" not in environment
    assert environment["GIT_CONFIG_NOSYSTEM"] == "1"
    assert environment["GIT_NO_REPLACE_OBJECTS"] == "1"
    assert environment["PYTHONDONTWRITEBYTECODE"] == "1"


def test_systemd_entry_is_fixed_and_replayable() -> None:
    unit = (
        Path(__file__).resolve().parents[2]
        / "deploy/developer-sandboxes/loom-developer-sandbox@.service"
    ).read_text(encoding="utf-8")

    assert "ConditionPathExists=/etc/loom/developer-sandboxes/desired/%i.json" in unit
    assert (
        "ExecStart=/usr/local/libexec/loom-developer-sandbox-host service-converge "
        "--legacy-v1-seed-migrate --sandbox %i"
    ) in unit
    assert "EnvironmentFile=" not in unit
    assert "RemainAfterExit=" not in unit
    assert "WantedBy=multi-user.target" in unit


def test_attestation_renewal_timer_is_persistent_and_bounded() -> None:
    root = Path(__file__).resolve().parents[2] / "deploy/developer-sandboxes"
    service = (root / "loom-developer-sandbox-attestation-renewal.service").read_text(
        encoding="utf-8"
    )
    timer = (root / "loom-developer-sandbox-attestation-renewal.timer").read_text(encoding="utf-8")
    tmpfiles = (root / "loom-developer-sandbox-installer.tmpfiles.conf").read_text(
        encoding="utf-8",
    )

    assert (
        "ExecStart=/usr/bin/python3 -I -B "
        "/usr/local/libexec/loom-developer-environment-deploy renew-active --execute"
    ) in service
    assert "NoNewPrivileges=false" in service
    assert "User=root" in service
    assert "UMask=0077" in service
    assert "loom-developer-sandbox-links.target" in service
    assert "OnUnitActiveSec=5min" in timer
    assert "Persistent=true" in timer
    assert "WantedBy=timers.target" in timer
    assert tmpfiles == "d /run/loom-developer-sandbox-installer 0700 root root -\n"
    assert "RuntimeDirectory=" not in service
    assert "ReadWritePaths=/run/loom-developer-sandbox-installer" in service

    sandbox_unit = (root / "loom-developer-sandbox@.service").read_text(encoding="utf-8")
    link_unit = (root / "loom-developer-sandbox-link@.service").read_text(encoding="utf-8")
    assert "loom-developer-sandbox-link@" not in sandbox_unit
    assert "After=network-online.target loom-developer-sandbox@%i.service" in link_unit
    assert "Requires=loom-developer-sandbox@%i.service" in link_unit


def test_installer_tmpfiles_install_applies_exact_source_and_reads_back(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = tmp_path / "source.conf"
    source.write_text(
        "d /run/loom-developer-sandbox-installer 0700 root root -\n",
        encoding="utf-8",
    )
    target = tmp_path / "installed.conf"
    state: dict[str, bytes] = {}
    events: list[str] = []
    monkeypatch.setattr(host, "INSTALLER_TMPFILES_PATH", target)
    monkeypatch.setattr(
        host,
        "_read_optional_tmpfiles_asset",
        lambda path: state.get(str(path)),
    )

    def write(path: Path, payload: bytes, *, mode: int) -> None:
        assert path == target
        assert mode == 0o644
        state[str(path)] = payload
        events.append("installed")

    monkeypatch.setattr(host, "_atomic_write", write)

    def run(argv: tuple[str, ...], **_kwargs: object) -> object:
        assert argv == ("systemd-tmpfiles", "--create", str(target))
        events.append("applied")
        return type("Completed", (), {"stdout": "", "returncode": 0})()

    monkeypatch.setattr(host, "_run", run)
    monkeypatch.setattr(
        host,
        "_validate_installer_runtime_directory",
        lambda: events.append("runtime-readback"),
    )

    host._install_tmpfiles_asset(source)

    assert state[str(target)] == source.read_bytes()
    assert events == ["installed", "applied", "runtime-readback"]


@pytest.mark.parametrize("previous", (None, b"d /run/prior 0700 root root -\n"))
def test_installer_tmpfiles_install_failure_rolls_back_exact_prior_asset(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    previous: bytes | None,
) -> None:
    source = tmp_path / "source.conf"
    source.write_text(
        "d /run/loom-developer-sandbox-installer 0700 root root -\n",
        encoding="utf-8",
    )
    target = tmp_path / "installed.conf"
    state: dict[str, bytes] = {}
    events: list[str] = []
    if previous is not None:
        state[str(target)] = previous
    monkeypatch.setattr(host, "INSTALLER_TMPFILES_PATH", target)
    monkeypatch.setattr(
        host,
        "_read_optional_tmpfiles_asset",
        lambda path: state.get(str(path)),
    )
    runtime_snapshot = host._InstallerRuntimeSnapshot(
        present=True,
        device=1,
        inode=2,
        uid=0,
        gid=0,
        mode=0o700,
    )
    monkeypatch.setattr(
        host,
        "_snapshot_installer_runtime_directory",
        lambda: runtime_snapshot,
    )

    def write(path: Path, payload: bytes, *, mode: int) -> None:
        assert mode == 0o644
        state[str(path)] = payload

    monkeypatch.setattr(host, "_atomic_write", write)
    monkeypatch.setattr(
        host,
        "_run",
        lambda *_args, **_kwargs: (
            events.append("apply") or type("Completed", (), {"stdout": "", "returncode": 0})()
        ),
    )
    validation_calls = 0

    def reject_runtime() -> None:
        nonlocal validation_calls
        validation_calls += 1
        raise host.HostConvergeError("runtime unsafe")

    monkeypatch.setattr(host, "_validate_installer_runtime_directory", reject_runtime)
    monkeypatch.setattr(
        host,
        "_validate_restored_installer_runtime",
        lambda snapshot: events.append(f"restored-runtime:{snapshot.present}"),
    )
    monkeypatch.setattr(
        Path,
        "unlink",
        lambda path, *, missing_ok=False: state.pop(str(path), None),
    )
    monkeypatch.setattr(host, "_fsync_directory", lambda *_args: None)

    with pytest.raises(host.HostConvergeError, match="failed and was rolled back"):
        host._install_tmpfiles_asset(source)

    assert state.get(str(target)) == previous
    assert validation_calls == 1
    assert events[-1] == "restored-runtime:True"
    assert events.count("apply") == (2 if previous is not None else 1)


def test_installer_tmpfiles_source_is_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = (
        Path(__file__).resolve().parents[2]
        / "deploy/developer-sandboxes/loom-developer-sandbox-installer.tmpfiles.conf"
    )
    assert source.read_bytes() == (b"d /run/loom-developer-sandbox-installer 0700 root root -\n")
    invalid = tmp_path / "invalid.conf"
    invalid.write_text("d /run/loom-developer-sandbox-installer 0755 root root -\n")
    monkeypatch.setattr(host, "INSTALLER_TMPFILES_PATH", tmp_path / "installed.conf")

    with pytest.raises(host.HostConvergeError, match="source is invalid"):
        host._install_tmpfiles_asset(invalid)


def _tmpfiles_install_source(tmp_path: Path) -> Path:
    source = tmp_path / "candidate"
    scripts = source / "scripts/ops"
    profiles = source / "deploy/developer-sandboxes"
    scripts.mkdir(parents=True)
    profiles.mkdir(parents=True)
    (scripts / "developer_sandbox_host.py").write_text("program\n", encoding="utf-8")
    for name in (
        "loom-developer-sandbox@.service",
        "loom-developer-sandbox-attestation-renewal.service",
        "loom-developer-sandbox-attestation-renewal.timer",
        "qianyi.toml",
        "hongjian.toml",
        "devansh.toml",
    ):
        (profiles / name).write_text(f"{name}\n", encoding="utf-8")
    (profiles / "loom-developer-sandbox-installer.tmpfiles.conf").write_text(
        "d /run/loom-developer-sandbox-installer 0700 root root -\n",
        encoding="utf-8",
    )
    return source


def test_install_assets_defers_tmpfiles_until_every_candidate_write_succeeds(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = _tmpfiles_install_source(tmp_path)
    events: list[str] = []
    monkeypatch.setattr(host, "_ensure_root_private_directory", lambda *_args: None)

    def write(path: Path, _payload: bytes, *, mode: int) -> None:
        events.append(f"write:{path}:{mode:o}")
        if path == host.RENEWAL_TIMER_PATH:
            raise host.HostConvergeError("later asset failed")

    monkeypatch.setattr(host, "_atomic_write", write)
    monkeypatch.setattr(
        host,
        "_install_tmpfiles_asset",
        lambda *_args: events.append("tmpfiles") or None,
    )

    with pytest.raises(host.HostConvergeError, match="later asset failed"):
        host._install_assets(source)

    assert "tmpfiles" not in events


def test_install_assets_rolls_back_tmpfiles_when_daemon_reload_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = _tmpfiles_install_source(tmp_path)
    events: list[str] = []
    snapshot = host._InstallerTmpfilesSnapshot(
        config=None,
        runtime=host._InstallerRuntimeSnapshot(False, None, None, None, None, None),
    )
    monkeypatch.setattr(host, "_ensure_root_private_directory", lambda *_args: None)
    monkeypatch.setattr(
        host,
        "_atomic_write",
        lambda path, _payload, *, mode: events.append(f"write:{path}:{mode:o}"),
    )
    monkeypatch.setattr(
        host,
        "_install_tmpfiles_asset",
        lambda *_args: events.append("tmpfiles") or snapshot,
    )

    def reload(argv: tuple[str, ...], **_kwargs: object) -> object:
        assert argv == ("systemctl", "daemon-reload")
        events.append("daemon-reload")
        raise host.HostConvergeError("reload failed")

    monkeypatch.setattr(host, "_run", reload)
    monkeypatch.setattr(
        host,
        "_restore_tmpfiles_asset",
        lambda restored: events.append(f"rollback:{restored is snapshot}"),
    )

    with pytest.raises(
        host.HostConvergeError,
        match="asset reload failed and tmpfiles was rolled back",
    ):
        host._install_assets(source)

    assert events[-3:] == ["tmpfiles", "daemon-reload", "rollback:True"]


def test_runtime_attestation_history_is_monotonic_and_candidate_bound(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    profile = _temporary_profile(tmp_path)
    tree = "b" * 40
    monkeypatch.setattr(host, "COMBINED_RECEIPT_ROOT", tmp_path / "attestations")
    monkeypatch.setattr(host, "FLEET_ATTESTATION_ROOT", tmp_path / "attestations")
    monkeypatch.setattr(host, "RENEWAL_STATE_ROOT", tmp_path / "renewal-state")
    monkeypatch.setattr(host.os, "fchown", lambda *_args: None)
    monkeypatch.setattr(host, "_read_combined_receipt_bytes", lambda path: path.read_bytes())

    def private_directory(path: Path) -> None:
        path.mkdir(parents=True, mode=0o700, exist_ok=True)
        path.chmod(0o700)

    monkeypatch.setattr(host, "_ensure_root_private_directory", private_directory)

    def receipt(
        candidate: str,
        *,
        minute: int,
        generation: int,
        embedded_candidate: str | None = None,
    ) -> host.ActivationReceipt:
        collected = datetime(2026, 7, 28, tzinfo=UTC) + timedelta(minutes=minute)
        expires = collected + timedelta(minutes=15)
        fleet_unsigned = {
            "schema_version": 1,
            "sandbox": profile.sandbox,
            "candidate_sha": embedded_candidate or candidate,
            "generated_at": collected.isoformat(),
            "expires_at": expires.isoformat(),
            "eligible_nodes": list(host.ELIGIBLE_LINK_NODES),
            "bundle_generation": {"candidate_sha": embedded_candidate or candidate},
            "server": {
                "node": "oldlab-2",
                "unit_active": True,
                "active_candidate_sha": embedded_candidate or candidate,
            },
            "nodes": {
                node: {"candidate_sha": embedded_candidate or candidate}
                for node in host.ELIGIBLE_LINK_NODES
            },
        }
        fleet_digest = (
            "sha256:"
            + hashlib.sha256(
                json.dumps(
                    fleet_unsigned,
                    sort_keys=True,
                    separators=(",", ":"),
                    ensure_ascii=True,
                ).encode(),
            ).hexdigest()
        )
        fleet_payload = {**fleet_unsigned, "payload_sha256": fleet_digest}
        fleet_path = host.COMBINED_RECEIPT_ROOT / profile.sandbox / candidate / "fleet.json"
        fleet_path.parent.mkdir(parents=True, mode=0o700, exist_ok=True)
        fleet_path.write_text(json.dumps(fleet_payload), encoding="utf-8")
        combined_unsigned = {
            "schema_version": 1,
            "kind": "loom.developer-runtime-combined-activation",
            "sandbox": profile.sandbox,
            "candidate_sha": embedded_candidate or candidate,
            "candidate_tree": tree,
            "collector": {
                "hostname": host.EXPECTED_HOSTNAME,
                "collected_at": collected.isoformat(),
                "expires_at": expires.isoformat(),
            },
            "fleet_attestation": {
                "path": str(fleet_path),
                "payload_sha256": fleet_digest,
                "generated_at": collected.isoformat(),
                "expires_at": expires.isoformat(),
            },
            "domains": {
                "oldlab": {"generation": generation},
                "gb10": {"generation": generation},
            },
        }
        digest = hashlib.sha256(
            json.dumps(
                combined_unsigned,
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=True,
            ).encode(),
        ).hexdigest()
        combined = {**combined_unsigned, "payload_sha256": digest}
        path = host.combined_receipt_path(profile, candidate)
        path.parent.mkdir(parents=True, mode=0o700, exist_ok=True)
        path.write_text(
            json.dumps(
                combined,
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=True,
            )
            + "\n",
            encoding="utf-8",
        )
        return host.ActivationReceipt(
            path=path,
            payload_sha256=digest,
            fleet_payload_sha256=fleet_digest,
            expires_at=expires,
        )

    first_receipt = receipt(SHA, minute=0, generation=1)
    real_atomic_write = host._atomic_write
    crashed = False

    def crash_after_history(
        path: Path,
        content: bytes,
        *,
        mode: int,
        identity: host.Identity | None = None,
    ) -> None:
        nonlocal crashed
        if path == host._renewal_state_file(profile) and not crashed:
            crashed = True
            raise host.HostConvergeError("simulated state-write crash")
        real_atomic_write(path, content, mode=mode, identity=identity)

    monkeypatch.setattr(host, "_atomic_write", crash_after_history)
    with pytest.raises(host.HostConvergeError, match="simulated state-write crash"):
        host._archive_runtime_attestation(profile, SHA, tree, first_receipt)
    monkeypatch.setattr(host, "_atomic_write", real_atomic_write)
    first = host._archive_runtime_attestation(profile, SHA, tree, first_receipt)
    second_receipt = receipt(SHA, minute=10, generation=2)
    second = host._archive_runtime_attestation(profile, SHA, tree, second_receipt)

    assert first["renewal_generation"] == 1
    assert second["renewal_generation"] == 2
    assert second["previous_payload_sha256"] == first["payload_sha256"]
    assert stat.S_IMODE(Path(second["path"]).stat().st_mode) == 0o600
    with pytest.raises(host.HostConvergeError, match="replay"):
        host._archive_runtime_attestation(profile, SHA, tree, second_receipt)

    other = "c" * 40
    with pytest.raises(host.HostConvergeError, match="binding"):
        host._archive_runtime_attestation(
            profile,
            other,
            tree,
            receipt(other, minute=20, generation=1, embedded_candidate=SHA),
        )

    history_path = Path(second["path"])
    generation, archived = host._archived_activation_from_path(
        profile,
        SHA,
        tree,
        history_path,
    )
    assert generation == 2
    assert archived.payload_sha256 == second_receipt.payload_sha256
    tampered = json.loads(history_path.read_text(encoding="utf-8"))
    tampered["candidate_tree"] = "f" * 40
    history_path.write_text(
        json.dumps(
            tampered,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
        )
        + "\n",
        encoding="utf-8",
    )
    with pytest.raises(host.HostConvergeError, match="binding"):
        host._archived_activation_from_path(profile, SHA, tree, history_path)


def test_service_converge_uses_expired_archive_before_links_are_active(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    profile = _temporary_profile(tmp_path)
    desired = {"candidate_sha": SHA}
    archived = _receipt(
        profile,
        SHA,
        expires_at=datetime.now(UTC) - timedelta(hours=1),
    )
    events: list[str] = []
    monkeypatch.setattr(host, "_identity", lambda user, _group: _current_identity(user))
    monkeypatch.setattr(
        host,
        "_sandbox_batch_identity",
        lambda sandbox: _current_identity(f"loom-sandbox-{sandbox}"),
    )
    monkeypatch.setattr(host, "verify_candidate_root", lambda *_args: None)
    monkeypatch.setattr(host, "verify_candidate", lambda *_args: "b" * 40)
    monkeypatch.setattr(host, "verify_worker_runtime_env", lambda *_args: None)
    monkeypatch.setattr(
        host,
        "verify_combined_receipt",
        lambda *_args: (_ for _ in ()).throw(host.HostConvergeError("expired")),
    )
    monkeypatch.setattr(
        host,
        "_verify_archived_activation",
        lambda *_args, **_kwargs: events.append("archive") or archived,
    )
    monkeypatch.setattr(
        host,
        "_renew_attestation_locked",
        lambda *_args, **_kwargs: pytest.fail(
            "sandbox convergence must not renew before its link is active",
        ),
    )
    monkeypatch.setattr(host, "_validate_desired_binding", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(host, "ensure_secret_files", lambda *_args: None)
    monkeypatch.setattr(host, "_sandbox_state_sha", lambda *_args: SHA)
    monkeypatch.setattr(
        host,
        "_invoke_lifecycle",
        lambda _profile, _sha, operation: events.append(operation),
    )
    monkeypatch.setattr(host, "bootstrap_runtime_tokens", lambda *_args: False)
    monkeypatch.setattr(host, "verify_listening_ports", lambda *_args: None)

    host._service_converge_locked(profile, desired)

    assert events == ["archive", "update", "check"]


def test_cli_plan_is_non_mutating_and_secret_safe(capsys: pytest.CaptureFixture[str]) -> None:
    rc = host.main(["plan", "--legacy-v1-seed-migrate", "--candidate-sha", SHA])
    captured = capsys.readouterr()

    assert rc == 0
    payload = json.loads(captured.out)
    assert payload["mutation_authorized"] is False
    assert not captured.err
    assert "LOOM_DEV_POSTGRES_PASSWORD" not in captured.out


def test_fixed_seed_commands_require_explicit_legacy_migration_gate() -> None:
    with pytest.raises(SystemExit):
        host._parser().parse_args(["install", "--candidate-sha", SHA])
    with pytest.raises(SystemExit):
        host._parser().parse_args(["service-converge", "--sandbox", "qianyi"])


def test_cli_executed_check_remains_read_only_and_reports_verified(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    checked: list[str] = []
    monkeypatch.setattr(host, "service_check", checked.append)

    rc = host.main(
        [
            "check",
            "--legacy-v1-seed-migrate",
            "--candidate-sha",
            SHA,
            "--sandbox",
            "qianyi",
            "--execute",
        ],
    )
    captured = capsys.readouterr()
    payload = json.loads(captured.out)

    assert rc == 0
    assert checked == ["qianyi"]
    assert payload["mutation_authorized"] is False
    assert payload["verified"] is True
    assert payload["status"] == "succeeded"


def test_failed_child_output_is_not_relayed_in_error() -> None:
    private_value = "private-child-output"

    with pytest.raises(host.HostConvergeError) as caught:
        host._run(
            (
                sys.executable,
                "-c",
                (
                    "import sys; "
                    f"print({private_value!r}); "
                    f"sys.stderr.write({private_value!r}); "
                    "raise SystemExit(7)"
                ),
            ),
        )

    assert private_value not in str(caught.value)
    assert "exit code 7" in str(caught.value)


def test_failed_first_install_removes_desired_and_stops_prepare(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    profile = _temporary_profile(tmp_path)
    monkeypatch.setattr(host, "DESIRED_ROOT", tmp_path / "desired")
    profile.desired_file.parent.mkdir(parents=True)
    profile.desired_file.write_text("{}\n", encoding="utf-8")
    events: list[str] = []
    monkeypatch.setattr(host, "_sandbox_state_sha", lambda _profile: None)
    monkeypatch.setattr(
        host,
        "_invoke_lifecycle",
        lambda _profile, sha, operation: events.append(f"{operation}:{sha}"),
    )
    monkeypatch.setattr(
        host,
        "_restore_relay",
        lambda _profile, previous, sha: events.append(f"relay:{previous}:{sha}"),
    )
    monkeypatch.setattr(
        host,
        "_invalidate_receipt",
        lambda _profile, sha: events.append(f"invalidate:{sha}"),
    )
    monkeypatch.setattr(host, "_remove_transaction", lambda _profile: events.append("remove"))

    host._recover_transaction(
        profile,
        {
            "candidate_sha": SHA,
            "previous_desired": None,
            "previous_relay_sha": None,
        },
    )

    assert not profile.desired_file.exists()
    assert events == [
        f"prepare-stop:{SHA}",
        f"relay:None:{SHA}",
        f"invalidate:{SHA}",
        "remove",
    ]


def test_failed_upgrade_restores_previous_desired_and_candidate(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    profile = _temporary_profile(tmp_path)
    monkeypatch.setattr(host, "DESIRED_ROOT", tmp_path / "desired")
    profile.desired_file.parent.mkdir(parents=True)
    previous_sha = "b" * 40
    previous = {"schema_version": 1, "sandbox": profile.sandbox, "candidate_sha": previous_sha}
    events: list[str] = []
    monkeypatch.setattr(
        host,
        "_invoke_lifecycle",
        lambda _profile, sha, operation: events.append(f"{operation}:{sha}"),
    )
    monkeypatch.setattr(host, "_restore_relay", lambda *_args: events.append("relay"))
    monkeypatch.setattr(host, "_invalidate_receipt", lambda *_args: events.append("invalidate"))
    monkeypatch.setattr(host, "_remove_transaction", lambda _profile: events.append("remove"))

    host._recover_transaction(
        profile,
        {
            "candidate_sha": SHA,
            "previous_desired": previous,
            "previous_relay_sha": previous_sha,
        },
    )

    assert json.loads(profile.desired_file.read_text(encoding="utf-8")) == previous
    assert events == [f"update:{previous_sha}", "relay", "invalidate", "remove"]


def test_rollback_waits_for_the_global_install_serialization(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    profile = _temporary_profile(tmp_path)
    gate = threading.Lock()
    attempted = threading.Event()
    entered = threading.Event()
    failures: list[BaseException] = []

    @contextmanager
    def install_lock() -> object:
        attempted.set()
        with gate:
            yield

    @contextmanager
    def activation_lock(_profile: host.Profile) -> object:
        yield

    monkeypatch.setattr(host, "_require_live_host", lambda: None)
    monkeypatch.setattr(host, "verify_nfs_mount", lambda: None)
    monkeypatch.setattr(host, "verify_state_parent", lambda: None)
    monkeypatch.setattr(host, "_install_lock", install_lock)
    monkeypatch.setattr(host, "_activation_lock", activation_lock)

    def transaction_payload(_profile: host.Profile) -> None:
        entered.set()
        return None

    monkeypatch.setattr(host, "_transaction_payload", transaction_payload)
    monkeypatch.setattr(host, "_load_json", lambda *_args: None)

    def run_rollback() -> None:
        try:
            host.rollback(profile, "b" * 40)
        except host.HostConvergeError:
            return
        except BaseException as exc:
            failures.append(exc)

    gate.acquire()
    worker = threading.Thread(target=run_rollback)
    worker.start()
    assert attempted.wait(timeout=1)
    assert not entered.is_set()
    gate.release()
    worker.join(timeout=1)

    assert not worker.is_alive()
    assert entered.is_set()
    assert not failures


def test_attestation_timer_waits_for_the_recovery_activation_lock(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    profile = _temporary_profile(tmp_path)
    gate = threading.Lock()
    attempted = threading.Event()
    renewed = threading.Event()
    failures: list[BaseException] = []

    @contextmanager
    def install_lock() -> object:
        yield

    @contextmanager
    def activation_lock(_profile: host.Profile) -> object:
        attempted.set()
        with gate:
            yield

    monkeypatch.setattr(host, "_require_live_host", lambda: None)
    monkeypatch.setattr(host, "verify_nfs_mount", lambda: None)
    monkeypatch.setattr(host, "verify_state_parent", lambda: None)
    monkeypatch.setattr(host, "_install_lock", install_lock)
    monkeypatch.setattr(host, "_activation_lock", activation_lock)
    monkeypatch.setattr(host, "_load_json", lambda *_args: {"candidate_sha": SHA})
    monkeypatch.setattr(host, "_identity", lambda user, _group: _current_identity(user))
    monkeypatch.setattr(
        host,
        "_sandbox_batch_identity",
        lambda sandbox: _current_identity(f"loom-sandbox-{sandbox}"),
    )
    monkeypatch.setattr(host, "verify_candidate_root", lambda *_args: None)
    monkeypatch.setattr(host, "verify_candidate", lambda *_args: "b" * 40)
    monkeypatch.setattr(host, "verify_worker_runtime_env", lambda *_args: None)
    monkeypatch.setattr(
        host,
        "_renew_attestation_locked",
        lambda *_args, **_kwargs: renewed.set(),
    )

    def run_timer() -> None:
        try:
            host.renew_attestations((profile,), execute=True)
        except BaseException as exc:
            failures.append(exc)

    gate.acquire()
    worker = threading.Thread(target=run_timer)
    worker.start()
    assert attempted.wait(timeout=1)
    assert not renewed.is_set()
    gate.release()
    worker.join(timeout=1)

    assert not worker.is_alive()
    assert renewed.is_set()
    assert not failures


def test_rollback_after_attestation_ttl_renews_before_positive_readiness(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    profile = _temporary_profile(tmp_path)
    current_sha = "b" * 40
    target_sha = "c" * 40
    target_tree = "d" * 40
    current = {
        "schema_version": 1,
        "sandbox": profile.sandbox,
        "candidate_sha": current_sha,
        "previous_sha": target_sha,
    }
    monkeypatch.setattr(host, "DESIRED_ROOT", tmp_path / "desired")
    monkeypatch.setattr(host, "TRANSACTION_ROOT", tmp_path / "transactions")
    profile.desired_file.parent.mkdir(parents=True)
    profile.desired_file.write_text(json.dumps(current), encoding="utf-8")
    profile.desired_file.chmod(0o600)
    events: list[str] = []

    @contextmanager
    def lock(*_args: object) -> object:
        yield

    monkeypatch.setattr(host, "_require_live_host", lambda: None)
    monkeypatch.setattr(host, "verify_nfs_mount", lambda: None)
    monkeypatch.setattr(host, "verify_state_parent", lambda: None)
    monkeypatch.setattr(host, "_install_lock", lock)
    monkeypatch.setattr(host, "_activation_lock", lock)
    monkeypatch.setattr(
        host,
        "_ensure_root_private_directory",
        lambda path: path.mkdir(parents=True, exist_ok=True),
    )
    monkeypatch.setattr(host, "_identity", lambda user, _group: _current_identity(user))
    monkeypatch.setattr(
        host,
        "_sandbox_batch_identity",
        lambda sandbox: _current_identity(f"loom-sandbox-{sandbox}"),
    )
    monkeypatch.setattr(
        host,
        "verify_candidate",
        lambda _profile, path, _sha, _identity: (
            target_tree if path.name == target_sha else "e" * 40
        ),
    )
    monkeypatch.setattr(host, "_migration_tree", lambda *_args: "same")
    monkeypatch.setattr(host, "verify_worker_runtime_env", lambda *_args: None)
    expired = _receipt(
        profile,
        target_sha,
        expires_at=datetime.now(UTC) - timedelta(hours=1),
    )

    def archived(*_args: object, **_kwargs: object) -> host.ActivationReceipt:
        events.append("archive")
        return expired

    monkeypatch.setattr(host, "_verify_archived_activation", archived)
    monkeypatch.setattr(host, "_current_relay_sha", lambda _profile: current_sha)
    monkeypatch.setattr(
        host,
        "_run",
        lambda command, **_kwargs: events.append(f"systemd:{command[-1]}"),
    )
    monkeypatch.setattr(
        host,
        "_restore_relay",
        lambda _profile, sha, _transaction_sha: events.append(f"relay:{sha}"),
    )
    fresh = _receipt(profile, target_sha)

    def renew(
        selected: host.Profile,
        *,
        sha: str,
        tree: str,
    ) -> host.ActivationReceipt:
        events.append("renew")
        host.write_desired(selected, sha, tree, fresh)
        return fresh

    monkeypatch.setattr(host, "_renew_attestation_locked", renew)

    def check(_sandbox: str) -> None:
        persisted = json.loads(profile.desired_file.read_text(encoding="utf-8"))
        assert persisted["combined_receipt_sha256"] == fresh.payload_sha256
        transaction = host._transaction_payload(profile)
        assert transaction["phase"] == "domains-proved"
        assert transaction["target_receipt_sha256"] == fresh.payload_sha256
        events.append("fresh-check")

    monkeypatch.setattr(host, "service_check", check)

    host.rollback(profile, target_sha)

    assert events == [
        "archive",
        f"systemd:{host.UNIT_NAME.format(sandbox=profile.sandbox)}",
        f"relay:{target_sha}",
        "renew",
        "fresh-check",
    ]
    assert (
        json.loads(profile.desired_file.read_text(encoding="utf-8"))["candidate_sha"] == target_sha
    )
    assert not host._transaction_file(profile).exists()


def test_rollback_crash_after_target_renew_recovers_and_can_retry(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class SimulatedCrash(BaseException):
        pass

    profile = _temporary_profile(tmp_path)
    current_sha = "b" * 40
    target_sha = "c" * 40
    target_tree = "d" * 40
    current_tree = "e" * 40
    previous = {
        "schema_version": 1,
        "sandbox": profile.sandbox,
        "candidate_sha": current_sha,
        "candidate_tree": current_tree,
        "previous_sha": target_sha,
    }
    replacement = {
        "schema_version": 1,
        "sandbox": profile.sandbox,
        "candidate_sha": target_sha,
        "candidate_tree": target_tree,
        "previous_sha": current_sha,
    }
    monkeypatch.setattr(host, "DESIRED_ROOT", tmp_path / "desired")
    monkeypatch.setattr(host, "TRANSACTION_ROOT", tmp_path / "transactions")
    profile.desired_file.parent.mkdir(parents=True)
    profile.desired_file.write_text(json.dumps(previous), encoding="utf-8")
    profile.desired_file.chmod(0o600)

    @contextmanager
    def lock(*_args: object) -> object:
        yield

    monkeypatch.setattr(host, "_require_live_host", lambda: None)
    monkeypatch.setattr(host, "verify_nfs_mount", lambda: None)
    monkeypatch.setattr(host, "verify_state_parent", lambda: None)
    monkeypatch.setattr(host, "_install_lock", lock)
    monkeypatch.setattr(host, "_activation_lock", lock)
    monkeypatch.setattr(
        host,
        "_ensure_root_private_directory",
        lambda path: path.mkdir(parents=True, exist_ok=True),
    )
    monkeypatch.setattr(host, "_identity", lambda user, _group: _current_identity(user))
    monkeypatch.setattr(
        host,
        "verify_candidate",
        lambda _profile, path, _sha, _identity: (
            target_tree if path.name == target_sha else "e" * 40
        ),
    )
    monkeypatch.setattr(host, "_migration_tree", lambda *_args: "same")
    monkeypatch.setattr(host, "verify_worker_runtime_env", lambda *_args: None)
    monkeypatch.setattr(
        host,
        "_verify_archived_activation",
        lambda *_args: _receipt(profile, target_sha),
    )
    monkeypatch.setattr(host, "_desired_payload", lambda *_args, **_kwargs: replacement)
    monkeypatch.setattr(host, "_current_relay_sha", lambda _profile: current_sha)
    monkeypatch.setattr(host, "_run", lambda *_args, **_kwargs: None)
    target_fresh = _receipt(profile, target_sha)
    monkeypatch.setattr(
        host,
        "_renew_attestation_locked",
        lambda *_args, **_kwargs: target_fresh,
    )
    switched: list[str] = []
    monkeypatch.setattr(
        host,
        "_restore_relay",
        lambda _profile, sha, _transaction_sha: switched.append(str(sha)),
    )
    write_transaction = host._write_transaction

    def write_then_crash(*args: object, **kwargs: object) -> None:
        write_transaction(*args, **kwargs)
        if kwargs["phase"] == "domains-proved":
            raise SimulatedCrash

    monkeypatch.setattr(host, "_write_transaction", write_then_crash)

    with pytest.raises(SimulatedCrash):
        host.rollback(profile, target_sha)

    assert json.loads(profile.desired_file.read_text(encoding="utf-8")) == replacement
    transaction = host._transaction_payload(profile)
    assert transaction is not None
    assert transaction["operation"] == "rollback"
    assert transaction["phase"] == "domains-proved"
    assert transaction["target_receipt_sha256"] == target_fresh.payload_sha256
    assert switched == [target_sha]
    events: list[str] = []
    monkeypatch.setattr(
        host,
        "_invalidate_exact_live_receipt",
        lambda _profile, sha, _tree, *, journal_digest: events.append(
            f"invalidate:{sha}:{journal_digest}",
        ),
    )
    monkeypatch.setattr(
        host,
        "_invoke_lifecycle",
        lambda _profile, sha, operation: events.append(f"{operation}:{sha}"),
    )
    monkeypatch.setattr(
        host,
        "_restore_relay",
        lambda _profile, sha, _target: events.append(f"relay:{sha}"),
    )
    monkeypatch.setattr(
        host,
        "_invalidate_receipt",
        lambda *_args: events.append("unexpected-invalidate"),
    )
    monkeypatch.setattr(
        host,
        "_renew_attestation_locked",
        lambda _profile, *, sha, tree: (
            events.append(f"renew:{sha}:{tree}") or _receipt(profile, sha)
        ),
    )

    host._recover_transaction(profile, transaction)

    assert json.loads(profile.desired_file.read_text(encoding="utf-8")) == previous
    assert events == [
        f"invalidate:{target_sha}:{target_fresh.payload_sha256}",
        f"update:{current_sha}",
        f"relay:{current_sha}",
        f"renew:{current_sha}:{current_tree}",
    ]
    assert not host._transaction_file(profile).exists()

    monkeypatch.setattr(host, "_write_transaction", write_transaction)
    monkeypatch.setattr(host, "_restore_relay", lambda *_args: None)
    monkeypatch.setattr(
        host,
        "_renew_attestation_locked",
        lambda *_args, **_kwargs: _receipt(profile, target_sha),
    )
    monkeypatch.setattr(host, "service_check", lambda *_args: None)

    host.rollback(profile, target_sha)

    assert not host._transaction_file(profile).exists()


def test_failed_post_renew_recovery_invalidates_target_then_refreshes_current(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    profile = _temporary_profile(tmp_path)
    current_sha = "b" * 40
    current_tree = "c" * 40
    target_sha = "d" * 40
    target_tree = "e" * 40
    target_digest = "f" * 64
    previous = {
        "candidate_sha": current_sha,
        "candidate_tree": current_tree,
    }
    events: list[str] = []
    monkeypatch.setattr(
        host,
        "_invalidate_exact_live_receipt",
        lambda _profile, sha, tree, *, journal_digest: events.append(
            f"invalidate:{sha}:{tree}:{journal_digest}",
        ),
    )
    monkeypatch.setattr(
        host,
        "_atomic_write",
        lambda *_args, **_kwargs: events.append("restore-desired"),
    )
    monkeypatch.setattr(
        host,
        "_invoke_lifecycle",
        lambda _profile, sha, operation: events.append(f"{operation}:{sha}"),
    )
    monkeypatch.setattr(
        host,
        "_restore_relay",
        lambda _profile, sha, _target: events.append(f"relay:{sha}"),
    )
    monkeypatch.setattr(
        host,
        "_renew_attestation_locked",
        lambda _profile, *, sha, tree: (
            events.append(f"renew:{sha}:{tree}") or _receipt(profile, sha)
        ),
    )
    monkeypatch.setattr(
        host,
        "_remove_transaction",
        lambda _profile: events.append("remove-journal"),
    )

    host._recover_transaction(
        profile,
        {
            "operation": "rollback",
            "candidate_sha": target_sha,
            "candidate_tree": target_tree,
            "phase": "domains-proved",
            "previous_desired": previous,
            "previous_relay_sha": current_sha,
            "target_receipt_sha256": target_digest,
        },
    )

    assert events == [
        f"invalidate:{target_sha}:{target_tree}:{target_digest}",
        "restore-desired",
        f"update:{current_sha}",
        f"relay:{current_sha}",
        f"renew:{current_sha}:{current_tree}",
        "remove-journal",
    ]


def test_failed_current_refresh_invalidates_current_and_keeps_journal(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    profile = _temporary_profile(tmp_path)
    current_sha = "b" * 40
    current_tree = "c" * 40
    target_sha = "d" * 40
    target_tree = "e" * 40
    invalidated: list[str] = []
    monkeypatch.setattr(
        host,
        "_invalidate_exact_live_receipt",
        lambda _profile, sha, _tree, *, journal_digest: invalidated.append(sha),
    )
    monkeypatch.setattr(host, "_atomic_write", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(host, "_invoke_lifecycle", lambda *_args: None)
    monkeypatch.setattr(host, "_restore_relay", lambda *_args: None)
    monkeypatch.setattr(
        host,
        "_renew_attestation_locked",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            host.HostConvergeError("current proof unavailable"),
        ),
    )
    monkeypatch.setattr(
        host,
        "_remove_transaction",
        lambda _profile: pytest.fail("failed recovery must retain its journal"),
    )

    with pytest.raises(host.HostConvergeError, match="current proof unavailable"):
        host._recover_transaction(
            profile,
            {
                "operation": "rollback",
                "candidate_sha": target_sha,
                "candidate_tree": target_tree,
                "phase": "domains-proved",
                "previous_desired": {
                    "candidate_sha": current_sha,
                    "candidate_tree": current_tree,
                },
                "previous_relay_sha": current_sha,
                "target_receipt_sha256": "f" * 64,
            },
        )

    assert invalidated == [target_sha, current_sha]


def test_exact_live_receipt_invalidation_is_idempotent_and_preserves_foreign(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    profile = _temporary_profile(tmp_path)
    target_sha = "b" * 40
    target_tree = "c" * 40
    monkeypatch.setattr(host, "COMBINED_RECEIPT_ROOT", tmp_path / "attestations")
    monkeypatch.setattr(host, "DESIRED_ROOT", tmp_path / "desired")
    path = host.combined_receipt_path(profile, target_sha)
    path.parent.mkdir(parents=True, mode=0o700)

    def write(candidate_sha: str) -> str:
        unsigned = {
            "schema_version": 1,
            "kind": "loom.developer-runtime-combined-activation",
            "sandbox": profile.sandbox,
            "candidate_sha": candidate_sha,
            "candidate_tree": target_tree,
        }
        payload = {
            **unsigned,
            "payload_sha256": hashlib.sha256(
                json.dumps(
                    unsigned,
                    sort_keys=True,
                    separators=(",", ":"),
                    ensure_ascii=True,
                ).encode(),
            ).hexdigest(),
        }
        path.write_text(
            json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n",
            encoding="utf-8",
        )
        path.chmod(0o600)
        return str(payload["payload_sha256"])

    write("d" * 40)
    with pytest.raises(host.HostConvergeError, match="does not match"):
        host._invalidate_exact_live_receipt(
            profile,
            target_sha,
            target_tree,
            journal_digest=None,
        )
    assert path.exists()

    exact_digest = write(target_sha)
    profile.desired_file.parent.mkdir(parents=True)
    profile.desired_file.write_text(
        json.dumps(
            {
                "candidate_sha": target_sha,
                "candidate_tree": target_tree,
                "combined_receipt_sha256": exact_digest,
            },
        ),
        encoding="utf-8",
    )
    profile.desired_file.chmod(0o600)
    host._invalidate_exact_live_receipt(
        profile,
        target_sha,
        target_tree,
        journal_digest="0" * 64,
    )
    host._invalidate_exact_live_receipt(
        profile,
        target_sha,
        target_tree,
        journal_digest=None,
    )
    assert not path.exists()


def test_service_converge_leaves_pending_rollback_for_link_and_fresh_proof(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    profile = _temporary_profile(tmp_path)
    desired = {"candidate_sha": SHA}
    locked = False
    events: list[str] = []
    transaction = {
        "operation": "rollback",
        "candidate_sha": SHA,
        "candidate_tree": "b" * 40,
        "phase": "desired-written",
        "previous_desired": {"candidate_sha": "c" * 40},
        "previous_relay_sha": "c" * 40,
    }

    @contextmanager
    def activation_lock(selected: host.Profile) -> object:
        nonlocal locked
        assert selected == profile
        locked = True
        try:
            yield
        finally:
            locked = False

    monkeypatch.setattr(host, "_require_live_host", lambda: None)
    monkeypatch.setattr(host, "verify_nfs_mount", lambda: None)
    monkeypatch.setattr(host, "verify_state_parent", lambda: None)
    monkeypatch.setattr(host, "_desired_for_service", lambda _sandbox: (profile, desired))
    monkeypatch.setattr(host, "_activation_lock", activation_lock)
    monkeypatch.setattr(host, "_transaction_payload", lambda _profile: transaction)

    def converge(selected: host.Profile, state: object) -> None:
        if not locked or selected != profile or state != desired:
            pytest.fail("service convergence bypassed the activation lock")
        events.append("converge")

    monkeypatch.setattr(host, "_service_converge_locked", converge)
    monkeypatch.setattr(
        host,
        "_write_transaction",
        lambda *_args, **kwargs: events.append(f"write:{kwargs['phase']}"),
    )
    monkeypatch.setattr(
        host,
        "_remove_transaction",
        lambda _profile: events.append("remove"),
    )

    host.service_converge(profile.sandbox)

    assert not locked
    assert events == ["converge"]


def test_empty_namespace_first_install_materializes_before_prepare_and_attest(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    profile = _temporary_profile(tmp_path)
    source_tree = "b" * 40
    events: list[str] = []
    secrets = {
        "LOOM_DEV_POSTGRES_PASSWORD": "postgres",
        "LOOM_DEV_MINIO_ROOT_PASSWORD": "minio",
        "LOOM_CP_STEP_JWT_SIGNING_KEY": "jwt",
        "LOOM_SECRET_STORE_MASTER_KEY": "master",
        "LOOM_WORKER_TOKEN": "worker",
    }

    @contextmanager
    def lock(*_args: object) -> object:
        yield

    monkeypatch.setattr(host, "_identity", lambda user, _group: _current_identity(user))
    monkeypatch.setattr(
        host,
        "_sandbox_batch_identity",
        lambda sandbox: _current_identity(f"loom-sandbox-{sandbox}"),
    )
    monkeypatch.setattr(
        host,
        "_bootstrap_domain_runtime_hosts",
        lambda *_args: events.append("bootstrap-empty-domain-roots"),
    )
    monkeypatch.setattr(host, "verify_developer_docker_access", lambda *_args: None)
    monkeypatch.setattr(host, "ensure_secret_files", lambda *_args: None)
    monkeypatch.setattr(
        host,
        "_materialize_domain_candidates",
        lambda *_args: events.append("materialize-no-fleet"),
    )
    monkeypatch.setattr(host, "verify_candidate_root", lambda *_args: None)

    def verify(*_args: object) -> str:
        assert events[:2] == ["bootstrap-empty-domain-roots", "materialize-no-fleet"]
        events.append("verify-20-node-candidate")
        return source_tree

    monkeypatch.setattr(host, "verify_candidate", verify)
    monkeypatch.setattr(
        host,
        "_install_assets",
        lambda *_args: events.append("install-exact-assets"),
    )
    monkeypatch.setattr(host, "verify_candidate_profile_bytes", lambda *_args: None)
    monkeypatch.setattr(host, "verify_candidate_consumer", lambda *_args: None)
    monkeypatch.setattr(host, "require_migration_compatible_update", lambda *_args: None)
    monkeypatch.setattr(host, "_converge_domain_runtime_hosts", lambda *_args: None)
    monkeypatch.setattr(host, "_parse_env_file", lambda *_args: secrets)
    monkeypatch.setattr(host, "_read_admin_token", lambda *_args: "admin")
    monkeypatch.setattr(host, "_activation_lock", lock)
    monkeypatch.setattr(host, "_transaction_payload", lambda *_args: None)
    monkeypatch.setattr(host, "_load_json", lambda *_args: None)
    monkeypatch.setattr(host, "_current_relay_sha", lambda *_args: None)
    monkeypatch.setattr(host, "_write_transaction", lambda *args, **kwargs: None)
    monkeypatch.setattr(host, "_assert_capacity_units_stopped", lambda *_args: None)
    monkeypatch.setattr(
        host,
        "_invoke_lifecycle",
        lambda _profile, _sha, operation: events.append(f"lifecycle:{operation}"),
    )
    monkeypatch.setattr(host, "verify_listening_ports", lambda *_args: None)
    monkeypatch.setattr(host, "assert_capacity_quiescent", lambda *_args: None)
    monkeypatch.setattr(
        host,
        "_install_remote_link_fleet",
        lambda *_args: events.append("fleet-after-prepare"),
    )

    def attest(*_args: object) -> None:
        assert "fleet-after-prepare" in events
        events.append("attest-after-fleet")

    monkeypatch.setattr(host, "_publish_domain_attestations", attest)
    monkeypatch.setattr(host, "verify_worker_runtime_env", lambda *_args: None)
    monkeypatch.setattr(
        host,
        "verify_combined_receipt",
        lambda *_args: _receipt(profile, SHA),
    )
    monkeypatch.setattr(host, "_archive_runtime_attestation", lambda *_args: None)
    monkeypatch.setattr(host, "write_desired", lambda *_args: None)
    monkeypatch.setattr(
        host,
        "_run",
        lambda *_args, **_kwargs: type("Completed", (), {"stdout": "", "returncode": 0})(),
    )
    monkeypatch.setattr(host, "service_check", lambda *_args: None)
    monkeypatch.setattr(host, "_remove_transaction", lambda *_args: None)

    host._install_materialized(
        (profile,),
        SHA,
        tmp_path / "candidate.bundle",
        source_tree,
    )

    assert events.index("bootstrap-empty-domain-roots") < events.index(
        "materialize-no-fleet",
    )
    assert events.index("materialize-no-fleet") < events.index("lifecycle:prepare")
    assert events.index("lifecycle:prepare") < events.index("fleet-after-prepare")
    assert events.index("fleet-after-prepare") < events.index("attest-after-fleet")


def test_remote_link_fleet_reads_each_client_from_issuance_clients_directory(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    profile = _dynamic_profile(tmp_path)
    issuance_root = tmp_path / "issuance"
    sources: list[Path] = []
    monkeypatch.setattr(host, "REMOTE_LINK_ISSUANCE_ROOT", issuance_root)
    monkeypatch.setattr(
        host,
        "_parse_env_file",
        lambda *_args: {
            "LOOM_WORKER_TOKEN": "worker",
            "LOOM_DEV_MINIO_ROOT_USER": "access",
            "LOOM_DEV_MINIO_ROOT_PASSWORD": "secret",
        },
    )
    monkeypatch.setattr(host, "_run", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(host, "_verify_remote_candidate", lambda *_args: None)

    def archive(source: Path, **_kwargs: object) -> bytes:
        sources.append(source)
        return b"credentials"

    monkeypatch.setattr(host, "_archive_credentials", archive)
    monkeypatch.setattr(
        host,
        "_node_authority",
        lambda *_args, **_kwargs: {"status": "succeeded"},
    )
    fleet_calls: list[tuple[str, str]] = []
    monkeypatch.setattr(
        host,
        "_collect_and_persist_remote_link_fleet",
        lambda _profile, sha, tree: fleet_calls.append((sha, tree)) or {},
    )

    host._install_remote_link_fleet(
        profile,
        SHA,
        "b" * 40,
        _current_identity("root"),
    )

    assert sources == [
        issuance_root / profile.sandbox / SHA / "clients" / node
        for node in host.ELIGIBLE_LINK_NODES
    ]
    assert fleet_calls == [(SHA, "b" * 40)]


def _link_client_report(
    profile: host.Profile,
    node: str,
    *,
    ca_fingerprint: str = "sha256:" + "c" * 64,
) -> dict[str, object]:
    return {
        "schema_version": 1,
        "sandbox": profile.sandbox,
        "candidate_sha": SHA,
        "node": node,
        "route": "ok",
        "tls_version": "TLSv1.3",
        "services": {
            name: {
                "listener_port": host.LEGACY_SEED_REMOTE_LINK_SERVICE_PORTS[profile.sandbox][name][
                    0
                ],
                "target_port": host.LEGACY_SEED_REMOTE_LINK_SERVICE_PORTS[profile.sandbox][name][1],
                "health": "ok",
            }
            for name in host.REMOTE_LINK_SERVICE_NAMES
        },
        "client_uri_san": host._link_client_uri(profile, SHA),
        "secret_files": {
            name: {"uid": 0, "gid": 0, "mode": "0600", "present": True}
            for name in (
                "worker-token",
                "minio-access-key",
                "minio-secret-key",
                "client-key.pem",
            )
        },
        "ca_fingerprint": ca_fingerprint,
        "client_cert_fingerprint": "sha256:" + "d" * 64,
    }


def _link_server_report(profile: host.Profile) -> dict[str, object]:
    return {
        "node": "oldlab-2",
        "address": host.REMOTE_LINK_SERVER_ADDRESS,
        "unit": f"loom-developer-sandbox-link@{profile.sandbox}.service",
        "unit_active": True,
        "active_candidate_sha": SHA,
        "ca_fingerprint": "sha256:" + "c" * 64,
        "server_cert_fingerprint": "sha256:" + "e" * 64,
        "client_uri_san": host._link_client_uri(profile, SHA),
        "services": {
            name: {
                "listener_port": host.LEGACY_SEED_REMOTE_LINK_SERVICE_PORTS[profile.sandbox][name][
                    0
                ],
                "target_host": "127.0.0.1",
                "target_port": host.LEGACY_SEED_REMOTE_LINK_SERVICE_PORTS[profile.sandbox][name][1],
                "health_path": host.REMOTE_LINK_HEALTH_PATHS[name],
                "tls_version": "TLSv1.3",
                "status": "active",
            }
            for name in host.REMOTE_LINK_SERVICE_NAMES
        },
    }


def test_fleet_collection_uses_only_node_authority_and_persists_canonical_payload(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    profile = _dynamic_profile(tmp_path)
    tree = "b" * 40
    calls: list[tuple[str, str, dict[str, object]]] = []

    def authority_call(node: str, verb: str, envelope: bytes) -> dict[str, object]:
        request = json.loads(envelope)
        calls.append((node, verb, request))
        if request["action"] == "inspect-link-client":
            return {
                "schema_version": 1,
                "request_id": request["request_id"],
                "status": "succeeded",
                "result": _link_client_report(profile, node),
            }
        if request["action"] == "inspect-link-server":
            return {
                "schema_version": 1,
                "request_id": request["request_id"],
                "status": "succeeded",
                "result": _link_server_report(profile),
            }
        assert request["action"] == "persist-fleet-attestation"
        payload = base64.b64decode(str(request["payload_base64"]), validate=True)
        assert payload == (
            json.dumps(
                json.loads(payload),
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=True,
            ).encode("ascii")
            + b"\n"
        )
        return {
            "schema_version": 1,
            "request_id": request["request_id"],
            "action": request["action"],
            "node": node,
            "domain": "oldlab",
            "sandbox": profile.sandbox,
            "candidate_sha": SHA,
            "candidate_tree": tree,
            "env_id": profile.env_id,
            "resource_generation": profile.resource_generation,
            "candidate_id": profile.candidate_id,
            "registry_generation": profile.registry_generation,
            "registry_payload_sha256": profile.registry_payload_sha256,
            "payload_sha256": hashlib.sha256(payload).hexdigest(),
            "result_sha256": "f" * 64,
            "inner_receipt": None,
            "completed_at": datetime.now(UTC).isoformat(),
            "status": "succeeded",
        }

    monkeypatch.setattr(host, "_node_authority", authority_call)
    fleet = host._collect_and_persist_remote_link_fleet(profile, SHA, tree)

    assert [request["action"] for _, _, request in calls].count(
        "inspect-link-client",
    ) == len(host.ELIGIBLE_LINK_NODES)
    assert [request["action"] for _, _, request in calls].count(
        "inspect-link-server",
    ) == 1
    assert [request["action"] for _, _, request in calls].count(
        "persist-fleet-attestation",
    ) == 1
    assert all(
        verb == "check" for _, verb, request in calls if request["action"].startswith("inspect-")
    )
    assert calls[-1][0:2] == ("oldlab-2", "transact")
    assert fleet["eligible_nodes"] == list(host.ELIGIBLE_LINK_NODES)
    assert fleet["payload_sha256"] == host._fleet_attestation_digest(fleet)


def test_fleet_collection_rejects_one_cross_bound_client(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    profile = _dynamic_profile(tmp_path)

    def authority_call(node: str, _verb: str, envelope: bytes) -> dict[str, object]:
        request = json.loads(envelope)
        report = _link_client_report(profile, node)
        if node == "trt-gb10-15":
            report["node"] = "trt-gb10-14"
        return {
            "schema_version": 1,
            "request_id": request["request_id"],
            "status": "succeeded",
            "result": report,
        }

    monkeypatch.setattr(host, "_node_authority", authority_call)
    with pytest.raises(host.HostConvergeError, match="trt-gb10-15"):
        host._collect_and_persist_remote_link_fleet(profile, SHA, "b" * 40)


def test_install_and_renew_share_the_same_fleet_collector(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    profile = _temporary_profile(tmp_path)
    tree = "b" * 40
    calls: list[str] = []
    receipt = _receipt(profile, SHA)
    monkeypatch.setattr(
        host,
        "_collect_and_persist_remote_link_fleet",
        lambda *_args: calls.append("fleet") or {},
    )
    monkeypatch.setattr(
        host,
        "_publish_domain_attestations",
        lambda *_args: calls.append("domains"),
    )
    monkeypatch.setattr(host, "verify_combined_receipt", lambda *_args: receipt)
    monkeypatch.setattr(host, "_archive_runtime_attestation", lambda *_args: None)
    monkeypatch.setattr(host, "write_desired", lambda *_args: None)

    assert host._renew_attestation_locked(profile, sha=SHA, tree=tree) == receipt
    assert calls == ["fleet", "domains"]


def test_node_authority_uses_only_the_two_fixed_sudo_commands(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[tuple[str, ...], bytes | None]] = []

    def run(argv: tuple[str, ...], **kwargs: object) -> object:
        calls.append((argv, kwargs.get("input")))
        return type(
            "Completed",
            (),
            {
                "returncode": 0,
                "stdout": b'{"status":"succeeded"}\n',
                "stderr": b"",
            },
        )()

    monkeypatch.setattr(host.subprocess, "run", run)
    profile = replace(
        next(item for item in host.load_profiles() if item.sandbox == "qianyi"),
        env_id="env-qianyi-000000000001",
        resource_generation=7,
        registry_generation=11,
        registry_payload_sha256="c" * 64,
        candidate_id="cand-qianyi-000000000001",
        candidate_tree="b" * 40,
        service_user="loom-dev-qianyi",
        worker_image_ids={
            "oldlab": "sha256:" + "d" * 64,
            "gb10": "sha256:" + "e" * 64,
        },
    )
    envelope = host._node_authority_envelope(
        action="host-converge",
        node="oldlab-1",
        domain="oldlab",
        sandbox="qianyi",
        sha=SHA,
        tree="b" * 40,
        profile=profile,
    )

    for verb in ("transact", "check"):
        host._node_authority("oldlab-1", verb, envelope)

    assert [argv for argv, _input in calls] == [
        (
            "/usr/local/libexec/loom-developer-sandbox-node-transport",
            "invoke",
            "--node",
            "oldlab-1",
            "--verb",
            verb,
        )
        for verb in ("transact", "check")
    ]
    assert all(input_bytes == envelope for _argv, input_bytes in calls)
    forbidden = {"install", "tar", "rm", "chown", "chmod", "python3"}
    assert all(not forbidden.intersection(argv) for argv, _input in calls)


def test_domain_attest_sends_fixed_worker_and_fleet_seed_archive(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    profile = _dynamic_profile(tmp_path)
    tree = "b" * 40
    fleet_root = tmp_path / "fleet"
    fleet = fleet_root / profile.sandbox / SHA / "fleet.json"
    fleet.parent.mkdir(parents=True)
    fleet.write_bytes(b'{"fleet":"proof"}\n')
    original_lstat = Path.lstat
    envelopes: list[dict[str, object]] = []
    collect_calls: list[tuple[str, ...]] = []

    def lstat(path: Path) -> object:
        if path == fleet:
            return SimpleNamespace(
                st_mode=stat.S_IFREG | 0o600,
                st_uid=0,
                st_gid=0,
                st_nlink=1,
            )
        return original_lstat(path)

    def authority_call(_node: str, _verb: str, envelope: bytes) -> dict[str, object]:
        envelopes.append(json.loads(envelope))
        return {"status": "succeeded"}

    monkeypatch.setattr(Path, "lstat", lstat)
    monkeypatch.setattr(host, "FLEET_ATTESTATION_ROOT", fleet_root)
    monkeypatch.setattr(
        host,
        "_worker_env_seed",
        lambda _profile, _sha, domain: f"POOL={domain}\n".encode(),
    )
    monkeypatch.setattr(host, "_node_authority", authority_call)
    monkeypatch.setattr(
        host,
        "_run",
        lambda args, **_kwargs: collect_calls.append(tuple(args)) or None,
    )

    host._publish_domain_attestations(profile, SHA, tree)

    assert len(envelopes) == 2
    for envelope, domain in zip(envelopes, ("oldlab", "gb10"), strict=True):
        assert envelope["payload_kind"] == "attestation-seed"
        payload = base64.b64decode(str(envelope["payload_base64"]), validate=True)
        with tarfile.open(fileobj=io.BytesIO(payload), mode="r:") as archive:
            assert {member.name for member in archive.getmembers()} == {
                "worker.env",
                "fleet.json",
            }
            assert archive.extractfile("worker.env").read() == f"POOL={domain}\n".encode()
            assert archive.extractfile("fleet.json").read() == b'{"fleet":"proof"}\n'
    assert "--candidate-tree" in collect_calls[0]
    assert tree in collect_calls[0]


@pytest.mark.parametrize("sandbox", host.LEGACY_SEED_RUNTIME_IDS)
def test_worker_env_seed_uses_checked_in_domain_capacity_contract(
    tmp_path: Path,
    sandbox: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    profile = _dynamic_profile(tmp_path, sandbox)
    monkeypatch.setattr(
        host,
        "INSTALLED_DOMAIN_RUNTIME_CONFIG",
        host.SOURCE_PROFILES / "runtime-domains.toml",
    )
    monkeypatch.setattr(
        host,
        "INSTALLED_CAPACITY_POLICY_ROOT",
        host.REPO_ROOT / "deploy/developer-sandboxes/shared-capacity-policies",
    )

    def parse(payload: bytes) -> dict[str, str]:
        return dict(line.split("=", 1) for line in payload.decode().splitlines())

    oldlab = parse(host._worker_env_seed(profile, SHA, "oldlab"))
    gb10 = parse(host._worker_env_seed(profile, SHA, "gb10"))

    assert oldlab["LOOM_WORKER_POOL_NAME"] == "oldlab"
    assert oldlab["LOOM_WORKER_MAX_CONCURRENT"] == "4"
    assert gb10["LOOM_WORKER_POOL_NAME"] == "gb10"
    assert gb10["LOOM_WORKER_MAX_CONCURRENT"] == "8"
    assert oldlab["LOOM_WORKER_IMAGE_ID"] == "sha256:" + "d" * 64
    assert gb10["LOOM_WORKER_IMAGE_ID"] == "sha256:" + "e" * 64
    assert oldlab["LOOM_WORKER_SANDBOX_IDENTITY"] == sandbox
    assert gb10["LOOM_WORKER_CANDIDATE_SHA"] == SHA
    legacy_ports = host.LEGACY_SEED_REMOTE_LINK_SERVICE_PORTS[sandbox]
    assert oldlab["LOOM_SANDBOX_LINK_CP_EXPECTED_PORT"] == str(
        legacy_ports["control-plane"][0],
    )
    assert oldlab["LOOM_SANDBOX_LINK_GATEWAY_EXPECTED_PORT"] == str(
        legacy_ports["gateway"][0],
    )
    assert oldlab["LOOM_SANDBOX_LINK_MINIO_EXPECTED_PORT"] == str(
        legacy_ports["minio"][0],
    )


def test_dynamic_worker_env_seed_uses_registry_allocated_link_ports(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    profile = _temporary_profile(tmp_path, "qianyi")
    dynamic = replace(
        profile,
        sandbox="dev-fourth-a1b2c3d4",
        compose_project="loom-env-fourth-a1b2c3d4",
        ports={
            **profile.ports,
            "relay_control_plane": 30080,
            "relay_gateway": 30100,
            "relay_minio": 30900,
        },
        env_id="denv-fourth-a1b2c3d4",
        resource_generation=2,
        registry_generation=9,
        registry_payload_sha256="f" * 64,
        candidate_id="cand-" + "a" * 40,
        candidate_tree="b" * 40,
        service_user="loom_env_fourth",
        worker_image_ids={
            "oldlab": "sha256:" + "d" * 64,
            "gb10": "sha256:" + "e" * 64,
        },
    )
    monkeypatch.setattr(
        host,
        "_worker_capacity_contract",
        lambda domain, *, installed: (domain, 4 if domain == "oldlab" else 8),
    )

    values = dict(
        line.split("=", 1)
        for line in host._worker_env_seed(dynamic, SHA, "oldlab").decode().splitlines()
    )

    assert values["LOOM_WORKER_SANDBOX_IDENTITY"] == dynamic.sandbox
    assert values["LOOM_SANDBOX_LINK_CP_UPSTREAM"].endswith(":30080")
    assert values["LOOM_SANDBOX_LINK_CP_EXPECTED_PORT"] == "30080"
    assert values["LOOM_SANDBOX_LINK_GATEWAY_EXPECTED_PORT"] == "30100"
    assert values["LOOM_SANDBOX_LINK_MINIO_EXPECTED_PORT"] == "30900"


def test_host_installer_and_profiles_require_the_full_ci_matrix() -> None:
    for changed_path in (
        "scripts/ops/developer_sandbox_host.py",
        "scripts/ops/developer_sandbox_node_authority.py",
        "tests/ops/test_developer_sandbox_node_authority.py",
        "deploy/developer-sandboxes/qianyi.toml",
        "deploy/developer-sandboxes/loom-developer-sandbox-node-authority.sudoers",
        "deploy/developer-sandboxes/loom-developer-sandbox@.service",
    ):
        plan = plan_validations(
            changed_paths=[changed_path],
            labels=set(),
            event_name="pull_request",
        )
        assert plan.selected_heavy_checks() == set(HEAVY_CHECKS)
        assert plan.unowned_runtime is False
        assert all("protected-developer-sandbox" in plan.reasons[name] for name in HEAVY_CHECKS)


def test_staging_allocation_config_separates_producer_batch_and_system_mount() -> None:
    config = host.load_staging_allocation_config(
        Path("deploy/developer-sandboxes/staging-external-slurm-authority.toml"),
    )

    assert (
        config.producer_user,
        config.producer_uid,
        config.producer_gid,
        config.producer_home,
        config.producer_shell,
    ) == (
        "loom-rollout",
        995,
        982,
        Path("/var/lib/loom-staging-rollout"),
        Path("/bin/sh"),
    )
    assert (
        config.batch_user,
        config.batch_uid,
        config.batch_gid,
        config.batch_home,
        config.batch_shell,
        config.batch_supplementary_groups,
    ) == (
        "loom-staging-worker",
        31024,
        31024,
        Path("/nonexistent"),
        Path("/usr/sbin/nologin"),
        ("docker",),
    )
    assert config.shared_mount_source == "192.168.20.12:/shared_work2/loom/staging"
    assert config.shared_mount_target == Path("/srv/loom/staging-shared")
    assert config.repository_root == config.shared_mount_target / "candidates"
    assert config.worker_env_root == config.shared_mount_target / "generated"
    assert config.result_root == config.shared_mount_target / "results"
    assert config.infrastructure_nodes == tuple(f"trt-gb10-{index}" for index in range(1, 16))
    assert config.allowed_nodes == config.infrastructure_nodes
    assert config.excluded_nodes == ()
    assert set(config.host_aliases) == set(config.infrastructure_nodes)
    assert config.host_aliases["trt-gb10-7"] == "gx10-0faf"
    assert "trt-gb10-7" in config.allowed_nodes


def test_staging_allocation_query_uses_only_fixed_cluster_and_parses_resources(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = host.load_staging_allocation_config(
        Path("deploy/developer-sandboxes/staging-external-slurm-authority.toml"),
    )
    commands: list[tuple[str, ...]] = []

    def fake_run(
        argv: tuple[str, ...] | list[str],
        **_kwargs: object,
    ) -> subprocess.CompletedProcess[str]:
        command = tuple(argv)
        commands.append(command)
        if command[0] == "squeue":
            return subprocess.CompletedProcess(
                command,
                0,
                (
                    "31415|loom827-staging-aaaaaaaaaaaa-trt-gb10-7-"
                    f"x{'d' * 64}|RUNNING|trt-gb10-7|loom-staging|"
                    "loom-staging-worker|None|loom-staging\n"
                ),
                "",
            )
        if command[0] == "sinfo":
            return subprocess.CompletedProcess(
                command,
                0,
                "trt-gb10-7|idle|20|128000|110000|0.25|0/20/0/20\n",
                "",
            )
        pytest.fail(f"unexpected query command: {command}")

    monkeypatch.setattr(host.os, "geteuid", lambda: 0)
    monkeypatch.setattr(host, "_staging_allocation_node", lambda _config: "trt-gb10-1")
    monkeypatch.setattr(
        host,
        "_staging_candidate_set_identity",
        lambda **_kwargs: ({"resource_generation": 7}, "c" * 64),
    )
    monkeypatch.setattr(host, "_run", fake_run)

    result = host.staging_allocation_query(
        config,
        candidate_sha=SHA,
        candidate_tree="b" * 40,
        request_id="6" * 64,
        job_ids=("31415",),
        nodes=("trt-gb10-7",),
    )

    assert result["cluster"] == "trt-gb10"
    assert result["jobs"][0]["job_id"] == "31415"
    assert result["jobs"][0]["nodelist"] == "trt-gb10-7"
    assert result["nodes"] == [
        {
            "hostname": "trt-gb10-7",
            "state": "idle",
            "cpus_total": 20,
            "free_memory_mib": 110000,
            "cpu_load": 0.25,
            "idle_cpus": 20,
        },
    ]
    assert all(f"--clusters={config.cluster}" in command for command in commands)
    assert [command[0] for command in commands] == ["squeue", "sinfo"]


@pytest.mark.parametrize("crash_point", ("after_wal", "after_sbatch"))
def test_staging_allocation_submit_recovers_without_duplicate_sbatch(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    crash_point: str,
) -> None:
    config = host.load_staging_allocation_config(
        Path("deploy/developer-sandboxes/staging-external-slurm-authority.toml"),
    )
    request_id = "6" * 64
    candidate_tree = "b" * 40
    candidate_set = "c" * 64
    wal_root = tmp_path / "wal"
    submitted = 0
    crashed = False
    prepare_calls: list[bool] = []

    def fake_atomic_write(path: Path, payload: bytes, *, mode: int) -> None:
        nonlocal crashed
        decoded = json.loads(payload)
        if crash_point == "after_sbatch" and decoded.get("phase") == "submitted" and not crashed:
            crashed = True
            raise RuntimeError("simulated crash after sbatch")
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(payload)
        path.chmod(mode)

    def fake_run(
        argv: tuple[str, ...] | list[str],
        **_kwargs: object,
    ) -> subprocess.CompletedProcess[str]:
        nonlocal crashed, submitted
        command = tuple(argv)
        if command[:2] == ("docker", "info"):
            return subprocess.CompletedProcess(command, 0, "systemd\n", "")
        if command[0] == "squeue":
            if crash_point == "after_wal" and not crashed:
                crashed = True
                raise RuntimeError("simulated crash after WAL")
            output = ""
            if crash_point == "after_sbatch" and submitted:
                job_name = next(
                    value.split("=", 1)[1]
                    for value in last_sbatch
                    if value.startswith("--job-name=")
                )
                output = (
                    f"27182|{job_name}|trt-gb10-8|loom-staging|"
                    f"loom-staging-worker|loom-staging|"
                    f"loom-cgroup-v1:pids=65536:r={request_id}\n"
                )
            return subprocess.CompletedProcess(command, 0, output, "")
        if command[0] == "sacct":
            return subprocess.CompletedProcess(command, 0, "", "")
        if command[0] == "sbatch":
            submitted += 1
            last_sbatch[:] = command
            return subprocess.CompletedProcess(command, 0, "27182;trt-gb10\n", "")
        pytest.fail(f"unexpected submit command: {command}")

    last_sbatch: list[str] = []
    monkeypatch.setattr(host, "STAGING_SUBMISSION_WAL_ROOT", wal_root)
    monkeypatch.setattr(
        host, "_ensure_root_private_directory", lambda path: path.mkdir(parents=True, exist_ok=True)
    )
    monkeypatch.setattr(host, "_read_combined_receipt_bytes", lambda path: path.read_bytes())
    monkeypatch.setattr(host, "_atomic_write", fake_atomic_write)
    monkeypatch.setattr(host.os, "geteuid", lambda: 0)
    monkeypatch.setattr(host, "_staging_allocation_node", lambda _config: "trt-gb10-1")
    monkeypatch.setattr(host, "_staging_identity_snapshot", lambda _config: {})
    monkeypatch.setattr(host, "_converge_staging_shared_namespace", lambda _config: {})
    monkeypatch.setattr(
        host,
        "_staging_candidate_binding",
        lambda *_args, **_kwargs: {"repository": {"path": "/candidate"}},
    )
    monkeypatch.setattr(
        host,
        "_staging_slurm_profile",
        lambda *_args: SimpleNamespace(docker_cgroup_driver="systemd"),
    )
    monkeypatch.setattr(
        host,
        "_staging_candidate_set_identity",
        lambda **_kwargs: ({"resource_generation": 7}, candidate_set),
    )
    monkeypatch.setattr(
        host,
        "_staging_slurm_live_config",
        lambda _config: {
            "cluster": "trt-gb10",
            "account": "loom-staging",
            "qos": "loom-staging",
        },
    )
    monkeypatch.setattr(
        host,
        "_staging_prepare_result_directory",
        lambda *_args, allow_existing=False, **_kwargs: (
            prepare_calls.append(allow_existing) or tmp_path
        ),
    )
    monkeypatch.setattr(host, "_run", fake_run)

    with pytest.raises(RuntimeError, match="simulated crash"):
        host._staging_allocation_submit_locked(
            config,
            candidate_sha=SHA,
            candidate_tree=candidate_tree,
            request_id=request_id,
            requested_node="trt-gb10-8",
        )
    result = host._staging_allocation_submit_locked(
        config,
        candidate_sha=SHA,
        candidate_tree=candidate_tree,
        request_id=request_id,
        requested_node="trt-gb10-8",
    )

    assert result["job_id"] == "27182"
    assert submitted == 1
    assert prepare_calls == [False, True]
    assert json.loads((wal_root / f"{request_id}.json").read_bytes())["phase"] == "submitted"
    assert f"--comment=loom-cgroup-v1:pids=65536:r={request_id}" in last_sbatch


def test_staging_allocation_submit_replays_completed_wal_without_slurm(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """A durable submitted WAL must return without squeue/sbatch/sacct."""
    config = host.load_staging_allocation_config(
        Path("deploy/developer-sandboxes/staging-external-slurm-authority.toml"),
    )
    request_id = "7" * 64
    candidate_tree = "b" * 40
    candidate_set = "c" * 64
    wal_root = tmp_path / "wal"
    wal_root.mkdir(parents=True)
    job_name = host._staging_allocation_job_name(
        candidate_sha=SHA,
        node="trt-gb10-8",
        candidate_set_sha256=candidate_set,
        resource_generation=7,
        request_id=request_id,
    )
    wrapped = " ".join(
        (
            "/usr/bin/srun",
            "--nodes=1",
            "--ntasks=1",
            "--nodelist=trt-gb10-8",
            str(host.INSTALLED_PROGRAM),
            "staging-allocation-worker",
            "--candidate-sha",
            SHA,
            "--candidate-tree",
            candidate_tree,
            "--request-id",
            request_id,
        )
    )
    result = {
        "schema_version": 1,
        "kind": "staging_external_slurm_allocation_submission",
        "request_id": request_id,
        "candidate_sha": SHA,
        "candidate_tree": candidate_tree,
        "job_id": "27182",
        "job_name": job_name,
        "candidate_set_sha256": candidate_set,
        "resource_generation": 7,
        "docker_cgroup_driver": "systemd",
        "node": "trt-gb10-8",
        "cluster": "trt-gb10",
        "account": "loom-staging",
        "qos": "loom-staging",
        "user": "loom-staging-worker",
        "uid": 31024,
        "gid": 31024,
        "service_identity": {},
        "mount": {},
        "submitted_at": "2026-07-30T00:00:00Z",
        "status": "submitted",
    }
    wal = {
        "schema_version": 1,
        "kind": "staging_external_slurm_submission_wal",
        "candidate_sha": SHA,
        "candidate_tree": candidate_tree,
        "request_id": request_id,
        "requested_node": "trt-gb10-8",
        "candidate_set_sha256": candidate_set,
        "resource_generation": 7,
        "job_name": job_name,
        "cluster": config.cluster,
        "partition": config.partition,
        "account": config.slurm_account,
        "qos": config.qos,
        "user": config.batch_user,
        "wrapped": wrapped,
        "phase": "submitted",
        "prepared_at": "2026-07-30T00:00:00Z",
        "result": result,
        "result_sha256": hashlib.sha256(host._staging_submission_wal_bytes(result)).hexdigest(),
    }
    unsigned = {key: value for key, value in wal.items() if key != "payload_sha256"}
    wal["payload_sha256"] = hashlib.sha256(host._staging_submission_wal_bytes(unsigned)).hexdigest()
    (wal_root / f"{request_id}.json").write_bytes(host._staging_submission_wal_bytes(wal))

    def fake_run(argv: tuple[str, ...] | list[str], **_kwargs: object) -> subprocess.CompletedProcess[str]:
        command = tuple(argv)
        if command[:2] == ("docker", "info"):
            return subprocess.CompletedProcess(command, 0, "systemd\n", "")
        pytest.fail(f"completed WAL replay must not contact Slurm: {command}")

    monkeypatch.setattr(host, "STAGING_SUBMISSION_WAL_ROOT", wal_root)
    monkeypatch.setattr(
        host, "_ensure_root_private_directory", lambda path: path.mkdir(parents=True, exist_ok=True)
    )
    monkeypatch.setattr(host, "_read_combined_receipt_bytes", lambda path: path.read_bytes())
    monkeypatch.setattr(host.os, "geteuid", lambda: 0)
    monkeypatch.setattr(host, "_staging_allocation_node", lambda _config: "trt-gb10-1")
    monkeypatch.setattr(host, "_staging_identity_snapshot", lambda _config: {})
    monkeypatch.setattr(host, "_converge_staging_shared_namespace", lambda _config: {})
    monkeypatch.setattr(
        host,
        "_staging_candidate_binding",
        lambda *_args, **_kwargs: {"repository": {"path": "/candidate"}},
    )
    monkeypatch.setattr(
        host,
        "_staging_slurm_profile",
        lambda *_args: SimpleNamespace(docker_cgroup_driver="systemd"),
    )
    monkeypatch.setattr(
        host,
        "_staging_candidate_set_identity",
        lambda **_kwargs: ({"resource_generation": 7}, candidate_set),
    )
    monkeypatch.setattr(
        host,
        "_staging_slurm_live_config",
        lambda _config: {
            "cluster": "trt-gb10",
            "account": "loom-staging",
            "qos": "loom-staging",
        },
    )
    monkeypatch.setattr(
        host,
        "_staging_prepare_result_directory",
        lambda *_args, **_kwargs: tmp_path,
    )
    monkeypatch.setattr(host, "_run", fake_run)

    replayed = host._staging_allocation_submit_locked(
        config,
        candidate_sha=SHA,
        candidate_tree=candidate_tree,
        request_id=request_id,
        requested_node="trt-gb10-8",
    )
    assert replayed == result


def _fixed_staging_binding() -> dict[str, object]:
    return host.slurm_policy.staging_guard_binding_payload(
        candidate_sha=SHA,
        candidate_tree="b" * 40,
        authority_generation=7,
        authority_convergence_id="c" * 64,
        authority_request_id="d" * 64,
        authority_requested_at="2026-07-29T12:00:00Z",
    )


@pytest.mark.parametrize(
    "line",
    (
        "JobId=999 Account=loom-staging UserId=loom-staging-worker(31024) "
        "NodeList=trt-gb10-2 StartTime=2026-07-29T12:00:00",
        "JobId=123 Account=loom-dev-qianyi UserId=loom-staging-worker(31024) "
        "NodeList=trt-gb10-2 StartTime=2026-07-29T12:00:00",
        "JobId=123 Account=loom-staging UserId=someone(31024) "
        "NodeList=trt-gb10-2 StartTime=2026-07-29T12:00:00",
        "JobId=123 Account=loom-staging UserId=loom-staging-worker(31024) "
        "NodeList=trt-gb10-3 StartTime=2026-07-29T12:00:00",
        "JobId=123 Account=loom-staging UserId=loom-staging-worker(31024) "
        "NodeList=trt-gb10-2 StartTime=Unknown",
    ),
)
def test_staging_allocation_job_start_rejects_wrong_closed_identity(
    monkeypatch: pytest.MonkeyPatch,
    line: str,
) -> None:
    config = host.load_staging_allocation_config(
        Path("deploy/developer-sandboxes/staging-external-slurm-authority.toml"),
    )
    monkeypatch.setattr(
        host,
        "_run",
        lambda *_args, **_kwargs: subprocess.CompletedProcess([], 0, line, ""),
    )

    with pytest.raises(host.HostConvergeError, match="start identity"):
        host._staging_allocation_job_start_time(
            config,
            job_id="123",
            node="trt-gb10-2",
        )


def test_staging_cgroup_command_uses_only_profile_and_authority_binding() -> None:
    config = host.load_staging_allocation_config(
        Path("deploy/developer-sandboxes/staging-external-slurm-authority.toml"),
    )
    profile = host.slurm_policy.load_profile(
        Path("deploy/slurm/developer-sandboxes/gb10.toml"),
    )
    binding = _fixed_staging_binding()

    command = host._staging_cgroup_command(
        config,
        profile,
        binding,
        cgroup_program=Path("/candidate/src/loom_control_plane/slurm_job_cgroup.py"),
        job_id="123",
        node="trt-gb10-2",
        job_start_time="2026-07-29T12:00:00",
    )

    assert command[command.index("--docker-driver") + 1] == "systemd"
    assert command[command.index("--job-start-time") + 1] == "2026-07-29T12:00:00"
    assert command[command.index("--env-id") + 1] == f"denv-staging-{SHA}"
    assert command[command.index("--resource-generation") + 1] == "7"
    assert command[command.index("--candidate-id") + 1] == f"cand-{SHA}"
    assert command[command.index("--candidate-tree") + 1] == "b" * 40


@pytest.mark.parametrize(
    "mutation",
    ("candidate", "limit", "unit", "receipt-extra"),
)
def test_staging_systemd_slice_receipt_rejects_adversarial_drift(
    tmp_path: Path,
    mutation: str,
) -> None:
    config = host.load_staging_allocation_config(
        Path("deploy/developer-sandboxes/staging-external-slurm-authority.toml"),
    )
    binding = _fixed_staging_binding()
    unit_name, identity_digest = host._staging_slice_identity(
        config,
        binding,
        job_id="123",
        node="trt-gb10-2",
        job_start_time="2026-07-29T12:00:00",
    )
    receipt_root = tmp_path / "receipts"
    unit_root = tmp_path / "units"
    receipt_root.mkdir()
    unit_root.mkdir()
    unit = b"[Slice]\nCPUQuota=200%\n"
    (unit_root / unit_name).write_bytes(unit)
    (unit_root / unit_name).chmod(0o644)
    unsigned: dict[str, object] = {
        "schema_version": 1,
        "kind": "loom.slurm-systemd-slice-receipt",
        "systemd_slice": unit_name,
        "slice_identity_sha256": identity_digest,
        "unit_sha256": hashlib.sha256(unit).hexdigest(),
        "job_id": "123",
        "job_start_time": "2026-07-29T12:00:00",
        "cluster": "trt-gb10",
        "node_list": "trt-gb10-2",
        "account": "loom-staging",
        "env_id": binding["env_id"],
        "resource_generation": 7,
        "runtime_id": "staging",
        "candidate_id": binding["candidate_id"],
        "candidate_sha": SHA,
        "candidate_tree": "b" * 40,
        "cpu_max": "200000 100000",
        "memory_max": "12058624000",
        "memory_swap_max_source": "max",
        "memory_swap_max_effective": "0",
        "pids_max": "65536",
        "cpuset_cpus": "0-1",
        "cpuset_mems": "0",
        "gpu_tres": "not-required",
        "gpu_detail": "not-required",
    }
    if mutation == "candidate":
        unsigned["candidate_tree"] = "e" * 40
    elif mutation == "limit":
        unsigned["pids_max"] = "65537"
    elif mutation == "unit":
        unsigned["unit_sha256"] = "f" * 64
    elif mutation == "receipt-extra":
        unsigned["unexpected"] = True
    receipt = {
        **unsigned,
        "payload_sha256": hashlib.sha256(
            json.dumps(unsigned, sort_keys=True, separators=(",", ":")).encode("ascii"),
        ).hexdigest(),
    }
    receipt_path = receipt_root / f"{unit_name}.json"
    receipt_path.write_text(
        json.dumps(receipt, sort_keys=True, separators=(",", ":")) + "\n",
    )
    receipt_path.chmod(0o444)

    with pytest.raises(host.HostConvergeError, match="receipt binding"):
        host._staging_systemd_slice_receipt(
            config,
            binding,
            job_id="123",
            node="trt-gb10-2",
            job_start_time="2026-07-29T12:00:00",
            cgroup_parent=unit_name,
            receipt_root=receipt_root,
            unit_root=unit_root,
            expected_authority_uid=os.getuid(),
            expected_authority_gid=os.getgid(),
        )


@pytest.mark.parametrize(
    "mutation",
    ("exact", "nested-same-name", "wrong-live-root", "weak-memory"),
)
def test_staging_container_containment_uses_live_systemd_control_group(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    mutation: str,
) -> None:
    systemd_slice = "loom-job-123-" + "a" * 40 + ".slice"
    live_slice = Path("/loom.slice") / "loom-job.slice" / "loom-job-123.slice" / systemd_slice
    if mutation == "wrong-live-root":
        live_slice = Path("/system.slice") / systemd_slice
    observed = live_slice / "docker-deadbeef.scope"
    if mutation == "nested-same-name":
        observed = Path("/foreign.slice") / systemd_slice / "docker-deadbeef.scope"
    proc_root = tmp_path / "proc"
    cgroup_root = tmp_path / "cgroup"
    (proc_root / "4242").mkdir(parents=True)
    (proc_root / "4242/cgroup").write_text(f"0::{observed}\n")
    slice_path = cgroup_root / live_slice.relative_to("/")
    slice_path.mkdir(parents=True)
    limits = {
        "cpu.max": "200000 100000\n",
        "memory.max": ("12058624001\n" if mutation == "weak-memory" else "12058624000\n"),
        "memory.swap.max": "0\n",
        "pids.max": "65536\n",
        "cpuset.cpus.effective": "0-1\n",
        "cpuset.mems.effective": "0\n",
    }
    for name, value in limits.items():
        (slice_path / name).write_text(value)
    calls = iter(
        (
            subprocess.CompletedProcess([], 0, "deadbeefcafe\n", ""),
            subprocess.CompletedProcess([], 0, "4242\n", ""),
            subprocess.CompletedProcess([], 0, f"{live_slice}\n", ""),
        ),
    )
    monkeypatch.setattr(host.subprocess, "run", lambda *_args, **_kwargs: next(calls))
    receipt = {
        "cpu_max": "200000 100000",
        "memory_max": "12058624000",
        "memory_swap_max_effective": "0",
        "pids_max": "65536",
        "cpuset_cpus": "0-1",
        "cpuset_mems": "0",
    }

    if mutation == "exact":
        assert host._staging_systemd_container_containment(
            ("docker", "compose"),
            environment={},
            repository=tmp_path,
            service="worker",
            systemd_slice=systemd_slice,
            receipt=receipt,
            proc_root=proc_root,
            cgroup_root=cgroup_root,
        ) == {
            "container_id": "deadbeefcafe",
            "observed_cgroup": str(observed),
            "cpu_max": "200000 100000",
            "memory_max": "12058624000",
            "memory_swap_max": "0",
            "pids_max": "65536",
            "cpuset_cpus": "0-1",
            "cpuset_mems": "0",
        }
    else:
        with pytest.raises(host.HostConvergeError):
            host._staging_systemd_container_containment(
                ("docker", "compose"),
                environment={},
                repository=tmp_path,
                service="worker",
                systemd_slice=systemd_slice,
                receipt=receipt,
                proc_root=proc_root,
                cgroup_root=cgroup_root,
            )
