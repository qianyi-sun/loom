from __future__ import annotations

import hashlib
import json
import os
import socket
import stat
from dataclasses import dataclass, field, replace
from pathlib import Path
from types import SimpleNamespace

import pytest
from scripts.ops import task_image_builder_host_converge as host

DESIRED_CGROUP = (
    b"CgroupPlugin=autodetect\n"
    b"ConstrainCores=yes\n"
    b"ConstrainRAMSpace=yes\n"
    b"ConstrainSwapSpace=yes\n"
    b"ConstrainDevices=yes\n"
)


def _quota_state(*, exact: bool, storage_root_exists: bool = True) -> dict[str, object]:
    return {
        "block_hard_limit": 104857600 if exact else 0,
        "block_soft_limit": 0,
        "block_used": 0,
        "inode_hard_limit": 1000000 if exact else 0,
        "inode_soft_limit": 0,
        "inode_used": 0,
        "project_id": 300993 if exact else (0 if storage_root_exists else None),
        "project_inherit": exact,
        "storage_root_entries": [] if storage_root_exists else None,
        "storage_root_exists": storage_root_exists,
        "storage_root_gid": 980 if exact else (0 if storage_root_exists else None),
        "storage_root_mode": 0o700 if storage_root_exists else None,
        "storage_root_uid": 993 if exact else (0 if storage_root_exists else None),
    }


def _legacy_quota_state() -> dict[str, object]:
    state = _quota_state(exact=True)
    state.update({"storage_root_uid": 0, "storage_root_gid": 0})
    return state


@dataclass
class FakeBackend:
    facts: host.HostFacts
    fail_preflight: str | None = None
    fail_stage: str | None = None
    fail_restore: bool = False
    calls: list[str] = field(default_factory=list)
    closed: bool = False

    def preflight(
        self,
        policy: host.HostPolicy,
        bundle: Path,
    ) -> host.HostFacts:
        del bundle
        self.calls.append("preflight")
        if self.fail_preflight is not None:
            raise host.HostConvergenceError(self.fail_preflight)
        host._quota_state_classification(policy, self.facts.quota_state)
        return self.facts

    def recovery_preflight(
        self,
        policy: host.HostPolicy,
        bundle: Path,
    ) -> host.HostFacts:
        del policy, bundle
        self.calls.append("recovery_preflight")
        return self.facts

    def install_packages(
        self,
        policy: host.HostPolicy,
        bundle: Path,
    ) -> None:
        del policy, bundle
        self.calls.append("packages:libsubid4,uidmap,quota")
        if self.fail_stage == "packages":
            raise host.HostConvergenceError("injected package failure")
        self.facts = replace(
            self.facts,
            packages={
                "libsubid4": "1:4.13+dfsg1-4ubuntu3.2",
                "uidmap": "1:4.13+dfsg1-4ubuntu3.2",
                "quota": "4.06-1build6",
            },
            helpers_exact=True,
        )

    def apply_quota(self, policy: host.HostPolicy) -> None:
        del policy
        self.calls.append("quota:apply")
        if self.fail_stage == "quota":
            raise host.HostConvergenceError("injected quota failure")
        self.facts = replace(self.facts, quota_exact=True, quota_state=_quota_state(exact=True))

    def install_inert_prerequisites(
        self,
        policy: host.HostPolicy,
        bundle: Path,
    ) -> None:
        del policy, bundle
        self.calls.append("inert:identity-runtime")
        if self.fail_stage == "runtime":
            raise host.HostConvergenceError("injected runtime failure")
        self.facts = replace(self.facts, identity_exact=True, runtime_exact=True)
        if self.fail_stage == "runtime_after_install":
            raise host.HostConvergenceError("injected post-install failure")

    def observe(self, policy: host.HostPolicy, bundle: Path) -> host.HostFacts:
        del policy, bundle
        self.calls.append("observe")
        if self.fail_stage == "observe_oserror":
            raise OSError("injected observation I/O failure")
        return self.facts

    def restore_quota(
        self,
        policy: host.HostPolicy,
        quota_prestate: dict[str, object],
    ) -> None:
        del policy
        self.calls.append("quota:restore")
        if self.fail_restore:
            raise host.HostConvergenceError("injected quota restore failure")
        self.facts = replace(
            self.facts,
            quota_exact=quota_prestate["block_hard_limit"] == 104857600,
            quota_state=quota_prestate,
        )

    def quota_matches(self, policy: host.HostPolicy, expected: dict[str, object]) -> bool:
        del policy
        self.calls.append("quota:verify-restore")
        return self.facts.quota_state == expected

    def close(self) -> None:
        self.closed = True


@dataclass(frozen=True)
class Fixture:
    policy: Path
    release: Path
    runtime_manifest: Path
    bundle: Path
    receipt_dir: Path
    cgroup_config: Path
    observed_cgroup: Path
    paths: host.HostReleasePaths
    backend: FakeBackend


def _sha(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _fixture(tmp_path: Path, transition: str = "shared_symlink_to_node_local") -> Fixture:
    observed = tmp_path / "shared-cgroup.conf"
    observed_bytes = b"CgroupPlugin=autodetect\nConstrainCores=yes\n"
    observed.write_bytes(observed_bytes)
    cgroup = tmp_path / "cgroup.conf"
    if transition == "shared_symlink_to_node_local":
        cgroup.symlink_to(observed)
    else:
        cgroup.write_bytes(observed_bytes)
        cgroup.chmod(0o640)
    storage = tmp_path / "builder-storage"
    storage.mkdir()
    bundle = tmp_path / "bundle"
    bundle.mkdir()
    release = tmp_path / "host-release-v1.json"
    release.write_text(
        json.dumps({"schema": "loom.task-image-builder-host-release/v1", "release": "host-release-v1"}),
        encoding="utf-8",
    )
    runtime = tmp_path / "rootless-runtime-v1.json"
    runtime.write_text(
        json.dumps(
            {
                "schema": "loom.task-image-builder-rootless-runtime/v1",
                "release": "rootless-runtime-v1",
            }
        ),
        encoding="utf-8",
    )
    policy = tmp_path / "prerequisites-v1.toml"
    policy.write_text(
        f"""
schema = "loom.task-image-builder-prerequisites/v1"
policy_version = "task-image-builder-prerequisites-v1"
production_certification_allowed = false
certified_nodes = []
unconditional_blockers = ["phase2_guard_provider_release_missing"]

[identity]
user = "loom-builder"
group = "loom-task-builder"
uid = 993
gid = 980
subid_start = 3000000
subid_count = 65536
home = "/nonexistent"
shell = "/usr/sbin/nologin"
forbidden_supplementary_groups = ["docker", "root", "sudo"]

[storage]
root = "{storage}/jobs"
mountpoint = "{storage}"
project_id = 300993
site_filesystem = "ext4"
required_mount_options = ["prjquota"]
automatic_block_device_changes = false
reject_root_filesystem = true
reject_network_filesystem = true
cache_enabled = false
cleanup_required = true

[[clusters]]
id = "test"
architecture = "x86_64"
controller = "test-controller"
builder_nodes = ["node-1"]
cgroup_config = "{cgroup}"
cgroup_transition = "{transition}"
cgroup_observed_path = "{observed}"
cgroup_observed_sha256 = "{_sha(observed_bytes)}"
""".lstrip(),
        encoding="utf-8",
    )
    receipt_dir = tmp_path / "receipts"
    receipt_dir.mkdir(mode=0o700)
    node_installer = tmp_path / "node-installer"
    node_installer.write_text("fixture node installer\n", encoding="utf-8")
    paths = host.HostReleasePaths(
        policy=policy,
        release=release,
        runtime_manifest=runtime,
        node_installer=node_installer,
        wrapper=tmp_path / "host-converger.py",
    )
    paths.wrapper.write_text("fixture host converger\n", encoding="utf-8")
    facts = host.HostFacts(
        architecture="x86_64",
        slurm_node="node-1",
        bundle_digest="a" * 64,
        packages={"libsubid4": None, "uidmap": None, "quota": None},
        helpers_exact=False,
        identity_exact=False,
        runtime_exact=False,
        quota_exact=False,
        quota_state=_quota_state(exact=False, storage_root_exists=False),
        storage_exact=True,
        kernel_exact=True,
        forbidden_sockets_absent=True,
    )
    return Fixture(
        policy,
        release,
        runtime,
        bundle,
        receipt_dir,
        cgroup,
        observed,
        paths,
        FakeBackend(facts),
    )


def _run(
    fixture: Fixture,
    action: str,
    *,
    operation_id: str = "00000000-0000-4000-8000-000000000011",
) -> dict[str, object]:
    return host.converge_host(
        action,
        "test",
        "node-1",
        fixture.bundle,
        fixture.receipt_dir,
        fixture.backend,
        fixture.paths,
        operation_id=operation_id,
        effective_uid=os.geteuid(),
        required_owner=os.geteuid(),
    )


def _receipt(fixture: Fixture) -> dict[str, object]:
    receipts = list(fixture.receipt_dir.glob("*.json"))
    assert len(receipts) == 1
    return json.loads(receipts[0].read_text(encoding="utf-8"))


def test_plan_and_check_are_read_only(tmp_path: Path) -> None:
    fixture = _fixture(tmp_path)
    cgroup_before = fixture.cgroup_config.lstat()
    observed_before = fixture.observed_cgroup.read_bytes()

    plan = _run(fixture, "plan")
    with pytest.raises(host.HostConvergenceError, match="not prepared"):
        _run(fixture, "check")

    assert plan["changes"] == ["packages", "helpers", "cgroup", "quota", "identity", "runtime"]
    assert fixture.cgroup_config.is_symlink()
    assert fixture.cgroup_config.lstat() == cgroup_before
    assert fixture.observed_cgroup.read_bytes() == observed_before
    assert fixture.backend.calls == ["preflight", "preflight"]
    assert list(fixture.receipt_dir.iterdir()) == []


def test_exact_legacy_root_owned_quota_state_is_correctable(tmp_path: Path) -> None:
    fixture = _fixture(tmp_path)
    legacy = _legacy_quota_state()
    fixture.backend.facts = replace(
        fixture.backend.facts,
        quota_exact=False,
        quota_state=legacy,
    )

    plan = _run(fixture, "plan")
    with pytest.raises(host.HostConvergenceError, match="not prepared"):
        _run(fixture, "check")
    fixture.backend.calls.clear()
    result = _run(fixture, "apply")

    assert "quota" in plan["changes"]
    assert result["state"] == "host_prepared"
    assert fixture.backend.facts.quota_state == _quota_state(exact=True)
    assert _receipt(fixture)["pre_state"]["quota_state"] == legacy
    assert "quota:apply" in fixture.backend.calls


@pytest.mark.parametrize(
    "failure",
    [
        "bundle verification failed",
        "node binding failed",
        "identity conflict",
        "forbidden socket is accessible",
        "kernel controllers are missing",
        "storage is root, network, loop, shared, or lacks prjquota",
        "insufficient free bytes or inodes",
        "quota drift",
    ],
)
def test_every_preflight_failure_precedes_host_mutation(
    tmp_path: Path,
    failure: str,
) -> None:
    fixture = _fixture(tmp_path)
    fixture.backend.fail_preflight = failure
    cgroup_target = os.readlink(fixture.cgroup_config)
    observed_before = fixture.observed_cgroup.read_bytes()

    with pytest.raises(host.HostConvergenceError, match=failure):
        _run(fixture, "apply")

    assert os.readlink(fixture.cgroup_config) == cgroup_target
    assert fixture.observed_cgroup.read_bytes() == observed_before
    assert fixture.backend.calls == ["preflight"]
    assert list(fixture.receipt_dir.iterdir()) == []


def test_host_convergence_closes_bundle_session_after_preflight_failure(
    tmp_path: Path,
) -> None:
    fixture = _fixture(tmp_path)
    fixture.backend.fail_preflight = "bundle verification failed"

    with pytest.raises(host.HostConvergenceError, match="bundle verification failed"):
        _run(fixture, "apply")

    assert fixture.backend.closed is True


@pytest.mark.parametrize("transition", ["shared_symlink_to_node_local", "node_local"])
def test_apply_and_explicit_rollback_restore_exact_cgroup_prestate(
    tmp_path: Path,
    transition: str,
) -> None:
    fixture = _fixture(tmp_path, transition)
    original_target = os.readlink(fixture.cgroup_config) if fixture.cgroup_config.is_symlink() else None
    original_bytes = fixture.observed_cgroup.read_bytes()
    original_mode = fixture.cgroup_config.lstat().st_mode & 0o777

    applied = _run(fixture, "apply")

    assert applied["state"] == "host_prepared"
    assert applied["activation_required"] is True
    assert fixture.cgroup_config.is_file() and not fixture.cgroup_config.is_symlink()
    assert fixture.cgroup_config.read_bytes() == DESIRED_CGROUP
    assert fixture.cgroup_config.stat().st_mode & 0o777 == 0o644
    assert fixture.observed_cgroup.read_bytes() == original_bytes

    rolled_back = _run(fixture, "rollback")

    assert rolled_back["state"] == "rolled_back"
    if original_target is not None:
        assert fixture.cgroup_config.is_symlink()
        assert os.readlink(fixture.cgroup_config) == original_target
    else:
        assert fixture.cgroup_config.read_bytes() == original_bytes
        assert fixture.cgroup_config.stat().st_mode & 0o777 == original_mode


def test_apply_orders_packages_quota_and_inert_runtime_and_records_receipt(
    tmp_path: Path,
) -> None:
    fixture = _fixture(tmp_path)

    result = _run(fixture, "apply")

    receipt = _receipt(fixture)
    assert result["production_certification_allowed"] is False
    assert result["certified_nodes"] == []
    assert result["blockers"] == ["phase2_guard_provider_release_missing"]
    assert fixture.backend.calls == [
        "preflight",
        "packages:libsubid4,uidmap,quota",
        "quota:apply",
        "inert:identity-runtime",
        "observe",
    ]
    assert receipt["schema"] == "loom.task-image-builder-host-receipt/v1"
    assert receipt["terminal_state"] == "host_prepared"
    assert receipt["activation_required"] is True
    assert receipt["rollback_verified"] is None
    assert receipt["pre_state"]["quota_exact"] is False
    assert receipt["pre_state"]["quota_state"] == _quota_state(
        exact=False,
        storage_root_exists=False,
    )
    assert receipt["post_state"]["quota_exact"] is True
    assert receipt["post_state"]["quota_state"] == _quota_state(exact=True)
    assert len(receipt["events"]) >= 4


def test_idempotent_apply_observes_without_repeating_mutations(tmp_path: Path) -> None:
    fixture = _fixture(tmp_path)
    _run(fixture, "apply")
    fixture.backend.calls.clear()

    result = _run(
        fixture,
        "apply",
        operation_id="00000000-0000-4000-8000-000000000012",
    )

    receipt = json.loads(
        (fixture.receipt_dir / "00000000-0000-4000-8000-000000000012.json").read_text(
            encoding="utf-8"
        )
    )
    assert result["state"] == "host_prepared"
    assert fixture.backend.calls == ["preflight", "observe"]
    assert receipt["created_inert_artifacts"] == []


def test_symlink_rollback_match_binds_metadata_and_shared_digest(tmp_path: Path) -> None:
    fixture = _fixture(tmp_path)
    policy, _ = host._load_policy(fixture.paths.policy, "test", "node-1")
    state, prestate = host._inspect_cgroup(policy)
    assert state == "observed"
    assert host._cgroup_matches(policy, prestate)

    fixture.observed_cgroup.write_bytes(b"shared drift\n")

    assert not host._cgroup_matches(policy, prestate)


def test_identity_preflight_checks_subgid_when_subuid_is_incomplete(
    tmp_path: Path,
) -> None:
    passwd = tmp_path / "passwd"
    group = tmp_path / "group"
    subuid = tmp_path / "subuid"
    subgid = tmp_path / "subgid"
    passwd.write_text("root:x:0:0:root:/root:/bin/bash\n", encoding="utf-8")
    group.write_text("root:x:0:\n", encoding="utf-8")
    subuid.write_text("", encoding="utf-8")
    subgid.write_text("foreign:3000000:65536\n", encoding="utf-8")
    for path in (passwd, group, subuid, subgid):
        path.chmod(0o644)

    with pytest.raises(host.HostConvergenceError, match="subgid range conflict"):
        host.SystemHostBackend._identity_exact(
            passwd_path=passwd,
            group_path=group,
            subuid_path=subuid,
            subgid_path=subgid,
            required_owner=os.geteuid(),
        )


def test_identity_preflight_rejects_symlinked_database(tmp_path: Path) -> None:
    passwd = tmp_path / "passwd"
    group = tmp_path / "group"
    subuid = tmp_path / "subuid"
    subgid_target = tmp_path / "subgid-target"
    subgid = tmp_path / "subgid"
    passwd.write_text("root:x:0:0:root:/root:/bin/bash\n", encoding="utf-8")
    group.write_text("root:x:0:\n", encoding="utf-8")
    subuid.write_text("", encoding="utf-8")
    subgid_target.write_text("", encoding="utf-8")
    subgid.symlink_to(subgid_target)
    for path in (passwd, group, subuid, subgid_target):
        path.chmod(0o644)

    with pytest.raises(host.HostConvergenceError, match="subgid database"):
        host.SystemHostBackend._identity_exact(
            passwd_path=passwd,
            group_path=group,
            subuid_path=subuid,
            subgid_path=subgid,
            required_owner=os.geteuid(),
        )


def test_runtime_readback_verifies_exact_installed_binary_digest(tmp_path: Path) -> None:
    payload = b"runtime-binary\n"
    manifest = tmp_path / "rootless-runtime-v1.json"
    manifest.write_text(
        json.dumps(
            {
                "schema": "loom.task-image-builder-rootless-runtime/v1",
                "release": "rootless-runtime-v1",
                "architectures": {
                    "x86_64": {"binaries": {"buildkitd": _sha(payload)}}
                },
            },
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    install_base = tmp_path / "install"
    release = install_base / "releases/rootless-runtime-v1"
    binary_dir = release / "bin"
    binary_dir.mkdir(parents=True)
    release.chmod(0o755)
    binary_dir.chmod(0o755)
    binary = binary_dir / "buildkitd"
    binary.write_bytes(payload)
    binary.chmod(0o755)
    receipt = {
        "schema": "loom.task-image-builder-installed-runtime/v1",
        "release": "rootless-runtime-v1",
        "architecture": "x86_64",
        "manifest_sha256": _sha(manifest.read_bytes()),
        "binary_sha256": {"buildkitd": _sha(payload)},
    }
    receipt_path = release / "receipt.json"
    receipt_path.write_bytes(host._canonical(receipt) + b"\n")
    receipt_path.chmod(0o644)
    (install_base / "current").symlink_to("releases/rootless-runtime-v1")

    assert host.SystemHostBackend._runtime_exact(
        manifest,
        "x86_64",
        install_base=install_base,
        required_owner=os.geteuid(),
    )

    binary.write_bytes(b"drift\n")
    with pytest.raises(host.HostConvergenceError, match="runtime drift"):
        host.SystemHostBackend._runtime_exact(
            manifest,
            "x86_64",
            install_base=install_base,
            required_owner=os.geteuid(),
        )


def test_runtime_install_consumes_verified_snapshot_and_backend_cleanup_removes_it(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture = _fixture(tmp_path)
    policy, _ = host._load_policy(fixture.paths.policy, "test", "node-1")
    snapshot = tmp_path / "private-snapshot"
    (snapshot / "runtime").mkdir(parents=True)
    snapshot.chmod(0o700)
    backend = host.SystemHostBackend(fixture.paths)
    backend._verified = host.host_release.VerifiedHostBundle(
        architecture="x86_64",
        bundle_digest="a" * 64,
        snapshot_root=snapshot,
        package_paths=(),
        runtime_paths=(),
    )
    commands: list[tuple[str, ...]] = []

    def fake_run(args: tuple[str, ...]) -> host.CommandResult:
        commands.append(args)
        return host.CommandResult(0, "", "")

    monkeypatch.setattr(host.SystemHostBackend, "_run", staticmethod(fake_run))

    backend.install_inert_prerequisites(policy, fixture.bundle)
    backend.close()

    assert commands == [
        (
            str(fixture.paths.node_installer),
            "apply",
            "test",
            "node-1",
            str(snapshot / "runtime"),
        )
    ]
    assert not snapshot.exists()

def test_unreceipted_desired_looking_cgroup_is_rejected(tmp_path: Path) -> None:
    fixture = _fixture(tmp_path)
    fixture.cgroup_config.unlink()
    fixture.cgroup_config.write_bytes(DESIRED_CGROUP)
    fixture.cgroup_config.chmod(0o644)

    with pytest.raises(host.HostConvergenceError, match="receipt"):
        _run(fixture, "plan")

    assert fixture.backend.calls == ["preflight"]
    assert list(fixture.receipt_dir.iterdir()) == []


def test_read_only_plan_uses_receipt_authority_for_desired_cgroup_metadata(
    tmp_path: Path,
) -> None:
    fixture = _fixture(tmp_path)
    _run(fixture, "apply")

    result = host.converge_host(
        "plan",
        "test",
        "node-1",
        fixture.bundle,
        fixture.receipt_dir,
        fixture.backend,
        fixture.paths,
        operation_id="00000000-0000-4000-8000-000000000012",
        effective_uid=os.geteuid() + 1,
        required_owner=os.geteuid(),
    )

    assert result["state"] == "planned"
    assert result["changes"] == []


def test_prepared_receipt_requires_terminal_event_binding(tmp_path: Path) -> None:
    fixture = _fixture(tmp_path)
    _run(fixture, "apply")
    receipt_path = next(fixture.receipt_dir.glob("*.json"))
    receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
    assert receipt["events"][-1]["type"] == "host_prepared"
    receipt["events"].pop()
    receipt_path.write_text(
        json.dumps(receipt, sort_keys=True, separators=(",", ":")) + "\n",
        encoding="utf-8",
    )

    with pytest.raises(host.HostConvergenceError, match="event chain"):
        _run(fixture, "plan", operation_id="00000000-0000-4000-8000-000000000012")


def test_rolled_back_receipt_preserves_prepared_post_state_event_binding(
    tmp_path: Path,
) -> None:
    fixture = _fixture(tmp_path)
    _run(fixture, "apply")
    _run(fixture, "rollback")
    receipt_path = next(fixture.receipt_dir.glob("*.json"))
    receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
    receipt["post_state"]["runtime_exact"] = False
    receipt_path.write_text(
        json.dumps(receipt, sort_keys=True, separators=(",", ":")) + "\n",
        encoding="utf-8",
    )

    with pytest.raises(host.HostConvergenceError, match="event chain"):
        host._read_receipt(receipt_path, os.geteuid())


def test_explicit_rollback_rejects_current_host_post_state_drift(tmp_path: Path) -> None:
    fixture = _fixture(tmp_path)
    _run(fixture, "apply")
    fixture.backend.facts = replace(fixture.backend.facts, runtime_exact=False)
    desired = fixture.cgroup_config.read_bytes()

    with pytest.raises(host.HostConvergenceError, match="post-state"):
        _run(fixture, "rollback")

    assert fixture.cgroup_config.read_bytes() == desired


def test_explicit_rollback_rejects_desired_cgroup_metadata_drift(tmp_path: Path) -> None:
    fixture = _fixture(tmp_path)
    _run(fixture, "apply")
    fixture.cgroup_config.chmod(0o600)

    with pytest.raises(host.HostConvergenceError, match="metadata"):
        _run(fixture, "rollback")

    assert fixture.cgroup_config.read_bytes() == DESIRED_CGROUP
    assert fixture.cgroup_config.stat().st_mode & 0o777 == 0o600


def test_rollback_rejects_receipt_release_digest_tampering(tmp_path: Path) -> None:
    fixture = _fixture(tmp_path)
    _run(fixture, "apply")
    receipt_path = next(fixture.receipt_dir.glob("*.json"))
    receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
    receipt["release_digest"] = "0" * 64
    receipt_path.write_text(
        json.dumps(receipt, sort_keys=True, separators=(",", ":")) + "\n",
        encoding="utf-8",
    )
    desired_before = fixture.cgroup_config.read_bytes()

    with pytest.raises(host.HostConvergenceError, match="digest"):
        _run(fixture, "rollback")

    assert fixture.cgroup_config.read_bytes() == desired_before


def test_forbidden_socket_check_rejects_only_builder_accessible_socket(
    tmp_path: Path,
) -> None:
    socket_path = tmp_path / "docker.sock"
    endpoint = socket.socket(socket.AF_UNIX)
    try:
        endpoint.bind(str(socket_path))
        socket_path.chmod(0o660)
        assert host.SystemHostBackend._forbidden_sockets_safe((socket_path,))

        socket_path.chmod(0o666)
        with pytest.raises(host.HostConvergenceError, match="accessible"):
            host.SystemHostBackend._forbidden_sockets_safe((socket_path,))
    finally:
        endpoint.close()


def test_forbidden_socket_check_rejects_extended_acl(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    socket_path = tmp_path / "containerd.sock"
    endpoint = socket.socket(socket.AF_UNIX)
    try:
        endpoint.bind(str(socket_path))
        socket_path.chmod(0o660)
        monkeypatch.setattr(
            host.os,
            "listxattr",
            lambda path, *, follow_symlinks: ["system.posix_acl_access"],
        )

        with pytest.raises(host.HostConvergenceError, match="ACL"):
            host.SystemHostBackend._forbidden_sockets_safe((socket_path,))
    finally:
        endpoint.close()


@pytest.mark.parametrize("stage", ["packages", "quota", "runtime"])
def test_injected_apply_failure_rolls_back_mutable_state(
    tmp_path: Path,
    stage: str,
) -> None:
    fixture = _fixture(tmp_path)
    fixture.backend.fail_stage = stage
    target = os.readlink(fixture.cgroup_config)

    with pytest.raises(host.HostConvergenceError, match="rolled back"):
        _run(fixture, "apply")

    receipt = _receipt(fixture)
    assert receipt["terminal_state"] == "rolled_back"
    assert receipt["rollback_verified"] is True
    assert fixture.cgroup_config.is_symlink()
    assert os.readlink(fixture.cgroup_config) == target
    assert fixture.backend.facts.quota_exact is False


def test_failure_receipt_records_inert_artifacts_left_after_rollback(
    tmp_path: Path,
) -> None:
    fixture = _fixture(tmp_path)
    fixture.backend.fail_stage = "runtime_after_install"

    with pytest.raises(host.HostConvergenceError, match="rolled back"):
        _run(fixture, "apply")

    receipt = _receipt(fixture)
    assert receipt["terminal_state"] == "rolled_back"
    assert receipt["rollback_verified"] is True
    assert receipt["created_inert_artifacts"] == ["packages", "identity", "runtime"]
    assert receipt["post_state"]["packages"] == {
        "libsubid4": "1:4.13+dfsg1-4ubuntu3.2",
        "quota": "4.06-1build6",
        "uidmap": "1:4.13+dfsg1-4ubuntu3.2",
    }
    assert receipt["post_state"]["identity_exact"] is True
    assert receipt["post_state"]["runtime_exact"] is True


def test_apply_failed_receipt_write_error_does_not_prevent_rollback(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture = _fixture(tmp_path)
    fixture.backend.fail_stage = "runtime_after_install"
    target = os.readlink(fixture.cgroup_config)
    real_write = host._write_document
    failed_once = False

    def injected_write(
        path: Path,
        document: dict[str, object],
        *,
        exclusive: bool,
    ) -> None:
        nonlocal failed_once
        events = document.get("events")
        last_type = events[-1]["type"] if isinstance(events, list) and events else None
        if last_type == "apply_failed" and not failed_once:
            failed_once = True
            raise OSError("injected failure-receipt write error")
        real_write(path, document, exclusive=exclusive)

    monkeypatch.setattr(host, "_write_document", injected_write)

    with pytest.raises(host.HostConvergenceError, match="rolled back"):
        _run(fixture, "apply")

    receipt = _receipt(fixture)
    assert failed_once
    assert receipt["terminal_state"] == "rolled_back"
    assert receipt["rollback_verified"] is True
    assert fixture.cgroup_config.is_symlink()
    assert os.readlink(fixture.cgroup_config) == target
    assert fixture.backend.facts.quota_exact is False


def test_observation_oserror_after_mutation_still_rolls_back(tmp_path: Path) -> None:
    fixture = _fixture(tmp_path)
    fixture.backend.fail_stage = "observe_oserror"
    target = os.readlink(fixture.cgroup_config)

    with pytest.raises(host.HostConvergenceError, match="rolled back"):
        _run(fixture, "apply")

    receipt = _receipt(fixture)
    assert receipt["post_state"] is None
    assert "observation I/O failure" in receipt["post_readback_error"]
    assert receipt["terminal_state"] == "rolled_back"
    assert receipt["rollback_verified"] is True
    assert fixture.cgroup_config.is_symlink()
    assert os.readlink(fixture.cgroup_config) == target
    assert fixture.backend.facts.quota_exact is False


def test_receipt_update_failure_preserves_previous_atomic_document(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    receipt_dir = tmp_path / "receipts"
    receipt_dir.mkdir(mode=0o700)
    receipt = receipt_dir / "operation.json"
    original = {"sequence": 1}
    host._write_document(receipt, original, exclusive=True)
    original_bytes = receipt.read_bytes()

    def fail_replace(source: Path, destination: Path) -> None:
        del source, destination
        raise OSError("injected atomic replacement failure")

    monkeypatch.setattr(host.os, "replace", fail_replace)

    with pytest.raises(OSError, match="atomic replacement"):
        host._write_document(receipt, {"sequence": 2}, exclusive=False)

    assert receipt.read_bytes() == original_bytes
    assert list(receipt_dir.iterdir()) == [receipt]


def _prepare_interrupted_apply(fixture: Fixture) -> host.HostPolicy:
    policy, policy_payload = host._load_policy(fixture.paths.policy, "test", "node-1")
    facts = fixture.backend.preflight(policy, fixture.bundle)
    _, cgroup_prestate = host._inspect_cgroup(policy)
    operation_id = "00000000-0000-4000-8000-000000000011"
    document = host._receipt_document(
        policy,
        host._digests(fixture.paths, policy_payload, policy),
        operation_id,
        facts,
        cgroup_prestate,
    )
    receipt_path = fixture.receipt_dir / f"{operation_id}.json"
    host._write_document(receipt_path, document, exclusive=True)
    host._event(document, "intent", {"changes": host._changes(facts, "observed")})
    host._write_document(receipt_path, document, exclusive=False)
    fixture.backend.install_packages(policy, fixture.bundle)
    host._apply_cgroup(policy, os.geteuid())
    fixture.backend.calls.clear()
    return policy


def test_explicit_rollback_recovers_interrupted_applying_receipt(tmp_path: Path) -> None:
    fixture = _fixture(tmp_path)
    policy = _prepare_interrupted_apply(fixture)
    fixture.backend.apply_quota(policy)
    fixture.backend.calls.clear()

    result = _run(fixture, "rollback")

    assert result["state"] == "rolled_back"
    assert fixture.cgroup_config.is_symlink()
    assert fixture.backend.facts.quota_exact is False


def test_interrupted_rollback_recovers_after_storage_root_creation(tmp_path: Path) -> None:
    fixture = _fixture(tmp_path)
    _prepare_interrupted_apply(fixture)
    fixture.backend.facts = replace(
        fixture.backend.facts,
        quota_exact=False,
        quota_state=_quota_state(exact=False, storage_root_exists=True),
    )

    result = _run(fixture, "rollback")

    assert result["state"] == "rolled_back"
    assert fixture.backend.facts.quota_state == _quota_state(
        exact=False,
        storage_root_exists=False,
    )
    assert fixture.backend.calls == [
        "recovery_preflight",
        "quota:restore",
        "quota:verify-restore",
    ]


def test_interrupted_rollback_recovers_after_project_inheritance(tmp_path: Path) -> None:
    fixture = _fixture(tmp_path)
    _prepare_interrupted_apply(fixture)
    partial = _quota_state(exact=False, storage_root_exists=True)
    partial.update(
        {
            "block_used": 1,
            "inode_used": 1,
            "project_id": 300993,
            "project_inherit": True,
        }
    )
    fixture.backend.facts = replace(
        fixture.backend.facts,
        quota_exact=False,
        quota_state=partial,
    )

    result = _run(fixture, "rollback")

    assert result["state"] == "rolled_back"
    assert fixture.backend.facts.quota_state == _quota_state(
        exact=False,
        storage_root_exists=False,
    )
    assert fixture.backend.calls == [
        "recovery_preflight",
        "quota:restore",
        "quota:verify-restore",
    ]


@pytest.mark.parametrize(
    "drift",
    [
        {"block_hard_limit": 1},
        {"project_id": 42},
    ],
)
def test_interrupted_rollback_rejects_unbounded_quota_drift(
    tmp_path: Path,
    drift: dict[str, object],
) -> None:
    fixture = _fixture(tmp_path)
    _prepare_interrupted_apply(fixture)
    partial = _quota_state(exact=False, storage_root_exists=True)
    partial.update(drift)
    fixture.backend.facts = replace(
        fixture.backend.facts,
        quota_exact=False,
        quota_state=partial,
    )

    with pytest.raises(host.HostConvergenceError, match="quota state is unsafe"):
        _run(fixture, "rollback")

    assert fixture.backend.calls == ["recovery_preflight"]
    assert _receipt(fixture)["terminal_state"] == "applying"


def test_apply_and_rollback_require_root_authority(tmp_path: Path) -> None:
    fixture = _fixture(tmp_path)

    with pytest.raises(host.HostConvergenceError, match="requires root"):
        host.converge_host(
            "apply",
            "test",
            "node-1",
            fixture.bundle,
            fixture.receipt_dir,
            fixture.backend,
            fixture.paths,
            operation_id="00000000-0000-4000-8000-000000000011",
            effective_uid=os.geteuid() + 1,
            required_owner=os.geteuid(),
        )

    assert fixture.backend.calls == ["preflight"]
    assert list(fixture.receipt_dir.iterdir()) == []

    prepared_path = tmp_path / "prepared"
    prepared_path.mkdir()
    prepared = _fixture(prepared_path)
    _run(prepared, "apply")
    desired = prepared.cgroup_config.read_bytes()
    with pytest.raises(host.HostConvergenceError, match="rollback requires root"):
        host.converge_host(
            "rollback",
            "test",
            "node-1",
            prepared.bundle,
            prepared.receipt_dir,
            prepared.backend,
            prepared.paths,
            operation_id="00000000-0000-4000-8000-000000000011",
            effective_uid=os.geteuid() + 1,
            required_owner=os.geteuid(),
        )
    assert prepared.cgroup_config.read_bytes() == desired


def test_apply_rejects_non_owner_only_receipt_root_before_mutation(
    tmp_path: Path,
) -> None:
    fixture = _fixture(tmp_path)
    fixture.receipt_dir.chmod(0o755)

    with pytest.raises(host.HostConvergenceError, match="owner-only"):
        _run(fixture, "apply")

    assert fixture.backend.calls == ["preflight"]
    assert list(fixture.receipt_dir.iterdir()) == []


def test_receipt_collision_precedes_host_mutation(tmp_path: Path) -> None:
    fixture = _fixture(tmp_path)
    collision = fixture.receipt_dir / "00000000-0000-4000-8000-000000000011.json"
    collision.write_text("occupied\n", encoding="utf-8")
    collision.chmod(0o600)

    with pytest.raises(FileExistsError):
        _run(fixture, "apply")

    assert fixture.backend.calls == ["preflight"]
    assert fixture.cgroup_config.is_symlink()
    assert collision.read_text(encoding="utf-8") == "occupied\n"


@pytest.mark.parametrize(
    ("target", "source", "filesystem", "options", "free_bytes", "error"),
    [
        ("/", "/dev/sda1", "ext4", "rw,prjquota", 200_000_000_000, "mount"),
        (None, "/dev/loop0", "ext4", "rw,prjquota", 200_000_000_000, "mount"),
        (None, "server:/builder", "nfs", "rw,prjquota", 200_000_000_000, "mount"),
        (None, "/dev/sdb1", "ext4", "rw", 200_000_000_000, "mount"),
        (None, "/dev/sdb1", "ext4", "rw,prjquota", 1, "capacity"),
    ],
)
def test_real_storage_preflight_rejects_unsafe_mounts_and_capacity(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    target: str | None,
    source: str,
    filesystem: str,
    options: str,
    free_bytes: int,
    error: str,
) -> None:
    fixture = _fixture(tmp_path)
    policy, _ = host._load_policy(fixture.paths.policy, "test", "node-1")

    def fake_run(args: tuple[str, ...]) -> host.CommandResult:
        if args[0] == "/usr/bin/findmnt":
            return host.CommandResult(
                0,
                json.dumps(
                    {
                        "filesystems": [
                            {
                                "target": target or str(policy.storage_mountpoint),
                                "source": source,
                                "fstype": filesystem,
                                "options": options,
                            }
                        ]
                    }
                ),
                "",
            )
        if args[0] == "/usr/bin/lsblk":
            return host.CommandResult(0, "part\n", "")
        raise AssertionError(args)

    monkeypatch.setattr(host.SystemHostBackend, "_run", staticmethod(fake_run))
    monkeypatch.setattr(
        host.os,
        "statvfs",
        lambda path: SimpleNamespace(
            f_bavail=free_bytes,
            f_frsize=1,
            f_favail=2_000_000,
        ),
    )

    with pytest.raises(host.HostConvergenceError, match=error):
        host.SystemHostBackend._storage_exact(policy)


def test_real_quota_classification_rejects_wrong_hard_limit(tmp_path: Path) -> None:
    fixture = _fixture(tmp_path)
    policy, _ = host._load_policy(fixture.paths.policy, "test", "node-1")
    drift = _quota_state(exact=True)
    drift["block_hard_limit"] = 1

    with pytest.raises(host.HostConvergenceError, match="quota drift"):
        host._quota_state_classification(policy, drift)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("storage_root_uid", 1),
        ("storage_root_gid", 1),
        ("storage_root_mode", 0o750),
        ("block_hard_limit", 1),
    ],
)
def test_project_bound_legacy_near_miss_is_rejected(
    tmp_path: Path,
    field: str,
    value: int,
) -> None:
    fixture = _fixture(tmp_path)
    policy, _ = host._load_policy(fixture.paths.policy, "test", "node-1")
    drift = _legacy_quota_state()
    drift[field] = value

    with pytest.raises(host.HostConvergenceError, match="quota drift"):
        host._quota_state_classification(policy, drift)


def test_zero_limit_project_quota_row_is_normalized_as_absent(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture = _fixture(tmp_path)
    policy, _ = host._load_policy(fixture.paths.policy, "test", "node-1")
    policy.storage_root.mkdir(parents=True)
    header = (
        "Project,BlockStatus,FileStatus,BlockUsed,BlockSoftLimit,"
        "BlockHardLimit,BlockGrace,FileUsed,FileSoftLimit,FileHardLimit,FileGrace\n"
    )

    def fake_run(args: tuple[str, ...]) -> host.CommandResult:
        if args[0] == "/usr/bin/lsattr":
            return host.CommandResult(
                0,
                f"    0 --------------e------- {policy.storage_root}\n",
                "",
            )
        if args[0] == "/usr/sbin/repquota":
            return host.CommandResult(
                0,
                header + "300993,--,--,0,0,0,0,0,0,0,0\n",
                "",
            )
        raise AssertionError(args)

    monkeypatch.setattr(host.SystemHostBackend, "_run", staticmethod(fake_run))

    assert host.SystemHostBackend._quota_state(policy) == {
        "block_hard_limit": 0,
        "block_soft_limit": 0,
        "block_used": 0,
        "inode_hard_limit": 0,
        "inode_soft_limit": 0,
        "inode_used": 0,
        "project_id": 0,
        "project_inherit": False,
        "storage_root_entries": [],
        "storage_root_exists": True,
        "storage_root_gid": os.getegid(),
        "storage_root_mode": stat.S_IMODE(policy.storage_root.lstat().st_mode),
        "storage_root_uid": os.geteuid(),
    }


def test_quota_readback_receipt_binds_exact_jobs_root_metadata_and_emptiness(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture = _fixture(tmp_path)
    policy, _ = host._load_policy(fixture.paths.policy, "test", "node-1")
    policy.storage_root.mkdir(parents=True)
    real_lstat = Path.lstat
    header = (
        "Project,BlockStatus,FileStatus,BlockUsed,BlockSoftLimit,"
        "BlockHardLimit,BlockGrace,FileUsed,FileSoftLimit,FileHardLimit,FileGrace\n"
    )

    def fake_lstat(path: Path) -> object:
        if path == policy.storage_root:
            return SimpleNamespace(
                st_mode=stat.S_IFDIR | 0o700,
                st_uid=993,
                st_gid=980,
            )
        return real_lstat(path)

    def fake_run(args: tuple[str, ...]) -> host.CommandResult:
        if args[0] == "/usr/bin/lsattr":
            return host.CommandResult(
                0,
                f"300993 ----P----------------- {policy.storage_root}\n",
                "",
            )
        if args[0] == "/usr/sbin/repquota":
            return host.CommandResult(
                0,
                header + "300993,--,--,0,0,104857600,0,0,0,1000000,0\n",
                "",
            )
        raise AssertionError(args)

    monkeypatch.setattr(host.Path, "lstat", fake_lstat)
    monkeypatch.setattr(host.SystemHostBackend, "_run", staticmethod(fake_run))

    state = host.SystemHostBackend._quota_state(policy)

    assert state["storage_root_uid"] == 993
    assert state["storage_root_gid"] == 980
    assert state["storage_root_mode"] == 0o700
    assert state["storage_root_entries"] == []


def test_quota_apply_converges_builder_owned_owner_only_jobs_root(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture = _fixture(tmp_path)
    policy, _ = host._load_policy(fixture.paths.policy, "test", "node-1")
    metadata_changes: list[tuple[int, int]] = []
    modes: list[int] = []
    commands: list[tuple[str, ...]] = []

    monkeypatch.setattr(
        host.os,
        "fchown",
        lambda descriptor, uid, gid: metadata_changes.append((uid, gid)),
    )
    monkeypatch.setattr(
        host.os,
        "fchmod",
        lambda descriptor, mode: modes.append(mode),
    )
    monkeypatch.setattr(
        host.SystemHostBackend,
        "_run",
        staticmethod(
            lambda args: commands.append(args) or host.CommandResult(0, "", "")
        ),
    )

    host.SystemHostBackend(fixture.paths).apply_quota(policy)

    assert metadata_changes == [(993, 980)]
    assert modes == [0o700]
    assert commands[0] == (
        "/usr/bin/chattr",
        "-p",
        "300993",
        "+P",
        str(policy.storage_root),
    )


def test_real_quota_restoration_is_read_only_when_prestate_already_matches(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture = _fixture(tmp_path)
    policy, _ = host._load_policy(fixture.paths.policy, "test", "node-1")
    prestate = _quota_state(exact=False, storage_root_exists=False)

    monkeypatch.setattr(
        host.SystemHostBackend,
        "_quota_state",
        staticmethod(lambda observed_policy: prestate),
    )

    def unexpected_run(args: tuple[str, ...]) -> host.CommandResult:
        raise AssertionError(f"unexpected quota mutation: {args}")

    monkeypatch.setattr(host.SystemHostBackend, "_run", staticmethod(unexpected_run))

    host.SystemHostBackend(fixture.paths).restore_quota(policy, prestate)


def test_receipt_rollback_restores_complete_preexisting_jobs_root_state(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fixture = _fixture(tmp_path)
    policy, _ = host._load_policy(fixture.paths.policy, "test", "node-1")
    policy.storage_root.mkdir(parents=True)
    prestate = _quota_state(exact=False)
    prestate.update(
        {
            "storage_root_uid": 123,
            "storage_root_gid": 456,
            "storage_root_mode": 0o750,
        }
    )
    current = _quota_state(exact=True)
    metadata_calls: list[tuple[str, int, int | None]] = []

    def quota_state(observed_policy: host.HostPolicy) -> dict[str, object]:
        assert observed_policy == policy
        return dict(current)

    def quota_command(args: tuple[str, ...]) -> host.CommandResult:
        if args[0] == "/usr/sbin/setquota":
            current.update(
                {
                    "block_soft_limit": int(args[3]),
                    "block_hard_limit": int(args[4]),
                    "inode_soft_limit": int(args[5]),
                    "inode_hard_limit": int(args[6]),
                }
            )
        elif args[0] == "/usr/bin/chattr":
            current["project_id"] = int(args[2])
            current["project_inherit"] = args[3] == "+P"
        else:
            raise AssertionError(args)
        return host.CommandResult(0, "", "")

    def restore_owner(descriptor: int, uid: int, gid: int) -> None:
        assert descriptor >= 0
        metadata_calls.append(("owner", uid, gid))
        current.update({"storage_root_uid": uid, "storage_root_gid": gid})

    def restore_mode(descriptor: int, mode: int) -> None:
        assert descriptor >= 0
        metadata_calls.append(("mode", mode, None))
        current["storage_root_mode"] = mode

    monkeypatch.setattr(host.SystemHostBackend, "_quota_state", staticmethod(quota_state))
    monkeypatch.setattr(host.SystemHostBackend, "_run", staticmethod(quota_command))
    monkeypatch.setattr(host.os, "fchown", restore_owner)
    monkeypatch.setattr(host.os, "fchmod", restore_mode)

    cgroup_state, cgroup_prestate = host._inspect_cgroup(policy)
    assert cgroup_state == "observed"
    host._apply_cgroup(policy, os.geteuid())
    metadata_calls.clear()
    document: dict[str, object] = {
        "cgroup_prestate": cgroup_prestate,
        "pre_state": {"quota_state": prestate},
        "terminal_state": "applying",
        "events": [],
    }
    receipt_path = fixture.receipt_dir / "rollback.json"
    host._write_document(receipt_path, document, exclusive=True)

    verified = host._rollback(
        policy,
        host.SystemHostBackend(fixture.paths),
        document,
        receipt_path,
    )

    assert verified is True
    assert document["rollback_verified"] is True
    assert document["terminal_state"] == "rolled_back"
    assert current == prestate
    assert metadata_calls == [("owner", 123, 456), ("mode", 0o750, None)]


@pytest.mark.parametrize("substitution", ["symlink", "file", "non_empty"])
def test_jobs_root_substitution_during_restore_fails_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    substitution: str,
) -> None:
    fixture = _fixture(tmp_path)
    policy, _ = host._load_policy(fixture.paths.policy, "test", "node-1")
    policy.storage_root.mkdir(parents=True)
    prestate = _quota_state(exact=False)
    current = _quota_state(exact=True)
    substituted = False

    monkeypatch.setattr(
        host.SystemHostBackend,
        "_quota_state",
        staticmethod(lambda observed_policy: dict(current)),
    )

    def substitute_on_quota(args: tuple[str, ...]) -> host.CommandResult:
        nonlocal substituted
        if args[0] == "/usr/sbin/setquota" and not substituted:
            substituted = True
            if substitution == "non_empty":
                (policy.storage_root / "unexpected").write_text("drift\n", encoding="utf-8")
            else:
                held = policy.storage_root.with_name("held-jobs-root")
                policy.storage_root.rename(held)
                if substitution == "symlink":
                    policy.storage_root.symlink_to(held, target_is_directory=True)
                else:
                    policy.storage_root.write_text("unsafe\n", encoding="utf-8")
        return host.CommandResult(0, "", "")

    monkeypatch.setattr(
        host.SystemHostBackend,
        "_run",
        staticmethod(substitute_on_quota),
    )

    with pytest.raises(host.HostConvergenceError, match=r"jobs root.*restoration"):
        host.SystemHostBackend(fixture.paths).restore_quota(policy, prestate)


def test_failed_restoration_is_recorded_and_never_claims_prepared(tmp_path: Path) -> None:
    fixture = _fixture(tmp_path)
    fixture.backend.fail_stage = "runtime"
    fixture.backend.fail_restore = True

    with pytest.raises(host.HostConvergenceError, match="rollback failed"):
        _run(fixture, "apply")

    receipt = _receipt(fixture)
    assert receipt["terminal_state"] == "rollback_failed"
    assert receipt["rollback_verified"] is False
    assert receipt["activation_required"] is False
