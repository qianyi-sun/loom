from __future__ import annotations

import hashlib
import os
import socket
import struct
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath
from uuid import UUID

import pytest

from loom_task_image_builder_guard.errors import GuardError
from loom_task_image_builder_guard.identity import (
    PeerInspector,
    derive_batch_cgroup,
    projection_request,
)
from loom_task_image_builder_guard.models import IdentityConfig

PID = 4242
UID = 993
GID = 980
GRANT = UUID("11111111-1111-4111-8111-111111111111")
REQUEST = UUID("22222222-2222-4222-8222-222222222222")
BOOT = UUID("33333333-3333-4333-8333-333333333333")
CGROUP = "/system.slice/slurmstepd.scope/job_123/step_batch/user/task_0"


class _Connection:
    family = socket.AF_UNIX

    def getsockopt(self, level: int, option: int, length: int) -> bytes:
        assert (level, option, length) == (socket.SOL_SOCKET, socket.SO_PEERCRED, 12)
        return struct.pack("3i", PID, UID, GID)


def _fixture(tmp_path: Path) -> tuple[PeerInspector, Path, dict[str, bool]]:
    proc_root = tmp_path / "proc"
    process = proc_root / str(PID)
    process.mkdir(parents=True)
    executable = tmp_path / "usr/local/libexec/loom-task-builder-supervisor"
    executable.parent.mkdir(parents=True)
    executable.write_bytes(b"trusted-supervisor-v1")
    executable.chmod(0o555)
    (process / "exe").symlink_to(executable)
    (process / "status").write_text(
        "Name:\tloom-builder\n"
        f"Pid:\t{PID}\n"
        f"Uid:\t{UID}\t{UID}\t{UID}\t{UID}\n"
        f"Gid:\t{GID}\t{GID}\t{GID}\t{GID}\n"
        "Groups:\t44 980\n",
        encoding="ascii",
    )
    (process / "cgroup").write_text(f"0::{CGROUP}\n", encoding="ascii")
    alive = {"value": True}
    inspector = PeerInspector(
        IdentityConfig(
            uid=UID,
            gid=GID,
            forbidden_supplementary_gids=(0, 27, 128),
            supervisor_path=Path("/usr/local/libexec/loom-task-builder-supervisor"),
            supervisor_sha256=hashlib.sha256(b"trusted-supervisor-v1").hexdigest(),
        ),
        proc_root=proc_root,
        host_root=tmp_path,
        trusted_file_uid=os.geteuid(),
        pidfd_open=lambda pid: os.open("/dev/null", os.O_RDONLY | os.O_CLOEXEC),
        pidfd_alive=lambda descriptor: alive["value"],
    )
    return inspector, executable, alive


def test_capture_derives_peer_identity_executable_and_job_from_kernel_files(
    tmp_path: Path,
) -> None:
    inspector, executable, _alive = _fixture(tmp_path)

    peer = inspector.capture(_Connection())  # type: ignore[arg-type]
    try:
        assert (peer.pid, peer.uid, peer.gid, peer.job_id) == (PID, UID, GID, "123")
        assert peer.cgroup_relative.as_posix() == CGROUP
        assert peer.executable_path == executable
        assert peer.executable_sha256 == hashlib.sha256(b"trusted-supervisor-v1").hexdigest()
        peer.assert_unchanged()
    finally:
        peer.close()


@pytest.mark.parametrize(
    ("line", "code"),
    [
        (f"Uid:\t{UID}\t{UID}\t0\t{UID}\n", "peer_identity_invalid"),
        (f"Gid:\t{GID}\t{GID}\t0\t{GID}\n", "peer_identity_invalid"),
        ("Groups:\t0 44 980\n", "peer_groups_forbidden"),
        ("Groups:\t44 44 980\n", "peer_status_invalid"),
    ],
)
def test_capture_rejects_elevated_or_malformed_process_identity(
    tmp_path: Path,
    line: str,
    code: str,
) -> None:
    inspector, _executable, _alive = _fixture(tmp_path)
    status = inspector.proc_root / str(PID) / "status"
    rows = status.read_text(encoding="ascii").splitlines(keepends=True)
    prefix = line.split(":", 1)[0] + ":"
    status.write_text(
        "".join(line if row.startswith(prefix) else row for row in rows),
        encoding="ascii",
    )

    with pytest.raises(GuardError) as caught:
        inspector.capture(_Connection())  # type: ignore[arg-type]

    assert caught.value.code == code


def test_capture_rejects_wrong_executable_path_or_digest(tmp_path: Path) -> None:
    inspector, executable, _alive = _fixture(tmp_path)
    other = tmp_path / "other-supervisor"
    other.write_bytes(executable.read_bytes())
    other.chmod(0o555)
    link = inspector.proc_root / str(PID) / "exe"
    link.unlink()
    link.symlink_to(other)

    with pytest.raises(GuardError) as caught:
        inspector.capture(_Connection())  # type: ignore[arg-type]
    assert caught.value.code == "peer_executable_invalid"

    link.unlink()
    link.symlink_to(executable)
    executable.chmod(0o755)
    executable.write_bytes(b"changed")
    executable.chmod(0o555)
    with pytest.raises(GuardError) as caught:
        inspector.capture(_Connection())  # type: ignore[arg-type]
    assert caught.value.code == "peer_executable_invalid"


def test_rejected_executable_digest_does_not_leak_open_descriptors(tmp_path: Path) -> None:
    inspector, executable, _alive = _fixture(tmp_path)
    executable.chmod(0o755)
    executable.write_bytes(b"wrong-digest")
    executable.chmod(0o555)
    before = len(tuple(Path("/proc/self/fd").iterdir()))

    for _ in range(8):
        with pytest.raises(GuardError):
            inspector.capture(_Connection())  # type: ignore[arg-type]

    assert len(tuple(Path("/proc/self/fd").iterdir())) == before


def test_peer_revalidation_detects_death_reexec_and_cgroup_drift(tmp_path: Path) -> None:
    inspector, executable, alive = _fixture(tmp_path)
    peer = inspector.capture(_Connection())  # type: ignore[arg-type]
    try:
        alive["value"] = False
        with pytest.raises(GuardError) as caught:
            peer.assert_unchanged()
        assert caught.value.code == "peer_dead"

        alive["value"] = True
        replacement = tmp_path / "replacement"
        replacement.write_bytes(executable.read_bytes())
        replacement.chmod(0o555)
        link = inspector.proc_root / str(PID) / "exe"
        link.unlink()
        link.symlink_to(replacement)
        with pytest.raises(GuardError) as caught:
            peer.assert_unchanged()
        assert caught.value.code == "peer_executable_changed"

        link.unlink()
        link.symlink_to(executable)
        (inspector.proc_root / str(PID) / "cgroup").write_text(
            "0::/system.slice/slurmstepd.scope/job_999/step_batch/user/task_0\n",
            encoding="ascii",
        )
        with pytest.raises(GuardError) as caught:
            peer.assert_unchanged()
        assert caught.value.code == "peer_cgroup_changed"
    finally:
        peer.close()


def test_peer_adopts_only_exact_trusted_service_cgroup_once(tmp_path: Path) -> None:
    inspector, _executable, _alive = _fixture(tmp_path)
    peer = inspector.capture(_Connection())  # type: ignore[arg-type]
    trusted = PurePosixPath(f"{CGROUP}/loom-builder/trusted-service")
    cgroup_file = inspector.proc_root / str(PID) / "cgroup"
    try:
        cgroup_file.write_text(f"0::{trusted}\n", encoding="ascii")
        peer.adopt_trusted_service_cgroup()
        assert peer.cgroup_relative == trusted
        peer.assert_unchanged()

        cgroup_file.write_text(
            f"0::{CGROUP}/loom-builder/build-egress\n", encoding="ascii"
        )
        with pytest.raises(GuardError) as caught:
            peer.adopt_trusted_service_cgroup()
        assert caught.value.code == "peer_cgroup_transition_invalid"
    finally:
        peer.close()


def test_capture_after_guard_restart_recovers_exact_trusted_service_membership(
    tmp_path: Path,
) -> None:
    inspector, _executable, _alive = _fixture(tmp_path)
    trusted = PurePosixPath(f"{CGROUP}/loom-builder/trusted-service")
    (inspector.proc_root / str(PID) / "cgroup").write_text(
        f"0::{trusted}\n", encoding="ascii"
    )

    peer = inspector.capture(_Connection())  # type: ignore[arg-type]
    try:
        assert peer.batch_cgroup_relative == PurePosixPath(CGROUP)
        assert peer.cgroup_relative == trusted
        assert peer.job_id == "123"
        peer.assert_unchanged()
        cgroup_root = tmp_path / "cgroup"
        batch_path = cgroup_root / CGROUP.removeprefix("/")
        trusted_path = batch_path / "loom-builder" / "trusted-service"
        trusted_path.mkdir(parents=True)
        (batch_path / "cgroup.type").write_text("domain\n", encoding="ascii")
        (batch_path / "cgroup.procs").write_text("", encoding="ascii")
        (trusted_path / "cgroup.type").write_text("domain\n", encoding="ascii")
        (trusted_path / "cgroup.procs").write_text(f"{PID}\n", encoding="ascii")
        batch = derive_batch_cgroup(peer, job_id="123", cgroup_root=cgroup_root)
        assert batch.path == batch_path
        with pytest.raises(GuardError) as caught:
            peer.adopt_trusted_service_cgroup()
        assert caught.value.code == "peer_cgroup_transition_invalid"
    finally:
        peer.close()


def test_recapture_pid_uses_persisted_uid_gid_boundary_without_a_socket(
    tmp_path: Path,
) -> None:
    inspector, _executable, _alive = _fixture(tmp_path)

    peer = inspector.capture_pid(PID, expected_uid=UID, expected_gid=GID)
    try:
        assert (peer.pid, peer.uid, peer.gid, peer.job_id) == (PID, UID, GID, "123")
        peer.assert_unchanged()
    finally:
        peer.close()

    with pytest.raises(GuardError) as caught:
        inspector.capture_pid(PID, expected_uid=0, expected_gid=GID)
    assert caught.value.code == "peer_credentials_invalid"


def test_derive_batch_cgroup_requires_exact_single_process_task(tmp_path: Path) -> None:
    inspector, _executable, _alive = _fixture(tmp_path)
    peer = inspector.capture(_Connection())  # type: ignore[arg-type]
    cgroup_root = tmp_path / "cgroup"
    task = cgroup_root / CGROUP.removeprefix("/")
    task.mkdir(parents=True)
    (task / "cgroup.type").write_text("domain\n", encoding="ascii")
    (task / "cgroup.procs").write_text(f"{PID}\n", encoding="ascii")
    try:
        batch = derive_batch_cgroup(peer, job_id="123", cgroup_root=cgroup_root)
        assert batch.path == task
        assert batch.inode == task.stat().st_ino

        (task / "cgroup.procs").write_text(f"{PID}\n9999\n", encoding="ascii")
        with pytest.raises(GuardError) as caught:
            derive_batch_cgroup(peer, job_id="123", cgroup_root=cgroup_root)
        assert caught.value.code == "batch_cgroup_processes_invalid"
    finally:
        peer.close()


def test_derive_batch_cgroup_rejects_nonbatch_or_symlink_path(tmp_path: Path) -> None:
    inspector, _executable, _alive = _fixture(tmp_path)
    cgroup_file = inspector.proc_root / str(PID) / "cgroup"
    cgroup_file.write_text(
        "0::/system.slice/slurmstepd.scope/job_123/step_extern/user/task_0\n",
        encoding="ascii",
    )
    with pytest.raises(GuardError) as caught:
        inspector.capture(_Connection())  # type: ignore[arg-type]
    assert caught.value.code == "peer_cgroup_invalid"


def test_projection_request_is_exact_phase2b1_wire_contract(tmp_path: Path) -> None:
    inspector, _executable, _alive = _fixture(tmp_path)
    peer = inspector.capture(_Connection())  # type: ignore[arg-type]
    cgroup_root = tmp_path / "cgroup"
    task = cgroup_root / CGROUP.removeprefix("/")
    task.mkdir(parents=True)
    (task / "cgroup.type").write_text("domain\n", encoding="ascii")
    (task / "cgroup.procs").write_text(f"{PID}\n", encoding="ascii")
    try:
        batch = derive_batch_cgroup(peer, job_id="123", cgroup_root=cgroup_root)
        request = projection_request(
            grant_id=GRANT,
            request_id=REQUEST,
            observed_at=datetime(2026, 9, 2, 12, 30, tzinfo=UTC),
            node_name="trt-eai-oldlab-3",
            node_boot_id=BOOT,
            cluster_id="oldlab",
            cpu_arch="x86_64",
            slurm_request_sha256="a" * 64,
            slurm_qos="loom-task-image-builder-rootless-oldlab",
            peer=peer,
            batch=batch,
        )
        assert request == {
            "schema_version": 1,
            "request_id": str(REQUEST),
            "grant_id": str(GRANT),
            "observed_at": "2026-09-02T12:30:00Z",
            "node_name": "trt-eai-oldlab-3",
            "node_boot_id": str(BOOT),
            "slurm_cluster_id": "oldlab",
            "slurm_job_id": "123",
            "supervisor_pid": PID,
            "supervisor_uid": UID,
            "supervisor_gid": GID,
            "supervisor_executable_sha256": peer.executable_sha256,
            "cgroup_path": f"/sys/fs/cgroup{CGROUP}",
            "cgroup_inode": batch.inode,
            "submitting_identity": "loom-builder",
            "slurm_account": "loom-task-builder",
            "slurm_partition": "loom-task-builder",
            "slurm_qos": "loom-task-image-builder-rootless-oldlab",
            "cpu_arch": "x86_64",
            "slurm_request_sha256": "a" * 64,
        }
    finally:
        peer.close()
