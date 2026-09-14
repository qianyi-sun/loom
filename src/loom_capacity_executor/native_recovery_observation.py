"""Read-only pre-launch host facts, not protected publication or cleanup authority.

Only the fixed original-UID installed worker may call this observer. The caller
must obtain node/config/profile identities from protected installed authority,
not feature inputs or the hostname. A successful observation does not authorize
execution, attest a launch profile, prove quiescence, or release capacity.
"""

from __future__ import annotations

import os
import stat
from contextlib import ExitStack
from pathlib import Path, PurePosixPath
from uuid import UUID

from pydantic import Field

from loom_capacity_agent.native_recovery import (
    NativeInstalledAttemptV1,
    NativeRecoveryPreparationV1,
)
from loom_capacity_executor.native_installed_release import (
    _Observation,
    _path,
    _require_original_identity,
)
from loom_capacity_executor.native_mapped_scratch import _mount_id
from loom_capacity_executor.native_oci_material import _open_directory
from loom_capacity_manager.contracts import (
    Identifier,
    StrictV1Model,
    canonical_bytes,
    canonical_digest,
)
from loom_control_plane.slurm_job_cgroup import _slurm_job_scope

_CGROUP_ROOT = Path("/sys/fs/cgroup")
_PROCESS = Path("/proc/self/cgroup")
_BOOT = Path("/proc/sys/kernel/random/boot_id")


class NativeRecoveryHostIdentityV1(StrictV1Model):
    """Boot-specific identity retained by a protected installer in host namespaces.

    The installer must archive old canonical records by digest for historical
    recovery. Reboot requires a fresh record and protected admission; the worker
    cannot refresh this authority by copying its current kernel observations.
    """

    node_id: Identifier
    boot_id: UUID
    original_uid: int = Field(gt=0, lt=2**32 - 1)
    original_gid: int = Field(gt=0, lt=2**32 - 1)
    cgroup_namespace_device: int = Field(ge=0)
    cgroup_namespace_inode: int = Field(gt=0)


def read_native_recovery_host_identity(path: Path, *, expected_sha256: str) -> NativeRecoveryHostIdentityV1:
    """Read only an exact immutable root-owned installer record in original UID."""
    _require_original_identity()
    _path(str(path))
    observation = _Observation()
    wire = observation.read(path, digest=expected_sha256, size=None, mode=0o444, bound=4096, collect=True)
    identity = NativeRecoveryHostIdentityV1.model_validate_json(wire)
    if canonical_bytes(identity) != wire:
        raise ValueError("native recovery host identity must be canonical")
    observation.finish()
    return identity


def _read_kernel_text(path: Path, bound: int) -> str:
    descriptor = os.open(path, os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW | os.O_NONBLOCK)
    try:
        if not stat.S_ISREG(os.fstat(descriptor).st_mode):
            raise ValueError("native recovery kernel observation is not a regular pseudofile")
        wire = bytearray()
        while len(wire) <= bound:
            chunk = os.read(descriptor, min(65536, bound + 1 - len(wire)))
            if not chunk:
                break
            wire.extend(chunk)
        if len(wire) > bound:
            raise ValueError("native recovery kernel observation exceeds byte bound")
        return wire.decode("ascii")
    finally:
        os.close(descriptor)


def _require_host_cgroup_namespace(identity: NativeRecoveryHostIdentityV1) -> None:
    # Intentional self proc namespace magic link. PID1 links require ptrace
    # permission an unprivileged worker must not have; use retained host facts.
    current = os.stat("/proc/self/ns/cgroup")
    if (identity.cgroup_namespace_device, identity.cgroup_namespace_inode) != (current.st_dev, current.st_ino):
        raise ValueError("native recovery requires the host cgroup namespace")


def _require_cgroup_mount(descriptor: int) -> int:
    mount = _mount_id(descriptor)
    rows = [row.split() for row in _read_kernel_text(Path("/proc/self/mountinfo"), 1024**2).splitlines()]
    matches = [row for row in rows if row and row[0] == str(mount)]
    if len(matches) != 1:
        raise ValueError("native recovery cgroup mount identity is unavailable")
    row = matches[0]
    separator = row.index("-") if "-" in row else -1
    if (mount <= 0 or separator < 6 or len(row) != separator + 4
        or row[3:5] != ["/", "/sys/fs/cgroup"] or row[separator + 1] != "cgroup2"):
        raise ValueError("native recovery requires the full host cgroup2 mount")
    return mount


def _process_scope(wire: str, job_id: str) -> tuple[PurePosixPath, PurePosixPath]:
    rows = wire.splitlines()
    if len(rows) != 1 or not rows[0].startswith("0::"):
        raise ValueError("native recovery requires one unified cgroup entry")
    process = PurePosixPath(_path(rows[0][3:]))
    job = _slurm_job_scope(process, job_id)
    if job not in process.parents or any(part.startswith("job_") for part in process.relative_to(job).parts):
        raise ValueError("native recovery process is not below its exact Slurm job")
    return process, job


def _directory_identity(descriptor: int, *, private: bool) -> tuple[int, int, int]:
    metadata = os.fstat(descriptor)
    if (not stat.S_ISDIR(metadata.st_mode) or (private and (
        (metadata.st_uid, metadata.st_gid) != (os.getuid(), os.getgid())
        or stat.S_IMODE(metadata.st_mode) != 0o700))):
        raise ValueError("native recovery attempt directory is not private and worker-owned")
    mount = _mount_id(descriptor)
    if mount <= 0:
        raise ValueError("native recovery directory mount identity is unavailable")
    return metadata.st_dev, metadata.st_ino, mount


def capture_native_recovery_preparation(locator: NativeInstalledAttemptV1, *,
    launch_profile_sha256: str, node_configuration_sha256: str,
    host_identity: NativeRecoveryHostIdentityV1,
) -> NativeRecoveryPreparationV1:
    """Capture exact host job/boot/directory facts before creating mapped material.

    All opened directories remain pinned through final kernel/path readback.
    The returned facts must still be committed through authenticated admission
    before mapped launch; a local record/hash is not that acknowledgment.
    """
    _require_original_identity()
    host_identity = NativeRecoveryHostIdentityV1.model_validate_json(host_identity.model_dump_json())
    if canonical_digest(host_identity) != node_configuration_sha256:
        raise ValueError("native recovery protected host identity digest changed")
    _require_host_cgroup_namespace(host_identity)
    locator = NativeInstalledAttemptV1.model_validate_json(locator.model_dump_json())
    attempt = _path(locator.directory)
    process_wire = _read_kernel_text(_PROCESS, 8192)
    process, job = _process_scope(process_wire, locator.physical.slurm_job_id)
    boot_wire = _read_kernel_text(_BOOT, 128)
    boot_id = UUID(boot_wire.strip())
    if boot_wire != f"{boot_id}\n":
        raise ValueError("native recovery boot identity is not canonical")
    if (boot_id != host_identity.boot_id or
        (os.getuid(), os.getgid()) != (host_identity.original_uid, host_identity.original_gid)):
        raise ValueError("native recovery host boot or worker identity changed")
    with ExitStack() as stack:
        root = _open_directory(_CGROUP_ROOT, stack)
        mount = _require_cgroup_mount(root)
        watched = {}
        for path, private in ((_CGROUP_ROOT, False), (_CGROUP_ROOT / job.relative_to("/"), False),
            (_CGROUP_ROOT / process.relative_to("/"), False), (attempt.parent, True), (attempt, True)):
            descriptor = _open_directory(path, stack)
            identity = _directory_identity(descriptor, private=private)
            if not private and identity[2] != mount:
                raise ValueError("native recovery cgroup path crosses a mount")
            watched[path] = identity, private
        actual_attempt = watched[attempt][0]
        if (actual_attempt[:2] != (locator.device, locator.inode)
            or actual_attempt[2] != watched[attempt.parent][0][2]):
            raise ValueError("native recovery locator directory identity changed")
        job_identity = watched[_CGROUP_ROOT / job.relative_to("/")][0]
        facts = NativeRecoveryPreparationV1(locator=locator, launch_profile_sha256=launch_profile_sha256,
            node_configuration_sha256=node_configuration_sha256, node_id=host_identity.node_id, boot_id=boot_id,
            original_uid=os.getuid(), original_gid=os.getgid(), cgroup_path=str(job),
            cgroup_device=job_identity[0], cgroup_inode=job_identity[1], cgroup_mount_id=job_identity[2])
        if _read_kernel_text(_PROCESS, 8192) != process_wire or _read_kernel_text(_BOOT, 128) != boot_wire:
            raise ValueError("native recovery process or boot changed during observation")
        _require_original_identity()
        _require_host_cgroup_namespace(host_identity)
        if (os.getuid(), os.getgid()) != (host_identity.original_uid, host_identity.original_gid):
            raise ValueError("native recovery worker identity changed during observation")
        if _require_cgroup_mount(root) != mount:
            raise ValueError("native recovery cgroup mount changed during observation")
        for path, (identity, private) in watched.items():
            if _directory_identity(_open_directory(path, stack), private=private) != identity:
                raise ValueError("native recovery directory changed during observation")
        return facts
