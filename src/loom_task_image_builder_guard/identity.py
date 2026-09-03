"""Kernel-derived peer, executable, and Slurm batch-cgroup identity."""

from __future__ import annotations

import ctypes
import hashlib
import os
import platform
import re
import select
import socket
import stat
import struct
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath
from uuid import UUID

from loom_task_image_builder_guard.errors import GuardError
from loom_task_image_builder_guard.models import IdentityConfig

_PEER_CREDENTIALS = struct.Struct("3i")
_JOB_COMPONENT = re.compile(r"^job_([1-9][0-9]{0,31})$")
_MAX_PROC_BYTES = 64 * 1024


def _pidfd_open(pid: int) -> int:
    native = getattr(os, "pidfd_open", None)
    if native is not None:
        return int(native(pid, 0))
    number = {"x86_64": 434, "aarch64": 434}.get(platform.machine())
    if number is None:
        raise GuardError("pidfd_unavailable")
    libc = ctypes.CDLL(None, use_errno=True)
    descriptor = int(libc.syscall(number, pid, 0))
    if descriptor < 0:
        error = ctypes.get_errno()
        raise OSError(error, os.strerror(error))
    os.set_inheritable(descriptor, False)
    return descriptor


def _pidfd_alive(descriptor: int) -> bool:
    poller = select.poll()
    poller.register(descriptor, select.POLLIN | select.POLLHUP | select.POLLERR)
    return not poller.poll(0)


def _read_bounded(path: Path, *, maximum: int = _MAX_PROC_BYTES) -> bytes:
    descriptor: int | None = None
    try:
        descriptor = os.open(path, os.O_RDONLY | os.O_CLOEXEC | getattr(os, "O_NOFOLLOW", 0))
        chunks: list[bytes] = []
        total = 0
        while total <= maximum:
            chunk = os.read(descriptor, min(16 * 1024, maximum + 1 - total))
            if not chunk:
                break
            chunks.append(chunk)
            total += len(chunk)
        if total > maximum:
            raise GuardError("peer_proc_invalid")
        return b"".join(chunks)
    except GuardError:
        raise
    except OSError as exc:
        raise GuardError("peer_proc_invalid") from exc
    finally:
        if descriptor is not None:
            os.close(descriptor)


def _decode_ascii(payload: bytes, *, code: str) -> str:
    try:
        value = payload.decode("ascii")
    except UnicodeDecodeError:
        raise GuardError(code) from None
    if "\x00" in value or "\r" in value:
        raise GuardError(code)
    return value


def _parse_status(payload: bytes, config: IdentityConfig) -> tuple[int, ...]:
    rows: dict[str, str] = {}
    for line in _decode_ascii(payload, code="peer_status_invalid").splitlines():
        key, separator, value = line.partition(":")
        if separator and key in {"Pid", "Uid", "Gid", "Groups"}:
            if key in rows:
                raise GuardError("peer_status_invalid")
            rows[key] = value.strip()
    if set(rows) != {"Pid", "Uid", "Gid", "Groups"}:
        raise GuardError("peer_status_invalid")
    try:
        uid_values = tuple(int(item) for item in rows["Uid"].split())
        gid_values = tuple(int(item) for item in rows["Gid"].split())
        groups = tuple(int(item) for item in rows["Groups"].split())
    except ValueError:
        raise GuardError("peer_status_invalid") from None
    if (
        len(uid_values) != 4
        or len(gid_values) != 4
        or any(item != config.uid for item in uid_values)
        or any(item != config.gid for item in gid_values)
    ):
        raise GuardError("peer_identity_invalid")
    if groups != tuple(sorted(set(groups))) or any(item < 0 for item in groups):
        raise GuardError("peer_status_invalid")
    if set(groups).intersection(config.forbidden_supplementary_gids):
        raise GuardError("peer_groups_forbidden")
    return groups


def _unified_cgroup_path(payload: bytes) -> PurePosixPath:
    matches: list[str] = []
    for line in _decode_ascii(payload, code="peer_cgroup_invalid").splitlines():
        hierarchy, separator, remainder = line.partition(":")
        controllers, second, raw_path = remainder.partition(":")
        if separator and second and hierarchy == "0" and controllers == "":
            matches.append(raw_path)
    if len(matches) != 1:
        raise GuardError("peer_cgroup_invalid")
    raw = matches[0]
    path = PurePosixPath(raw)
    if (
        not path.is_absolute()
        or path == PurePosixPath("/")
        or "//" in raw
        or any(part in {"", ".", ".."} for part in raw.split("/")[1:])
        or path.as_posix() != raw
    ):
        raise GuardError("peer_cgroup_invalid")
    return path


def _parse_cgroup(payload: bytes) -> tuple[PurePosixPath, str]:
    path = _unified_cgroup_path(payload)
    parts = path.parts[1:]
    if len(parts) < 6 or tuple(parts[-3:]) != ("step_batch", "user", "task_0"):
        raise GuardError("peer_cgroup_invalid")
    job_index = len(parts) - 4
    matched = _JOB_COMPONENT.fullmatch(parts[job_index])
    if matched is None or not any(
        part == "slurm" or part == "slurmstepd.scope" or part.endswith("_slurmstepd.scope")
        for part in parts[:job_index]
    ):
        raise GuardError("peer_cgroup_invalid")
    if any(_JOB_COMPONENT.fullmatch(part) for part in parts[:job_index]):
        raise GuardError("peer_cgroup_invalid")
    return path, matched.group(1)


def _hash_open_file(descriptor: int, *, maximum: int = 128 * 1024 * 1024) -> str:
    digest = hashlib.sha256()
    offset = 0
    while offset <= maximum:
        chunk = os.pread(descriptor, min(1024 * 1024, maximum + 1 - offset), offset)
        if not chunk:
            return digest.hexdigest()
        digest.update(chunk)
        offset += len(chunk)
    raise GuardError("peer_executable_invalid")


@dataclass(slots=True)
class PeerHandle:
    pid: int
    uid: int
    gid: int
    groups: tuple[int, ...]
    pidfd: int
    executable_fd: int
    executable_path: Path
    executable_device: int
    executable_inode: int
    executable_sha256: str
    batch_cgroup_relative: PurePosixPath
    cgroup_relative: PurePosixPath
    job_id: str
    _inspector: PeerInspector = field(repr=False)
    _closed: bool = field(default=False, init=False, repr=False)

    def assert_unchanged(self) -> None:
        if self._closed:
            raise GuardError("peer_closed")
        self._inspector._assert_unchanged(self)

    def adopt_trusted_service_cgroup(self) -> None:
        if self._closed or self.cgroup_relative != self.batch_cgroup_relative:
            raise GuardError("peer_cgroup_transition_invalid")
        expected = self.batch_cgroup_relative / "loom-builder" / "trusted-service"
        try:
            self._inspector._assert_unchanged(self, expected_cgroup=expected)
        except GuardError as exc:
            if exc.code == "peer_cgroup_changed":
                raise GuardError("peer_cgroup_transition_invalid") from None
            raise
        self.cgroup_relative = expected

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        os.close(self.executable_fd)
        os.close(self.pidfd)


class PeerInspector:
    """Capture a Unix peer before slow work and retain pid/executable FDs."""

    def __init__(
        self,
        config: IdentityConfig,
        *,
        proc_root: Path = Path("/proc"),
        host_root: Path = Path("/"),
        trusted_file_uid: int = 0,
        pidfd_open: Callable[[int], int] = _pidfd_open,
        pidfd_alive: Callable[[int], bool] = _pidfd_alive,
    ) -> None:
        self.config = config
        self.proc_root = proc_root
        self.host_root = host_root
        self.trusted_file_uid = trusted_file_uid
        self._pidfd_open = pidfd_open
        self._pidfd_alive = pidfd_alive

    def _expected_executable(self) -> Path:
        if self.host_root == Path("/"):
            return self.config.supervisor_path
        return self.host_root.joinpath(*self.config.supervisor_path.parts[1:])

    def _open_executable(self, pid: int) -> tuple[int, Path, os.stat_result, str]:
        proc_exe = self.proc_root / str(pid) / "exe"
        descriptor: int | None = None
        complete = False
        try:
            target = Path(os.readlink(proc_exe))
            expected = self._expected_executable()
            if target != expected or expected.resolve(strict=True) != expected:
                raise GuardError("peer_executable_invalid")
            descriptor = os.open(proc_exe, os.O_RDONLY | os.O_CLOEXEC)
            metadata = os.fstat(descriptor)
            if (
                not stat.S_ISREG(metadata.st_mode)
                or metadata.st_nlink != 1
                or metadata.st_uid != self.trusted_file_uid
                or stat.S_IMODE(metadata.st_mode) & 0o022
                or metadata.st_size <= 0
            ):
                raise GuardError("peer_executable_invalid")
            digest = _hash_open_file(descriptor)
            if digest != self.config.supervisor_sha256:
                raise GuardError("peer_executable_invalid")
            complete = True
            return descriptor, target, metadata, digest
        except GuardError:
            raise
        except OSError as exc:
            raise GuardError("peer_executable_invalid") from exc
        finally:
            if descriptor is not None and not complete:
                os.close(descriptor)

    def capture(self, connection: socket.socket) -> PeerHandle:
        if connection.family != socket.AF_UNIX:
            raise GuardError("peer_socket_invalid")
        pidfd: int | None = None
        executable_fd: int | None = None
        complete = False
        try:
            credentials = connection.getsockopt(
                socket.SOL_SOCKET, socket.SO_PEERCRED, _PEER_CREDENTIALS.size
            )
            if not isinstance(credentials, bytes) or len(credentials) != _PEER_CREDENTIALS.size:
                raise GuardError("peer_credentials_invalid")
            pid, uid, gid = _PEER_CREDENTIALS.unpack(credentials)
            if pid <= 0 or uid != self.config.uid or gid != self.config.gid:
                raise GuardError("peer_credentials_invalid")
            pidfd = self._pidfd_open(pid)
            if not self._pidfd_alive(pidfd):
                raise GuardError("peer_dead")
            status = _read_bounded(self.proc_root / str(pid) / "status")
            groups = _parse_status(status, self.config)
            pid_row = next(
                (
                    line
                    for line in _decode_ascii(status, code="peer_status_invalid").splitlines()
                    if line.startswith("Pid:")
                ),
                "",
            )
            if pid_row.split()[1:] != [str(pid)]:
                raise GuardError("peer_status_invalid")
            cgroup_relative, job_id = _parse_cgroup(
                _read_bounded(self.proc_root / str(pid) / "cgroup")
            )
            executable_fd, executable_path, metadata, digest = self._open_executable(pid)
            complete = True
            return PeerHandle(
                pid=pid,
                uid=uid,
                gid=gid,
                groups=groups,
                pidfd=pidfd,
                executable_fd=executable_fd,
                executable_path=executable_path,
                executable_device=metadata.st_dev,
                executable_inode=metadata.st_ino,
                executable_sha256=digest,
                batch_cgroup_relative=cgroup_relative,
                cgroup_relative=cgroup_relative,
                job_id=job_id,
                _inspector=self,
            )
        except GuardError:
            raise
        except OSError as exc:
            raise GuardError("peer_inspection_failed") from exc
        finally:
            if not complete:
                if executable_fd is not None:
                    os.close(executable_fd)
                if pidfd is not None:
                    os.close(pidfd)

    def _assert_unchanged(
        self,
        peer: PeerHandle,
        *,
        expected_cgroup: PurePosixPath | None = None,
    ) -> None:
        if not self._pidfd_alive(peer.pidfd):
            raise GuardError("peer_dead")
        try:
            metadata = os.fstat(peer.executable_fd)
        except OSError as exc:
            raise GuardError("peer_executable_changed") from exc
        if (metadata.st_dev, metadata.st_ino) != (
            peer.executable_device,
            peer.executable_inode,
        ) or _hash_open_file(peer.executable_fd) != peer.executable_sha256:
            raise GuardError("peer_executable_changed")
        replacement_fd: int | None = None
        try:
            replacement_fd, path, replacement, digest = self._open_executable(peer.pid)
            if (
                path != peer.executable_path
                or (replacement.st_dev, replacement.st_ino)
                != (peer.executable_device, peer.executable_inode)
                or digest != peer.executable_sha256
            ):
                raise GuardError("peer_executable_changed")
        except GuardError as exc:
            if exc.code == "peer_executable_invalid":
                raise GuardError("peer_executable_changed") from None
            raise
        finally:
            if replacement_fd is not None:
                os.close(replacement_fd)
        status = _read_bounded(self.proc_root / str(peer.pid) / "status")
        if _parse_status(status, self.config) != peer.groups:
            raise GuardError("peer_identity_changed")
        cgroup = _unified_cgroup_path(
            _read_bounded(self.proc_root / str(peer.pid) / "cgroup")
        )
        if cgroup != (peer.cgroup_relative if expected_cgroup is None else expected_cgroup):
            raise GuardError("peer_cgroup_changed")


@dataclass(frozen=True, slots=True)
class BatchCgroup:
    path: Path
    relative_path: PurePosixPath
    inode: int
    peer_pid: int

    @property
    def authority_path(self) -> str:
        return f"/sys/fs/cgroup{self.relative_path.as_posix()}"


def derive_batch_cgroup(
    peer: PeerHandle,
    *,
    job_id: str,
    cgroup_root: Path = Path("/sys/fs/cgroup"),
) -> BatchCgroup:
    """Bind the peer to one exact, sole-process Slurm batch task cgroup."""

    peer.assert_unchanged()
    if peer.job_id != job_id:
        raise GuardError("batch_cgroup_job_invalid")
    expected = cgroup_root.joinpath(*peer.cgroup_relative.parts[1:])
    try:
        root = cgroup_root.resolve(strict=True)
        resolved = expected.resolve(strict=True)
        resolved.relative_to(root)
        if resolved != expected or not resolved.is_dir():
            raise GuardError("batch_cgroup_path_invalid")
        metadata = resolved.stat()
        cgroup_type = _decode_ascii(
            _read_bounded(resolved / "cgroup.type"), code="batch_cgroup_type_invalid"
        ).strip()
        process_rows = _decode_ascii(
            _read_bounded(resolved / "cgroup.procs"), code="batch_cgroup_processes_invalid"
        ).split()
    except GuardError:
        raise
    except (OSError, ValueError) as exc:
        raise GuardError("batch_cgroup_path_invalid") from exc
    if cgroup_type != "domain":
        raise GuardError("batch_cgroup_type_invalid")
    if process_rows != [str(peer.pid)]:
        raise GuardError("batch_cgroup_processes_invalid")
    peer.assert_unchanged()
    final = resolved.stat()
    if (final.st_dev, final.st_ino) != (metadata.st_dev, metadata.st_ino):
        raise GuardError("batch_cgroup_changed")
    return BatchCgroup(resolved, peer.cgroup_relative, metadata.st_ino, peer.pid)


def _timestamp(value: datetime) -> str:
    if value.tzinfo is None or value.utcoffset() is None:
        raise GuardError("projection_time_invalid")
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def projection_request(
    *,
    grant_id: UUID,
    request_id: UUID,
    observed_at: datetime,
    node_name: str,
    node_boot_id: UUID,
    cluster_id: str,
    cpu_arch: str,
    slurm_request_sha256: str,
    slurm_qos: str,
    peer: PeerHandle,
    batch: BatchCgroup,
) -> dict[str, object]:
    """Render the exact Phase 2B1 projection request from local evidence."""

    if grant_id.int == 0 or request_id.int == 0 or node_boot_id.int == 0:
        raise GuardError("projection_identity_invalid")
    return {
        "schema_version": 1,
        "request_id": str(request_id),
        "grant_id": str(grant_id),
        "observed_at": _timestamp(observed_at),
        "node_name": node_name,
        "node_boot_id": str(node_boot_id),
        "slurm_cluster_id": cluster_id,
        "slurm_job_id": peer.job_id,
        "supervisor_pid": peer.pid,
        "supervisor_uid": peer.uid,
        "supervisor_gid": peer.gid,
        "supervisor_executable_sha256": peer.executable_sha256,
        "cgroup_path": batch.authority_path,
        "cgroup_inode": batch.inode,
        "submitting_identity": "loom-builder",
        "slurm_account": "loom-task-builder",
        "slurm_partition": "loom-task-builder",
        "slurm_qos": slurm_qos,
        "cpu_arch": cpu_arch,
        "slurm_request_sha256": slurm_request_sha256,
    }


__all__ = [
    "BatchCgroup",
    "PeerHandle",
    "PeerInspector",
    "derive_batch_cgroup",
    "projection_request",
]
