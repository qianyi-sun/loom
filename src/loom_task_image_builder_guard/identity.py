"""Kernel-derived peer, executable, and Slurm batch-cgroup identity."""

from __future__ import annotations

import ctypes
import errno
import hashlib
import os
import platform
import re
import select
import signal
import socket
import stat
import struct
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath
from uuid import UUID

from loom_task_image_builder_guard.errors import GuardError
from loom_task_image_builder_guard.models import IdentityConfig
from loom_task_image_builder_guard.protocol import PeerCredentials

_PEER_CREDENTIALS = struct.Struct("3i")
_JOB_COMPONENT = re.compile(r"^job_([1-9][0-9]{0,31})$")
_SOCKET_DESCRIPTOR = re.compile(r"^socket:\[([1-9][0-9]{0,31})\]$")
_MAX_PROC_BYTES = 64 * 1024
_MAX_VISIBLE_PROCESSES = 131072
_MAX_VISIBLE_DESCRIPTORS = 1 << 20
_PROCESS_STATE_TIMEOUT_SECONDS = 2.0
_NETLINK_SOCK_DIAG = 4
_SOCK_DIAG_BY_FAMILY = 20
_NLM_F_REQUEST = 1
_NLMSG_ERROR = 2
_UNIX_DIAG_PEER = 2
_UDIAG_SHOW_PEER = 4
_UNIX_STATE_ESTABLISHED = 1
_MAX_NETLINK_BYTES = 64 * 1024
_NLMSG_HEADER = struct.Struct("=IHHII")
_UNIX_DIAG_REQUEST = struct.Struct("=BBHIIIII")
_UNIX_DIAG_MESSAGE = struct.Struct("=BBBBIII")
_NETLINK_ATTRIBUTE = struct.Struct("=HH")


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


def _pidfd_send_signal(descriptor: int, signum: int) -> None:
    number = {"x86_64": 424, "aarch64": 424}.get(platform.machine())
    if number is None:
        raise GuardError("pidfd_unavailable")
    libc = ctypes.CDLL(None, use_errno=True)
    result = int(libc.syscall(number, descriptor, signum, 0, 0))
    if result < 0:
        error = ctypes.get_errno()
        raise OSError(error, os.strerror(error))


def _fd_table_shared(first_pid: int, second_pid: int) -> bool:
    number = {"x86_64": 312, "aarch64": 272}.get(platform.machine())
    if number is None:
        raise GuardError("pidfd_unavailable")
    libc = ctypes.CDLL(None, use_errno=True)
    result = int(libc.syscall(number, first_pid, second_pid, 2, 0, 0))
    if result < 0:
        error = ctypes.get_errno()
        raise OSError(error, os.strerror(error))
    return result == 0


def _pidfd_getfd(pidfd: int, target_fd: int) -> int:
    number = {"x86_64": 438, "aarch64": 438}.get(platform.machine())
    if number is None:
        raise GuardError("pidfd_unavailable")
    libc = ctypes.CDLL(None, use_errno=True)
    descriptor = int(libc.syscall(number, pidfd, target_fd, 0))
    if descriptor < 0:
        error = ctypes.get_errno()
        raise OSError(error, os.strerror(error))
    os.set_inheritable(descriptor, False)
    return descriptor


def _unix_peer_inode(connection: socket.socket) -> int:
    """Resolve one accepted Unix endpoint to its exact connected peer inode."""

    diagnostic: socket.socket | None = None
    try:
        if (
            connection.family != socket.AF_UNIX
            or connection.getsockopt(socket.SOL_SOCKET, socket.SO_TYPE)
            != socket.SOCK_SEQPACKET
        ):
            raise GuardError("peer_socket_invalid")
        inode = os.fstat(connection.fileno()).st_ino
        if not 1 <= inode <= (1 << 32) - 1:
            raise GuardError("peer_socket_invalid")
        diagnostic = socket.socket(
            socket.AF_NETLINK,
            socket.SOCK_RAW | socket.SOCK_CLOEXEC,
            _NETLINK_SOCK_DIAG,
        )
        diagnostic.settimeout(1.0)
        diagnostic.bind((0, 0))
        local = diagnostic.getsockname()
        if not isinstance(local, tuple) or len(local) != 2 or local[0] <= 0:
            raise GuardError("peer_socket_invalid")
        sequence = inode
        request = _UNIX_DIAG_REQUEST.pack(
            socket.AF_UNIX,
            0,
            0,
            (1 << 32) - 1,
            inode,
            _UDIAG_SHOW_PEER,
            (1 << 32) - 1,
            (1 << 32) - 1,
        )
        header = _NLMSG_HEADER.pack(
            _NLMSG_HEADER.size + len(request),
            _SOCK_DIAG_BY_FAMILY,
            _NLM_F_REQUEST,
            sequence,
            0,
        )
        outbound = header + request
        if diagnostic.sendto(outbound, (0, 0)) != len(outbound):
            raise GuardError("peer_socket_invalid")
        payload, _ancillary, flags, address = diagnostic.recvmsg(_MAX_NETLINK_BYTES)
        if (
            flags & (socket.MSG_TRUNC | socket.MSG_CTRUNC)
            or not isinstance(address, tuple)
            or address != (0, 0)
            or len(payload) < _NLMSG_HEADER.size
        ):
            raise GuardError("peer_socket_invalid")
        offset = 0
        peer_inode: int | None = None
        messages = 0
        while offset < len(payload):
            if len(payload) - offset < _NLMSG_HEADER.size:
                raise GuardError("peer_socket_invalid")
            length, kind, _message_flags, observed_sequence, port_id = (
                _NLMSG_HEADER.unpack_from(payload, offset)
            )
            if (
                length < _NLMSG_HEADER.size
                or offset + length > len(payload)
                or observed_sequence != sequence
                or port_id != local[0]
                or kind == _NLMSG_ERROR
                or kind != _SOCK_DIAG_BY_FAMILY
            ):
                raise GuardError("peer_socket_invalid")
            body = payload[offset + _NLMSG_HEADER.size : offset + length]
            if len(body) < _UNIX_DIAG_MESSAGE.size:
                raise GuardError("peer_socket_invalid")
            family, socket_type, state, _pad, observed_inode, _cookie0, _cookie1 = (
                _UNIX_DIAG_MESSAGE.unpack_from(body)
            )
            if (
                family != socket.AF_UNIX
                or socket_type != socket.SOCK_SEQPACKET
                or state != _UNIX_STATE_ESTABLISHED
                or observed_inode != inode
            ):
                raise GuardError("peer_socket_invalid")
            attribute_offset = _UNIX_DIAG_MESSAGE.size
            found: int | None = None
            while attribute_offset < len(body):
                if len(body) - attribute_offset < _NETLINK_ATTRIBUTE.size:
                    raise GuardError("peer_socket_invalid")
                attribute_length, attribute_kind = _NETLINK_ATTRIBUTE.unpack_from(
                    body, attribute_offset
                )
                if (
                    attribute_length < _NETLINK_ATTRIBUTE.size
                    or attribute_offset + attribute_length > len(body)
                ):
                    raise GuardError("peer_socket_invalid")
                value = body[
                    attribute_offset + _NETLINK_ATTRIBUTE.size :
                    attribute_offset + attribute_length
                ]
                if attribute_kind == _UNIX_DIAG_PEER:
                    if found is not None or len(value) != 4:
                        raise GuardError("peer_socket_invalid")
                    found = struct.unpack("=I", value)[0]
                attribute_offset += (attribute_length + 3) & ~3
            if found is None or found <= 0 or peer_inode is not None:
                raise GuardError("peer_socket_invalid")
            peer_inode = found
            messages += 1
            offset += (length + 3) & ~3
        if messages != 1 or peer_inode is None:
            raise GuardError("peer_socket_invalid")
        return peer_inode
    except GuardError:
        raise
    except (OSError, TimeoutError, TypeError, ValueError) as exc:
        raise GuardError("peer_socket_invalid") from exc
    finally:
        if diagnostic is not None:
            diagnostic.close()


def _socket_identity(
    pidfd: int,
    target_fd: int,
) -> tuple[int, int, object, object, int]:
    descriptor = _pidfd_getfd(pidfd, target_fd)
    try:
        inode = os.fstat(descriptor).st_ino
        duplicate = socket.socket(fileno=descriptor)
        descriptor = -1
        with duplicate:
            family = int(duplicate.family)
            socket_type = int(duplicate.getsockopt(socket.SOL_SOCKET, socket.SO_TYPE))
            if family != socket.AF_UNIX:
                return family, socket_type, None, None, inode
            return (
                family,
                socket_type,
                duplicate.getsockname(),
                duplicate.getpeername(),
                inode,
            )
    finally:
        if descriptor >= 0:
            os.close(descriptor)


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


def _process_state(payload: bytes, pid: int) -> tuple[str, int]:
    rows: dict[str, str] = {}
    for line in _decode_ascii(payload, code="peer_status_invalid").splitlines():
        key, separator, value = line.partition(":")
        if separator and key in {"Pid", "State", "Threads"}:
            if key in rows:
                raise GuardError("peer_status_invalid")
            rows[key] = value.strip()
    if set(rows) != {"Pid", "State", "Threads"}:
        raise GuardError("peer_status_invalid")
    state_fields = rows["State"].split()
    try:
        observed_pid = int(rows["Pid"])
        threads = int(rows["Threads"])
    except ValueError:
        raise GuardError("peer_status_invalid") from None
    if observed_pid != pid or len(state_fields) < 1 or len(state_fields[0]) != 1 or threads <= 0:
        raise GuardError("peer_status_invalid")
    return state_fields[0], threads


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


def _parse_cgroup(payload: bytes) -> tuple[PurePosixPath, PurePosixPath, str]:
    path = _unified_cgroup_path(payload)
    parts = path.parts[1:]
    trusted_suffix = ("step_batch", "user", "task_0", "loom-builder", "trusted-service")
    if len(parts) >= 8 and tuple(parts[-5:]) == trusted_suffix:
        batch = PurePosixPath(*path.parts[:-2])
        batch_parts = parts[:-2]
    elif len(parts) >= 6 and tuple(parts[-3:]) == ("step_batch", "user", "task_0"):
        batch = path
        batch_parts = parts
    else:
        raise GuardError("peer_cgroup_invalid")
    job_index = len(batch_parts) - 4
    matched = _JOB_COMPONENT.fullmatch(batch_parts[job_index])
    if matched is None or not any(
        part == "slurm" or part == "slurmstepd.scope" or part.endswith("_slurmstepd.scope")
        for part in batch_parts[:job_index]
    ):
        raise GuardError("peer_cgroup_invalid")
    if any(_JOB_COMPONENT.fullmatch(part) for part in batch_parts[:job_index]):
        raise GuardError("peer_cgroup_invalid")
    return batch, path, matched.group(1)


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
    control_socket_path: str | None
    control_peer_inode: int | None
    _inspector: PeerInspector = field(repr=False)
    _closed: bool = field(default=False, init=False, repr=False)
    _containment_held: bool = field(default=False, init=False, repr=False)

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

    @contextmanager
    def containment_hold(self) -> Iterator[None]:
        if self._closed or self._containment_held:
            raise GuardError("peer_containment_hold_invalid")
        inventory = self._inspector._begin_containment_hold(self)
        self._containment_held = True
        try:
            yield
        finally:
            try:
                self._inspector._end_containment_hold(self, inventory)
            finally:
                self._containment_held = False

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        for descriptor in (self.executable_fd, self.pidfd):
            try:
                os.close(descriptor)
            except OSError:
                pass


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
        pidfd_send_signal: Callable[[int, int], None] = _pidfd_send_signal,
        fd_table_shared: Callable[[int, int], bool] = _fd_table_shared,
        visible_processes: Callable[[], tuple[int, ...]] | None = None,
        socket_peer_inode: Callable[[socket.socket], int] = _unix_peer_inode,
        socket_identity: Callable[[int, int], tuple[int, int, object, object, int]] = (
            _socket_identity
        ),
        progress: Callable[[], None] = lambda: None,
    ) -> None:
        self.config = config
        self.proc_root = proc_root
        self.host_root = host_root
        self.trusted_file_uid = trusted_file_uid
        self._pidfd_open = pidfd_open
        self._pidfd_alive = pidfd_alive
        self._pidfd_send_signal = pidfd_send_signal
        self._fd_table_shared = fd_table_shared
        self._visible_processes_source = visible_processes
        self._socket_peer_inode = socket_peer_inode
        self._socket_identity = socket_identity
        self._progress = progress

    def _wait_for_state(self, peer: PeerHandle, *, stopped: bool) -> None:
        deadline = time.monotonic() + _PROCESS_STATE_TIMEOUT_SECONDS
        while True:
            self._progress()
            if not self._pidfd_alive(peer.pidfd):
                raise GuardError("peer_dead")
            state, threads = _process_state(
                _read_bounded(self.proc_root / str(peer.pid) / "status"),
                peer.pid,
            )
            if threads != 1:
                raise GuardError("peer_threads_invalid")
            if self._task_inventory(peer) != (peer.pid,):
                raise GuardError("peer_threads_invalid")
            if (state == "T") == stopped and state != "t":
                return
            if time.monotonic() >= deadline:
                raise GuardError(
                    "peer_stop_failed" if stopped else "peer_resume_failed"
                )
            time.sleep(0.005)

    def _task_inventory(self, peer: PeerHandle) -> tuple[int, ...]:
        try:
            tasks = tuple(
                sorted(
                    int(entry.name)
                    for entry in (self.proc_root / str(peer.pid) / "task").iterdir()
                    if entry.name.isascii() and entry.name.isdigit()
                )
            )
        except (OSError, ValueError) as exc:
            raise GuardError("peer_threads_invalid") from exc
        if not tasks or len(tasks) != len(set(tasks)) or any(task <= 0 for task in tasks):
            raise GuardError("peer_threads_invalid")
        self._progress()
        return tasks

    def _descriptor_inventory(self, peer: PeerHandle) -> tuple[tuple[int, str], ...]:
        fd_root = self.proc_root / str(peer.pid) / "fd"
        try:
            names = tuple(sorted(fd_root.iterdir(), key=lambda item: int(item.name)))
            if not names or len(names) > 65536 or any(not item.name.isascii() or not item.name.isdigit() for item in names):
                raise GuardError("peer_fd_table_invalid")
            inventory: list[tuple[int, str]] = []
            socket_count = 0
            for entry in names:
                self._progress()
                descriptor = int(entry.name)
                target = os.readlink(entry)
                matched = _SOCKET_DESCRIPTOR.fullmatch(target)
                if target.startswith("socket:") and matched is None:
                    raise GuardError("peer_fd_table_invalid")
                if matched is not None:
                    socket_count += 1
                    family, socket_type, local, remote, inode = self._socket_identity(
                        peer.pidfd,
                        descriptor,
                    )
                    if family in {socket.AF_INET, socket.AF_INET6}:
                        raise GuardError("peer_network_socket_present")
                    if (
                        family != socket.AF_UNIX
                        or socket_type != socket.SOCK_SEQPACKET
                        or local not in {"", b""}
                        or peer.control_socket_path is None
                        or remote != peer.control_socket_path
                        or peer.control_peer_inode is None
                        or inode != peer.control_peer_inode
                    ):
                        raise GuardError("peer_unexpected_unix_socket")
                inventory.append((descriptor, target))
            if socket_count != 1:
                raise GuardError("peer_unexpected_unix_socket")
            if tuple(item.name for item in names) != tuple(
                item.name for item in sorted(fd_root.iterdir(), key=lambda item: int(item.name))
            ):
                raise GuardError("peer_fd_table_invalid")
            self._progress()
            return tuple(inventory)
        except GuardError:
            raise
        except (OSError, ValueError) as exc:
            raise GuardError("peer_fd_table_invalid") from exc

    def _visible_processes(self) -> tuple[int, ...]:
        if self._visible_processes_source is not None:
            result = self._visible_processes_source()
            if (
                type(result) is not tuple
                or not result
                or len(result) > _MAX_VISIBLE_PROCESSES
                or any(type(pid) is not int or pid <= 0 for pid in result)
                or tuple(sorted(result)) != result
                or len(result) != len(set(result))
            ):
                raise GuardError("peer_fd_table_invalid")
            self._progress()
            return result
        try:
            result = tuple(
                sorted(
                    int(entry.name)
                    for entry in self.proc_root.iterdir()
                    if entry.name.isascii() and entry.name.isdigit()
                )
            )
        except (OSError, ValueError) as exc:
            raise GuardError("peer_fd_table_invalid") from exc
        if (
            not result
            or len(result) > _MAX_VISIBLE_PROCESSES
            or any(pid <= 0 for pid in result)
            or len(result) != len(set(result))
        ):
            raise GuardError("peer_fd_table_invalid")
        self._progress()
        return result

    def _process_fd_inventory(self, pid: int) -> tuple[tuple[int, str], ...] | None:
        fd_root = self.proc_root / str(pid) / "fd"
        try:
            names = tuple(sorted(fd_root.iterdir(), key=lambda item: int(item.name)))
            if len(names) > 65536 or any(
                not item.name.isascii() or not item.name.isdigit() for item in names
            ):
                raise GuardError("peer_fd_table_invalid")
            inventory_items: list[tuple[int, str]] = []
            for index, entry in enumerate(names):
                inventory_items.append((int(entry.name), os.readlink(entry)))
                if index % 256 == 0:
                    self._progress()
            inventory = tuple(inventory_items)
            final_names = tuple(
                sorted(fd_root.iterdir(), key=lambda item: int(item.name))
            )
            if tuple(item.name for item in names) != tuple(
                item.name for item in final_names
            ):
                return None
            self._progress()
            return inventory
        except (FileNotFoundError, ProcessLookupError):
            return None
        except GuardError:
            raise
        except (OSError, ValueError) as exc:
            raise GuardError("peer_fd_table_invalid") from exc

    def _assert_unshared_fd_table(self, peer: PeerHandle) -> None:
        for _attempt in range(8):
            self._progress()
            before = self._visible_processes()
            changed = False
            for other_pid in before:
                self._progress()
                if other_pid == peer.pid:
                    continue
                try:
                    shared = self._fd_table_shared(peer.pid, other_pid)
                except OSError as exc:
                    if exc.errno == errno.ESRCH:
                        changed = True
                        break
                    raise GuardError("peer_fd_table_invalid") from None
                if shared:
                    raise GuardError("peer_fd_table_shared")
            if changed:
                continue
            expected_socket = f"socket:[{peer.control_peer_inode}]"
            holders: list[tuple[int, int]] = []
            descriptor_count = 0
            for process in before:
                self._progress()
                first = self._process_fd_inventory(process)
                second = self._process_fd_inventory(process)
                if first is None or second is None or first != second:
                    changed = True
                    break
                descriptor_count += len(first)
                if descriptor_count > _MAX_VISIBLE_DESCRIPTORS:
                    raise GuardError("peer_fd_table_invalid")
                holders.extend(
                    (process, descriptor)
                    for descriptor, target in first
                    if target == expected_socket
                )
            if changed:
                continue
            if len(holders) != 1 or holders[0][0] != peer.pid:
                raise GuardError("peer_socket_shared")
            if self._visible_processes() == before:
                return
        raise GuardError("peer_fd_table_invalid")

    def _resume_after_failed_hold(self, peer: PeerHandle) -> None:
        if not self._pidfd_alive(peer.pidfd):
            return
        try:
            self._pidfd_send_signal(peer.pidfd, signal.SIGCONT)
            self._wait_for_state(peer, stopped=False)
        except (GuardError, OSError):
            raise GuardError("peer_resume_failed") from None

    def _begin_containment_hold(self, peer: PeerHandle) -> tuple[tuple[int, str], ...]:
        if not self._pidfd_alive(peer.pidfd):
            raise GuardError("peer_dead")
        state, threads = _process_state(
            _read_bounded(self.proc_root / str(peer.pid) / "status"),
            peer.pid,
        )
        if state not in {"R", "S", "D", "I"} or threads != 1:
            raise GuardError("peer_threads_invalid")
        stopped = False
        try:
            self._pidfd_send_signal(peer.pidfd, signal.SIGSTOP)
            stopped = True
            self._wait_for_state(peer, stopped=True)
            inventory = self._descriptor_inventory(peer)
            self._assert_unshared_fd_table(peer)
            return inventory
        except (GuardError, OSError):
            if stopped:
                self._resume_after_failed_hold(peer)
            raise

    def _end_containment_hold(
        self,
        peer: PeerHandle,
        inventory: tuple[tuple[int, str], ...],
    ) -> None:
        try:
            self._wait_for_state(peer, stopped=True)
            if self._descriptor_inventory(peer) != inventory:
                raise GuardError("peer_fd_table_changed")
            self._assert_unshared_fd_table(peer)
        finally:
            self._resume_after_failed_hold(peer)

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

    def capture(
        self,
        connection: socket.socket,
        message_credentials: PeerCredentials,
    ) -> PeerHandle:
        if connection.family != socket.AF_UNIX:
            raise GuardError("peer_socket_invalid")
        try:
            credentials = connection.getsockopt(
                socket.SOL_SOCKET, socket.SO_PEERCRED, _PEER_CREDENTIALS.size
            )
        except OSError as exc:
            raise GuardError("peer_inspection_failed") from exc
        if not isinstance(credentials, bytes) or len(credentials) != _PEER_CREDENTIALS.size:
            raise GuardError("peer_credentials_invalid")
        pid, uid, gid = _PEER_CREDENTIALS.unpack(credentials)
        if message_credentials != PeerCredentials(pid, uid, gid):
            raise GuardError("peer_credentials_invalid")
        peer = self._capture_identity(
            pid,
            uid,
            gid,
            control_socket_path=None,
            control_peer_inode=None,
        )
        try:
            control_socket_path = connection.getsockname()
            if (
                not isinstance(control_socket_path, str)
                or not control_socket_path.startswith("/")
                or "\x00" in control_socket_path
                or len(os.fsencode(control_socket_path)) > 107
                or PurePosixPath(control_socket_path).as_posix() != control_socket_path
            ):
                raise GuardError("peer_socket_invalid")
            control_peer_inode = self._socket_peer_inode(connection)
            if type(control_peer_inode) is not int or control_peer_inode <= 0:
                raise GuardError("peer_socket_invalid")
            peer.control_socket_path = control_socket_path
            peer.control_peer_inode = control_peer_inode
            return peer
        except GuardError:
            peer.close()
            raise
        except OSError as exc:
            peer.close()
            raise GuardError("peer_socket_invalid") from exc
        except Exception:
            peer.close()
            raise GuardError("peer_socket_invalid") from None

    def capture_pid(self, pid: int, *, expected_uid: int, expected_gid: int) -> PeerHandle:
        """Re-establish a pidfd-backed handle from one trusted ledger identity."""

        if (
            type(pid) is not int
            or pid <= 0
            or expected_uid != self.config.uid
            or expected_gid != self.config.gid
        ):
            raise GuardError("peer_credentials_invalid")
        return self._capture_identity(
            pid,
            expected_uid,
            expected_gid,
            control_socket_path=None,
            control_peer_inode=None,
        )

    def _capture_identity(
        self,
        pid: int,
        uid: int,
        gid: int,
        *,
        control_socket_path: str | None,
        control_peer_inode: int | None,
    ) -> PeerHandle:
        pidfd: int | None = None
        executable_fd: int | None = None
        complete = False
        try:
            if pid <= 0 or uid != self.config.uid or gid != self.config.gid:
                raise GuardError("peer_credentials_invalid")
            pidfd = self._pidfd_open(pid)
            if not self._pidfd_alive(pidfd):
                raise GuardError("peer_dead")
            self._progress()
            status = _read_bounded(self.proc_root / str(pid) / "status")
            groups = _parse_status(status, self.config)
            self._progress()
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
            batch_cgroup_relative, cgroup_relative, job_id = _parse_cgroup(
                _read_bounded(self.proc_root / str(pid) / "cgroup")
            )
            self._progress()
            executable_fd, executable_path, metadata, digest = self._open_executable(pid)
            self._progress()
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
                batch_cgroup_relative=batch_cgroup_relative,
                cgroup_relative=cgroup_relative,
                job_id=job_id,
                control_socket_path=control_socket_path,
                control_peer_inode=control_peer_inode,
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
    expected = cgroup_root.joinpath(*peer.batch_cgroup_relative.parts[1:])
    resident = cgroup_root.joinpath(*peer.cgroup_relative.parts[1:])
    try:
        root = cgroup_root.resolve(strict=True)
        resolved = expected.resolve(strict=True)
        resident_resolved = resident.resolve(strict=True)
        resolved.relative_to(root)
        resident_resolved.relative_to(root)
        if (
            resolved != expected
            or resident_resolved != resident
            or not resolved.is_dir()
            or not resident_resolved.is_dir()
        ):
            raise GuardError("batch_cgroup_path_invalid")
        metadata = resolved.stat()
        cgroup_type = _decode_ascii(
            _read_bounded(resolved / "cgroup.type"), code="batch_cgroup_type_invalid"
        ).strip()
        batch_process_rows = _decode_ascii(
            _read_bounded(resolved / "cgroup.procs"), code="batch_cgroup_processes_invalid"
        ).split()
        resident_type = _decode_ascii(
            _read_bounded(resident_resolved / "cgroup.type"),
            code="batch_cgroup_type_invalid",
        ).strip()
        resident_process_rows = _decode_ascii(
            _read_bounded(resident_resolved / "cgroup.procs"),
            code="batch_cgroup_processes_invalid",
        ).split()
    except GuardError:
        raise
    except (OSError, ValueError) as exc:
        raise GuardError("batch_cgroup_path_invalid") from exc
    if cgroup_type != "domain" or resident_type != "domain":
        raise GuardError("batch_cgroup_type_invalid")
    expected_batch_processes = (
        [str(peer.pid)] if peer.cgroup_relative == peer.batch_cgroup_relative else []
    )
    if (
        batch_process_rows != expected_batch_processes
        or resident_process_rows != [str(peer.pid)]
    ):
        raise GuardError("batch_cgroup_processes_invalid")
    peer.assert_unchanged()
    final = resolved.stat()
    if (final.st_dev, final.st_ino) != (metadata.st_dev, metadata.st_ino):
        raise GuardError("batch_cgroup_changed")
    return BatchCgroup(
        resolved,
        peer.batch_cgroup_relative,
        metadata.st_ino,
        peer.pid,
    )


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
