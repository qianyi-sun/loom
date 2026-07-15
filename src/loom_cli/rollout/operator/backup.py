"""Create and pin one private, verified staging restore point."""

from __future__ import annotations

import base64
import hmac
import json
import os
import pwd
import re
import stat
import subprocess
import threading
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from functools import partial
from pathlib import Path
from typing import Any, BinaryIO, Literal, Protocol, TypeVar, cast
from uuid import uuid4

import boto3
import yaml  # type: ignore[import-untyped]
from botocore.config import Config

from loom_cli.cluster_backup_guard import (
    DEFAULT_BACKUP_MAX_AGE_HOURS,
    DEFAULT_BACKUP_MAX_DEPTH,
    DEFAULT_BACKUP_MAX_ELAPSED_SECONDS,
    DEFAULT_BACKUP_MAX_ENTRIES,
    BackupTraversalLimits,
    backup_manifest_created_at,
    backup_manifest_sha256,
    validate_backup_manifest,
    write_backup_manifest,
)
from loom_cli.cluster_config import load_cluster_config

from .config import (
    APPROVED_BACKUP_MAX_ENTRIES,
    APPROVED_BACKUP_MAX_OBJECTS,
    OperatorConfig,
)
from .model import RolloutRequest

_PRIVATE_FILE_MODE = 0o600
_PRIVATE_DIRECTORY_MODE = 0o700
_POSTGRES_DUMP_COMMAND = 'exec pg_dump -Fc -U "$POSTGRES_USER" -d "$POSTGRES_DB"'
_MINIO_BUCKETS = (
    "loom-staging-trajectories",
    "loom-staging-artifacts",
)
_RESTORE_SECRET_NAMES = (
    "loom-secrets",
    "loom-admin-secret",
    "loom-staging-tls",
)
_MINIO_LOCAL_HOST = "127.0.0.1"
_MINIO_LOCAL_PORT = 19000
_POSTGRES_MAX_BYTES = 1024**4
_BACKUP_MAX_TOTAL_BYTES = 16 * 1024**4
_MINIO_MAX_PAGES = 20_000
_BACKUP_NON_MINIO_FILE_ALLOWANCE = 4
_BACKUP_NON_MINIO_ENTRY_ALLOWANCE = 6
_MINIO_MAX_OBJECTS = APPROVED_BACKUP_MAX_OBJECTS
_MINIO_MAX_ENTRIES = APPROVED_BACKUP_MAX_ENTRIES - _BACKUP_NON_MINIO_ENTRY_ALLOWANCE
_MINIO_MAX_TOTAL_BYTES = _BACKUP_MAX_TOTAL_BYTES - _POSTGRES_MAX_BYTES
_MINIO_TOTAL_TIMEOUT_SECONDS = float(DEFAULT_BACKUP_MAX_ELAPSED_SECONDS)
_MINIO_DISK_RESERVE_BYTES = 256 * 1024**2
_MINIO_INODE_RESERVE = 1024
_PORT_FORWARD_READY_TIMEOUT_SECONDS = 15.0
_PORT_FORWARD_STOP_TIMEOUT_SECONDS = 5.0
_PORT_FORWARD_CLEANUP_WAIT_SECONDS = (2 * _PORT_FORWARD_STOP_TIMEOUT_SECONDS) + 1.0
_PORT_FORWARD_STARTUP_OUTPUT_LIMIT = 64 * 1024
_POSTGRES_TIMEOUT_SECONDS = 600.0
_KUBECTL_READ_TIMEOUT_SECONDS = 30.0
_BACKUP_MIN_REMAINING_HOURS = 2
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_BUNDLE_NAME_RE = re.compile(r"^[0-9]{8}T[0-9]{6}Z-(?P<request_id>[a-z0-9][a-z0-9-]{7,79})$")


class PortForwardHandle(Protocol):
    """Bounded lifecycle for a localhost-only kubectl port-forward."""

    def wait_ready(self, host: str, port: int, timeout_seconds: float) -> None: ...

    def terminate(self) -> None: ...

    def wait(self, timeout_seconds: float) -> bool: ...

    def kill(self) -> None: ...

    def close(self) -> None: ...


class BackupCommandRunner(Protocol):
    """Binary-safe command boundary used by the protected backup creator."""

    def stream_stdout(
        self,
        argv: Sequence[str],
        sink: BinaryIO,
        *,
        env: Mapping[str, str],
        timeout_seconds: float,
    ) -> None: ...

    def capture_stdout(
        self,
        argv: Sequence[str],
        *,
        env: Mapping[str, str],
        timeout_seconds: float,
    ) -> bytes: ...

    def start(
        self,
        argv: Sequence[str],
        *,
        env: Mapping[str, str],
    ) -> PortForwardHandle: ...


class MinioMirror(Protocol):
    """Mirror the allowlisted staging buckets using in-memory credentials."""

    def mirror(
        self,
        *,
        endpoint_url: str,
        access_key: str,
        secret_key: str,
        buckets: tuple[str, ...],
        destination: Path,
        cancel_on_timeout: Callable[[], None],
        resources: _BackupResourceBudget,
    ) -> None: ...


S3ClientFactory = Callable[..., Any]
AvailableBytes = Callable[[Path], int]
AvailableInodes = Callable[[Path], int]
DeadlineWaiter = Callable[[threading.Event, float], bool]


Clock = Callable[[], datetime]
BackupPublicReason = Literal["backup_failed", "backup_object_limit_exceeded"]
_BACKUP_PUBLIC_REASONS = frozenset({"backup_failed", "backup_object_limit_exceeded"})


def normalize_backup_public_reason(value: object) -> BackupPublicReason:
    if isinstance(value, str) and value in _BACKUP_PUBLIC_REASONS:
        return cast(BackupPublicReason, value)
    return "backup_failed"


@dataclass(frozen=True, slots=True)
class _CapacitySnapshot:
    free_bytes: int
    free_inodes: int
    block_size: int

    def __post_init__(self) -> None:
        for name, value in (
            ("free_bytes", self.free_bytes),
            ("free_inodes", self.free_inodes),
            ("block_size", self.block_size),
        ):
            if type(value) is not int or value < 0 or (name == "block_size" and value == 0):
                raise ValueError(f"{name} is invalid")


CapacityProvider = Callable[[Path], _CapacitySnapshot]


def _capacity_snapshot(path: Path) -> _CapacitySnapshot:
    values = os.statvfs(path)
    block_size = values.f_frsize or values.f_bsize
    return _CapacitySnapshot(
        free_bytes=values.f_bavail * block_size,
        free_inodes=values.f_favail,
        block_size=block_size,
    )


def _round_up(value: int, unit: int) -> int:
    return ((value + unit - 1) // unit) * unit


@dataclass(slots=True)
class _WriterAccount:
    path: Path
    component: str
    logical_size: int = 0
    allocated_charge: int = 0
    inode_charge: int = 0
    active: bool = True


class _BackupResourceBudget:
    """One live byte/inode budget shared by every component in a bundle."""

    def __init__(
        self,
        root: Path,
        *,
        max_postgres_bytes: int,
        max_total_bytes: int,
        disk_reserve_bytes: int,
        inode_reserve: int,
        capacity_provider: CapacityProvider,
        max_entries: int = DEFAULT_BACKUP_MAX_ENTRIES,
    ) -> None:
        self._root = root
        self._max_postgres_bytes = max_postgres_bytes
        self._max_total_bytes = max_total_bytes
        self._disk_reserve_bytes = disk_reserve_bytes
        self._inode_reserve = inode_reserve
        self._capacity_provider = capacity_provider
        if type(max_entries) is not int or max_entries <= 0:
            raise ValueError("max_entries must be a positive integer")
        self._max_entries = max_entries
        self._logical_total = 0
        self._postgres_total = 0
        self._allocated_total = 0
        self._inode_total = 0
        self._lock = threading.RLock()

    def _snapshot(self) -> _CapacitySnapshot:
        snapshot = self._capacity_provider(self._root)
        if not isinstance(snapshot, _CapacitySnapshot):
            raise ValueError("backup capacity provider returned invalid data")
        return snapshot

    def check_live(self, *, projected_bytes: int = 0, projected_inodes: int = 0) -> None:
        with self._lock:
            snapshot = self._snapshot()
            self._check_live_snapshot(
                snapshot,
                projected_bytes=projected_bytes,
                projected_inodes=projected_inodes,
            )

    def _check_live_snapshot(
        self,
        snapshot: _CapacitySnapshot,
        *,
        projected_bytes: int = 0,
        projected_inodes: int = 0,
    ) -> None:
        if snapshot.free_bytes - projected_bytes < self._disk_reserve_bytes:
            raise ValueError("backup filesystem free-space reserve would be crossed")
        if snapshot.free_inodes - projected_inodes < self._inode_reserve:
            raise ValueError("backup filesystem inode reserve would be crossed")

    def reserve_entry(
        self,
        path: Path,
        *,
        component: str,
        inode: bool = True,
    ) -> _WriterAccount:
        """Reserve a conservative metadata block before creating an entry."""
        with self._lock:
            snapshot = self._snapshot()
            charge = snapshot.block_size
            if self._allocated_total + charge > self._max_total_bytes:
                raise ValueError("backup exceeded allocated-byte limit")
            if inode and self._inode_total >= self._max_entries:
                raise ValueError("backup exceeded entry limit")
            self._check_live_snapshot(
                snapshot,
                projected_bytes=charge,
                projected_inodes=int(inode),
            )
            self._allocated_total += charge
            self._inode_total += int(inode)
            return _WriterAccount(
                path=path,
                component=component,
                allocated_charge=charge,
                inode_charge=int(inode),
            )

    def release_entry(self, account: _WriterAccount) -> None:
        """Release a zero-byte reservation after failed creation or confirmed removal."""
        with self._lock:
            if not account.active:
                return
            if account.logical_size:
                raise ValueError("cannot release a written backup entry")
            self._allocated_total -= account.allocated_charge
            self._inode_total -= account.inode_charge
            account.allocated_charge = 0
            account.inode_charge = 0
            account.active = False

    def prepare_write(self, account: _WriterAccount, size: int) -> None:
        if type(size) is not int or size < 0:
            raise ValueError("backup write size is invalid")
        with self._lock:
            if not account.active:
                raise ValueError("backup writer reservation is unavailable")
            new_total = self._logical_total + size
            if new_total > self._max_total_bytes:
                raise ValueError("backup exceeded total byte limit")
            if (
                account.component == "postgres"
                and self._postgres_total + size > self._max_postgres_bytes
            ):
                raise ValueError("PostgreSQL dump exceeded byte limit")
            snapshot = self._snapshot()
            projected_charge = max(
                account.allocated_charge,
                _round_up(account.logical_size + size, snapshot.block_size),
            )
            charge_delta = projected_charge - account.allocated_charge
            if self._allocated_total + charge_delta > self._max_total_bytes:
                raise ValueError("backup exceeded allocated-byte limit")
            self._check_live_snapshot(snapshot, projected_bytes=charge_delta)

    def commit_write(self, account: _WriterAccount, size: int) -> None:
        with self._lock:
            snapshot = self._snapshot()
            new_size = account.logical_size + size
            projected_charge = max(
                account.allocated_charge,
                _round_up(new_size, snapshot.block_size),
            )
            charge_delta = projected_charge - account.allocated_charge
            if self._logical_total + size > self._max_total_bytes:
                raise ValueError("backup exceeded total byte limit")
            if self._allocated_total + charge_delta > self._max_total_bytes:
                raise ValueError("backup exceeded allocated-byte limit")
            account.logical_size = new_size
            account.allocated_charge = projected_charge
            self._logical_total += size
            self._allocated_total += charge_delta
            if account.component == "postgres":
                self._postgres_total += size
            self._check_live_snapshot(snapshot)

    def reconcile_writer(self, account: _WriterAccount) -> None:
        with self._lock:
            snapshot = self._snapshot()
            metadata = account.path.stat(follow_symlinks=False)
            actual_charge = max(
                account.allocated_charge,
                _round_up(metadata.st_size, snapshot.block_size),
                metadata.st_blocks * 512,
            )
            charge_delta = actual_charge - account.allocated_charge
            if self._allocated_total + charge_delta > self._max_total_bytes:
                raise ValueError("backup exceeded allocated-byte limit")
            account.allocated_charge = actual_charge
            self._allocated_total += charge_delta
            self._check_live_snapshot(snapshot)


class _BudgetedWriter:
    def __init__(
        self,
        sink: BinaryIO,
        *,
        path: Path,
        resources: _BackupResourceBudget,
        component: str,
        account: _WriterAccount | None = None,
    ) -> None:
        self._sink = sink
        self._resources = resources
        self._account = account or resources.reserve_entry(path, component=component)

    def write(self, payload: bytes) -> int:
        self._resources.prepare_write(self._account, len(payload))
        written = self._sink.write(payload)
        if written is None:
            written = len(payload)
        if written < 0 or written > len(payload):
            raise OSError("backup sink returned an invalid write size")
        self._resources.commit_write(self._account, written)
        return written

    def flush(self) -> None:
        self._sink.flush()
        self._resources.reconcile_writer(self._account)

    def fileno(self) -> int:
        return self._sink.fileno()


class BackupError(RuntimeError):
    """Safe stage-coded backup failure with no captured secret-bearing cause."""

    def __init__(
        self,
        code: str,
        *,
        public_reason: BackupPublicReason = "backup_failed",
    ) -> None:
        if public_reason not in _BACKUP_PUBLIC_REASONS:
            raise ValueError("backup public reason is not approved")
        super().__init__(code)
        self.code = code
        self.public_reason = public_reason


class BackupPolicyLimitError(BackupError, ValueError):
    """A stable, secret-safe backup policy exhaustion signal."""

    def __init__(
        self,
        code: str,
        *,
        public_reason: BackupPublicReason,
        message: str,
    ) -> None:
        super().__init__(code, public_reason=public_reason)
        self.args = (message,)


class _LatestPublicationError(RuntimeError):
    def __init__(self, *, rollback_confirmed: bool) -> None:
        super().__init__("latest publication failed")
        self.rollback_confirmed = rollback_confirmed


class _LatestStageError(BackupError):
    def __init__(self, *, rollback_confirmed: bool) -> None:
        super().__init__("latest_publish_failed")
        self.rollback_confirmed = rollback_confirmed


@dataclass(slots=True)
class _LatestRollback:
    name: str
    account: _WriterAccount
    exists: bool = True


class _OnceCloser:
    """Serialize one resource close across watchdog and normal-finally races."""

    def __init__(
        self,
        close: Callable[[], None],
        *,
        wait_timeout_seconds: float = 5.0,
    ) -> None:
        self._close = close
        self._wait_timeout_seconds = wait_timeout_seconds
        self._lock = threading.Lock()
        self._claimed = False
        self._done = threading.Event()
        self._failed = False

    def __call__(self) -> None:
        owner = False
        with self._lock:
            if self._claimed:
                done = self._done
            else:
                self._claimed = True
                owner = True
                done = self._done
        if not owner:
            if not done.wait(self._wait_timeout_seconds):
                raise RuntimeError("resource cleanup exceeded wait bound")
            if self.failed:
                raise RuntimeError("resource cleanup failed")
            return
        try:
            self._close()
        except BaseException:
            with self._lock:
                self._failed = True
            raise
        finally:
            self._done.set()

    @property
    def failed(self) -> bool:
        with self._lock:
            return self._done.is_set() and self._failed


def _wait_for_stop(stop: threading.Event, timeout_seconds: float) -> bool:
    return stop.wait(timeout_seconds)


def _do_nothing() -> None:
    return None


class _MirrorCancellationScope:
    """Real-time watchdog for synchronous MinIO I/O and exact PF cleanup."""

    def __init__(
        self,
        *,
        deadline: float,
        monotonic: Callable[[], float],
        waiter: DeadlineWaiter,
        cancellation_grace_seconds: float,
        cancel_external: _OnceCloser,
    ) -> None:
        self._deadline = deadline
        self._monotonic = monotonic
        self._waiter = waiter
        self._cancellation_grace_seconds = cancellation_grace_seconds
        self._cancel_external = cancel_external
        self._stop = threading.Event()
        self._expired = threading.Event()
        self._state_lock = threading.Lock()
        self._client: _OnceCloser | None = None
        self._body: _OnceCloser | None = None
        self._thread = threading.Thread(target=self._watch, daemon=True)

    @staticmethod
    def _close_safely(close: Callable[[], None] | None) -> None:
        if close is None:
            return
        try:
            close()
        except BaseException:
            pass

    def bind_client(self, close: _OnceCloser) -> None:
        with self._state_lock:
            self._client = close
            expired = self._expired.is_set()
        if expired:
            self._close_safely(close)

    def bind_body(self, close: _OnceCloser) -> None:
        with self._state_lock:
            self._body = close
            expired = self._expired.is_set()
        if expired:
            self._close_safely(close)
            self.raise_if_expired()

    def release_body(self, close: _OnceCloser) -> None:
        with self._state_lock:
            if self._body is close:
                self._body = None
        close()

    def start(self) -> None:
        self._thread.start()

    def _watch(self) -> None:
        try:
            remaining_seconds = self._deadline - self._monotonic()
            stopped = (
                self._stop.is_set()
                if remaining_seconds <= 0
                else self._waiter(self._stop, remaining_seconds)
            )
        except BaseException:
            stopped = False
        if stopped:
            return
        self._expired.set()
        with self._state_lock:
            body = self._body
            client = self._client
        # Ordering is deliberate: release the active response before its pool,
        # then tear down the exact tunnel that owns any blocked socket.
        self._close_safely(body)
        self._close_safely(client)
        self._close_safely(self._cancel_external)

    def raise_if_expired(self) -> None:
        if self._expired.is_set():
            raise ValueError("MinIO mirror exceeded total deadline")

    def finish(self) -> bool:
        self._stop.set()
        self._thread.join(timeout=self._cancellation_grace_seconds)
        if self._thread.is_alive():
            self._expired.set()
            raise RuntimeError("MinIO deadline cleanup exceeded grace period")
        if any(
            close is not None and close.failed
            for close in (self._body, self._client, self._cancel_external)
        ):
            raise RuntimeError("MinIO deadline cleanup failed")
        return self._expired.is_set()


@dataclass(frozen=True, slots=True)
class VerifiedBackup:
    """The exact immutable manifest selected for every request attempt."""

    manifest_path: Path
    manifest_sha256: str

    def __post_init__(self) -> None:
        if not self.manifest_path.is_absolute() or ".." in self.manifest_path.parts:
            raise ValueError("manifest_path must be an absolute protected path")
        if _SHA256_RE.fullmatch(self.manifest_sha256) is None:
            raise ValueError("manifest_sha256 must be 64 lowercase hexadecimal characters")


_T = TypeVar("_T")


def _stage(code: str, operation: Callable[[], _T]) -> _T:
    """Run a stage and discard any secret-bearing exception before raising."""
    try:
        result = operation()
    except BackupError:
        raise
    except Exception:
        pass
    else:
        return result
    raise BackupError(code)


def _command_environment(config: OperatorConfig) -> dict[str, str]:
    return {
        "HOME": str(config.state_root),
        "USER": config.service_user,
        "LOGNAME": config.service_user,
        "PATH": "/usr/local/bin:/usr/bin:/bin",
        "KUBECONFIG": str(config.kubeconfig_path),
        "LC_ALL": "C.UTF-8",
    }


def _clock_utc(clock: Clock) -> datetime:
    value = clock()
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("backup clock must return a timezone-aware datetime")
    return value.astimezone(UTC)


def _require_launch_freshness(*, created_at: datetime, now: datetime) -> None:
    age = now - created_at
    if age.total_seconds() < 0:
        raise ValueError("backup creation time is in the future")
    remaining = timedelta(hours=DEFAULT_BACKUP_MAX_AGE_HOURS) - age
    if remaining < timedelta(hours=_BACKUP_MIN_REMAINING_HOURS):
        raise ValueError("backup expires before the required launch window")


def _private_directory(path: Path, *, resources: _BackupResourceBudget) -> None:
    account = resources.reserve_entry(path, component="directory")
    try:
        path.mkdir(mode=_PRIVATE_DIRECTORY_MODE)
    except BaseException:
        resources.release_entry(account)
        raise
    path.chmod(_PRIVATE_DIRECTORY_MODE)
    resources.reconcile_writer(account)


def _directory_open_flags() -> int:
    return (
        os.O_RDONLY
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_DIRECTORY", 0)
        | getattr(os, "O_NOFOLLOW", 0)
    )


def _open_directory_no_follow(path: Path) -> int:
    if not path.is_absolute() or ".." in path.parts or len(path.parts) < 2:
        raise ValueError("directory is not an absolute protected path")
    current_fd = os.open("/", _directory_open_flags())
    try:
        for part in path.parts[1:]:
            next_fd = os.open(part, _directory_open_flags(), dir_fd=current_fd)
            os.close(current_fd)
            current_fd = next_fd
        result_fd = current_fd
        current_fd = -1
        return result_fd
    finally:
        if current_fd >= 0:
            os.close(current_fd)


def _existing_backups_directory_is_approved(
    metadata: os.stat_result,
    *,
    service_uid: int,
) -> bool:
    mode = stat.S_IMODE(metadata.st_mode)
    return (metadata.st_uid == service_uid and mode == _PRIVATE_DIRECTORY_MODE) or (
        metadata.st_uid != service_uid and mode == 0o770
    )


def _create_bundle_root(
    rollout_root: Path,
    bundle_name: str,
    *,
    service_uid: int,
    resources: _BackupResourceBudget,
) -> None:
    rollout_fd = _open_directory_no_follow(rollout_root)
    backups_fd: int | None = None
    try:
        backups_created = False
        backups_path = rollout_root / "backups"
        backups_account = resources.reserve_entry(backups_path, component="directory")
        try:
            os.mkdir("backups", _PRIVATE_DIRECTORY_MODE, dir_fd=rollout_fd)
            backups_created = True
        except FileExistsError:
            resources.release_entry(backups_account)
        except BaseException:
            resources.release_entry(backups_account)
            raise
        if backups_created:
            os.chmod(
                "backups",
                _PRIVATE_DIRECTORY_MODE,
                dir_fd=rollout_fd,
                follow_symlinks=False,
            )
        backups_fd = os.open("backups", _directory_open_flags(), dir_fd=rollout_fd)
        if backups_created:
            os.fsync(rollout_fd)
            resources.reconcile_writer(backups_account)
        if backups_fd is None:
            raise ValueError("backups directory is unavailable")
        backups_metadata = os.fstat(backups_fd)
        backups_mode = stat.S_IMODE(backups_metadata.st_mode)
        if backups_created:
            if backups_metadata.st_uid != service_uid:
                raise ValueError("backups owner UID does not match service account")
            if backups_mode != _PRIVATE_DIRECTORY_MODE:
                raise ValueError("backups mode must be 0700")
        elif not _existing_backups_directory_is_approved(
            backups_metadata,
            service_uid=service_uid,
        ):
            raise ValueError("existing backups directory metadata is not approved")
        bundle_path = backups_path / bundle_name
        bundle_account = resources.reserve_entry(bundle_path, component="directory")
        try:
            os.mkdir(bundle_name, _PRIVATE_DIRECTORY_MODE, dir_fd=backups_fd)
        except BaseException:
            resources.release_entry(bundle_account)
            raise
        os.chmod(
            bundle_name,
            _PRIVATE_DIRECTORY_MODE,
            dir_fd=backups_fd,
            follow_symlinks=False,
        )
        bundle_fd = os.open(bundle_name, _directory_open_flags(), dir_fd=backups_fd)
        try:
            os.fchmod(bundle_fd, _PRIVATE_DIRECTORY_MODE)
            metadata = os.fstat(bundle_fd)
            if metadata.st_uid != service_uid:
                raise ValueError("bundle owner UID does not match service account")
            if stat.S_IMODE(metadata.st_mode) != _PRIVATE_DIRECTORY_MODE:
                raise ValueError("bundle mode must be 0700")
        finally:
            os.close(bundle_fd)
        os.fsync(backups_fd)
        resources.reconcile_writer(bundle_account)
    finally:
        if backups_fd is not None:
            os.close(backups_fd)
        os.close(rollout_fd)


def _fsync_directory(path: Path) -> None:
    fd = _open_directory_no_follow(path)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _fsync_private_file(path: Path) -> None:
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    fd = os.open(path, flags)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _fsync_private_tree(root: Path) -> None:
    def fsync_directory_fd(directory_fd: int) -> None:
        for name in os.listdir(directory_fd):
            metadata = os.stat(name, dir_fd=directory_fd, follow_symlinks=False)
            if stat.S_ISDIR(metadata.st_mode):
                child_fd = os.open(name, _directory_open_flags(), dir_fd=directory_fd)
                try:
                    fsync_directory_fd(child_fd)
                finally:
                    os.close(child_fd)
                continue
            if not stat.S_ISREG(metadata.st_mode):
                raise ValueError("backup component tree contains a non-regular entry")
            file_fd = os.open(
                name,
                os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0),
                dir_fd=directory_fd,
            )
            try:
                if not stat.S_ISREG(os.fstat(file_fd).st_mode):
                    raise ValueError("backup component tree contains a non-regular entry")
                os.fsync(file_fd)
            finally:
                os.close(file_fd)
        os.fsync(directory_fd)

    root_fd = _open_directory_no_follow(root)
    try:
        fsync_directory_fd(root_fd)
    finally:
        os.close(root_fd)


def _write_private_bytes(
    path: Path,
    payload: bytes,
    *,
    resources: _BackupResourceBudget,
    component: str,
) -> None:
    flags = (
        os.O_WRONLY
        | os.O_CREAT
        | os.O_EXCL
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_NOFOLLOW", 0)
    )
    account = resources.reserve_entry(path, component=component)
    try:
        fd = os.open(path, flags, _PRIVATE_FILE_MODE)
    except BaseException:
        resources.release_entry(account)
        raise
    try:
        os.fchmod(fd, _PRIVATE_FILE_MODE)
        with os.fdopen(fd, "wb", closefd=False) as sink:
            guarded_sink = _BudgetedWriter(
                sink,
                path=path,
                resources=resources,
                component=component,
                account=account,
            )
            written = guarded_sink.write(payload)
            if written != len(payload):
                raise OSError("private backup write was incomplete")
            guarded_sink.flush()
            os.fsync(sink.fileno())
            resources.reconcile_writer(account)
    finally:
        os.close(fd)


def _read_previous_latest_target(directory_fd: int) -> str | None:
    try:
        latest_metadata = os.stat(
            "latest",
            dir_fd=directory_fd,
            follow_symlinks=False,
        )
    except FileNotFoundError:
        return None
    if not stat.S_ISLNK(latest_metadata.st_mode):
        raise ValueError("latest must be a relative snapshot symlink")
    target = os.readlink("latest", dir_fd=directory_fd)
    if (
        not target
        or os.path.isabs(target)
        or os.path.basename(target) != target
        or target in {".", ".."}
    ):
        raise ValueError("latest must be a relative snapshot symlink")
    target_metadata = os.stat(
        target,
        dir_fd=directory_fd,
        follow_symlinks=False,
    )
    if not stat.S_ISDIR(target_metadata.st_mode):
        raise ValueError("latest target must be a snapshot directory")
    return target


def _restore_latest(
    directory_fd: int,
    previous_target: str | None,
    *,
    rollback: _LatestRollback | None,
) -> None:
    if previous_target is None:
        os.unlink("latest", dir_fd=directory_fd)
        os.fsync(directory_fd)
        return
    if rollback is None or not rollback.exists:
        raise ValueError("latest rollback reservation is unavailable")
    os.replace(
        rollback.name,
        "latest",
        src_dir_fd=directory_fd,
        dst_dir_fd=directory_fd,
    )
    rollback.exists = False
    os.fsync(directory_fd)


def _discard_latest_rollback(
    directory_fd: int,
    rollback: _LatestRollback,
    *,
    resources: _BackupResourceBudget,
) -> None:
    os.unlink(rollback.name, dir_fd=directory_fd)
    rollback.exists = False
    resources.release_entry(rollback.account)
    os.fsync(directory_fd)
    resources.check_live()


def _latest_matches(directory_fd: int, target: str | None) -> bool:
    try:
        metadata = os.stat(
            "latest",
            dir_fd=directory_fd,
            follow_symlinks=False,
        )
    except FileNotFoundError:
        return target is None
    if target is None or not stat.S_ISLNK(metadata.st_mode):
        return False
    return os.readlink("latest", dir_fd=directory_fd) == target


def _recover_ambiguous_latest_replace(
    directory_fd: int,
    *,
    previous_target: str | None,
    new_target: str,
    rollback: _LatestRollback | None,
) -> bool:
    try:
        if _latest_matches(directory_fd, previous_target):
            return True
        if not _latest_matches(directory_fd, new_target):
            return False
        _restore_latest(
            directory_fd,
            previous_target,
            rollback=rollback,
        )
        return _latest_matches(directory_fd, previous_target)
    except Exception:
        return False


def _publish_latest(
    bundle_root: Path,
    *,
    resources: _BackupResourceBudget,
) -> None:
    try:
        directory_fd = _open_directory_no_follow(bundle_root.parent)
    except Exception:
        raise _LatestPublicationError(rollback_confirmed=True) from None
    temp_name = f".latest.{uuid4().hex}.tmp"
    temp_exists = False
    rollback: _LatestRollback | None = None
    try:
        try:
            previous_target = _read_previous_latest_target(directory_fd)
            if previous_target is not None:
                rollback_name = f".latest.{uuid4().hex}.rollback"
                rollback_account = resources.reserve_entry(
                    bundle_root.parent / rollback_name,
                    component="publication-rollback",
                )
                try:
                    os.symlink(previous_target, rollback_name, dir_fd=directory_fd)
                except BaseException:
                    resources.release_entry(rollback_account)
                    raise
                rollback = _LatestRollback(rollback_name, rollback_account)
                resources.reconcile_writer(rollback_account)
                os.fsync(directory_fd)
            latest_account = resources.reserve_entry(
                bundle_root.parent / temp_name,
                component="publication",
            )
            try:
                os.symlink(bundle_root.name, temp_name, dir_fd=directory_fd)
            except BaseException:
                resources.release_entry(latest_account)
                raise
            temp_exists = True
        except Exception:
            raise _LatestPublicationError(rollback_confirmed=True) from None
        try:
            os.replace(
                temp_name,
                "latest",
                src_dir_fd=directory_fd,
                dst_dir_fd=directory_fd,
            )
        except Exception:
            rollback_confirmed = _recover_ambiguous_latest_replace(
                directory_fd,
                previous_target=previous_target,
                new_target=bundle_root.name,
                rollback=rollback,
            )
            raise _LatestPublicationError(rollback_confirmed=rollback_confirmed) from None
        temp_exists = False
        try:
            os.fsync(directory_fd)
            resources.check_live()
        except Exception:
            try:
                _restore_latest(
                    directory_fd,
                    previous_target,
                    rollback=rollback,
                )
                if not _latest_matches(directory_fd, previous_target):
                    raise OSError("latest rollback verification failed")
            except Exception:
                raise _LatestPublicationError(rollback_confirmed=False) from None
            raise _LatestPublicationError(rollback_confirmed=True) from None
        if rollback is not None:
            try:
                _discard_latest_rollback(
                    directory_fd,
                    rollback,
                    resources=resources,
                )
            except Exception:
                raise _LatestPublicationError(rollback_confirmed=False) from None
    finally:
        if rollback is not None and rollback.exists:
            try:
                os.unlink(rollback.name, dir_fd=directory_fd)
            except OSError:
                pass
            else:
                rollback.exists = False
                resources.release_entry(rollback.account)
                try:
                    os.fsync(directory_fd)
                except OSError:
                    pass
        if temp_exists:
            try:
                os.unlink(temp_name, dir_fd=directory_fd)
            except OSError:
                pass
        try:
            os.close(directory_fd)
        except OSError:
            pass


def _publish_latest_stage(
    bundle_root: Path,
    *,
    resources: _BackupResourceBudget,
) -> None:
    try:
        _publish_latest(bundle_root, resources=resources)
    except _LatestPublicationError as exc:
        rollback_confirmed = exc.rollback_confirmed
    else:
        return
    raise _LatestStageError(rollback_confirmed=rollback_confirmed)


def _publish_manifest(
    pending_path: Path,
    manifest_path: Path,
    *,
    resources: _BackupResourceBudget,
) -> None:
    directory_fd = _open_directory_no_follow(pending_path.parent)
    try:
        link_account = resources.reserve_entry(
            manifest_path,
            component="manifest-metadata",
            inode=False,
        )
        try:
            os.link(
                pending_path.name,
                manifest_path.name,
                src_dir_fd=directory_fd,
                dst_dir_fd=directory_fd,
                follow_symlinks=False,
            )
        except BaseException:
            resources.release_entry(link_account)
            raise
        os.fsync(directory_fd)
        os.unlink(pending_path.name, dir_fd=directory_fd)
        os.fsync(directory_fd)
        resources.check_live()
    finally:
        os.close(directory_fd)


def _remove_failed_manifests(*paths: Path) -> None:
    parent: Path | None = None
    for path in paths:
        parent = path.parent
        try:
            path.unlink(missing_ok=True)
        except OSError:
            pass
    if parent is not None:
        try:
            _fsync_directory(parent)
        except OSError:
            pass


def _require_private_directory(path: Path) -> None:
    try:
        metadata = path.lstat()
    except OSError:
        raise ValueError("private snapshot directory is unavailable") from None
    if not stat.S_ISDIR(metadata.st_mode) or stat.S_ISLNK(metadata.st_mode):
        raise ValueError("private snapshot path must be a directory")
    if metadata.st_uid != os.geteuid():
        raise ValueError("private snapshot directory owner does not match")
    if stat.S_IMODE(metadata.st_mode) != _PRIVATE_DIRECTORY_MODE:
        raise ValueError("private snapshot directory mode must be 0700")


@dataclass(slots=True)
class _CleanupTraversal:
    limits: BackupTraversalLimits
    deadline: float
    root_device: int
    files: int = 0
    entries: int = 0
    total_bytes: int = 0

    def fresh_pass(self) -> _CleanupTraversal:
        return _CleanupTraversal(
            limits=self.limits,
            deadline=self.deadline,
            root_device=self.root_device,
        )

    def check_deadline(self) -> None:
        if self.limits.monotonic() >= self.deadline:
            raise ValueError("backup cleanup exceeded elapsed-time limit")

    def check(self, metadata: os.stat_result, *, depth: int) -> None:
        self.check_deadline()
        if metadata.st_dev != self.root_device:
            raise ValueError("backup cleanup crossed a filesystem boundary")
        if depth > self.limits.max_depth:
            raise ValueError("backup cleanup exceeded depth limit")
        self.entries += 1
        if self.entries > self.limits.max_entries:
            raise ValueError("backup cleanup exceeded entry limit")
        if stat.S_ISREG(metadata.st_mode):
            self.files += 1
            self.total_bytes += metadata.st_size
            if self.files > self.limits.max_files:
                raise ValueError("backup cleanup exceeded file limit")
            if self.total_bytes > self.limits.max_total_bytes:
                raise ValueError("backup cleanup exceeded byte limit")


def _require_cleanup_entry(metadata: os.stat_result, *, service_uid: int) -> None:
    if metadata.st_uid != service_uid:
        raise ValueError("backup cleanup entry owner does not match service account")
    mode = stat.S_IMODE(metadata.st_mode)
    if stat.S_ISDIR(metadata.st_mode):
        if mode != _PRIVATE_DIRECTORY_MODE:
            raise ValueError("backup cleanup directory mode must be 0700")
        return
    if stat.S_ISREG(metadata.st_mode):
        if mode != _PRIVATE_FILE_MODE or metadata.st_nlink != 1:
            raise ValueError("backup cleanup file metadata is unsafe")
        return
    raise ValueError("backup cleanup tree contains a non-regular entry")


def _validate_cleanup_directory(
    directory_fd: int,
    *,
    service_uid: int,
    budget: _CleanupTraversal,
    depth: int,
) -> None:
    directory_metadata = os.fstat(directory_fd)
    _require_cleanup_entry(directory_metadata, service_uid=service_uid)
    if directory_metadata.st_dev != budget.root_device:
        raise ValueError("backup cleanup crossed a filesystem boundary")
    names: list[str] = []
    with os.scandir(directory_fd) as entries:
        for entry in entries:
            budget.check_deadline()
            names.append(entry.name)
            if len(names) > budget.limits.max_directory_entries:
                raise ValueError("backup cleanup exceeded directory entry limit")
    for name in names:
        metadata = os.stat(name, dir_fd=directory_fd, follow_symlinks=False)
        _require_cleanup_entry(metadata, service_uid=service_uid)
        budget.check(metadata, depth=depth)
        if not stat.S_ISDIR(metadata.st_mode):
            continue
        child_fd = os.open(name, _directory_open_flags(), dir_fd=directory_fd)
        try:
            opened = os.fstat(child_fd)
            if (opened.st_dev, opened.st_ino) != (metadata.st_dev, metadata.st_ino):
                raise ValueError("backup cleanup tree changed during validation")
            _validate_cleanup_directory(
                child_fd,
                service_uid=service_uid,
                budget=budget,
                depth=depth + 1,
            )
        finally:
            os.close(child_fd)


def _remove_cleanup_directory(
    directory_fd: int,
    *,
    service_uid: int,
    budget: _CleanupTraversal,
    depth: int,
) -> None:
    directory_metadata = os.fstat(directory_fd)
    _require_cleanup_entry(directory_metadata, service_uid=service_uid)
    if directory_metadata.st_dev != budget.root_device:
        raise ValueError("backup cleanup crossed a filesystem boundary")
    names: list[str] = []
    with os.scandir(directory_fd) as entries:
        for entry in entries:
            budget.check_deadline()
            names.append(entry.name)
            if len(names) > budget.limits.max_directory_entries:
                raise ValueError("backup cleanup exceeded directory entry limit")
    if depth == 1 and "backup-manifest.json" in names:
        raise ValueError("manifest-backed backup cannot be cleaned")
    for name in names:
        metadata = os.stat(name, dir_fd=directory_fd, follow_symlinks=False)
        _require_cleanup_entry(metadata, service_uid=service_uid)
        budget.check(metadata, depth=depth)
        if stat.S_ISDIR(metadata.st_mode):
            budget.check_deadline()
            child_fd = os.open(name, _directory_open_flags(), dir_fd=directory_fd)
            try:
                opened = os.fstat(child_fd)
                if (opened.st_dev, opened.st_ino) != (metadata.st_dev, metadata.st_ino):
                    raise ValueError("backup cleanup tree changed during removal")
                _remove_cleanup_directory(
                    child_fd,
                    service_uid=service_uid,
                    budget=budget,
                    depth=depth + 1,
                )
            finally:
                os.close(child_fd)
            budget.check_deadline()
            os.rmdir(name, dir_fd=directory_fd)
            budget.check_deadline()
        else:
            budget.check_deadline()
            os.unlink(name, dir_fd=directory_fd)
            budget.check_deadline()
    budget.check_deadline()
    os.fsync(directory_fd)
    budget.check_deadline()


def _ensure_private_directory(
    path: Path,
    *,
    resources: _BackupResourceBudget,
) -> None:
    try:
        path.lstat()
    except FileNotFoundError:
        pass
    else:
        _require_private_directory(path)
        resources.check_live()
        return
    account = resources.reserve_entry(path, component="directory")
    try:
        path.mkdir(mode=_PRIVATE_DIRECTORY_MODE)
    except FileExistsError:
        resources.release_entry(account)
        _require_private_directory(path)
        return
    except BaseException:
        resources.release_entry(account)
        raise
    path.chmod(_PRIVATE_DIRECTORY_MODE)
    _require_private_directory(path)
    resources.reconcile_writer(account)


def _safe_object_parts(key: object) -> tuple[str, ...]:
    if not isinstance(key, str) or not key or key.startswith("/") or "\\" in key:
        raise ValueError("object key is not a safe relative path")
    parts = tuple(key.split("/"))
    if any(
        not part
        or part in {".", ".."}
        or "\x00" in part
        or any(ord(character) < 32 for character in part)
        for part in parts
    ):
        raise ValueError("object key is not a safe relative path")
    if len(parts) > DEFAULT_BACKUP_MAX_DEPTH - 1:
        raise ValueError("object key exceeds backup traversal depth")
    return parts


@dataclass(slots=True)
class _MirrorBudget:
    max_pages: int
    max_objects: int
    max_total_bytes: int
    max_entries: int
    deadline: float
    monotonic: Callable[[], float]
    pages: int = 0
    objects: int = 0
    total_bytes: int = 0
    entries: int = 0

    def check_deadline(self) -> None:
        if self.monotonic() >= self.deadline:
            raise ValueError("MinIO mirror exceeded total deadline")

    def consume_page(self) -> None:
        self.check_deadline()
        if self.pages >= self.max_pages:
            raise ValueError("MinIO mirror exceeded page limit")
        self.pages += 1

    def consume_object(self) -> None:
        self.check_deadline()
        if self.objects >= self.max_objects:
            raise BackupPolicyLimitError(
                "minio_object_limit_exceeded",
                public_reason="backup_object_limit_exceeded",
                message="MinIO mirror exceeded object limit",
            )
        self.objects += 1

    def reserve_bytes(self, size: int) -> None:
        self.check_deadline()
        if size > self.max_total_bytes - self.total_bytes:
            raise ValueError("MinIO mirror exceeded byte limit")
        self.total_bytes += size

    def reserve_entries(self, count: int) -> None:
        self.check_deadline()
        if count > self.max_entries - self.entries:
            raise ValueError("MinIO mirror exceeded inode limit")
        self.entries += count


def _available_bytes(path: Path) -> int:
    return _capacity_snapshot(path).free_bytes


def _available_inodes(path: Path) -> int:
    return _capacity_snapshot(path).free_inodes


def _stream_s3_object(
    client: Any,
    *,
    bucket: str,
    key: str,
    destination: Path,
    budget: _MirrorBudget,
    cancellation: _MirrorCancellationScope,
    resources: _BackupResourceBudget,
) -> None:
    budget.check_deadline()
    response = client.get_object(Bucket=bucket, Key=key)
    body: object | None = None
    read: object | None = None
    body_closer: _OnceCloser | None = None
    if isinstance(response, dict):
        body = response.get("Body")
        close = getattr(body, "close", None)
        if callable(close):
            body_closer = _OnceCloser(close)
            cancellation.bind_body(body_closer)

    fd: int | None = None
    temp_exists = False
    temp_account: _WriterAccount | None = None
    temp_path: Path | None = None
    try:
        budget.check_deadline()
        cancellation.raise_if_expired()
        if not isinstance(response, dict):
            raise ValueError("object response is malformed")
        read = getattr(body, "read", None)
        expected_size = response.get("ContentLength")
        if type(expected_size) is not int or expected_size < 0:
            raise ValueError("object content length is malformed")
        if not callable(read) or body_closer is None:
            raise ValueError("object body is malformed")
        budget.reserve_bytes(expected_size)
        temp_path = destination.parent / f".{destination.name}.{uuid4().hex}.part"
        flags = (
            os.O_WRONLY
            | os.O_CREAT
            | os.O_EXCL
            | getattr(os, "O_CLOEXEC", 0)
            | getattr(os, "O_NOFOLLOW", 0)
        )
        temp_account = resources.reserve_entry(temp_path, component="minio")
        resources.prepare_write(temp_account, expected_size)
        try:
            fd = os.open(temp_path, flags, _PRIVATE_FILE_MODE)
        except BaseException:
            resources.release_entry(temp_account)
            raise
        temp_exists = True
        os.fchmod(fd, _PRIVATE_FILE_MODE)
        total_size = 0
        with os.fdopen(fd, "wb", closefd=True) as sink:
            fd = None
            guarded_sink = _BudgetedWriter(
                sink,
                path=temp_path,
                resources=resources,
                component="minio",
                account=temp_account,
            )
            while True:
                budget.check_deadline()
                chunk = read(1024 * 1024)
                budget.check_deadline()
                cancellation.raise_if_expired()
                if not isinstance(chunk, bytes):
                    raise ValueError("object body returned non-bytes data")
                if not chunk:
                    break
                if len(chunk) > expected_size - total_size:
                    raise ValueError("object body length does not match")
                written = guarded_sink.write(chunk)
                if written != len(chunk):
                    raise OSError("object snapshot write was incomplete")
                total_size += written
            if total_size != expected_size:
                raise ValueError("object body length does not match")
            guarded_sink.flush()
            os.fsync(sink.fileno())
            resources.reconcile_writer(temp_account)
        cancellation.raise_if_expired()
        link_account = resources.reserve_entry(
            destination,
            component="minio-metadata",
            inode=False,
        )
        try:
            os.link(temp_path, destination, follow_symlinks=False)
        except BaseException:
            resources.release_entry(link_account)
            raise
        temp_path.unlink()
        temp_exists = False
        _fsync_directory(destination.parent)
        resources.check_live()
    finally:
        if fd is not None:
            os.close(fd)
        try:
            if body_closer is not None:
                cancellation.release_body(body_closer)
        finally:
            if temp_exists and temp_path is not None:
                try:
                    temp_path.unlink()
                except OSError:
                    pass


def _mirror_s3_bucket(
    client: Any,
    *,
    bucket: str,
    destination: Path,
    budget: _MirrorBudget,
    cancellation: _MirrorCancellationScope,
    resources: _BackupResourceBudget,
) -> None:
    bucket_root = destination / bucket
    budget.reserve_entries(1)
    _ensure_private_directory(bucket_root, resources=resources)
    token: str | None = None
    seen_tokens: set[str] = set()
    while True:
        budget.consume_page()
        kwargs: dict[str, str] = {"Bucket": bucket}
        if token is not None:
            kwargs["ContinuationToken"] = token
        page = client.list_objects_v2(**kwargs)
        budget.check_deadline()
        cancellation.raise_if_expired()
        if not isinstance(page, dict):
            raise ValueError("object listing response is malformed")
        contents = page.get("Contents", [])
        if not isinstance(contents, list):
            raise ValueError("object listing contents are malformed")
        if len(contents) > 1000:
            raise ValueError("object listing page exceeds protocol limit")
        for entry in contents:
            if not isinstance(entry, dict):
                raise ValueError("object listing entry is malformed")
            key = entry.get("Key")
            parts = _safe_object_parts(key)
            budget.consume_object()
            # Conservatively charge every component. Shared directory prefixes may
            # be counted more than once, but no object can exceed the inode budget.
            budget.reserve_entries(len(parts))
            parent = bucket_root
            for part in parts[:-1]:
                parent = parent / part
                _ensure_private_directory(parent, resources=resources)
            _stream_s3_object(
                client,
                bucket=bucket,
                key=cast(str, key),
                destination=parent / parts[-1],
                budget=budget,
                cancellation=cancellation,
                resources=resources,
            )
        truncated = page.get("IsTruncated", False)
        if type(truncated) is not bool:
            raise ValueError("object listing truncation state is malformed")
        if not truncated:
            break
        next_token = page.get("NextContinuationToken")
        if not isinstance(next_token, str) or not next_token or next_token in seen_tokens:
            raise ValueError("object listing continuation token is malformed")
        seen_tokens.add(next_token)
        token = next_token


class Boto3MinioMirror:
    """Bounded, paginated S3 mirror with private no-partial publication."""

    def __init__(
        self,
        *,
        client_factory: S3ClientFactory = boto3.client,
        max_pages: int = _MINIO_MAX_PAGES,
        max_objects: int = _MINIO_MAX_OBJECTS,
        max_entries: int = _MINIO_MAX_ENTRIES,
        max_total_bytes: int = _MINIO_MAX_TOTAL_BYTES,
        timeout_seconds: float = _MINIO_TOTAL_TIMEOUT_SECONDS,
        disk_reserve_bytes: int = _MINIO_DISK_RESERVE_BYTES,
        inode_reserve: int = _MINIO_INODE_RESERVE,
        available_bytes: AvailableBytes = _available_bytes,
        available_inodes: AvailableInodes = _available_inodes,
        monotonic: Callable[[], float] = time.monotonic,
        deadline_waiter: DeadlineWaiter = _wait_for_stop,
        cancellation_grace_seconds: float = 5.0,
    ) -> None:
        for name, value in (
            ("max_pages", max_pages),
            ("max_objects", max_objects),
            ("max_entries", max_entries),
            ("max_total_bytes", max_total_bytes),
        ):
            if type(value) is not int or value <= 0:
                raise ValueError(f"{name} must be a positive integer")
        if type(disk_reserve_bytes) is not int or disk_reserve_bytes < 0:
            raise ValueError("disk_reserve_bytes must be a non-negative integer")
        if type(inode_reserve) is not int or inode_reserve < 0:
            raise ValueError("inode_reserve must be a non-negative integer")
        if (
            isinstance(timeout_seconds, bool)
            or not isinstance(timeout_seconds, (int, float))
            or timeout_seconds <= 0
        ):
            raise ValueError("timeout_seconds must be positive")
        if (
            isinstance(cancellation_grace_seconds, bool)
            or not isinstance(cancellation_grace_seconds, (int, float))
            or cancellation_grace_seconds <= 0
        ):
            raise ValueError("cancellation_grace_seconds must be positive")
        self._client_factory = client_factory
        self._max_pages = max_pages
        self._max_objects = max_objects
        self._max_entries = max_entries
        self._max_total_bytes = max_total_bytes
        self._timeout_seconds = float(timeout_seconds)
        self._disk_reserve_bytes = disk_reserve_bytes
        self._inode_reserve = inode_reserve
        self._available_bytes = available_bytes
        self._available_inodes = available_inodes
        self._monotonic = monotonic
        self._deadline_waiter = deadline_waiter
        self._cancellation_grace_seconds = float(cancellation_grace_seconds)

    def mirror(
        self,
        *,
        endpoint_url: str,
        access_key: str,
        secret_key: str,
        buckets: tuple[str, ...],
        destination: Path,
        cancel_on_timeout: Callable[[], None] = _do_nothing,
        resources: _BackupResourceBudget | None = None,
    ) -> None:
        deadline = self._monotonic() + self._timeout_seconds
        if endpoint_url != f"http://{_MINIO_LOCAL_HOST}:{_MINIO_LOCAL_PORT}":
            raise ValueError("MinIO endpoint is not approved")
        if buckets != _MINIO_BUCKETS:
            raise ValueError("MinIO bucket set is not approved")
        _require_private_directory(destination)
        available_bytes = self._available_bytes(destination)
        if type(available_bytes) is not int or available_bytes < 0:
            raise ValueError("available backup capacity is invalid")
        capacity = available_bytes - self._disk_reserve_bytes
        if capacity <= 0:
            raise ValueError("available backup capacity is below reserve")
        available_inodes = self._available_inodes(destination)
        if type(available_inodes) is not int or available_inodes < 0:
            raise ValueError("available backup inode capacity is invalid")
        inode_capacity = available_inodes - self._inode_reserve
        if inode_capacity <= 0:
            raise ValueError("available backup inode capacity is below reserve")
        budget = _MirrorBudget(
            max_pages=self._max_pages,
            max_objects=self._max_objects,
            max_total_bytes=min(self._max_total_bytes, capacity),
            max_entries=min(self._max_entries, inode_capacity),
            deadline=deadline,
            monotonic=self._monotonic,
        )
        if resources is None:

            def legacy_capacity(path: Path) -> _CapacitySnapshot:
                legacy_bytes = self._available_bytes(path)
                legacy_inodes = self._available_inodes(path)
                if type(legacy_bytes) is not int or legacy_bytes < 0:
                    raise ValueError("available backup capacity is invalid")
                if type(legacy_inodes) is not int or legacy_inodes < 0:
                    raise ValueError("available backup inode capacity is invalid")
                values = os.statvfs(path)
                return _CapacitySnapshot(
                    free_bytes=legacy_bytes,
                    free_inodes=legacy_inodes,
                    block_size=values.f_frsize or values.f_bsize,
                )

            resources = _BackupResourceBudget(
                destination,
                max_postgres_bytes=min(self._max_total_bytes, capacity),
                max_total_bytes=min(self._max_total_bytes, capacity),
                disk_reserve_bytes=self._disk_reserve_bytes,
                inode_reserve=self._inode_reserve,
                capacity_provider=legacy_capacity,
                max_entries=min(self._max_entries, inode_capacity),
            )
        budget.check_deadline()
        client = self._client_factory(
            "s3",
            endpoint_url=endpoint_url,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name="us-east-1",
            config=Config(
                connect_timeout=5,
                read_timeout=30,
                retries={"total_max_attempts": 3, "mode": "standard"},
                proxies={},
            ),
        )
        close = getattr(client, "close", None)
        if not callable(close):
            raise ValueError("object client does not provide bounded cleanup")
        client_closer = _OnceCloser(close)
        if self._monotonic() >= deadline:
            client_closer()
            raise ValueError("MinIO mirror exceeded total deadline")
        try:
            cancellation = _MirrorCancellationScope(
                deadline=deadline,
                monotonic=self._monotonic,
                waiter=self._deadline_waiter,
                cancellation_grace_seconds=self._cancellation_grace_seconds,
                cancel_external=(
                    cancel_on_timeout
                    if isinstance(cancel_on_timeout, _OnceCloser)
                    else _OnceCloser(cancel_on_timeout)
                ),
            )
            cancellation.bind_client(client_closer)
            cancellation.start()
        except BaseException:
            try:
                client_closer()
            except BaseException:
                raise RuntimeError("MinIO watchdog startup cleanup failed") from None
            raise
        expired = False
        close_error: BaseException | None = None
        try:
            for bucket in buckets:
                _mirror_s3_bucket(
                    client,
                    bucket=bucket,
                    destination=destination,
                    budget=budget,
                    cancellation=cancellation,
                    resources=resources,
                )
            budget.check_deadline()
            cancellation.raise_if_expired()
        finally:
            try:
                client_closer()
            except BaseException as exc:
                close_error = exc
            try:
                expired = cancellation.finish()
            except BaseException:
                if close_error is None:
                    raise
            if close_error is not None:
                raise close_error
        if expired:
            raise ValueError("MinIO mirror exceeded total deadline")
        budget.check_deadline()


class _SubprocessPortForward:
    def __init__(
        self,
        process: subprocess.Popen[bytes],
        startup_output: BinaryIO | None = None,
    ) -> None:
        self._process = process
        output = startup_output if startup_output is not None else process.stdout
        if output is None:
            raise ValueError("port-forward startup output is unavailable")
        self._startup_output = output
        self._ready = threading.Event()
        self._output_failed = threading.Event()
        self._reader = threading.Thread(target=self._drain_output, daemon=True)
        self._reader.start()

    def _drain_output(self) -> None:
        expected = (f"Forwarding from {_MINIO_LOCAL_HOST}:{_MINIO_LOCAL_PORT} -> 9000").encode(
            "ascii"
        )
        startup_bytes = 0
        try:
            while True:
                line = self._startup_output.readline(4097)
                if not line:
                    if not self._ready.is_set():
                        self._output_failed.set()
                    return
                if self._ready.is_set() or self._output_failed.is_set():
                    continue
                startup_bytes += len(line)
                if startup_bytes > _PORT_FORWARD_STARTUP_OUTPUT_LIMIT:
                    self._output_failed.set()
                    continue
                if line.rstrip(b"\r\n") == expected:
                    self._ready.set()
        except Exception:
            self._output_failed.set()

    def wait_ready(self, host: str, port: int, timeout_seconds: float) -> None:
        if host != _MINIO_LOCAL_HOST or port != _MINIO_LOCAL_PORT:
            raise RuntimeError("port-forward readiness target is not approved")
        deadline = time.monotonic() + timeout_seconds
        while True:
            if self._output_failed.is_set():
                raise RuntimeError("port-forward readiness output is unavailable")
            if self._process.poll() is not None:
                raise RuntimeError("port-forward exited before readiness")
            if self._ready.is_set():
                return
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise RuntimeError("port-forward readiness timed out")
            self._ready.wait(timeout=min(0.05, remaining))

    def terminate(self) -> None:
        self._process.terminate()

    def wait(self, timeout_seconds: float) -> bool:
        try:
            self._process.wait(timeout=timeout_seconds)
        except subprocess.TimeoutExpired:
            return False
        return True

    def kill(self) -> None:
        self._process.kill()

    def close(self) -> None:
        self._startup_output.close()
        self._reader.join(timeout=0.25)


def _reap_unwrapped_process(process: subprocess.Popen[bytes]) -> None:
    """Best-effort bounded cleanup when handle construction cannot complete."""
    try:
        process.terminate()
    except BaseException:
        pass
    try:
        process.wait(timeout=_PORT_FORWARD_STOP_TIMEOUT_SECONDS)
    except BaseException:
        try:
            process.kill()
        except BaseException:
            pass
        try:
            process.wait(timeout=_PORT_FORWARD_STOP_TIMEOUT_SECONDS)
        except BaseException:
            pass
    output = process.stdout
    if output is not None:
        try:
            output.close()
        except BaseException:
            pass


class SubprocessBackupCommandRunner:
    """Run binary backup commands with no inherited or captured secret output."""

    def stream_stdout(
        self,
        argv: Sequence[str],
        sink: BinaryIO,
        *,
        env: Mapping[str, str],
        timeout_seconds: float,
    ) -> None:
        process = subprocess.Popen(
            list(argv),
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            env=dict(env),
        )
        completed: threading.Event | None = None
        try:
            if process.stdout is None:
                raise RuntimeError("backup command stdout is unavailable")
            completed = threading.Event()
            timed_out = threading.Event()

            def enforce_timeout() -> None:
                assert completed is not None
                if completed.wait(timeout_seconds):
                    return
                timed_out.set()
                try:
                    process.kill()
                except BaseException:
                    pass

            watchdog = threading.Thread(target=enforce_timeout, daemon=True)
            watchdog.start()
        except BaseException:
            if completed is not None:
                completed.set()
            _reap_unwrapped_process(process)
            raise
        assert completed is not None
        failed = True
        try:
            while True:
                chunk = process.stdout.read(1024 * 1024)
                if not chunk:
                    break
                written = sink.write(chunk)
                if written != len(chunk):
                    raise OSError("backup sink write was incomplete")
            return_code = process.wait(timeout=_PORT_FORWARD_STOP_TIMEOUT_SECONDS)
            failed = return_code != 0 or timed_out.is_set()
        finally:
            completed.set()
            if failed:
                _reap_unwrapped_process(process)
            else:
                process.stdout.close()
            watchdog.join(timeout=_PORT_FORWARD_STOP_TIMEOUT_SECONDS)
        if failed:
            raise RuntimeError("backup command failed")

    def capture_stdout(
        self,
        argv: Sequence[str],
        *,
        env: Mapping[str, str],
        timeout_seconds: float,
    ) -> bytes:
        result = subprocess.run(
            list(argv),
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            env=dict(env),
            check=False,
            timeout=timeout_seconds,
        )
        if result.returncode != 0:
            raise RuntimeError("backup command failed")
        return result.stdout

    def start(
        self,
        argv: Sequence[str],
        *,
        env: Mapping[str, str],
    ) -> PortForwardHandle:
        process = subprocess.Popen(
            list(argv),
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            env=dict(env),
        )
        try:
            return _SubprocessPortForward(process)
        except BaseException:
            _reap_unwrapped_process(process)
            raise


def _decode_minio_credentials(payload: bytes) -> tuple[str, str]:
    loaded = json.loads(payload.decode("utf-8"))
    if not isinstance(loaded, dict):
        raise ValueError("credential secret must be an object")
    data = loaded.get("data")
    if not isinstance(data, dict):
        raise ValueError("credential secret data must be an object")

    decoded: list[str] = []
    for key in ("minio-access-key", "minio-secret-key"):
        raw = data.get(key)
        if not isinstance(raw, str) or not raw:
            raise ValueError("credential field is unavailable")
        value = base64.b64decode(raw, validate=True).decode("utf-8")
        if not value:
            raise ValueError("credential field is empty")
        decoded.append(value)
    return decoded[0], decoded[1]


def _stop_port_forward(handle: PortForwardHandle) -> None:
    try:
        handle.terminate()
    except Exception:
        pass
    try:
        stopped = handle.wait(_PORT_FORWARD_STOP_TIMEOUT_SECONDS)
    except Exception:
        stopped = False
    if not stopped:
        try:
            handle.kill()
        except Exception:
            pass
        try:
            stopped = handle.wait(_PORT_FORWARD_STOP_TIMEOUT_SECONDS)
        except Exception:
            stopped = False
    try:
        handle.close()
    except Exception:
        stopped = False
    if not stopped:
        raise RuntimeError("port-forward cleanup could not confirm exit")


class BackupCreator:
    """Create a timestamped staging backup without publishing partial state."""

    def __init__(
        self,
        config: OperatorConfig,
        *,
        service_uid: int | None = None,
        runner: BackupCommandRunner | None = None,
        minio: MinioMirror | None = None,
        now: Clock | None = None,
        max_postgres_bytes: int = _POSTGRES_MAX_BYTES,
        max_total_bytes: int = _BACKUP_MAX_TOTAL_BYTES,
        disk_reserve_bytes: int = _MINIO_DISK_RESERVE_BYTES,
        inode_reserve: int = _MINIO_INODE_RESERVE,
        capacity_provider: CapacityProvider = _capacity_snapshot,
        traversal_limits: BackupTraversalLimits | None = None,
    ) -> None:
        self.config = config
        if service_uid is None:
            try:
                resolved_uid = pwd.getpwnam(config.service_user).pw_uid
            except (KeyError, OSError):
                pass
            else:
                service_uid = resolved_uid
        if service_uid is None:
            raise BackupError("service_account_unavailable")
        self.service_uid = service_uid
        self._runner = runner or SubprocessBackupCommandRunner()
        self._minio = minio or Boto3MinioMirror(
            max_objects=config.backup_max_objects,
            max_entries=config.backup_max_entries - _BACKUP_NON_MINIO_ENTRY_ALLOWANCE,
        )
        self._now = now or (lambda: datetime.now(UTC))
        self._env = _command_environment(config)
        for name, value in (
            ("max_postgres_bytes", max_postgres_bytes),
            ("max_total_bytes", max_total_bytes),
        ):
            if type(value) is not int or value <= 0:
                raise ValueError(f"{name} must be a positive integer")
        for name, value in (
            ("disk_reserve_bytes", disk_reserve_bytes),
            ("inode_reserve", inode_reserve),
        ):
            if type(value) is not int or value < 0:
                raise ValueError(f"{name} must be a non-negative integer")
        if max_postgres_bytes > max_total_bytes:
            raise ValueError("max_postgres_bytes cannot exceed max_total_bytes")
        self._max_postgres_bytes = max_postgres_bytes
        self._max_total_bytes = max_total_bytes
        self._disk_reserve_bytes = disk_reserve_bytes
        self._inode_reserve = inode_reserve
        self._capacity_provider = capacity_provider
        self._traversal_limits = traversal_limits or BackupTraversalLimits(
            max_files=config.backup_max_objects + _BACKUP_NON_MINIO_FILE_ALLOWANCE,
            max_entries=config.backup_max_entries,
            max_total_bytes=max_total_bytes,
        )

    def _bundle_root(self, request: RolloutRequest, created_at: datetime) -> Path:
        return (
            self.config.rollout_root
            / "backups"
            / self.bundle_name(
                request.request_id,
                created_at,
            )
        )

    @staticmethod
    def bundle_name(request_id: str, created_at: datetime) -> str:
        timestamp = created_at.astimezone(UTC).strftime("%Y%m%dT%H%M%SZ")
        return f"{timestamp}-{request_id}"

    def _load_buckets(self) -> tuple[str, ...]:
        cluster_config = load_cluster_config(self.config.cluster_config_path)
        buckets = cast(
            tuple[str, ...],
            (cluster_config.trajectories_bucket, cluster_config.artifacts_bucket),
        )
        if buckets != _MINIO_BUCKETS:
            raise ValueError("staging bucket configuration is not approved")
        return buckets

    def _dump_postgres(
        self,
        destination: Path,
        *,
        resources: _BackupResourceBudget,
    ) -> None:
        flags = (
            os.O_WRONLY
            | os.O_CREAT
            | os.O_EXCL
            | getattr(os, "O_CLOEXEC", 0)
            | getattr(os, "O_NOFOLLOW", 0)
        )
        account = resources.reserve_entry(destination, component="postgres")
        try:
            fd = os.open(destination, flags, _PRIVATE_FILE_MODE)
        except BaseException:
            resources.release_entry(account)
            raise
        try:
            os.fchmod(fd, _PRIVATE_FILE_MODE)
            with os.fdopen(fd, "wb", closefd=False) as sink:
                guarded_sink = _BudgetedWriter(
                    sink,
                    path=destination,
                    resources=resources,
                    component="postgres",
                    account=account,
                )
                self._runner.stream_stdout(
                    [
                        "kubectl",
                        "-n",
                        self.config.namespace,
                        "exec",
                        "statefulset/loom-postgres",
                        "--",
                        "sh",
                        "-ceu",
                        _POSTGRES_DUMP_COMMAND,
                    ],
                    cast(BinaryIO, guarded_sink),
                    env=self._env,
                    timeout_seconds=_POSTGRES_TIMEOUT_SECONDS,
                )
                guarded_sink.flush()
                os.fsync(sink.fileno())
                resources.reconcile_writer(account)
        finally:
            os.close(fd)

    def _read_minio_credentials(self) -> tuple[str, str]:
        payload = self._runner.capture_stdout(
            [
                "kubectl",
                "-n",
                self.config.namespace,
                "get",
                "secret",
                "loom-secrets",
                "-o",
                "json",
            ],
            env=self._env,
            timeout_seconds=_KUBECTL_READ_TIMEOUT_SECONDS,
        )
        return _decode_minio_credentials(payload)

    def _export_secret(
        self,
        name: str,
        destination: Path,
        *,
        resources: _BackupResourceBudget,
    ) -> None:
        payload = self._runner.capture_stdout(
            [
                "kubectl",
                "-n",
                self.config.namespace,
                "get",
                "secret",
                name,
                "-o",
                "yaml",
            ],
            env=self._env,
            timeout_seconds=_KUBECTL_READ_TIMEOUT_SECONDS,
        )
        loaded = yaml.safe_load(payload.decode("utf-8"))
        if not isinstance(loaded, dict) or loaded.get("kind") != "Secret":
            raise ValueError("exported object is not a Secret")
        metadata = loaded.get("metadata")
        if not isinstance(metadata, dict) or metadata.get("name") != name:
            raise ValueError("exported Secret name does not match")
        _write_private_bytes(
            destination / f"{name}.yaml",
            payload,
            resources=resources,
            component="k8s_secrets",
        )

    def _mirror_minio(
        self,
        destination: Path,
        *,
        buckets: tuple[str, ...],
        access_key: str,
        secret_key: str,
        resources: _BackupResourceBudget | None = None,
    ) -> None:
        if resources is None:
            resources = _BackupResourceBudget(
                self.config.rollout_root,
                max_postgres_bytes=self._max_postgres_bytes,
                max_total_bytes=self._max_total_bytes,
                disk_reserve_bytes=self._disk_reserve_bytes,
                inode_reserve=self._inode_reserve,
                capacity_provider=self._capacity_provider,
                max_entries=self._traversal_limits.max_entries,
            )
        handle = self._runner.start(
            [
                "kubectl",
                "-n",
                self.config.namespace,
                "port-forward",
                "--address",
                _MINIO_LOCAL_HOST,
                "service/loom-minio",
                f"{_MINIO_LOCAL_PORT}:9000",
            ],
            env=self._env,
        )
        stop_port_forward = _OnceCloser(
            lambda: _stop_port_forward(handle),
            wait_timeout_seconds=_PORT_FORWARD_CLEANUP_WAIT_SECONDS,
        )
        try:
            handle.wait_ready(
                _MINIO_LOCAL_HOST,
                _MINIO_LOCAL_PORT,
                _PORT_FORWARD_READY_TIMEOUT_SECONDS,
            )
            self._minio.mirror(
                endpoint_url=f"http://{_MINIO_LOCAL_HOST}:{_MINIO_LOCAL_PORT}",
                access_key=access_key,
                secret_key=secret_key,
                buckets=buckets,
                destination=destination,
                cancel_on_timeout=stop_port_forward,
                resources=resources,
            )
        finally:
            stop_port_forward()

    def _write_manifest(
        self,
        manifest_path: Path,
        *,
        created_at: datetime,
        postgres_path: Path,
        minio_path: Path,
        secrets_path: Path,
        resources: _BackupResourceBudget,
    ) -> None:
        write_backup_manifest(
            environment=self.config.environment,
            namespace=self.config.namespace,
            output_path=manifest_path,
            components={
                "postgres": postgres_path,
                "minio": minio_path,
                "k8s_secrets": secrets_path,
            },
            now=created_at,
            limits=self._traversal_limits,
            write_output=lambda path, payload: _write_private_bytes(
                path,
                payload,
                resources=resources,
                component="manifest",
            ),
        )
        _fsync_private_file(manifest_path)
        _fsync_directory(manifest_path.parent)

    def _validate_manifest(self, manifest_path: Path, *, now: datetime) -> None:
        problems = validate_backup_manifest(
            manifest_path,
            environment=self.config.environment,
            namespace=self.config.namespace,
            min_remaining_hours=_BACKUP_MIN_REMAINING_HOURS,
            now=now,
            expected_owner_uid=self.service_uid,
            require_private_files=True,
            enforce_freshness=True,
            limits=self._traversal_limits,
        )
        if problems:
            raise ValueError("backup manifest did not pass strict validation")

    def revalidate(
        self,
        backup: VerifiedBackup,
        *,
        enforce_freshness: bool,
    ) -> VerifiedBackup:
        backups_root = self.config.rollout_root / "backups"
        if (
            backup.manifest_path.name != "backup-manifest.json"
            or backup.manifest_path.parent.parent != backups_root
        ):
            raise BackupError("backup_path_not_approved")
        validated_at = _stage(
            "backup_clock_invalid",
            lambda: _clock_utc(self._now),
        )
        problems = _stage(
            "backup_revalidation_failed",
            lambda: validate_backup_manifest(
                backup.manifest_path,
                environment=self.config.environment,
                namespace=self.config.namespace,
                min_remaining_hours=(_BACKUP_MIN_REMAINING_HOURS if enforce_freshness else 0),
                now=validated_at,
                expected_owner_uid=self.service_uid,
                require_private_files=True,
                enforce_freshness=enforce_freshness,
                limits=self._traversal_limits,
            ),
        )
        if problems:
            raise BackupError("backup_revalidation_failed")
        created_at: datetime | None = None
        if enforce_freshness:
            created_at = _stage(
                "backup_revalidation_failed",
                lambda: backup_manifest_created_at(
                    backup.manifest_path,
                    expected_owner_uid=self.service_uid,
                    require_private_file=True,
                    limits=self._traversal_limits,
                ),
            )
        actual_digest = _stage(
            "backup_revalidation_failed",
            lambda: backup_manifest_sha256(
                backup.manifest_path,
                expected_owner_uid=self.service_uid,
                require_private_file=True,
                limits=self._traversal_limits,
            ),
        )
        if not hmac.compare_digest(actual_digest, backup.manifest_sha256):
            raise BackupError("backup_manifest_digest_mismatch")
        if created_at is not None:
            completion_time = _stage(
                "backup_clock_invalid",
                lambda: _clock_utc(self._now),
            )
            _stage(
                "backup_revalidation_failed",
                lambda: _require_launch_freshness(
                    created_at=created_at,
                    now=completion_time,
                ),
            )
        return backup

    def cleanup_incomplete(self, request_id: str, *, bundle_name: str | None = None) -> bool:
        """Remove only the no-manifest backup root bound to one failed request."""
        if not re.fullmatch(r"[a-z0-9][a-z0-9-]{7,79}", request_id):
            raise BackupError("backup_cleanup_request_invalid")

        backups_root = self.config.rollout_root / "backups"
        requested_bundle_name = bundle_name

        def cleanup() -> bool:
            backups_fd = _open_directory_no_follow(backups_root)
            bundle_fd: int | None = None
            try:
                cleanup_deadline = (
                    self._traversal_limits.monotonic() + self._traversal_limits.max_elapsed_seconds
                )
                backups_metadata = os.fstat(backups_fd)
                if not _existing_backups_directory_is_approved(
                    backups_metadata,
                    service_uid=self.service_uid,
                ):
                    raise ValueError("backups directory metadata is not approved")
                matches: list[str] = []
                if requested_bundle_name is not None:
                    matched = _BUNDLE_NAME_RE.fullmatch(requested_bundle_name)
                    if matched is None or matched.group("request_id") != request_id:
                        raise ValueError("backup cleanup root binding is invalid")
                    matches.append(requested_bundle_name)
                else:
                    scanned = 0
                    with os.scandir(backups_fd) as entries:
                        for entry in entries:
                            if self._traversal_limits.monotonic() >= cleanup_deadline:
                                raise ValueError("backup cleanup exceeded elapsed-time limit")
                            scanned += 1
                            if scanned > self._traversal_limits.max_directory_entries:
                                raise ValueError("backup cleanup exceeded directory entry limit")
                            matched = _BUNDLE_NAME_RE.fullmatch(entry.name)
                            if matched is not None and matched.group("request_id") == request_id:
                                matches.append(entry.name)
                if not matches:
                    return False
                if len(matches) != 1:
                    raise ValueError("backup cleanup request has ambiguous roots")
                bundle_name = matches[0]
                try:
                    bundle_metadata = os.stat(
                        bundle_name,
                        dir_fd=backups_fd,
                        follow_symlinks=False,
                    )
                except FileNotFoundError:
                    if requested_bundle_name is not None:
                        return False
                    raise
                _require_cleanup_entry(bundle_metadata, service_uid=self.service_uid)
                if not stat.S_ISDIR(bundle_metadata.st_mode):
                    raise ValueError("backup cleanup target is not a directory")
                if bundle_metadata.st_dev != backups_metadata.st_dev:
                    raise ValueError("backup cleanup root crosses a filesystem boundary")
                latest_target = _read_previous_latest_target(backups_fd)
                if latest_target == bundle_name:
                    raise ValueError("manifest-backed latest backup cannot be cleaned")
                bundle_fd = os.open(bundle_name, _directory_open_flags(), dir_fd=backups_fd)
                opened = os.fstat(bundle_fd)
                if (opened.st_dev, opened.st_ino) != (
                    bundle_metadata.st_dev,
                    bundle_metadata.st_ino,
                ):
                    raise ValueError("backup cleanup root changed during validation")
                traversal = _CleanupTraversal(
                    limits=self._traversal_limits,
                    deadline=cleanup_deadline,
                    root_device=backups_metadata.st_dev,
                )
                try:
                    manifest_metadata = os.stat(
                        "backup-manifest.json",
                        dir_fd=bundle_fd,
                        follow_symlinks=False,
                    )
                except FileNotFoundError:
                    pass
                else:
                    del manifest_metadata
                    raise ValueError("manifest-backed backup cannot be cleaned")
                _validate_cleanup_directory(
                    bundle_fd,
                    service_uid=self.service_uid,
                    budget=traversal,
                    depth=1,
                )
                current = os.stat(bundle_name, dir_fd=backups_fd, follow_symlinks=False)
                if (current.st_dev, current.st_ino) != (
                    bundle_metadata.st_dev,
                    bundle_metadata.st_ino,
                ):
                    raise ValueError("backup cleanup root changed during validation")
                if _read_previous_latest_target(backups_fd) == bundle_name:
                    raise ValueError("manifest-backed latest backup cannot be cleaned")
                try:
                    os.stat(
                        "backup-manifest.json",
                        dir_fd=bundle_fd,
                        follow_symlinks=False,
                    )
                except FileNotFoundError:
                    pass
                else:
                    raise ValueError("manifest-backed backup cannot be cleaned")
                _remove_cleanup_directory(
                    bundle_fd,
                    service_uid=self.service_uid,
                    budget=traversal.fresh_pass(),
                    depth=1,
                )
                traversal.check_deadline()
                os.close(bundle_fd)
                bundle_fd = None
                current = os.stat(bundle_name, dir_fd=backups_fd, follow_symlinks=False)
                if (current.st_dev, current.st_ino) != (
                    bundle_metadata.st_dev,
                    bundle_metadata.st_ino,
                ):
                    raise ValueError("backup cleanup root changed during removal")
                os.rmdir(bundle_name, dir_fd=backups_fd)
                traversal.check_deadline()
                os.fsync(backups_fd)
                traversal.check_deadline()
                return True
            finally:
                if bundle_fd is not None:
                    os.close(bundle_fd)
                os.close(backups_fd)

        return _stage("backup_cleanup_failed", cleanup)

    def create(
        self,
        request: RolloutRequest,
        *,
        created_at: datetime | None = None,
    ) -> VerifiedBackup:
        if request.status != "pending":
            raise BackupError("backup_request_not_pending")
        if created_at is None:
            created_at = _stage(
                "backup_clock_invalid",
                lambda: _clock_utc(self._now),
            )
        else:
            provided_created_at = created_at
            created_at = _stage(
                "backup_clock_invalid",
                lambda: _clock_utc(lambda: provided_created_at),
            )
        bundle_root = self._bundle_root(request, created_at)
        resources = _BackupResourceBudget(
            self.config.rollout_root,
            max_postgres_bytes=self._max_postgres_bytes,
            max_total_bytes=self._max_total_bytes,
            disk_reserve_bytes=self._disk_reserve_bytes,
            inode_reserve=self._inode_reserve,
            capacity_provider=self._capacity_provider,
            max_entries=self._traversal_limits.max_entries,
        )
        _stage("backup_capacity_unavailable", resources.check_live)
        _stage(
            "backup_root_create_failed",
            lambda: _create_bundle_root(
                self.config.rollout_root,
                bundle_root.name,
                service_uid=self.service_uid,
                resources=resources,
            ),
        )
        postgres_dir = bundle_root / "postgres"
        minio_dir = bundle_root / "minio"
        secrets_dir = bundle_root / "secrets"
        for directory in (postgres_dir, minio_dir, secrets_dir):
            _stage(
                "backup_root_create_failed",
                partial(_private_directory, directory, resources=resources),
            )

        buckets = _stage("minio_bucket_config_invalid", self._load_buckets)
        _stage(
            "postgres_dump_failed",
            lambda: self._dump_postgres(
                postgres_dir / "loom.dump",
                resources=resources,
            ),
        )
        access_key, secret_key = _stage(
            "minio_credentials_failed",
            self._read_minio_credentials,
        )
        _stage(
            "minio_snapshot_failed",
            lambda: self._mirror_minio(
                minio_dir,
                buckets=buckets,
                access_key=access_key,
                secret_key=secret_key,
                resources=resources,
            ),
        )
        for secret_name in _RESTORE_SECRET_NAMES:
            _stage(
                "secret_export_failed",
                partial(
                    self._export_secret,
                    secret_name,
                    secrets_dir,
                    resources=resources,
                ),
            )
        _stage("component_sync_failed", lambda: _fsync_private_tree(bundle_root))
        pending_manifest_path = bundle_root / f".backup-manifest.{uuid4().hex}.pending"
        manifest_path = bundle_root / "backup-manifest.json"
        try:
            _stage(
                "manifest_write_failed",
                lambda: self._write_manifest(
                    pending_manifest_path,
                    created_at=created_at,
                    postgres_path=postgres_dir / "loom.dump",
                    minio_path=minio_dir,
                    secrets_path=secrets_dir,
                    resources=resources,
                ),
            )
            validation_time = _stage(
                "backup_clock_invalid",
                lambda: _clock_utc(self._now),
            )
            _stage(
                "manifest_validation_failed",
                lambda: self._validate_manifest(
                    pending_manifest_path,
                    now=validation_time,
                ),
            )
            digest = _stage(
                "manifest_hash_failed",
                lambda: backup_manifest_sha256(
                    pending_manifest_path,
                    expected_owner_uid=self.service_uid,
                    require_private_file=True,
                    limits=self._traversal_limits,
                ),
            )
            completion_time = _stage(
                "backup_clock_invalid",
                lambda: _clock_utc(self._now),
            )
            _stage(
                "manifest_validation_failed",
                lambda: _require_launch_freshness(
                    created_at=created_at,
                    now=completion_time,
                ),
            )
            _stage(
                "manifest_publish_failed",
                lambda: _publish_manifest(
                    pending_manifest_path,
                    manifest_path,
                    resources=resources,
                ),
            )
            _publish_latest_stage(bundle_root, resources=resources)
        except _LatestStageError as exc:
            if exc.rollback_confirmed:
                _remove_failed_manifests(pending_manifest_path, manifest_path)
            else:
                _remove_failed_manifests(pending_manifest_path)
            raise
        except BackupError:
            _remove_failed_manifests(pending_manifest_path, manifest_path)
            raise
        return VerifiedBackup(
            manifest_path=manifest_path,
            manifest_sha256=digest,
        )


__all__ = [
    "BackupCommandRunner",
    "BackupCreator",
    "BackupError",
    "BackupPolicyLimitError",
    "BackupPublicReason",
    "MinioMirror",
    "PortForwardHandle",
    "SubprocessBackupCommandRunner",
    "VerifiedBackup",
    "normalize_backup_public_reason",
]
