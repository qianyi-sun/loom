"""Private, crash-durable persistence for staging rollout requests."""

from __future__ import annotations

import contextlib
import fcntl
import hashlib
import json
import os
import stat
from collections.abc import Callable, Iterator, Mapping
from contextlib import contextmanager
from dataclasses import replace
from pathlib import Path
from typing import TYPE_CHECKING, cast
from uuid import uuid4

from ..preflight_artifact_retention import MAX_RETIREMENTS_PER_PLAN

if TYPE_CHECKING:
    from loom_cli.rollout.preflight_pipeline import PreflightAssessment, PreflightRehearsal

from .backup_job import (
    BackupJobEnvelope,
    BackupJobState,
    PreflightBackupJobEnvelope,
    validate_job_binding,
)
from .backup_lease import BackupLease
from .backup_rotation import (
    BackupPayloadPhase,
    BackupPayloadRecord,
    BackupRetirementRecord,
    BackupRotationState,
)
from .model import (
    ActivePointer,
    DriverEnvelope,
    PreflightRequest,
    RequestEvent,
    RolloutRequest,
    validate_safe_identifier,
)

_PRIVATE_FILE_MODE = 0o600
_PRIVATE_DIRECTORY_MODE = 0o700


class RequestStoreError(RuntimeError):
    """Raised when request persistence cannot be completed safely."""


def _json_bytes(payload: Mapping[str, object]) -> bytes:
    rendered = json.dumps(
        dict(payload),
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    )
    return (rendered + "\n").encode("utf-8")


def _reject_duplicate_keys(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate JSON key {key!r}")
        result[key] = value
    return result


def _load_json_object(payload: bytes, object_name: str) -> dict[str, object]:
    try:
        decoded = payload.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise RequestStoreError(f"{object_name} must be valid UTF-8") from exc
    try:
        loaded = json.loads(
            decoded,
            object_pairs_hook=_reject_duplicate_keys,
            parse_constant=lambda value: (_ for _ in ()).throw(
                ValueError(f"non-finite JSON value {value!r}")
            ),
        )
    except (json.JSONDecodeError, ValueError) as exc:
        raise RequestStoreError(f"{object_name} is not valid JSON: {exc}") from exc
    if not isinstance(loaded, dict):
        raise RequestStoreError(f"{object_name} must be a JSON object")
    return cast(dict[str, object], loaded)


def _request_id(value: object) -> str:
    try:
        return validate_safe_identifier(value, "request_id")
    except ValueError as exc:
        raise RequestStoreError(str(exc)) from exc


def _attempt_number(value: object) -> int:
    if type(value) is not int or value < 1:
        raise RequestStoreError("attempt_number must be a positive integer")
    return value


def _is_sha256(value: object) -> bool:
    return (
        isinstance(value, str)
        and len(value) == 64
        and not any(character not in "0123456789abcdef" for character in value)
    )


def _directory_flags() -> int:
    return (
        os.O_RDONLY
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_DIRECTORY", 0)
        | getattr(os, "O_NOFOLLOW", 0)
    )


def _fsync_directory(path: Path) -> None:
    try:
        fd = os.open(path, _directory_flags())
    except OSError as exc:
        raise RequestStoreError(f"could not open persistence directory {path}") from exc
    try:
        os.fsync(fd)
    except OSError as exc:
        raise RequestStoreError(f"could not fsync persistence directory {path}") from exc
    finally:
        os.close(fd)


def _metadata_identity(metadata: os.stat_result) -> tuple[int, ...]:
    return (
        metadata.st_dev,
        metadata.st_ino,
        metadata.st_mode,
        metadata.st_nlink,
        metadata.st_uid,
        metadata.st_gid,
        metadata.st_size,
        metadata.st_mtime_ns,
        metadata.st_ctime_ns,
    )


def _validate_private_directory(path: Path, label: str) -> None:
    try:
        metadata = path.lstat()
    except FileNotFoundError as exc:
        raise RequestStoreError(f"{label} does not exist") from exc
    except OSError as exc:
        raise RequestStoreError(f"could not inspect {label}") from exc
    if not stat.S_ISDIR(metadata.st_mode):
        raise RequestStoreError(f"{label} must be a directory, not a symlink")
    if metadata.st_uid != os.geteuid():
        raise RequestStoreError(f"{label} must be owned by the effective service UID")
    if stat.S_IMODE(metadata.st_mode) != _PRIVATE_DIRECTORY_MODE:
        raise RequestStoreError(f"{label} must have mode 0700")


def _ensure_private_directory(path: Path, label: str) -> bool:
    """Ensure a private directory exists and return whether it was created."""
    try:
        path.mkdir(mode=_PRIVATE_DIRECTORY_MODE)
    except FileExistsError:
        _validate_private_directory(path, label)
        return False
    except FileNotFoundError as exc:
        raise RequestStoreError(f"parent directory for {label} does not exist") from exc
    except OSError as exc:
        raise RequestStoreError(f"could not create {label}") from exc
    try:
        path.chmod(_PRIVATE_DIRECTORY_MODE)
        _validate_private_directory(path, label)
        _fsync_directory(path.parent)
    except Exception:
        with contextlib.suppress(OSError):
            path.rmdir()
        raise
    return True


def _read_private_bytes(
    path: Path,
    object_name: str,
    *,
    lock_operation: int | None = None,
    require_single_link: bool = False,
) -> bytes:
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        fd = os.open(path, flags)
    except FileNotFoundError as exc:
        raise RequestStoreError(f"{object_name} does not exist") from exc
    except OSError as exc:
        raise RequestStoreError(f"could not open {object_name} as a private regular file") from exc
    locked = False
    try:
        metadata = os.fstat(fd)
        if not stat.S_ISREG(metadata.st_mode):
            raise RequestStoreError(f"{object_name} must be a regular file")
        if metadata.st_uid != os.geteuid():
            raise RequestStoreError(f"{object_name} must be owned by the effective service UID")
        if stat.S_IMODE(metadata.st_mode) != _PRIVATE_FILE_MODE:
            raise RequestStoreError(f"{object_name} must have mode 0600")
        if require_single_link and metadata.st_nlink != 1:
            raise RequestStoreError(f"{object_name} must be single-link")
        if lock_operation is not None:
            fcntl.flock(fd, lock_operation)
            locked = True
        with os.fdopen(fd, "rb", closefd=False) as handle:
            return handle.read()
    except OSError as exc:
        raise RequestStoreError(f"could not read {object_name}") from exc
    finally:
        if locked:
            with contextlib.suppress(OSError):
                fcntl.flock(fd, fcntl.LOCK_UN)
        os.close(fd)


def _read_json(
    path: Path,
    object_name: str,
    *,
    require_single_link: bool = False,
) -> dict[str, object]:
    return _load_json_object(
        _read_private_bytes(
            path,
            object_name,
            require_single_link=require_single_link,
        ),
        object_name,
    )


def _open_parent_directory(path: Path) -> int:
    try:
        return os.open(path, _directory_flags())
    except OSError as exc:
        raise RequestStoreError(f"could not open persistence directory {path}") from exc


def _write_temp_file(directory_fd: int, temp_name: str, payload: bytes) -> None:
    flags = (
        os.O_WRONLY
        | os.O_CREAT
        | os.O_EXCL
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_NOFOLLOW", 0)
    )
    fd: int | None = None
    try:
        fd = os.open(temp_name, flags, _PRIVATE_FILE_MODE, dir_fd=directory_fd)
        os.fchmod(fd, _PRIVATE_FILE_MODE)
        with os.fdopen(fd, "wb", closefd=True) as handle:
            fd = None
            written = handle.write(payload)
            if written != len(payload):
                raise RequestStoreError("private persistence write was incomplete")
            handle.flush()
            os.fsync(handle.fileno())
    except RequestStoreError:
        raise
    except OSError as exc:
        raise RequestStoreError("could not write and fsync private temporary file") from exc
    finally:
        if fd is not None:
            os.close(fd)


def _publish_immutable(path: Path, payload: Mapping[str, object]) -> Path:
    """Publish with hard-link no-replace semantics and directory durability."""
    directory_fd = _open_parent_directory(path.parent)
    temp_name = f".{path.name}.{uuid4().hex}.tmp"
    temp_exists = False
    try:
        _write_temp_file(directory_fd, temp_name, _json_bytes(payload))
        temp_exists = True
        try:
            os.link(
                temp_name,
                path.name,
                src_dir_fd=directory_fd,
                dst_dir_fd=directory_fd,
                follow_symlinks=False,
            )
        except FileExistsError as exc:
            raise RequestStoreError(f"{path.name} already exists") from exc
        except OSError as exc:
            raise RequestStoreError(f"could not publish immutable {path.name}") from exc
        os.unlink(temp_name, dir_fd=directory_fd)
        temp_exists = False
        try:
            os.fsync(directory_fd)
        except OSError as exc:
            raise RequestStoreError(f"could not fsync immutable {path.name} directory") from exc
        return path
    finally:
        if temp_exists:
            with contextlib.suppress(OSError):
                os.unlink(temp_name, dir_fd=directory_fd)
                os.fsync(directory_fd)
        os.close(directory_fd)


def _recover_exact_immutable_temp_link(
    path: Path,
    *,
    expected_payload: bytes,
    object_name: str,
) -> None:
    directory_fd = _open_parent_directory(path.parent)
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    descriptor = -1
    try:
        descriptor = os.open(path.name, flags, dir_fd=directory_fd)
        before = os.fstat(descriptor)
        if before.st_nlink == 1:
            return
        if (
            not stat.S_ISREG(before.st_mode)
            or before.st_uid != os.geteuid()
            or stat.S_IMODE(before.st_mode) != _PRIVATE_FILE_MODE
            or before.st_nlink != 2
            or before.st_size != len(expected_payload)
        ):
            raise RequestStoreError(f"{object_name} temp-link residue is unsafe")
        chunks: list[bytes] = []
        remaining = len(expected_payload) + 1
        while remaining > 0:
            chunk = os.read(descriptor, min(64 * 1024, remaining))
            if not chunk:
                break
            chunks.append(chunk)
            remaining -= len(chunk)
        after_read = os.fstat(descriptor)
        if _metadata_identity(before) != _metadata_identity(after_read):
            raise RequestStoreError(f"{object_name} temp-link residue changed during read")
        if b"".join(chunks) != expected_payload:
            raise RequestStoreError(f"{object_name} temp-link residue identity drifted")
        prefix = f".{path.name}."
        suffix = ".tmp"
        aliases: list[str] = []
        with os.scandir(directory_fd) as entries:
            for entry in entries:
                if not entry.name.startswith(prefix) or not entry.name.endswith(suffix):
                    continue
                token = entry.name[len(prefix) : -len(suffix)]
                if len(token) != 32 or any(
                    character not in "0123456789abcdef" for character in token
                ):
                    continue
                metadata = entry.stat(follow_symlinks=False)
                if (metadata.st_dev, metadata.st_ino) == (before.st_dev, before.st_ino):
                    aliases.append(entry.name)
        if len(aliases) != 1:
            raise RequestStoreError(
                f"{object_name} must be single-link; temp-link residue is ambiguous"
            )
        alias_descriptor = os.open(aliases[0], flags, dir_fd=directory_fd)
        try:
            if _metadata_identity(os.fstat(alias_descriptor)) != _metadata_identity(before):
                raise RequestStoreError(f"{object_name} temp-link residue changed during open")
        finally:
            os.close(alias_descriptor)
        os.unlink(aliases[0], dir_fd=directory_fd)
        os.fsync(directory_fd)
        after_unlink = os.fstat(descriptor)
        stable_before = (
            before.st_dev,
            before.st_ino,
            before.st_mode,
            before.st_uid,
            before.st_gid,
            before.st_size,
            before.st_mtime_ns,
        )
        stable_after = (
            after_unlink.st_dev,
            after_unlink.st_ino,
            after_unlink.st_mode,
            after_unlink.st_uid,
            after_unlink.st_gid,
            after_unlink.st_size,
            after_unlink.st_mtime_ns,
        )
        if stable_after != stable_before or after_unlink.st_nlink != 1:
            raise RequestStoreError(f"{object_name} temp-link residue did not converge")
    except RequestStoreError:
        raise
    except OSError as exc:
        raise RequestStoreError(f"could not recover {object_name} temp-link residue") from exc
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        os.close(directory_fd)


def _replace_mutable(path: Path, payload: Mapping[str, object]) -> Path:
    directory_fd = _open_parent_directory(path.parent)
    temp_name = f".{path.name}.{uuid4().hex}.tmp"
    temp_exists = False
    try:
        _write_temp_file(directory_fd, temp_name, _json_bytes(payload))
        temp_exists = True
        try:
            os.replace(
                temp_name,
                path.name,
                src_dir_fd=directory_fd,
                dst_dir_fd=directory_fd,
            )
        except OSError as exc:
            raise RequestStoreError(f"could not atomically replace {path.name}") from exc
        temp_exists = False
        try:
            os.fsync(directory_fd)
        except OSError as exc:
            raise RequestStoreError(f"could not fsync mutable {path.name} directory") from exc
        return path
    finally:
        if temp_exists:
            with contextlib.suppress(OSError):
                os.unlink(temp_name, dir_fd=directory_fd)
                os.fsync(directory_fd)
        os.close(directory_fd)


class RequestStore:
    """Derive all request paths from validated identifiers under one root."""

    def __init__(self, root: Path | str) -> None:
        self.root = Path(root)
        if not self.root.is_absolute():
            raise RequestStoreError("request store root must be absolute")
        self.requests_root = self.root / "requests"
        self.active_path = self.root / "active.json"
        self._active_lock_path = self.root / ".active.lock"
        self.backup_rotation_path = self.root / "backup-rotation.json"
        self._backup_rotation_lock_path = self.root / ".backup-rotation.lock"
        self.backup_retention_claim_path = self.root / "backup-retention-claim.json"
        self.preflight_artifact_retention_claim_path = (
            self.root / "preflight-artifact-retention-claim.json"
        )
        self.backup_leases_root = self.root / "backup-leases"
        self.backup_retirements_root = self.root / "backup-retirements"
        self.preflight_artifact_retirements_root = self.root / "preflight-artifact-retirements"

    def _ensure_store(self) -> None:
        if not self.root.exists():
            try:
                self.root.mkdir(parents=True, mode=_PRIVATE_DIRECTORY_MODE)
                self.root.chmod(_PRIVATE_DIRECTORY_MODE)
                _fsync_directory(self.root.parent)
            except OSError as exc:
                raise RequestStoreError("could not create request store root") from exc
        _validate_private_directory(self.root, "request store root")
        _ensure_private_directory(self.requests_root, "requests directory")

    def request_ids(self) -> tuple[str, ...]:
        """Return every typed request identity without skipping unknown entries."""
        try:
            self.root.lstat()
        except FileNotFoundError:
            return ()
        except OSError as exc:
            raise RequestStoreError("could not inspect request store root") from exc
        _validate_private_directory(self.root, "request store root")
        try:
            before = self.requests_root.lstat()
        except FileNotFoundError:
            return ()
        except OSError as exc:
            raise RequestStoreError("could not inspect requests directory") from exc
        _validate_private_directory(self.requests_root, "requests directory")
        request_ids: list[str] = []
        directory_fd: int | None = None
        try:
            directory_fd = os.open(self.requests_root, _directory_flags())
            opened = os.fstat(directory_fd)
            if _metadata_identity(before) != _metadata_identity(opened):
                raise RequestStoreError("requests directory changed during inventory")
            with os.scandir(directory_fd) as entries:
                for entry in entries:
                    try:
                        request_id = _request_id(entry.name)
                        metadata = entry.stat(follow_symlinks=False)
                    except (OSError, RequestStoreError) as exc:
                        raise RequestStoreError(
                            "requests directory contains an unsafe entry"
                        ) from exc
                    if (
                        not stat.S_ISDIR(metadata.st_mode)
                        or metadata.st_uid != os.geteuid()
                        or stat.S_IMODE(metadata.st_mode) != _PRIVATE_DIRECTORY_MODE
                    ):
                        raise RequestStoreError("requests directory contains an unsafe entry")
                    self._require_record_directory(request_id)
                    try:
                        self.read_preflight_request(request_id)
                    except RequestStoreError as exc:
                        if str(exc) != "preflight request does not exist":
                            raise
                    try:
                        self.read_request(request_id)
                    except RequestStoreError as exc:
                        if str(exc) != "rollout request is not promoted":
                            raise
                    request_ids.append(request_id)
        except RequestStoreError:
            raise
        except OSError as exc:
            raise RequestStoreError("could not inspect requests directory") from exc
        finally:
            if directory_fd is not None:
                os.close(directory_fd)
        try:
            after = self.requests_root.lstat()
        except OSError as exc:
            raise RequestStoreError("requests directory changed during inventory") from exc
        if _metadata_identity(before) != _metadata_identity(after):
            raise RequestStoreError("requests directory changed during inventory")
        if len(set(request_ids)) != len(request_ids):
            raise RequestStoreError("requests directory contains duplicate authority")
        return tuple(sorted(request_ids))

    def attempt_numbers(self, request_id: str) -> tuple[int, ...]:
        """Return every complete consecutive immutable attempt for one request."""
        request_directory = self._require_request_directory(request_id)
        attempts_directory = request_directory / "attempts"
        try:
            before = attempts_directory.lstat()
        except FileNotFoundError:
            return ()
        except OSError as exc:
            raise RequestStoreError("could not inspect attempts directory") from exc
        _validate_private_directory(attempts_directory, "attempts directory")
        numbers: list[int] = []
        directory_fd: int | None = None
        try:
            directory_fd = os.open(attempts_directory, _directory_flags())
            opened = os.fstat(directory_fd)
            if _metadata_identity(before) != _metadata_identity(opened):
                raise RequestStoreError("attempts directory changed during inventory")
            with os.scandir(directory_fd) as entries:
                for entry in entries:
                    if (
                        not entry.name.isdecimal()
                        or entry.name.startswith("0")
                        or not entry.is_dir(follow_symlinks=False)
                    ):
                        raise RequestStoreError("attempts directory contains an unsafe entry")
                    number = _attempt_number(int(entry.name))
                    metadata = entry.stat(follow_symlinks=False)
                    if (
                        not stat.S_ISDIR(metadata.st_mode)
                        or metadata.st_uid != os.geteuid()
                        or stat.S_IMODE(metadata.st_mode) != _PRIVATE_DIRECTORY_MODE
                    ):
                        raise RequestStoreError("attempts directory contains an unsafe entry")
                    numbers.append(number)
        except RequestStoreError:
            raise
        except OSError as exc:
            raise RequestStoreError("could not inspect attempts directory") from exc
        finally:
            if directory_fd is not None:
                os.close(directory_fd)
        ordered = tuple(sorted(numbers))
        if ordered != tuple(range(1, len(ordered) + 1)):
            raise RequestStoreError("attempt directories are not consecutive")
        for number in ordered:
            try:
                self.read_attempt_envelope(request_id, number)
            except RequestStoreError as exc:
                raise RequestStoreError("attempt inventory envelope is unavailable") from exc
        try:
            after = attempts_directory.lstat()
        except OSError as exc:
            raise RequestStoreError("attempts directory changed during inventory") from exc
        if _metadata_identity(before) != _metadata_identity(after):
            raise RequestStoreError("attempts directory changed during inventory")
        return ordered

    def publish_backup_lease(self, lease: BackupLease) -> Path:
        """Publish one complete restore-verified lease by its evidence digest."""
        self._ensure_store()
        _ensure_private_directory(self.backup_leases_root, "backup leases directory")
        path = self.backup_leases_root / f"{lease.evidence_digest}.json"
        try:
            path.lstat()
        except FileNotFoundError:
            return _publish_immutable(path, lease.to_dict())
        except OSError as exc:
            raise RequestStoreError("could not inspect backup lease") from exc
        if self.read_backup_lease(lease.evidence_digest) != lease:
            raise RequestStoreError("backup lease digest collision")
        return path

    def read_backup_lease(self, digest: str) -> BackupLease:
        if len(digest) != 64 or any(character not in "0123456789abcdef" for character in digest):
            raise RequestStoreError("backup lease digest is invalid")
        _validate_private_directory(self.root, "request store root")
        _validate_private_directory(self.backup_leases_root, "backup leases directory")
        try:
            lease = BackupLease.from_dict(
                _read_json(self.backup_leases_root / f"{digest}.json", "backup lease")
            )
        except ValueError as exc:
            raise RequestStoreError(str(exc)) from exc
        if lease.evidence_digest != digest:
            raise RequestStoreError("backup lease digest does not match its payload")
        return lease

    def read_backup_rotation(self) -> BackupRotationState:
        try:
            self.root.lstat()
        except FileNotFoundError:
            return BackupRotationState()
        _validate_private_directory(self.root, "request store root")
        try:
            payload = _read_json(self.backup_rotation_path, "backup rotation state")
        except RequestStoreError as exc:
            if "does not exist" in str(exc):
                return BackupRotationState()
            raise
        try:
            state = BackupRotationState.from_dict(payload)
        except ValueError as exc:
            raise RequestStoreError(str(exc)) from exc
        for record in (state.active, state.candidate):
            if record is not None and record.lease is not None:
                if self.read_backup_lease(record.lease.evidence_digest) != record.lease:
                    raise RequestStoreError("backup rotation lease authority drifted")
        return state

    def replace_backup_rotation(
        self,
        state: BackupRotationState,
        *,
        expected_generation: int,
    ) -> Path:
        """CAS-publish rotation before any returned payload deletion is applied."""
        if expected_generation < 0 or state.generation != expected_generation + 1:
            raise RequestStoreError("backup rotation generation is not the next value")
        self._ensure_store()
        flags = os.O_RDWR | os.O_CREAT | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
        try:
            fd = os.open(self._backup_rotation_lock_path, flags, _PRIVATE_FILE_MODE)
        except OSError as exc:
            raise RequestStoreError("could not open backup rotation lock") from exc
        locked = False
        try:
            os.fchmod(fd, _PRIVATE_FILE_MODE)
            metadata = os.fstat(fd)
            if not stat.S_ISREG(metadata.st_mode) or metadata.st_uid != os.geteuid():
                raise RequestStoreError("backup rotation lock is unsafe")
            fcntl.flock(fd, fcntl.LOCK_EX)
            locked = True
            current = self.read_backup_rotation()
            if current.generation != expected_generation:
                raise RequestStoreError("backup rotation changed concurrently")
            for record in (state.active, state.candidate):
                if record is not None and record.lease is not None:
                    if self.read_backup_lease(record.lease.evidence_digest) != record.lease:
                        raise RequestStoreError("backup rotation lease authority is unpublished")
            return _replace_mutable(self.backup_rotation_path, state.to_dict())
        except OSError as exc:
            raise RequestStoreError("backup rotation lock failed") from exc
        finally:
            if locked:
                with contextlib.suppress(OSError):
                    fcntl.flock(fd, fcntl.LOCK_UN)
            os.close(fd)

    def referenced_backup_payload_ids(self) -> frozenset[str]:
        """Return payloads referenced by the sole active rollout attempt."""
        active = self.read_active()
        if active is None:
            return frozenset()
        readers = (self.read_preflight_backup_job, self.read_backup_job)
        for reader in readers:
            try:
                envelope = reader(active.request_id)
            except RequestStoreError as exc:
                if "does not exist" in str(exc) or "not promoted" in str(exc):
                    continue
                raise
            return frozenset({envelope.payload_id})
        raise RequestStoreError("active rollout payload reference is missing")

    def read_backup_retention_claim(self) -> tuple[str, tuple[str, ...]] | None:
        """Return the exact durable maintenance claim blocking new references."""
        try:
            self.root.lstat()
        except FileNotFoundError:
            return None
        except OSError as exc:
            raise RequestStoreError("could not inspect request store root") from exc
        _validate_private_directory(self.root, "request store root")
        try:
            payload = _read_json(
                self.backup_retention_claim_path,
                "backup retention claim",
            )
        except RequestStoreError as exc:
            if "does not exist" in str(exc):
                return None
            raise
        if (
            set(payload) != {"plan_sha256", "retirement_payload_ids", "schema_version"}
            or payload["schema_version"] != 1
            or not isinstance(payload["plan_sha256"], str)
            or len(payload["plan_sha256"]) != 64
            or any(character not in "0123456789abcdef" for character in payload["plan_sha256"])
            or not isinstance(payload["retirement_payload_ids"], list)
            or not all(isinstance(item, str) for item in payload["retirement_payload_ids"])
        ):
            raise RequestStoreError("backup retention claim schema is invalid")
        payload_ids = tuple(
            validate_safe_identifier(item, "retirement_payload_id")
            for item in payload["retirement_payload_ids"]
        )
        if tuple(sorted(set(payload_ids))) != payload_ids:
            raise RequestStoreError("backup retention claim payload identities are invalid")
        return payload["plan_sha256"], payload_ids

    def claim_backup_retention(
        self,
        plan_sha256: str,
        retirement_payload_ids: tuple[str, ...],
    ) -> Path:
        """Atomically block active-pointer publication for one approved plan."""
        if len(plan_sha256) != 64 or any(
            character not in "0123456789abcdef" for character in plan_sha256
        ):
            raise RequestStoreError("backup retention plan digest is invalid")
        payload_ids = tuple(
            sorted(
                validate_safe_identifier(item, "retirement_payload_id")
                for item in retirement_payload_ids
            )
        )
        if len(set(payload_ids)) != len(payload_ids):
            raise RequestStoreError("backup retention claim payload identities are invalid")
        expected = (plan_sha256, payload_ids)
        self._ensure_store()
        with self._active_lock():
            try:
                self.active_path.lstat()
            except FileNotFoundError:
                pass
            except OSError as exc:
                raise RequestStoreError("could not inspect active.json") from exc
            else:
                raise RequestStoreError("active rollout blocks backup retention claim")
            existing = self.read_backup_retention_claim()
            if existing is not None:
                if existing != expected:
                    raise RequestStoreError("another backup retention claim is already active")
                return self.backup_retention_claim_path
            return _replace_mutable(
                self.backup_retention_claim_path,
                {
                    "plan_sha256": plan_sha256,
                    "retirement_payload_ids": list(payload_ids),
                    "schema_version": 1,
                },
            )

    def clear_backup_retention_claim(self, plan_sha256: str) -> bool:
        """Clear only the exact completed claim while still excluding launches."""
        with self._active_lock():
            existing = self.read_backup_retention_claim()
            if existing is None:
                return False
            if existing[0] != plan_sha256:
                raise RequestStoreError("backup retention claim identity does not match")
            try:
                self.backup_retention_claim_path.unlink()
                _fsync_directory(self.root)
            except OSError as exc:
                raise RequestStoreError("could not clear backup retention claim") from exc
            return True

    def read_preflight_artifact_retention_claim(
        self,
    ) -> tuple[str, tuple[str, ...]] | None:
        """Return the exact bounded artifact-retirement authority, if present."""
        try:
            self.root.lstat()
        except FileNotFoundError:
            return None
        except OSError as exc:
            raise RequestStoreError("could not inspect request store root") from exc
        _validate_private_directory(self.root, "request store root")
        try:
            payload = _read_json(
                self.preflight_artifact_retention_claim_path,
                "preflight artifact retention claim",
                require_single_link=True,
            )
        except RequestStoreError as exc:
            if str(exc) == "preflight artifact retention claim does not exist":
                return None
            raise
        bundle_values = payload.get("bundle_digests")
        plan_sha256 = payload.get("plan_sha256")
        if (
            set(payload) != {"bundle_digests", "plan_sha256", "schema_version"}
            or payload.get("schema_version") != 1
            or not _is_sha256(plan_sha256)
            or not isinstance(bundle_values, list)
            or len(bundle_values) > MAX_RETIREMENTS_PER_PLAN
            or not all(_is_sha256(item) for item in bundle_values)
        ):
            raise RequestStoreError("preflight artifact retention claim schema is invalid")
        bundle_digests = tuple(cast(list[str], bundle_values))
        if tuple(sorted(set(bundle_digests))) != bundle_digests:
            raise RequestStoreError("preflight artifact retention claim bundle digests are invalid")
        return cast(str, plan_sha256), bundle_digests

    def claim_preflight_artifact_retention(
        self,
        plan_sha256: str,
        bundle_digests: tuple[str, ...],
    ) -> Path:
        """Claim one exact bounded artifact-retirement plan under the active lock."""
        if not _is_sha256(plan_sha256):
            raise RequestStoreError("preflight artifact retention plan digest is invalid")
        if len(bundle_digests) > MAX_RETIREMENTS_PER_PLAN:
            raise RequestStoreError("preflight artifact retention permits at most 32 bundles")
        if not all(_is_sha256(item) for item in bundle_digests):
            raise RequestStoreError("preflight artifact retention bundle digests are invalid")
        canonical = tuple(sorted(bundle_digests))
        if len(set(canonical)) != len(canonical):
            raise RequestStoreError("preflight artifact retention bundle digests are invalid")
        expected = (plan_sha256, canonical)
        self._ensure_store()
        with self._active_lock():
            try:
                self.active_path.lstat()
            except FileNotFoundError:
                pass
            except OSError as exc:
                raise RequestStoreError("could not inspect active.json") from exc
            else:
                raise RequestStoreError("active rollout blocks preflight artifact retention claim")
            existing = self.read_preflight_artifact_retention_claim()
            if existing is not None:
                if existing != expected:
                    raise RequestStoreError(
                        "another preflight artifact retention claim is already active"
                    )
                return self.preflight_artifact_retention_claim_path
            return _replace_mutable(
                self.preflight_artifact_retention_claim_path,
                {
                    "bundle_digests": list(canonical),
                    "plan_sha256": plan_sha256,
                    "schema_version": 1,
                },
            )

    def clear_preflight_artifact_retention_claim(self, plan_sha256: str) -> bool:
        """Clear only the exact completed artifact claim under the active lock."""
        if not _is_sha256(plan_sha256):
            raise RequestStoreError("preflight artifact retention plan digest is invalid")
        with self._active_lock():
            existing = self.read_preflight_artifact_retention_claim()
            if existing is None:
                return False
            if existing[0] != plan_sha256:
                raise RequestStoreError(
                    "preflight artifact retention claim identity does not match"
                )
            try:
                self.preflight_artifact_retention_claim_path.unlink()
                _fsync_directory(self.root)
            except OSError as exc:
                raise RequestStoreError(
                    "could not clear preflight artifact retention claim"
                ) from exc
            return True

    def publish_preflight_artifact_retirement_receipt(
        self,
        bundle_digest: str,
        *,
        plan_sha256: str,
        inventory_record_sha256: str,
    ) -> Path:
        """Publish one immutable receipt bound to the exact plan and record."""
        self._validate_preflight_artifact_receipt_digests(
            bundle_digest,
            plan_sha256=plan_sha256,
            inventory_record_sha256=inventory_record_sha256,
        )
        self._ensure_store()
        _ensure_private_directory(
            self.preflight_artifact_retirements_root,
            "preflight artifact retirements directory",
        )
        path = self.preflight_artifact_retirements_root / f"{bundle_digest}.json"
        payload = {
            "bundle_digest": bundle_digest,
            "inventory_record_sha256": inventory_record_sha256,
            "plan_sha256": plan_sha256,
            "schema_version": 1,
        }
        try:
            path.lstat()
        except FileNotFoundError:
            pass
        except OSError as exc:
            raise RequestStoreError(
                "could not inspect preflight artifact retirement receipt"
            ) from exc
        else:
            _recover_exact_immutable_temp_link(
                path,
                expected_payload=_json_bytes(payload),
                object_name="preflight artifact retirement receipt",
            )
            self.read_preflight_artifact_retirement_receipt(
                bundle_digest,
                plan_sha256=plan_sha256,
                inventory_record_sha256=inventory_record_sha256,
            )
            return path
        try:
            return _publish_immutable(path, payload)
        except RequestStoreError as exc:
            if str(exc) != f"{bundle_digest}.json already exists":
                raise
            self.read_preflight_artifact_retirement_receipt(
                bundle_digest,
                plan_sha256=plan_sha256,
                inventory_record_sha256=inventory_record_sha256,
            )
            return path

    def read_preflight_artifact_retirement_receipt(
        self,
        bundle_digest: str,
        *,
        plan_sha256: str,
        inventory_record_sha256: str,
    ) -> bool:
        """Require an existing receipt to match its expected exact authority."""
        self._validate_preflight_artifact_receipt_digests(
            bundle_digest,
            plan_sha256=plan_sha256,
            inventory_record_sha256=inventory_record_sha256,
        )
        try:
            _validate_private_directory(self.root, "request store root")
            _validate_private_directory(
                self.preflight_artifact_retirements_root,
                "preflight artifact retirements directory",
            )
        except RequestStoreError as exc:
            if str(exc) in {
                "request store root does not exist",
                "preflight artifact retirements directory does not exist",
            }:
                return False
            raise
        path = self.preflight_artifact_retirements_root / f"{bundle_digest}.json"
        expected = {
            "bundle_digest": bundle_digest,
            "inventory_record_sha256": inventory_record_sha256,
            "plan_sha256": plan_sha256,
            "schema_version": 1,
        }
        try:
            path.lstat()
        except FileNotFoundError:
            return False
        except OSError as exc:
            raise RequestStoreError(
                "could not inspect preflight artifact retirement receipt"
            ) from exc
        _recover_exact_immutable_temp_link(
            path,
            expected_payload=_json_bytes(expected),
            object_name="preflight artifact retirement receipt",
        )
        try:
            payload = _read_json(
                path,
                "preflight artifact retirement receipt",
                require_single_link=True,
            )
        except RequestStoreError as exc:
            if str(exc) == "preflight artifact retirement receipt does not exist":
                return False
            raise
        if (
            set(payload) != set(expected)
            or payload.get("schema_version") != 1
            or not all(
                _is_sha256(payload.get(field))
                for field in (
                    "bundle_digest",
                    "inventory_record_sha256",
                    "plan_sha256",
                )
            )
        ):
            raise RequestStoreError("preflight artifact retirement receipt schema is invalid")
        if payload != expected:
            raise RequestStoreError("preflight artifact retirement receipt identity drifted")
        return True

    @staticmethod
    def _validate_preflight_artifact_receipt_digests(
        bundle_digest: str,
        *,
        plan_sha256: str,
        inventory_record_sha256: str,
    ) -> None:
        if not all(
            _is_sha256(value) for value in (bundle_digest, plan_sha256, inventory_record_sha256)
        ):
            raise RequestStoreError("preflight artifact retirement receipt digest is invalid")

    def resolve_backup_retirement(
        self,
        record: BackupRetirementRecord,
    ) -> BackupRetirementRecord:
        """Upgrade a legacy retirement from its immutable job authority."""
        if record.bundle_name is not None:
            return record
        readers = (
            (self.read_preflight_backup_job, self.read_preflight_backup_job_state),
            (self.read_backup_job, self.read_backup_job_state),
        )
        for read_job, read_state in readers:
            try:
                envelope = read_job(record.request_id)
                state = read_state(record.request_id)
            except RequestStoreError as exc:
                if "does not exist" in str(exc) or "not promoted" in str(exc):
                    continue
                raise
            if envelope.payload_id != record.payload_id:
                raise RequestStoreError("legacy retirement payload identity drifted")
            manifest_sha256 = record.manifest_sha256 or state.manifest_sha256
            if record.reason == "superseded" and manifest_sha256 is None:
                raise RequestStoreError("legacy retirement manifest authority is missing")
            return replace(
                record,
                bundle_name=envelope.bundle_name,
                manifest_sha256=manifest_sha256,
            )
        raise RequestStoreError("legacy retirement job authority is missing")

    def resolve_failed_retirement_active(
        self,
        record: BackupRetirementRecord,
    ) -> BackupPayloadRecord:
        """Recover exact active authority from one uniquely matching immutable lease."""
        record = self.resolve_backup_retirement(record)
        if (
            record.reason != "failed"
            or record.bundle_name is None
            or record.manifest_sha256 is None
        ):
            raise RequestStoreError("failed retirement is not recoverable")
        try:
            _validate_private_directory(self.root, "request store root")
            _validate_private_directory(self.backup_leases_root, "backup leases directory")
            with os.scandir(self.backup_leases_root) as entries:
                names = tuple(sorted(entry.name for entry in entries))
        except (FileNotFoundError, OSError) as exc:
            raise RequestStoreError("failed retirement lease authority is missing") from exc
        leases: list[BackupLease] = []
        for name in names:
            if (
                len(name) != 69
                or not name.endswith(".json")
                or any(character not in "0123456789abcdef" for character in name[:64])
            ):
                raise RequestStoreError("backup lease directory contains unsafe authority")
            lease = self.read_backup_lease(name[:64])
            if (
                lease.source_request_id == record.request_id
                and lease.manifest_sha256 == record.manifest_sha256
            ):
                leases.append(lease)
        if len(leases) != 1:
            raise RequestStoreError("failed retirement requires one exact backup lease")
        lease = leases[0]
        if lease.environment != "staging" or lease.namespace != "loom-staging":
            raise RequestStoreError("failed retirement lease authority is outside staging")
        return BackupPayloadRecord(
            payload_id=record.payload_id,
            request_id=record.request_id,
            bundle_name=record.bundle_name,
            phase=BackupPayloadPhase.ACTIVE,
            created_at=lease.created_at,
            manifest_sha256=record.manifest_sha256,
            lease=lease,
        )

    def publish_backup_retirement_evidence(
        self,
        record: BackupRetirementRecord,
        *,
        manifest_path: Path | None,
    ) -> Path:
        """Persist compact exact evidence before deleting a large payload."""
        self._ensure_store()
        _ensure_private_directory(
            self.backup_retirements_root,
            "backup retirements directory",
        )
        path = self.backup_retirements_root / f"{record.payload_id}.json"
        try:
            path.lstat()
        except FileNotFoundError:
            pass
        else:
            existing = _read_json(path, "backup retirement evidence")
            if existing.get("record") != record.to_dict():
                raise RequestStoreError("backup retirement evidence identity drifted")
            return path
        manifest_size: int | None = None
        if record.manifest_sha256 is None:
            if manifest_path is not None:
                raise RequestStoreError("incomplete retirement cannot bind a manifest")
        else:
            if manifest_path is None or not manifest_path.is_absolute():
                raise RequestStoreError("manifest retirement evidence path is invalid")
            flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
            try:
                fd = os.open(manifest_path, flags)
            except OSError as exc:
                raise RequestStoreError("could not open retirement manifest evidence") from exc
            try:
                metadata = os.fstat(fd)
                if (
                    not stat.S_ISREG(metadata.st_mode)
                    or metadata.st_uid != os.geteuid()
                    or stat.S_IMODE(metadata.st_mode) != _PRIVATE_FILE_MODE
                ):
                    raise RequestStoreError("retirement manifest evidence is unsafe")
                digest = hashlib.sha256()
                while chunk := os.read(fd, 1024 * 1024):
                    digest.update(chunk)
                manifest_size = metadata.st_size
            finally:
                os.close(fd)
            if digest.hexdigest() != record.manifest_sha256:
                raise RequestStoreError("retirement manifest evidence digest drifted")
        payload: dict[str, object] = {
            "manifest_size": manifest_size,
            "record": record.to_dict(),
            "schema_version": 1,
        }
        return _publish_immutable(path, payload)

    def publish_backup_retirement_receipt(self, payload_id: str) -> Path:
        """Record exact payload absence after idempotent retirement."""
        validate_safe_identifier(payload_id, "payload_id")
        _validate_private_directory(
            self.backup_retirements_root,
            "backup retirements directory",
        )
        evidence_path = self.backup_retirements_root / f"{payload_id}.json"
        evidence = _read_json(evidence_path, "backup retirement evidence")
        payload = {
            "evidence_sha256": hashlib.sha256(_json_bytes(evidence)).hexdigest(),
            "payload_id": payload_id,
            "schema_version": 1,
        }
        path = self.backup_retirements_root / f"{payload_id}.deleted.json"
        try:
            path.lstat()
        except FileNotFoundError:
            return _publish_immutable(path, payload)
        if _read_json(path, "backup retirement receipt") != payload:
            raise RequestStoreError("backup retirement receipt identity drifted")
        return path

    def has_backup_retirement_receipt(self, payload_id: str) -> bool:
        """Return true only for a complete digest-bound retirement receipt."""
        validate_safe_identifier(payload_id, "payload_id")
        try:
            _validate_private_directory(
                self.backup_retirements_root,
                "backup retirements directory",
            )
        except RequestStoreError as exc:
            if "does not exist" in str(exc):
                return False
            raise
        evidence_path = self.backup_retirements_root / f"{payload_id}.json"
        receipt_path = self.backup_retirements_root / f"{payload_id}.deleted.json"
        try:
            evidence = _read_json(evidence_path, "backup retirement evidence")
            receipt = _read_json(receipt_path, "backup retirement receipt")
        except RequestStoreError as exc:
            if "does not exist" in str(exc):
                return False
            raise
        expected = {
            "evidence_sha256": hashlib.sha256(_json_bytes(evidence)).hexdigest(),
            "payload_id": payload_id,
            "schema_version": 1,
        }
        if receipt != expected:
            raise RequestStoreError("backup retirement receipt identity drifted")
        return True

    def _request_directory(self, request_id: object) -> Path:
        return self.requests_root / _request_id(request_id)

    def _validate_request_roots(self) -> None:
        try:
            _validate_private_directory(self.root, "request store root")
            _validate_private_directory(self.requests_root, "requests directory")
        except RequestStoreError as exc:
            if str(exc) in {
                "request store root does not exist",
                "requests directory does not exist",
            }:
                raise RequestStoreError("request does not exist") from exc
            raise

    def _require_request_directory(self, request_id: object) -> Path:
        request_directory = self._require_record_directory(request_id)
        try:
            _read_private_bytes(request_directory / "request.json", "request.json")
        except RequestStoreError as exc:
            if "does not exist" in str(exc):
                raise RequestStoreError("rollout request is not promoted") from exc
            raise
        return request_directory

    def _require_record_directory(self, request_id: object) -> Path:
        validated_request_id = _request_id(request_id)
        self._validate_request_roots()
        request_directory = self.requests_root / validated_request_id
        try:
            _validate_private_directory(request_directory, "request directory")
        except RequestStoreError as exc:
            if "does not exist" in str(exc):
                raise RequestStoreError("request does not exist") from exc
            raise
        for name in ("request.json", "preflight.json"):
            try:
                _read_private_bytes(request_directory / name, name)
            except RequestStoreError as exc:
                if "does not exist" not in str(exc):
                    raise
            else:
                break
        else:
            raise RequestStoreError("request identity document does not exist")
        return request_directory

    def _require_preflight_request_directory(self, request_id: object) -> Path:
        request_directory = self._require_record_directory(request_id)
        try:
            _read_private_bytes(request_directory / "preflight.json", "preflight.json")
        except RequestStoreError as exc:
            if "does not exist" in str(exc):
                raise RequestStoreError("preflight request does not exist") from exc
            raise
        return request_directory

    def create_preflight_request(self, request: PreflightRequest) -> Path:
        """Reserve one immutable Tier 0-2 request before detached backup I/O."""
        try:
            payload = request.to_dict()
        except ValueError as exc:
            raise RequestStoreError(str(exc)) from exc
        self._ensure_store()
        request_directory = self._request_directory(request.request_id)
        try:
            request_directory.mkdir(mode=_PRIVATE_DIRECTORY_MODE)
        except FileExistsError as exc:
            raise RequestStoreError(f"request {request.request_id} already exists") from exc
        except OSError as exc:
            raise RequestStoreError("could not create request directory") from exc
        request_directory.chmod(_PRIVATE_DIRECTORY_MODE)
        _validate_private_directory(request_directory, "request directory")
        _fsync_directory(self.requests_root)
        return _publish_immutable(request_directory / "preflight.json", payload)

    def read_preflight_request(self, request_id: str) -> PreflightRequest:
        request_directory = self._require_preflight_request_directory(request_id)
        payload = _read_json(request_directory / "preflight.json", "preflight.json")
        try:
            request = PreflightRequest.from_dict(payload)
        except ValueError as exc:
            raise RequestStoreError(str(exc)) from exc
        if request.request_id != request_id:
            raise RequestStoreError("preflight.json request_id does not match its directory")
        return request

    def publish_preflight_assessment(
        self,
        request_id: str,
        assessment: PreflightAssessment,
    ) -> Path:
        """Publish complete Tier 0-2 evidence after its identity is reserved."""
        preliminary = self.read_preflight_request(request_id)
        if (
            not assessment.passed
            or assessment.assessment_digest != preliminary.preflight_assessment_sha256
            or assessment.registry_digest != preliminary.preflight_registry_sha256
            or assessment.coverage_digest != preliminary.preflight_coverage_sha256
        ):
            raise RequestStoreError("preflight assessment does not match request authority")
        return _publish_immutable(
            self._require_preflight_request_directory(request_id) / "assessment.json",
            assessment.to_record(),
        )

    def read_preflight_assessment(self, request_id: str) -> PreflightAssessment:
        from loom_cli.rollout.preflight_pipeline import PreflightAssessment

        directory = self._require_preflight_request_directory(request_id)
        try:
            assessment = PreflightAssessment.from_record(
                _read_json(directory / "assessment.json", "preflight assessment")
            )
        except ValueError as exc:
            raise RequestStoreError(str(exc)) from exc
        preliminary = self.read_preflight_request(request_id)
        if (
            assessment.assessment_digest != preliminary.preflight_assessment_sha256
            or assessment.registry_digest != preliminary.preflight_registry_sha256
            or assessment.coverage_digest != preliminary.preflight_coverage_sha256
        ):
            raise RequestStoreError("preflight assessment does not match request authority")
        return assessment

    def publish_preflight_rehearsal(
        self,
        request_id: str,
        rehearsal: PreflightRehearsal,
    ) -> Path:
        """Publish exact Tier 0-3 rehearsal evidence before lease attestation."""
        preliminary = self.read_preflight_request(request_id)
        if (
            not rehearsal.passed
            or rehearsal.registry_digest != preliminary.preflight_registry_sha256
            or rehearsal.coverage_digest != preliminary.preflight_coverage_sha256
        ):
            raise RequestStoreError("preflight rehearsal does not match request authority")
        directory = self._require_preflight_backup_job_directory(request_id)
        return _publish_immutable(directory / "rehearsal.json", rehearsal.to_record())

    def read_preflight_rehearsal(self, request_id: str) -> PreflightRehearsal:
        from loom_cli.rollout.preflight_pipeline import PreflightRehearsal

        directory = self._require_preflight_backup_job_directory(request_id)
        try:
            rehearsal = PreflightRehearsal.from_record(
                _read_json(directory / "rehearsal.json", "preflight rehearsal")
            )
        except ValueError as exc:
            raise RequestStoreError(str(exc)) from exc
        preliminary = self.read_preflight_request(request_id)
        if (
            rehearsal.registry_digest != preliminary.preflight_registry_sha256
            or rehearsal.coverage_digest != preliminary.preflight_coverage_sha256
        ):
            raise RequestStoreError("preflight rehearsal does not match request authority")
        return rehearsal

    def promote_preflight_request(self, request: RolloutRequest) -> Path:
        """Publish final Tier 0-3 authority without replacing preliminary evidence."""
        preliminary = self.read_preflight_request(request.request_id)
        if preliminary.status != "pending" or request.status != "pending":
            raise RequestStoreError("only pending preflight requests may be promoted")
        if (
            request.request_id != preliminary.request_id
            or request.rollout_id != preliminary.rollout_id
            or request.caller != preliminary.caller
            or request.candidate != preliminary.candidate
            or request.requested_at != preliminary.requested_at
            or request.runner_config_sha256 != preliminary.runner_config_sha256
            or request.preflight_registry_sha256 != preliminary.preflight_registry_sha256
            or request.preflight_coverage_sha256 != preliminary.preflight_coverage_sha256
        ):
            raise RequestStoreError("promoted request does not match preflight authority")
        request_directory = self._require_preflight_request_directory(request.request_id)
        return _publish_immutable(request_directory / "request.json", request.to_dict())

    def create_request(self, request: RolloutRequest) -> Path:
        """Persist the immutable pre-backup request without replacement."""
        try:
            payload = request.to_dict()
        except ValueError as exc:
            raise RequestStoreError(str(exc)) from exc
        self._ensure_store()
        request_directory = self._request_directory(request.request_id)
        try:
            request_directory.mkdir(mode=_PRIVATE_DIRECTORY_MODE)
        except FileExistsError as exc:
            raise RequestStoreError(f"request {request.request_id} already exists") from exc
        except OSError as exc:
            raise RequestStoreError("could not create request directory") from exc
        try:
            request_directory.chmod(_PRIVATE_DIRECTORY_MODE)
            _validate_private_directory(request_directory, "request directory")
            _fsync_directory(self.requests_root)
            return _publish_immutable(request_directory / "request.json", payload)
        except Exception:
            # Keep a published or possibly crash-reserved directory. IDs are never reused.
            raise

    def read_request(self, request_id: str) -> RolloutRequest:
        request_directory = self._require_request_directory(request_id)
        payload = _read_json(request_directory / "request.json", "request.json")
        try:
            request = RolloutRequest.from_dict(payload)
        except ValueError as exc:
            raise RequestStoreError(str(exc)) from exc
        if request.request_id != request_id:
            raise RequestStoreError("request.json request_id does not match its directory")
        return request

    def publish_backup_job(self, envelope: BackupJobEnvelope) -> Path:
        """Publish one immutable detached-backup job under its request."""
        request = self.read_request(envelope.request_id)
        if request.status != "pending":
            raise RequestStoreError("preview request cannot publish a backup job")
        if (
            request.candidate.resolved_sha != envelope.candidate_sha
            or request.candidate.resolved_tree != envelope.candidate_tree
            or request.preflight_attestation_sha256 != envelope.preflight_attestation_sha256
        ):
            raise RequestStoreError("backup job does not match immutable request binding")
        request_directory = self._require_request_directory(envelope.request_id)
        backup_directory = request_directory / "backup"
        _ensure_private_directory(backup_directory, "backup job directory")
        path = _publish_immutable(backup_directory / "job.json", envelope.to_dict())
        initial = BackupJobState(
            job_id=envelope.job_id,
            request_id=envelope.request_id,
            updated_at=envelope.created_at,
        )
        try:
            _publish_immutable(backup_directory / "state.json", initial.to_dict())
        except Exception:
            # Keep the immutable job. Reconciliation may safely initialize a
            # missing state file, but the job identity is never reused.
            raise
        return path

    def publish_preflight_backup_job(self, envelope: PreflightBackupJobEnvelope) -> Path:
        """Publish one detached job bound to immutable Tier 0-2 authority."""
        request = self.read_preflight_request(envelope.request_id)
        assessment = self.read_preflight_assessment(envelope.request_id)
        if request.status != "pending":
            raise RequestStoreError("preview request cannot publish a backup job")
        if (
            request.candidate.resolved_sha != envelope.candidate_sha
            or request.candidate_tree != envelope.candidate_tree
            or request.preflight_assessment_sha256 != envelope.preflight_assessment_sha256
            or assessment.assessment_digest != envelope.preflight_assessment_sha256
            or request.preflight_registry_sha256 != envelope.preflight_registry_sha256
            or request.preflight_coverage_sha256 != envelope.preflight_coverage_sha256
            or request.mutation_epoch != envelope.mutation_epoch
            or request.environment != envelope.environment
            or request.namespace != envelope.namespace
        ):
            raise RequestStoreError("backup job does not match preflight request binding")
        request_directory = self._require_preflight_request_directory(envelope.request_id)
        backup_directory = request_directory / "preflight-backup"
        _ensure_private_directory(backup_directory, "preflight backup job directory")
        path = _publish_immutable(backup_directory / "job.json", envelope.to_dict())
        initial = BackupJobState(
            job_id=envelope.job_id,
            request_id=envelope.request_id,
            updated_at=envelope.created_at,
        )
        _publish_immutable(backup_directory / "state.json", initial.to_dict())
        return path

    def read_preflight_backup_job(self, request_id: str) -> PreflightBackupJobEnvelope:
        directory = self._require_preflight_backup_job_directory(request_id)
        try:
            envelope = PreflightBackupJobEnvelope.from_dict(
                _read_json(directory / "job.json", "preflight backup job envelope")
            )
        except ValueError as exc:
            raise RequestStoreError(str(exc)) from exc
        if envelope.request_id != request_id:
            raise RequestStoreError("preflight backup job identity does not match directory")
        return envelope

    def read_preflight_backup_job_state(self, request_id: str) -> BackupJobState:
        directory = self._require_preflight_backup_job_directory(request_id)
        try:
            state = BackupJobState.from_dict(
                _read_json(directory / "state.json", "preflight backup job state")
            )
            validate_job_binding(self.read_preflight_backup_job(request_id), state)
        except ValueError as exc:
            raise RequestStoreError(str(exc)) from exc
        return state

    def replace_preflight_backup_job_state(
        self,
        state: BackupJobState,
        *,
        expected_sequence: int,
    ) -> Path:
        """CAS-update preflight backup state without holding the launch lock."""
        directory = self._require_preflight_backup_job_directory(state.request_id)
        return self._replace_job_state(
            directory,
            state,
            expected_sequence=expected_sequence,
            current=lambda: self.read_preflight_backup_job_state(state.request_id),
            envelope=lambda: self.read_preflight_backup_job(state.request_id),
        )

    def read_backup_job(self, request_id: str) -> BackupJobEnvelope:
        directory = self._require_backup_job_directory(request_id)
        try:
            envelope = BackupJobEnvelope.from_dict(
                _read_json(directory / "job.json", "backup job envelope")
            )
        except ValueError as exc:
            raise RequestStoreError(str(exc)) from exc
        if envelope.request_id != request_id:
            raise RequestStoreError("backup job request identity does not match directory")
        return envelope

    def read_backup_job_state(self, request_id: str) -> BackupJobState:
        directory = self._require_backup_job_directory(request_id)
        try:
            state = BackupJobState.from_dict(
                _read_json(directory / "state.json", "backup job state")
            )
            validate_job_binding(self.read_backup_job(request_id), state)
        except ValueError as exc:
            raise RequestStoreError(str(exc)) from exc
        return state

    def replace_backup_job_state(
        self,
        state: BackupJobState,
        *,
        expected_sequence: int,
    ) -> Path:
        """Atomically publish a single-writer state transition with CAS."""
        directory = self._require_backup_job_directory(state.request_id)
        return self._replace_job_state(
            directory,
            state,
            expected_sequence=expected_sequence,
            current=lambda: self.read_backup_job_state(state.request_id),
            envelope=lambda: self.read_backup_job(state.request_id),
        )

    def _replace_job_state(
        self,
        directory: Path,
        state: BackupJobState,
        *,
        expected_sequence: int,
        current: Callable[[], BackupJobState],
        envelope: Callable[[], BackupJobEnvelope | PreflightBackupJobEnvelope],
    ) -> Path:
        if expected_sequence < 0 or state.sequence != expected_sequence + 1:
            raise RequestStoreError("backup job state sequence is not the next generation")
        lock_path = directory / ".state.lock"
        flags = os.O_RDWR | os.O_CREAT | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
        try:
            fd = os.open(lock_path, flags, _PRIVATE_FILE_MODE)
        except OSError as exc:
            raise RequestStoreError("could not open backup job state lock") from exc
        locked = False
        try:
            os.fchmod(fd, _PRIVATE_FILE_MODE)
            metadata = os.fstat(fd)
            if not stat.S_ISREG(metadata.st_mode) or metadata.st_uid != os.geteuid():
                raise RequestStoreError("backup job state lock is unsafe")
            fcntl.flock(fd, fcntl.LOCK_EX)
            locked = True
            current_state = current()
            if current_state.sequence != expected_sequence:
                raise RequestStoreError("backup job state changed concurrently")
            try:
                validate_job_binding(envelope(), state)
            except ValueError as exc:
                raise RequestStoreError(str(exc)) from exc
            return _replace_mutable(directory / "state.json", state.to_dict())
        except OSError as exc:
            raise RequestStoreError("backup job state lock failed") from exc
        finally:
            if locked:
                with contextlib.suppress(OSError):
                    fcntl.flock(fd, fcntl.LOCK_UN)
            os.close(fd)

    def _require_backup_job_directory(self, request_id: str) -> Path:
        request_directory = self._require_request_directory(request_id)
        directory = request_directory / "backup"
        _validate_private_directory(directory, "backup job directory")
        return directory

    def _require_preflight_backup_job_directory(self, request_id: str) -> Path:
        request_directory = self._require_preflight_request_directory(request_id)
        directory = request_directory / "preflight-backup"
        _validate_private_directory(directory, "preflight backup job directory")
        return directory

    def publish_attempt_envelope(self, envelope: DriverEnvelope) -> Path:
        """Publish the immutable post-backup driver envelope without replacement."""
        try:
            payload = envelope.to_dict()
        except ValueError as exc:
            raise RequestStoreError(str(exc)) from exc
        request = self.read_request(envelope.request_id)
        if request.status == "preview":
            raise RequestStoreError("preview request cannot publish a driver envelope")
        request_binding = {
            "request_id": request.request_id,
            "rollout_id": request.rollout_id,
            "initiating_operator": request.caller.username,
            "initiating_uid": request.caller.uid,
            "remote_url": request.candidate.remote_url,
            "target_ref": request.candidate.target_ref,
            "resolved_sha": request.candidate.resolved_sha,
            "image_tag": request.candidate.image_tag,
            "fetched_at": request.candidate.fetched_at,
            "source_mode": request.candidate.source_mode,
            "resolved_tree": request.candidate.resolved_tree,
            "approved_base_sha": request.candidate.approved_base_sha,
            "runner_config_sha256": request.runner_config_sha256,
            "preflight_attestation_sha256": request.preflight_attestation_sha256,
            "preflight_registry_sha256": request.preflight_registry_sha256,
            "preflight_coverage_sha256": request.preflight_coverage_sha256,
        }
        envelope_binding = {
            "request_id": envelope.request_id,
            "rollout_id": envelope.rollout_id,
            "initiating_operator": envelope.initiating_operator,
            "initiating_uid": envelope.initiating_uid,
            "remote_url": envelope.remote_url,
            "target_ref": envelope.target_ref,
            "resolved_sha": envelope.resolved_sha,
            "image_tag": envelope.image_tag,
            "fetched_at": envelope.fetched_at,
            "source_mode": envelope.source_mode,
            "resolved_tree": envelope.resolved_tree,
            "approved_base_sha": envelope.approved_base_sha,
            "runner_config_sha256": envelope.runner_config_sha256,
            "preflight_attestation_sha256": envelope.preflight_attestation_sha256,
            "preflight_registry_sha256": envelope.preflight_registry_sha256,
            "preflight_coverage_sha256": envelope.preflight_coverage_sha256,
        }
        if envelope_binding != request_binding:
            raise RequestStoreError("driver envelope does not match immutable request binding")

        if envelope.attempt_number > 1:
            try:
                first = self.read_attempt_envelope(envelope.request_id, 1)
            except RequestStoreError as exc:
                if "does not exist" in str(exc):
                    raise RequestStoreError("first attempt envelope does not exist") from exc
                raise
            first_binding = first.to_dict()
            new_binding = envelope.to_dict()
            for attribution_field in (
                "attempt_number",
                "attempt_operator",
                "attempt_uid",
                "resume",
            ):
                first_binding.pop(attribution_field)
                new_binding.pop(attribution_field)
            if new_binding != first_binding:
                raise RequestStoreError("driver envelope does not match first attempt binding")

        request_directory = self._require_request_directory(envelope.request_id)
        attempts_directory = request_directory / "attempts"
        _ensure_private_directory(attempts_directory, "attempts directory")
        attempt_directory = attempts_directory / str(_attempt_number(envelope.attempt_number))
        _ensure_private_directory(attempt_directory, "attempt directory")
        return _publish_immutable(attempt_directory / "envelope.json", payload)

    def read_attempt_envelope(
        self,
        request_id: str,
        attempt_number: int,
    ) -> DriverEnvelope:
        number = _attempt_number(attempt_number)
        request_directory = self._require_request_directory(request_id)
        attempts_directory = request_directory / "attempts"
        _validate_private_directory(attempts_directory, "attempts directory")
        attempt_directory = attempts_directory / str(number)
        _validate_private_directory(attempt_directory, "attempt directory")
        path = attempt_directory / "envelope.json"
        payload = _read_json(path, "envelope.json")
        try:
            envelope = DriverEnvelope.from_dict(payload)
        except ValueError as exc:
            raise RequestStoreError(str(exc)) from exc
        if envelope.request_id != request_id:
            raise RequestStoreError("envelope.json request_id does not match its directory")
        if envelope.attempt_number != number:
            raise RequestStoreError("envelope.json attempt_number does not match its directory")
        return envelope

    def next_attempt_number(self, request_id: str) -> int:
        request_directory = self._require_request_directory(request_id)
        attempts_directory = request_directory / "attempts"
        try:
            attempts_directory.lstat()
        except FileNotFoundError:
            return 1
        except OSError as exc:
            raise RequestStoreError("could not inspect attempts directory") from exc
        _validate_private_directory(attempts_directory, "attempts directory")
        highest = 0
        try:
            entries = list(os.scandir(attempts_directory))
        except OSError as exc:
            raise RequestStoreError("could not inspect attempts directory") from exc
        for entry in entries:
            if not entry.name.isdecimal():
                continue
            number = int(entry.name)
            if number < 1 or not entry.is_dir(follow_symlinks=False):
                raise RequestStoreError("attempt directory contains an unsafe numeric entry")
            highest = max(highest, number)
        return highest + 1

    @contextmanager
    def _active_lock(self) -> Iterator[None]:
        self._ensure_store()
        flags = os.O_RDWR | os.O_CREAT | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
        try:
            fd = os.open(self._active_lock_path, flags, _PRIVATE_FILE_MODE)
        except OSError as exc:
            raise RequestStoreError("could not open active pointer lock") from exc
        locked = False
        try:
            os.fchmod(fd, _PRIVATE_FILE_MODE)
            metadata = os.fstat(fd)
            if not stat.S_ISREG(metadata.st_mode) or metadata.st_uid != os.geteuid():
                raise RequestStoreError("active pointer lock is not a service-owned regular file")
            fcntl.flock(fd, fcntl.LOCK_EX)
            locked = True
            yield
        except OSError as exc:
            raise RequestStoreError("active pointer lock failed") from exc
        finally:
            if locked:
                with contextlib.suppress(OSError):
                    fcntl.flock(fd, fcntl.LOCK_UN)
            os.close(fd)

    def set_active(self, pointer: ActivePointer) -> Path:
        try:
            payload = pointer.to_dict()
        except ValueError as exc:
            raise RequestStoreError(str(exc)) from exc
        with self._active_lock():
            if self.read_backup_retention_claim() is not None:
                raise RequestStoreError("backup retention maintenance blocks active publication")
            if self.read_preflight_artifact_retention_claim() is not None:
                raise RequestStoreError(
                    "preflight artifact retention maintenance blocks active publication"
                )
            try:
                self.active_path.lstat()
            except FileNotFoundError:
                pass
            except OSError as exc:
                raise RequestStoreError("could not inspect active.json") from exc
            else:
                current = self._read_active_existing()
                current_attempt = (
                    current.request_id,
                    current.attempt_number,
                    current.unit_name,
                )
                requested_attempt = (
                    pointer.request_id,
                    pointer.attempt_number,
                    pointer.unit_name,
                )
                if current_attempt != requested_attempt:
                    raise RequestStoreError("active pointer already belongs to another attempt")
            return _replace_mutable(self.active_path, payload)

    def clear_active_if_matches(self, pointer: ActivePointer) -> bool:
        # Validate before comparing to persisted state.
        pointer.to_dict()
        with self._active_lock():
            try:
                self.active_path.lstat()
            except FileNotFoundError:
                return False
            except OSError as exc:
                raise RequestStoreError("could not inspect active.json") from exc
            current = self._read_active_existing()
            if current != pointer:
                return False
            try:
                self.active_path.unlink()
                _fsync_directory(self.root)
            except OSError as exc:
                raise RequestStoreError("could not clear matching active pointer") from exc
            return True

    def _read_active_existing(self) -> ActivePointer:
        payload = _read_json(self.active_path, "active.json")
        try:
            return ActivePointer.from_dict(payload)
        except ValueError as exc:
            raise RequestStoreError(str(exc)) from exc

    def read_active(self) -> ActivePointer | None:
        try:
            self.root.lstat()
        except FileNotFoundError:
            return None
        except OSError as exc:
            raise RequestStoreError("could not inspect request store root") from exc
        _validate_private_directory(self.root, "request store root")
        try:
            self.active_path.lstat()
        except FileNotFoundError:
            return None
        except OSError as exc:
            raise RequestStoreError("could not inspect active.json") from exc
        return self._read_active_existing()

    def append_event(self, event: RequestEvent) -> Path:
        """Append one complete JSON line while holding the request event flock."""
        try:
            payload = _json_bytes(event.to_dict())
        except ValueError as exc:
            raise RequestStoreError(str(exc)) from exc
        request_directory = self._require_record_directory(event.request_id)
        path = request_directory / "events.jsonl"
        flags = (
            os.O_WRONLY
            | os.O_APPEND
            | os.O_CREAT
            | getattr(os, "O_CLOEXEC", 0)
            | getattr(os, "O_NOFOLLOW", 0)
        )
        try:
            fd = os.open(path, flags, _PRIVATE_FILE_MODE)
        except OSError as exc:
            raise RequestStoreError("could not open events.jsonl for append") from exc
        locked = False
        try:
            metadata = os.fstat(fd)
            if not stat.S_ISREG(metadata.st_mode) or metadata.st_uid != os.geteuid():
                raise RequestStoreError("events.jsonl must be a service-owned regular file")
            if stat.S_IMODE(metadata.st_mode) != _PRIVATE_FILE_MODE:
                raise RequestStoreError("events.jsonl must have mode 0600")
            fcntl.flock(fd, fcntl.LOCK_EX)
            locked = True
            try:
                written = os.write(fd, payload)
                if written != len(payload):
                    raise RequestStoreError("events.jsonl append was incomplete")
                os.fsync(fd)
            except OSError as exc:
                raise RequestStoreError("could not append and fsync events.jsonl") from exc
        finally:
            if locked:
                with contextlib.suppress(OSError):
                    fcntl.flock(fd, fcntl.LOCK_UN)
            os.close(fd)
        _fsync_directory(request_directory)
        return path

    def read_events(self, request_id: str) -> list[RequestEvent]:
        request_directory = self._require_record_directory(request_id)
        path = request_directory / "events.jsonl"
        try:
            path.lstat()
        except FileNotFoundError:
            return []
        except OSError as exc:
            raise RequestStoreError("could not inspect events.jsonl") from exc
        payload = _read_private_bytes(
            path,
            "events.jsonl",
            lock_operation=fcntl.LOCK_SH,
        )
        if not payload:
            return []
        events: list[RequestEvent] = []
        for line_number, line in enumerate(payload.splitlines(), start=1):
            try:
                raw = _load_json_object(line, f"events.jsonl line {line_number}")
                event = RequestEvent.from_dict(raw)
            except (RequestStoreError, ValueError) as exc:
                raise RequestStoreError(f"events.jsonl line {line_number}: {exc}") from exc
            if event.request_id != request_id:
                raise RequestStoreError(
                    f"events.jsonl line {line_number}: request_id does not match its directory"
                )
            events.append(event)
        return events


__all__ = ["RequestStore", "RequestStoreError"]
