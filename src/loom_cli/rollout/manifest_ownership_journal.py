"""Private no-replace evidence journal for manifest ownership maintenance."""

from __future__ import annotations

import contextlib
import fcntl
import json
import os
import re
import stat
from collections.abc import Mapping
from pathlib import Path
from uuid import uuid4

_REQUEST_RE = re.compile(r"^req-manifest-ownership-[a-z0-9]{8,32}$")
_PRIVATE_DIRECTORY_MODE = 0o700
_PRIVATE_FILE_MODE = 0o600
_MAX_RECORD_BYTES = 64 * 1024


class ManifestOwnershipJournalError(RuntimeError):
    """Raised when ownership-maintenance evidence cannot be persisted safely."""


class ManifestOwnershipJournal:
    """Persist one immutable inventory and append-only transition journal."""

    def __init__(self, state_root: Path | str, *, service_uid: int) -> None:
        self.state_root = Path(state_root)
        self.root = self.state_root / "maintenance" / "manifest-ownership"
        self.service_uid = service_uid
        if not self.state_root.is_absolute() or ".." in self.state_root.parts or service_uid < 1:
            raise ManifestOwnershipJournalError("ownership journal authority is invalid")

    def publish_inventory(
        self,
        request_id: str,
        inventory: Mapping[str, object],
    ) -> None:
        directory = self._create_request_directory(request_id)
        payload = _json_bytes({"request_id": request_id, **dict(inventory)})
        _publish_no_replace(
            directory / "inventory.json",
            payload,
            service_uid=self.service_uid,
        )

    def append(self, request_id: str, event: Mapping[str, object]) -> None:
        directory = self._require_request_directory(request_id)
        inventory = directory / "inventory.json"
        _validate_file(inventory, service_uid=self.service_uid, allow_multiple_links=False)
        payload = _json_bytes({"request_id": request_id, **dict(event)})
        path = directory / "events.jsonl"
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
            raise ManifestOwnershipJournalError("ownership events journal is unavailable") from exc
        locked = False
        try:
            metadata = os.fstat(fd)
            if (
                not stat.S_ISREG(metadata.st_mode)
                or metadata.st_uid != self.service_uid
                or metadata.st_nlink != 1
                or stat.S_IMODE(metadata.st_mode) != _PRIVATE_FILE_MODE
            ):
                raise ManifestOwnershipJournalError("ownership events journal is unsafe")
            fcntl.flock(fd, fcntl.LOCK_EX)
            locked = True
            if os.write(fd, payload) != len(payload):
                raise ManifestOwnershipJournalError("ownership event append was incomplete")
            os.fsync(fd)
        except OSError as exc:
            raise ManifestOwnershipJournalError("ownership event append failed") from exc
        finally:
            if locked:
                with contextlib.suppress(OSError):
                    fcntl.flock(fd, fcntl.LOCK_UN)
            os.close(fd)
        _fsync_directory(directory, service_uid=self.service_uid)

    def _create_request_directory(self, request_id: str) -> Path:
        _validate_request_id(request_id)
        _validate_directory(self.state_root, service_uid=self.service_uid)
        maintenance = self.state_root / "maintenance"
        _ensure_directory(maintenance, service_uid=self.service_uid)
        _ensure_directory(self.root, service_uid=self.service_uid)
        request = self.root / request_id
        try:
            request.mkdir(mode=_PRIVATE_DIRECTORY_MODE)
        except FileExistsError as exc:
            raise ManifestOwnershipJournalError(
                "ownership maintenance request already exists"
            ) from exc
        except OSError as exc:
            raise ManifestOwnershipJournalError(
                "ownership maintenance request cannot be created"
            ) from exc
        try:
            os.chmod(request, _PRIVATE_DIRECTORY_MODE, follow_symlinks=False)
            _validate_directory(request, service_uid=self.service_uid)
            _fsync_directory(self.root, service_uid=self.service_uid)
        except Exception:
            with contextlib.suppress(OSError):
                request.rmdir()
            raise
        return request

    def _require_request_directory(self, request_id: str) -> Path:
        _validate_request_id(request_id)
        _validate_directory(self.state_root, service_uid=self.service_uid)
        _validate_directory(self.state_root / "maintenance", service_uid=self.service_uid)
        _validate_directory(self.root, service_uid=self.service_uid)
        request = self.root / request_id
        _validate_directory(request, service_uid=self.service_uid)
        return request


def _validate_request_id(value: str) -> None:
    if _REQUEST_RE.fullmatch(value) is None:
        raise ManifestOwnershipJournalError("ownership journal request identity is invalid")


def _json_bytes(value: Mapping[str, object]) -> bytes:
    try:
        payload = (json.dumps(dict(value), sort_keys=True, separators=(",", ":")) + "\n").encode()
    except (TypeError, ValueError) as exc:
        raise ManifestOwnershipJournalError("ownership journal payload is invalid") from exc
    if not payload or len(payload) > _MAX_RECORD_BYTES:
        raise ManifestOwnershipJournalError("ownership journal payload is unbounded")
    return payload


def _ensure_directory(path: Path, *, service_uid: int) -> None:
    try:
        path.mkdir(mode=_PRIVATE_DIRECTORY_MODE)
    except FileExistsError:
        _validate_directory(path, service_uid=service_uid)
        return
    except OSError as exc:
        raise ManifestOwnershipJournalError("ownership journal directory is unavailable") from exc
    try:
        os.chmod(path, _PRIVATE_DIRECTORY_MODE, follow_symlinks=False)
        _validate_directory(path, service_uid=service_uid)
        _fsync_directory(path.parent, service_uid=service_uid)
    except Exception:
        with contextlib.suppress(OSError):
            path.rmdir()
        raise


def _validate_directory(path: Path, *, service_uid: int) -> None:
    try:
        metadata = path.lstat()
    except OSError as exc:
        raise ManifestOwnershipJournalError("ownership journal directory is unavailable") from exc
    if (
        not stat.S_ISDIR(metadata.st_mode)
        or stat.S_ISLNK(metadata.st_mode)
        or metadata.st_uid != service_uid
        or stat.S_IMODE(metadata.st_mode) != _PRIVATE_DIRECTORY_MODE
    ):
        raise ManifestOwnershipJournalError("ownership journal directory is unsafe")


def _validate_file(path: Path, *, service_uid: int, allow_multiple_links: bool) -> None:
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        fd = os.open(path, flags)
    except OSError as exc:
        raise ManifestOwnershipJournalError("ownership journal file is unavailable") from exc
    try:
        metadata = os.fstat(fd)
        if (
            not stat.S_ISREG(metadata.st_mode)
            or metadata.st_uid != service_uid
            or (not allow_multiple_links and metadata.st_nlink != 1)
            or stat.S_IMODE(metadata.st_mode) != _PRIVATE_FILE_MODE
        ):
            raise ManifestOwnershipJournalError("ownership journal file is unsafe")
    finally:
        os.close(fd)


def _publish_no_replace(path: Path, payload: bytes, *, service_uid: int) -> None:
    _validate_directory(path.parent, service_uid=service_uid)
    directory_flags = (
        os.O_RDONLY
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_DIRECTORY", 0)
        | getattr(os, "O_NOFOLLOW", 0)
    )
    try:
        directory_fd = os.open(path.parent, directory_flags)
    except OSError as exc:
        raise ManifestOwnershipJournalError("ownership journal directory cannot be opened") from exc
    temporary = f".{path.name}.{uuid4().hex}.tmp"
    temporary_exists = False
    try:
        flags = (
            os.O_WRONLY
            | os.O_CREAT
            | os.O_EXCL
            | getattr(os, "O_CLOEXEC", 0)
            | getattr(os, "O_NOFOLLOW", 0)
        )
        fd = os.open(temporary, flags, _PRIVATE_FILE_MODE, dir_fd=directory_fd)
        temporary_exists = True
        try:
            os.fchmod(fd, _PRIVATE_FILE_MODE)
            metadata = os.fstat(fd)
            if (
                not stat.S_ISREG(metadata.st_mode)
                or metadata.st_uid != service_uid
                or metadata.st_nlink != 1
            ):
                raise ManifestOwnershipJournalError("ownership temporary journal file is unsafe")
            if os.write(fd, payload) != len(payload):
                raise ManifestOwnershipJournalError("ownership inventory write was incomplete")
            os.fsync(fd)
        finally:
            os.close(fd)
        try:
            os.link(
                temporary,
                path.name,
                src_dir_fd=directory_fd,
                dst_dir_fd=directory_fd,
                follow_symlinks=False,
            )
        except FileExistsError as exc:
            raise ManifestOwnershipJournalError("ownership inventory already exists") from exc
        os.unlink(temporary, dir_fd=directory_fd)
        temporary_exists = False
        os.fsync(directory_fd)
        _validate_file(path, service_uid=service_uid, allow_multiple_links=False)
    except OSError as exc:
        raise ManifestOwnershipJournalError("ownership inventory publication failed") from exc
    finally:
        if temporary_exists:
            with contextlib.suppress(OSError):
                os.unlink(temporary, dir_fd=directory_fd)
        os.close(directory_fd)


def _fsync_directory(path: Path, *, service_uid: int) -> None:
    _validate_directory(path, service_uid=service_uid)
    flags = (
        os.O_RDONLY
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_DIRECTORY", 0)
        | getattr(os, "O_NOFOLLOW", 0)
    )
    try:
        fd = os.open(path, flags)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
    except OSError as exc:
        raise ManifestOwnershipJournalError("ownership journal directory fsync failed") from exc


__all__ = ["ManifestOwnershipJournal", "ManifestOwnershipJournalError"]
