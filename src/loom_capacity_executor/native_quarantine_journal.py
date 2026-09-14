"""Root-private, exact-inode cleanup progress; not terminal or capacity authority.

Only the authenticated fixed node helper may construct this operation, after
validating installed policy and stable no-writer exclusion. The key is the digest
of its authenticated history, not caller-selected local locator data. This module
has no CLI or worker route. A journal records filesystem progress only; it never
allocates or releases capacity or replaces the node's terminal/quiescence fence.
"""

from __future__ import annotations

import ctypes
import fcntl
import hashlib
import json
import os
import re
import stat
from contextlib import ExitStack
from dataclasses import asdict
from pathlib import Path
from types import TracebackType
from typing import Literal, Self

from loom_capacity_executor.native_installed_release import _Observation, _path
from loom_capacity_executor.native_mapped_scratch import _mount_id
from loom_capacity_executor.native_oci_material import _open_directory
from loom_capacity_executor.native_quarantine_prune import (
    NativeQuarantineIdentity,
    _require_initial_root,
    prune_native_quarantine,
)
from loom_capacity_executor.trusted_launcher import _write_all
from loom_capacity_manager.contracts import Digest, StrictV1Model, canonical_bytes

Phase = Literal["quarantining", "quarantined", "pruned", "removing", "completed"]
_DIRECTORY = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC


class NativeQuarantineProgressV1(StrictV1Model):
    key: Digest
    source: str
    identity_sha256: Digest
    phase: Phase


def _regular(fd: int) -> os.stat_result:
    value = os.fstat(fd)
    if (not stat.S_ISREG(value.st_mode) or value.st_uid != 0 or value.st_gid != 0
        or stat.S_IMODE(value.st_mode) != 0o600 or value.st_nlink != 1 or value.st_size > 4096):
        raise ValueError("quarantine journal file is not bounded root-private regular data")
    return value


def _rename_exclusive(source_parent: int, source_name: str, destination_parent: int) -> None:
    libc = ctypes.CDLL(None, use_errno=True)
    rename = libc.renameat2
    rename.argtypes = [ctypes.c_int, ctypes.c_char_p, ctypes.c_int, ctypes.c_char_p, ctypes.c_uint]
    rename.restype = ctypes.c_int
    if rename(source_parent, os.fsencode(source_name), destination_parent, b"attempt", 1) != 0:
        error = ctypes.get_errno()
        raise OSError(error, os.strerror(error))


class NativeQuarantineJournal:
    def __init__(self, directory: Path, *, key: str, source: Path, identity: NativeQuarantineIdentity) -> None:
        _path(str(directory))
        _path(str(source))
        identity.validate()
        if (re.fullmatch(r"[0-9a-f]{64}", key) is None
            or re.fullmatch(r"attempt-[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}", source.name) is None
            or directory == source or directory in source.parents or source in directory.parents
            or identity.uid_ranges[0][1] != 1 or identity.gid_ranges[0][1] != 1):
            raise ValueError("quarantine journal scope changed")
        self._directory, self._key, self._source, self._identity = directory, key, source, identity
        self._identity_digest = hashlib.sha256(json.dumps(asdict(identity), sort_keys=True, separators=(",", ":")).encode("ascii")).hexdigest()
        self._stack: ExitStack | None = None
        self._fd: int | None = None
        self._progress: NativeQuarantineProgressV1 | None = None

    def __enter__(self) -> Self:
        _require_initial_root()
        if self._stack is not None:
            raise ValueError("quarantine journal is already open")
        stack = ExitStack()
        try:
            parent = _Observation().directory(self._directory, stack)
            if stat.S_IMODE(os.fstat(parent).st_mode) != 0o700:
                raise ValueError("quarantine journal root must be private")
            try:
                os.mkdir(self._key, mode=0o700, dir_fd=parent)
                os.fsync(parent)
            except FileExistsError:
                pass
            fd = os.open(self._key, _DIRECTORY, dir_fd=parent)
            stack.callback(os.close, fd)
            metadata = os.fstat(fd)
            if (metadata.st_uid != 0 or metadata.st_gid != 0 or stat.S_IMODE(metadata.st_mode) != 0o700
                or metadata.st_dev != self._identity.device or _mount_id(fd) != self._identity.mount_id):
                raise ValueError("quarantine journal must be protected on the exact scratch mount")
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            self._fd, self._stack = fd, stack
            self._progress = self._read()
            return self
        except BaseException:
            self._fd, self._stack = None, None
            stack.close()
            raise

    def __exit__(self, exc_type: type[BaseException] | None, exc: BaseException | None, traceback: TracebackType | None) -> None:
        if self._stack is not None:
            self._stack.close()
        self._fd, self._stack = None, None

    def _descriptor(self) -> int:
        if self._fd is None:
            raise ValueError("quarantine journal must hold its directory lock")
        return self._fd

    def _read(self) -> NativeQuarantineProgressV1 | None:
        try:
            fd = os.open("progress.json", os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW | os.O_NONBLOCK,
                dir_fd=self._descriptor())
        except FileNotFoundError:
            return None
        try:
            before = _regular(fd)
            wire = bytearray()
            while part := os.read(fd, 4097 - len(wire)):
                wire.extend(part)
                if len(wire) > 4096:
                    raise ValueError("quarantine journal exceeds bound")
            if len(wire) != before.st_size:
                raise ValueError("quarantine journal changed during read")
            result = NativeQuarantineProgressV1.model_validate_json(bytes(wire))
            if (canonical_bytes(result) != wire or result.key != self._key or result.source != str(self._source)
                or result.identity_sha256 != self._identity_digest):
                raise ValueError("quarantine journal historical identity changed")
            return result
        finally:
            os.close(fd)

    def _save(self, phase: Phase) -> None:
        progress = NativeQuarantineProgressV1(key=self._key, source=str(self._source),
            identity_sha256=self._identity_digest, phase=phase)
        wire = canonical_bytes(progress)
        if len(wire) > 4096:
            raise ValueError("quarantine journal exceeds byte bound")
        fd = os.open(".pending", os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW | os.O_CLOEXEC | os.O_NONBLOCK,
            0o600, dir_fd=self._descriptor())
        try:
            _regular(fd)  # Validate an interrupted pending file before truncation.
            os.ftruncate(fd, 0)
            _write_all(fd, wire)
            os.fsync(fd)
        finally:
            os.close(fd)
        os.replace(".pending", "progress.json", src_dir_fd=self._descriptor(), dst_dir_fd=self._descriptor())
        os.fsync(self._descriptor())
        self._progress = progress

    def _attempt(self, parent: int, name: str, stack: ExitStack) -> int | None:
        try:
            fd = os.open(name, _DIRECTORY, dir_fd=parent)
        except FileNotFoundError:
            return None
        stack.callback(os.close, fd)
        value = os.fstat(fd)
        if ((value.st_dev, value.st_ino) != (self._identity.device, self._identity.inode)
            or _mount_id(fd) != self._identity.mount_id
            or (value.st_uid, value.st_gid) != (self._identity.uid_ranges[0][0], self._identity.gid_ranges[0][0])
            or stat.S_IMODE(value.st_mode) != 0o700):
            raise ValueError("quarantine attempt identity changed")
        return fd

    def reconcile(self) -> Literal["completed"]:
        """Finish only this already terminal-fenced, no-writer-excluded attempt."""
        _require_initial_root()
        destination = self._descriptor()
        with ExitStack() as stack:
            quarantined = self._attempt(destination, "attempt", stack)
            if self._progress is not None and self._progress.phase == "completed":
                if quarantined is not None:
                    raise ValueError("completed quarantine unexpectedly reappeared")
                return "completed"  # Owner teardown may already have removed its scratch root.
            source_parent = _open_directory(self._source.parent, stack)
            metadata = os.fstat(source_parent)
            if ((metadata.st_uid, metadata.st_gid) != (self._identity.uid_ranges[0][0], self._identity.gid_ranges[0][0])
                or stat.S_IMODE(metadata.st_mode) != 0o700 or _mount_id(source_parent) != self._identity.mount_id):
                raise ValueError("quarantine source parent identity changed")
            source = self._attempt(source_parent, self._source.name, stack)
            if self._progress is None:
                if source is None or quarantined is not None:
                    raise ValueError("quarantine has no retained transition explaining its location")
                self._save("quarantining")
            assert self._progress is not None
            if self._progress.phase == "quarantining":
                if source is not None and quarantined is None:
                    _rename_exclusive(source_parent, self._source.name, destination)
                    os.fsync(source_parent)
                    os.fsync(destination)
                    quarantined = self._attempt(destination, "attempt", stack)
                    source = None
                elif source is not None or quarantined is None:
                    raise ValueError("quarantine transition has ambiguous directory identity")
                self._save("quarantined")
            if source is not None:
                raise ValueError("quarantine source unexpectedly reappeared")
            if quarantined is None:
                if self._progress.phase != "removing":
                    raise ValueError("quarantine absence lacks a durable removal transition")
                self._save("completed")
                return "completed"
            if self._progress.phase == "quarantined":
                prune_native_quarantine(quarantined, identity=self._identity)
                self._save("pruned")
            names: list[str] = []
            with os.scandir(quarantined) as children:
                for child in children:
                    names.append(child.name)
                    if len(names) > 1:
                        break  # Finalization accepts at most the single retained locator.
            if names not in (["recovery.json"], []) or (not names and self._progress.phase != "removing"):
                raise ValueError("quarantine finalization residue changed")
            if names:
                locator = os.stat("recovery.json", dir_fd=quarantined, follow_symlinks=False)
                if (not stat.S_ISREG(locator.st_mode) or locator.st_nlink != 1
                    or (locator.st_uid, locator.st_gid) != (self._identity.uid_ranges[0][0], self._identity.gid_ranges[0][0])):
                    raise ValueError("quarantine locator identity changed")
            if self._progress.phase == "pruned":
                self._save("removing")
            if names:
                os.unlink("recovery.json", dir_fd=quarantined)
                os.fsync(quarantined)
            os.rmdir("attempt", dir_fd=destination)
            os.fsync(destination)
            self._save("completed")
            return "completed"
