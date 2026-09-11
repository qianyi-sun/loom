"""Owner-private, additive recovery scope registry with atomic publication."""

from __future__ import annotations

import fcntl
import os
import re
import stat
from hashlib import sha256
from pathlib import Path
from types import TracebackType
from typing import Literal, Self
from uuid import uuid4

from loom_capacity_manager.contracts import canonical_bytes
from loom_service.personal_dev_build_management import (
    BuildManagementFileV1,
    BuildManagementScopeV1,
    BuildManagementServiceConfigV1,
)

_MAX_BYTES = 1024 * 1024
_NAME = "management.json"
BuildScopeCredentialKind = Literal["bearer_token", "ca", "certificate", "private_key"]
CREDENTIAL_KINDS: tuple[BuildScopeCredentialKind, ...] = ("bearer_token", "ca", "certificate", "private_key")


def reporter_file_name(kind: BuildScopeCredentialKind, digest: str) -> str:
    if kind not in CREDENTIAL_KINDS or re.fullmatch(r"[0-9a-f]{64}", digest) is None:
        raise ValueError("invalid reporter credential snapshot identity")
    return f"reporter-{kind}-{digest}.bin"


def scope_snapshot_name(config: BuildManagementServiceConfigV1) -> str:
    return f"management-{sha256(canonical_bytes(config)).hexdigest()}.json"


class BuildScopeRegistry:
    """Lock the directory itself; never unlink/recreate a shared lock file.

    The protected caller provisions this current-UID 0700 directory outside the
    candidate filesystem. Every update retains existing owners and rotations.
    A stale expected hash is rejected before the caller submits membership.
    """

    def __init__(self, directory: Path) -> None:
        if (not directory.is_absolute() or ".." in directory.parts or directory == Path("/")):
            raise ValueError("build scope registry requires an absolute private directory")
        self.directory = directory
        self.current: BuildManagementServiceConfigV1 | None = None
        self._wire: bytes | None = None
        self._descriptor: int | None = None
        self._proposal: bytes | None = None

    def __enter__(self) -> Self:
        if self._descriptor is not None:
            raise ValueError("build scope registry is already locked")
        descriptor = os.open(self.directory, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC)
        try:
            metadata = os.fstat(descriptor)
            if metadata.st_uid != os.getuid() or stat.S_IMODE(metadata.st_mode) != 0o700:
                raise ValueError("build scope registry requires a current-UID 0700 directory")
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
            self._descriptor = descriptor
            self._wire = self._read()
            self.current = None if self._wire is None else BuildManagementServiceConfigV1.model_validate_json(self._wire)
            if self.current is not None and canonical_bytes(self.current) != self._wire:
                raise ValueError("build scope registry is not canonical")
            return self
        except BaseException:
            self._descriptor = None
            os.close(descriptor)
            raise

    def __exit__(self, exc_type: type[BaseException] | None, exc: BaseException | None, traceback: TracebackType | None) -> None:
        if self._descriptor is not None:
            os.close(self._descriptor)
            self._descriptor = None
        self._proposal = None

    def _fd(self) -> int:
        if self._descriptor is None:
            raise ValueError("build scope registry must be locked")
        return self._descriptor

    def _read(self, name: str = _NAME) -> bytes | None:
        try:
            descriptor = os.open(name, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC | os.O_NONBLOCK, dir_fd=self._fd())
        except FileNotFoundError:
            return None
        with os.fdopen(descriptor, "rb") as stream:
            metadata = os.fstat(stream.fileno())
            if (not stat.S_ISREG(metadata.st_mode) or metadata.st_uid != os.getuid()
                or stat.S_IMODE(metadata.st_mode) != 0o600 or metadata.st_size > _MAX_BYTES):
                raise ValueError("build scope registry file must be bounded owner-only regular data")
            wire = stream.read(_MAX_BYTES + 1)
            if not wire or len(wire) > _MAX_BYTES:
                raise ValueError("build scope registry file exceeds byte bound")
            return wire

    def propose(self, scope: BuildManagementScopeV1, *, expected_sha256: str | None) -> BuildManagementServiceConfigV1:
        self._fd()
        scope = BuildManagementScopeV1.model_validate_json(canonical_bytes(scope))
        scopes = () if self.current is None else self.current.scopes
        existing = next((item for item in scopes if item.installation.id == scope.installation.id), None)
        if existing is not None and existing != scope:
            raise ValueError("build scope installation cannot be rebound")
        actual_sha256 = None if self._wire is None else sha256(self._wire).hexdigest()
        if actual_sha256 != expected_sha256 and existing != scope:
            raise ValueError("build scope registry changed before onboarding")
        result = BuildManagementServiceConfigV1(mode="recovery-only", scopes=scopes if existing is not None else (*scopes, scope))
        wire = canonical_bytes(result)
        if len(wire) > _MAX_BYTES:
            raise ValueError("build scope registry exceeds service byte bound")
        self._proposal = wire
        return result

    def retain_reporter_file(self, kind: BuildScopeCredentialKind, wire: bytes) -> BuildManagementFileV1:
        """Retain only verified reporter inputs, never the management credential."""
        descriptor = self._fd()
        if not wire or len(wire) > _MAX_BYTES:
            raise ValueError("reporter snapshot exceeds byte bound")
        digest = sha256(wire).hexdigest()
        name = reporter_file_name(kind, digest)
        saved = self._read(name)
        if saved is not None and saved != wire:
            raise ValueError("reporter credential snapshot changed")
        if saved is None:
            temporary = f".reporter-{uuid4().hex}.tmp"
            file_descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC,
                0o600, dir_fd=descriptor)
            try:
                with os.fdopen(file_descriptor, "wb") as stream:
                    stream.write(wire)
                    stream.flush()
                    os.fsync(stream.fileno())
                os.link(temporary, name, src_dir_fd=descriptor, dst_dir_fd=descriptor, follow_symlinks=False)
            finally:
                os.unlink(temporary, dir_fd=descriptor)
        os.fsync(descriptor)
        return BuildManagementFileV1(path=str(self.directory / name), sha256=digest)

    def publish(self, config: BuildManagementServiceConfigV1) -> None:
        descriptor = self._fd()
        wire = canonical_bytes(config)
        if wire != self._proposal or self._read() != self._wire:
            raise ValueError("build scope registry proposal changed")
        snapshot = scope_snapshot_name(config)
        saved = self._read(snapshot)
        if saved is not None and saved != wire:
            raise ValueError("build scope immutable snapshot changed")
        if wire == self._wire and saved == wire:
            # A prior rename can be visible despite its directory fsync failing.
            # Exact replay must reestablish that durability before success.
            os.fsync(descriptor)
            return
        temporary = f".management-{uuid4().hex}.tmp"
        file_descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC,
            0o600, dir_fd=descriptor)
        try:
            with os.fdopen(file_descriptor, "wb") as stream:
                stream.write(wire)
                stream.flush()
                os.fsync(stream.fileno())
            if saved is None:
                os.link(temporary, snapshot, src_dir_fd=descriptor, dst_dir_fd=descriptor, follow_symlinks=False)
            os.replace(temporary, _NAME, src_dir_fd=descriptor, dst_dir_fd=descriptor)
            os.fsync(descriptor)
            self._wire, self.current = wire, config
        finally:
            try:
                os.unlink(temporary, dir_fd=descriptor)
            except FileNotFoundError:
                pass
