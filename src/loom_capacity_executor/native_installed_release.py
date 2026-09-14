"""Read-only original-UID release observation, not installation or admission.

The protected publisher owns the complete inventory, including Python imports
and platform system dependencies. The trusted launcher must authenticate this
verifier before importing it. Feature source never supplies a manifest digest.
Root-owned installations must not be modified in place while workers use them.
"""

from __future__ import annotations

import hashlib
import os
import stat
from contextlib import ExitStack
from dataclasses import dataclass
from pathlib import Path
from typing import Annotated, Literal, Self

from pydantic import Field, field_validator, model_validator

from loom_capacity_manager.contracts import Digest, StrictV1Model, canonical_bytes

_MAX_MANIFEST = 4 * 1024**2
_MAX_TOTAL = 32 * 1024**3
_CHUNK = 1024**2
_PUBLISHED_RUNSC_FILES = (
    "runsc", "containerd-shim-runsc-v1", "gvisor-bin/checkpointgofer",
    "gvisor-bin/gvisor_sentry", "gvisor-bin/runsc-metric-server",
)


def _path(value: str) -> Path:
    path = Path(value)
    if (not path.is_absolute() or path == Path("/") or str(path) != value or value.startswith("//")
        or ".." in path.parts or len(path.parts) > 64 or len(value) > 4096
        or any(c in value for c in ("\0", "\n", "\r"))):
        raise ValueError("native release path must be canonical and absolute")
    return path


class NativeInstalledFileV1(StrictV1Model):
    path: str
    sha256: Digest
    size_bytes: int = Field(ge=0, le=8 * 1024**3)
    mode: Literal[0o444, 0o555]

    @field_validator("path")
    @classmethod
    def _valid_path(cls, value: str) -> str:
        _path(value)
        return value


class NativeInstalledReleaseV1(StrictV1Model):
    source_sha: Annotated[str, Field(pattern=r"^[0-9a-f]{40}$")]
    platform: Literal["linux/amd64", "linux/arm64"]
    runsc_root: str
    python_root: str
    python: str
    rootlesskit: str
    rootfs: str
    seccomp: str
    files: tuple[NativeInstalledFileV1, ...] = Field(min_length=1, max_length=20000)

    @model_validator(mode="after")
    def _complete_inventory(self) -> Self:
        roots = [_path(self.runsc_root), _path(self.python_root)]
        if roots[0] == roots[1] or any(a in b.parents for a, b in (roots, roots[::-1])):
            raise ValueError("native release runtime roots must be disjoint")
        inventory = {item.path: item for item in self.files}
        if len(inventory) != len(self.files) or list(inventory) != sorted(inventory):
            raise ValueError("native release inventory must be sorted and unique")
        if sum(item.size_bytes for item in self.files) > _MAX_TOTAL:
            raise ValueError("native release inventory exceeds total byte bound")
        roles = [self.python, self.rootlesskit, self.rootfs, self.seccomp]
        if len(set(roles)) != len(roles):
            raise ValueError("native release roles must be distinct")
        for role in roles:
            _path(role)
            if role not in inventory or inventory[role].size_bytes == 0:
                raise ValueError("native release inventory misses required material")
        if roots[1] not in Path(self.python).parents:
            raise ValueError("native release Python is outside its complete import tree")
        executables = [self.python, self.rootlesskit]
        executables.extend(str(roots[0] / name) for name in _PUBLISHED_RUNSC_FILES)
        if any(path not in inventory or inventory[path].mode != 0o555 for path in executables):
            raise ValueError("native release misses a readonly published runtime executable")
        if inventory[self.seccomp].size_bytes > 256 * 1024:
            raise ValueError("native release seccomp exceeds byte bound")
        if any(inventory[role].mode != 0o444 for role in (self.rootfs, self.seccomp)):
            raise ValueError("native release data must be readonly")
        return self


@dataclass(frozen=True, slots=True)
class NativeInstalledReleaseObservation:
    manifest: NativeInstalledReleaseV1
    manifest_sha256: str
    client_seccomp: bytes


def _read_identity_map(name: str) -> str:
    with Path(f"/proc/self/{name}").open("r", encoding="ascii") as source:
        return source.read(4097)


def _require_original_identity() -> None:
    if (os.getuid() == 0 or os.getuid() != os.geteuid() or os.getgid() == 0
        or os.getgid() != os.getegid()):
        raise ValueError("native release verification requires the original unprivileged identity")
    for name in ("uid_map", "gid_map"):
        if _read_identity_map(name).split() != ["0", "0", "4294967295"]:
            raise ValueError("native release verification requires the original host identity map")


def _identity(metadata: os.stat_result) -> tuple[int, ...]:
    return (metadata.st_dev, metadata.st_ino, metadata.st_mode, metadata.st_uid, metadata.st_gid,
        metadata.st_nlink, metadata.st_size, metadata.st_mtime_ns, metadata.st_ctime_ns)


class _Observation:
    def __init__(self) -> None:
        self.directories: dict[Path, tuple[int, ...]] = {}
        self.closed_directories: dict[Path, tuple[int, ...]] = {}
        self.files: dict[Path, tuple[int, ...]] = {}

    def directory(self, path: Path, stack: ExitStack) -> int:
        descriptor = os.open("/", os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC | os.O_NOFOLLOW)
        stack.callback(os.close, descriptor)
        current = Path("/")
        for component in (None, *path.parts[1:]):
            if component is not None:
                descriptor = os.open(component, os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC | os.O_NOFOLLOW,
                    dir_fd=descriptor)
                stack.callback(os.close, descriptor)
                current /= component
            metadata = os.fstat(descriptor)
            if (not stat.S_ISDIR(metadata.st_mode) or metadata.st_uid != 0 or metadata.st_gid != 0
                or metadata.st_mode & 0o7022):
                raise ValueError("native release parent must be protected host-root directory")
            # A protected ancestor may host unrelated concurrent installations.
            # Its inode/authority must stay fixed, not its sibling-entry count or
            # timestamps. Closed payload directories get stricter checks below.
            identity = (metadata.st_dev, metadata.st_ino, metadata.st_mode, metadata.st_uid, metadata.st_gid)
            previous = self.directories.setdefault(current, identity)
            if previous != identity:
                raise ValueError("native release directory changed during verification")
        return descriptor

    def read(self, path: Path, *, digest: str, size: int | None, mode: int, bound: int, collect: bool) -> bytes:
        with ExitStack() as stack:
            directory = self.directory(path.parent, stack)
            descriptor = os.open(path.name, os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW | os.O_NONBLOCK,
                dir_fd=directory)
            stack.callback(os.close, descriptor)
            metadata = os.fstat(descriptor)
            if (not stat.S_ISREG(metadata.st_mode) or metadata.st_uid != 0 or metadata.st_gid != 0
                or stat.S_IMODE(metadata.st_mode) != mode or metadata.st_nlink != 1
                or metadata.st_size > bound or (size is not None and size != metadata.st_size)):
                raise ValueError("native release file must match protected readonly inventory")
            identity = _identity(metadata)
            result, hashed, count = bytearray(), hashlib.sha256(), 0
            while part := os.read(descriptor, min(_CHUNK, metadata.st_size - count + 1)):
                count += len(part)
                if count > metadata.st_size:
                    raise ValueError("native release file grew during verification")
                hashed.update(part)
                if collect:
                    result.extend(part)
            if count != metadata.st_size or hashed.hexdigest() != digest or _identity(os.fstat(descriptor)) != identity:
                raise ValueError("native release file bytes changed during verification")
            if self.files.setdefault(path, identity) != identity:
                raise ValueError("native release file identity changed during verification")
            return bytes(result)

    def closed_tree(self, root: Path, inventory: set[Path]) -> None:
        expected: dict[Path, set[str]] = {root: set()}
        for path in inventory:
            if root not in path.parents:
                continue
            current = path
            while current != root:
                expected.setdefault(current.parent, set()).add(current.name)
                current = current.parent
        for directory, names in expected.items():
            with ExitStack() as stack:
                descriptor = self.directory(directory, stack)
                identity = _identity(os.fstat(descriptor))
                if self.closed_directories.setdefault(directory, identity) != identity:
                    raise ValueError("native release closed runtime directory changed")
                with os.scandir(descriptor) as entries:
                    observed = set()
                    for entry in entries:
                        if entry.name not in names:
                            raise ValueError("native release contains an unlisted runtime member")
                        observed.add(entry.name)
                    if observed != names:
                        raise ValueError("native release is missing a runtime member")

    def finish(self) -> None:
        # Reopen through every protected ancestor, not merely fstat old handles.
        for path, identity in self.files.items():
            with ExitStack() as stack:
                directory = self.directory(path.parent, stack)
                descriptor = os.open(path.name, os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW | os.O_NONBLOCK,
                    dir_fd=directory)
                stack.callback(os.close, descriptor)
                if _identity(os.fstat(descriptor)) != identity:
                    raise ValueError("native release file was replaced during verification")
        for path in tuple(self.directories):
            with ExitStack() as stack:
                descriptor = self.directory(path, stack)
                if path in self.closed_directories and _identity(os.fstat(descriptor)) != self.closed_directories[path]:
                    raise ValueError("native release closed runtime directory changed")


def verify_native_installed_release(path: Path, *, expected_sha256: str,
    expected_source_sha: str, expected_platform: str,
) -> NativeInstalledReleaseObservation:
    """Authenticate protected material without credentials, namespace entry or writes.

    This is not dependency discovery: the trusted publisher must enumerate the
    installed Python/import tree and system dependencies. It is not a durable
    readiness certificate; callers retain the protected release for the attempt.
    """
    _require_original_identity()
    _path(str(path))
    if len(expected_sha256) != 64 or any(c not in "0123456789abcdef" for c in expected_sha256):
        raise ValueError("native release expected manifest digest is invalid")
    observation = _Observation()
    wire = observation.read(path, digest=expected_sha256, size=None, mode=0o444, bound=_MAX_MANIFEST, collect=True)
    manifest = NativeInstalledReleaseV1.model_validate_json(wire)
    if canonical_bytes(manifest) != wire:
        raise ValueError("native release manifest must be canonical")
    if manifest.source_sha != expected_source_sha or manifest.platform != expected_platform:
        raise ValueError("native release source or platform differs from protected installation")
    inventory = {Path(item.path) for item in manifest.files}
    if path in inventory:
        raise ValueError("native release manifest cannot list itself")
    for root in (manifest.runsc_root, manifest.python_root):
        observation.closed_tree(Path(root), inventory)
    profile = b""
    for item in manifest.files:
        content = observation.read(Path(item.path), digest=item.sha256, size=item.size_bytes,
            mode=item.mode, bound=8 * 1024**3, collect=item.path == manifest.seccomp)
        if item.path == manifest.seccomp:
            profile = content
    observation.finish()
    return NativeInstalledReleaseObservation(manifest, expected_sha256, profile)
