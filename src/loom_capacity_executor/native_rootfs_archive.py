"""Bounded trusted-release unpacking; not installation or execution authority.

The caller authenticates the release digest and runs preparation in its mapped
namespace before feature execution. This is never a personal-source extractor.
Capability restoration and runtime/bundle verification remain separate steps.
Abrupt process death can retain partial scratch; the allocation owns its recovery.
"""

from __future__ import annotations

import hashlib
import io
import os
import stat
import tarfile
from contextlib import suppress
from dataclasses import dataclass
from pathlib import Path, PurePosixPath

_CHUNK = 1024**2
_DIRECTORY = os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC | os.O_NOFOLLOW
_Identity = tuple[int, int]
_Parts = tuple[str, ...]


@dataclass(frozen=True, slots=True)
class UnpackedNativeRootfs:
    """Local unpack observation, not proof of complete ready runtime material."""

    entries: int
    unpacked_bytes: int


class _BoundedReader(io.BufferedReader):
    def read(self, size: int | None = -1) -> bytes:
        # tarfile also reads PAX/GNU metadata through this interface. Reject an
        # oversized metadata allocation before it is materialized in memory.
        if size is None or not 0 <= size <= _CHUNK:
            raise ValueError("native rootfs archive read exceeds bound")
        return super().read(size)


def _identity(metadata: os.stat_result) -> _Identity:
    return metadata.st_dev, metadata.st_ino


def _snapshot(metadata: os.stat_result) -> tuple[int, ...]:
    return (metadata.st_dev, metadata.st_ino, metadata.st_mode, metadata.st_uid,
        metadata.st_gid, metadata.st_nlink, metadata.st_size, metadata.st_mtime_ns, metadata.st_ctime_ns)


def _parts(name: str) -> _Parts:
    if name.startswith("./"):
        name = name[2:]
    path = PurePosixPath(name)
    if (not name or path.is_absolute() or ".." in path.parts or len(name) > 4096
        or any(c in name for c in ("\x00", "\n", "\r")) or str(path) != name):
        raise ValueError("native rootfs archive path is invalid")
    return () if name == "." else path.parts


def _guest_link(parent: _Parts, target: str) -> None:
    if not target or len(target) > 4096 or any(c in target for c in ("\x00", "\n", "\r")):
        raise ValueError("native rootfs link is invalid")
    resolved = [] if target.startswith("/") else list(parent)
    for part in target.split("/"):
        if part == "..":
            if not resolved:
                raise ValueError("native rootfs link escapes guest root")
            resolved.pop()
        elif part not in ("", "."):
            resolved.append(part)


def _inventory(archive: tarfile.TarFile, *, max_entries: int, max_unpacked_bytes: int,
) -> tuple[dict[_Parts, tarfile.TarInfo], int]:
    members: dict[_Parts, tarfile.TarInfo] = {}
    total = 0
    for member in archive:
        parts = _parts(member.name)
        if parts in members or len(members) >= max_entries:
            raise ValueError("native rootfs duplicate or excessive entries")
        if (not (member.isdir() or member.isfile() or member.issym()) or member.issparse()
            or not 0 <= member.uid < 65536 or not 0 <= member.gid < 65536
            or not 0 <= member.mode <= 0o7777 or member.size < 0
            or (not member.isfile() and member.size != 0) or (not parts and not member.isdir())):
            raise ValueError("native rootfs member metadata is invalid")
        if member.issym():
            _guest_link(parts[:-1], member.linkname)
        total += member.size
        if total > max_unpacked_bytes:
            raise ValueError("native rootfs unpacked size exceeds bound")
        members[parts] = member
    if not members:
        raise ValueError("native rootfs archive is empty")
    for parts in members:
        for length in range(1, len(parts)):
            ancestor = members.get(parts[:length])
            if ancestor is None or not ancestor.isdir():
                raise ValueError("native rootfs parent is absent or not a directory")
    return members, total


def _parent(root: int, parts: _Parts, created: dict[_Parts, _Identity]) -> int:
    descriptor = os.dup(root)
    try:
        for length, name in enumerate(parts, 1):
            child = os.open(name, _DIRECTORY, dir_fd=descriptor)
            os.close(descriptor)
            descriptor = child
            if _identity(os.fstat(descriptor)) != created.get(parts[:length]):
                raise ValueError("native rootfs directory identity changed")
        return descriptor
    except BaseException:
        os.close(descriptor)
        raise


def _cleanup(root: int, parent: int, name: str, created: dict[_Parts, _Identity]) -> None:
    # Remove only tracked inodes, never a replacement or its contents. The root
    # descriptor remains anchored even if the destination itself was renamed.
    with suppress(OSError):
        os.fchmod(root, 0o700)
    for parts in sorted((item for item in created if item), key=len):
        try:
            directory = _parent(root, parts, created)
        except (OSError, ValueError):
            continue
        try:
            # Published readonly modes must not prevent exact failure cleanup.
            # _parent has checked every inode before we modify any directory.
            with suppress(OSError):
                os.fchmod(directory, 0o700)
        finally:
            os.close(directory)
    for parts in sorted((item for item in created if item), key=len, reverse=True):
        try:
            directory = _parent(root, parts[:-1], created)
        except (OSError, ValueError):
            continue
        try:
            with suppress(OSError):
                metadata = os.stat(parts[-1], dir_fd=directory, follow_symlinks=False)
                if _identity(metadata) == created[parts]:
                    if stat.S_ISDIR(metadata.st_mode):
                        os.rmdir(parts[-1], dir_fd=directory)
                    else:
                        os.unlink(parts[-1], dir_fd=directory)
        finally:
            os.close(directory)
    with suppress(OSError):
        if _identity(os.stat(name, dir_fd=parent, follow_symlinks=False)) == created[()]:
            os.rmdir(name, dir_fd=parent)


def _write_member(archive: tarfile.TarFile, member: tarfile.TarInfo, descriptor: int) -> None:
    payload = archive.extractfile(member)
    if payload is None:
        raise ValueError("native rootfs regular file has no payload")
    with payload:
        remaining = member.size
        while remaining:
            chunk = payload.read(min(_CHUNK, remaining))
            if not chunk:
                raise ValueError("native rootfs member is truncated")
            remaining -= len(chunk)
            view = memoryview(chunk)
            while view:
                written = os.write(descriptor, view)
                if written <= 0:
                    raise OSError("native rootfs write did not progress")
                view = view[written:]
    os.fchown(descriptor, member.uid, member.gid)
    os.fchmod(descriptor, member.mode)
    os.fsync(descriptor)


def _verify_members(root: int, members: dict[_Parts, tarfile.TarInfo], created: dict[_Parts, _Identity]) -> None:
    for parts, member in members.items():
        if not parts:
            continue
        directory = _parent(root, parts[:-1], created)
        try:
            try:
                metadata = os.stat(parts[-1], dir_fd=directory, follow_symlinks=False)
            except FileNotFoundError:
                raise ValueError("native rootfs member changed during preparation") from None
            valid_type = (stat.S_ISREG(metadata.st_mode) if member.isfile()
                else stat.S_ISDIR(metadata.st_mode) if member.isdir() else stat.S_ISLNK(metadata.st_mode))
            if (not valid_type or _identity(metadata) != created[parts]
                or metadata.st_uid != member.uid or metadata.st_gid != member.gid
                or (not member.issym() and stat.S_IMODE(metadata.st_mode) != member.mode)
                or (member.isfile() and (metadata.st_size != member.size or metadata.st_nlink != 1))
                or (member.issym() and os.readlink(parts[-1], dir_fd=directory) != member.linkname)):
                raise ValueError("native rootfs member changed during preparation")
        finally:
            os.close(directory)


def unpack_native_rootfs_archive(*, archive: Path, destination: Path, expected_sha256: str,
    expected_size_bytes: int, max_unpacked_bytes: int, max_entries: int,
) -> UnpackedNativeRootfs:
    """Unpack exact protected bytes under a fresh, private attempt parent.

    Numeric image owners require the caller's declared subordinate UID/GID map.
    Guest absolute symlinks are preserved, but no archive write follows a link.
    The result is NOT ready-to-execute until published capabilities and all other
    protected material have been restored and independently checked by the caller.
    """
    if (len(expected_sha256) != 64 or any(c not in "0123456789abcdef" for c in expected_sha256)
        or type(expected_size_bytes) is not int or not 1 <= expected_size_bytes <= 8 * 1024**3
        or type(max_unpacked_bytes) is not int or not 1 <= max_unpacked_bytes <= 32 * 1024**3
        or type(max_entries) is not int or not 1 <= max_entries <= 100000):
        raise ValueError("native rootfs digest or bounds are invalid")
    for path in (archive, destination):
        if not path.is_absolute() or path == Path("/") or ".." in path.parts:
            raise ValueError("native rootfs material path is invalid")
    source = os.open(archive, os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW | os.O_NONBLOCK)
    try:
        original = os.fstat(source)
        if not stat.S_ISREG(original.st_mode) or original.st_size != expected_size_bytes:
            raise ValueError("native rootfs archive size/type changed")
        digest = hashlib.sha256()
        offset = 0
        while offset < expected_size_bytes:
            chunk = os.pread(source, min(_CHUNK, expected_size_bytes - offset), offset)
            if not chunk:
                raise ValueError("native rootfs archive truncated")
            digest.update(chunk)
            offset += len(chunk)
        if digest.hexdigest() != expected_sha256 or _snapshot(os.fstat(source)) != _snapshot(original):
            raise ValueError("native rootfs archive digest or identity changed")
        with _BoundedReader(io.FileIO(os.dup(source), "rb")) as stream, tarfile.open(fileobj=stream, mode="r:") as tar:
            members, total = _inventory(tar, max_entries=max_entries, max_unpacked_bytes=max_unpacked_bytes)
            parent = os.open(destination.parent, _DIRECTORY)
            try:
                metadata = os.fstat(parent)
                if metadata.st_uid != os.geteuid() or stat.S_IMODE(metadata.st_mode) != 0o700:
                    raise ValueError("native rootfs attempt parent must be private and owned")
                os.mkdir(destination.name, 0o700, dir_fd=parent)
                root_metadata = os.stat(destination.name, dir_fd=parent, follow_symlinks=False)
                root = os.open(destination.name, _DIRECTORY, dir_fd=parent)
                if _identity(os.fstat(root)) != _identity(root_metadata):
                    os.close(root)
                    raise ValueError("native rootfs root changed during creation")
                created: dict[_Parts, _Identity] = {(): _identity(root_metadata)}
                try:
                    for parts, member in sorted(members.items(), key=lambda item: len(item[0])):
                        if not parts:
                            continue
                        directory = _parent(root, parts[:-1], created)
                        try:
                            if member.isdir():
                                os.mkdir(parts[-1], 0o700, dir_fd=directory)
                                created[parts] = _identity(os.stat(parts[-1], dir_fd=directory, follow_symlinks=False))
                            elif member.issym():
                                os.symlink(member.linkname, parts[-1], dir_fd=directory)
                                created[parts] = _identity(os.stat(parts[-1], dir_fd=directory, follow_symlinks=False))
                                os.chown(parts[-1], member.uid, member.gid, dir_fd=directory, follow_symlinks=False)
                            else:
                                descriptor = os.open(parts[-1], os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_CLOEXEC
                                    | os.O_NOFOLLOW, 0o600, dir_fd=directory)
                                created[parts] = _identity(os.fstat(descriptor))
                                try:
                                    _write_member(tar, member, descriptor)
                                finally:
                                    os.close(descriptor)
                        finally:
                            os.close(directory)
                    for parts, member in sorted(members.items(), key=lambda item: len(item[0]), reverse=True):
                        if member.isdir():
                            directory = _parent(root, parts, created)
                            try:
                                os.fchown(directory, member.uid, member.gid)
                                os.fchmod(directory, member.mode)
                                os.fsync(directory)
                            finally:
                                os.close(directory)
                    if () not in members:
                        os.fchmod(root, 0o755)
                    _verify_members(root, members, created)
                    if (_snapshot(os.fstat(source)) != _snapshot(original)
                        or _identity(os.stat(destination.name, dir_fd=parent, follow_symlinks=False)) != created[()]
                        or _identity(destination.parent.lstat()) != _identity(metadata)):
                        raise ValueError("native rootfs material changed during unpacking")
                    os.fsync(root)
                    os.fsync(parent)
                    return UnpackedNativeRootfs(len(members), total)
                except BaseException:
                    _cleanup(root, parent, destination.name, created)
                    raise
                finally:
                    os.close(root)
            finally:
                os.close(parent)
    finally:
        os.close(source)
