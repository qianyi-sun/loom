"""Restore only the two published helper capabilities in a mapped rootfs.

The caller supplies an unpacked authenticated release before feature execution.
This has no host installation, namespace creation or runtime-launch authority.
"""

from __future__ import annotations

import errno
import os
import stat
from contextlib import ExitStack
from pathlib import Path

_CAPABILITIES = {
    "newuidmap": bytes.fromhex("0100000280000000000000000000000000000000"),
    "newgidmap": bytes.fromhex("0100000240000000000000000000000000000000"),
}
_DIRECTORY = os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC | os.O_NOFOLLOW


def _require_mapped_root() -> None:
    if os.geteuid() != 0 or os.getegid() != 0:
        raise RuntimeError("native capability restoration requires mapped root")
    for name in ("uid_map", "gid_map"):
        with Path("/proc/self", name).open("rb") as stream:
            wire = stream.read(4097)
        if not wire or len(wire) > 4096:
            raise RuntimeError("native root mapping is invalid")
        fields = wire.splitlines()[0].split()
        if len(fields) != 3 or not all(value.isdigit() for value in fields):
            raise RuntimeError("native root mapping is invalid")
        inside, outside, count = map(int, fields)
        if inside != 0 or outside == 0 or count != 1:
            raise RuntimeError("native capability restoration refuses initial root")


def _owned_directory(descriptor: int) -> None:
    metadata = os.fstat(descriptor)
    if (not stat.S_ISDIR(metadata.st_mode) or metadata.st_uid != 0 or metadata.st_gid != 0
        or stat.S_IMODE(metadata.st_mode) != 0o755):
        raise ValueError("native mapper parent directory metadata changed")


def _capability(descriptor: int) -> bytes | None:
    try:
        return os.getxattr(descriptor, "security.capability")
    except OSError as error:
        if error.errno != errno.ENODATA:
            raise
        return None


def restore_native_mapper_capabilities(rootfs: Path) -> None:
    """Authenticate material first; this readback is not an activation receipt.

    Validate both fixed regular helper files before writing either. No arbitrary
    paths/capability strings, setuid fallback or permission relaxation is accepted.
    A failed partial write leaves material unusable until caller recovery.
    """
    _require_mapped_root()
    if not rootfs.is_absolute() or rootfs == Path("/") or ".." in rootfs.parts:
        raise ValueError("native mapper rootfs path is invalid")
    with ExitStack() as stack:
        root = os.open(rootfs, _DIRECTORY)
        stack.callback(os.close, root)
        _owned_directory(root)
        parent = root
        directories: list[tuple[int, str, int]] = []
        for part in ("usr", "bin"):
            child = os.open(part, _DIRECTORY, dir_fd=parent)
            directories.append((parent, part, child))
            parent = child
            stack.callback(os.close, parent)
            _owned_directory(parent)
        helpers: list[tuple[str, int, os.stat_result, bytes | None]] = []
        for name in _CAPABILITIES:
            descriptor = os.open(name, os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=parent)
            stack.callback(os.close, descriptor)
            metadata = os.fstat(descriptor)
            if (not stat.S_ISREG(metadata.st_mode) or metadata.st_uid != 0 or metadata.st_gid != 0
                or stat.S_IMODE(metadata.st_mode) != 0o755 or metadata.st_nlink != 1):
                raise ValueError("native mapper helper metadata changed")
            capability = _capability(descriptor)
            if capability is not None and capability != _CAPABILITIES[name]:
                raise ValueError("native mapper helper capability changed")
            helpers.append((name, descriptor, metadata, capability))
        for name, descriptor, _metadata, capability in helpers:
            if capability is None:
                os.setxattr(descriptor, "security.capability", _CAPABILITIES[name])
                os.fsync(descriptor)
        os.fsync(parent)
        root_path, root_descriptor = rootfs.lstat(), os.fstat(root)
        if (root_path.st_dev, root_path.st_ino) != (root_descriptor.st_dev, root_descriptor.st_ino):
            raise ValueError("native mapper rootfs changed during restoration")
        _owned_directory(root)
        for ancestor, part, child in directories:
            observed, opened = os.stat(part, dir_fd=ancestor, follow_symlinks=False), os.fstat(child)
            if (observed.st_dev, observed.st_ino) != (opened.st_dev, opened.st_ino):
                raise ValueError("native mapper directory changed during restoration")
            _owned_directory(child)
        for name, descriptor, metadata, _capability_before in helpers:
            observed = os.stat(name, dir_fd=parent, follow_symlinks=False)
            if ((observed.st_dev, observed.st_ino) != (metadata.st_dev, metadata.st_ino)
                or (observed.st_mode, observed.st_size) != (metadata.st_mode, metadata.st_size)
                or (os.fstat(descriptor).st_uid, os.fstat(descriptor).st_gid) != (0, 0)
                or observed.st_nlink != 1
                or _capability(descriptor) != _CAPABILITIES[name]):
                raise ValueError("native mapper helper changed during restoration")
