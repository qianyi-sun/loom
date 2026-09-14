"""Metadata-only pruning inside a protected, terminal-fenced quarantine.

This primitive supplies NO authentication, terminal/quiescence proof, quarantine,
journal or capacity release. Only the fixed root node helper may call it, after
establishing stable no-writer exclusion and retaining the exact inode in its
durable journal. Never call it on live worker scratch. It leaves recovery.json
and the top directory for the helper's journaled finalization.
"""

from __future__ import annotations

import os
import stat
import time
from dataclasses import dataclass
from pathlib import Path

from loom_capacity_executor.native_mapped_scratch import _mount_id

_MAX_ENTRIES = 200000
_MAX_DEPTH = 64
_MAX_SECONDS = 60
_DIRECTORY = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC


def _require_initial_root() -> None:
    if os.getresuid() != (0, 0, 0) or os.getresgid() != (0, 0, 0):
        raise ValueError("quarantine pruning requires initial host root")
    for name in ("uid_map", "gid_map"):
        with Path("/proc/self", name).open("rb") as stream:
            wire = stream.read(4097)
        if wire.split() != [b"0", b"0", b"4294967295"]:
            raise ValueError("quarantine pruning requires initial host identity maps")


@dataclass(frozen=True, slots=True)
class NativeQuarantineIdentity:
    device: int
    inode: int
    mount_id: int
    uid_ranges: tuple[tuple[int, int], ...]
    gid_ranges: tuple[tuple[int, int], ...]

    def validate(self) -> None:
        if (any(type(value) is not int for value in (self.device, self.inode, self.mount_id))
            or self.device < 0 or self.inode <= 0 or self.mount_id <= 0):
            raise ValueError("quarantine directory identity is invalid")
        for ranges in (self.uid_ranges, self.gid_ranges):
            if not 1 <= len(ranges) <= 340:
                raise ValueError("quarantine ownership ranges exceed bound")
            previous_end = 0
            for start, count in sorted(ranges):
                if (type(start) is not int or type(count) is not int or start <= 0 or count <= 0
                    or start < previous_end or start + count > 2**32 - 1):
                    raise ValueError("quarantine ownership ranges are invalid")
                previous_end = start + count


def prune_native_quarantine(descriptor: int, *, identity: NativeQuarantineIdentity) -> None:
    """Remove only allowed-ownership same-mount entries, preserving the locator.

    Files are opened O_PATH only, never for data, and links are unlinked rather
    than followed. No chmod/chown, mount operation or subprocess is performed.
    Interrupted traversal leaves remaining contents for the exact journaled inode
    to resume; this method never treats absence as a completed cleanup receipt.
    """
    _require_initial_root()
    identity.validate()
    if type(descriptor) is not int or descriptor < 0:
        raise ValueError("quarantine descriptor identity is invalid")
    deadline = time.monotonic() + _MAX_SECONDS
    entries = 0

    def budget(depth: int, extra: int = 0) -> None:
        if depth > _MAX_DEPTH or entries + extra > _MAX_ENTRIES or time.monotonic() >= deadline:
            raise ValueError("quarantine pruning exceeds bound")

    def checked(fd: int, *, root: bool = False) -> os.stat_result:
        observed = os.fstat(fd)
        if root and (not stat.S_ISDIR(observed.st_mode)
            or (observed.st_dev, observed.st_ino) != (identity.device, identity.inode)
            or stat.S_IMODE(observed.st_mode) != 0o700):
            raise ValueError("quarantine root identity changed")
        if observed.st_dev != identity.device or _mount_id(fd) != identity.mount_id:
            raise ValueError("quarantine entry crosses a mount")
        if (not any(start <= observed.st_uid < start + count for start, count in identity.uid_ranges)
            or not any(start <= observed.st_gid < start + count for start, count in identity.gid_ranges)):
            raise ValueError("quarantine entry ownership is outside retained mappings")
        return observed

    def same(left: os.stat_result, right: os.stat_result) -> bool:
        return (left.st_dev, left.st_ino, left.st_mode, left.st_uid, left.st_gid) == (
            right.st_dev, right.st_ino, right.st_mode, right.st_uid, right.st_gid)

    def visit(parent: int, name: str, *, depth: int) -> None:
        nonlocal entries
        budget(depth)
        checked(descriptor, root=True)
        node = os.open(name, os.O_PATH | os.O_NOFOLLOW | os.O_CLOEXEC, dir_fd=parent)
        try:
            observed = checked(node)
            current = os.stat(name, dir_fd=parent, follow_symlinks=False)
            if not same(observed, current):
                raise ValueError("quarantine entry identity changed")
            if depth == 0 and name == "recovery.json":
                if not stat.S_ISREG(observed.st_mode) or observed.st_nlink != 1:
                    raise ValueError("quarantine locator identity changed")
                return
            entries += 1
            budget(depth)
            if not stat.S_ISDIR(observed.st_mode):
                os.unlink(name, dir_fd=parent)
                return
            directory = os.open(name, _DIRECTORY, dir_fd=parent)
            try:
                if not same(observed, checked(directory)):
                    raise ValueError("quarantine directory identity changed")
                walk(directory, depth + 1)
                budget(depth)
                if not same(observed, os.stat(name, dir_fd=parent, follow_symlinks=False)):
                    raise ValueError("quarantine directory identity changed before removal")
                os.rmdir(name, dir_fd=parent)
            finally:
                os.close(directory)
        finally:
            os.close(node)

    def walk(parent: int, depth: int) -> None:
        names: list[str] = []
        more = False
        with os.scandir(parent) as children:
            for child in children:
                if depth == 0 and child.name == "recovery.json":
                    continue
                budget(depth)
                if len(names) >= _MAX_ENTRIES - entries:
                    more = True
                    break
                names.append(child.name)
        for name in names:
            visit(parent, name, depth=depth)
        os.fsync(parent)
        if more:
            raise ValueError("quarantine pruning exceeds bound; partial progress retained")

    checked(descriptor, root=True)
    visit(descriptor, "recovery.json", depth=0)
    walk(descriptor, 0)
    checked(descriptor, root=True)
