"""Bounded pruning of previously captured mapped scratch, never physical release.

Capture after trusted material preparation and before any feature execution.
The caller must stop/reap its broker, confirm runtime cleanup and obtain durable
receiver acknowledgment before pruning exported output. SIGKILL recovery requires
separate protected terminal authority; this module provides none of those fences.
Runsc state is retained: its null-netns bind mount belongs to the live mapper's
mount namespace and disappears only when that namespace exits.
"""

from __future__ import annotations

import os
import stat
import time
from contextlib import ExitStack
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

from pydantic import TypeAdapter

from loom_capacity_executor.native_mapper_capabilities import _require_mapped_root
from loom_capacity_executor.native_oci_material import _open_directory

if TYPE_CHECKING:
    from loom_capacity_executor.native_rootless_runtime import NativeRootlessSpecV2

_MAX_ENTRIES = 200000
_MAX_DEPTH = 64
_MAX_SECONDS = 60
_DIRECTORY = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC


def _mount_id(descriptor: int) -> int:
    with Path(f"/proc/self/fdinfo/{descriptor}").open("rb") as source:
        wire = source.read(4097)
    matches = [line.split()[1:] for line in wire.splitlines() if line.startswith(b"mnt_id:")]
    if len(wire) > 4096 or len(matches) != 1 or len(matches[0]) != 1 or not matches[0][0].isdigit():
        raise ValueError("native scratch mount identity is unavailable")
    return int(matches[0][0])


@dataclass(frozen=True, slots=True)
class _Directory:
    path: Path
    device: int
    inode: int
    mount: int

    def check(self, descriptor: int) -> None:
        metadata = os.fstat(descriptor)
        if (not stat.S_ISDIR(metadata.st_mode)
            or (metadata.st_dev, metadata.st_ino) != (self.device, self.inode)):
            raise ValueError("native scratch directory identity changed")
        if _mount_id(descriptor) != self.mount:
            raise ValueError("native scratch mount identity changed")


def _private(descriptor: int) -> None:
    metadata = os.fstat(descriptor)
    if ((metadata.st_uid, metadata.st_gid) != (os.geteuid(), os.getegid())
        or stat.S_IMODE(metadata.st_mode) != 0o700):
        raise ValueError("native scratch parent must be private and mapped-worker-owned")


def _capture(path: Path, stack: ExitStack, *, private: bool) -> _Directory:
    descriptor = _open_directory(path, stack)
    metadata = os.fstat(descriptor)
    if private:
        _private(descriptor)
    return _Directory(path, metadata.st_dev, metadata.st_ino, _mount_id(descriptor))


@dataclass(frozen=True, slots=True)
class NativeMappedScratch:
    """In-process pre-execution observation; not serialized cleanup authority."""

    attempt: _Directory
    workspace: _Directory
    roots: tuple[_Directory, ...]


def capture_native_mapped_scratch(spec: NativeRootlessSpecV2) -> NativeMappedScratch:
    from loom_capacity_executor.native_rootless_runtime import (
        NativeRootlessSpecV2,
        NativeRootlessSpecV3,
    )

    _require_mapped_root()
    # Revalidate even a caller-created model_copy before deriving deletion paths.
    # python -m executes the specification class in __main__, distinct from
    # this canonical import. Validate serialized identity, not class identity.
    spec = TypeAdapter(NativeRootlessSpecV2 | NativeRootlessSpecV3).validate_json(spec.model_dump_json())
    workspace = Path(spec.workspace)
    with ExitStack() as stack:
        attempt = _capture(workspace.parent, stack, private=True)
        work = _capture(workspace, stack, private=True)
        if attempt.mount != work.mount:
            raise ValueError("native scratch workspace crosses a mount")
        roots = []
        for path in (workspace.parent / "material", workspace / "output", workspace / "buildkit-run"):
            if not path.exists() and not path.is_symlink():
                continue  # A later-created target is never adopted for deletion.
            root = _capture(path, stack, private=False)
            if root.mount != attempt.mount:
                raise ValueError("native scratch target crosses a mount")
            roots.append(root)
    return NativeMappedScratch(attempt, work, tuple(roots))


def clean_native_mapped_scratch(snapshot: NativeMappedScratch) -> None:
    """Prune exact captured roots once; exceptions retain all remaining scratch."""
    _require_mapped_root()
    deadline = time.monotonic() + _MAX_SECONDS
    entries = 0

    def budget(depth: int) -> None:
        if depth > _MAX_DEPTH or entries > _MAX_ENTRIES or time.monotonic() >= deadline:
            raise ValueError("native scratch cleanup exceeds bound")

    def parents() -> None:
        with ExitStack() as stack:
            for root in (snapshot.attempt, snapshot.workspace):
                descriptor = _open_directory(root.path, stack)
                root.check(descriptor)
                _private(descriptor)

    def remove(parent: int, name: str, chain: tuple[tuple[int, str, int, int], ...], depth: int,
        expected: _Directory | None = None,
    ) -> None:
        nonlocal entries
        entries += 1
        budget(depth)
        parents()
        for ancestor, part, device, inode in chain:
            metadata = os.stat(part, dir_fd=ancestor, follow_symlinks=False)
            if (metadata.st_dev, metadata.st_ino) != (device, inode) or not stat.S_ISDIR(metadata.st_mode):
                raise ValueError("native scratch path changed during deletion")
        before = os.stat(name, dir_fd=parent, follow_symlinks=False)
        descriptor = os.open(name, os.O_PATH | os.O_NOFOLLOW | os.O_CLOEXEC, dir_fd=parent)
        try:
            if expected is not None:
                expected.check(descriptor)
            opened = os.fstat(descriptor)
            if (opened.st_dev, opened.st_ino, opened.st_mode) != (before.st_dev, before.st_ino, before.st_mode):
                raise ValueError("native scratch entry changed before open")
            if _mount_id(descriptor) != snapshot.attempt.mount:
                raise ValueError("native scratch entry crosses a mount")
        finally:
            os.close(descriptor)
        if not stat.S_ISDIR(before.st_mode):
            os.unlink(name, dir_fd=parent)  # Includes links themselves, never their targets.
            return
        directory = os.open(name, _DIRECTORY, dir_fd=parent)
        try:
            observed = os.fstat(directory)
            if (observed.st_dev, observed.st_ino) != (before.st_dev, before.st_ino):
                raise ValueError("native scratch directory changed before traversal")
            if _mount_id(directory) != snapshot.attempt.mount:
                raise ValueError("native scratch traversal crosses a mount")
            os.fchmod(directory, 0o700)  # Mapped scratch only; readonly release bytes remain outside.
            names: list[str] = []
            with os.scandir(directory) as children:
                for entry in children:
                    budget(depth)
                    if len(names) + entries >= _MAX_ENTRIES:
                        raise ValueError("native scratch inventory exceeds bound")
                    names.append(entry.name)
            below = (*chain, (parent, name, before.st_dev, before.st_ino))
            for child in names:
                remove(directory, child, below, depth + 1)
            parents()
            current = os.stat(name, dir_fd=parent, follow_symlinks=False)
            if (current.st_dev, current.st_ino) != (before.st_dev, before.st_ino):
                raise ValueError("native scratch directory changed before removal")
            os.rmdir(name, dir_fd=parent)
        finally:
            os.close(directory)

    parents()
    # Reject replacements before mutating any captured root.
    with ExitStack() as stack:
        for root in snapshot.roots:
            root.check(_open_directory(root.path, stack))
    for root in snapshot.roots:
        with ExitStack() as stack:
            parent = _open_directory(root.path.parent, stack)
            root.check(_open_directory(root.path, stack))
            remove(parent, root.path.name, (), 0, expected=root)
            os.fsync(parent)
