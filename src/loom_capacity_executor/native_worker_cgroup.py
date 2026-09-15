"""Pinned, bounded aggregate-limit readback for native worker launch.

This is a launch-time check, not permission to prepare a cgroup or evidence of
continuous containment/cleanup. The node guard owns preparation and lifetime.
"""

from __future__ import annotations

import os
import re
import stat
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass, replace
from pathlib import Path

from loom_capacity_executor.native_worker_container import (
    NativeContainerError,
    NativeWorkerAllocation,
)

_MAX_CONTROL_BYTES = 4096
_QUANTITY = re.compile(r"[1-9][0-9]{0,18}", re.ASCII)
_CPU_RANGE = re.compile(r"(0|[1-9][0-9]{0,9})(?:-(0|[1-9][0-9]{0,9}))?", re.ASCII)


def _cpu_count(value: str) -> int:
    """Count canonical nonoverlapping ranges without expanding untrusted ranges."""
    previous, count = -1, 0
    for component in value.split(","):
        match = _CPU_RANGE.fullmatch(component)
        if match is None:
            raise NativeContainerError("native cgroup cpuset is malformed")
        low, high = int(match[1]), int(match[2] or match[1])
        if low <= previous or high < low or high >= 1 << 31:
            raise NativeContainerError("native cgroup cpuset is malformed")
        count += high - low + 1
        previous = high
    return count


def _finite(value: str) -> int:
    if _QUANTITY.fullmatch(value) is None or int(value) >= 1 << 63:
        raise NativeContainerError("native cgroup limit is not finite and positive")
    return int(value)


def _directory(info: os.stat_result, trusted_uid: int) -> tuple[int, int]:
    if not stat.S_ISDIR(info.st_mode) or info.st_uid != trusted_uid or info.st_mode & 0o022:
        raise NativeContainerError("native cgroup directory is not protected")
    return info.st_dev, info.st_ino


@dataclass(frozen=True)
class NativeWorkerCgroup:
    allocation: NativeWorkerAllocation
    path: Path
    descriptor: int
    identity: tuple[int, int]
    trusted_uid: int
    directory_chain: tuple[tuple[Path, tuple[int, int]], ...]

    def _read(self, name: str) -> str:
        descriptor = os.open(name, os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW | os.O_NONBLOCK,
            dir_fd=self.descriptor)
        try:
            info = os.fstat(descriptor)
            if not stat.S_ISREG(info.st_mode) or info.st_uid != self.trusted_uid or info.st_mode & 0o022:
                raise NativeContainerError("native cgroup control is not protected")
            raw = os.read(descriptor, _MAX_CONTROL_BYTES + 1)
            if not raw or len(raw) > _MAX_CONTROL_BYTES:
                raise NativeContainerError("native cgroup control exceeds its bound")
            return raw.decode("ascii").removesuffix("\n")
        finally:
            os.close(descriptor)

    def _assert_identity(self) -> None:
        for path, identity in self.directory_chain:
            if _directory(path.lstat(), self.trusted_uid) != identity:
                raise NativeContainerError("native cgroup parent identity changed during launch")
        if (self.path.resolve(strict=True) != self.path
            or _directory(self.path.lstat(), self.trusted_uid) != self.identity
            or _directory(os.fstat(self.descriptor), self.trusted_uid) != self.identity):
            raise NativeContainerError("native cgroup identity changed during launch")

    def assert_current(self) -> None:
        """Reread the same opened allocation before consuming/using launch authority."""
        try:
            self._assert_identity()
            memory = _finite(self._read("memory.max"))
            pids = _finite(self._read("pids.max"))
            cpus = _cpu_count(self._read("cpuset.cpus.effective"))
            if (memory > self.allocation.memory_bytes or pids != self.allocation.pids_max
                or not 0 < cpus * 1000 <= self.allocation.cpu_millicores
                or self._read("memory.swap.max") != "0"):
                raise NativeContainerError("native cgroup aggregate limits differ from allocation")
            self._assert_identity()
        except (OSError, UnicodeError):
            raise NativeContainerError("native cgroup aggregate readback is unavailable") from None


@contextmanager
def open_native_cgroup(
    allocation: NativeWorkerAllocation, *, cgroup_root: Path = Path("/sys/fs/cgroup"),
    trusted_uid: int = 0,
) -> Iterator[NativeWorkerCgroup]:
    """Hold the root-owned exact job directory across the asynchronous handoff."""
    descriptor: int | None = None
    try:
        allocation = replace(allocation)  # Revalidate even a tampered frozen value.
        if cgroup_root.resolve(strict=True) != cgroup_root:
            raise NativeContainerError("native cgroup mount path is not canonical")
        descriptor = os.open(cgroup_root, os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW | os.O_DIRECTORY)
        chain = [(cgroup_root, _directory(os.fstat(descriptor), trusted_uid))]
        path = cgroup_root
        relative = Path(allocation.cgroup_parent.removeprefix("/"))
        for component in relative.parts:
            child = os.open(component, os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW | os.O_DIRECTORY,
                dir_fd=descriptor)
            os.close(descriptor)
            descriptor = child
            path /= component
            chain.append((path, _directory(os.fstat(descriptor), trusted_uid)))
        identity = _directory(os.fstat(descriptor), trusted_uid)
        opened = NativeWorkerCgroup(allocation, path, descriptor, identity, trusted_uid, tuple(chain))
        opened.assert_current()
    except OSError:
        if descriptor is not None:
            os.close(descriptor)
        raise NativeContainerError("native cgroup ancestor is unavailable") from None
    except BaseException:
        if descriptor is not None:
            os.close(descriptor)
        raise
    try:
        yield opened
    finally:
        os.close(descriptor)
