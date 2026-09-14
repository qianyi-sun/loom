"""Read-only cgroup nondelegation prerequisite, not terminal/quiescence proof.

The caller authenticates the full host cgroup2 mount and installed lifecycle.
Check ancestors and every existing job descendant before untrusted execution;
never try to revoke delegation after writers may already hold descriptors.
Trusted Slurm/root must preserve nondelegation for the retained job lifetime.
"""

from __future__ import annotations

import errno
import os
import stat
from contextlib import ExitStack
from pathlib import Path

from loom_capacity_executor.native_mapped_scratch import _mount_id
from loom_capacity_executor.native_oci_material import _open_directory as _open_directory

_MAX_NODES = 4096
_MAX_DEPTH = 64
_DIRECTORY = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC


def require_native_cgroup_authority(root_fd: int, *, relative: Path) -> None:
    """Refuse writable ancestor/job migration controls, ACLs, and mount drift.

    Empty cgroup.procs is deliberately irrelevant here; whole-subtree populated
    state and terminal no-future-entry fencing are separate recovery checks.
    """
    if relative.is_absolute() or not relative.parts or any(part in {".", ".."} for part in relative.parts):
        raise ValueError("native cgroup authority requires exact relative job scope")
    if len(relative.parts) > _MAX_DEPTH:
        raise ValueError("native cgroup authority exceeds depth bound")
    mount = _mount_id(root_fd)
    device = os.fstat(root_fd).st_dev
    count = 0

    def checked(fd: int, *, directory: bool) -> tuple[int, ...]:
        value = os.fstat(fd)
        if ((value.st_uid, value.st_gid) != (0, 0) or value.st_mode & 0o7022
            or (not stat.S_ISDIR(value.st_mode) if directory else not stat.S_ISREG(value.st_mode))
            or value.st_dev != device or _mount_id(fd) != mount):
            raise ValueError("native cgroup authority is delegated or crosses a mount")
        for name in ("system.posix_acl_access", "system.posix_acl_default"):
            try:
                os.getxattr(fd, name)
            except OSError as error:
                if error.errno not in {errno.ENODATA, errno.EOPNOTSUPP}:
                    raise
            else:
                raise ValueError("native cgroup authority has an ACL")
        return value.st_dev, value.st_ino, value.st_mode, value.st_uid, value.st_gid

    def controls(fd: int) -> None:
        nonlocal count
        count += 1
        if count > _MAX_NODES:
            raise ValueError("native cgroup authority exceeds node bound")
        checked(fd, directory=True)
        for name in ("cgroup.procs", "cgroup.threads"):
            control = os.open(name, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC | os.O_NONBLOCK, dir_fd=fd)
            try:
                checked(control, directory=False)
            finally:
                os.close(control)

    def walk(fd: int, depth: int) -> None:
        if depth > _MAX_DEPTH:
            raise ValueError("native cgroup authority exceeds depth bound")
        controls(fd)
        names = []
        with os.scandir(fd) as children:
            for child in children:
                if child.is_symlink():
                    raise ValueError("native cgroup authority contains a symlink")
                if child.is_dir(follow_symlinks=False):
                    names.append(child.name)
                    if count + len(names) > _MAX_NODES:
                        raise ValueError("native cgroup authority exceeds node bound")
        for name in names:
            child = os.open(name, _DIRECTORY, dir_fd=fd)
            try:
                identity = checked(child, directory=True)
                walk(child, depth + 1)
                current = os.open(name, _DIRECTORY, dir_fd=fd)
                try:
                    if identity != checked(current, directory=True):
                        raise ValueError("native cgroup authority identity changed")
                finally:
                    os.close(current)
            finally:
                os.close(child)
        checked(fd, directory=True)

    with ExitStack() as stack:
        descriptor = root_fd
        for part in relative.parts:
            controls(descriptor)
            descriptor = os.open(part, _DIRECTORY, dir_fd=descriptor)
            stack.callback(os.close, descriptor)
        walk(descriptor, len(relative.parts))
