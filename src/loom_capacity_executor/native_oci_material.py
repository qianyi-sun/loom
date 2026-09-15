"""Fresh mapped OCI material, not release provenance or execution authority.

The caller authenticates the rootfs before mapping and restores its published
capabilities before this step. No feature is running during preparation. Failure
removes only recorded resources; abrupt death still requires allocation recovery.
"""

from __future__ import annotations

import os
import stat
from contextlib import ExitStack
from pathlib import Path

from loom_capacity_agent.build_admission import BuildSourceContextV1
from loom_capacity_executor.native_mapper_capabilities import _require_mapped_root
from loom_capacity_executor.native_oci_bundles import (
    NativeOciBundlePolicy,
    NativeOciBundles,
    render_native_oci_bundles,
)
from loom_capacity_executor.native_rootfs_archive import _cleanup, _identity

_DIRECTORY = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC


def _open_directory(path: Path, stack: ExitStack) -> int:
    """Reject symlinks in every component, including caller-supplied parents."""
    descriptor = os.open("/", _DIRECTORY)
    try:
        for part in path.parts[1:]:
            child = os.open(part, _DIRECTORY, dir_fd=descriptor)
            os.close(descriptor)
            descriptor = child
    except BaseException:
        os.close(descriptor)
        raise
    stack.callback(os.close, descriptor)
    return descriptor


def _metadata(descriptor: int, mode: int, uid: int = 0, gid: int = 0) -> None:
    observed = os.fstat(descriptor)
    if (not stat.S_ISDIR(observed.st_mode) or stat.S_IMODE(observed.st_mode) != mode
        or (observed.st_uid, observed.st_gid) != (uid, gid)):
        raise ValueError("native OCI directory metadata changed")


def prepare_native_oci_material(context: BuildSourceContextV1, *,
    policy: NativeOciBundlePolicy, bundle_root: Path,
) -> NativeOciBundles:
    """Create fixed readonly bundles and private mapped output without reuse.

Workspace and bundle parent must already be private mapped-root directories.
Staged input is neither modified nor owned by this helper. Returned bytes prove
only local assembly, never permission to execute or physical capacity release.
"""
    _require_mapped_root()
    bundles = render_native_oci_bundles(context, policy)
    if (not isinstance(bundle_root, Path) or not bundle_root.is_absolute()
        or bundle_root == Path("/") or ".." in bundle_root.parts
        or any(c in str(bundle_root) for c in ("\x00", "\n", "\r"))
        or any(bundle_root == path or bundle_root in path.parents or path in bundle_root.parents
            for path in (policy.workspace, policy.rootfs))):
        raise ValueError("native OCI bundle path must be disjoint")
    with ExitStack() as stack:
        workspace = _open_directory(policy.workspace, stack)
        parent = _open_directory(bundle_root.parent, stack)
        rootfs = _open_directory(policy.rootfs, stack)
        for descriptor in (workspace, parent):
            _metadata(descriptor, 0o700)
        _metadata(rootfs, 0o755)
        # Keep descriptors open through final readback and failure cleanup.
        roots: list[tuple[int, int, str, dict[tuple[str, ...], tuple[int, int]]]] = []
        directories: list[tuple[int, str, int, int, int]] = []

        def directory(ancestor: int, name: str, mode: int, uid: int = 0) -> int:
            os.mkdir(name, 0o700, dir_fd=ancestor)
            created_identity = _identity(os.stat(name, dir_fd=ancestor, follow_symlinks=False))
            descriptor = os.open(name, _DIRECTORY, dir_fd=ancestor)
            stack.callback(os.close, descriptor)
            if _identity(os.fstat(descriptor)) != created_identity:
                raise ValueError("native OCI directory changed before open")
            _metadata(descriptor, 0o700)
            directories.append((ancestor, name, descriptor, mode, uid))
            return descriptor

        try:
            for name, mode, uid in (("output", 0o700, 1000), ("buildkit-run", 0o1777, 0)):
                descriptor = directory(workspace, name, mode, uid)
                roots.append((descriptor, workspace, name, {(): _identity(os.fstat(descriptor))}))
                os.fchown(descriptor, uid, uid)
                os.fchmod(descriptor, mode)
                os.fsync(descriptor)
            root = directory(parent, bundle_root.name, 0o555)
            created: dict[tuple[str, ...], tuple[int, int]] = {(): _identity(os.fstat(root))}
            roots.append((root, parent, bundle_root.name, created))
            files: list[tuple[int, int, bytes]] = []
            for role in ("pause", "buildkit", "client"):
                role_fd = directory(root, role, 0o555)
                created[(role,)] = _identity(os.fstat(role_fd))
                descriptor = os.open("config.json", os.O_RDWR | os.O_CREAT | os.O_EXCL
                    | os.O_NOFOLLOW | os.O_CLOEXEC, 0o600, dir_fd=role_fd)
                stack.callback(os.close, descriptor)
                created[(role, "config.json")] = _identity(os.fstat(descriptor))
                wire = getattr(bundles, role)
                if not 1 <= len(wire) <= 1024**2:
                    raise ValueError("native OCI config exceeds bound")
                view = memoryview(wire)
                while view:
                    written = os.write(descriptor, view)
                    if written <= 0:
                        raise OSError("native OCI config write did not progress")
                    view = view[written:]
                os.fchmod(descriptor, 0o444)
                os.fsync(descriptor)
                files.append((role_fd, descriptor, wire))
                os.fchmod(role_fd, 0o555)
                os.fsync(role_fd)
            os.fchmod(root, 0o555)
            os.fsync(root)
            for role_fd, descriptor, wire in files:
                observed = os.fstat(descriptor)
                path_metadata = os.stat("config.json", dir_fd=role_fd, follow_symlinks=False)
                if (_identity(path_metadata) != _identity(observed) or not stat.S_ISREG(observed.st_mode)
                    or stat.S_IMODE(observed.st_mode) != 0o444 or observed.st_nlink != 1
                    or (observed.st_uid, observed.st_gid) != (0, 0) or observed.st_size != len(wire)
                    or os.pread(descriptor, len(wire) + 1, 0) != wire):
                    raise ValueError("native OCI config changed during preparation")
            for ancestor, name, descriptor, mode, uid in directories:
                _metadata(descriptor, mode, uid, uid)
                if _identity(os.stat(name, dir_fd=ancestor, follow_symlinks=False)) != _identity(os.fstat(descriptor)):
                    raise ValueError("native OCI directory changed during preparation")
            for path, descriptor, mode in ((policy.workspace, workspace, 0o700),
                (bundle_root.parent, parent, 0o700), (policy.rootfs, rootfs, 0o755)):
                reopened = _open_directory(path, stack)
                _metadata(descriptor, mode)
                if _identity(os.fstat(reopened)) != _identity(os.fstat(descriptor)):
                    raise ValueError("native OCI parent changed during preparation")
            os.fsync(workspace)
            os.fsync(parent)
            return bundles
        except BaseException:
            for descriptor, ancestor, name, entries in reversed(roots):
                _cleanup(descriptor, ancestor, name, entries)
            raise
