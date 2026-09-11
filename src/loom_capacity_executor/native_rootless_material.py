"""Bounded mapped preparation for V2; immutable release provenance is external.

This module cannot authenticate a host-root release from a mapped overflow UID.
The original-UID caller must verify protected material before launching. Partial
material remains owned attempt scratch after errors or death, never readiness.
"""

from __future__ import annotations

import os
from contextlib import ExitStack
from pathlib import Path
from typing import TYPE_CHECKING

from pydantic import Field

from loom_capacity_executor.native_mapper_capabilities import (
    _require_mapped_root,
    restore_native_mapper_capabilities,
)
from loom_capacity_executor.native_oci_bundles import NativeOciBundlePolicy
from loom_capacity_executor.native_oci_material import _open_directory, prepare_native_oci_material
from loom_capacity_executor.native_rootfs_archive import _identity, unpack_native_rootfs_archive
from loom_capacity_manager.contracts import Digest, StrictV1Model

if TYPE_CHECKING:
    from loom_capacity_executor.native_rootless_runtime import NativeRootlessSpecV2


class NativeRootlessMaterialV1(StrictV1Model):
    archive: str
    archive_sha256: Digest
    archive_size_bytes: int = Field(gt=0, le=8 * 1024**3)
    max_unpacked_bytes: int = Field(gt=0, le=32 * 1024**3)
    max_entries: int = Field(gt=0, le=100000)
    client_seccomp: str = Field(min_length=2, max_length=256 * 1024)
    client_seccomp_sha256: Digest
    tmp_bytes: int = Field(ge=1024**2, le=64 * 1024**3)
    buildkit_state_bytes: int = Field(ge=1024**2, le=64 * 1024**3)

    def policy(self, workspace: Path) -> NativeOciBundlePolicy:
        return NativeOciBundlePolicy(rootfs=workspace.parent / "material/rootfs", workspace=workspace,
            client_seccomp=self.client_seccomp.encode("utf-8"), client_seccomp_sha256=self.client_seccomp_sha256,
            tmp_bytes=self.tmp_bytes, buildkit_state_bytes=self.buildkit_state_bytes)


def prepare_native_rootless_material(spec: NativeRootlessSpecV2) -> None:
    """Prepare once before any broker or feature; errors retain recovery scope."""
    _require_mapped_root()
    workspace = Path(spec.workspace)
    material = workspace.parent / "material"
    with ExitStack() as stack:
        parent = _open_directory(material.parent, stack)
        metadata = os.fstat(parent)
        if ((metadata.st_uid, metadata.st_gid) != (os.geteuid(), os.getegid())
            or metadata.st_mode & 0o7777 != 0o700):
            raise ValueError("native material attempt parent must be private and owned")
        os.mkdir("material", mode=0o700, dir_fd=parent)
        created = os.stat("material", dir_fd=parent, follow_symlinks=False)
        directory = _open_directory(material, stack)
        if _identity(created) != _identity(os.fstat(directory)):
            raise ValueError("native material directory changed before preparation")

        def unchanged() -> None:
            reopened = _open_directory(material, stack)
            observed = os.fstat(reopened)
            if (_identity(observed) != _identity(created) or observed.st_mode & 0o7777 != 0o700
                or (observed.st_uid, observed.st_gid) != (os.geteuid(), os.getegid())):
                raise ValueError("native material directory changed during preparation")

        unchanged()
        unpack_native_rootfs_archive(archive=Path(spec.material.archive), destination=material / "rootfs",
            expected_sha256=spec.material.archive_sha256, expected_size_bytes=spec.material.archive_size_bytes,
            max_unpacked_bytes=spec.material.max_unpacked_bytes, max_entries=spec.material.max_entries)
        unchanged()
        restore_native_mapper_capabilities(material / "rootfs")
        unchanged()
        prepare_native_oci_material(spec.context, policy=spec.material.policy(workspace), bundle_root=material / "bundles")
        unchanged()
