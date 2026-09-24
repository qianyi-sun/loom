"""Bind exact image references to a validated public workspace archive."""

from __future__ import annotations

import asyncio
import json
from pathlib import Path, PurePosixPath
from typing import TYPE_CHECKING

from loom.mutable_paths import validate_workspace_reference_files
from loom.trial.mutable_snapshot import _archive_evidence, _reference_evidence
from loom.trial.workspace_snapshot import (
    WorkspaceSnapshotError,
    _import_workspace_archive,
    _validate_workspace_archive,
)

if TYPE_CHECKING:
    from loom.driver.base import Driver
    from loom.trial.workspace import WorkspaceStagingPolicy


async def _document(
    driver: Driver, archive: Path, root: PurePosixPath, policy: WorkspaceStagingPolicy,
    reference_files: tuple[PurePosixPath, ...], reference_symlinks: dict[str, str] | None,
) -> dict[str, object]:
    validate_workspace_reference_files(reference_files, paths=(), workdir=root)
    evidence = await asyncio.to_thread(_archive_evidence, archive, root, reference_files)
    await asyncio.to_thread(_validate_workspace_archive, archive, policy, root=root,
                            external_reference_files=frozenset(reference_files))
    references = await _reference_evidence(driver, reference_files, reference_symlinks=reference_symlinks)
    return {"schema_version": 1, "root": str(root), "archive": evidence, "reference_files": references}


async def export_workspace_references(
    driver: Driver, archive: Path, *, root: PurePosixPath, policy: WorkspaceStagingPolicy,
    reference_files: tuple[PurePosixPath, ...], reference_symlinks: dict[str, str] | None = None,
) -> None:
    """Call after all source snapshot commands, while task processes are stopped."""
    manifest = archive.with_name("workspace-references.json")
    manifest.unlink(missing_ok=True)
    document = await _document(driver, archive, root, policy, reference_files, reference_symlinks)
    temporary = manifest.with_suffix(".json.tmp")
    temporary.write_text(json.dumps(document, sort_keys=True) + "\n")
    temporary.replace(manifest)


async def import_workspace_with_references(
    driver: Driver, archive: Path, root: PurePosixPath, *, policy: WorkspaceStagingPolicy,
    reference_files: tuple[PurePosixPath, ...], reference_symlinks: dict[str, str] | None = None,
    preserve_acls: bool = False,
) -> None:
    """Validate archive and fresh-image identity before any workspace replacement."""
    manifest = archive.with_name("workspace-references.json")
    try:
        if manifest.is_symlink() or not manifest.is_file() or manifest.stat().st_size > 64 * 1024:
            raise ValueError("invalid manifest")
        declared = json.loads(manifest.read_bytes())
    except (OSError, ValueError) as exc:
        raise WorkspaceSnapshotError("workspace references manifest is missing or invalid") from exc
    expected = await _document(driver, archive, root, policy, reference_files, reference_symlinks)
    if declared != expected:
        raise WorkspaceSnapshotError("workspace references manifest differs from archive or image references")
    await _import_workspace_archive(driver, archive, root, policy=policy, preserve_acls=preserve_acls,
                                    external_reference_files=frozenset(reference_files))
    observed = await _reference_evidence(driver, reference_files, reference_symlinks=reference_symlinks)
    if observed != expected["reference_files"]:
        raise WorkspaceSnapshotError("workspace image reference changed during restore")
