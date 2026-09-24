"""Find terminal file links within a bounded group of mutable archives.

This analysis grants no directory authority. Individual archive validation and
the complete manifest comparison must still succeed before any restore.
"""

from __future__ import annotations

import tarfile
from pathlib import Path, PurePosixPath

from loom.mutable_paths import MAX_MUTABLE_BYTES, MAX_MUTABLE_ENTRIES
from loom.trial.workspace_snapshot import (
    WorkspaceSnapshotError,
    _external_reference_target,
    _member_path,
)


def mutable_file_targets(
    archives: dict[PurePosixPath, Path], references: tuple[PurePosixPath, ...],
    reference_symlinks: dict[str, str] | None = None,
) -> tuple[dict[PurePosixPath, int], bool]:
    """Return exact file targets and their remaining symlink expansion counts.

    Root ancestry and declarations belong to the caller. All member structure,
    including hardlink targets and private paths, is subsequently checked by
    the ordinary archive validator. An invalid group cannot reach extraction.
    """
    members: dict[PurePosixPath, tarfile.TarInfo] = {}
    owners: dict[PurePosixPath, PurePosixPath] = {}
    count = expanded = size = 0
    try:
        for root, archive in archives.items():
            if archive.is_symlink() or not archive.is_file():
                raise WorkspaceSnapshotError("mutable path archive is not a regular file")
            size += archive.stat().st_size
            if size > MAX_MUTABLE_BYTES:
                raise WorkspaceSnapshotError("mutable path archives exceed aggregate limits")
            with tarfile.open(archive) as stream:
                for member in stream:
                    count += 1
                    expanded += member.size
                    if count > MAX_MUTABLE_ENTRIES or expanded > MAX_MUTABLE_BYTES:
                        raise WorkspaceSnapshotError("mutable path archives exceed aggregate limits")
                    relative = _member_path(member.name)
                    if not relative.parts:
                        continue
                    path = root / relative
                    if path in members:
                        raise WorkspaceSnapshotError(f"mutable archives repeat path: {path}")
                    members[path] = member
                    owners[path] = root
    except (OSError, tarfile.TarError) as exc:
        raise WorkspaceSnapshotError("mutable path archive is unreadable") from exc

    # A nominal file below a link or regular file cannot be a terminal leaf.
    symlinks = {path for path, member in members.items() if member.issym()}
    for path in members:
        if any(parent in members and not members[parent].isdir() for parent in path.parents):
            raise WorkspaceSnapshotError(f"mutable archive entry is nested below nondirectory: {path}")
    files = {path for path, member in members.items() if member.isreg() or member.islnk()}
    candidates = frozenset(files | symlinks | set(references))
    terminal = files | set(references)
    alias_depths = {PurePosixPath(path): 1 for path in (reference_symlinks or {})
                    if PurePosixPath(path) in references}
    targets = {**dict.fromkeys(files, 0), **alias_depths}
    cross_root_links = False
    for start in symlinks:
        current = start
        seen = {start}
        # A source link plus the recorded target count cannot exceed Linux's
        # 40 expansions, even when a chain crosses back into its first root.
        for depth in range(1, 41):
            root = owners[current]
            target = _external_reference_target(
                members[current].linkname, current.relative_to(root), root,
                candidates, allow_relative=True,
            )
            if target in owners and owners[target] != root:
                cross_root_links = True
            if target in terminal:
                total = depth + alias_depths.get(target, 0)
                if total <= 40:
                    targets[start] = total
                break
            if target is None or target in seen or target not in symlinks:
                break
            seen.add(target)
            current = target
    return targets, cross_root_links
