"""Bounded, manifest-bound directory snapshots for independent verification.

Only task-declared directory roots are restored. A root may be absent, must not
traverse symlinks, and may contain only ordinary files/directories and links
contained within that root or exact declared external executable leaves.
External references must match the fresh verifier before restore. Processes
must be quiesced by the caller.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import shlex
import tarfile
from pathlib import Path, PurePosixPath
from typing import TYPE_CHECKING, Protocol, runtime_checkable

from loom.errors import DriverError
from loom.mutable_paths import (
    MAX_MUTABLE_BYTES,
    MAX_MUTABLE_ENTRIES,
    validate_mutable_paths,
    validate_mutable_reference_files,
    validate_reference_file_symlinks,
)
from loom.trial.workspace import WorkspaceStagingPolicy
from loom.trial.workspace_snapshot import (
    WorkspaceSnapshotError,
    _export_workspace_archive,
    _import_workspace_archive,
    _validate_workspace_archive,
)

if TYPE_CHECKING:
    from loom.driver.base import Driver

_POLICY = WorkspaceStagingPolicy((".loom/**",), (".loom/**",), ())


def _archive_evidence(
    archive: Path, root: PurePosixPath | None = None,
    reference_files: tuple[PurePosixPath, ...] = (),
) -> dict[str, int | str]:
    if archive.is_symlink() or not archive.is_file():
        raise WorkspaceSnapshotError("mutable path archive is not a regular file")
    size = archive.stat().st_size
    if size > MAX_MUTABLE_BYTES:
        raise WorkspaceSnapshotError("mutable path archive exceeds 256 MiB limit")
    expanded = count = 0
    try:
        with tarfile.open(archive) as stream:
            for member in stream:
                count += 1
                expanded += member.size
                if count > MAX_MUTABLE_ENTRIES or expanded > MAX_MUTABLE_BYTES:
                    raise WorkspaceSnapshotError("mutable path archive exceeds content limits")
        _validate_workspace_archive(archive, _POLICY, root=root,
                                    external_reference_files=frozenset(reference_files))
        with archive.open("rb") as stream:
            digest = hashlib.file_digest(stream, "sha256").hexdigest()
    except (tarfile.TarError, OSError) as exc:
        raise WorkspaceSnapshotError("mutable path archive is unreadable") from exc
    return {"size_bytes": size, "expanded_bytes": expanded, "entries": count, "sha256": digest}


@runtime_checkable
class _ReferenceInspector(Protocol):
    async def inspect_reference_file(
        self, path: PurePosixPath, *, max_bytes: int,
    ) -> dict[str, int | str]: ...


@runtime_checkable
class _SymlinkInspector(Protocol):
    async def inspect_reference_symlink(self, path: PurePosixPath) -> str: ...


async def _reference_evidence(
    driver: Driver, references: tuple[PurePosixPath, ...],
    *, reference_symlinks: dict[str, str] | None = None,
) -> list[dict[str, int | str]]:
    """Inspect through trusted native RPC, never task-modifiable userland."""
    if not references:
        return []
    if not isinstance(driver, _ReferenceInspector):
        raise WorkspaceSnapshotError("driver lacks trusted mutable path reference inspection")
    aliases = {str(path): reference_symlinks[str(path)] for path in references
               if reference_symlinks and str(path) in reference_symlinks}
    validate_reference_file_symlinks(aliases, groups=(references,))
    if aliases and not isinstance(driver, _SymlinkInspector):
        raise WorkspaceSnapshotError("driver lacks trusted reference symlink inspection")
    records: list[dict[str, int | str]] = []
    remaining = MAX_MUTABLE_BYTES
    for path in references:
        try:
            if str(path) in aliases:
                assert isinstance(driver, _SymlinkInspector)
                target = await driver.inspect_reference_symlink(path)
                if target != aliases[str(path)]:
                    raise WorkspaceSnapshotError(f"reference symlink differs from declaration: {path}")
                records.append({"path": str(path), "target": target})
                continue
            record = await driver.inspect_reference_file(path, max_bytes=remaining)
        except DriverError as exc:
            raise WorkspaceSnapshotError(f"cannot fingerprint mutable path reference file: {path}") from exc
        remaining -= int(record["size_bytes"])
        records.append(record)
    return records


async def _check_root(
    driver: Driver, root: PurePosixPath, *, writable: bool = False, removing: bool = False,
) -> bool:
    """Return presence after rejecting links, nondirectories and inaccessible ancestors."""
    components = (*reversed(root.parents), root)
    checks = []
    for path in components:
        quoted = shlex.quote(str(path))
        accessible = "" if removing and path == root else f" && test -x {quoted}"
        checks.append(f"test ! -L {quoted} && (test ! -e {quoted} || (test -d {quoted}{accessible}))")
    quoted = shlex.quote(str(root))
    permission = f"test {'-w' if writable else '-r'} {quoted}"
    if removing:
        # Removing an empty directory needs write/search on its parent, not the
        # leaf. Nonempty trees can still fail explicitly during recursive removal.
        permission = f"test -w {shlex.quote(str(root.parent))}"
    checks.append(f"if test ! -e {quoted}; then exit 3; else {permission}; fi")
    result = await driver.exec(" && ".join(checks))
    if result.return_code not in (0, 3) or result.stderr or result.truncated:
        raise WorkspaceSnapshotError(
            f"mutable path must be an accessible directory without symlink ancestors: {root}",
        )
    return result.return_code == 0


def _empty_archive(archive: Path) -> None:
    # Keep the required execution-output contract, without inventing a directory.
    archive.unlink(missing_ok=True)
    with tarfile.open(archive, "w"):
        pass


def _manifest_version(records: list[dict[str, int | str]], *, has_references: bool) -> int:
    return 3 if any(record.get("state") == "absent" for record in records) else 2 if has_references else 1


def _check_totals(records: list[dict[str, int | str]]) -> None:
    for key, limit in (("size_bytes", MAX_MUTABLE_BYTES), ("expanded_bytes", MAX_MUTABLE_BYTES),
                       ("entries", MAX_MUTABLE_ENTRIES)):
        if sum(int(record[key]) for record in records) > limit:
            raise WorkspaceSnapshotError("declared mutable paths exceed aggregate snapshot limits")


async def _check_cross_root_hardlinks(driver: Driver, roots: tuple[PurePosixPath, ...]) -> None:
    # Each archive preserves links within its root. Links between archives
    # cannot be restored faithfully, including links into the workdir archive.
    seen: set[bytes] = set()
    for root in roots:
        result = await driver.exec(
            f"find {shlex.quote(str(root))} -type f -links +1 "
            "-exec stat -c '%d:%i' -- {} +",
        )
        identities = set(result.stdout.splitlines())
        if (result.return_code or result.stderr or result.truncated
                or any(len(parts := value.split(b":")) != 2
                       or not all(part.isdigit() for part in parts)
                       for value in identities)):
            raise WorkspaceSnapshotError(f"cannot inspect mutable path hardlinks: {root}")
        if seen & identities:
            raise WorkspaceSnapshotError("hardlinks across declared roots or workdir cannot be preserved")
        seen.update(identities)


async def export_mutable_paths(
    driver: Driver, paths: tuple[PurePosixPath, ...], directory: Path, *, workdir: PurePosixPath,
    preserve_acls: bool = False,
    reference_files: tuple[PurePosixPath, ...] = (),
    reference_symlinks: dict[str, str] | None = None,
) -> None:
    validate_mutable_paths(paths, workdir=workdir)
    validate_mutable_reference_files(reference_files, paths=paths, workdir=workdir)
    directory.mkdir(parents=True, exist_ok=True)
    records: list[dict[str, int | str]] = []
    # Never leave an old manifest certifying an incomplete newer export.
    manifest = directory / "manifest.json"
    manifest.unlink(missing_ok=True)
    present = {root: await _check_root(driver, root) for root in paths}
    if paths:
        await _check_cross_root_hardlinks(driver, (workdir, *(root for root in paths if present[root])))
    for index, root in enumerate(paths):
        archive = directory / f"{index}.tar"
        if present[root]:
            await _export_workspace_archive(driver, root, archive, preserve_acls=preserve_acls)
        else:
            await asyncio.to_thread(_empty_archive, archive)
        evidence = await asyncio.to_thread(_archive_evidence, archive, root, reference_files)
        records.append({"path": str(root), "archive": archive.name, **evidence,
                        **({"state": "absent"} if not present[root] else {})})
        _check_totals(records)
    # Export commands execute task-owned utilities. Inspect references only
    # after every source command has completed.
    references = await _reference_evidence(driver, reference_files, reference_symlinks=reference_symlinks)
    temporary = directory / "manifest.json.tmp"
    document = {"schema_version": _manifest_version(records, has_references=bool(reference_files)), "paths": records,
                **({"reference_files": references} if reference_files else {})}
    temporary.write_text(json.dumps(document, sort_keys=True) + "\n")
    temporary.replace(manifest)


async def import_mutable_paths(
    driver: Driver, paths: tuple[PurePosixPath, ...], directory: Path, *, workdir: PurePosixPath,
    preserve_acls: bool = False,
    reference_files: tuple[PurePosixPath, ...] = (),
    reference_symlinks: dict[str, str] | None = None,
) -> None:
    validate_mutable_paths(paths, workdir=workdir)
    validate_mutable_reference_files(reference_files, paths=paths, workdir=workdir)
    from loom.trial.workspace_acls import check_acl_declaration, require_acl_support
    manifest = directory / "manifest.json"
    try:
        if manifest.is_symlink() or manifest.stat().st_size > 64 * 1024:
            raise ValueError("invalid manifest file")
        declared = json.loads(manifest.read_text())
        if (not isinstance(declared, dict) or not isinstance(declared.get("paths"), list)
                or len(declared["paths"]) != len(paths)
                or any(not isinstance(record, dict) for record in declared["paths"])):
            raise ValueError("invalid manifest paths")
    except (OSError, ValueError) as exc:
        raise WorkspaceSnapshotError("mutable paths manifest is missing or invalid") from exc
    records: list[dict[str, int | str]] = []
    references = await _reference_evidence(driver, reference_files, reference_symlinks=reference_symlinks)
    # Validate every archive before changing any verifier directory.
    for index, root in enumerate(paths):
        archive = directory / f"{index}.tar"
        evidence = await asyncio.to_thread(_archive_evidence, archive, root, reference_files)
        await asyncio.to_thread(check_acl_declaration, archive, preserve_acls=preserve_acls)
        absent = declared["paths"][index].get("state") == "absent"
        if absent and evidence["entries"] != 0:
            raise WorkspaceSnapshotError("absent mutable path archive must be empty")
        records.append({"path": str(root), "archive": archive.name, **evidence,
                        **({"state": "absent"} if absent else {})})
        _check_totals(records)
    expected = {"schema_version": _manifest_version(records, has_references=bool(reference_files)), "paths": records,
                **({"reference_files": references} if reference_files else {})}
    if declared != expected:
        raise WorkspaceSnapshotError("mutable paths manifest differs from declared paths, reference files or archive content")
    identity = await driver.exec("id -u; id -g")
    try:
        uid, gid = (int(value) for value in identity.stdout.split())
        if identity.return_code:
            raise ValueError("identity failed")
    except ValueError as exc:
        raise WorkspaceSnapshotError("cannot determine mutable path restore identity") from exc
    present = {}
    for index, root in enumerate(paths):
        present[root] = await _check_root(
            driver, root, writable=True, removing=records[index].get("state") == "absent",
        )
        if records[index].get("state") == "absent":
            continue
        if preserve_acls:
            await require_acl_support(driver, root)
        if uid != 0:
            with tarfile.open(directory / f"{index}.tar") as stream:
                if any(member.uid != uid or member.gid != gid for member in stream):
                    raise WorkspaceSnapshotError(f"mutable path ownership cannot be preserved by verifier: {root}")
    for index, root in enumerate(paths):
        if records[index].get("state") == "absent":
            if not present[root]:
                continue
            quoted = shlex.quote(str(root))
            # rmdir needs no leaf read/search permission for an empty directory;
            # recursive rm does, even when no child exists.
            result = await driver.exec(f"rmdir -- {quoted} 2>/dev/null || rm -rf -- {quoted}")
            if result.return_code or result.stderr or result.truncated:
                raise WorkspaceSnapshotError(f"cannot remove absent verifier mutable directory: {root}")
            continue
        # A fresh verifier has image-owned baseline files. Replace the declared
        # contents, so agent deletions cannot silently reappear during grading.
        result = await driver.exec(
            f"mkdir -p {shlex.quote(str(root))} && "
            f"find {shlex.quote(str(root))} -mindepth 1 -maxdepth 1 -exec rm -rf -- {{}} +",
        )
        if result.return_code or result.stderr:
            raise WorkspaceSnapshotError(f"cannot replace verifier mutable directory: {root}")
        await _import_workspace_archive(
            driver, directory / f"{index}.tar", root, preserve_acls=preserve_acls,
        )
    if await _reference_evidence(driver, reference_files, reference_symlinks=reference_symlinks) != references:
        raise WorkspaceSnapshotError("mutable path reference changed during restore")
