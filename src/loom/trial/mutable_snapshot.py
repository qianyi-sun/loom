"""Bounded, manifest-bound directory snapshots for independent verification.

Only task-declared directory roots are restored. A root must exist, must not
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
from typing import TYPE_CHECKING

from loom.mutable_paths import (
    MAX_MUTABLE_BYTES,
    MAX_MUTABLE_ENTRIES,
    validate_mutable_paths,
    validate_mutable_reference_files,
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


async def _reference_evidence(
    driver: Driver, references: tuple[PurePosixPath, ...],
) -> list[dict[str, int | str]]:
    """Fingerprint exact executable leaves without following any symlink.

    The caller quiesces the source; the verifier is fresh and has no task
    processes. References may not overlap any directory this restore changes.
    """
    records: list[dict[str, int | str]] = []
    total = 0
    for path in references:
        quoted = shlex.quote(str(path))
        checks = [f"test ! -L {shlex.quote(str(part))}" for part in (*reversed(path.parents), path)]
        checks.extend((f"test -f {quoted}", f"test -x {quoted}",
                       f'test "$(stat -c %s {quoted})" -le {MAX_MUTABLE_BYTES}'))
        result = await driver.exec(
            " && ".join(checks) + f" && stat -c '%s %a %u %g' {quoted} && sha256sum < {quoted}",
        )
        try:
            if result.return_code or result.stderr or result.truncated:
                raise ValueError("inspection failed")
            size, mode, uid, gid, digest, marker = result.stdout.decode("ascii").split()
            if marker != "-" or len(digest) != 64 or any(c not in "0123456789abcdef" for c in digest):
                raise ValueError("invalid fingerprint")
            record: dict[str, int | str] = {"path": str(path), "size_bytes": int(size),
                "mode": int(mode, 8), "uid": int(uid), "gid": int(gid), "sha256": digest}
            total += int(size)
            if total > MAX_MUTABLE_BYTES:
                raise ValueError("reference files exceed bounded inspection size")
        except (ValueError, UnicodeError) as exc:
            raise WorkspaceSnapshotError(f"cannot fingerprint mutable path reference file: {path}") from exc
        records.append(record)
    return records


async def _check_root(driver: Driver, root: PurePosixPath, *, writable: bool = False) -> None:
    components = (*reversed(root.parents), root)
    checks = [f"test ! -L {shlex.quote(str(path))}" for path in components]
    quoted = shlex.quote(str(root))
    if writable:
        checks.append(f"(test ! -e {quoted} || (test -d {quoted} && test -w {quoted}))")
    else:
        checks.extend((f"test -d {quoted}", f"test -r {quoted}"))
    result = await driver.exec(" && ".join(checks))
    if result.return_code:
        raise WorkspaceSnapshotError(
            f"mutable path must be an accessible directory without symlink ancestors: {root}",
        )


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
) -> None:
    validate_mutable_paths(paths, workdir=workdir)
    validate_mutable_reference_files(reference_files, paths=paths, workdir=workdir)
    directory.mkdir(parents=True, exist_ok=True)
    records: list[dict[str, int | str]] = []
    # Never leave an old manifest certifying an incomplete newer export.
    manifest = directory / "manifest.json"
    manifest.unlink(missing_ok=True)
    references = await _reference_evidence(driver, reference_files)
    for root in paths:
        await _check_root(driver, root)
    if paths:
        await _check_cross_root_hardlinks(driver, (workdir, *paths))
    for index, root in enumerate(paths):
        archive = directory / f"{index}.tar"
        await _export_workspace_archive(driver, root, archive, preserve_acls=preserve_acls)
        evidence = await asyncio.to_thread(_archive_evidence, archive, root, reference_files)
        records.append({"path": str(root), "archive": archive.name, **evidence})
        _check_totals(records)
    temporary = directory / "manifest.json.tmp"
    document = {"schema_version": 2 if reference_files else 1, "paths": records,
                **({"reference_files": references} if reference_files else {})}
    temporary.write_text(json.dumps(document, sort_keys=True) + "\n")
    temporary.replace(manifest)


async def import_mutable_paths(
    driver: Driver, paths: tuple[PurePosixPath, ...], directory: Path, *, workdir: PurePosixPath,
    preserve_acls: bool = False,
    reference_files: tuple[PurePosixPath, ...] = (),
) -> None:
    validate_mutable_paths(paths, workdir=workdir)
    validate_mutable_reference_files(reference_files, paths=paths, workdir=workdir)
    from loom.trial.workspace_acls import check_acl_declaration, require_acl_support
    manifest = directory / "manifest.json"
    try:
        if manifest.is_symlink() or manifest.stat().st_size > 64 * 1024:
            raise ValueError("invalid manifest file")
        declared = json.loads(manifest.read_text())
    except (OSError, ValueError) as exc:
        raise WorkspaceSnapshotError("mutable paths manifest is missing or invalid") from exc
    records: list[dict[str, int | str]] = []
    references = await _reference_evidence(driver, reference_files)
    # Validate every archive before changing any verifier directory.
    for index, root in enumerate(paths):
        archive = directory / f"{index}.tar"
        evidence = await asyncio.to_thread(_archive_evidence, archive, root, reference_files)
        await asyncio.to_thread(check_acl_declaration, archive, preserve_acls=preserve_acls)
        records.append({"path": str(root), "archive": archive.name, **evidence})
        _check_totals(records)
    expected = {"schema_version": 2 if reference_files else 1, "paths": records,
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
    for index, root in enumerate(paths):
        await _check_root(driver, root, writable=True)
        if preserve_acls:
            await require_acl_support(driver, root)
        if uid != 0:
            with tarfile.open(directory / f"{index}.tar") as stream:
                if any(member.uid != uid or member.gid != gid for member in stream):
                    raise WorkspaceSnapshotError(f"mutable path ownership cannot be preserved by verifier: {root}")
    for index, root in enumerate(paths):
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
