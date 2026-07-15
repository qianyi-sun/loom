"""Safe cross-driver workspace snapshots for isolated verification.

The ordinary :class:`~loom.driver.base.Driver` file transfer API intentionally
handles one regular file at a time.  Verifier handoff needs a stronger contract:
the verifier must observe the same directory, mode, symlink, and hardlink
semantics that the agent produced.  This module builds that contract on top of
the existing production Driver boundary by transferring one tar archive, then
validating the complete archive before it can be extracted in the verifier.
"""

from __future__ import annotations

import asyncio
import os
import shlex
import tarfile
from pathlib import Path, PurePosixPath
from typing import TYPE_CHECKING
from uuid import uuid4

if TYPE_CHECKING:
    from loom.driver.base import Driver
    from loom.trial.workspace import WorkspaceStagingPolicy


class WorkspaceSnapshotError(RuntimeError):
    """The agent workspace cannot be transferred without changing semantics."""


async def handoff_workspace_snapshot(
    *,
    agent_driver: Driver,
    verifier_driver: Driver,
    workdir: PurePosixPath,
    policy: WorkspaceStagingPolicy,
) -> None:
    """Copy one validated public workspace snapshot between sandbox drivers."""

    import tempfile

    with tempfile.TemporaryDirectory(prefix="loom-verifier-handoff-") as temp:
        archive = Path(temp) / "workspace.tar"
        await _export_workspace_archive(agent_driver, workdir, archive)
        await asyncio.to_thread(_strip_private_entries, archive, policy)
        await asyncio.to_thread(_validate_workspace_archive, archive, policy)
        await _import_workspace_archive(verifier_driver, archive, workdir)


async def _export_workspace_archive(
    driver: Driver,
    src: PurePosixPath,
    dst: Path,
) -> None:
    """Export via a driver-native test hook or the production POSIX boundary."""

    native = getattr(driver, "export_workspace_archive", None)
    if native is not None:
        await native(src, dst)
        return

    token = uuid4().hex
    remote_archive = PurePosixPath(f"/tmp/loom-workspace-{token}.tar")
    src_q = shlex.quote(src.as_posix())
    archive_q = shlex.quote(remote_archive.as_posix())
    # POSIX tar cannot represent sockets and some implementations silently
    # ignore them.  Detect every unsupported special entry before archiving;
    # device/FIFO entries that are representable are rejected again by the
    # local archive validator.
    special = await driver.exec(
        f"find {src_q} \\( -type b -o -type c -o -type p -o -type s \\) "
        "-print -quit",
        user="root",
    )
    if special.return_code != 0:
        raise WorkspaceSnapshotError(
            "unable to inspect agent workspace for unsupported special files",
        )
    if special.stdout:
        path = special.stdout.decode("utf-8", errors="replace").strip()
        raise WorkspaceSnapshotError(
            f"agent workspace contains unsupported device, FIFO, or socket: {path}",
        )

    try:
        result = await driver.exec(
            f"tar -C {src_q} -cf {archive_q} .",
            user="root",
        )
        if result.return_code != 0 or result.stderr:
            detail = result.stderr.decode("utf-8", errors="replace").strip()
            raise WorkspaceSnapshotError(
                "unable to create a stable agent workspace archive"
                + (f": {detail}" if detail else ""),
            )
        await driver.download(remote_archive, dst)
    finally:
        try:
            await driver.exec(f"rm -f {archive_q}", user="root")
        except Exception:
            # The sandbox lifecycle remains the authoritative cleanup for a
            # failed best-effort removal of this random /tmp file.
            pass


async def _import_workspace_archive(
    driver: Driver,
    src: Path,
    dst: PurePosixPath,
) -> None:
    """Import an already-validated archive through the production boundary."""

    native = getattr(driver, "import_workspace_archive", None)
    if native is not None:
        await native(src, dst)
        return

    token = uuid4().hex
    remote_archive = PurePosixPath(f"/tmp/loom-workspace-{token}.tar")
    archive_q = shlex.quote(remote_archive.as_posix())
    dst_q = shlex.quote(dst.as_posix())
    try:
        await driver.upload(src, remote_archive)
        result = await driver.exec(
            f"mkdir -p {dst_q} && tar -C {dst_q} -xpf {archive_q}",
            user="root",
        )
        if result.return_code != 0 or result.stderr:
            detail = result.stderr.decode("utf-8", errors="replace").strip()
            raise WorkspaceSnapshotError(
                "unable to restore agent workspace archive"
                + (f": {detail}" if detail else ""),
            )
    finally:
        try:
            await driver.exec(f"rm -f {archive_q}", user="root")
        except Exception:
            pass


def _validate_workspace_archive(
    archive: Path,
    policy: WorkspaceStagingPolicy,
) -> None:
    """Fail closed unless every archive entry is safe to overlay.

    Validation rejects ambiguous duplicate entries, traversal/absolute paths,
    every private path or link target, special files, hardlinks without a
    regular in-archive target, and entries nested below an archived symlink.
    """

    try:
        with tarfile.open(archive, mode="r:*") as tf:
            members = tf.getmembers()
    except (tarfile.TarError, OSError) as exc:
        raise WorkspaceSnapshotError("agent workspace archive is unreadable") from exc

    paths: dict[PurePosixPath, tarfile.TarInfo] = {}
    symlink_targets: dict[PurePosixPath, PurePosixPath] = {}
    hardlink_targets: dict[PurePosixPath, PurePosixPath] = {}
    for member in members:
        path = _member_path(member.name)
        if not path.parts:
            if not member.isdir():
                raise WorkspaceSnapshotError("workspace archive root must be a directory")
            continue
        if path in paths:
            raise WorkspaceSnapshotError(f"workspace archive repeats path: {path}")
        if _is_private(policy, path):
            raise WorkspaceSnapshotError(f"workspace archive contains private path: {path}")
        if not (member.isdir() or member.isreg() or member.issym() or member.islnk()):
            raise WorkspaceSnapshotError(
                f"workspace archive contains unsupported device, FIFO, or socket: {path}",
            )
        paths[path] = member
        if member.issym():
            target = _resolve_relative_target(path.parent, member.linkname)
            if _is_private(policy, target):
                raise WorkspaceSnapshotError(
                    f"workspace symlink {path} targets private path: {target}",
                )
            symlink_targets[path] = target
        elif member.islnk():
            target = _hardlink_target(member.linkname)
            if _is_private(policy, target):
                raise WorkspaceSnapshotError(
                    f"workspace hardlink {path} targets private path: {target}",
                )
            hardlink_targets[path] = target

    symlink_paths = set(symlink_targets)
    for path in paths:
        for parent in path.parents:
            if parent in symlink_paths:
                raise WorkspaceSnapshotError(
                    f"workspace archive entry {path} is nested below symlink {parent}",
                )

    for path in symlink_targets:
        _resolve_symlink_chain(path, symlink_targets, policy)

    for path, target in hardlink_targets.items():
        seen = {path}
        while target in hardlink_targets:
            if target in seen:
                raise WorkspaceSnapshotError(f"workspace hardlink cycle includes {path}")
            seen.add(target)
            target = hardlink_targets[target]
        target_member = paths.get(target)
        if target_member is None or not target_member.isreg():
            raise WorkspaceSnapshotError(
                f"workspace hardlink {path} has no regular archive target: {target}",
            )


def _strip_private_entries(
    archive: Path,
    policy: WorkspaceStagingPolicy,
) -> None:
    """Remove legitimate trusted-oracle private files before validation.

    Trusted Oracle runtimes may receive ``solution/**`` in the agent sandbox,
    but the verifier independently stages its authoritative private copy.
    Private members therefore never cross the handoff. Public links targeting
    one of those removed paths remain present and are rejected by validation.
    """

    filtered = archive.with_name(f"{archive.name}.public")
    try:
        with tarfile.open(archive, mode="r:*") as source, tarfile.open(
            filtered,
            mode="w",
            format=tarfile.PAX_FORMAT,
        ) as target:
            for member in source.getmembers():
                path = _member_path(member.name)
                if path.parts and _is_private(policy, path):
                    continue
                fileobj = source.extractfile(member) if member.isreg() else None
                target.addfile(member, fileobj)
        os.replace(filtered, archive)
    except (tarfile.TarError, OSError) as exc:
        raise WorkspaceSnapshotError("agent workspace archive is unreadable") from exc
    finally:
        filtered.unlink(missing_ok=True)


def _member_path(raw: str) -> PurePosixPath:
    path = PurePosixPath(raw)
    if path.is_absolute() or ".." in path.parts:
        raise WorkspaceSnapshotError(f"workspace archive path traverses: {raw}")
    parts = tuple(part for part in path.parts if part not in {"", "."})
    return PurePosixPath(*parts)


def _resolve_relative_target(base: PurePosixPath, raw: str) -> PurePosixPath:
    target = PurePosixPath(raw)
    if not raw or target.is_absolute():
        raise WorkspaceSnapshotError(f"workspace symlink has unsafe target: {raw}")
    stack = list(base.parts)
    for part in target.parts:
        if part in {"", "."}:
            continue
        if part == "..":
            if not stack:
                raise WorkspaceSnapshotError(
                    f"workspace symlink target escapes workdir: {raw}",
                )
            stack.pop()
        else:
            stack.append(part)
    return PurePosixPath(*stack)


def _hardlink_target(raw: str) -> PurePosixPath:
    target = _member_path(raw)
    if not target.parts:
        raise WorkspaceSnapshotError("workspace hardlink targets archive root")
    return target


def _resolve_symlink_chain(
    start: PurePosixPath,
    links: dict[PurePosixPath, PurePosixPath],
    policy: WorkspaceStagingPolicy,
) -> PurePosixPath:
    current = links[start]
    seen = {start}
    while True:
        prefix = next(
            (parent for parent in (current, *current.parents) if parent in links),
            None,
        )
        if prefix is None:
            return current
        if prefix in seen:
            raise WorkspaceSnapshotError(f"workspace symlink cycle includes {start}")
        seen.add(prefix)
        suffix = current.relative_to(prefix)
        current = links[prefix] / suffix
        if _is_private(policy, current):
            raise WorkspaceSnapshotError(
                f"workspace symlink {start} resolves to private path: {current}",
            )


def _is_private(policy: WorkspaceStagingPolicy, path: PurePosixPath) -> bool:
    if not path.parts:
        return False
    # Glob policies such as ``solution/**`` do not match the directory root
    # itself.  Probe one descendant so a link to ``solution`` is still private.
    return policy.is_private(path) or policy.is_private(path / ".loom-private-probe")


__all__ = [
    "WorkspaceSnapshotError",
    "handoff_workspace_snapshot",
]
