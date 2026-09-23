"""Explicit POSIX ACL snapshot capability; no general xattr or privilege transfer."""
from __future__ import annotations

import re
import shlex
import tarfile
from pathlib import Path, PurePosixPath
from typing import TYPE_CHECKING

from loom.trial.workspace_snapshot import WorkspaceSnapshotError

if TYPE_CHECKING:
    from loom.driver.base import Driver


async def require_acl_support(driver: Driver, root: PurePosixPath) -> None:
    """Prove numeric access/default ACL roundtrip on the target filesystem.

    The native boundary uses the declared sandbox identity. Legacy POSIX
    transfer uses its existing root boundary. The temporary probe never enters
    the snapshot or supplies any task solution state.
    """
    if root.anchor != "/" or len(root.parts) < 2 or ".." in root.parts:
        raise WorkspaceSnapshotError("ACL snapshot root must be an absolute non-root directory")
    checks = " && ".join(
        f"test ! -L {shlex.quote(str(path))}" for path in (*reversed(root.parents), root)
    )
    # An agent may create a declared mutable root later; its nearest existing
    # ancestor establishes filesystem support without creating that task path.
    command = (
        f"set -eu; ({checks}) || exit 1; loom_acl_parent={shlex.quote(str(root))}; "
        'while test ! -d "$loom_acl_parent"; do loom_acl_parent=$(dirname "$loom_acl_parent"); done; '
        'loom_acl_probe=$(mktemp -d "$loom_acl_parent/.loom-acl-probe.XXXXXX"); '
        "trap 'rm -rf -- \"$loom_acl_probe\"' 0; "
        'mkdir "$loom_acl_probe/source" "$loom_acl_probe/restored"; '
        'setfacl -m u:65530:r-x,d:u::rwx,d:g::r-x,d:o::--- "$loom_acl_probe/source"; '
        'tar --acls --numeric-owner --format=pax -C "$loom_acl_probe/source" '
        '-cf "$loom_acl_probe/probe.tar" .; '
        'tar --acls --numeric-owner -C "$loom_acl_probe/restored" -xpf "$loom_acl_probe/probe.tar"; '
        'loom_acl_before=$(getfacl -cpn "$loom_acl_probe/source"); '
        'loom_acl_after=$(getfacl -cpn "$loom_acl_probe/restored"); '
        'test "$loom_acl_before" = "$loom_acl_after"'
    )
    user = None if getattr(driver, "export_workspace_archive", None) is not None else "root"
    result = await driver.exec(command, user=user)
    if result.return_code or result.stderr or result.truncated:
        raise WorkspaceSnapshotError(
            "POSIX ACL snapshots require tar --acls, getfacl/setfacl, "
            "and an ACL-capable writable filesystem",
        )


def check_acl_declaration(archive: Path, *, preserve_acls: bool) -> None:
    """Never discard ACL-bearing archive metadata through ordinary extraction."""
    if preserve_acls:
        return
    with tarfile.open(archive) as stream:
        if any(key.startswith("SCHILY.acl.") for member in stream for key in member.pax_headers):
            raise WorkspaceSnapshotError("ACL archive requires environment.preserve_acls=true")


def validate_acl_headers(member: tarfile.TarInfo) -> None:
    """Accept only numeric Linux POSIX ACLs, before destination replacement."""
    for key, value in member.pax_headers.items():
        if not key.startswith("SCHILY.acl."):
            continue
        if (key not in {"SCHILY.acl.access", "SCHILY.acl.default"}
                or not (member.isdir() or member.isreg() or member.islnk())
                or (key == "SCHILY.acl.default" and not member.isdir())):
            raise WorkspaceSnapshotError("workspace archive contains unsupported ACL metadata")
        entries: set[tuple[str, str]] = set()
        for line in value.removesuffix("\n").split("\n"):
            match = re.fullmatch(r"(user|group|mask|other):([0-9]*):[r-][w-][x-]", line)
            if match is None:
                raise WorkspaceSnapshotError("workspace archive requires numeric POSIX ACL entries")
            tag, identity = match.groups()
            entry = (tag, identity)
            if (entry in entries or (identity and (tag not in {"user", "group"}
                                                  or len(identity) > 10 or str(int(identity)) != identity
                                                  or int(identity) >= 2**32 - 1))):
                raise WorkspaceSnapshotError("workspace archive contains invalid POSIX ACL entries")
            entries.add(entry)
        if (not {("user", ""), ("group", ""), ("other", "")} <= entries
                or (any(identity for _, identity in entries) and ("mask", "") not in entries)):
            raise WorkspaceSnapshotError("workspace archive contains incomplete POSIX ACL entries")
