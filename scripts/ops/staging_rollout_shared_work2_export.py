#!/usr/bin/python3
"""Converge the one fixed platform-dev client allowance on the GB10 NFS exporter."""

from __future__ import annotations

import argparse
import ctypes
import errno
import os
import re
import secrets
import stat
import subprocess
import sys
from collections.abc import Callable, Sequence
from pathlib import Path

try:
    from scripts.ops.staging_rollout_sealed_source import (
        SealedSource,
        SealedSourceError,
        validate_sealed_source,
    )
except ModuleNotFoundError:  # direct execution from scripts/ops
    from staging_rollout_sealed_source import (
        SealedSource,
        SealedSourceError,
        validate_sealed_source,
    )  # type: ignore[import-not-found, no-redef]

REPO_ROOT = Path(__file__).resolve().parents[2]
EXPORT_ROOT = Path("/shared_work2")
CLIENT = "192.168.50.103/32"
EXPORTS_DIRECTORY = Path("/etc/exports.d")
EXPORTS_PATH = EXPORTS_DIRECTORY / "loom-staging-rollout-platform-dev.exports"
EXPORTFS = Path("/usr/sbin/exportfs")
ASSET = REPO_ROOT / "deploy/worker-pools/gb10/loom-staging-rollout-platform-dev.exports"
EXPECTED_OPTIONS = frozenset(
    {
        "rw",
        "sync",
        "wdelay",
        "hide",
        "nocrossmnt",
        "secure",
        "no_root_squash",
        "no_all_squash",
        "no_subtree_check",
        "secure_locks",
        "acl",
        "no_pnfs",
        "anonuid=65534",
        "anongid=65534",
        "sec=sys",
    }
)
_DIRECTORY_FLAGS = (
    os.O_RDONLY | os.O_DIRECTORY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
)


class ExportError(RuntimeError):
    """A bounded export-contract failure safe for operator output."""


CommandRunner = Callable[[Sequence[str]], subprocess.CompletedProcess[str]]


def _run(argv: Sequence[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(argv, check=False, text=True, capture_output=True)


def _asset_payload() -> bytes:
    try:
        payload = ASSET.read_bytes()
    except OSError as exc:
        raise ExportError("shared_work2 export asset is unavailable") from exc
    try:
        line = payload.decode("ascii").strip()
    except UnicodeDecodeError as exc:
        raise ExportError("shared_work2 export asset is invalid") from exc
    match = re.fullmatch(r"/shared_work2 192[.]168[.]50[.]103/32\(([^()]*)\)", line)
    if match is None or frozenset(match.group(1).split(",")) != EXPECTED_OPTIONS:
        raise ExportError("shared_work2 export asset is invalid")
    return payload


def _validate_directory(path: Path) -> int:
    try:
        fd = os.open(path, _DIRECTORY_FLAGS)
        metadata = os.fstat(fd)
        lexical = os.lstat(path)
    except OSError as exc:
        raise ExportError("exports.d authority is unavailable") from exc
    if (
        not stat.S_ISDIR(metadata.st_mode)
        or stat.S_ISLNK(lexical.st_mode)
        or metadata.st_uid != 0
        or metadata.st_gid != 0
        or stat.S_IMODE(metadata.st_mode) != 0o755
        or (metadata.st_dev, metadata.st_ino) != (lexical.st_dev, lexical.st_ino)
    ):
        os.close(fd)
        raise ExportError("exports.d authority is unsafe")
    return fd


def _file_is_exact(payload: bytes) -> bool:
    try:
        metadata = os.lstat(EXPORTS_PATH)
        content = EXPORTS_PATH.read_bytes()
    except FileNotFoundError:
        return False
    except OSError as exc:
        raise ExportError("platform-dev export allowance is unreadable") from exc
    if (
        not stat.S_ISREG(metadata.st_mode)
        or stat.S_ISLNK(metadata.st_mode)
        or metadata.st_uid != 0
        or metadata.st_gid != 0
        or stat.S_IMODE(metadata.st_mode) != 0o644
        or metadata.st_nlink != 1
        or content != payload
    ):
        raise ExportError("platform-dev export allowance drifted")
    return True


def _rename_noreplace(directory_fd: int, source: str, destination: str) -> None:
    libc = ctypes.CDLL(None, use_errno=True)
    try:
        rename = libc.renameat2
    except AttributeError as exc:
        raise ExportError("atomic export publication is unavailable") from exc
    rename.argtypes = [ctypes.c_int, ctypes.c_char_p, ctypes.c_int, ctypes.c_char_p, ctypes.c_uint]
    rename.restype = ctypes.c_int
    if rename(directory_fd, os.fsencode(source), directory_fd, os.fsencode(destination), 1) == 0:
        return
    error_number = ctypes.get_errno()
    if error_number in {errno.EEXIST, errno.ENOTEMPTY}:
        raise FileExistsError(error_number, "destination exists")
    raise ExportError("atomic export publication failed safely")


def _install_file(payload: bytes) -> bool:
    directory_fd = _validate_directory(EXPORTS_DIRECTORY)
    temp_name = f".{EXPORTS_PATH.name}.tmp-{secrets.token_hex(16)}"
    temp_fd: int | None = None
    try:
        if _file_is_exact(payload):
            return False
        temp_fd = os.open(
            temp_name,
            os.O_WRONLY
            | os.O_CREAT
            | os.O_EXCL
            | getattr(os, "O_CLOEXEC", 0)
            | getattr(os, "O_NOFOLLOW", 0),
            0o600,
            dir_fd=directory_fd,
        )
        os.write(temp_fd, payload)
        os.fchown(temp_fd, 0, 0)
        os.fchmod(temp_fd, 0o644)
        os.fsync(temp_fd)
        os.close(temp_fd)
        temp_fd = None
        try:
            _rename_noreplace(directory_fd, temp_name, EXPORTS_PATH.name)
        except FileExistsError:
            os.unlink(temp_name, dir_fd=directory_fd)
            if not _file_is_exact(payload):  # pragma: no cover - exact helper owns this
                raise ExportError(
                    "platform-dev export allowance raced with another writer"
                ) from None
            return False
        os.fsync(directory_fd)
        if not _file_is_exact(payload):  # pragma: no cover - publication invariant
            raise ExportError("platform-dev export allowance did not converge")
        return True
    finally:
        if temp_fd is not None:
            os.close(temp_fd)
        try:
            os.unlink(temp_name, dir_fd=directory_fd)
        except FileNotFoundError:
            pass
        os.close(directory_fd)


def _export_is_active(run: CommandRunner) -> bool:
    result = run([str(EXPORTFS), "-v"])
    if result.returncode != 0:
        return False
    normalized = " ".join(result.stdout.split())
    match = re.search(
        r"(?:^| )/shared_work2\s+192[.]168[.]50[.]103/32\(([^()]*)\)",
        normalized,
    )
    return bool(match is not None and EXPECTED_OPTIONS.issubset(match.group(1).split(",")))


def converge(*, install: bool, run: CommandRunner = _run) -> bool:
    payload = _asset_payload()
    if install:
        if os.geteuid() != 0:
            raise ExportError("export installation requires root")
        changed = _install_file(payload)
        refreshed = run([str(EXPORTFS), "-ra"])
        if refreshed.returncode != 0:
            raise ExportError("NFS export refresh failed safely")
    else:
        if not _file_is_exact(payload):
            raise ExportError("platform-dev export allowance is not installed")
        changed = False
    if not _export_is_active(run):
        raise ExportError("platform-dev export allowance is not active")
    return changed


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("check", "install"))
    parser.add_argument("--sealed-source-sha")
    parser.add_argument("--sealed-source-tree")
    parser.add_argument("--sealed-approved-base-sha")
    args = parser.parse_args(argv)
    try:
        sealed_values = (
            args.sealed_source_sha,
            args.sealed_source_tree,
            args.sealed_approved_base_sha,
        )
        if args.command == "install" and any(value is None for value in sealed_values):
            raise ExportError("export installation requires an exact sealed source binding")
        if any(value is not None for value in sealed_values):
            if any(value is None for value in sealed_values):
                raise ExportError("sealed source binding is incomplete")
            source = SealedSource(
                path=REPO_ROOT,
                commit_sha=args.sealed_source_sha,
                tree_sha=args.sealed_source_tree,
                base_sha=args.sealed_approved_base_sha,
            )
            validate_sealed_source(source)
        changed = converge(install=args.command == "install")
    except (ExportError, OSError, SealedSourceError):
        print("error: shared_work2 export check failed safely", file=sys.stderr)
        return 1
    print("changed" if changed else "ok")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
