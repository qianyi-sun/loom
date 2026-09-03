#!/usr/bin/env python3
"""Stage a verified node-guard release without activating any runtime surface."""

from __future__ import annotations

import argparse
import ctypes
import errno
import hashlib
import json
import os
import platform
import shutil
import stat
import tempfile
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import cast
from uuid import uuid4

from scripts.ops.task_image_builder_guard_release import (
    Architecture,
    GuardReleaseError,
    VerifiedGuardRelease,
    verify_release_directory,
)

_RELEASES = Path("opt/loom-task-image-builder-guard/releases")
_RECEIPTS = Path("var/lib/loom-task-image-builder-guard/staged")
_RECEIPT_SCHEMA = "loom.task-image-builder-guard-stage-receipt/v1"
_RENAME_NOREPLACE = 1
_AT_FDCWD = -100


class GuardInstallError(ValueError):
    """The release cannot be staged without weakening the inert boundary."""


@dataclass(frozen=True, slots=True)
class InstallContext:
    """Explicit target and trust anchor for one staging operation."""

    root: Path
    live: bool
    expected_release_sha256: str
    architecture: Architecture

    def __post_init__(self) -> None:
        if (
            not isinstance(self.root, Path)
            or not self.root.is_absolute()
            or len(self.expected_release_sha256) != 64
            or any(
                character not in "0123456789abcdef"
                for character in self.expected_release_sha256
            )
            or self.architecture not in {"x86_64", "aarch64"}
        ):
            raise GuardInstallError("install context is invalid")


@dataclass(frozen=True, slots=True)
class StageReceipt:
    """Non-authorizing evidence that one exact release is present on disk."""

    release_sha256: str
    architecture: Architecture
    manifest_sha256: str
    installed_path: str
    activated: bool = False
    production_ready: bool = False

    def as_dict(self) -> dict[str, object]:
        return {
            "activated": self.activated,
            "architecture": self.architecture,
            "installed_path": self.installed_path,
            "manifest_sha256": self.manifest_sha256,
            "production_ready": self.production_ready,
            "release_sha256": self.release_sha256,
            "schema": _RECEIPT_SCHEMA,
        }


def _canonical(value: object) -> bytes:
    return (
        json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True) + "\n"
    ).encode("ascii")


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(
        path,
        os.O_RDONLY | os.O_DIRECTORY | getattr(os, "O_CLOEXEC", 0),
    )
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _rename_noreplace(source: Path, destination: Path) -> None:
    libc = ctypes.CDLL(None, use_errno=True)
    renameat2 = getattr(libc, "renameat2", None)
    if renameat2 is None:
        raise GuardInstallError("atomic no-replace publication is unavailable")
    renameat2.argtypes = [ctypes.c_int, ctypes.c_char_p, ctypes.c_int, ctypes.c_char_p, ctypes.c_uint]
    renameat2.restype = ctypes.c_int
    if (
        renameat2(
            _AT_FDCWD,
            os.fsencode(source),
            _AT_FDCWD,
            os.fsencode(destination),
            _RENAME_NOREPLACE,
        )
        == 0
    ):
        return
    error = ctypes.get_errno()
    if error in {errno.EEXIST, errno.ENOTEMPTY}:
        raise GuardInstallError("release destination collision")
    raise GuardInstallError("atomic release publication failed") from OSError(
        error, os.strerror(error)
    )


def _safe_root(root: Path) -> None:
    try:
        metadata = root.lstat()
    except OSError as exc:
        raise GuardInstallError("install root is unavailable") from exc
    if root.is_symlink() or not stat.S_ISDIR(metadata.st_mode):
        raise GuardInstallError("install root is unsafe")


def _ensure_directory(
    root: Path,
    relative: Path,
    *,
    final_mode: int,
    expected_uid: int,
    expected_gid: int,
) -> Path:
    current = root
    for index, part in enumerate(relative.parts):
        current /= part
        mode = final_mode if index == len(relative.parts) - 1 else 0o755
        try:
            current.mkdir(mode=mode)
            _fsync_directory(current.parent)
        except FileExistsError:
            pass
        except OSError as exc:
            raise GuardInstallError("install directory is unavailable") from exc
        try:
            metadata = current.lstat()
        except OSError as exc:
            raise GuardInstallError("install directory is unavailable") from exc
        if (
            stat.S_ISLNK(metadata.st_mode)
            or not stat.S_ISDIR(metadata.st_mode)
            or stat.S_IMODE(metadata.st_mode) != mode
            or metadata.st_uid != expected_uid
            or metadata.st_gid != expected_gid
        ):
            raise GuardInstallError("install directory is unsafe")
    return current


def _write_file(path: Path, payload: bytes, mode: int) -> None:
    descriptor = -1
    try:
        descriptor = os.open(
            path,
            os.O_WRONLY
            | os.O_CREAT
            | os.O_EXCL
            | getattr(os, "O_CLOEXEC", 0)
            | getattr(os, "O_NOFOLLOW", 0),
            mode,
        )
        position = 0
        while position < len(payload):
            written = os.write(descriptor, payload[position:])
            if written <= 0:
                raise OSError(errno.EIO, "short write")
            position += written
        os.fsync(descriptor)
        os.fchmod(descriptor, mode)
        os.fsync(descriptor)
    except OSError as exc:
        raise GuardInstallError("staged release member could not be written") from exc
    finally:
        if descriptor >= 0:
            os.close(descriptor)


def _copy_candidate(release: VerifiedGuardRelease, releases: Path) -> Path:
    candidate = Path(tempfile.mkdtemp(prefix=".stage-", dir=releases))
    try:
        for name, mode, payload in release.members:
            _write_file(candidate / name, payload, mode)
        _write_file(candidate / "release-manifest.json", release.manifest_payload, 0o444)
        _fsync_directory(candidate)
        candidate.chmod(0o555)
        _fsync_directory(candidate)
        return candidate
    except BaseException:
        candidate.chmod(0o700)
        shutil.rmtree(candidate)
        raise


def _existing_is_exact(target: Path, context: InstallContext) -> bool:
    try:
        verify_release_directory(
            target,
            expected_release_sha256=context.expected_release_sha256,
            expected_architecture=context.architecture,
            expected_uid=0 if context.live else os.geteuid(),
        )
        return True
    except GuardReleaseError:
        return False


def _preserve_conflict(candidate: Path, releases: Path, release_sha256: str) -> None:
    conflict = releases / f".{release_sha256}.conflict.{uuid4()}"
    _rename_noreplace(candidate, conflict)
    _fsync_directory(releases)


def _receipt_for(release: VerifiedGuardRelease) -> StageReceipt:
    return StageReceipt(
        release_sha256=release.release_sha256,
        architecture=release.architecture,
        manifest_sha256=hashlib.sha256(release.manifest_payload).hexdigest(),
        installed_path=(
            f"/opt/loom-task-image-builder-guard/releases/{release.release_sha256}"
        ),
    )


def _load_existing_receipt(path: Path, expected: StageReceipt) -> StageReceipt | None:
    if not path.exists() and not path.is_symlink():
        return None
    try:
        metadata = path.lstat()
        if (
            stat.S_ISLNK(metadata.st_mode)
            or not stat.S_ISREG(metadata.st_mode)
            or metadata.st_nlink != 1
            or stat.S_IMODE(metadata.st_mode) != 0o600
            or metadata.st_size <= 0
            or metadata.st_size > 64 * 1024
        ):
            raise GuardInstallError("staging receipt is unsafe")
        payload = path.read_bytes()
        value = json.loads(payload)
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise GuardInstallError("staging receipt is invalid") from exc
    if payload != _canonical(value) or value != expected.as_dict():
        raise GuardInstallError("staging receipt differs from exact release")
    return expected


def _write_receipt(path: Path, receipt: StageReceipt) -> None:
    parent = path.parent
    temporary = parent / f".{path.name}.tmp.{uuid4()}"
    try:
        _write_file(temporary, _canonical(receipt.as_dict()), 0o600)
        _rename_noreplace(temporary, path)
        _fsync_directory(parent)
    finally:
        temporary.unlink(missing_ok=True)


def stage_guard_release(bundle: Path, context: InstallContext) -> StageReceipt:
    """Verify and stage a release while leaving every activation input absent."""

    _safe_root(context.root)
    if context.live:
        if context.root != Path("/") or os.geteuid() != 0:
            raise GuardInstallError("live staging requires root authority")
        if platform.machine() != context.architecture:
            raise GuardInstallError("live staging architecture is not native")
    elif context.root == Path("/"):
        raise GuardInstallError("non-live staging cannot target the real root")
    try:
        release = verify_release_directory(
            bundle,
            expected_release_sha256=context.expected_release_sha256,
            expected_architecture=context.architecture,
        )
    except GuardReleaseError as exc:
        raise GuardInstallError("guard release verification failed") from exc

    expected_uid = 0 if context.live else os.geteuid()
    expected_gid = 0 if context.live else os.getegid()
    releases = _ensure_directory(
        context.root,
        _RELEASES,
        final_mode=0o755,
        expected_uid=expected_uid,
        expected_gid=expected_gid,
    )
    target = releases / release.release_sha256
    candidate = _copy_candidate(release, releases)
    published = False
    preserved = False
    try:
        if target.exists() or target.is_symlink():
            if not _existing_is_exact(target, context):
                _preserve_conflict(candidate, releases, release.release_sha256)
                preserved = True
                raise GuardInstallError("release destination collision")
        else:
            try:
                _rename_noreplace(candidate, target)
                published = True
                _fsync_directory(releases)
            except GuardInstallError:
                if not (target.exists() or target.is_symlink()):
                    raise
                if not _existing_is_exact(target, context):
                    _preserve_conflict(candidate, releases, release.release_sha256)
                    preserved = True
                    raise GuardInstallError("release destination collision") from None
        receipt = _receipt_for(release)
        receipts = _ensure_directory(
            context.root,
            _RECEIPTS,
            final_mode=0o700,
            expected_uid=expected_uid,
            expected_gid=expected_gid,
        )
        receipt_path = receipts / f"{release.release_sha256}.json"
        if _load_existing_receipt(receipt_path, receipt) is None:
            _write_receipt(receipt_path, receipt)
        return receipt
    finally:
        if not published and not preserved and candidate.exists():
            candidate.chmod(0o700)
            shutil.rmtree(candidate)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bundle", type=Path, required=True)
    parser.add_argument("--release-sha256", required=True)
    parser.add_argument("--architecture", choices=("aarch64", "x86_64"), required=True)
    parser.add_argument("--root", type=Path, default=Path("/"))
    parser.add_argument("--live", action="store_true")
    arguments = parser.parse_args(argv)
    try:
        receipt = stage_guard_release(
            arguments.bundle,
            InstallContext(
                root=arguments.root,
                live=arguments.live,
                expected_release_sha256=arguments.release_sha256,
                architecture=cast(Architecture, arguments.architecture),
            ),
        )
    except GuardInstallError as exc:
        parser.error(str(exc))
    print(_canonical(receipt.as_dict()).decode("ascii"), end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = [
    "GuardInstallError",
    "InstallContext",
    "StageReceipt",
    "main",
    "stage_guard_release",
]
