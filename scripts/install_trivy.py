#!/usr/bin/env python3
"""Install the exact reviewed Trivy release binary for this runner."""

from __future__ import annotations

import argparse
import hashlib
import os
import platform
import re
import shutil
import stat
import sys
import tarfile
import tempfile
import urllib.error
import urllib.request
from collections.abc import Callable
from contextlib import AbstractContextManager
from pathlib import Path
from typing import TYPE_CHECKING, BinaryIO, NamedTuple, cast

if TYPE_CHECKING:
    from scripts.write_trivy_release_policy import TRIVY_VERSION
elif __package__:
    from scripts.write_trivy_release_policy import TRIVY_VERSION
else:  # pragma: no cover - direct workflow entry point
    from write_trivy_release_policy import TRIVY_VERSION

_HEX64 = re.compile(r"[0-9a-f]{64}")
_DOWNLOAD_TIMEOUT_SECONDS = 60
_MAX_ARCHIVE_BYTES = 128 * 1024 * 1024
_MAX_BINARY_BYTES = 256 * 1024 * 1024
_RELEASE_BASE_URL = "https://github.com/aquasecurity/trivy/releases/download"


class TrivyArchive(NamedTuple):
    """One architecture-specific, immutable Trivy release archive."""

    filename: str
    sha256: str


class TrivyRelease(NamedTuple):
    """The reviewed Trivy release artifacts accepted by the installer."""

    version: str
    archives: dict[str, TrivyArchive]


TRIVY_RELEASE = TrivyRelease(
    version=TRIVY_VERSION,
    archives={
        "amd64": TrivyArchive(
            filename="trivy_0.70.0_Linux-64bit.tar.gz",
            sha256="8b4376d5d6befe5c24d503f10ff136d9e0c49f9127a4279fd110b727929a5aa9",
        ),
        "arm64": TrivyArchive(
            filename="trivy_0.70.0_Linux-ARM64.tar.gz",
            sha256="2f6bb988b553a1bbac6bdd1ce890f5e412439564e17522b88a4541b4f364fc8d",
        ),
    },
)
TRIVY_RELEASE_URL = f"{_RELEASE_BASE_URL}/{TRIVY_RELEASE.version}"
TRIVY_ARCHIVE_SHA256 = {
    architecture: archive.sha256
    for architecture, archive in TRIVY_RELEASE.archives.items()
}


class TrivyInstallError(RuntimeError):
    """The pinned scanner could not be installed exactly and safely."""


_Opener = Callable[..., AbstractContextManager[BinaryIO]]


def _runner_architecture(machine: str) -> str:
    normalized = machine.strip().lower()
    if normalized in {"x86_64", "amd64"}:
        return "amd64"
    if normalized in {"aarch64", "arm64"}:
        return "arm64"
    raise TrivyInstallError("unsupported runner architecture")


def _validate_release(release: TrivyRelease) -> None:
    if release.version != TRIVY_VERSION:
        raise TrivyInstallError("Trivy release version is inconsistent")
    if set(release.archives) != {"amd64", "arm64"}:
        raise TrivyInstallError("Trivy release architecture set is invalid")
    version = release.version.removeprefix("v")
    for architecture, archive in release.archives.items():
        expected_filename = {
            "amd64": f"trivy_{version}_Linux-64bit.tar.gz",
            "arm64": f"trivy_{version}_Linux-ARM64.tar.gz",
        }[architecture]
        if (
            archive.filename != expected_filename
            or _HEX64.fullmatch(archive.sha256) is None
        ):
            raise TrivyInstallError("Trivy release archive identity is invalid")


def _validate_install_root(install_root: Path) -> None:
    if not install_root.is_absolute():
        raise TrivyInstallError("Trivy install root must be absolute")
    metadata = install_root.lstat()
    if not stat.S_ISDIR(metadata.st_mode) or install_root.is_symlink():
        raise TrivyInstallError("Trivy install root is invalid")


def _download_archive(
    *,
    url: str,
    destination: Path,
    expected_sha256: str,
    opener: _Opener,
) -> None:
    digest = hashlib.sha256()
    byte_count = 0
    with opener(url, timeout=_DOWNLOAD_TIMEOUT_SECONDS) as response:
        with destination.open("xb") as output:
            while chunk := response.read(1024 * 1024):
                byte_count += len(chunk)
                if byte_count > _MAX_ARCHIVE_BYTES:
                    raise TrivyInstallError("Trivy release archive is too large")
                digest.update(chunk)
                output.write(chunk)
            output.flush()
            os.fsync(output.fileno())
    if digest.hexdigest() != expected_sha256:
        raise TrivyInstallError("Trivy release archive digest mismatch")


def _extract_binary(archive_path: Path, executable: Path) -> None:
    with tarfile.open(archive_path, mode="r:gz") as archive:
        candidates = [member for member in archive.getmembers() if member.name == "trivy"]
        if len(candidates) != 1:
            raise TrivyInstallError("Trivy archive must contain exactly one binary")
        member = candidates[0]
        if not member.isfile():
            raise TrivyInstallError("Trivy archive binary must be a regular file")
        if member.size <= 0 or member.size > _MAX_BINARY_BYTES:
            raise TrivyInstallError("Trivy archive binary size is invalid")
        source = archive.extractfile(member)
        if source is None:
            raise TrivyInstallError("Trivy archive binary is unreadable")
        temporary_executable = executable.with_suffix(".tmp")
        byte_count = 0
        with source, temporary_executable.open("xb") as output:
            while chunk := source.read(1024 * 1024):
                byte_count += len(chunk)
                if byte_count > member.size or byte_count > _MAX_BINARY_BYTES:
                    raise TrivyInstallError("Trivy archive binary exceeds its declared size")
                output.write(chunk)
            output.flush()
            os.fsync(output.fileno())
        if byte_count != member.size:
            raise TrivyInstallError("Trivy archive binary is truncated")
        temporary_executable.chmod(0o755)
        os.replace(temporary_executable, executable)


def _install_trivy(
    install_root: Path,
    *,
    machine: str,
    release: TrivyRelease,
    opener: _Opener,
) -> Path:
    """Install a supplied reviewed release; dependency injection supports tests."""

    _validate_install_root(install_root)
    _validate_release(release)
    architecture = _runner_architecture(machine)
    selected = release.archives[architecture]
    install_dir = Path(tempfile.mkdtemp(prefix="loom-trivy-", dir=install_root))
    archive_path = install_dir / selected.filename
    executable = install_dir / "trivy"
    try:
        _download_archive(
            url=f"{_RELEASE_BASE_URL}/{release.version}/{selected.filename}",
            destination=archive_path,
            expected_sha256=selected.sha256,
            opener=opener,
        )
        _extract_binary(archive_path, executable)
        archive_path.unlink()
        metadata = executable.lstat()
        if (
            not stat.S_ISREG(metadata.st_mode)
            or executable.is_symlink()
            or stat.S_IMODE(metadata.st_mode) != 0o755
        ):
            raise TrivyInstallError("installed Trivy binary verification failed")
    except TrivyInstallError:
        shutil.rmtree(install_dir)
        raise
    except (OSError, tarfile.TarError, urllib.error.URLError) as exc:
        shutil.rmtree(install_dir)
        raise TrivyInstallError("pinned Trivy installation failed") from exc
    return executable


def install_trivy(install_root: Path) -> Path:
    """Install the repository-pinned Trivy release for the current machine."""

    return _install_trivy(
        install_root,
        machine=platform.machine(),
        release=TRIVY_RELEASE,
        opener=cast(_Opener, urllib.request.urlopen),
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--install-root", type=Path, required=True)
    arguments = parser.parse_args()
    try:
        executable = install_trivy(arguments.install_root)
    except (OSError, TrivyInstallError):
        sys.stderr.write("error: pinned Trivy installation failed\n")
        raise SystemExit(1) from None
    sys.stdout.write(f"{executable}\n")


if __name__ == "__main__":
    main()


__all__ = [
    "TRIVY_ARCHIVE_SHA256",
    "TRIVY_RELEASE",
    "TRIVY_RELEASE_URL",
    "TrivyArchive",
    "TrivyInstallError",
    "TrivyRelease",
    "install_trivy",
]
