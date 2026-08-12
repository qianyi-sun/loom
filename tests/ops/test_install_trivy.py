from __future__ import annotations

import hashlib
import io
import stat
import tarfile
from pathlib import Path

import pytest
import scripts.install_trivy as installer


def _archive(
    payload: bytes = b"pinned-trivy-binary",
    *,
    member_type: bytes = tarfile.REGTYPE,
    duplicate: bool = False,
) -> bytes:
    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode="w:gz") as archive:
        license_info = tarfile.TarInfo("LICENSE")
        license_payload = b"license"
        license_info.size = len(license_payload)
        archive.addfile(license_info, io.BytesIO(license_payload))

        count = 2 if duplicate else 1
        for _ in range(count):
            binary = tarfile.TarInfo("trivy")
            binary.type = member_type
            binary.mode = 0o755
            if member_type == tarfile.REGTYPE:
                binary.size = len(payload)
                archive.addfile(binary, io.BytesIO(payload))
            else:
                binary.linkname = "LICENSE"
                archive.addfile(binary)
    return buffer.getvalue()


def _release(archive: bytes) -> installer.TrivyRelease:
    return installer.TrivyRelease(
        version="v0.70.0",
        archives={
            "amd64": installer.TrivyArchive(
                filename="trivy_0.70.0_Linux-64bit.tar.gz",
                sha256=hashlib.sha256(archive).hexdigest(),
            ),
            "arm64": installer.TrivyArchive(
                filename="trivy_0.70.0_Linux-ARM64.tar.gz",
                sha256="a" * 64,
            ),
        },
    )


def test_installer_verifies_archive_and_extracts_only_trivy(tmp_path: Path) -> None:
    payload = b"verified-trivy"
    archive = _archive(payload)
    requested: list[tuple[str, int]] = []

    def open_archive(url: str, *, timeout: int) -> io.BytesIO:
        requested.append((url, timeout))
        return io.BytesIO(archive)

    executable = installer._install_trivy(
        tmp_path,
        machine="x86_64",
        release=_release(archive),
        opener=open_archive,
    )

    assert requested == [
        (
            "https://github.com/aquasecurity/trivy/releases/download/v0.70.0/"
            "trivy_0.70.0_Linux-64bit.tar.gz",
            60,
        )
    ]
    assert executable.read_bytes() == payload
    assert stat.S_IMODE(executable.stat().st_mode) == 0o755
    assert executable.parent.parent == tmp_path
    assert sorted(path.name for path in executable.parent.iterdir()) == ["trivy"]


def test_installer_rejects_archive_digest_mismatch_without_leaving_files(
    tmp_path: Path,
) -> None:
    archive = _archive()
    release = _release(archive)
    release.archives["amd64"] = installer.TrivyArchive(
        filename=release.archives["amd64"].filename,
        sha256="0" * 64,
    )

    with pytest.raises(installer.TrivyInstallError, match="digest"):
        installer._install_trivy(
            tmp_path,
            machine="x86_64",
            release=release,
            opener=lambda _url, *, timeout: io.BytesIO(archive),
        )

    assert list(tmp_path.iterdir()) == []


@pytest.mark.parametrize(
    ("archive", "message"),
    [
        (_archive(member_type=tarfile.SYMTYPE), "regular file"),
        (_archive(duplicate=True), "exactly one"),
    ],
)
def test_installer_rejects_ambiguous_or_unsafe_binary_member(
    tmp_path: Path,
    archive: bytes,
    message: str,
) -> None:
    with pytest.raises(installer.TrivyInstallError, match=message):
        installer._install_trivy(
            tmp_path,
            machine="x86_64",
            release=_release(archive),
            opener=lambda _url, *, timeout: io.BytesIO(archive),
        )

    assert list(tmp_path.iterdir()) == []


def test_installer_rejects_unsupported_architecture_before_download(
    tmp_path: Path,
) -> None:
    downloaded = False

    def unexpected_download(_url: str, *, timeout: int) -> io.BytesIO:
        nonlocal downloaded
        downloaded = True
        return io.BytesIO()

    with pytest.raises(installer.TrivyInstallError, match="unsupported runner architecture"):
        installer._install_trivy(
            tmp_path,
            machine="riscv64",
            release=_release(_archive()),
            opener=unexpected_download,
        )

    assert downloaded is False
    assert list(tmp_path.iterdir()) == []
