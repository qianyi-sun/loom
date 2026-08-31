from __future__ import annotations

import hashlib
import io
import stat
import tarfile
import urllib.error
from pathlib import Path

import pytest
import scripts.install_trivy as installer


def _archive(
    payload: bytes = b"pinned-trivy-binary",
    *,
    member_type: bytes = tarfile.REGTYPE,
    duplicate: bool = False,
    global_pax_size: int = 0,
    gnu_long_name_size: int = 0,
) -> bytes:
    buffer = io.BytesIO()
    archive_format = tarfile.GNU_FORMAT if gnu_long_name_size else tarfile.PAX_FORMAT
    pax_headers = {"comment": "x" * global_pax_size} if global_pax_size else None
    with tarfile.open(
        fileobj=buffer,
        mode="w:gz",
        format=archive_format,
        pax_headers=pax_headers,
    ) as archive:
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
        if gnu_long_name_size:
            metadata = tarfile.TarInfo("x" * gnu_long_name_size)
            metadata_payload = b"metadata"
            metadata.size = len(metadata_payload)
            archive.addfile(metadata, io.BytesIO(metadata_payload))
    return buffer.getvalue()


def _release(
    archive: bytes,
    *,
    arm_archive: bytes | None = None,
) -> installer.TrivyRelease:
    return installer.TrivyRelease(
        version="v0.74.0",
        archives={
            "amd64": installer.TrivyArchive(
                filename="trivy_0.74.0_Linux-64bit.tar.gz",
                sha256=hashlib.sha256(archive).hexdigest(),
            ),
            "arm64": installer.TrivyArchive(
                filename="trivy_0.74.0_Linux-ARM64.tar.gz",
                sha256=(
                    hashlib.sha256(arm_archive).hexdigest()
                    if arm_archive is not None
                    else "a" * 64
                ),
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
            "https://github.com/aquasecurity/trivy/releases/download/v0.74.0/"
            "trivy_0.74.0_Linux-64bit.tar.gz",
            60,
        )
    ]
    assert executable.read_bytes() == payload
    assert stat.S_IMODE(executable.stat().st_mode) == 0o755
    assert executable.parent.parent == tmp_path
    assert sorted(path.name for path in executable.parent.iterdir()) == ["trivy"]


def test_installer_can_prepare_an_explicit_cross_architecture_binary(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    amd64_archive = _archive(b"amd64-trivy")
    arm64_archive = _archive(b"arm64-trivy")
    requested: list[str] = []

    def open_archive(url: str, *, timeout: int) -> io.BytesIO:
        requested.append(url)
        assert timeout == 60
        return io.BytesIO(arm64_archive)

    monkeypatch.setattr(
        installer,
        "TRIVY_RELEASE",
        _release(amd64_archive, arm_archive=arm64_archive),
    )
    monkeypatch.setattr(installer.urllib.request, "urlopen", open_archive)
    monkeypatch.setattr(installer.platform, "machine", lambda: "riscv64")

    executable = installer.install_trivy(tmp_path, architecture="arm64")

    assert requested == [
        "https://github.com/aquasecurity/trivy/releases/download/v0.74.0/"
        "trivy_0.74.0_Linux-ARM64.tar.gz"
    ]
    assert executable.read_bytes() == b"arm64-trivy"


def test_installer_retries_transient_downloads_and_reverifies_digest(
    tmp_path: Path,
) -> None:
    payload = b"verified-after-transient-downloads"
    archive = _archive(payload)
    attempts = 0
    delays: list[float] = []

    def transient_open(_url: str, *, timeout: int) -> io.BytesIO:
        nonlocal attempts
        attempts += 1
        assert timeout == 60
        if attempts < 3:
            raise urllib.error.URLError("temporary release asset failure")
        return io.BytesIO(archive)

    executable = installer._install_trivy(
        tmp_path,
        machine="x86_64",
        release=_release(archive),
        opener=transient_open,
        sleeper=delays.append,
    )

    assert attempts == 3
    assert delays == [1.0, 2.0]
    assert executable.read_bytes() == payload
    assert sorted(path.name for path in executable.parent.iterdir()) == ["trivy"]


def test_installer_does_not_retry_non_transient_http_failure(tmp_path: Path) -> None:
    attempts = 0
    delays: list[float] = []

    def missing_release(url: str, *, timeout: int) -> io.BytesIO:
        nonlocal attempts
        attempts += 1
        raise urllib.error.HTTPError(url, 404, "not found", {}, None)

    with pytest.raises(installer.TrivyInstallError, match="after 1 attempt"):
        installer._install_trivy(
            tmp_path,
            machine="x86_64",
            release=_release(_archive()),
            opener=missing_release,
            sleeper=delays.append,
        )

    assert attempts == 1
    assert delays == []
    assert list(tmp_path.iterdir()) == []


def test_installer_retries_stale_signed_release_redirect(tmp_path: Path) -> None:
    archive = _archive()
    attempts = 0

    def stale_then_current(url: str, *, timeout: int) -> io.BytesIO:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise urllib.error.HTTPError(url, 403, "stale signed redirect", {}, None)
        return io.BytesIO(archive)

    executable = installer._install_trivy(
        tmp_path,
        machine="x86_64",
        release=_release(archive),
        opener=stale_then_current,
        sleeper=lambda _delay: None,
    )

    assert attempts == 2
    assert executable.is_file()


def test_installer_bounds_transient_download_retries(tmp_path: Path) -> None:
    attempts = 0
    delays: list[float] = []

    def unavailable_release(_url: str, *, timeout: int) -> io.BytesIO:
        nonlocal attempts
        attempts += 1
        raise urllib.error.URLError("temporary release asset failure")

    with pytest.raises(installer.TrivyInstallError, match="after 5 attempt"):
        installer._install_trivy(
            tmp_path,
            machine="x86_64",
            release=_release(_archive()),
            opener=unavailable_release,
            sleeper=delays.append,
        )

    assert attempts == 5
    assert delays == [1.0, 2.0, 4.0, 8.0]
    assert list(tmp_path.iterdir()) == []


def test_installer_rejects_archive_digest_mismatch_without_leaving_files(
    tmp_path: Path,
) -> None:
    archive = _archive()
    release = _release(archive)
    release.archives["amd64"] = installer.TrivyArchive(
        filename=release.archives["amd64"].filename,
        sha256="0" * 64,
    )

    attempts = 0

    def mismatched_archive(_url: str, *, timeout: int) -> io.BytesIO:
        nonlocal attempts
        attempts += 1
        return io.BytesIO(archive)

    with pytest.raises(installer.TrivyInstallError, match="digest"):
        installer._install_trivy(
            tmp_path,
            machine="x86_64",
            release=release,
            opener=mismatched_archive,
        )

    assert attempts == 1
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


def test_installer_normalizes_truncated_archive_and_cleans_up(tmp_path: Path) -> None:
    archive = _archive()
    truncated = archive[: len(archive) // 2]

    with pytest.raises(installer.TrivyInstallError, match="installation failed"):
        installer._install_trivy(
            tmp_path,
            machine="x86_64",
            release=_release(truncated),
            opener=lambda _url, *, timeout: io.BytesIO(truncated),
        )

    assert list(tmp_path.iterdir()) == []


@pytest.mark.parametrize(
    ("limit_name", "limit_value", "message"),
    [
        ("_MAX_ARCHIVE_MEMBERS", 1, "too many members"),
        ("_MAX_ARCHIVE_EXPANDED_BYTES", 1, "expanded size"),
    ],
)
def test_installer_bounds_archive_traversal(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    limit_name: str,
    limit_value: int,
    message: str,
) -> None:
    archive = _archive()
    monkeypatch.setattr(installer, limit_name, limit_value)

    with pytest.raises(installer.TrivyInstallError, match=message):
        installer._install_trivy(
            tmp_path,
            machine="x86_64",
            release=_release(archive),
            opener=lambda _url, *, timeout: io.BytesIO(archive),
        )

    assert list(tmp_path.iterdir()) == []


@pytest.mark.parametrize(
    "archive",
    [
        _archive(global_pax_size=16 * 1024),
        _archive(gnu_long_name_size=16 * 1024),
    ],
    ids=["pax-global-header", "gnu-long-name"],
)
def test_installer_bounds_hidden_archive_metadata(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    archive: bytes,
) -> None:
    monkeypatch.setattr(installer, "_MAX_ARCHIVE_EXPANDED_BYTES", 12 * 1024)

    with pytest.raises(installer.TrivyInstallError, match="expanded size"):
        installer._install_trivy(
            tmp_path,
            machine="x86_64",
            release=_release(archive),
            opener=lambda _url, *, timeout: io.BytesIO(archive),
        )

    assert list(tmp_path.iterdir()) == []
