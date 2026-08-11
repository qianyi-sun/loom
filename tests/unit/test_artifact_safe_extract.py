from __future__ import annotations

import io
import tarfile
import zipfile
from pathlib import Path

import pytest
import zstandard

from loom_worker.artifact_safe_extract import SafeExtractionError, extract_archive


def _tar(path: Path, members: dict[str, bytes]) -> None:
    with tarfile.open(path, "w") as archive:
        for name, payload in members.items():
            info = tarfile.TarInfo(name)
            info.size = len(payload)
            info.mode = 0o644
            archive.addfile(info, io.BytesIO(payload))


def test_tar_extracts_only_confined_regular_files_read_only(tmp_path: Path) -> None:
    archive = tmp_path / "input.tar"
    _tar(archive, {"payload/a.txt": b"abc"})

    inventory = extract_archive(
        archive_path=archive,
        archive_format="tar",
        destination=tmp_path / "out",
        stored_size_bytes=archive.stat().st_size,
        expected_file_count=1,
        expected_unpacked_size_bytes=3,
        require_payload_root=True,
    )

    assert inventory.relative_paths == ("payload/a.txt",)
    assert (tmp_path / "out/a.txt").read_bytes() == b"abc"
    assert (tmp_path / "out/a.txt").stat().st_mode & 0o777 == 0o444


@pytest.mark.parametrize("name", ["../escape", "/absolute", "payload/../escape", "payload\\x"])
def test_tar_rejects_unsafe_paths_without_output(tmp_path: Path, name: str) -> None:
    archive = tmp_path / "bad.tar"
    _tar(archive, {name: b"bad"})

    with pytest.raises(SafeExtractionError):
        extract_archive(
            archive_path=archive,
            archive_format="tar",
            destination=tmp_path / "out",
            stored_size_bytes=archive.stat().st_size,
            expected_file_count=1,
            expected_unpacked_size_bytes=3,
            require_payload_root=True,
        )

    assert not (tmp_path / "out").exists()


def test_zip_rejects_casefold_collisions(tmp_path: Path) -> None:
    archive = tmp_path / "bad.zip"
    with zipfile.ZipFile(archive, "w") as output:
        output.writestr("A.txt", b"a")
        output.writestr("a.txt", b"b")

    with pytest.raises(SafeExtractionError, match="collides"):
        extract_archive(
            archive_path=archive,
            archive_format="zip",
            destination=tmp_path / "out",
            stored_size_bytes=archive.stat().st_size,
            expected_file_count=2,
            expected_unpacked_size_bytes=2,
        )


def test_zip_rejects_extended_metadata(tmp_path: Path) -> None:
    archive = tmp_path / "metadata.zip"
    with zipfile.ZipFile(archive, "w") as output:
        member = zipfile.ZipInfo("payload.txt")
        member.extra = b"\x55\x54\x01\x00\x00"
        output.writestr(member, b"a")

    with pytest.raises(SafeExtractionError, match="metadata"):
        extract_archive(
            archive_path=archive,
            archive_format="zip",
            destination=tmp_path / "out",
            stored_size_bytes=archive.stat().st_size,
            expected_file_count=1,
            expected_unpacked_size_bytes=1,
        )


def test_tar_zst_streams_through_same_safe_extractor(tmp_path: Path) -> None:
    raw_tar = tmp_path / "raw.tar"
    _tar(raw_tar, {"payload/data.bin": b"abcd"})
    archive = tmp_path / "payload.tar.zst"
    archive.write_bytes(zstandard.ZstdCompressor().compress(raw_tar.read_bytes()))

    inventory = extract_archive(
        archive_path=archive,
        archive_format="tar.zst",
        destination=tmp_path / "out",
        stored_size_bytes=archive.stat().st_size,
        expected_file_count=1,
        expected_unpacked_size_bytes=4,
        require_payload_root=True,
    )

    assert inventory.file_count == 1
    assert (tmp_path / "out/data.bin").read_bytes() == b"abcd"
