"""Fail-closed extraction for immutable Pipeline Artifact inputs."""

from __future__ import annotations

import os
import shutil
import stat
import tarfile
import unicodedata
import zipfile
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import BinaryIO, Literal, Protocol, cast

MAX_EXTRACTED_FILES = 2_000_000
MAX_UNPACKED_BYTES = 1_649_267_441_664
MAX_EXPANSION_RATIO = 100
MAX_PATH_BYTES = 240
COPY_BUFFER_BYTES = 64 * 1024 * 1024


class SafeExtractionError(ValueError):
    """Archive metadata or contents violate the closed input contract."""


class _ClosableReader(Protocol):
    def close(self) -> None: ...


@dataclass(frozen=True)
class ExtractedInventory:
    file_count: int
    unpacked_size_bytes: int
    relative_paths: tuple[str, ...]


@dataclass
class _Tracker:
    destination: Path
    stored_size_bytes: int
    expected_file_count: int
    expected_unpacked_size_bytes: int
    require_payload_root: bool
    paths: set[str]
    folded_paths: set[str]
    file_count: int = 0
    unpacked_size_bytes: int = 0

    def path(self, raw: str, *, directory: bool) -> tuple[Path, str]:
        normalized = unicodedata.normalize("NFC", raw)
        if normalized != raw:
            raise SafeExtractionError("archive path is not NFC")
        if not normalized or normalized.startswith("/") or "\\" in normalized or "\x00" in normalized:
            raise SafeExtractionError("archive path is not confined")
        stripped = normalized.rstrip("/") if directory else normalized
        parts = stripped.split("/")
        if any(part in {"", ".", ".."} for part in parts):
            raise SafeExtractionError("archive path contains an invalid component")
        if len(stripped.encode("utf-8")) > MAX_PATH_BYTES:
            raise SafeExtractionError("archive path is too long")
        if self.require_payload_root and (len(parts) < 2 or parts[0] != "payload"):
            raise SafeExtractionError("input import archive member is outside payload")
        key = "/".join(parts)
        folded = key.casefold()
        if key in self.paths or folded in self.folded_paths:
            raise SafeExtractionError("archive path collides with another member")
        self.paths.add(key)
        self.folded_paths.add(folded)
        relative = PurePosixPath(*parts[1:]) if self.require_payload_root else PurePosixPath(*parts)
        if not relative.parts:
            raise SafeExtractionError("payload root is not an output member")
        return self.destination.joinpath(*relative.parts), key

    def add_file(self, size: int) -> None:
        if size < 0:
            raise SafeExtractionError("archive member size is negative")
        self.file_count += 1
        self.unpacked_size_bytes += size
        if self.file_count > MAX_EXTRACTED_FILES:
            raise SafeExtractionError("archive exceeds the file-count limit")
        if self.unpacked_size_bytes > MAX_UNPACKED_BYTES:
            raise SafeExtractionError("archive exceeds the unpacked-size limit")
        if self.stored_size_bytes == 0 and self.unpacked_size_bytes:
            raise SafeExtractionError("non-empty archive has zero stored bytes")
        if self.unpacked_size_bytes > self.stored_size_bytes * MAX_EXPANSION_RATIO:
            raise SafeExtractionError("archive exceeds the expansion-ratio limit")

    def finish(self) -> ExtractedInventory:
        if self.file_count != self.expected_file_count:
            raise SafeExtractionError("archive file count does not match the manifest")
        if self.unpacked_size_bytes != self.expected_unpacked_size_bytes:
            raise SafeExtractionError("archive size does not match the manifest")
        return ExtractedInventory(
            file_count=self.file_count,
            unpacked_size_bytes=self.unpacked_size_bytes,
            relative_paths=tuple(sorted(self.paths, key=str.encode)),
        )


def _mkdir_closed(path: Path) -> None:
    parents: list[Path] = []
    current = path
    while not current.exists():
        parents.append(current)
        current = current.parent
    if current.is_symlink() or not current.is_dir():
        raise SafeExtractionError("archive parent is not a real directory")
    for item in reversed(parents):
        item.mkdir(mode=0o700)


def _copy_regular(source: BinaryIO, destination: Path, expected_size: int) -> None:
    _mkdir_closed(destination.parent)
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    fd = os.open(destination, flags, 0o400)
    observed = 0
    try:
        with os.fdopen(fd, "wb", closefd=False) as output:
            while True:
                chunk = source.read(COPY_BUFFER_BYTES)
                if not chunk:
                    break
                observed += len(chunk)
                if observed > expected_size:
                    raise SafeExtractionError("archive member exceeds its declared size")
                output.write(chunk)
            output.flush()
            os.fsync(output.fileno())
        if observed != expected_size:
            raise SafeExtractionError("archive member is truncated")
        os.fchmod(fd, 0o444)
    finally:
        os.close(fd)


def _reject_mode(mode: int) -> None:
    if mode & (stat.S_ISUID | stat.S_ISGID):
        raise SafeExtractionError("setid archive metadata is forbidden")


def _open_tar(path: Path, archive_format: str) -> tarfile.TarFile:
    if archive_format == "tar":
        return tarfile.open(path, mode="r:")
    if archive_format != "tar.zst":
        raise SafeExtractionError("unsupported tar format")
    try:
        import zstandard
    except ImportError as exc:
        raise SafeExtractionError("tar.zst support is unavailable") from exc
    source = path.open("rb")
    reader = zstandard.ZstdDecompressor().stream_reader(source)
    try:
        archive = tarfile.open(fileobj=reader, mode="r|")
    except Exception:
        cast(_ClosableReader, reader).close()
        source.close()
        raise
    archive._loom_zstd_reader = reader  # type: ignore[attr-defined]
    archive._loom_zstd_source = source  # type: ignore[attr-defined]
    return archive


def _extract_tar(path: Path, tracker: _Tracker, archive_format: str) -> None:
    archive = _open_tar(path, archive_format)
    try:
        for member in archive:
            _reject_mode(member.mode)
            if member.pax_headers:
                raise SafeExtractionError("tar extended metadata is forbidden")
            if member.isdir():
                destination, _ = tracker.path(member.name, directory=True)
                _mkdir_closed(destination)
                continue
            if not member.isreg() or member.issparse():
                raise SafeExtractionError("tar member type is forbidden")
            destination, _ = tracker.path(member.name, directory=False)
            tracker.add_file(member.size)
            stream = archive.extractfile(member)
            if stream is None:
                raise SafeExtractionError("tar regular member has no content")
            with stream:
                _copy_regular(cast(BinaryIO, stream), destination, member.size)
    finally:
        archive.close()
        reader = getattr(archive, "_loom_zstd_reader", None)
        source = getattr(archive, "_loom_zstd_source", None)
        if reader is not None:
            cast(_ClosableReader, reader).close()
        if source is not None:
            source.close()


def _extract_zip(path: Path, tracker: _Tracker) -> None:
    with zipfile.ZipFile(path) as archive:
        for member in archive.infolist():
            if member.flag_bits & 0x1 or member.extra:
                raise SafeExtractionError("zip extended or encrypted metadata is forbidden")
            mode = member.external_attr >> 16
            _reject_mode(mode)
            member_type = stat.S_IFMT(mode)
            is_directory = member.is_dir()
            if member_type not in {0, stat.S_IFREG, stat.S_IFDIR}:
                raise SafeExtractionError("zip member type is forbidden")
            destination, _ = tracker.path(member.filename, directory=is_directory)
            if is_directory:
                _mkdir_closed(destination)
                continue
            tracker.add_file(member.file_size)
            with archive.open(member, mode="r") as source:
                _copy_regular(cast(BinaryIO, source), destination, member.file_size)


def extract_archive(
    *,
    archive_path: Path,
    archive_format: Literal["tar", "tar.zst", "zip"],
    destination: Path,
    stored_size_bytes: int,
    expected_file_count: int,
    expected_unpacked_size_bytes: int,
    require_payload_root: bool = False,
) -> ExtractedInventory:
    """Extract one verified stored object without trusting archive metadata."""

    if destination.exists():
        if destination.is_symlink() or not destination.is_dir() or any(destination.iterdir()):
            raise SafeExtractionError("extraction destination must be an empty real directory")
    else:
        destination.mkdir(parents=True, mode=0o700)
    tracker = _Tracker(
        destination=destination,
        stored_size_bytes=stored_size_bytes,
        expected_file_count=expected_file_count,
        expected_unpacked_size_bytes=expected_unpacked_size_bytes,
        require_payload_root=require_payload_root,
        paths=set(),
        folded_paths=set(),
    )
    try:
        if archive_format in {"tar", "tar.zst"}:
            _extract_tar(archive_path, tracker, archive_format)
        else:
            _extract_zip(archive_path, tracker)
        inventory = tracker.finish()
        for root, directories, _files in os.walk(destination, topdown=False, followlinks=False):
            for name in directories:
                os.chmod(Path(root, name), 0o555, follow_symlinks=False)
        os.chmod(destination, 0o555, follow_symlinks=False)
        return inventory
    except Exception:
        shutil.rmtree(destination, ignore_errors=True)
        raise


__all__ = ["ExtractedInventory", "SafeExtractionError", "extract_archive"]
