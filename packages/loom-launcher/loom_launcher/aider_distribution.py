"""Build the reviewed dependency-consistent local Aider wheel."""

from __future__ import annotations

import argparse
import base64
import csv
import hashlib
import io
import os
import stat
import sys
import tempfile
import zipfile
from collections.abc import Sequence
from email.parser import BytesParser
from pathlib import Path, PurePosixPath
from typing import BinaryIO

SOURCE_FILENAME = "aider_chat-0.86.2-py3-none-any.whl"
SOURCE_SHA256 = "64f6a0c66c9f4633ad9f479bca3e64ebcba02b9da03c6b604b74a44736b2416e"
SOURCE_VERSION = "0.86.2"
LOCAL_VERSION = "0.86.2+loom.1"
OUTPUT_FILENAME = f"aider_chat-{LOCAL_VERSION}-py3-none-any.whl"
SOURCE_DIST_INFO = f"aider_chat-{SOURCE_VERSION}.dist-info"
LOCAL_DIST_INFO = f"aider_chat-{LOCAL_VERSION}.dist-info"
OLD_LITELLM_REQUIREMENT = "litellm==1.81.10"
NEW_LITELLM_REQUIREMENT = "litellm==1.84.1"
OLD_IMPORTLIB_METADATA_REQUIREMENT = "importlib-metadata==7.2.1"
NEW_IMPORTLIB_METADATA_REQUIREMENT = "importlib-metadata==8.9.0"
MAX_MEMBERS = 512
MAX_MEMBER_BYTES = 16 * 1024 * 1024
MAX_TOTAL_BYTES = 64 * 1024 * 1024

_METADATA_NAME = f"{SOURCE_DIST_INFO}/METADATA"
_WHEEL_NAME = f"{SOURCE_DIST_INFO}/WHEEL"
_RECORD_NAME = f"{SOURCE_DIST_INFO}/RECORD"
_LOCAL_METADATA_NAME = f"{LOCAL_DIST_INFO}/METADATA"
_LOCAL_RECORD_NAME = f"{LOCAL_DIST_INFO}/RECORD"
_FIXED_TIMESTAMP = (1980, 1, 1, 0, 0, 0)


class AiderDistributionError(RuntimeError):
    """The source or reconstructed Aider distribution is not acceptable."""


def _fail(message: str) -> AiderDistributionError:
    return AiderDistributionError(message)


def _sha256_stream(stream: BinaryIO) -> str:
    digest = hashlib.sha256()
    while chunk := stream.read(1024 * 1024):
        digest.update(chunk)
    return digest.hexdigest()


def _validate_member_name(name: str) -> None:
    if "\\" in name:
        raise _fail("archive member name contains a backslash")
    path = PurePosixPath(name)
    if (
        not name
        or name.startswith("/")
        or name.endswith("/")
        or "//" in name
        or any(part in {"", ".", ".."} for part in path.parts)
        or path.as_posix() != name
    ):
        raise _fail("unsafe archive member name")


def _member_mode(info: zipfile.ZipInfo) -> int:
    raw_mode = info.external_attr >> 16
    file_type = stat.S_IFMT(raw_mode)
    if file_type not in {0, stat.S_IFREG}:
        raise _fail("archive members must be regular files")
    return 0o755 if raw_mode & 0o111 else 0o644


def _inspect_members(infos: list[zipfile.ZipInfo]) -> dict[str, tuple[zipfile.ZipInfo, int]]:
    if len(infos) > MAX_MEMBERS:
        raise _fail("archive has too many archive members")
    inspected: dict[str, tuple[zipfile.ZipInfo, int]] = {}
    total_size = 0
    for info in infos:
        name = info.filename
        _validate_member_name(name)
        if name in inspected:
            raise _fail("archive contains a duplicate archive member")
        if info.flag_bits & 0x1:
            raise _fail("encrypted archive members are not allowed")
        mode = _member_mode(info)
        if info.file_size > MAX_MEMBER_BYTES:
            raise _fail("archive member exceeds the size limit")
        total_size += info.file_size
        if total_size > MAX_TOTAL_BYTES:
            raise _fail("archive aggregate exceeds the size limit")
        top_level = name.partition("/")[0]
        if top_level == LOCAL_DIST_INFO:
            raise _fail("archive contains pre-existing local dist-info")
        if top_level.endswith(".dist-info") and top_level != SOURCE_DIST_INFO:
            raise _fail("archive has an unexpected dist-info layout")
        inspected[name] = (info, mode)
    required = {_METADATA_NAME, _WHEEL_NAME, _RECORD_NAME}
    if not required.issubset(inspected):
        raise _fail("archive has an unexpected dist-info layout")
    return inspected


def _read_member(archive: zipfile.ZipFile, info: zipfile.ZipInfo) -> bytes:
    with archive.open(info, "r") as member:
        payload = member.read(MAX_MEMBER_BYTES + 1)
        if len(payload) > MAX_MEMBER_BYTES:
            raise _fail("archive member exceeds the size limit")
        if member.read(1):
            raise _fail("archive member exceeds the size limit")
    if len(payload) != info.file_size:
        raise _fail("archive member size does not match its header")
    return payload


def _record_rows(payload: bytes) -> list[list[str]]:
    try:
        text = payload.decode("utf-8")
        return list(csv.reader(io.StringIO(text, newline=""), strict=True))
    except (UnicodeDecodeError, csv.Error) as error:
        raise _fail("wheel RECORD is invalid") from error


def _record_digest(payload: bytes) -> str:
    encoded = base64.urlsafe_b64encode(hashlib.sha256(payload).digest()).rstrip(b"=")
    return f"sha256={encoded.decode('ascii')}"


def _validate_record(payloads: dict[str, bytes], record_name: str) -> None:
    rows = _record_rows(payloads[record_name])
    if any(len(row) != 3 for row in rows):
        raise _fail("wheel RECORD is invalid")
    by_name: dict[str, tuple[str, str]] = {}
    for name, digest, size in rows:
        if name in by_name:
            raise _fail("wheel RECORD contains duplicate rows")
        by_name[name] = (digest, size)
    if set(by_name) != set(payloads):
        raise _fail("wheel RECORD does not cover every member")
    for name, member_payload in payloads.items():
        digest, size = by_name[name]
        if name == record_name:
            if digest or size:
                raise _fail("wheel RECORD hashes itself")
        elif digest != _record_digest(member_payload) or size != str(len(member_payload)):
            raise _fail("wheel RECORD member verification failed")


def _replace_exact_line(payload: bytes, old: bytes, new: bytes, description: str) -> bytes:
    lines = payload.splitlines(keepends=True)
    matches = [index for index, line in enumerate(lines) if line.rstrip(b"\r\n") == old]
    if len(matches) != 1:
        raise _fail(f"metadata must contain exactly one {description}")
    index = matches[0]
    newline = lines[index][len(lines[index].rstrip(b"\r\n")) :]
    lines[index] = new + newline
    return b"".join(lines)


def _transform_metadata(payload: bytes) -> bytes:
    transformed = _replace_exact_line(
        payload,
        f"Version: {SOURCE_VERSION}".encode(),
        f"Version: {LOCAL_VERSION}".encode(),
        "source version",
    )
    transformed = _replace_exact_line(
        transformed,
        f"Requires-Dist: {OLD_LITELLM_REQUIREMENT}".encode(),
        f"Requires-Dist: {NEW_LITELLM_REQUIREMENT}".encode(),
        "target requirement for LiteLLM",
    )
    transformed = _replace_exact_line(
        transformed,
        f"Requires-Dist: {OLD_IMPORTLIB_METADATA_REQUIREMENT}".encode(),
        f"Requires-Dist: {NEW_IMPORTLIB_METADATA_REQUIREMENT}".encode(),
        "target requirement for importlib-metadata",
    )
    try:
        metadata = BytesParser().parsebytes(transformed)
    except (TypeError, ValueError) as error:
        raise _fail("wheel METADATA is invalid") from error
    if metadata.get("Version") != LOCAL_VERSION:
        raise _fail("wheel METADATA has the wrong local version")
    requirements = metadata.get_all("Requires-Dist", [])
    if requirements.count(NEW_LITELLM_REQUIREMENT) != 1:
        raise _fail("wheel METADATA has the wrong LiteLLM requirement")
    if requirements.count(NEW_IMPORTLIB_METADATA_REQUIREMENT) != 1:
        raise _fail("wheel METADATA has the wrong importlib-metadata requirement")
    return transformed


def _validate_wheel_metadata(payload: bytes) -> None:
    try:
        wheel_metadata = BytesParser().parsebytes(payload)
    except (TypeError, ValueError) as error:
        raise _fail("wheel WHEEL metadata is invalid") from error
    if wheel_metadata.get("Root-Is-Purelib", "").lower() != "true":
        raise _fail("wheel is not a universal pure-Python wheel")
    if wheel_metadata.get_all("Tag", []) != ["py3-none-any"]:
        raise _fail("wheel tag must be py3-none-any")


def _build_record(payloads: dict[str, bytes]) -> bytes:
    stream = io.StringIO(newline="")
    writer = csv.writer(stream, lineterminator="\n")
    for name in sorted(payloads):
        if name == _LOCAL_RECORD_NAME:
            writer.writerow((name, "", ""))
        else:
            payload = payloads[name]
            writer.writerow((name, _record_digest(payload), str(len(payload))))
    return stream.getvalue().encode("utf-8")


def _output_info(name: str, mode: int) -> zipfile.ZipInfo:
    info = zipfile.ZipInfo(name, _FIXED_TIMESTAMP)
    info.create_system = 3
    info.compress_type = zipfile.ZIP_DEFLATED
    info.external_attr = (stat.S_IFREG | mode) << 16
    return info


def _write_output(descriptor: int, payloads: dict[str, bytes], modes: dict[str, int]) -> None:
    with os.fdopen(os.dup(descriptor), "w+b") as stream:
        with zipfile.ZipFile(
            stream,
            "w",
            compression=zipfile.ZIP_DEFLATED,
            compresslevel=9,
            strict_timestamps=True,
        ) as archive:
            for name in sorted(payloads):
                archive.writestr(
                    _output_info(name, modes[name]),
                    payloads[name],
                    compress_type=zipfile.ZIP_DEFLATED,
                    compresslevel=9,
                )
        stream.flush()
        os.fsync(stream.fileno())
    os.lseek(descriptor, 0, os.SEEK_SET)


def _validate_output(descriptor: int, expected_payloads: dict[str, bytes]) -> None:
    os.lseek(descriptor, 0, os.SEEK_SET)
    with os.fdopen(os.dup(descriptor), "rb") as stream:
        with zipfile.ZipFile(stream, "r") as archive:
            infos = archive.infolist()
            if [info.filename for info in infos] != sorted(expected_payloads):
                raise _fail("reconstructed wheel member order is invalid")
            if archive.testzip() is not None:
                raise _fail("reconstructed wheel checksum validation failed")
            actual_payloads: dict[str, bytes] = {}
            for info in infos:
                _validate_member_name(info.filename)
                if info.date_time != _FIXED_TIMESTAMP:
                    raise _fail("reconstructed wheel timestamp is not deterministic")
                if info.compress_type != zipfile.ZIP_DEFLATED:
                    raise _fail("reconstructed wheel member is not deflated")
                if stat.S_IFMT(info.external_attr >> 16) != stat.S_IFREG:
                    raise _fail("reconstructed wheel member is not a regular file")
                actual_payloads[info.filename] = _read_member(archive, info)
    if actual_payloads != expected_payloads:
        raise _fail("reconstructed wheel payload validation failed")
    _validate_record(actual_payloads, _LOCAL_RECORD_NAME)
    _validate_wheel_metadata(actual_payloads[f"{LOCAL_DIST_INFO}/WHEEL"])
    metadata = BytesParser().parsebytes(actual_payloads[_LOCAL_METADATA_NAME])
    if metadata.get("Version") != LOCAL_VERSION:
        raise _fail("reconstructed wheel has the wrong local version")


def _same_file(left: os.stat_result, right: os.stat_result) -> bool:
    return (left.st_dev, left.st_ino) == (right.st_dev, right.st_ino)


def _entry_matches_descriptor(directory_descriptor: int, name: str, descriptor: int) -> bool:
    try:
        entry_stat = os.stat(name, dir_fd=directory_descriptor, follow_symlinks=False)
    except OSError:
        return False
    descriptor_stat = os.fstat(descriptor)
    return stat.S_ISREG(entry_stat.st_mode) and _same_file(entry_stat, descriptor_stat)


def _unlink_matching_entry(directory_descriptor: int, name: str, descriptor: int) -> None:
    if _entry_matches_descriptor(directory_descriptor, name, descriptor):
        os.unlink(name, dir_fd=directory_descriptor)


def _rebuild_aider_wheel(
    source_wheel: Path,
    output_directory: Path,
    *,
    expected_source_sha256: str,
) -> Path:
    """Rebuild an exact Aider source wheel with the two reviewed dependency pins."""

    directory_descriptor: int | None = None
    temporary_descriptor: int | None = None
    temporary_name: str | None = None
    published_name: str | None = None
    completed = False
    try:
        if source_wheel.name != SOURCE_FILENAME:
            raise _fail("source filename is not the reviewed Aider wheel")
        try:
            source_stat = source_wheel.lstat()
        except OSError as error:
            raise _fail("source wheel must be a readable regular file") from error
        if not stat.S_ISREG(source_stat.st_mode):
            raise _fail("source wheel must be a regular file")
        if not output_directory.is_absolute():
            raise _fail("output directory must be absolute")
        if Path(os.path.realpath(output_directory)) != output_directory:
            raise _fail("output directory must be an absolute real directory")
        try:
            output_stat = output_directory.lstat()
        except OSError as error:
            raise _fail("output directory must already exist") from error
        if not stat.S_ISDIR(output_stat.st_mode):
            raise _fail("output directory must be a regular directory")
        if output_stat.st_uid != os.geteuid() or output_stat.st_mode & 0o022:
            raise _fail("output directory must be owner-controlled")
        directory_descriptor = os.open(
            output_directory,
            os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC,
        )
        retained_output_stat = os.fstat(directory_descriptor)
        if (
            not stat.S_ISDIR(retained_output_stat.st_mode)
            or retained_output_stat.st_uid != os.geteuid()
            or retained_output_stat.st_mode & 0o022
            or not _same_file(output_stat, retained_output_stat)
        ):
            raise _fail("output directory must be owner-controlled")
        output_path = output_directory / OUTPUT_FILENAME
        try:
            os.stat(OUTPUT_FILENAME, dir_fd=directory_descriptor, follow_symlinks=False)
        except FileNotFoundError:
            pass
        else:
            raise _fail("output wheel already exists")

        with source_wheel.open("rb") as source_stream:
            if not stat.S_ISREG(os.fstat(source_stream.fileno()).st_mode):
                raise _fail("source wheel must be a regular file")
            actual_sha256 = _sha256_stream(source_stream)
            if actual_sha256 != expected_source_sha256:
                raise _fail("source SHA-256 does not match the reviewed digest")
            source_stream.seek(0)
            with zipfile.ZipFile(source_stream, "r") as source_archive:
                inspected = _inspect_members(source_archive.infolist())
                payloads = {
                    name: _read_member(source_archive, info)
                    for name, (info, _mode) in inspected.items()
                }
                modes = {name: mode for name, (_info, mode) in inspected.items()}

        _validate_record(payloads, _RECORD_NAME)
        _validate_wheel_metadata(payloads[_WHEEL_NAME])
        transformed: dict[str, bytes] = {}
        transformed_modes: dict[str, int] = {}
        for name, payload in payloads.items():
            if name == _RECORD_NAME:
                continue
            local_name = (
                f"{LOCAL_DIST_INFO}{name[len(SOURCE_DIST_INFO):]}"
                if name.startswith(f"{SOURCE_DIST_INFO}/")
                else name
            )
            transformed[local_name] = (
                _transform_metadata(payload) if name == _METADATA_NAME else payload
            )
            transformed_modes[local_name] = modes[name]
        transformed[_LOCAL_RECORD_NAME] = b""
        transformed_modes[_LOCAL_RECORD_NAME] = modes[_RECORD_NAME]
        transformed[_LOCAL_RECORD_NAME] = _build_record(transformed)

        temporary_descriptor, raw_temporary_name = tempfile.mkstemp(
            prefix=f".{OUTPUT_FILENAME}.",
            suffix=".tmp",
            dir=output_directory,
        )
        temporary_name = Path(raw_temporary_name).name
        if not _entry_matches_descriptor(
            directory_descriptor, temporary_name, temporary_descriptor
        ):
            raise _fail("temporary wheel pathname identity changed")
        _write_output(temporary_descriptor, transformed, transformed_modes)
        _validate_output(temporary_descriptor, transformed)
        os.fchmod(temporary_descriptor, 0o444)
        os.fsync(temporary_descriptor)
        if not _entry_matches_descriptor(
            directory_descriptor, temporary_name, temporary_descriptor
        ):
            raise _fail("temporary wheel pathname identity changed")
        os.link(
            temporary_name,
            OUTPUT_FILENAME,
            src_dir_fd=directory_descriptor,
            dst_dir_fd=directory_descriptor,
            follow_symlinks=False,
        )
        published_name = OUTPUT_FILENAME
        published_stat = os.stat(
            published_name,
            dir_fd=directory_descriptor,
            follow_symlinks=False,
        )
        if (
            not stat.S_ISREG(published_stat.st_mode)
            or not _same_file(published_stat, os.fstat(temporary_descriptor))
            or stat.S_IMODE(published_stat.st_mode) != 0o444
        ):
            raise _fail("published wheel identity validation failed")
        if not _entry_matches_descriptor(
            directory_descriptor, temporary_name, temporary_descriptor
        ):
            raise _fail("temporary wheel pathname identity changed")
        os.unlink(temporary_name, dir_fd=directory_descriptor)
        temporary_name = None
        os.fsync(directory_descriptor)
        completed = True
        return output_path
    except AiderDistributionError:
        raise
    except FileExistsError as error:
        raise _fail("output wheel already exists") from error
    except (OSError, zipfile.BadZipFile, csv.Error, UnicodeError, ValueError) as error:
        raise _fail("unable to safely rebuild the Aider wheel") from error
    finally:
        if not completed and directory_descriptor is not None and temporary_descriptor is not None:
            try:
                if temporary_name is not None:
                    _unlink_matching_entry(
                        directory_descriptor, temporary_name, temporary_descriptor
                    )
            except OSError:
                pass
            try:
                if published_name is not None:
                    _unlink_matching_entry(
                        directory_descriptor, published_name, temporary_descriptor
                    )
            except OSError:
                pass
        if temporary_descriptor is not None:
            os.close(temporary_descriptor)
        if directory_descriptor is not None:
            os.close(directory_descriptor)


def patch_aider_wheel(source_wheel: Path, output_directory: Path) -> Path:
    """Patch only the exact upstream Aider wheel reviewed by Loom."""

    return _rebuild_aider_wheel(
        source_wheel,
        output_directory,
        expected_source_sha256=SOURCE_SHA256,
    )


def main(argv: Sequence[str] | None = None) -> int:
    """Patch a reviewed source wheel and print its absolute output path."""

    parser = argparse.ArgumentParser(
        description="Build Loom's reviewed local Aider wheel",
        exit_on_error=False,
    )
    parser.add_argument("--source-wheel")
    parser.add_argument("--output-directory")
    try:
        try:
            arguments, unknown = parser.parse_known_args(argv)
        except argparse.ArgumentError as error:
            raise _fail("invalid command arguments") from error
        if unknown or arguments.source_wheel is None or arguments.output_directory is None:
            raise _fail("--source-wheel and --output-directory are required")
        output = patch_aider_wheel(
            Path(arguments.source_wheel),
            Path(arguments.output_directory),
        )
    except AiderDistributionError as error:
        message = str(error).replace("\n", " ")[:450]
        print(f"aider wheel error: {message}", file=sys.stderr)
        return 1
    print(output.absolute())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
