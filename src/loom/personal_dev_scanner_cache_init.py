"""Publish one immutable scanner-cache generation on a personal-dev PVC."""

from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import os
import re
import secrets
import stat
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from loom.personal_dev_scanner_cache import (
    PersonalDevScannerCacheBinding,
    PersonalDevScannerCacheFiles,
)

_MAX_DATABASE_BYTES = 4 * 1024 * 1024 * 1024
_MAX_METADATA_BYTES = 64 * 1024
_MAX_IDENTITY_BYTES = 4096
_MAX_TOTAL_SOURCE_BYTES = 8 * 1024 * 1024 * 1024
_MAX_DELETE_BYTES = 16 * 1024 * 1024 * 1024
_MAX_DELETE_ENTRIES = 100_000
_MAX_GENERATION_ENTRIES = 16
_GENERATION_RE = re.compile(r"[0-9a-f]{64}")
_STAGING_RE = re.compile(r"[.]loom-scanner-cache-staging-[0-9a-f]{24}")
_ACTIVE_STAGING_RE = re.compile(r"[.]active-generation-[0-9a-f]{24}")
_RFC3339_RE = re.compile(
    r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}"
    r"(?:\.[0-9]{1,9})?Z"
)
_METADATA_KEYS = frozenset({"DownloadedAt", "NextUpdate", "UpdatedAt", "Version"})
_IDENTITY_KEYS = frozenset(
    {
        "cache_identity_sha256",
        "database_metadata_sha256",
        "database_sha256",
        "java_database_metadata_sha256",
        "java_database_sha256",
        "scanner_binary_sha256",
        "schema_version",
    }
)
_DIRECTORY_FLAGS = (
    os.O_RDONLY
    | getattr(os, "O_CLOEXEC", 0)
    | getattr(os, "O_NOFOLLOW", 0)
    | getattr(os, "O_DIRECTORY", 0)
)
_FILE_READ_FLAGS = (
    os.O_RDONLY
    | getattr(os, "O_CLOEXEC", 0)
    | getattr(os, "O_NOFOLLOW", 0)
    | getattr(os, "O_NONBLOCK", 0)
)
_FILE_WRITE_FLAGS = (
    os.O_WRONLY
    | os.O_CREAT
    | os.O_EXCL
    | getattr(os, "O_CLOEXEC", 0)
    | getattr(os, "O_NOFOLLOW", 0)
)
_STABLE_FIELDS = (
    "st_dev",
    "st_ino",
    "st_mode",
    "st_nlink",
    "st_uid",
    "st_gid",
    "st_size",
    "st_mtime_ns",
    "st_ctime_ns",
)
_GENERATION_ROOT_ENTRIES = frozenset({"db", "fanal", "identity.json", "java-db"})
_INSTALLER_LOCK = ".loom-scanner-cache-installer.lock"
_SOURCE_ROOT_ENTRIES = frozenset({"db", "java-db"})
_DATABASE_FILES = frozenset({"metadata.json", "trivy.db"})
_JAVA_DATABASE_FILES = frozenset({"metadata.json", "trivy-java.db"})
_GENERIC_ERROR = "personal-dev scanner cache installation failed"


class PersonalDevScannerCacheInstallError(ValueError):
    """The source, destination, or requested scanner generation is unsafe."""


def _invalid() -> PersonalDevScannerCacheInstallError:
    return PersonalDevScannerCacheInstallError(_GENERIC_ERROR)


def _canonical_json(value: object) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("ascii")


def _unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    value: dict[str, Any] = {}
    for key, item in pairs:
        if key in value:
            raise _invalid()
        value[key] = item
    return value


def _is_sha256(value: object) -> bool:
    return (
        isinstance(value, str)
        and len(value) == 64
        and value != "0" * 64
        and _GENERATION_RE.fullmatch(value) is not None
    )


def _identity_value(binding: PersonalDevScannerCacheBinding) -> dict[str, object]:
    return {
        "cache_identity_sha256": binding.cache_identity_sha256,
        "database_metadata_sha256": binding.files.database_metadata_sha256,
        "database_sha256": binding.files.database_sha256,
        "java_database_metadata_sha256": binding.files.java_database_metadata_sha256,
        "java_database_sha256": binding.files.java_database_sha256,
        "scanner_binary_sha256": binding.scanner_binary_sha256,
        "schema_version": 1,
    }


def _validate_binding(binding: PersonalDevScannerCacheBinding) -> None:
    if any(
        not _is_sha256(value)
        for value in (
            binding.cache_identity_sha256,
            binding.scanner_binary_sha256,
            binding.files.database_sha256,
            binding.files.database_metadata_sha256,
            binding.files.java_database_sha256,
            binding.files.java_database_metadata_sha256,
        )
    ):
        raise _invalid()


def _file_identity(metadata: os.stat_result) -> tuple[object, ...]:
    return tuple(getattr(metadata, field) for field in _STABLE_FIELDS)


def _write_all(descriptor: int, payload: bytes) -> None:
    offset = 0
    while offset < len(payload):
        amount = os.write(descriptor, payload[offset:])
        if amount <= 0:
            raise _invalid()
        offset += amount


def _read_descriptor(
    descriptor: int,
    *,
    maximum_bytes: int,
    capture: bool,
) -> tuple[str, bytes | None, os.stat_result]:
    before = os.fstat(descriptor)
    if (
        not stat.S_ISREG(before.st_mode)
        or before.st_nlink != 1
        or not 0 < before.st_size <= maximum_bytes
    ):
        raise _invalid()
    digest = hashlib.sha256()
    payload = bytearray() if capture else None
    total = 0
    while total <= maximum_bytes:
        chunk = os.read(descriptor, min(1024 * 1024, maximum_bytes + 1 - total))
        if not chunk:
            break
        total += len(chunk)
        if total > maximum_bytes:
            raise _invalid()
        digest.update(chunk)
        if payload is not None:
            payload.extend(chunk)
    after = os.fstat(descriptor)
    if total != before.st_size or _file_identity(after) != _file_identity(before):
        raise _invalid()
    return digest.hexdigest(), bytes(payload) if payload is not None else None, after


def _validate_metadata(payload: bytes, *, version: int) -> None:
    try:
        value = json.loads(payload, object_pairs_hook=_unique_object)
    except PersonalDevScannerCacheInstallError:
        raise
    except (TypeError, UnicodeError, ValueError):
        raise _invalid() from None
    if (
        not isinstance(value, dict)
        or value.keys() != _METADATA_KEYS
        or type(value.get("Version")) is not int
        or value["Version"] != version
        or any(
            not isinstance(value.get(field), str)
            or _RFC3339_RE.fullmatch(value[field]) is None
            for field in ("DownloadedAt", "NextUpdate", "UpdatedAt")
        )
    ):
        raise _invalid()


def _open_path_directory(path: Path) -> tuple[int, os.stat_result]:
    metadata = path.lstat()
    if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISDIR(metadata.st_mode):
        raise _invalid()
    descriptor = os.open(path, _DIRECTORY_FLAGS)
    opened = os.fstat(descriptor)
    if (opened.st_dev, opened.st_ino) != (metadata.st_dev, metadata.st_ino):
        os.close(descriptor)
        raise _invalid()
    return descriptor, opened


def _open_directory_at(parent: int, name: str) -> tuple[int, os.stat_result]:
    metadata = os.stat(name, dir_fd=parent, follow_symlinks=False)
    if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISDIR(metadata.st_mode):
        raise _invalid()
    descriptor = os.open(name, _DIRECTORY_FLAGS, dir_fd=parent)
    opened = os.fstat(descriptor)
    if (opened.st_dev, opened.st_ino) != (metadata.st_dev, metadata.st_ino):
        os.close(descriptor)
        raise _invalid()
    return descriptor, opened


@dataclass(slots=True)
class _SourceSnapshot:
    root: int
    root_identity: tuple[int, int]
    directories: dict[str, int]
    directory_identities: dict[str, tuple[object, ...]]
    files: dict[tuple[str, str], os.stat_result]

    def validate(self) -> None:
        if (
            (os.fstat(self.root).st_dev, os.fstat(self.root).st_ino) != self.root_identity
            or set(os.listdir(self.root)) != _SOURCE_ROOT_ENTRIES
        ):
            raise _invalid()
        expected = {"db": _DATABASE_FILES, "java-db": _JAVA_DATABASE_FILES}
        for directory_name, descriptor in self.directories.items():
            if _file_identity(os.fstat(descriptor)) != self.directory_identities[directory_name]:
                raise _invalid()
            if set(os.listdir(descriptor)) != expected[directory_name]:
                raise _invalid()
        for (directory_name, filename), before in self.files.items():
            after = os.stat(
                filename,
                dir_fd=self.directories[directory_name],
                follow_symlinks=False,
            )
            if _file_identity(after) != _file_identity(before):
                raise _invalid()

    def close(self) -> None:
        for descriptor in self.directories.values():
            os.close(descriptor)
        os.close(self.root)


def _source_snapshot(source_root: Path) -> _SourceSnapshot:
    root, root_metadata = _open_path_directory(source_root)
    directories: dict[str, int] = {}
    snapshot: _SourceSnapshot | None = None
    try:
        if set(os.listdir(root)) != _SOURCE_ROOT_ENTRIES or root_metadata.st_mode & 0o022:
            raise _invalid()
        directory_identities: dict[str, tuple[object, ...]] = {}
        files: dict[tuple[str, str], os.stat_result] = {}
        expected = {"db": _DATABASE_FILES, "java-db": _JAVA_DATABASE_FILES}
        total_bytes = 0
        for directory_name, names in expected.items():
            directory, metadata = _open_directory_at(root, directory_name)
            directories[directory_name] = directory
            if (
                metadata.st_mode & 0o022
                or (metadata.st_uid == os.geteuid() and metadata.st_mode & 0o200)
                or metadata.st_uid not in {0, os.geteuid()}
                or set(os.listdir(directory)) != names
            ):
                raise _invalid()
            directory_identities[directory_name] = _file_identity(metadata)
            for filename in names:
                file_metadata = os.stat(filename, dir_fd=directory, follow_symlinks=False)
                maximum = _MAX_METADATA_BYTES if filename == "metadata.json" else _MAX_DATABASE_BYTES
                if (
                    not stat.S_ISREG(file_metadata.st_mode)
                    or file_metadata.st_nlink != 1
                    or file_metadata.st_uid != os.geteuid()
                    or file_metadata.st_gid != os.getegid()
                    or stat.S_IMODE(file_metadata.st_mode) != 0o444
                    or not 0 < file_metadata.st_size <= maximum
                ):
                    raise _invalid()
                total_bytes += file_metadata.st_size
                files[(directory_name, filename)] = file_metadata
        if total_bytes > _MAX_TOTAL_SOURCE_BYTES:
            raise _invalid()
        snapshot = _SourceSnapshot(
            root=root,
            root_identity=(root_metadata.st_dev, root_metadata.st_ino),
            directories=directories,
            directory_identities=directory_identities,
            files=files,
        )
        snapshot.validate()
        return snapshot
    finally:
        if snapshot is None:
            for descriptor in directories.values():
                os.close(descriptor)
            os.close(root)


def _read_source_file(
    snapshot: _SourceSnapshot,
    directory_name: str,
    filename: str,
    *,
    maximum_bytes: int,
    capture: bool,
) -> tuple[str, bytes | None]:
    descriptor = os.open(filename, _FILE_READ_FLAGS, dir_fd=snapshot.directories[directory_name])
    try:
        opened = os.fstat(descriptor)
        if _file_identity(opened) != _file_identity(snapshot.files[(directory_name, filename)]):
            raise _invalid()
        digest, payload, closed = _read_descriptor(
            descriptor,
            maximum_bytes=maximum_bytes,
            capture=capture,
        )
        if _file_identity(closed) != _file_identity(opened):
            raise _invalid()
        return digest, payload
    finally:
        os.close(descriptor)


def _verify_source(snapshot: _SourceSnapshot, expected: PersonalDevScannerCacheBinding) -> None:
    specifications = (
        ("db", "trivy.db", _MAX_DATABASE_BYTES, False, expected.files.database_sha256, None),
        (
            "db",
            "metadata.json",
            _MAX_METADATA_BYTES,
            True,
            expected.files.database_metadata_sha256,
            2,
        ),
        (
            "java-db",
            "trivy-java.db",
            _MAX_DATABASE_BYTES,
            False,
            expected.files.java_database_sha256,
            None,
        ),
        (
            "java-db",
            "metadata.json",
            _MAX_METADATA_BYTES,
            True,
            expected.files.java_database_metadata_sha256,
            1,
        ),
    )
    for directory, filename, maximum, capture, expected_digest, metadata_version in specifications:
        digest, payload = _read_source_file(
            snapshot,
            directory,
            filename,
            maximum_bytes=maximum,
            capture=capture,
        )
        if digest != expected_digest:
            raise _invalid()
        if metadata_version is not None:
            if payload is None:
                raise _invalid()
            _validate_metadata(payload, version=metadata_version)
    snapshot.validate()


def _open_generations(destination: int) -> int:
    try:
        metadata = os.stat("generations", dir_fd=destination, follow_symlinks=False)
    except FileNotFoundError:
        os.mkdir("generations", 0o755, dir_fd=destination)
        metadata = os.stat("generations", dir_fd=destination, follow_symlinks=False)
    if (
        not stat.S_ISDIR(metadata.st_mode)
        or stat.S_ISLNK(metadata.st_mode)
        or metadata.st_uid != os.geteuid()
        or metadata.st_gid != os.getegid()
        or stat.S_IMODE(metadata.st_mode) != 0o755
    ):
        raise _invalid()
    descriptor, opened = _open_directory_at(destination, "generations")
    if _file_identity(opened) != _file_identity(metadata):
        os.close(descriptor)
        raise _invalid()
    return descriptor


def _acquire_installer_lock(destination: int) -> int:
    flags = (
        os.O_RDWR
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_NOFOLLOW", 0)
        | getattr(os, "O_NONBLOCK", 0)
    )
    created = False
    try:
        try:
            descriptor = os.open(
                _INSTALLER_LOCK,
                flags | os.O_CREAT | os.O_EXCL,
                0o600,
                dir_fd=destination,
            )
            created = True
        except FileExistsError:
            descriptor = os.open(_INSTALLER_LOCK, flags, dir_fd=destination)
        try:
            if created:
                os.fchmod(descriptor, 0o600)
            metadata = os.fstat(descriptor)
            if (
                not stat.S_ISREG(metadata.st_mode)
                or metadata.st_nlink != 1
                or metadata.st_uid != os.geteuid()
                or metadata.st_gid != os.getegid()
                or metadata.st_size != 0
                or stat.S_IMODE(metadata.st_mode) != 0o600
            ):
                raise _invalid()
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
            locked = os.fstat(descriptor)
            named = os.stat(
                _INSTALLER_LOCK,
                dir_fd=destination,
                follow_symlinks=False,
            )
            if (
                _file_identity(locked) != _file_identity(metadata)
                or _file_identity(named) != _file_identity(locked)
            ):
                raise _invalid()
            if created:
                os.fsync(descriptor)
                os.fsync(destination)
            return descriptor
        except BaseException:
            os.close(descriptor)
            raise
    except (BlockingIOError, OSError):
        raise _invalid() from None


def _classify_generation_entries(generations: int) -> tuple[set[str], set[str]]:
    names = set(os.listdir(generations))
    if len(names) > _MAX_GENERATION_ENTRIES:
        raise _invalid()
    generation_names = {name for name in names if _GENERATION_RE.fullmatch(name)}
    staging_names = {name for name in names if _STAGING_RE.fullmatch(name)}
    if generation_names | staging_names != names:
        raise _invalid()
    return generation_names, staging_names


def _preflight_delete_tree(
    parent: int,
    name: str,
    *,
    total: list[int],
    entries: list[int],
) -> None:
    metadata = os.stat(name, dir_fd=parent, follow_symlinks=False)
    if (
        not stat.S_ISDIR(metadata.st_mode)
        or stat.S_ISLNK(metadata.st_mode)
        or metadata.st_uid != os.geteuid()
        or metadata.st_gid != os.getegid()
    ):
        raise _invalid()
    directory, opened = _open_directory_at(parent, name)
    try:
        if (opened.st_uid, opened.st_gid) != (os.geteuid(), os.getegid()):
            raise _invalid()
        for child in os.listdir(directory):
            entries[0] += 1
            if entries[0] > _MAX_DELETE_ENTRIES:
                raise _invalid()
            child_metadata = os.stat(child, dir_fd=directory, follow_symlinks=False)
            if (child_metadata.st_uid, child_metadata.st_gid) != (
                os.geteuid(),
                os.getegid(),
            ):
                raise _invalid()
            if stat.S_ISREG(child_metadata.st_mode):
                if child_metadata.st_nlink != 1:
                    raise _invalid()
                total[0] += child_metadata.st_size
                if total[0] > _MAX_DELETE_BYTES:
                    raise _invalid()
            elif stat.S_ISDIR(child_metadata.st_mode):
                _preflight_delete_tree(directory, child, total=total, entries=entries)
            else:
                raise _invalid()
    finally:
        os.close(directory)


def _delete_tree(parent: int, name: str) -> None:
    directory, metadata = _open_directory_at(parent, name)
    try:
        if (metadata.st_uid, metadata.st_gid) != (os.geteuid(), os.getegid()):
            raise _invalid()
        os.fchmod(directory, 0o700)
        for child in os.listdir(directory):
            child_metadata = os.stat(child, dir_fd=directory, follow_symlinks=False)
            if (
                child_metadata.st_uid,
                child_metadata.st_gid,
            ) != (os.geteuid(), os.getegid()):
                raise _invalid()
            if stat.S_ISREG(child_metadata.st_mode) and child_metadata.st_nlink == 1:
                os.unlink(child, dir_fd=directory)
            elif stat.S_ISDIR(child_metadata.st_mode):
                _delete_tree(directory, child)
            else:
                raise _invalid()
        os.fsync(directory)
    finally:
        os.close(directory)
    os.rmdir(name, dir_fd=parent)


def _cleanup_orphaned_staging(generations: int, names: set[str]) -> None:
    total = [0]
    entries = [0]
    for name in sorted(names):
        metadata = os.stat(name, dir_fd=generations, follow_symlinks=False)
        if (
            not stat.S_ISDIR(metadata.st_mode)
            or metadata.st_uid != os.geteuid()
            or metadata.st_gid != os.getegid()
            or stat.S_IMODE(metadata.st_mode) != 0o700
        ):
            raise _invalid()
        _preflight_delete_tree(generations, name, total=total, entries=entries)
    for name in sorted(names):
        _delete_tree(generations, name)
    if names:
        os.fsync(generations)


def _read_regular_at(
    directory: int,
    name: str,
    *,
    maximum_bytes: int,
    mode: int,
    capture: bool,
) -> tuple[str, bytes | None, os.stat_result]:
    metadata = os.stat(name, dir_fd=directory, follow_symlinks=False)
    if (
        not stat.S_ISREG(metadata.st_mode)
        or metadata.st_nlink != 1
        or metadata.st_uid != os.geteuid()
        or metadata.st_gid != os.getegid()
        or stat.S_IMODE(metadata.st_mode) != mode
    ):
        raise _invalid()
    descriptor = os.open(name, _FILE_READ_FLAGS, dir_fd=directory)
    try:
        opened = os.fstat(descriptor)
        if _file_identity(opened) != _file_identity(metadata):
            raise _invalid()
        digest, payload, closed = _read_descriptor(
            descriptor,
            maximum_bytes=maximum_bytes,
            capture=capture,
        )
        if _file_identity(closed) != _file_identity(opened):
            raise _invalid()
        return digest, payload, closed
    finally:
        os.close(descriptor)


def _parse_identity(payload: bytes, generation_name: str) -> PersonalDevScannerCacheBinding:
    try:
        value = json.loads(payload, object_pairs_hook=_unique_object)
    except PersonalDevScannerCacheInstallError:
        raise
    except (TypeError, UnicodeError, ValueError):
        raise _invalid() from None
    if (
        not isinstance(value, dict)
        or value.keys() != _IDENTITY_KEYS
        or _canonical_json(value) != payload
        or type(value.get("schema_version")) is not int
        or value["schema_version"] != 1
        or value.get("cache_identity_sha256") != generation_name
        or any(not _is_sha256(value.get(key)) for key in _IDENTITY_KEYS - {"schema_version"})
    ):
        raise _invalid()
    return PersonalDevScannerCacheBinding(
        cache_identity_sha256=generation_name,
        scanner_binary_sha256=value["scanner_binary_sha256"],
        files=PersonalDevScannerCacheFiles(
            database_sha256=value["database_sha256"],
            database_metadata_sha256=value["database_metadata_sha256"],
            java_database_sha256=value["java_database_sha256"],
            java_database_metadata_sha256=value["java_database_metadata_sha256"],
        ),
    )


@dataclass(frozen=True, slots=True)
class _Generation:
    binding: PersonalDevScannerCacheBinding
    metadata: os.stat_result


def _validate_generation(generations: int, name: str) -> _Generation:
    directory, metadata = _open_directory_at(generations, name)
    try:
        if (
            metadata.st_uid != os.geteuid()
            or metadata.st_gid != os.getegid()
            or stat.S_IMODE(metadata.st_mode) != 0o555
            or set(os.listdir(directory)) != _GENERATION_ROOT_ENTRIES
        ):
            raise _invalid()
        identity_digest, identity_payload, _ = _read_regular_at(
            directory,
            "identity.json",
            maximum_bytes=_MAX_IDENTITY_BYTES,
            mode=0o444,
            capture=True,
        )
        del identity_digest
        if identity_payload is None:
            raise _invalid()
        binding = _parse_identity(identity_payload, name)
        observed: dict[str, str] = {}
        for child_name, expected_files in (
            ("db", _DATABASE_FILES),
            ("java-db", _JAVA_DATABASE_FILES),
        ):
            child, child_metadata = _open_directory_at(directory, child_name)
            try:
                if (
                    child_metadata.st_uid != os.geteuid()
                    or child_metadata.st_gid != os.getegid()
                    or stat.S_IMODE(child_metadata.st_mode) != 0o555
                    or set(os.listdir(child)) != expected_files
                ):
                    raise _invalid()
                database_name = "trivy.db" if child_name == "db" else "trivy-java.db"
                database_digest, _, _ = _read_regular_at(
                    child,
                    database_name,
                    maximum_bytes=_MAX_DATABASE_BYTES,
                    mode=0o444,
                    capture=False,
                )
                metadata_digest, metadata_payload, _ = _read_regular_at(
                    child,
                    "metadata.json",
                    maximum_bytes=_MAX_METADATA_BYTES,
                    mode=0o444,
                    capture=True,
                )
                if metadata_payload is None:
                    raise _invalid()
                _validate_metadata(metadata_payload, version=2 if child_name == "db" else 1)
                observed[child_name + "-database"] = database_digest
                observed[child_name + "-metadata"] = metadata_digest
            finally:
                os.close(child)
        fanal, fanal_metadata = _open_directory_at(directory, "fanal")
        try:
            if (
                fanal_metadata.st_uid != os.geteuid()
                or fanal_metadata.st_gid != os.getegid()
                or stat.S_IMODE(fanal_metadata.st_mode) != 0o770
                or os.listdir(fanal)
            ):
                raise _invalid()
        finally:
            os.close(fanal)
        if observed != {
            "db-database": binding.files.database_sha256,
            "db-metadata": binding.files.database_metadata_sha256,
            "java-db-database": binding.files.java_database_sha256,
            "java-db-metadata": binding.files.java_database_metadata_sha256,
        }:
            raise _invalid()
        after = os.fstat(directory)
        if (after.st_dev, after.st_ino) != (metadata.st_dev, metadata.st_ino):
            raise _invalid()
        return _Generation(binding=binding, metadata=after)
    finally:
        os.close(directory)


def _read_active_generation(destination: int) -> str | None:
    try:
        _, payload, _ = _read_regular_at(
            destination,
            "active-generation",
            maximum_bytes=65,
            mode=0o444,
            capture=True,
        )
    except FileNotFoundError:
        return None
    if payload is None or len(payload) != 65 or payload[-1:] != b"\n":
        raise _invalid()
    try:
        value = payload[:-1].decode("ascii")
    except UnicodeDecodeError:
        raise _invalid() from None
    if not _is_sha256(value):
        raise _invalid()
    return value


def _cleanup_active_staging(destination: int) -> None:
    names = {name for name in os.listdir(destination) if _ACTIVE_STAGING_RE.fullmatch(name)}
    if len(names) > 2:
        raise _invalid()
    for name in names:
        metadata = os.stat(name, dir_fd=destination, follow_symlinks=False)
        if (
            not stat.S_ISREG(metadata.st_mode)
            or metadata.st_nlink != 1
            or metadata.st_uid != os.geteuid()
            or metadata.st_gid != os.getegid()
            or metadata.st_size > 65
        ):
            raise _invalid()
    for name in names:
        os.unlink(name, dir_fd=destination)
    if names:
        os.fsync(destination)


def _create_directory(parent: int, name: str, mode: int) -> int:
    os.mkdir(name, mode, dir_fd=parent)
    descriptor, metadata = _open_directory_at(parent, name)
    if (metadata.st_uid, metadata.st_gid) != (os.geteuid(), os.getegid()):
        os.close(descriptor)
        raise _invalid()
    os.fchmod(descriptor, mode)
    return descriptor


def _copy_source_file(
    snapshot: _SourceSnapshot,
    source_directory: str,
    source_name: str,
    destination: int,
    destination_name: str,
    *,
    maximum_bytes: int,
    expected_sha256: str,
    capture: bool,
) -> bytes | None:
    source = os.open(
        source_name,
        _FILE_READ_FLAGS,
        dir_fd=snapshot.directories[source_directory],
    )
    target: int | None = None
    try:
        before = os.fstat(source)
        if _file_identity(before) != _file_identity(
            snapshot.files[(source_directory, source_name)]
        ):
            raise _invalid()
        target = os.open(destination_name, _FILE_WRITE_FLAGS, 0o400, dir_fd=destination)
        digest = hashlib.sha256()
        payload = bytearray() if capture else None
        total = 0
        while total <= maximum_bytes:
            chunk = os.read(source, min(1024 * 1024, maximum_bytes + 1 - total))
            if not chunk:
                break
            total += len(chunk)
            if total > maximum_bytes:
                raise _invalid()
            digest.update(chunk)
            _write_all(target, chunk)
            if payload is not None:
                payload.extend(chunk)
        after = os.fstat(source)
        target_metadata = os.fstat(target)
        if (
            total != before.st_size
            or _file_identity(after) != _file_identity(before)
            or digest.hexdigest() != expected_sha256
            or target_metadata.st_size != total
            or target_metadata.st_nlink != 1
            or (target_metadata.st_uid, target_metadata.st_gid)
            != (os.geteuid(), os.getegid())
        ):
            raise _invalid()
        os.fchmod(target, 0o444)
        os.fsync(target)
        return bytes(payload) if payload is not None else None
    finally:
        if target is not None:
            os.close(target)
        os.close(source)


def _write_protected_file(directory: int, name: str, payload: bytes, mode: int = 0o444) -> None:
    descriptor = os.open(name, _FILE_WRITE_FLAGS, 0o400, dir_fd=directory)
    try:
        _write_all(descriptor, payload)
        os.fchmod(descriptor, mode)
        os.fsync(descriptor)
        metadata = os.fstat(descriptor)
        if (
            metadata.st_size != len(payload)
            or metadata.st_nlink != 1
            or metadata.st_uid != os.geteuid()
            or metadata.st_gid != os.getegid()
            or stat.S_IMODE(metadata.st_mode) != mode
        ):
            raise _invalid()
    finally:
        os.close(descriptor)


def _publish_generation(
    snapshot: _SourceSnapshot,
    generations: int,
    expected: PersonalDevScannerCacheBinding,
) -> None:
    staging_name = ".loom-scanner-cache-staging-" + secrets.token_hex(12)
    staging: int | None = None
    child_descriptors: list[int] = []
    published = False
    try:
        staging = _create_directory(generations, staging_name, 0o700)
        database = _create_directory(staging, "db", 0o700)
        child_descriptors.append(database)
        java_database = _create_directory(staging, "java-db", 0o700)
        child_descriptors.append(java_database)
        fanal = _create_directory(staging, "fanal", 0o700)
        child_descriptors.append(fanal)
        _copy_source_file(
            snapshot,
            "db",
            "trivy.db",
            database,
            "trivy.db",
            maximum_bytes=_MAX_DATABASE_BYTES,
            expected_sha256=expected.files.database_sha256,
            capture=False,
        )
        database_metadata = _copy_source_file(
            snapshot,
            "db",
            "metadata.json",
            database,
            "metadata.json",
            maximum_bytes=_MAX_METADATA_BYTES,
            expected_sha256=expected.files.database_metadata_sha256,
            capture=True,
        )
        _copy_source_file(
            snapshot,
            "java-db",
            "trivy-java.db",
            java_database,
            "trivy-java.db",
            maximum_bytes=_MAX_DATABASE_BYTES,
            expected_sha256=expected.files.java_database_sha256,
            capture=False,
        )
        java_database_metadata = _copy_source_file(
            snapshot,
            "java-db",
            "metadata.json",
            java_database,
            "metadata.json",
            maximum_bytes=_MAX_METADATA_BYTES,
            expected_sha256=expected.files.java_database_metadata_sha256,
            capture=True,
        )
        if database_metadata is None or java_database_metadata is None:
            raise _invalid()
        _validate_metadata(database_metadata, version=2)
        _validate_metadata(java_database_metadata, version=1)
        _write_protected_file(staging, "identity.json", _canonical_json(_identity_value(expected)))
        for descriptor, mode in ((database, 0o555), (java_database, 0o555), (fanal, 0o770)):
            os.fchmod(descriptor, mode)
            os.fsync(descriptor)
        if set(os.listdir(fanal)):
            raise _invalid()
        os.fchmod(staging, 0o555)
        os.fsync(staging)
        snapshot.validate()
        for descriptor in child_descriptors:
            os.close(descriptor)
        child_descriptors.clear()
        os.close(staging)
        staging = None
        os.rename(
            staging_name,
            expected.cache_identity_sha256,
            src_dir_fd=generations,
            dst_dir_fd=generations,
        )
        published = True
        os.fsync(generations)
    finally:
        for descriptor in child_descriptors:
            os.close(descriptor)
        if staging is not None:
            os.close(staging)
        if not published:
            try:
                total = [0]
                entries = [0]
                _preflight_delete_tree(generations, staging_name, total=total, entries=entries)
                _delete_tree(generations, staging_name)
            except (FileNotFoundError, PersonalDevScannerCacheInstallError, OSError):
                pass


def _select_active(destination: int, selected: str, active: str | None) -> None:
    if active == selected:
        return
    temporary = ".active-generation-" + secrets.token_hex(12)
    created = False
    try:
        _write_protected_file(destination, temporary, (selected + "\n").encode("ascii"))
        created = True
        os.replace(
            temporary,
            "active-generation",
            src_dir_fd=destination,
            dst_dir_fd=destination,
        )
        created = False
        os.fsync(destination)
    finally:
        if created:
            try:
                os.unlink(temporary, dir_fd=destination)
            except OSError:
                pass


def _prune_generations(
    generations: int,
    available: dict[str, _Generation],
    *,
    selected: str,
    previously_active: str | None,
) -> None:
    retain = {selected}
    if previously_active is not None and previously_active != selected:
        retain.add(previously_active)
    elif others := [generation for name, generation in available.items() if name != selected]:
        newest = max(
            others,
            key=lambda generation: (
                generation.metadata.st_ctime_ns,
                generation.binding.cache_identity_sha256,
            ),
        )
        retain.add(newest.binding.cache_identity_sha256)
    delete = sorted(set(available) - retain)
    total = [0]
    entries = [0]
    for name in delete:
        _preflight_delete_tree(generations, name, total=total, entries=entries)
    for name in delete:
        _delete_tree(generations, name)
    if delete:
        os.fsync(generations)


def install_personal_dev_scanner_cache(
    source_root: Path,
    destination_root: Path,
    *,
    expected: PersonalDevScannerCacheBinding,
) -> Path:
    """Publish and select one exact immutable generation, retaining one rollback."""

    source: _SourceSnapshot | None = None
    destination: int | None = None
    installer_lock: int | None = None
    generations: int | None = None
    try:
        _validate_binding(expected)
        destination, _ = _open_path_directory(destination_root)
        installer_lock = _acquire_installer_lock(destination)
        _cleanup_active_staging(destination)
        generations = _open_generations(destination)
        generation_names, staging_names = _classify_generation_entries(generations)
        _cleanup_orphaned_staging(generations, staging_names)
        generation_names, remaining_staging = _classify_generation_entries(generations)
        if remaining_staging:
            raise _invalid()
        available = {
            name: _validate_generation(generations, name) for name in sorted(generation_names)
        }
        active = _read_active_generation(destination)
        if active is not None and active not in available:
            raise _invalid()
        if active is None and available and set(available) != {expected.cache_identity_sha256}:
            raise _invalid()
        source = _source_snapshot(source_root)
        existing = available.get(expected.cache_identity_sha256)
        if existing is not None:
            if existing.binding != expected:
                raise _invalid()
            _verify_source(source, expected)
        else:
            _publish_generation(source, generations, expected)
            published = _validate_generation(generations, expected.cache_identity_sha256)
            if published.binding != expected:
                raise _invalid()
            available[expected.cache_identity_sha256] = published
        _select_active(destination, expected.cache_identity_sha256, active)
        _prune_generations(
            generations,
            available,
            selected=expected.cache_identity_sha256,
            previously_active=active,
        )
        return destination_root / "generations" / expected.cache_identity_sha256
    except PersonalDevScannerCacheInstallError:
        raise
    except (KeyError, OSError, TypeError, UnicodeError, ValueError):
        raise _invalid() from None
    finally:
        if source is not None:
            source.close()
        if generations is not None:
            os.close(generations)
        if installer_lock is not None:
            os.close(installer_lock)
        if destination is not None:
            os.close(destination)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-root", type=Path, required=True)
    parser.add_argument("--destination-root", type=Path, required=True)
    parser.add_argument("--cache-identity-sha256", required=True)
    parser.add_argument("--scanner-binary-sha256", required=True)
    parser.add_argument("--database-sha256", required=True)
    parser.add_argument("--database-metadata-sha256", required=True)
    parser.add_argument("--java-database-sha256", required=True)
    parser.add_argument("--java-database-metadata-sha256", required=True)
    arguments = parser.parse_args()
    binding = PersonalDevScannerCacheBinding(
        cache_identity_sha256=arguments.cache_identity_sha256,
        scanner_binary_sha256=arguments.scanner_binary_sha256,
        files=PersonalDevScannerCacheFiles(
            database_sha256=arguments.database_sha256,
            database_metadata_sha256=arguments.database_metadata_sha256,
            java_database_sha256=arguments.java_database_sha256,
            java_database_metadata_sha256=arguments.java_database_metadata_sha256,
        ),
    )
    try:
        install_personal_dev_scanner_cache(
            arguments.source_root,
            arguments.destination_root,
            expected=binding,
        )
    except PersonalDevScannerCacheInstallError:
        sys.stderr.write("error: personal-dev scanner cache installation failed\n")
        raise SystemExit(1) from None


if __name__ == "__main__":
    main()


__all__ = [
    "PersonalDevScannerCacheInstallError",
    "install_personal_dev_scanner_cache",
    "main",
]
