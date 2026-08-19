#!/usr/bin/env python3
"""Materialize one reviewed personal-development scanner-cache asset tree."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import re
import selectors
import shutil
import signal
import stat
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

from loom.personal_dev_scanner_cache import (
    PersonalDevScannerCacheError,
    PersonalDevScannerCacheFiles,
    PersonalDevScannerCacheLock,
    PersonalDevScannerCacheSource,
    load_personal_dev_scanner_cache_lock,
)

_MAX_MANIFEST_BYTES = 1024 * 1024
_MAX_TRIVY_BYTES = 512 * 1024 * 1024
_MAX_DATABASE_BYTES = 4 * 1024 * 1024 * 1024
_MAX_METADATA_BYTES = 64 * 1024
_MAX_TOTAL_ASSET_BYTES = 8 * 1024 * 1024 * 1024
_OCI_MANIFEST_MEDIA_TYPE = "application/vnd.oci.image.manifest.v1+json"
_OCI_EMPTY_MEDIA_TYPE = "application/vnd.oci.empty.v1+json"
_TRIVY_ARTIFACT_TYPE = "application/vnd.aquasec.trivy.config.v1+json"
_TRIVY_DATABASE_LAYER_MEDIA_TYPE = "application/vnd.aquasec.trivy.db.layer.v1.tar+gzip"
_TRIVY_JAVA_DATABASE_LAYER_MEDIA_TYPE = (
    "application/vnd.aquasec.trivy.javadb.layer.v1.tar+gzip"
)
_EMPTY_CONFIG_SHA256 = "sha256:44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a"
_METADATA_KEYS = frozenset({"DownloadedAt", "NextUpdate", "UpdatedAt", "Version"})
_RFC3339 = re.compile(
    r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}"
    r"(?:\.[0-9]{1,9})?Z"
)
_STABLE_STAT_FIELDS = (
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
_DIRECTORY_IDENTITY_FIELDS = ("st_dev", "st_ino", "st_mode", "st_uid", "st_gid")


@dataclass(frozen=True, slots=True)
class _FileSnapshot:
    digest: str
    metadata: os.stat_result


class _DigestSink(Protocol):
    def update(self, payload: bytes, /) -> None: ...


def _failure() -> PersonalDevScannerCacheError:
    return PersonalDevScannerCacheError("scanner cache preparation failed")


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
            raise _failure()
        value[key] = item
    return value


def _stable(before: os.stat_result, after: os.stat_result) -> bool:
    return all(getattr(before, field) == getattr(after, field) for field in _STABLE_STAT_FIELDS)


def _same_directory(before: os.stat_result, after: os.stat_result) -> bool:
    return all(
        getattr(before, field) == getattr(after, field) for field in _DIRECTORY_IDENTITY_FIELDS
    )


def _hash_descriptor(
    descriptor: int,
    *,
    maximum_bytes: int,
    framed_payload: _DigestSink | None = None,
) -> _FileSnapshot:
    before = os.fstat(descriptor)
    if (
        not stat.S_ISREG(before.st_mode)
        or before.st_nlink != 1
        or not 0 < before.st_size <= maximum_bytes
    ):
        raise _failure()
    os.lseek(descriptor, 0, os.SEEK_SET)
    digest = hashlib.sha256()
    if framed_payload is not None:
        framed_payload.update(before.st_size.to_bytes(8, "big"))
    byte_count = 0
    while chunk := os.read(descriptor, min(1024 * 1024, maximum_bytes + 1 - byte_count)):
        byte_count += len(chunk)
        if byte_count > maximum_bytes:
            raise _failure()
        digest.update(chunk)
        if framed_payload is not None:
            framed_payload.update(chunk)
    after = os.fstat(descriptor)
    if byte_count != before.st_size or not _stable(before, after):
        raise _failure()
    return _FileSnapshot(digest=digest.hexdigest(), metadata=after)


def _open_regular(path: Path) -> int:
    flags = (
        os.O_RDONLY
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_NOFOLLOW", 0)
        | getattr(os, "O_NONBLOCK", 0)
    )
    try:
        return os.open(path, flags)
    except OSError:
        raise _failure() from None


def _hash_regular(
    path: Path,
    *,
    maximum_bytes: int,
    framed_payload: _DigestSink | None = None,
) -> _FileSnapshot:
    descriptor = _open_regular(path)
    try:
        snapshot = _hash_descriptor(
            descriptor,
            maximum_bytes=maximum_bytes,
            framed_payload=framed_payload,
        )
        os.fsync(descriptor)
        return snapshot
    except OSError:
        raise _failure() from None
    finally:
        os.close(descriptor)


def _platform_name() -> str:
    machine = platform.machine().lower()
    if machine in {"amd64", "x86_64"}:
        return "linux/amd64"
    if machine in {"aarch64", "arm64"}:
        return "linux/arm64"
    raise _failure()


def _open_verified_trivy(path: Path, lock: PersonalDevScannerCacheLock) -> tuple[int, _FileSnapshot]:
    descriptor = _open_regular(path)
    try:
        snapshot = _hash_descriptor(descriptor, maximum_bytes=_MAX_TRIVY_BYTES)
        if (
            stat.S_IMODE(snapshot.metadata.st_mode) != 0o555
            or snapshot.digest != lock.binary_sha256[_platform_name()]
        ):
            raise _failure()
        return descriptor, snapshot
    except BaseException:
        os.close(descriptor)
        raise


def _bounded_command_output(command: list[str]) -> bytes:
    process: subprocess.Popen[bytes] | None = None
    stdout: Any = None
    completed = False
    try:
        process = subprocess.Popen(
            command,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            start_new_session=True,
        )
        stdout = process.stdout
        if stdout is None:
            raise OSError("manifest inspection stdout is unavailable")
        payload = bytearray()
        deadline = time.monotonic() + 60
        with selectors.DefaultSelector() as selector:
            selector.register(stdout, selectors.EVENT_READ)
            while True:
                remaining = deadline - time.monotonic()
                if remaining <= 0 or not selector.select(remaining):
                    raise subprocess.TimeoutExpired(command, 60)
                chunk = os.read(
                    stdout.fileno(),
                    min(64 * 1024, _MAX_MANIFEST_BYTES + 1 - len(payload)),
                )
                if not chunk:
                    break
                payload.extend(chunk)
                if len(payload) > _MAX_MANIFEST_BYTES:
                    raise OverflowError("manifest inspection output exceeds the bound")
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise subprocess.TimeoutExpired(command, 60)
        returncode = process.wait(timeout=remaining)
        if returncode != 0:
            raise subprocess.CalledProcessError(returncode, command)
        completed = True
    except (OSError, OverflowError, subprocess.SubprocessError):
        raise _failure() from None
    finally:
        if process is not None and not completed:
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except OSError:
                pass
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
        if stdout is not None:
            stdout.close()
    if not payload:
        raise _failure()
    return bytes(payload)


def _verify_manifest(
    source: PersonalDevScannerCacheSource,
    *,
    layer_media_type: str,
) -> None:
    payload = _bounded_command_output(
        ["docker", "buildx", "imagetools", "inspect", "--raw", source.image]
    )
    expected_manifest_sha256 = source.image.rsplit("@sha256:", 1)[1]
    if hashlib.sha256(payload).hexdigest() != expected_manifest_sha256:
        raise _failure()
    try:
        value = json.loads(payload, object_pairs_hook=_unique_object)
    except PersonalDevScannerCacheError:
        raise
    except (TypeError, UnicodeError, ValueError):
        raise _failure() from None
    if (
        not isinstance(value, dict)
        or value.get("schemaVersion") != 2
        or value.get("mediaType") != _OCI_MANIFEST_MEDIA_TYPE
        or value.get("artifactType") != _TRIVY_ARTIFACT_TYPE
    ):
        raise _failure()
    config = value.get("config")
    if (
        not isinstance(config, dict)
        or config.get("mediaType") != _OCI_EMPTY_MEDIA_TYPE
        or config.get("digest") != _EMPTY_CONFIG_SHA256
        or config.get("size") != 2
    ):
        raise _failure()
    layers = value.get("layers")
    if not isinstance(layers, list) or len(layers) != 1 or not isinstance(layers[0], dict):
        raise _failure()
    layer = layers[0]
    layer_size = layer.get("size")
    if (
        layer.get("mediaType") != layer_media_type
        or layer.get("digest") != "sha256:" + source.layer_sha256
        or type(layer_size) is not int
        or not 0 < layer_size <= _MAX_DATABASE_BYTES
    ):
        raise _failure()


def _run_trivy(descriptor: int, arguments: list[str]) -> None:
    try:
        result = subprocess.run(
            [f"/proc/self/fd/{descriptor}", *arguments],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            pass_fds=(descriptor,),
            check=False,
            timeout=15 * 60,
        )
    except (OSError, subprocess.SubprocessError):
        raise _failure() from None
    if result.returncode != 0:
        raise _failure()


def _require_exact_inventory(staging: Path) -> None:
    try:
        root_entries = {entry.name: entry for entry in os.scandir(staging)}
        if root_entries.keys() != {"db", "java-db"}:
            raise _failure()
        expected_files = {
            "db": {"metadata.json", "trivy.db"},
            "java-db": {"metadata.json", "trivy-java.db"},
        }
        for directory_name, names in expected_files.items():
            directory_entry = root_entries[directory_name]
            if not directory_entry.is_dir(follow_symlinks=False):
                raise _failure()
            entries = {
                entry.name: entry for entry in os.scandir(staging / directory_name)
            }
            if entries.keys() != names or any(
                not entry.is_file(follow_symlinks=False) for entry in entries.values()
            ):
                raise _failure()
    except OSError:
        raise _failure() from None


def _validate_metadata(payload: bytes, *, version: int) -> None:
    try:
        value = json.loads(payload, object_pairs_hook=_unique_object)
    except PersonalDevScannerCacheError:
        raise
    except (TypeError, UnicodeError, ValueError):
        raise _failure() from None
    if (
        not isinstance(value, dict)
        or value.keys() != _METADATA_KEYS
        or type(value.get("Version")) is not int
        or value["Version"] != version
        or any(
            not isinstance(value.get(field), str)
            or _RFC3339.fullmatch(value[field]) is None
            for field in ("DownloadedAt", "NextUpdate", "UpdatedAt")
        )
    ):
        raise _failure()


def _read_regular(path: Path, *, maximum_bytes: int) -> tuple[bytes, _FileSnapshot]:
    descriptor = _open_regular(path)
    try:
        snapshot = _hash_descriptor(descriptor, maximum_bytes=maximum_bytes)
        os.lseek(descriptor, 0, os.SEEK_SET)
        payload = bytearray()
        while chunk := os.read(descriptor, min(64 * 1024, maximum_bytes + 1 - len(payload))):
            payload.extend(chunk)
        after = os.fstat(descriptor)
        if len(payload) != snapshot.metadata.st_size or not _stable(snapshot.metadata, after):
            raise _failure()
        os.fsync(descriptor)
        return bytes(payload), snapshot
    except OSError:
        raise _failure() from None
    finally:
        os.close(descriptor)


def _write_evidence(staging: Path, value: object) -> None:
    path = staging / "scanner-cache-evidence.json"
    flags = (
        os.O_WRONLY
        | os.O_CREAT
        | os.O_EXCL
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_NOFOLLOW", 0)
    )
    try:
        descriptor = os.open(path, flags, 0o400)
        try:
            payload = _canonical_json(value) + b"\n"
            offset = 0
            while offset < len(payload):
                offset += os.write(descriptor, payload[offset:])
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
    except OSError:
        raise _failure() from None


def _fsync_directory(path: Path) -> None:
    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
    except OSError:
        raise _failure() from None


def _protect_tree(staging: Path) -> None:
    files = (
        staging / "db/trivy.db",
        staging / "db/metadata.json",
        staging / "java-db/trivy-java.db",
        staging / "java-db/metadata.json",
        staging / "scanner-cache-evidence.json",
    )
    try:
        for path in files:
            os.chmod(path, 0o444, follow_symlinks=False)
            descriptor = _open_regular(path)
            try:
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
        for path in (staging / "db", staging / "java-db"):
            os.chmod(path, 0o555, follow_symlinks=False)
            _fsync_directory(path)
        os.chmod(staging, 0o555, follow_symlinks=False)
        _fsync_directory(staging)
    except OSError:
        raise _failure() from None


def _cleanup_staging(path: Path | None) -> None:
    if path is None:
        return
    try:
        for root, directories, _files in os.walk(path):
            os.chmod(root, 0o700, follow_symlinks=False)
            for name in directories:
                child = Path(root) / name
                if not child.is_symlink():
                    os.chmod(child, 0o700, follow_symlinks=False)
        shutil.rmtree(path)
    except OSError:
        pass


def verify_personal_dev_scanner_cache_assets(lock_path: Path, output: Path) -> str:
    """Revalidate one transported asset tree and return its framed fingerprint."""

    try:
        lock = load_personal_dev_scanner_cache_lock(lock_path)
        metadata = output.lstat()
        if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISDIR(metadata.st_mode):
            raise _failure()
        root_entries = {entry.name: entry for entry in os.scandir(output)}
        if root_entries.keys() != {"db", "java-db", "scanner-cache-evidence.json"}:
            raise _failure()
        expected_files = {
            "db": {"metadata.json", "trivy.db"},
            "java-db": {"metadata.json", "trivy-java.db"},
        }
        for directory_name, names in expected_files.items():
            directory_entry = root_entries[directory_name]
            if not directory_entry.is_dir(follow_symlinks=False):
                raise _failure()
            entries = {entry.name: entry for entry in os.scandir(output / directory_name)}
            if entries.keys() != names or any(
                not entry.is_file(follow_symlinks=False) for entry in entries.values()
            ):
                raise _failure()
        evidence_bytes, _ = _read_regular(
            output / "scanner-cache-evidence.json",
            maximum_bytes=_MAX_METADATA_BYTES,
        )
        evidence = json.loads(evidence_bytes, object_pairs_hook=_unique_object)
        if (
            not isinstance(evidence, dict)
            or evidence.keys()
            != {
                "binary_platform",
                "binary_sha256",
                "database",
                "java_database",
                "lock_sha256",
                "schema_version",
                "trivy_version",
            }
            or _canonical_json(evidence) + b"\n" != evidence_bytes
            or evidence.get("schema_version") != 1
            or evidence.get("trivy_version") != lock.trivy_version
            or evidence.get("lock_sha256") != lock.sha256
        ):
            raise _failure()
        binary_platform = evidence.get("binary_platform")
        if (
            not isinstance(binary_platform, str)
            or evidence.get("binary_sha256") != lock.binary_sha256.get(binary_platform)
        ):
            raise _failure()
        database = evidence.get("database")
        java_database = evidence.get("java_database")
        expected_record_keys = {"image", "layer_sha256", "metadata_sha256", "sha256"}
        if (
            not isinstance(database, dict)
            or database.keys() != expected_record_keys
            or database.get("image") != lock.database.image
            or database.get("layer_sha256") != lock.database.layer_sha256
            or not isinstance(java_database, dict)
            or java_database.keys() != expected_record_keys
            or java_database.get("image") != lock.java_database.image
            or java_database.get("layer_sha256") != lock.java_database.layer_sha256
        ):
            raise _failure()
        paths = {
            "db/metadata.json": (database.get("metadata_sha256"), _MAX_METADATA_BYTES, 2),
            "db/trivy.db": (database.get("sha256"), _MAX_DATABASE_BYTES, None),
            "java-db/metadata.json": (
                java_database.get("metadata_sha256"),
                _MAX_METADATA_BYTES,
                1,
            ),
            "java-db/trivy-java.db": (
                java_database.get("sha256"),
                _MAX_DATABASE_BYTES,
                None,
            ),
        }
        fingerprint = hashlib.sha256(b"loom-scanner-cache-build-context-v1\0")
        for relative, (expected_digest, maximum_bytes, metadata_version) in paths.items():
            fingerprint.update(relative.encode("ascii") + b"\0")
            if metadata_version is None:
                payload = None
                snapshot = _hash_regular(
                    output / relative,
                    maximum_bytes=maximum_bytes,
                    framed_payload=fingerprint,
                )
            else:
                payload, snapshot = _read_regular(
                    output / relative,
                    maximum_bytes=maximum_bytes,
                )
                fingerprint.update(len(payload).to_bytes(8, "big") + payload)
            if snapshot.digest != expected_digest:
                raise _failure()
            if metadata_version is not None:
                if payload is None:
                    raise _failure()
                _validate_metadata(payload, version=metadata_version)
        fingerprint.update(evidence_bytes)
        return fingerprint.hexdigest()
    except PersonalDevScannerCacheError:
        raise PersonalDevScannerCacheError("scanner cache asset verification failed") from None
    except (KeyError, OSError, TypeError, UnicodeError, ValueError):
        raise PersonalDevScannerCacheError("scanner cache asset verification failed") from None


def prepare_personal_dev_scanner_cache_assets(
    lock_path: Path,
    trivy: Path,
    output: Path,
) -> PersonalDevScannerCacheFiles:
    """Download, validate, and atomically publish exact scanner-cache assets."""

    staging: Path | None = None
    trivy_descriptor: int | None = None
    try:
        try:
            output.lstat()
        except FileNotFoundError:
            pass
        else:
            raise _failure()
        parent = output.parent
        parent_metadata = parent.lstat()
        if not stat.S_ISDIR(parent_metadata.st_mode) or stat.S_ISLNK(parent_metadata.st_mode):
            raise _failure()
        lock = load_personal_dev_scanner_cache_lock(lock_path)
        trivy_descriptor, original_trivy = _open_verified_trivy(trivy, lock)
        _verify_manifest(
            lock.database,
            layer_media_type=_TRIVY_DATABASE_LAYER_MEDIA_TYPE,
        )
        _verify_manifest(
            lock.java_database,
            layer_media_type=_TRIVY_JAVA_DATABASE_LAYER_MEDIA_TYPE,
        )
        staging = Path(tempfile.mkdtemp(prefix=f".{output.name}.staging-", dir=parent))
        _run_trivy(
            trivy_descriptor,
            [
                "image",
                "--download-db-only",
                "--db-repository",
                lock.database.image,
                "--cache-dir",
                str(staging),
                "--no-progress",
            ],
        )
        _run_trivy(
            trivy_descriptor,
            [
                "image",
                "--download-java-db-only",
                "--java-db-repository",
                lock.java_database.image,
                "--cache-dir",
                str(staging),
                "--no-progress",
            ],
        )
        current_trivy = _hash_descriptor(trivy_descriptor, maximum_bytes=_MAX_TRIVY_BYTES)
        if current_trivy.digest != original_trivy.digest or not _stable(
            original_trivy.metadata, current_trivy.metadata
        ):
            raise _failure()
        _require_exact_inventory(staging)
        database = _hash_regular(staging / "db/trivy.db", maximum_bytes=_MAX_DATABASE_BYTES)
        database_metadata_bytes, database_metadata = _read_regular(
            staging / "db/metadata.json", maximum_bytes=_MAX_METADATA_BYTES
        )
        java_database = _hash_regular(
            staging / "java-db/trivy-java.db", maximum_bytes=_MAX_DATABASE_BYTES
        )
        java_database_metadata_bytes, java_database_metadata = _read_regular(
            staging / "java-db/metadata.json", maximum_bytes=_MAX_METADATA_BYTES
        )
        total_bytes = sum(
            snapshot.metadata.st_size
            for snapshot in (
                database,
                database_metadata,
                java_database,
                java_database_metadata,
            )
        )
        if total_bytes > _MAX_TOTAL_ASSET_BYTES:
            raise _failure()
        _validate_metadata(database_metadata_bytes, version=2)
        _validate_metadata(java_database_metadata_bytes, version=1)
        files = PersonalDevScannerCacheFiles(
            database_sha256=database.digest,
            database_metadata_sha256=database_metadata.digest,
            java_database_sha256=java_database.digest,
            java_database_metadata_sha256=java_database_metadata.digest,
        )
        evidence = {
            "binary_platform": _platform_name(),
            "binary_sha256": original_trivy.digest,
            "database": {
                "image": lock.database.image,
                "layer_sha256": lock.database.layer_sha256,
                "metadata_sha256": files.database_metadata_sha256,
                "sha256": files.database_sha256,
            },
            "java_database": {
                "image": lock.java_database.image,
                "layer_sha256": lock.java_database.layer_sha256,
                "metadata_sha256": files.java_database_metadata_sha256,
                "sha256": files.java_database_sha256,
            },
            "lock_sha256": lock.sha256,
            "schema_version": 1,
            "trivy_version": lock.trivy_version,
        }
        _write_evidence(staging, evidence)
        _protect_tree(staging)
        try:
            output.lstat()
        except FileNotFoundError:
            pass
        else:
            raise _failure()
        if not _same_directory(parent_metadata, parent.lstat()):
            raise _failure()
        os.rename(staging, output)
        staging = None
        _fsync_directory(parent)
        return files
    except PersonalDevScannerCacheError:
        raise
    except (KeyError, OSError, TypeError, ValueError):
        raise _failure() from None
    finally:
        if trivy_descriptor is not None:
            os.close(trivy_descriptor)
        _cleanup_staging(staging)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--lock", type=Path, required=True)
    parser.add_argument("--trivy", type=Path)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--output", type=Path)
    mode.add_argument("--verify-output", type=Path)
    arguments = parser.parse_args()
    try:
        if arguments.verify_output is not None:
            if arguments.trivy is not None:
                raise PersonalDevScannerCacheError("scanner cache asset verification failed")
            fingerprint = verify_personal_dev_scanner_cache_assets(
                arguments.lock,
                arguments.verify_output,
            )
            sys.stdout.write(fingerprint + "\n")
        else:
            if arguments.trivy is None or arguments.output is None:
                raise PersonalDevScannerCacheError("scanner cache preparation failed")
            prepare_personal_dev_scanner_cache_assets(
                arguments.lock,
                arguments.trivy,
                arguments.output,
            )
    except PersonalDevScannerCacheError:
        sys.stderr.write("error: scanner cache preparation failed\n")
        raise SystemExit(1) from None


if __name__ == "__main__":
    main()


__all__ = [
    "prepare_personal_dev_scanner_cache_assets",
    "verify_personal_dev_scanner_cache_assets",
]
