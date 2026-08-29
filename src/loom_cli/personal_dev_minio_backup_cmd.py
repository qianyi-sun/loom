"""Read-only capture and isolated restore workflows for personal-development MinIO."""

from __future__ import annotations

import hashlib
import json
import os
import re
import stat
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import BinaryIO, Protocol
from urllib.parse import SplitResult, unquote_to_bytes, urlsplit

from loom.personal_dev_minio_backup import (
    PersonalDevMinioBackupError,
    PersonalDevMinioListedObject,
    PersonalDevMinioManifest,
    PersonalDevMinioObject,
    build_personal_dev_minio_manifest,
    install_personal_dev_minio_payload,
    load_personal_dev_minio_manifest,
    normalize_personal_dev_minio_object,
    parse_personal_dev_minio_listing,
    personal_dev_minio_restore_attributes,
    validate_personal_dev_minio_payload_root,
    write_personal_dev_minio_manifest,
)

_BUCKETS = ("artifacts", "trajectories")
_CAPTURE_ALIAS = "local"
_MAX_OBJECTS = 10_000
_MAX_TOTAL_BYTES = 1024 * 1024 * 1024 * 1024
_MAX_COMMAND_STREAM_BYTES = 64 * 1024 * 1024
_MAX_STDERR_BYTES = 64 * 1024
_MAX_BUCKET_LIST_BYTES = 1024 * 1024
_MAX_FEATURE_BYTES = 64 * 1024
_MAX_LISTING_BYTES = 64 * 1024 * 1024
_MAX_STAT_BYTES = 1024 * 1024
_COMMAND_TIMEOUT_SECONDS = 60
_STREAM_TIMEOUT_SECONDS = 3600
_REQUEST_ID_RE = re.compile(r"[0-9A-F]+")
_HOST_ID_RE = re.compile(r"[0-9a-f]+")
_INVALID_URL_ESCAPE_RE = re.compile(r"%(?![0-9A-Fa-f]{2})")


@dataclass(frozen=True, slots=True)
class PersonalDevMinioCommandResult:
    """One bounded internal command result whose bytes are never public errors."""

    returncode: int
    stdout: bytes
    stderr: bytes

    def __post_init__(self) -> None:
        if (
            type(self.returncode) is not int
            or type(self.stdout) is not bytes
            or type(self.stderr) is not bytes
            or len(self.stdout) > _MAX_COMMAND_STREAM_BYTES
            or len(self.stderr) > _MAX_COMMAND_STREAM_BYTES
        ):
            raise PersonalDevMinioBackupError()


class PersonalDevMinioTransport(Protocol):
    """The credential-containing transport boundary used by capture and restore."""

    def run(
        self,
        arguments: Sequence[str],
        *,
        maximum_stdout_bytes: int,
        timeout_seconds: int,
    ) -> PersonalDevMinioCommandResult: ...

    def stream(
        self,
        arguments: Sequence[str],
        *,
        destination: BinaryIO | None,
        expected_size: int,
        timeout_seconds: int,
    ) -> str: ...


def _invalid() -> PersonalDevMinioBackupError:
    return PersonalDevMinioBackupError()


def _reject_duplicate_keys(pairs: list[tuple[str, object]]) -> dict[str, object]:
    value: dict[str, object] = {}
    for key, item in pairs:
        if key in value:
            raise _invalid()
        value[key] = item
    return value


def _one_json(payload: bytes, *, maximum_bytes: int) -> dict[str, object]:
    if type(payload) is not bytes or not payload or len(payload) > maximum_bytes:
        raise _invalid()
    try:
        value = json.loads(
            payload.decode("ascii"),
            object_pairs_hook=_reject_duplicate_keys,
        )
    except (UnicodeDecodeError, json.JSONDecodeError, TypeError, ValueError):
        raise _invalid() from None
    if not isinstance(value, dict):
        raise _invalid()
    return value


def _validate_result(
    result: PersonalDevMinioCommandResult,
    *,
    maximum_stdout_bytes: int,
) -> None:
    if (
        not isinstance(result, PersonalDevMinioCommandResult)
        or len(result.stdout) > maximum_stdout_bytes
        or len(result.stderr) > _MAX_STDERR_BYTES
    ):
        raise _invalid()


def _run_success(
    transport: PersonalDevMinioTransport,
    arguments: tuple[str, ...],
    *,
    maximum_stdout_bytes: int,
) -> bytes:
    result = transport.run(
        arguments,
        maximum_stdout_bytes=maximum_stdout_bytes,
        timeout_seconds=_COMMAND_TIMEOUT_SECONDS,
    )
    _validate_result(result, maximum_stdout_bytes=maximum_stdout_bytes)
    if result.returncode != 0 or result.stderr:
        raise _invalid()
    return result.stdout


def _one_nonzero_json(result: PersonalDevMinioCommandResult) -> dict[str, object]:
    if result.returncode == 0:
        raise _invalid()
    if result.stdout and result.stderr:
        expected_stderr = f"command terminated with exit code {result.returncode}\n".encode("ascii")
        if result.returncode < 1 or result.stderr != expected_stderr:
            raise _invalid()
        payload = result.stdout
    elif result.stdout or result.stderr:
        payload = result.stdout or result.stderr
    else:
        raise _invalid()
    return _one_json(payload, maximum_bytes=_MAX_FEATURE_BYTES)


def _run_absent(
    transport: PersonalDevMinioTransport,
    arguments: tuple[str, ...],
) -> dict[str, object]:
    result = transport.run(
        arguments,
        maximum_stdout_bytes=_MAX_FEATURE_BYTES,
        timeout_seconds=_COMMAND_TIMEOUT_SECONDS,
    )
    _validate_result(result, maximum_stdout_bytes=_MAX_FEATURE_BYTES)
    return _one_nonzero_json(result)


def _bounded_observation(value: object, *, maximum_bytes: int = 4096) -> str:
    if not isinstance(value, str) or not value or len(value.encode("utf-8")) > maximum_bytes:
        raise _invalid()
    if any(ord(character) < 32 or ord(character) == 127 for character in value):
        raise _invalid()
    return value


def _credential_free_url(value: object) -> SplitResult:
    observed = _bounded_observation(value)
    try:
        target = urlsplit(observed)
        hostname = target.hostname
        _ = target.port
    except ValueError:
        raise _invalid() from None
    if (
        target.scheme not in {"http", "https"}
        or not hostname
        or target.username is not None
        or target.password is not None
        or target.query
        or target.fragment
        or _INVALID_URL_ESCAPE_RE.search(target.path) is not None
    ):
        raise _invalid()
    return target


def _check_object_url(value: object, *, listed: PersonalDevMinioListedObject) -> None:
    target = _credential_free_url(value)
    expected_path = f"/{listed.bucket}/{listed.key}".encode()
    if unquote_to_bytes(target.path) != expected_path:
        raise _invalid()


def _check_bucket_inventory(transport: PersonalDevMinioTransport, *, alias: str) -> None:
    payload = _run_success(
        transport,
        ("ls", "--json", alias),
        maximum_stdout_bytes=_MAX_BUCKET_LIST_BYTES,
    )
    try:
        lines = payload.splitlines()
        records = [
            json.loads(line.decode("ascii"), object_pairs_hook=_reject_duplicate_keys)
            for line in lines
        ]
    except (UnicodeDecodeError, json.JSONDecodeError, TypeError, ValueError):
        raise _invalid() from None
    expected_keys = {
        "status",
        "type",
        "lastModified",
        "size",
        "key",
        "etag",
        "url",
        "versionOrdinal",
    }
    if len(records) != len(_BUCKETS):
        raise _invalid()
    buckets: list[str] = []
    for record in records:
        if (
            not isinstance(record, dict)
            or set(record) != expected_keys
            or record["status"] != "success"
            or record["type"] != "folder"
            or type(record["size"]) is not int
            or record["size"] != 0
            or record["etag"] != ""
            or type(record["versionOrdinal"]) is not int
            or record["versionOrdinal"] < 1
        ):
            raise _invalid()
        _bounded_observation(record["lastModified"])
        _credential_free_url(record["url"])
        key = record["key"]
        if not isinstance(key, str) or not key.endswith("/"):
            raise _invalid()
        buckets.append(key[:-1])
    if tuple(buckets) != _BUCKETS:
        raise _invalid()


def _check_versioning(
    transport: PersonalDevMinioTransport,
    *,
    alias: str,
    bucket: str,
) -> None:
    target = f"{alias}/{bucket}"
    payload = _run_success(
        transport,
        ("version", "info", "--json", target),
        maximum_stdout_bytes=_MAX_FEATURE_BYTES,
    )
    if _one_json(payload, maximum_bytes=_MAX_FEATURE_BYTES) != {
        "Op": "info",
        "status": "success",
        "url": target,
        "versioning": {"status": "", "MFADelete": ""},
    }:
        raise _invalid()


def _check_retention(
    transport: PersonalDevMinioTransport,
    *,
    alias: str,
    bucket: str,
) -> None:
    value = _run_absent(
        transport,
        ("retention", "info", "--json", f"{alias}/{bucket}"),
    )
    if value != {
        "status": "error",
        "error": {
            "message": "Remote bucket `%s` does not support locking",
            "cause": {"message": "", "error": {}},
            "type": "fatal",
        },
    }:
        raise _invalid()


def _check_encryption(
    transport: PersonalDevMinioTransport,
    *,
    alias: str,
    bucket: str,
) -> None:
    value = _run_absent(
        transport,
        ("encrypt", "info", "--json", f"{alias}/{bucket}"),
    )
    message = "The server side encryption configuration was not found"
    try:
        error = value["error"]
        cause = error["cause"]  # type: ignore[index]
        detail = cause["error"]
    except (KeyError, TypeError):
        raise _invalid() from None
    if (
        set(value) != {"status", "error"}
        or value["status"] != "error"
        or not isinstance(error, dict)
        or set(error) != {"message", "cause", "type"}
        or error["message"] != "Unable to get encryption info"
        or error["type"] != "fatal"
        or not isinstance(cause, dict)
        or set(cause) != {"message", "error"}
        or cause["message"] != message
        or not isinstance(detail, dict)
        or set(detail)
        != {
            "Code",
            "Message",
            "BucketName",
            "Key",
            "Resource",
            "RequestID",
            "HostID",
            "Region",
            "Server",
        }
        or detail["Code"] != "ServerSideEncryptionConfigurationNotFoundError"
        or detail["Message"] != message
        or detail["BucketName"] != bucket
        or detail["Key"] != ""
        or detail["Resource"] != f"/{bucket}/"
        or detail["Region"] != ""
        or detail["Server"] != "MinIO"
        or not isinstance(detail["RequestID"], str)
        or _REQUEST_ID_RE.fullmatch(detail["RequestID"]) is None
        or len(detail["RequestID"]) > 128
        or not isinstance(detail["HostID"], str)
        or _HOST_ID_RE.fullmatch(detail["HostID"]) is None
        or len(detail["HostID"]) > 128
    ):
        raise _invalid()


def _check_tags(
    transport: PersonalDevMinioTransport,
    *,
    alias: str,
    listed: PersonalDevMinioListedObject,
) -> None:
    result = transport.run(
        ("tag", "list", "--json", f"{alias}/{listed.bucket}/{listed.key}"),
        maximum_stdout_bytes=_MAX_FEATURE_BYTES,
        timeout_seconds=_COMMAND_TIMEOUT_SECONDS,
    )
    _validate_result(result, maximum_stdout_bytes=_MAX_FEATURE_BYTES)
    if result.returncode == 0:
        if result.stderr or not result.stdout:
            raise _invalid()
        value = _one_json(result.stdout, maximum_bytes=_MAX_FEATURE_BYTES)
        if (
            set(value) != {"status", "url", "versionID"}
            or value["status"] != "success"
            or value["versionID"] != ""
        ):
            raise _invalid()
        _check_object_url(value["url"], listed=listed)
        return
    value = _one_nonzero_json(result)
    try:
        error = value["error"]
        cause = error["cause"]  # type: ignore[index]
        message = error["message"]  # type: ignore[index]
    except (KeyError, TypeError):
        raise _invalid() from None
    message_prefix = "No tags found  for "
    observed_message = _bounded_observation(message) if isinstance(message, str) else ""
    if (
        set(value) != {"status", "error"}
        or value["status"] != "error"
        or not isinstance(error, dict)
        or set(error) != {"message", "cause", "type"}
        or error["type"] != "fatal"
        or not isinstance(cause, dict)
        or cause
        != {
            "message": "check 'mc tag set --help' on how to set tags",
            "error": {},
        }
        or not isinstance(message, str)
        or not observed_message.startswith(message_prefix)
        or len(message) == len(message_prefix)
    ):
        raise _invalid()
    _check_object_url(observed_message.removeprefix(message_prefix), listed=listed)


def _check_features(transport: PersonalDevMinioTransport, *, alias: str) -> None:
    for bucket in _BUCKETS:
        _check_versioning(transport, alias=alias, bucket=bucket)
    for bucket in _BUCKETS:
        _check_retention(transport, alias=alias, bucket=bucket)
    for bucket in _BUCKETS:
        _check_encryption(transport, alias=alias, bucket=bucket)


def _list_objects(
    transport: PersonalDevMinioTransport,
    *,
    alias: str,
) -> tuple[PersonalDevMinioListedObject, ...]:
    listed = tuple(
        item
        for bucket in _BUCKETS
        for item in parse_personal_dev_minio_listing(
            _run_success(
                transport,
                ("ls", "--recursive", "--json", f"{alias}/{bucket}"),
                maximum_stdout_bytes=_MAX_LISTING_BYTES,
            ),
            bucket=bucket,
        )
    )
    identities = tuple((item.bucket, item.key) for item in listed)
    if (
        len(listed) > _MAX_OBJECTS
        or sum(item.size_bytes for item in listed) > _MAX_TOTAL_BYTES
        or identities != tuple(sorted(identities))
        or len(set(identities)) != len(identities)
    ):
        raise _invalid()
    return listed


def _stat_object(
    transport: PersonalDevMinioTransport,
    *,
    alias: str,
    listed: PersonalDevMinioListedObject,
) -> bytes:
    return _run_success(
        transport,
        ("stat", "--json", f"{alias}/{listed.bucket}/{listed.key}"),
        maximum_stdout_bytes=_MAX_STAT_BYTES,
    )


def _temporary_payload(payload_root: Path, index: int) -> tuple[Path, BinaryIO]:
    path = payload_root / f".capture-{index:05d}.tmp"
    descriptor = os.open(
        path,
        os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_CLOEXEC,
        0o600,
    )
    return path, os.fdopen(descriptor, "wb")


def _path_exists_without_following(path: Path) -> bool:
    try:
        os.lstat(path)
    except FileNotFoundError:
        return False
    return True


def _would_pollute_payload_root(path: Path, payload_root: Path) -> bool:
    normalized_root = os.path.abspath(payload_root)
    return (
        os.path.abspath(path) == normalized_root or os.path.abspath(path.parent) == normalized_root
    )


def _has_symlinked_ancestor(path: Path) -> bool:
    absolute = Path(os.path.abspath(path))
    current = Path(absolute.anchor)
    for component in absolute.parts[1:-1]:
        current /= component
        try:
            value = os.lstat(current)
        except FileNotFoundError:
            break
        if stat.S_ISLNK(value.st_mode):
            return True
    return False


def _capture_impl(
    *,
    transport: PersonalDevMinioTransport,
    source_manifest_path: Path,
    payload_root: Path,
) -> PersonalDevMinioManifest:
    if (
        not isinstance(source_manifest_path, Path)
        or not isinstance(payload_root, Path)
        or _has_symlinked_ancestor(source_manifest_path)
        or _has_symlinked_ancestor(payload_root)
        or _would_pollute_payload_root(source_manifest_path, payload_root)
        or _path_exists_without_following(source_manifest_path)
        or _path_exists_without_following(payload_root)
    ):
        raise _invalid()
    os.mkdir(payload_root, mode=0o700)
    _check_bucket_inventory(transport, alias=_CAPTURE_ALIAS)
    _check_features(transport, alias=_CAPTURE_ALIAS)
    first_listing = _list_objects(transport, alias=_CAPTURE_ALIAS)
    captured: list[PersonalDevMinioObject] = []
    retained_paths: dict[tuple[str, str], Path] = {}
    stat_sha256s: dict[tuple[str, str], bytes] = {}
    for index, listed in enumerate(first_listing):
        _check_tags(transport, alias=_CAPTURE_ALIAS, listed=listed)
        stat_payload = _stat_object(transport, alias=_CAPTURE_ALIAS, listed=listed)
        temporary_path, destination = _temporary_payload(payload_root, index)
        with destination:
            streamed_digest = transport.stream(
                ("cat", f"{_CAPTURE_ALIAS}/{listed.bucket}/{listed.key}"),
                destination=destination,
                expected_size=listed.size_bytes,
                timeout_seconds=_STREAM_TIMEOUT_SECONDS,
            )
        normalized = normalize_personal_dev_minio_object(
            listed=listed,
            stat_payload=stat_payload,
            payload_path=temporary_path,
        )
        if streamed_digest != normalized.payload_sha256:
            raise _invalid()
        retained_path = install_personal_dev_minio_payload(
            temporary_path=temporary_path,
            payload_root=payload_root,
            object=normalized,
        )
        captured.append(normalized)
        identity = (listed.bucket, listed.key)
        retained_paths[identity] = retained_path
        stat_sha256s[identity] = hashlib.sha256(stat_payload).digest()
    first_manifest = build_personal_dev_minio_manifest(captured)
    _check_bucket_inventory(transport, alias=_CAPTURE_ALIAS)
    _check_features(transport, alias=_CAPTURE_ALIAS)
    second_listing = _list_objects(transport, alias=_CAPTURE_ALIAS)
    if second_listing != first_listing:
        raise _invalid()
    second_objects: list[PersonalDevMinioObject] = []
    for listed in second_listing:
        _check_tags(transport, alias=_CAPTURE_ALIAS, listed=listed)
        identity = (listed.bucket, listed.key)
        second_stat_payload = _stat_object(
            transport,
            alias=_CAPTURE_ALIAS,
            listed=listed,
        )
        if hashlib.sha256(second_stat_payload).digest() != stat_sha256s[identity]:
            raise _invalid()
        second_objects.append(
            normalize_personal_dev_minio_object(
                listed=listed,
                stat_payload=second_stat_payload,
                payload_path=retained_paths[identity],
            )
        )
    second_manifest = build_personal_dev_minio_manifest(second_objects)
    if second_manifest.canonical_bytes != first_manifest.canonical_bytes:
        raise _invalid()
    validate_personal_dev_minio_payload_root(first_manifest, payload_root)
    write_personal_dev_minio_manifest(source_manifest_path, first_manifest)
    return first_manifest


def capture_personal_dev_minio_backup(
    *,
    transport: PersonalDevMinioTransport,
    source_manifest_path: Path,
    payload_root: Path,
) -> PersonalDevMinioManifest:
    """Capture live MinIO through read-only calls into new retained authority."""
    try:
        return _capture_impl(
            transport=transport,
            source_manifest_path=source_manifest_path,
            payload_root=payload_root,
        )
    except Exception:
        pass
    raise _invalid()


def _restore_impl(
    *,
    transport: PersonalDevMinioTransport,
    source_manifest_path: Path,
    payload_root: Path,
    restored_manifest_path: Path,
) -> PersonalDevMinioManifest:
    if (
        not isinstance(source_manifest_path, Path)
        or not isinstance(payload_root, Path)
        or not isinstance(restored_manifest_path, Path)
        or _has_symlinked_ancestor(source_manifest_path)
        or _has_symlinked_ancestor(restored_manifest_path)
        or _has_symlinked_ancestor(payload_root)
        or _would_pollute_payload_root(restored_manifest_path, payload_root)
        or _path_exists_without_following(restored_manifest_path)
    ):
        raise _invalid()
    source = load_personal_dev_minio_manifest(source_manifest_path)
    validate_personal_dev_minio_payload_root(source, payload_root)
    for bucket in _BUCKETS:
        _run_success(
            transport,
            ("mb", f"restore/{bucket}"),
            maximum_stdout_bytes=_MAX_FEATURE_BYTES,
        )
    for object in source.objects:
        _run_success(
            transport,
            (
                "cp",
                "--attr",
                personal_dev_minio_restore_attributes(object),
                str(payload_root / object.payload_sha256),
                f"restore/{object.bucket}/{object.key}",
            ),
            maximum_stdout_bytes=_MAX_STAT_BYTES,
        )
    _check_bucket_inventory(transport, alias="restore")
    _check_features(transport, alias="restore")
    restored_listing = _list_objects(transport, alias="restore")
    expected_listing = tuple(
        PersonalDevMinioListedObject(
            bucket=object.bucket,
            key=object.key,
            size_bytes=object.size_bytes,
        )
        for object in source.objects
    )
    if restored_listing != expected_listing:
        raise _invalid()
    restored_objects: list[PersonalDevMinioObject] = []
    source_by_identity = {(object.bucket, object.key): object for object in source.objects}
    for listed in restored_listing:
        _check_tags(transport, alias="restore", listed=listed)
        source_object = source_by_identity[(listed.bucket, listed.key)]
        normalized_stat = normalize_personal_dev_minio_object(
            listed=listed,
            stat_payload=_stat_object(transport, alias="restore", listed=listed),
            payload_path=payload_root / source_object.payload_sha256,
        )
        readback_digest = transport.stream(
            ("cat", f"restore/{listed.bucket}/{listed.key}"),
            destination=None,
            expected_size=listed.size_bytes,
            timeout_seconds=_STREAM_TIMEOUT_SECONDS,
        )
        restored_objects.append(
            PersonalDevMinioObject(
                bucket=listed.bucket,
                key=listed.key,
                payload_sha256=readback_digest,
                size_bytes=listed.size_bytes,
                content_type=normalized_stat.content_type,
                cache_control=normalized_stat.cache_control,
                metadata=normalized_stat.metadata,
            )
        )
    restored = build_personal_dev_minio_manifest(restored_objects)
    if restored.canonical_bytes != source.canonical_bytes:
        raise _invalid()
    validate_personal_dev_minio_payload_root(source, payload_root)
    write_personal_dev_minio_manifest(restored_manifest_path, restored)
    return restored


def restore_personal_dev_minio_backup(
    *,
    transport: PersonalDevMinioTransport,
    source_manifest_path: Path,
    payload_root: Path,
    restored_manifest_path: Path,
) -> PersonalDevMinioManifest:
    """Restore retained authority into isolation and independently read it back."""
    try:
        return _restore_impl(
            transport=transport,
            source_manifest_path=source_manifest_path,
            payload_root=payload_root,
            restored_manifest_path=restored_manifest_path,
        )
    except Exception:
        pass
    raise _invalid()
