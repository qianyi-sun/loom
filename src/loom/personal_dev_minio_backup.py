"""Strict authority records for personal-development MinIO backup payloads."""

from __future__ import annotations

import base64
import binascii
import hashlib
import json
import os
import re
import stat
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType
from urllib.parse import urlsplit

_ERROR_MESSAGE = "personal-dev MinIO backup is invalid"
_BUCKETS = ("artifacts", "trajectories")
_MANIFEST_SCHEMA = "loom-personal-dev-minio-backup-manifest-v1"
_INVENTORY_SCHEMA = "loom-personal-dev-minio-payload-inventory-v1"
_MAX_OBJECTS = 10_000
_MAX_OBJECT_BYTES = 64 * 1024 * 1024 * 1024
_MAX_TOTAL_BYTES = 1024 * 1024 * 1024 * 1024
_MAX_KEY_BYTES = 1_024
_MAX_METADATA_ENTRIES = 64
_MAX_METADATA_KEY_BYTES = 128
_MAX_METADATA_VALUE_BYTES = 2_048
_MAX_METADATA_BYTES = 16 * 1024
_MAX_MANIFEST_BYTES = 64 * 1024 * 1024
_MAX_OBSERVATION_BYTES = 4_096
_SHA256_RE = re.compile(r"[0-9a-f]{64}")
_METADATA_KEY_RE = re.compile(r"[a-z][a-z0-9-]{0,127}")
_CONTENT_TYPE_RE = re.compile(r"[A-Za-z0-9!#$&^_.+-]+/[A-Za-z0-9!#$&^_.+-]+")


class PersonalDevMinioBackupError(ValueError):
    """The public, non-sensitive failure for invalid backup authority."""

    def __init__(self) -> None:
        super().__init__(_ERROR_MESSAGE)


def _invalid() -> PersonalDevMinioBackupError:
    return PersonalDevMinioBackupError()


def _is_plain_int(value: object) -> bool:
    return type(value) is int


def _has_unsafe_delimiter(value: str) -> bool:
    return ";" in value or any(ord(character) < 32 or ord(character) == 127 for character in value)


def _validate_key(value: object) -> str:
    if (
        not isinstance(value, str)
        or not value
        or any(ord(character) < 32 or ord(character) == 127 for character in value)
    ):
        raise _invalid()
    try:
        encoded = value.encode("utf-8")
    except UnicodeEncodeError:
        raise _invalid() from None
    if (
        len(encoded) > _MAX_KEY_BYTES
        or "\\" in value
        or value.startswith("/")
        or "//" in value
        or any(segment in {".", ".."} for segment in value.split("/"))
    ):
        raise _invalid()
    return value


def _validate_attribute(value: object, *, content_type: bool = False) -> str:
    if not isinstance(value, str) or not value or _has_unsafe_delimiter(value):
        raise _invalid()
    try:
        encoded = value.encode("utf-8")
    except UnicodeEncodeError:
        raise _invalid() from None
    if len(encoded) > _MAX_METADATA_VALUE_BYTES:
        raise _invalid()
    if content_type and _CONTENT_TYPE_RE.fullmatch(value) is None:
        raise _invalid()
    return value


def _canonical_bytes(payload: object) -> bytes:
    try:
        value = json.dumps(
            payload,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            allow_nan=False,
        ).encode("ascii")
    except (TypeError, ValueError, UnicodeEncodeError):
        raise _invalid() from None
    if len(value) > _MAX_MANIFEST_BYTES:
        raise _invalid()
    return value


@dataclass(frozen=True, slots=True)
class PersonalDevMinioListedObject:
    """A list result whose size is subsequently pinned by stat and payload bytes."""

    bucket: str
    key: str
    size_bytes: int

    def __post_init__(self) -> None:
        if self.bucket not in _BUCKETS:
            raise _invalid()
        _validate_key(self.key)
        if not _is_plain_int(self.size_bytes) or not 0 <= self.size_bytes <= _MAX_OBJECT_BYTES:
            raise _invalid()


@dataclass(frozen=True, slots=True)
class PersonalDevMinioObject:
    """An immutable restorable S3 object bound to retained payload bytes."""

    bucket: str
    key: str
    payload_sha256: str
    size_bytes: int
    content_type: str
    cache_control: str | None
    metadata: Mapping[str, str]

    def __post_init__(self) -> None:
        if self.bucket not in _BUCKETS:
            raise _invalid()
        _validate_key(self.key)
        if (
            not isinstance(self.payload_sha256, str)
            or _SHA256_RE.fullmatch(self.payload_sha256) is None
        ):
            raise _invalid()
        if not _is_plain_int(self.size_bytes) or not 0 <= self.size_bytes <= _MAX_OBJECT_BYTES:
            raise _invalid()
        _validate_attribute(self.content_type, content_type=True)
        if self.cache_control is not None:
            _validate_attribute(self.cache_control)
        if not isinstance(self.metadata, Mapping) or len(self.metadata) > _MAX_METADATA_ENTRIES:
            raise _invalid()
        normalized: dict[str, str] = {}
        total_bytes = 0
        for key, value in self.metadata.items():
            if (
                not isinstance(key, str)
                or key != key.lower()
                or _METADATA_KEY_RE.fullmatch(key) is None
            ):
                raise _invalid()
            encoded_key = key.encode("utf-8")
            if len(encoded_key) > _MAX_METADATA_KEY_BYTES:
                raise _invalid()
            normalized[key] = _validate_attribute(value)
            total_bytes += len(encoded_key) + len(normalized[key].encode("utf-8"))
        if total_bytes > _MAX_METADATA_BYTES:
            raise _invalid()
        object.__setattr__(self, "metadata", MappingProxyType(normalized))

    def _manifest_value(self) -> dict[str, object]:
        return {
            "bucket": self.bucket,
            "cache_control": self.cache_control,
            "content_type": self.content_type,
            "key": self.key,
            "metadata": dict(self.metadata),
            "payload_sha256": self.payload_sha256,
            "size_bytes": self.size_bytes,
        }


@dataclass(frozen=True, slots=True)
class PersonalDevMinioManifest:
    """A canonical, identity-sorted collection of restorable MinIO objects."""

    objects: tuple[PersonalDevMinioObject, ...]

    def __post_init__(self) -> None:
        if not isinstance(self.objects, tuple) or len(self.objects) > _MAX_OBJECTS:
            raise _invalid()
        if any(not isinstance(item, PersonalDevMinioObject) for item in self.objects):
            raise _invalid()
        identities = tuple((item.bucket, item.key) for item in self.objects)
        if identities != tuple(sorted(identities)) or len(set(identities)) != len(identities):
            raise _invalid()
        digest_sizes: dict[str, int] = {}
        for item in self.objects:
            if (
                item.payload_sha256 in digest_sizes
                and digest_sizes[item.payload_sha256] != item.size_bytes
            ):
                raise _invalid()
            digest_sizes[item.payload_sha256] = item.size_bytes
        if sum(item.size_bytes for item in self.objects) > _MAX_TOTAL_BYTES:
            raise _invalid()
        if len(self.canonical_bytes) > _MAX_MANIFEST_BYTES:
            raise _invalid()

    @property
    def canonical_bytes(self) -> bytes:
        if not self.objects:
            return _canonical_bytes({"buckets": list(_BUCKETS), "objects": []})
        return _canonical_bytes(
            {
                "buckets": list(_BUCKETS),
                "objects": [item._manifest_value() for item in self.objects],
                "schema": _MANIFEST_SCHEMA,
            }
        )

    @property
    def object_count(self) -> int:
        return len(self.objects)

    @property
    def total_payload_bytes(self) -> int:
        return sum(item.size_bytes for item in self.objects)

    @property
    def payload_inventory_bytes(self) -> bytes:
        payloads = sorted({(item.payload_sha256, item.size_bytes) for item in self.objects})
        return _canonical_bytes(
            {
                "payloads": [
                    {"sha256": payload_sha256, "size_bytes": size_bytes}
                    for payload_sha256, size_bytes in payloads
                ],
                "schema": _INVENTORY_SCHEMA,
            }
        )


def build_personal_dev_minio_manifest(
    objects: Sequence[PersonalDevMinioObject],
) -> PersonalDevMinioManifest:
    """Build a canonical manifest from already-normalized objects."""
    try:
        return PersonalDevMinioManifest(tuple(objects))
    except (TypeError, ValueError):
        raise _invalid() from None


def _reject_duplicate_json_keys(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise _invalid()
        result[key] = value
    return result


def _load_manifest_value(payload: bytes) -> PersonalDevMinioManifest:
    if type(payload) is not bytes or len(payload) > _MAX_MANIFEST_BYTES:
        raise _invalid()
    try:
        decoded = payload.decode("ascii")
        value = json.loads(decoded, object_pairs_hook=_reject_duplicate_json_keys)
    except (UnicodeDecodeError, json.JSONDecodeError, TypeError, ValueError):
        raise _invalid() from None
    if not isinstance(value, dict) or set(value) not in (
        {"buckets", "objects"},
        {"buckets", "objects", "schema"},
    ):
        raise _invalid()
    buckets = value.get("buckets")
    objects = value.get("objects")
    if buckets != list(_BUCKETS) or not isinstance(objects, list):
        raise _invalid()
    if not objects:
        if set(value) != {"buckets", "objects"}:
            raise _invalid()
    elif value.get("schema") != _MANIFEST_SCHEMA:
        raise _invalid()
    try:
        manifest = build_personal_dev_minio_manifest(
            tuple(
                PersonalDevMinioObject(
                    bucket=item["bucket"],
                    key=item["key"],
                    payload_sha256=item["payload_sha256"],
                    size_bytes=item["size_bytes"],
                    content_type=item["content_type"],
                    cache_control=item["cache_control"],
                    metadata=item["metadata"],
                )
                for item in objects
                if isinstance(item, dict)
                and set(item)
                == {
                    "bucket",
                    "cache_control",
                    "content_type",
                    "key",
                    "metadata",
                    "payload_sha256",
                    "size_bytes",
                }
            )
        )
    except (KeyError, TypeError, ValueError):
        raise _invalid() from None
    if len(manifest.objects) != len(objects) or manifest.canonical_bytes != payload:
        raise _invalid()
    return manifest


def load_personal_dev_minio_manifest(path: Path) -> PersonalDevMinioManifest:
    """Load only a byte-for-byte canonical manifest from an owner-controlled path."""
    if not isinstance(path, Path):
        raise _invalid()
    try:
        _validate_payload_root(path.parent)
        _, _, _, payload = _read_owner_only_payload(
            path,
            maximum_size=_MAX_MANIFEST_BYTES,
            capture_bytes=True,
        )
        if payload is None:
            raise _invalid()
        return _load_manifest_value(payload)
    except (OSError, TypeError, ValueError):
        raise _invalid() from None


def _load_json_lines(payload: bytes) -> tuple[dict[str, object], ...]:
    if type(payload) is not bytes:
        raise _invalid()
    try:
        lines = payload.splitlines()
        if any(not line for line in lines):
            raise _invalid()
        return tuple(
            value
            for line in lines
            for value in (
                json.loads(line.decode("ascii"), object_pairs_hook=_reject_duplicate_json_keys),
            )
            if isinstance(value, dict)
        )
    except (UnicodeDecodeError, json.JSONDecodeError, TypeError, ValueError):
        raise _invalid() from None


def _validate_observation(value: object) -> str:
    if not isinstance(value, str) or not value or _has_unsafe_delimiter(value):
        raise _invalid()
    return value


def _validate_listing_observations(record: Mapping[str, object]) -> bool:
    if "url" not in record:
        return True
    _validate_key(record.get("key"))
    url = _validate_observation(record.get("url"))
    try:
        encoded_url = url.encode("utf-8")
        parsed = urlsplit(url)
        hostname = parsed.hostname
        _ = parsed.port
        username = parsed.username
        password = parsed.password
    except (UnicodeEncodeError, ValueError):
        raise _invalid() from None
    if (
        record.get("storageClass") != "STANDARD"
        or not _is_plain_int(record.get("versionOrdinal"))
        or record["versionOrdinal"] < 1  # type: ignore[operator]
        or parsed.scheme not in {"http", "https"}
        or not hostname
        or username is not None
        or password is not None
        or parsed.query
        or parsed.fragment
        or len(encoded_url) > _MAX_OBSERVATION_BYTES
    ):
        raise _invalid()
    return True


def parse_personal_dev_minio_listing(
    payload: bytes,
    *,
    bucket: str,
) -> tuple[PersonalDevMinioListedObject, ...]:
    """Parse only the fixed JSON-lines surface emitted by the trusted ``mc`` client."""
    if type(payload) is not bytes or bucket not in _BUCKETS:
        raise _invalid()
    records = _load_json_lines(payload)
    if len(records) != len(payload.splitlines()) or len(records) > _MAX_OBJECTS:
        raise _invalid()
    try:
        listed = tuple(
            PersonalDevMinioListedObject(
                bucket=bucket,
                key=record["key"],
                size_bytes=record["size"],
            )
            for record in records
            if set(record)
            in (
                {
                    "status",
                    "type",
                    "key",
                    "size",
                    "etag",
                    "lastModified",
                    "storageClass",
                    "url",
                    "versionOrdinal",
                },
            )
            and record["status"] == "success"
            and record["type"] == "file"
            and _validate_listing_observations(record)
            and _validate_observation(record["etag"])
            and _validate_observation(record["lastModified"])
        )
    except (KeyError, TypeError, ValueError):
        raise _invalid() from None
    if len(listed) != len(records):
        raise _invalid()
    return listed


def _normalized_stat_metadata(value: object) -> tuple[str, str | None, Mapping[str, str]]:
    if not isinstance(value, dict) or "Content-Type" not in value:
        raise _invalid()
    content_type: str | None = None
    cache_control: str | None = None
    custom: dict[str, str] = {}
    for header, header_value in value.items():
        if header == "Content-Type":
            content_type = _validate_attribute(header_value, content_type=True)
        elif header == "Cache-Control":
            cache_control = _validate_attribute(header_value)
        elif isinstance(header, str) and header.startswith("X-Amz-Meta-"):
            key = header.removeprefix("X-Amz-Meta-").lower()
            if key in custom:
                raise _invalid()
            custom[key] = _validate_attribute(header_value)
        else:
            raise _invalid()
    if content_type is None:
        raise _invalid()
    return content_type, cache_control, custom


def _validate_stat_checksum(value: object) -> None:
    if not isinstance(value, dict) or set(value) != {"CRC32C"}:
        raise _invalid()
    checksum = value["CRC32C"]
    if not isinstance(checksum, str):
        raise _invalid()
    encoded, separator, part_count = checksum.partition("-")
    if separator and (not re.fullmatch(r"[1-9][0-9]{0,4}", part_count) or int(part_count) > 10_000):
        raise _invalid()
    try:
        decoded = base64.b64decode(encoded.encode("ascii"), validate=True)
    except (UnicodeEncodeError, binascii.Error, ValueError):
        raise _invalid() from None
    if len(decoded) != 4:
        raise _invalid()


def _hash_payload_path(path: Path) -> tuple[str, int]:
    digest, size, _, _ = _read_owner_only_payload(path)
    return digest, size


def normalize_personal_dev_minio_object(
    *,
    listed: PersonalDevMinioListedObject,
    stat_payload: bytes,
    payload_path: Path,
) -> PersonalDevMinioObject:
    """Bind one listed object, its fixed stat response, and captured payload bytes."""
    if (
        not isinstance(listed, PersonalDevMinioListedObject)
        or type(stat_payload) is not bytes
        or not isinstance(payload_path, Path)
    ):
        raise _invalid()
    records = _load_json_lines(stat_payload)
    if len(records) != 1:
        raise _invalid()
    record = records[0]
    supported_keys = {
        "status",
        "type",
        "name",
        "size",
        "etag",
        "lastModified",
        "metadata",
    }
    if set(record) not in (supported_keys, supported_keys | {"checksum"}):
        raise _invalid()
    try:
        if (
            record["status"] != "success"
            or record["type"] != "file"
            or record["name"] != listed.key.rsplit("/", 1)[-1]
            or type(record["size"]) is not int
            or record["size"] != listed.size_bytes
        ):
            raise _invalid()
        _validate_observation(record["etag"])
        _validate_observation(record["lastModified"])
        if "checksum" in record:
            _validate_stat_checksum(record["checksum"])
        content_type, cache_control, metadata = _normalized_stat_metadata(record["metadata"])
        payload_sha256, payload_size = _hash_payload_path(payload_path)
        if payload_size != listed.size_bytes:
            raise _invalid()
        return PersonalDevMinioObject(
            bucket=listed.bucket,
            key=listed.key,
            payload_sha256=payload_sha256,
            size_bytes=listed.size_bytes,
            content_type=content_type,
            cache_control=cache_control,
            metadata=metadata,
        )
    except (KeyError, TypeError, ValueError):
        raise _invalid() from None


def personal_dev_minio_restore_attributes(object: PersonalDevMinioObject) -> str:
    """Build one delimiter-safe ``mc cp --attr`` value from supported authority."""
    if not isinstance(object, PersonalDevMinioObject):
        raise _invalid()
    attributes = [f"Content-Type={object.content_type}"]
    if object.cache_control is not None:
        attributes.append(f"Cache-Control={object.cache_control}")
    attributes.extend(f"X-Amz-Meta-{key}={value}" for key, value in sorted(object.metadata.items()))
    return ";".join(attributes)


def _stat_identity(value: os.stat_result) -> tuple[int, int, int, int, int, int, int, int]:
    return (
        value.st_dev,
        value.st_ino,
        value.st_mode,
        value.st_uid,
        value.st_nlink,
        value.st_size,
        value.st_mtime_ns,
        value.st_ctime_ns,
    )


def _file_handle_identity(value: os.stat_result) -> tuple[int, int, int, int, int]:
    return value.st_dev, value.st_ino, value.st_mode, value.st_uid, value.st_nlink


def _validate_payload_stat(value: os.stat_result) -> None:
    if (
        not stat.S_ISREG(value.st_mode)
        or stat.S_IMODE(value.st_mode) != 0o600
        or value.st_uid != os.geteuid()
        or value.st_nlink != 1
        or not 0 <= value.st_size <= _MAX_OBJECT_BYTES
    ):
        raise _invalid()


def _validate_payload_root(path: Path) -> os.stat_result:
    try:
        value = os.lstat(path)
    except (OSError, TypeError):
        raise _invalid() from None
    if (
        not stat.S_ISDIR(value.st_mode)
        or stat.S_IMODE(value.st_mode) != 0o700
        or value.st_uid != os.geteuid()
    ):
        raise _invalid()
    return value


def _read_owner_only_payload(
    path: Path,
    *,
    maximum_size: int = _MAX_OBJECT_BYTES,
    capture_bytes: bool = False,
) -> tuple[str, int, os.stat_result, bytes | None]:
    """Hash one immutable payload without following a path or inode swap."""
    try:
        before = os.lstat(path)
        _validate_payload_stat(before)
        if before.st_size > maximum_size:
            raise _invalid()
        flags = os.O_RDONLY | os.O_CLOEXEC | getattr(os, "O_NOFOLLOW", 0)
        descriptor = os.open(path, flags)
    except (OSError, TypeError, ValueError):
        raise _invalid() from None
    try:
        opened = os.fstat(descriptor)
        _validate_payload_stat(opened)
        if _stat_identity(opened) != _stat_identity(before):
            raise _invalid()
        digest = hashlib.sha256()
        size = 0
        captured = bytearray() if capture_bytes else None
        while chunk := os.read(descriptor, 1024 * 1024):
            size += len(chunk)
            if size > maximum_size:
                raise _invalid()
            digest.update(chunk)
            if captured is not None:
                captured.extend(chunk)
        after = os.fstat(descriptor)
        current = os.lstat(path)
        if _stat_identity(after) != _stat_identity(opened) or _stat_identity(
            current
        ) != _stat_identity(opened):
            raise _invalid()
    except (OSError, TypeError, ValueError):
        raise _invalid() from None
    finally:
        try:
            os.close(descriptor)
        except OSError:
            pass
    return digest.hexdigest(), size, opened, bytes(captured) if captured is not None else None


def _create_payload_root(path: Path) -> None:
    try:
        path.mkdir(mode=0o700)
    except FileExistsError:
        pass
    except (OSError, TypeError):
        raise _invalid() from None
    _validate_payload_root(path)


def install_personal_dev_minio_payload(
    *,
    temporary_path: Path,
    payload_root: Path,
    object: PersonalDevMinioObject,
) -> Path:
    """Atomically retain one verified temporary payload under its SHA-256 name."""
    if (
        not isinstance(object, PersonalDevMinioObject)
        or not isinstance(temporary_path, Path)
        or not isinstance(payload_root, Path)
        or temporary_path.parent != payload_root
    ):
        raise _invalid()
    _create_payload_root(payload_root)
    temporary_digest, temporary_size, _, _ = _read_owner_only_payload(temporary_path)
    if temporary_digest != object.payload_sha256 or temporary_size != object.size_bytes:
        raise _invalid()
    final_path = payload_root / object.payload_sha256
    if temporary_path == final_path:
        return final_path
    try:
        os.link(temporary_path, final_path, follow_symlinks=False)
    except FileExistsError:
        existing_digest, existing_size, _, _ = _read_owner_only_payload(final_path)
        if existing_digest != object.payload_sha256 or existing_size != object.size_bytes:
            raise _invalid() from None
    except (OSError, TypeError):
        raise _invalid() from None
    try:
        os.unlink(temporary_path)
        final_digest, final_size, final_stat, _ = _read_owner_only_payload(final_path)
    except (OSError, TypeError, ValueError):
        raise _invalid() from None
    if (
        final_digest != object.payload_sha256
        or final_size != object.size_bytes
        or final_stat.st_nlink != 1
    ):
        raise _invalid()
    return final_path


def validate_personal_dev_minio_payload_root(
    manifest: PersonalDevMinioManifest,
    payload_root: Path,
) -> bytes:
    """Require an exact, immutable content-addressed payload directory."""
    if not isinstance(manifest, PersonalDevMinioManifest) or not isinstance(payload_root, Path):
        raise _invalid()
    expected: dict[str, int] = {}
    for object in manifest.objects:
        previous_size = expected.setdefault(object.payload_sha256, object.size_bytes)
        if previous_size != object.size_bytes:
            raise _invalid()
    before_root = _validate_payload_root(payload_root)
    try:
        names_before = {entry.name for entry in os.scandir(payload_root)}
    except (OSError, TypeError):
        raise _invalid() from None
    if names_before != set(expected):
        raise _invalid()
    for digest, size_bytes in expected.items():
        payload_path = payload_root / digest
        actual_digest, actual_size, _, _ = _read_owner_only_payload(payload_path)
        if actual_digest != digest or actual_size != size_bytes:
            raise _invalid()
    after_root = _validate_payload_root(payload_root)
    try:
        names_after = {entry.name for entry in os.scandir(payload_root)}
    except (OSError, TypeError):
        raise _invalid() from None
    if _stat_identity(before_root) != _stat_identity(after_root) or names_after != set(expected):
        raise _invalid()
    return manifest.payload_inventory_bytes


def write_personal_dev_minio_manifest(path: Path, manifest: PersonalDevMinioManifest) -> None:
    """Create an owner-only manifest exactly once, without replacing any existing path."""
    if not isinstance(path, Path) or not isinstance(manifest, PersonalDevMinioManifest):
        raise _invalid()
    _validate_payload_root(path.parent)
    try:
        descriptor = os.open(
            path,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_CLOEXEC,
            0o600,
        )
    except (OSError, TypeError):
        raise _invalid() from None
    try:
        created = os.fstat(descriptor)
        _validate_payload_stat(created)
        remaining = manifest.canonical_bytes
        while remaining:
            written = os.write(descriptor, remaining)
            if written <= 0:
                raise _invalid()
            remaining = remaining[written:]
        finished = os.fstat(descriptor)
        _validate_payload_stat(finished)
        if _file_handle_identity(created) != _file_handle_identity(finished):
            raise _invalid()
    except (OSError, TypeError, ValueError):
        raise _invalid() from None
    finally:
        try:
            os.close(descriptor)
        except OSError:
            pass
