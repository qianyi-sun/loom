"""Immutable scanner-cache source and installed-file bindings."""

from __future__ import annotations

import hashlib
import json
import os
import stat
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType
from typing import Any

_MAX_LOCK_BYTES = 1024 * 1024
_LOCK_KEYS = frozenset(
    {"binary_sha256", "database", "java_database", "schema_version", "trivy_version"}
)
_BINARY_KEYS = frozenset({"linux/amd64", "linux/arm64"})
_SOURCE_KEYS = frozenset({"image", "layer_sha256"})
_TRIVY_VERSION = "v0.74.0"
_DATABASE_REPOSITORY = "ghcr.io/aquasecurity/trivy-db"
_JAVA_DATABASE_REPOSITORY = "ghcr.io/aquasecurity/trivy-java-db"


class PersonalDevScannerCacheError(ValueError):
    """The scanner-cache binding is unsafe, unstable, or inconsistent."""


@dataclass(frozen=True, slots=True)
class PersonalDevScannerCacheSource:
    """One immutable OCI database artifact and its single data layer."""

    image: str
    layer_sha256: str


@dataclass(frozen=True, slots=True)
class PersonalDevScannerCacheFiles:
    """Exact protected files installed into one cache generation."""

    database_sha256: str
    database_metadata_sha256: str
    java_database_sha256: str
    java_database_metadata_sha256: str

    def canonical_value(self) -> dict[str, str]:
        return {
            "database_metadata_sha256": self.database_metadata_sha256,
            "database_sha256": self.database_sha256,
            "java_database_metadata_sha256": self.java_database_metadata_sha256,
            "java_database_sha256": self.java_database_sha256,
        }

    def canonical_bytes(self) -> bytes:
        return json.dumps(
            self.canonical_value(),
            sort_keys=True,
            separators=(",", ":"),
        ).encode("ascii")


@dataclass(frozen=True, slots=True)
class PersonalDevScannerCacheBinding:
    """Release binding for one installed cache generation."""

    cache_identity_sha256: str
    scanner_binary_sha256: str
    files: PersonalDevScannerCacheFiles


@dataclass(frozen=True, slots=True)
class PersonalDevScannerCacheLock:
    """Reviewed upstream sources and scanner binaries for one cache release."""

    schema_version: int
    trivy_version: str
    binary_sha256: MappingProxyType[str, str]
    database: PersonalDevScannerCacheSource
    java_database: PersonalDevScannerCacheSource
    sha256: str


def _invalid_lock() -> PersonalDevScannerCacheError:
    return PersonalDevScannerCacheError("scanner cache lock is invalid")


def _read_lock(path: Path) -> bytes:
    flags = (
        os.O_RDONLY
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_NOFOLLOW", 0)
        | getattr(os, "O_NONBLOCK", 0)
    )
    try:
        descriptor = os.open(path, flags)
    except OSError:
        raise _invalid_lock() from None
    try:
        before = os.fstat(descriptor)
        if (
            not stat.S_ISREG(before.st_mode)
            or before.st_nlink != 1
            or not 0 < before.st_size <= _MAX_LOCK_BYTES
        ):
            raise _invalid_lock()
        chunks: list[bytes] = []
        remaining = _MAX_LOCK_BYTES + 1
        while remaining:
            chunk = os.read(descriptor, min(64 * 1024, remaining))
            if not chunk:
                break
            chunks.append(chunk)
            remaining -= len(chunk)
        payload = b"".join(chunks)
        after = os.fstat(descriptor)
        stable_fields = (
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
        if (
            not 0 < len(payload) <= _MAX_LOCK_BYTES
            or len(payload) != before.st_size
            or any(getattr(before, field) != getattr(after, field) for field in stable_fields)
        ):
            raise _invalid_lock()
        return payload
    except OSError:
        raise _invalid_lock() from None
    finally:
        os.close(descriptor)


def _unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    value: dict[str, Any] = {}
    for key, item in pairs:
        if key in value:
            raise _invalid_lock()
        value[key] = item
    return value


def _canonical_json(value: object) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("ascii")


def _is_sha256(value: object) -> bool:
    return (
        isinstance(value, str)
        and len(value) == 64
        and value != "0" * 64
        and all(character in "0123456789abcdef" for character in value)
    )


def _source(value: object, *, repository: str) -> PersonalDevScannerCacheSource:
    if not isinstance(value, dict) or value.keys() != _SOURCE_KEYS:
        raise _invalid_lock()
    image = value.get("image")
    layer_sha256 = value.get("layer_sha256")
    prefix = repository + "@sha256:"
    if (
        not isinstance(image, str)
        or not image.startswith(prefix)
        or not _is_sha256(image.removeprefix(prefix))
        or not isinstance(layer_sha256, str)
        or not _is_sha256(layer_sha256)
    ):
        raise _invalid_lock()
    return PersonalDevScannerCacheSource(image=image, layer_sha256=layer_sha256)


def load_personal_dev_scanner_cache_lock(path: Path) -> PersonalDevScannerCacheLock:
    """Load the checked-in scanner-cache lock."""

    try:
        payload = _read_lock(path)
        value = json.loads(payload, object_pairs_hook=_unique_object)
        if (
            not isinstance(value, dict)
            or value.keys() != _LOCK_KEYS
            or _canonical_json(value) != payload
            or type(value.get("schema_version")) is not int
            or value["schema_version"] != 1
            or value.get("trivy_version") != _TRIVY_VERSION
        ):
            raise _invalid_lock()
        binary_sha256 = value.get("binary_sha256")
        if (
            not isinstance(binary_sha256, dict)
            or binary_sha256.keys() != _BINARY_KEYS
            or any(not _is_sha256(digest) for digest in binary_sha256.values())
        ):
            raise _invalid_lock()
        validated_binary_sha256 = {
            str(platform): str(digest) for platform, digest in binary_sha256.items()
        }
        database = _source(value.get("database"), repository=_DATABASE_REPOSITORY)
        java_database = _source(
            value.get("java_database"), repository=_JAVA_DATABASE_REPOSITORY
        )
    except PersonalDevScannerCacheError:
        raise
    except (KeyError, TypeError, UnicodeError, ValueError):
        raise _invalid_lock() from None
    return PersonalDevScannerCacheLock(
        schema_version=1,
        trivy_version=_TRIVY_VERSION,
        binary_sha256=MappingProxyType(validated_binary_sha256),
        database=database,
        java_database=java_database,
        sha256=hashlib.sha256(payload).hexdigest(),
    )


__all__ = [
    "PersonalDevScannerCacheBinding",
    "PersonalDevScannerCacheError",
    "PersonalDevScannerCacheFiles",
    "PersonalDevScannerCacheLock",
    "PersonalDevScannerCacheSource",
    "load_personal_dev_scanner_cache_lock",
]
