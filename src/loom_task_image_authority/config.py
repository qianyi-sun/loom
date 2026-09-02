"""Fail-closed configuration for the task-image authority service."""

from __future__ import annotations

import base64
import binascii
import os
import ssl
import stat
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from types import MappingProxyType
from typing import Annotated, Any, Literal

from pydantic import BaseModel, ConfigDict, Field, ValidationError, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

_MAX_KEYRING_BYTES = 1024 * 1024
_MAX_KEY_VERSION = (1 << 31) - 1


class TaskImageAuthorityConfigurationError(ValueError):
    """Raised when an authority configuration input is unsafe or invalid."""


class TaskImageAuthoritySettings(BaseSettings):
    """Immutable process settings loaded only from the authority namespace."""

    model_config = SettingsConfigDict(
        env_prefix="LOOM_TASK_IMAGE_AUTHORITY_",
        extra="forbid",
        frozen=True,
        strict=True,
    )

    principals_file: Path
    db_url_file: Path
    secret_store_keyring_file: Path
    tls_cert_file: Path
    tls_key_file: Path
    tls_client_ca_file: Path
    host: Literal["127.0.0.1"] = "127.0.0.1"
    port: int = Field(default=8445, ge=1, le=65535)
    request_rate_limit_per_second: int = Field(default=64, ge=1, le=10_000)
    request_concurrency_limit: int = Field(default=32, ge=1, le=1024)


def read_owner_only_bytes(path: Path, *, max_bytes: int = 16 * 1024) -> bytes:
    """Read an exact bounded payload from a stable owner-only regular file."""

    if type(max_bytes) is not int or max_bytes <= 0:
        raise ValueError("max_bytes must be a positive integer")
    try:
        metadata = path.lstat()
    except OSError as exc:
        raise TaskImageAuthorityConfigurationError("cannot read owner-only file") from exc
    if (
        not stat.S_ISREG(metadata.st_mode)
        or metadata.st_uid != os.getuid()
        or stat.S_IMODE(metadata.st_mode) != 0o600
    ):
        raise TaskImageAuthorityConfigurationError(
            "file must be a current-uid-owned 0600 regular nonsymlink"
        )

    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags)
        try:
            opened = os.fstat(descriptor)
            if (
                (opened.st_dev, opened.st_ino) != (metadata.st_dev, metadata.st_ino)
                or not stat.S_ISREG(opened.st_mode)
                or opened.st_uid != os.getuid()
                or stat.S_IMODE(opened.st_mode) != 0o600
            ):
                raise TaskImageAuthorityConfigurationError(
                    "owner-only file metadata changed while opening"
                )
            chunks: list[bytes] = []
            total = 0
            while total <= max_bytes:
                chunk = os.read(descriptor, min(64 * 1024, max_bytes + 1 - total))
                if not chunk:
                    break
                chunks.append(chunk)
                total += len(chunk)
            finished = os.fstat(descriptor)
            if (
                finished.st_dev,
                finished.st_ino,
                finished.st_mode,
                finished.st_uid,
                finished.st_size,
                finished.st_mtime_ns,
                finished.st_ctime_ns,
            ) != (
                opened.st_dev,
                opened.st_ino,
                opened.st_mode,
                opened.st_uid,
                opened.st_size,
                opened.st_mtime_ns,
                opened.st_ctime_ns,
            ):
                raise TaskImageAuthorityConfigurationError(
                    "owner-only file changed while reading"
                )
            payload = b"".join(chunks)
        finally:
            os.close(descriptor)
    except TaskImageAuthorityConfigurationError:
        raise
    except OSError as exc:
        raise TaskImageAuthorityConfigurationError("cannot read owner-only file") from exc

    if len(payload) > max_bytes:
        raise TaskImageAuthorityConfigurationError("file exceeds maximum byte size")
    return payload


def read_owner_only_secret(path: Path, *, max_bytes: int = 16 * 1024) -> str:
    """Read one nonempty UTF-8 line without silently trimming its value."""

    payload = read_owner_only_bytes(path, max_bytes=max_bytes)
    try:
        value = payload.decode("utf-8")
    except UnicodeDecodeError:
        raise TaskImageAuthorityConfigurationError("secret file is not valid UTF-8") from None
    if value.endswith("\n"):
        value = value[:-1]
    if not value or any(character in value for character in ("\r", "\n", "\x00")):
        raise TaskImageAuthorityConfigurationError(
            "secret file must contain exactly one nonempty line"
        )
    return value


class _StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)


class _KeyEntry(_StrictModel):
    version: Annotated[int, Field(ge=1, le=_MAX_KEY_VERSION)]
    key_base64: Annotated[str, Field(min_length=1, max_length=128)]


class _KeyringDocument(_StrictModel):
    schema_version: Literal[1]
    primary: _KeyEntry
    fallbacks: Annotated[tuple[_KeyEntry, ...], Field(max_length=32)] = ()

    @model_validator(mode="before")
    @classmethod
    def _restore_json_tuple(cls, value: Any) -> Any:
        if not isinstance(value, dict):
            return value
        normalized = dict(value)
        if isinstance(normalized.get("fallbacks"), list):
            normalized["fallbacks"] = tuple(normalized["fallbacks"])
        return normalized

    @model_validator(mode="after")
    def _versions_are_canonical(self) -> _KeyringDocument:
        versions = tuple(entry.version for entry in self.fallbacks)
        if (
            len(versions) != len(set(versions))
            or any(version >= self.primary.version for version in versions)
            or versions != tuple(sorted(versions, reverse=True))
        ):
            raise ValueError("keyring versions are not unique lower ordered fallbacks")
        return self


@dataclass(frozen=True, slots=True)
class TaskImageSecretStoreKeyring:
    """Decoded immutable key material with an intentionally redacted repr."""

    primary_key: bytes = field(repr=False)
    primary_version: int
    fallback_keys: Mapping[int, bytes] = field(repr=False)

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "fallback_keys",
            MappingProxyType(dict(self.fallback_keys)),
        )

    def __repr__(self) -> str:
        versions = tuple(self.fallback_keys)
        return (
            "TaskImageSecretStoreKeyring("
            f"primary_version={self.primary_version}, fallback_versions={versions!r})"
        )


def _decode_key(encoded: str) -> bytes:
    try:
        decoded = base64.b64decode(encoded, validate=True)
    except (ValueError, binascii.Error):
        raise TaskImageAuthorityConfigurationError("invalid keyring key encoding") from None
    if len(decoded) != 32:
        raise TaskImageAuthorityConfigurationError("invalid keyring key length")
    return decoded


def load_secret_store_keyring(path: Path) -> TaskImageSecretStoreKeyring:
    """Load and decode an owner-only versioned AES-256 keyring document."""

    raw = read_owner_only_bytes(path, max_bytes=_MAX_KEYRING_BYTES)
    try:
        document = _KeyringDocument.model_validate_json(raw)
    except ValidationError:
        raise TaskImageAuthorityConfigurationError("invalid keyring document") from None

    primary_key = _decode_key(document.primary.key_base64)
    fallback_keys = {
        entry.version: _decode_key(entry.key_base64) for entry in document.fallbacks
    }
    if len({primary_key, *fallback_keys.values()}) != 1 + len(fallback_keys):
        raise TaskImageAuthorityConfigurationError("keyring key material must be unique")
    return TaskImageSecretStoreKeyring(
        primary_key=primary_key,
        primary_version=document.primary.version,
        fallback_keys=MappingProxyType(fallback_keys),
    )


def build_uvicorn_kwargs(settings: TaskImageAuthoritySettings) -> dict[str, object]:
    """Return a loopback server configuration with mandatory client TLS."""

    return {
        "host": settings.host,
        "port": settings.port,
        "ssl_certfile": str(settings.tls_cert_file),
        "ssl_keyfile": str(settings.tls_key_file),
        "ssl_ca_certs": str(settings.tls_client_ca_file),
        "ssl_cert_reqs": ssl.CERT_REQUIRED,
        "server_header": False,
    }


__all__ = [
    "TaskImageAuthorityConfigurationError",
    "TaskImageAuthoritySettings",
    "TaskImageSecretStoreKeyring",
    "build_uvicorn_kwargs",
    "load_secret_store_keyring",
    "read_owner_only_bytes",
    "read_owner_only_secret",
]
