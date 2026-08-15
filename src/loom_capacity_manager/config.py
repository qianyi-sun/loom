"""Production-only configuration for the global capacity manager service."""

from __future__ import annotations

import os
import re
import ssl
import stat
from pathlib import Path
from uuid import UUID

from pydantic import Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class CapacityManagerSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="LOOM_CAPACITY_",
        extra="forbid",
        frozen=True,
    )

    principals_file: Path
    db_url_file: Path
    expected_authority_incarnation: UUID
    tls_cert_file: Path
    tls_key_file: Path
    tls_client_ca_file: Path
    ownership_public_keys_file: Path | None = None
    execution_policy_file: Path | None = None
    execution_policy_sha256: str | None = None
    host: str = "127.0.0.1"
    port: int = Field(default=8443, ge=1, le=65535)
    freshness_seconds: int = Field(default=120, ge=1, le=3600)
    allocation_timeout_seconds: float = Field(default=1.0, gt=0, le=60)
    reconciliation_max_attempts: int = Field(default=3, ge=1, le=10)

    @model_validator(mode="after")
    def _paired_execution_policy(self) -> CapacityManagerSettings:
        if (self.execution_policy_file is None) != (self.execution_policy_sha256 is None):
            raise ValueError("execution policy path and digest must be configured together")
        if self.execution_policy_sha256 is not None and (
            re.fullmatch(r"[0-9a-f]{64}", self.execution_policy_sha256) is None
            or self.execution_policy_sha256 == "0" * 64
        ):
            raise ValueError("execution policy digest must be a nonzero SHA-256")
        return self


def read_owner_only_secret(path: Path, *, max_bytes: int = 16 * 1024) -> str:
    """Read a small current-UID-owned 0600 regular file without following links."""

    if type(max_bytes) is not int or max_bytes <= 0:
        raise ValueError("max_bytes must be a positive integer")
    metadata = path.lstat()
    if (
        not stat.S_ISREG(metadata.st_mode)
        or path.is_symlink()
        or metadata.st_uid != os.getuid()
        or stat.S_IMODE(metadata.st_mode) != 0o600
    ):
        raise ValueError("secret file must be a current-UID-owned 0600 regular nonsymlink")
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(path, flags)
    try:
        opened = os.fstat(descriptor)
        if (
            (opened.st_dev, opened.st_ino) != (metadata.st_dev, metadata.st_ino)
            or not stat.S_ISREG(opened.st_mode)
            or opened.st_uid != os.getuid()
            or stat.S_IMODE(opened.st_mode) != 0o600
        ):
            raise ValueError("secret file metadata changed while opening")
        chunks: list[bytes] = []
        total = 0
        while total <= max_bytes:
            chunk = os.read(descriptor, min(4096, max_bytes + 1 - total))
            if not chunk:
                break
            chunks.append(chunk)
            total += len(chunk)
    finally:
        os.close(descriptor)
    payload = b"".join(chunks)
    if len(payload) > max_bytes:
        raise ValueError("secret file exceeds maximum byte size")
    try:
        value = payload.decode("utf-8").strip()
    except UnicodeDecodeError as exc:
        raise ValueError("secret file is not UTF-8") from exc
    if not value or any(character in value for character in ("\r", "\n", "\x00")):
        raise ValueError("secret file must contain one nonempty line")
    return value


def build_uvicorn_kwargs(settings: CapacityManagerSettings) -> dict[str, object]:
    """Return server options that always enforce trusted client certificates."""

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
    "CapacityManagerSettings",
    "build_uvicorn_kwargs",
    "read_owner_only_secret",
]
