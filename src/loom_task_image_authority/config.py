"""Bounded secret-file and origin validation for signed-result readers."""

from __future__ import annotations

import ipaddress
import os
import re
import stat
from pathlib import Path
from urllib.parse import urlsplit

_DNS_HOST_PATTERN = re.compile(
    r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?"
    r"(?:\.[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?)*"
)
_REGISTRY_IDENTITY_PATTERN = r"^[a-z0-9][a-z0-9_.:-]{0,127}$"


class TaskImageAuthorityConfigurationError(ValueError):
    """Raised when an authority configuration input is unsafe or invalid."""


def _validate_https_origin(value: str, *, label: str) -> str:
    if (
        type(value) is not str
        or not value.isascii()
        or any(character.isspace() or ord(character) < 0x20 for character in value)
        or "\\" in value
    ):
        raise ValueError(f"{label} must be a canonical origin-only HTTPS URL")
    try:
        parsed = urlsplit(value)
        port = parsed.port
    except ValueError:
        raise ValueError(f"{label} must be a canonical origin-only HTTPS URL") from None
    try:
        address = ipaddress.ip_address(parsed.hostname or "")
    except ValueError:
        if (
            parsed.hostname is None
            or len(parsed.hostname) > 253
            or _DNS_HOST_PATTERN.fullmatch(parsed.hostname) is None
        ):
            raise ValueError(
                f"{label} must be a canonical origin-only HTTPS URL"
            ) from None
        canonical_host = parsed.hostname
    else:
        canonical_host = (
            f"[{address.compressed}]" if address.version == 6 else address.compressed
        )
    canonical_netloc = (
        f"{canonical_host}:{port}"
        if canonical_host is not None and port is not None
        else canonical_host
    )
    if (
        parsed.scheme != "https"
        or not parsed.netloc
        or parsed.username is not None
        or parsed.password is not None
        or parsed.hostname is None
        or parsed.path
        or parsed.query
        or parsed.fragment
        or parsed.netloc != canonical_netloc
        or port == 0
        or parsed.geturl() != value
    ):
        raise ValueError(f"{label} must be a canonical origin-only HTTPS URL")
    return value


def _validate_registry_identity(value: str, *, label: str) -> str:
    if type(value) is not str or re.fullmatch(_REGISTRY_IDENTITY_PATTERN, value) is None:
        raise ValueError(f"{label} is invalid")
    return value


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
                raise TaskImageAuthorityConfigurationError("owner-only file changed while reading")
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
