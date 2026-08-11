"""Fail-closed loading for controller-local Ed25519 ownership keys."""

from __future__ import annotations

import errno
import hmac
import os
import stat
from pathlib import Path

from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from loom_capacity_manager.ownership import public_key_fingerprint


class ExecutorKeyError(RuntimeError):
    """A configured executor ownership key cannot be trusted."""


def load_ownership_private_key(
    path: Path,
    *,
    expected_public_key_sha256: str,
) -> Ed25519PrivateKey:
    """Load an exact raw key only from an owner-only, non-symlink file."""

    key_path = Path(path)
    try:
        descriptor = os.open(key_path, os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW)
    except OSError as exc:
        if exc.errno in {errno.ELOOP, errno.EMLINK}:
            raise ExecutorKeyError(f"ownership key symlink is forbidden: {key_path}") from exc
        raise ExecutorKeyError(f"cannot open ownership key: {key_path}") from exc
    try:
        metadata = os.fstat(descriptor)
        if not stat.S_ISREG(metadata.st_mode):
            raise ExecutorKeyError("ownership key is not a regular file")
        if metadata.st_uid != os.geteuid():
            raise ExecutorKeyError("ownership key has another owner")
        if metadata.st_mode & 0o077:
            raise ExecutorKeyError("ownership key permissions are too broad")
        if metadata.st_size != 32:
            raise ExecutorKeyError("ownership key must contain exactly 32 raw bytes")
        raw = b""
        while len(raw) < 32:
            chunk = os.read(descriptor, 32 - len(raw))
            if not chunk:
                break
            raw += chunk
    finally:
        os.close(descriptor)
    if len(raw) != 32:
        raise ExecutorKeyError("ownership key must contain exactly 32 raw bytes")
    private_key = Ed25519PrivateKey.from_private_bytes(raw)
    actual = public_key_fingerprint(private_key.public_key())
    if not hmac.compare_digest(actual, expected_public_key_sha256):
        raise ExecutorKeyError("ownership key fingerprint does not match registration")
    return private_key


__all__ = ["ExecutorKeyError", "load_ownership_private_key"]
