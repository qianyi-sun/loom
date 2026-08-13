"""Fail-closed loading for controller-local Ed25519 ownership keys."""

from __future__ import annotations

import errno
import hmac
import os
import re
import stat
from dataclasses import dataclass
from pathlib import Path

from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from loom_capacity_manager.ownership import public_key_fingerprint


class ExecutorKeyError(RuntimeError):
    """A configured executor ownership key cannot be trusted."""


_KEY_ID_RE = re.compile(r"[a-z0-9][a-z0-9-]{0,127}")
_DIGEST_RE = re.compile(r"[0-9a-f]{64}")


@dataclass(frozen=True, slots=True)
class ExecutorOwnershipKey:
    """One exact controller-local signer and its registered identity."""

    signing_key_id: str
    private_key: Ed25519PrivateKey
    public_key_sha256: str

    def __post_init__(self) -> None:
        if (
            not isinstance(self.signing_key_id, str)
            or _KEY_ID_RE.fullmatch(self.signing_key_id) is None
        ):
            raise ExecutorKeyError("ownership signing key id is invalid")
        if not isinstance(self.private_key, Ed25519PrivateKey):
            raise ExecutorKeyError("ownership private key is invalid")
        if (
            not isinstance(self.public_key_sha256, str)
            or _DIGEST_RE.fullmatch(self.public_key_sha256) is None
        ):
            raise ExecutorKeyError("ownership public key fingerprint is invalid")
        actual = public_key_fingerprint(self.private_key.public_key())
        if not hmac.compare_digest(actual, self.public_key_sha256):
            raise ExecutorKeyError("ownership key fingerprint does not match registration")


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


def load_executor_ownership_key(
    path: Path,
    *,
    signing_key_id: str,
    expected_public_key_sha256: str,
) -> ExecutorOwnershipKey:
    """Load one complete registered signer from the owner-only key file."""

    if not isinstance(signing_key_id, str) or _KEY_ID_RE.fullmatch(signing_key_id) is None:
        raise ExecutorKeyError("ownership signing key id is invalid")
    private_key = load_ownership_private_key(
        path,
        expected_public_key_sha256=expected_public_key_sha256,
    )
    return ExecutorOwnershipKey(
        signing_key_id=signing_key_id,
        private_key=private_key,
        public_key_sha256=expected_public_key_sha256,
    )


__all__ = [
    "ExecutorKeyError",
    "ExecutorOwnershipKey",
    "load_executor_ownership_key",
    "load_ownership_private_key",
]
