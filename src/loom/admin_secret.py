"""Singleton admin secret file support for service-mode auth."""

from __future__ import annotations

import hashlib
import hmac
import stat
import tomllib
from dataclasses import dataclass
from pathlib import Path


class AdminSecretConfigError(ValueError):
    """The configured singleton admin secret file is missing or unsafe."""


_ADMIN_PREFIX = "loom_admin_"
_MIN_SECRET_SUFFIX_LEN = 32


@dataclass(frozen=True)
class AdminSecretVerifier:
    """Constant-time verifier for the singleton admin bearer token."""

    token_hash: bytes

    @classmethod
    def from_token(cls, token: str) -> AdminSecretVerifier:
        _validate_admin_token(token)
        return cls(token_hash=hashlib.sha256(token.encode()).digest())

    def verify(self, candidate: str) -> bool:
        candidate_hash = hashlib.sha256(candidate.encode()).digest()
        return hmac.compare_digest(candidate_hash, self.token_hash)


def load_admin_secret_file(
    path: Path,
    *,
    require_safe_permissions: bool,
) -> AdminSecretVerifier:
    """Load and validate a singleton admin secret TOML file."""
    if not path.is_file():
        raise AdminSecretConfigError(f"admin secret file not found: {path}")
    if require_safe_permissions:
        mode = stat.S_IMODE(path.stat().st_mode)
        if mode & 0o077:
            raise AdminSecretConfigError(
                f"admin secret file permissions must not grant group/other "
                f"access: {path} has mode {mode:o}",
            )
    try:
        data = tomllib.loads(path.read_text(encoding="utf-8"))
    except tomllib.TOMLDecodeError as exc:
        raise AdminSecretConfigError(
            f"admin secret file is not valid TOML: {path}",
        ) from exc
    admin_section = data.get("admin")
    if not isinstance(admin_section, dict):
        raise AdminSecretConfigError("admin secret file missing [admin] section")
    token = admin_section.get("token")
    if not isinstance(token, str):
        raise AdminSecretConfigError("admin secret file missing admin.token")
    return AdminSecretVerifier.from_token(token)


def _validate_admin_token(token: str) -> None:
    if not token.startswith(_ADMIN_PREFIX):
        raise AdminSecretConfigError("admin token must start with loom_admin_")
    suffix = token[len(_ADMIN_PREFIX):]
    if len(suffix) < _MIN_SECRET_SUFFIX_LEN:
        raise AdminSecretConfigError(
            "admin token entropy is too low; generate with token_urlsafe(32)",
        )
