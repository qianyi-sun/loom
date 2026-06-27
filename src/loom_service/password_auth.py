"""Username/password primitives for browser and CLI account auth."""

from __future__ import annotations

import re

from argon2 import PasswordHasher
from argon2.exceptions import InvalidHashError, VerificationError, VerifyMismatchError
from fastapi import HTTPException

_USERNAME_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{1,63}$")
_PASSWORD_MIN_LEN = 12

_HASHER = PasswordHasher()


def normalize_username(username: str) -> str:
    cleaned = username.strip()
    if not cleaned:
        raise HTTPException(status_code=422, detail="username is required")
    if not _USERNAME_RE.fullmatch(cleaned):
        raise HTTPException(
            status_code=422,
            detail=(
                "username must be 2-64 characters and contain only letters, "
                "numbers, underscore, dot, or dash"
            ),
        )
    return cleaned.casefold()


def validate_password_pair(password: str, confirm_password: str) -> None:
    if password != confirm_password:
        raise HTTPException(status_code=400, detail="passwords do not match")
    if len(password) < _PASSWORD_MIN_LEN:
        raise HTTPException(
            status_code=422,
            detail=f"password must be at least {_PASSWORD_MIN_LEN} characters",
        )


def hash_password(password: str) -> str:
    if len(password) < _PASSWORD_MIN_LEN:
        raise HTTPException(
            status_code=422,
            detail=f"password must be at least {_PASSWORD_MIN_LEN} characters",
        )
    return _HASHER.hash(password)


def verify_password(password: str, password_hash: str) -> bool:
    try:
        return bool(_HASHER.verify(password_hash, password))
    except (InvalidHashError, VerificationError, VerifyMismatchError):
        return False


def needs_rehash(password_hash: str) -> bool:
    try:
        return bool(_HASHER.check_needs_rehash(password_hash))
    except (InvalidHashError, VerificationError):
        return True
