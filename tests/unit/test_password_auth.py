from __future__ import annotations

import pytest
from fastapi import HTTPException

from loom_service.password_auth import (
    hash_password,
    needs_rehash,
    normalize_username,
    validate_password_pair,
    verify_password,
)


def test_normalize_username_is_case_insensitive_and_space_insensitive() -> None:
    assert normalize_username("  Qianyi  ") == "qianyi"
    assert normalize_username("Hongjian") == "hongjian"


def test_normalize_username_rejects_empty_or_invalid_values() -> None:
    with pytest.raises(HTTPException) as empty:
        normalize_username("   ")
    assert empty.value.status_code == 422

    with pytest.raises(HTTPException) as invalid:
        normalize_username("not an email@example.com")
    assert invalid.value.status_code == 422


def test_password_hash_uses_argon2id_and_verifies() -> None:
    encoded = hash_password("correct horse battery staple")

    assert encoded.startswith("$argon2id$")
    assert verify_password("correct horse battery staple", encoded)
    assert not verify_password("wrong", encoded)
    assert not needs_rehash(encoded)


def test_validate_password_pair_rejects_mismatch_and_short_password() -> None:
    with pytest.raises(HTTPException) as mismatch:
        validate_password_pair("long-passphrase-1", "long-passphrase-2")
    assert mismatch.value.status_code == 400

    with pytest.raises(HTTPException) as short:
        validate_password_pair("short", "short")
    assert short.value.status_code == 422
