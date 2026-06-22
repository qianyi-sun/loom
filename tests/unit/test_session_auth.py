from __future__ import annotations

import hashlib
from uuid import uuid4

import pytest
from fastapi import HTTPException

from loom.auth import AuthContext
from loom_service.session_auth import hash_secret, verify_csrf


def _session_ctx(csrf_raw: str = "csrf-token") -> AuthContext:
    return AuthContext(
        token_hash=b"",
        type="user",
        scopes=["read:own"],
        team_id=uuid4(),
        expires_at=None,
        role="viewer",
        session_hash=hash_secret("session-token"),
        csrf_hash=hash_secret(csrf_raw),
        auth_kind="session",
    )


def test_hash_secret_matches_sha256_bytes() -> None:
    assert hash_secret("abc") == hashlib.sha256(b"abc").digest()


def test_verify_csrf_accepts_matching_session_header() -> None:
    verify_csrf(_session_ctx(), "csrf-token")


def test_verify_csrf_rejects_missing_session_header() -> None:
    with pytest.raises(HTTPException) as ei:
        verify_csrf(_session_ctx(), None)
    assert ei.value.status_code == 403
    assert "CSRF" in ei.value.detail


def test_verify_csrf_rejects_mismatched_session_header() -> None:
    with pytest.raises(HTTPException) as ei:
        verify_csrf(_session_ctx(), "wrong-token")
    assert ei.value.status_code == 403


def test_verify_csrf_skips_bearer_contexts() -> None:
    ctx = AuthContext(
        token_hash=hash_secret("team-token"),
        type="team",
        scopes=["submit"],
        team_id=uuid4(),
        expires_at=None,
        auth_kind="bearer",
    )
    verify_csrf(ctx, None)
