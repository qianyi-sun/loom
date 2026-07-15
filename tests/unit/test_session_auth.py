from __future__ import annotations

import hashlib
from types import SimpleNamespace
from uuid import uuid4

import pytest
from fastapi import HTTPException

from loom.auth import AuthContext
from loom_service.session_auth import (
    hash_secret,
    is_staging_admin_browser_session,
    session_cookie_options,
    verify_csrf,
)


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


def test_session_cookie_options_support_controlled_short_secure_cookie(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("LOOM_ENV", "staging")
    settings = SimpleNamespace(
        auth_session_cookie_name="loom_session",
        auth_session_ttl_sec=604800,
    )

    options = session_cookie_options(  # type: ignore[arg-type]
        settings,
        max_age=900,
        force_secure=True,
    )

    assert options == {
        "key": "loom_session",
        "httponly": True,
        "secure": True,
        "samesite": "lax",
        "max_age": 900,
        "path": "/",
    }


def test_staging_admin_session_prefix_is_exact() -> None:
    assert is_staging_admin_browser_session(
        "loom_session_staging_admin_example-secret",
    )
    assert not is_staging_admin_browser_session("loom_session_example-secret")
    assert not is_staging_admin_browser_session(None)


def test_session_cookie_options_reject_non_positive_override() -> None:
    settings = SimpleNamespace(
        auth_session_cookie_name="loom_session",
        auth_session_ttl_sec=604800,
    )

    with pytest.raises(ValueError, match="max_age must be positive"):
        session_cookie_options(settings, max_age=0)  # type: ignore[arg-type]


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
