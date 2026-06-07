"""Step-scoped JWT mint + verify (Plan 9 Task 3)."""

from __future__ import annotations

from uuid import uuid4

import jwt
import pytest

from loom.auth import (
    AuthContext,
    mint_step_jwt,
    verify_step_jwt,
)

_KEY = "x" * 32


def test_round_trip() -> None:
    team = uuid4()
    trial = uuid4()
    token = mint_step_jwt(
        team_id=team, trial_id=trial, step_id="main",
        ttl_sec=60, signing_key=_KEY,
    )
    assert token.startswith("loom_step_")
    ctx = verify_step_jwt(token, signing_key=_KEY)
    assert isinstance(ctx, AuthContext)
    assert ctx.team_id == team
    assert ctx.trial_id == trial
    assert ctx.step_id == "main"
    assert "llm:call" in ctx.scopes
    assert ctx.type == "step_session"


def test_expired_rejected() -> None:
    token = mint_step_jwt(
        team_id=uuid4(), trial_id=uuid4(), step_id="main",
        ttl_sec=-1, signing_key=_KEY,
    )
    with pytest.raises(jwt.ExpiredSignatureError):
        verify_step_jwt(token, signing_key=_KEY)


def test_bad_signature_rejected() -> None:
    token = mint_step_jwt(
        team_id=uuid4(), trial_id=uuid4(), step_id="main",
        ttl_sec=60, signing_key=_KEY,
    )
    with pytest.raises(jwt.InvalidSignatureError):
        verify_step_jwt(token, signing_key="y" * 32)


def test_missing_prefix_rejected() -> None:
    with pytest.raises(jwt.InvalidTokenError):
        verify_step_jwt("not-a-step-token", signing_key=_KEY)


def test_authcontext_backwards_compat_fields_present() -> None:
    """The existing token_hash/type/scopes/team_id/expires_at fields MUST
    remain on AuthContext so Plans 17-20 (service layer) keep working."""
    token = mint_step_jwt(
        team_id=uuid4(), trial_id=uuid4(), step_id="main",
        ttl_sec=60, signing_key=_KEY,
    )
    ctx = verify_step_jwt(token, signing_key=_KEY)
    # New optional fields
    assert ctx.trial_id is not None
    assert ctx.step_id is not None
    # Existing fields still present
    assert ctx.token_hash == b""   # synthetic for JWT branch — no DB row
    assert isinstance(ctx.scopes, list)
    assert ctx.expires_at is not None
