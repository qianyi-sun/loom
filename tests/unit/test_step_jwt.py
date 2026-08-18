"""Step-scoped JWT mint + verify (Plan 9 Task 3)."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
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


# ──────────────────────────────────────────────────────────────────────
# Issue #72 — provider_connection_id in step-JWT scope
# ──────────────────────────────────────────────────────────────────────


def test_mint_without_provider_connection_id_omits_claim() -> None:
    """Default mint stays compatible with the legacy unbound JWT shape."""
    token = mint_step_jwt(
        team_id=uuid4(), trial_id=uuid4(), step_id="main",
        ttl_sec=60, signing_key=_KEY,
    )
    ctx = verify_step_jwt(token, signing_key=_KEY)
    assert ctx.provider_connection_id is None
    assert ctx.provider_connection_id_bound is False


def test_pre_provider_claim_jwt_keeps_legacy_unbound_shape() -> None:
    """Rolling-upgrade compatibility: an old JWT with no claim still verifies."""
    now = datetime.now(UTC)
    body = jwt.encode(
        {
            "iss": "loom-control-plane",
            "sub": "step-session",
            "team_id": str(uuid4()),
            "trial_id": str(uuid4()),
            "step_id": "main",
            "exp": int((now + timedelta(seconds=60)).timestamp()),
            "iat": int(now.timestamp()),
            "scopes": ["llm:call"],
        },
        _KEY,
        algorithm="HS256",
    )
    ctx = verify_step_jwt(f"loom_step_{body}", signing_key=_KEY)
    assert ctx.provider_connection_id is None
    assert ctx.provider_connection_id_bound is False


def test_mint_with_provider_connection_id_roundtrips() -> None:
    """Issue #72: connection_id passed at mint surfaces on verify."""
    conn = uuid4()
    token = mint_step_jwt(
        team_id=uuid4(), trial_id=uuid4(), step_id="main",
        ttl_sec=60, signing_key=_KEY,
        provider_connection_id=conn,
    )
    ctx = verify_step_jwt(token, signing_key=_KEY)
    assert ctx.provider_connection_id == conn


def test_mint_with_provider_connection_id_does_not_affect_other_claims() -> None:
    """Adding the new claim mustn't break the existing
    team/trial/step/scope fields."""
    team_id = uuid4()
    trial_id = uuid4()
    conn = uuid4()
    token = mint_step_jwt(
        team_id=team_id, trial_id=trial_id, step_id="step-1",
        ttl_sec=60, signing_key=_KEY,
        provider_connection_id=conn,
    )
    ctx = verify_step_jwt(token, signing_key=_KEY)
    assert ctx.team_id == team_id
    assert ctx.trial_id == trial_id
    assert ctx.step_id == "step-1"
    assert "llm:call" in ctx.scopes
    assert ctx.provider_connection_id == conn


def test_explicit_none_provider_connection_id_equivalent_to_omitted() -> None:
    """Passing None explicitly stays equivalent to omitting the kwarg."""
    a = mint_step_jwt(
        team_id=uuid4(), trial_id=uuid4(), step_id="s",
        ttl_sec=60, signing_key=_KEY,
        provider_connection_id=None,
    )
    ctx = verify_step_jwt(a, signing_key=_KEY)
    assert ctx.provider_connection_id is None
    assert ctx.provider_connection_id_bound is False


def test_explicit_null_provider_binding_roundtrips() -> None:
    token = mint_step_jwt(
        team_id=uuid4(),
        trial_id=uuid4(),
        step_id="family_evolver",
        ttl_sec=60,
        signing_key=_KEY,
        provider_connection_id=None,
        provider_connection_id_bound=True,
    )
    ctx = verify_step_jwt(token, signing_key=_KEY)
    assert ctx.provider_connection_id is None
    assert ctx.provider_connection_id_bound is True


def test_execution_attempt_subject_round_trips_without_trial_identity() -> None:
    attempt_id = uuid4()
    token = mint_step_jwt(
        team_id=uuid4(),
        execution_attempt_id=attempt_id,
        step_id="offline_judge",
        ttl_sec=3900,
        signing_key=_KEY,
    )
    ctx = verify_step_jwt(token, signing_key=_KEY)
    assert ctx.trial_id is None
    assert ctx.execution_attempt_id == attempt_id
    assert ctx.token_subject is not None
    assert ctx.token_subject.kind == "execution_attempt"


def test_execution_attempt_dispatch_authority_round_trips() -> None:
    attempt_id = uuid4()
    token_id = uuid4()
    spec_digest = "sha256:" + "a" * 64
    binding_digest = "sha256:" + "b" * 64
    authorization_digest = "sha256:" + "c" * 64
    token = mint_step_jwt(
        team_id=uuid4(),
        execution_attempt_id=attempt_id,
        step_id="generate_card_00",
        ttl_sec=600,
        signing_key=_KEY,
        provider_connection_id=None,
        provider_connection_id_bound=True,
        step_jwt_id=token_id,
        execution_attempt_lease_epoch=7,
        execution_spec_digest=spec_digest,
        control_binding_snapshot_digest=binding_digest,
        execution_authorization_digest=authorization_digest,
    )

    ctx = verify_step_jwt(token, signing_key=_KEY)

    assert ctx.execution_attempt_id == attempt_id
    assert ctx.step_jwt_id == token_id
    assert ctx.execution_attempt_lease_epoch == 7
    assert ctx.execution_spec_digest == spec_digest
    assert ctx.control_binding_snapshot_digest == binding_digest
    assert ctx.execution_authorization_digest == authorization_digest
    assert ctx.provider_connection_id_bound is True
