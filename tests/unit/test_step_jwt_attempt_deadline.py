from __future__ import annotations

from datetime import UTC, datetime, timedelta
from uuid import uuid4

import jwt
import pytest

from loom.auth import mint_step_jwt, verify_step_jwt

_SIGNING_KEY = "deadline-test-signing-key-at-least-32-bytes"


def test_attempt_deadline_is_signed_and_exposed_with_exact_expiry() -> None:
    issued_at = datetime.now(UTC).replace(microsecond=0)
    deadline = issued_at + timedelta(seconds=600)

    token = mint_step_jwt(
        team_id=uuid4(),
        trial_id=uuid4(),
        step_id="main",
        ttl_sec=900,
        signing_key=_SIGNING_KEY,
        issued_at=issued_at,
        attempt_deadline_wall_clock=deadline,
    )
    claims = jwt.decode(
        token.removeprefix("loom_step_"),
        _SIGNING_KEY,
        algorithms=["HS256"],
        options={"verify_exp": False},
    )
    context = verify_step_jwt(token, signing_key=_SIGNING_KEY)

    assert claims["iat"] == int(issued_at.timestamp())
    assert claims["exp"] == int((issued_at + timedelta(seconds=900)).timestamp())
    assert claims["attempt_deadline_wall_clock"] == deadline.isoformat()
    assert context.expires_at == issued_at + timedelta(seconds=900)
    assert context.attempt_deadline_wall_clock == deadline


def test_mint_rejects_expiry_that_does_not_cover_deadline_grace() -> None:
    issued_at = datetime.now(UTC).replace(microsecond=0)
    with pytest.raises(ValueError, match="deadline plus 300 seconds"):
        mint_step_jwt(
            team_id=uuid4(),
            trial_id=uuid4(),
            step_id="main",
            ttl_sec=899,
            signing_key=_SIGNING_KEY,
            issued_at=issued_at,
            attempt_deadline_wall_clock=issued_at + timedelta(seconds=600),
        )


def test_verify_rejects_tampered_deadline_lifetime() -> None:
    issued_at = datetime.now(UTC).replace(microsecond=0)
    deadline = issued_at + timedelta(seconds=60)
    token = mint_step_jwt(
        team_id=uuid4(),
        trial_id=uuid4(),
        step_id="main",
        ttl_sec=360,
        signing_key=_SIGNING_KEY,
        issued_at=issued_at,
        attempt_deadline_wall_clock=deadline,
    )
    claims = jwt.decode(
        token.removeprefix("loom_step_"),
        _SIGNING_KEY,
        algorithms=["HS256"],
        options={"verify_exp": False},
    )
    claims["exp"] = int((deadline + timedelta(seconds=299)).timestamp())
    tampered = "loom_step_" + jwt.encode(claims, _SIGNING_KEY, algorithm="HS256")

    with pytest.raises(jwt.InvalidTokenError, match="invalid step JWT authority"):
        verify_step_jwt(tampered, signing_key=_SIGNING_KEY)


def test_verify_rejects_numeric_monotonic_deadline_claim() -> None:
    issued_at = datetime.now(UTC).replace(microsecond=0)
    claims = {
        "iss": "loom-control-plane",
        "sub": "step-session",
        "team_id": str(uuid4()),
        "trial_id": str(uuid4()),
        "subject_kind": "trial",
        "step_id": "main",
        "iat": int(issued_at.timestamp()),
        "exp": int((issued_at + timedelta(seconds=900)).timestamp()),
        "scopes": ["llm:call"],
        "attempt_deadline_wall_clock": 12345.0,
    }
    token = "loom_step_" + jwt.encode(claims, _SIGNING_KEY, algorithm="HS256")

    with pytest.raises(jwt.InvalidTokenError, match="invalid step JWT authority"):
        verify_step_jwt(token, signing_key=_SIGNING_KEY)
