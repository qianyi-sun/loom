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


def test_subsecond_deadline_requires_one_numericdate_encoding_second() -> None:
    issued_at = datetime.now(UTC).replace(microsecond=999999)
    deadline = issued_at + timedelta(seconds=10)

    with pytest.raises(ValueError, match="deadline plus 300 seconds"):
        mint_step_jwt(
            team_id=uuid4(),
            trial_id=uuid4(),
            step_id="main",
            ttl_sec=310,
            signing_key=_SIGNING_KEY,
            issued_at=issued_at,
            attempt_deadline_wall_clock=deadline,
        )

    token = mint_step_jwt(
        team_id=uuid4(),
        trial_id=uuid4(),
        step_id="main",
        ttl_sec=311,
        signing_key=_SIGNING_KEY,
        issued_at=issued_at,
        attempt_deadline_wall_clock=deadline,
    )
    context = verify_step_jwt(token, signing_key=_SIGNING_KEY)
    assert context.expires_at >= deadline + timedelta(seconds=300)


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


def test_native_deadline_exception_requires_complete_short_lived_authority() -> None:
    now = datetime.now(UTC).replace(microsecond=0)
    native = dict(
        team_id=uuid4(), trial_id=uuid4(), step_id="agent", signing_key=_SIGNING_KEY,
        issued_at=now, attempt_deadline_wall_clock=now + timedelta(seconds=900),
        provider_connection_id_bound=True, step_jwt_id=uuid4(),
        service_execution_lease_id=uuid4(), service_execution_generation=1,
        service_execution_role="attempt", service_execution_runtime_contract_sha256="sha256:" + "a" * 64,
        service_execution_candidate_sha="b" * 40,
        service_execution_task_revision_sha256="sha256:" + "c" * 64,
        service_execution_command_identity_sha256="sha256:" + "d" * 64,
    )
    token = mint_step_jwt(**native, ttl_sec=480)
    assert verify_step_jwt(token, signing_key=_SIGNING_KEY).attempt_deadline_wall_clock == native["attempt_deadline_wall_clock"]
    with pytest.raises(ValueError, match="600 seconds"):
        mint_step_jwt(**native, ttl_sec=601)
    claims = jwt.decode(token.removeprefix("loom_step_"), _SIGNING_KEY, algorithms=["HS256"])
    for change in (
        {"service_execution_lease_id": 123},
        {"service_execution_candidate_sha": None},
        {"exp": int(now.timestamp()) + 601},
    ):
        bad = "loom_step_" + jwt.encode({**claims, **change}, _SIGNING_KEY, algorithm="HS256")
        with pytest.raises(jwt.InvalidTokenError):
            verify_step_jwt(bad, signing_key=_SIGNING_KEY)
