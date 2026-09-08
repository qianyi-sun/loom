from datetime import UTC, datetime, timedelta
from uuid import uuid4

import httpx
import jwt
import pytest
from pydantic import ValidationError

from loom.attempt_deadline import AttemptDeadline
from loom.auth import mint_step_jwt, verify_step_jwt
from loom_control_plane.routes.step_tokens import _IssueStepTokenRequest
from loom_worker.control_plane_client import HttpControlPlaneClient, StepTokenGrant

SIGNING_KEY = "test-only-agent-attempt-signing-key"


@pytest.mark.parametrize("scope", ["pipeline", "no_deadline", "verifier", "family_evolver"])
def test_cp_request_rejects_cross_scope_agent_identity(scope: str) -> None:
    body = {
        "team_id": str(uuid4()),
        "trial_id": str(uuid4()),
        "step_id": "main",
        "ttl_sec": 600,
        "agent_attempt_id": str(uuid4()),
        "attempt_deadline_wall_clock": (datetime.now(UTC) + timedelta(seconds=60)).isoformat(),
    }
    if scope == "pipeline":
        body["execution_attempt_id"] = body.pop("trial_id")
    elif scope == "no_deadline":
        del body["attempt_deadline_wall_clock"]
    else:
        body["step_id"] = scope
    with pytest.raises(ValidationError, match="agent attempt identity"):
        _IssueStepTokenRequest.model_validate(body)


def test_trial_attempt_identity_and_grant_are_signed() -> None:
    attempt_id, grant_id = uuid4(), uuid4()
    token = mint_step_jwt(
        team_id=uuid4(),
        trial_id=uuid4(),
        step_id="main",
        ttl_sec=600,
        signing_key=SIGNING_KEY,
        agent_attempt_id=attempt_id,
        step_jwt_id=grant_id,
        attempt_deadline_wall_clock=datetime.now(UTC) + timedelta(seconds=60),
    )
    context = verify_step_jwt(token, signing_key=SIGNING_KEY)
    assert context.agent_attempt_id == attempt_id
    assert context.step_jwt_id == grant_id


@pytest.mark.parametrize("subject", ["pipeline", "no_deadline", "no_grant"])
def test_attempt_identity_requires_trial_deadline_and_grant(subject: str) -> None:
    kwargs = (
        {"trial_id": uuid4()}
        if subject != "pipeline"
        else {
            "execution_attempt_id": uuid4(),
        }
    )
    with pytest.raises(ValueError, match="agent attempt"):
        mint_step_jwt(
            team_id=uuid4(),
            step_id="main",
            ttl_sec=600,
            signing_key=SIGNING_KEY,
            agent_attempt_id=uuid4(),
            step_jwt_id=uuid4() if subject != "no_grant" else None,
            attempt_deadline_wall_clock=(
                datetime.now(UTC) + timedelta(seconds=60) if subject != "no_deadline" else None
            ),
            **kwargs,
        )


@pytest.mark.parametrize("change", ["pipeline", "deadline", "grant", "bad_uuid", "verifier"])
def test_signed_but_invalid_identity_claims_are_rejected(change: str) -> None:
    token = mint_step_jwt(
        team_id=uuid4(),
        trial_id=uuid4(),
        step_id="main",
        ttl_sec=600,
        signing_key=SIGNING_KEY,
        agent_attempt_id=uuid4(),
        step_jwt_id=uuid4(),
        attempt_deadline_wall_clock=datetime.now(UTC) + timedelta(seconds=60),
    )
    claims = jwt.decode(token[len("loom_step_") :], SIGNING_KEY, algorithms=["HS256"])
    if change == "pipeline":
        claims["execution_attempt_id"] = claims.pop("trial_id")
        claims["subject_kind"] = "execution_attempt"
    elif change == "deadline":
        del claims["attempt_deadline_wall_clock"]
    elif change == "grant":
        del claims["jti"]
    elif change == "verifier":
        claims["step_id"] = "verifier"
    else:
        claims["agent_attempt_id"] = 123
    invalid = "loom_step_" + jwt.encode(claims, SIGNING_KEY, algorithm="HS256")
    with pytest.raises(jwt.InvalidTokenError):
        verify_step_jwt(invalid, signing_key=SIGNING_KEY)


def test_legacy_token_has_no_inferred_attempt_identity() -> None:
    token = mint_step_jwt(
        team_id=uuid4(),
        trial_id=uuid4(),
        step_id="main",
        ttl_sec=600,
        signing_key=SIGNING_KEY,
    )
    context = verify_step_jwt(token, signing_key=SIGNING_KEY)
    assert context.agent_attempt_id is None
    assert context.step_jwt_id is None


@pytest.mark.parametrize("mismatch", ["attempt", "grant", "deadline", "local_only"])
def test_grant_response_must_match_signed_claims(mismatch: str) -> None:
    attempt_id, grant_id = uuid4(), uuid4()
    deadline = datetime.now(UTC) + timedelta(seconds=60)
    token = mint_step_jwt(
        team_id=uuid4(),
        trial_id=uuid4(),
        step_id="main",
        ttl_sec=600,
        signing_key=SIGNING_KEY,
        agent_attempt_id=attempt_id,
        step_jwt_id=grant_id,
        attempt_deadline_wall_clock=deadline,
    )
    context = verify_step_jwt(token, signing_key=SIGNING_KEY)
    assert context.expires_at is not None
    payload = {
        "token": token,
        "expires_at": context.expires_at.isoformat(),
        "attempt_deadline_wall_clock": deadline.isoformat(),
        "agent_attempt_id": str(attempt_id),
        "step_jwt_id": str(grant_id),
    }
    if mismatch == "attempt":
        payload["agent_attempt_id"] = str(uuid4())
    elif mismatch == "grant":
        payload["step_jwt_id"] = str(uuid4())
    elif mismatch == "deadline":
        payload["attempt_deadline_wall_clock"] = (deadline + timedelta(seconds=1)).isoformat()
    else:
        payload["token"] = "loom_step_cli-token"
        payload["local_only"] = True  # type: ignore[assignment]
    with pytest.raises(ValueError):
        StepTokenGrant.from_payload(payload)


async def test_deadline_records_safe_grant_and_rejects_other_attempt() -> None:
    observed = []

    async def observer(attempt_id, grant_id):  # type: ignore[no-untyped-def]
        observed.append((attempt_id, grant_id))

    attempt_id, grant_id = uuid4(), uuid4()
    deadline = AttemptDeadline.after(60, agent_attempt_id=attempt_id, grant_observer=observer)
    await deadline.record_step_token_grant(agent_attempt_id=attempt_id, step_jwt_id=grant_id)
    assert observed == [(attempt_id, grant_id)]
    with pytest.raises(ValueError, match="does not match"):
        await deadline.record_step_token_grant(agent_attempt_id=uuid4(), step_jwt_id=grant_id)
    assert observed == [(attempt_id, grant_id)]


@pytest.mark.parametrize("mismatch", [None, "subject", "missing_identity"])
async def test_http_client_validates_requested_identity(mismatch: str | None) -> None:
    team_id, trial_id, attempt_id, grant_id = uuid4(), uuid4(), uuid4(), uuid4()
    deadline = datetime.now(UTC) + timedelta(seconds=60)
    token = mint_step_jwt(
        team_id=team_id,
        trial_id=trial_id if mismatch != "subject" else uuid4(),
        step_id="main",
        ttl_sec=600,
        signing_key=SIGNING_KEY,
        agent_attempt_id=attempt_id,
        step_jwt_id=grant_id,
        attempt_deadline_wall_clock=deadline,
    )
    context = verify_step_jwt(token, signing_key=SIGNING_KEY)
    assert context.expires_at is not None
    response = {
        "token": token,
        "expires_at": context.expires_at.isoformat(),
        "attempt_deadline_wall_clock": deadline.isoformat(),
        "agent_attempt_id": str(attempt_id) if mismatch != "missing_identity" else None,
        "step_jwt_id": str(grant_id),
        "local_only": True,  # An HTTP server cannot grant a local-only bypass.
    }
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda request: httpx.Response(201, json=response)),
        base_url="http://control-plane.test",
    ) as http:
        client = HttpControlPlaneClient(
            base_url="http://control-plane.test",
            token="test-worker-token",
            _client=http,
        )

        async def mint() -> StepTokenGrant:
            return await client.mint_attempt_step_token(
                team_id=team_id,
                trial_id=trial_id,
                step_id="main",
                ttl_sec=600,
                attempt_deadline_wall_clock=deadline,
                agent_attempt_id=attempt_id,
            )

        if mismatch is not None:
            with pytest.raises(ValueError, match="changed"):
                await mint()
        else:
            grant = await mint()
            assert grant.agent_attempt_id == attempt_id
            assert grant.step_jwt_id == grant_id
            assert not grant.local_only
