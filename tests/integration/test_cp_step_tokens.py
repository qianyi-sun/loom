"""POST /admin/step-tokens (Plan 9 Task 4)."""

import hashlib
import os
from collections.abc import Iterator
from datetime import UTC, datetime
from uuid import uuid4

import jwt
import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, delete, insert
from sqlalchemy.orm import sessionmaker

from loom.auth import verify_step_jwt
from loom.db.schema import Task, Team, TeamQuota, Token, Trial
from loom_control_plane.app import create_app
from loom_control_plane.config import ControlPlaneSettings


@pytest.fixture
def seed(postgres_url: str) -> Iterator[dict]:
    """Seed a worker token + one team + one trial (so the step-token mint
    can verify the trial exists and belongs to the team)."""
    engine = create_engine(postgres_url)
    session_local = sessionmaker(engine)
    raw = f"loom_w_{uuid4().hex}"
    team_id = uuid4()
    trial_id = uuid4()
    with session_local() as s:
        s.execute(insert(Token).values(
            token_hash=hashlib.sha256(raw.encode()).digest(),
            type="worker", scopes=["worker:report"], team_id=None,
            issued_at=datetime.now(UTC), expires_at=None,
        ))
        s.execute(insert(Team).values(id=team_id, name=f"t-{team_id}"))
        s.execute(insert(TeamQuota).values(team_id=team_id))
        s.execute(insert(Task).values(id="t", checksum="0" * 64, config={}))
        s.execute(insert(Trial).values(
            id=trial_id, team_id=team_id, task_id="t",
            config={}, requires_caps={}, state="running",
        ))
        s.commit()
    try:
        yield {"token": raw, "team_id": team_id, "trial_id": trial_id}
    finally:
        from loom.db.schema import ProviderConnection
        with session_local() as s:
            s.execute(delete(Trial))
            # Tests in this file may seed a ProviderConnection to
            # exercise issue #72; clean it up before Team to satisfy
            # the FK.
            s.execute(delete(ProviderConnection))
            s.execute(delete(Token))
            s.execute(delete(TeamQuota))
            s.execute(delete(Team))
            s.execute(delete(Task))
            s.commit()
        engine.dispose()


@pytest.fixture
def worker_token(seed: dict) -> str:
    return seed["token"]


@pytest.fixture
def app(monkeypatch: pytest.MonkeyPatch, postgres_url: str, worker_token: str):
    for k, v in {
        "LOOM_CP_DB_URL": postgres_url,
        "LOOM_CP_MINIO_ENDPOINT": "http://minio:9000",
        "LOOM_CP_MINIO_ACCESS_KEY": "x",
        "LOOM_CP_MINIO_SECRET_KEY": "y",
        "LOOM_CP_LLM_GATEWAY_URL": "http://gw:9100/",
    }.items():
        monkeypatch.setenv(k, v)
    return create_app(ControlPlaneSettings(_env_file=None))


def test_issue_step_token_returns_verifiable_jwt(app, seed):  # type: ignore[no-untyped-def]
    worker_token = seed["token"]
    team_id = seed["team_id"]
    trial_id = seed["trial_id"]
    with TestClient(app) as client:
        r = client.post(
            "/admin/step-tokens",
            headers={"Authorization": f"Bearer {worker_token}"},
            json={
                "team_id": str(team_id),
                "trial_id": str(trial_id),
                "step_id": "main",
                "ttl_sec": 60,
            },
        )
        assert r.status_code == 201, r.text
        body = r.json()
        assert body["token"].startswith("loom_step_")
        # The minted token verifies against the same signing key.
        signing_key = os.environ["LOOM_CP_STEP_JWT_SIGNING_KEY"]
        ctx = verify_step_jwt(body["token"], signing_key=signing_key)
        assert ctx.team_id == team_id
        assert ctx.trial_id == trial_id
        assert ctx.step_id == "main"


def test_issue_step_token_rejects_missing_scope(app, postgres_url):  # type: ignore[no-untyped-def]
    """A team token (scope=submit, no worker:report) should be rejected."""
    engine = create_engine(postgres_url)
    session_local = sessionmaker(engine)
    raw = f"team_{uuid4().hex}"
    with session_local() as s:
        s.execute(insert(Token).values(
            token_hash=hashlib.sha256(raw.encode()).digest(),
            type="team", scopes=["submit"], team_id=None,
            issued_at=datetime.now(UTC), expires_at=None,
        ))
        s.commit()
    engine.dispose()
    with TestClient(app) as client:
        r = client.post(
            "/admin/step-tokens",
            headers={"Authorization": f"Bearer {raw}"},
            json={
                "team_id": str(uuid4()),
                "trial_id": str(uuid4()),
                "step_id": "main",
                "ttl_sec": 60,
            },
        )
        assert r.status_code == 403


def test_issue_step_token_rejects_unauthenticated(app, worker_token):  # type: ignore[no-untyped-def]
    with TestClient(app) as client:
        r = client.post(
            "/admin/step-tokens",
            json={
                "team_id": str(uuid4()),
                "trial_id": str(uuid4()),
                "step_id": "main",
                "ttl_sec": 60,
            },
        )
        assert r.status_code == 403


def test_issue_step_token_validates_ttl(app, worker_token):  # type: ignore[no-untyped-def]
    with TestClient(app) as client:
        r = client.post(
            "/admin/step-tokens",
            headers={"Authorization": f"Bearer {worker_token}"},
            json={
                "team_id": str(uuid4()),
                "trial_id": str(uuid4()),
                "step_id": "main",
                "ttl_sec": 99999,  # > 7200 cap
            },
        )
        assert r.status_code == 422


def test_issue_step_token_rejects_unknown_trial(app, seed):  # type: ignore[no-untyped-def]
    """Plan 9 audit fix: trial_id must exist in trials. Otherwise a
    worker could mint tokens for fictional trials and llm_calls would
    accumulate orphan rows."""
    with TestClient(app) as client:
        r = client.post(
            "/admin/step-tokens",
            headers={"Authorization": f"Bearer {seed['token']}"},
            json={
                "team_id": str(seed["team_id"]),
                "trial_id": str(uuid4()),   # fictional
                "step_id": "main",
                "ttl_sec": 60,
            },
        )
        assert r.status_code == 404


def test_issue_step_token_rejects_team_id_mismatch(app, seed):  # type: ignore[no-untyped-def]
    """Plan 9 audit fix: team_id MUST match trial.team_id."""
    with TestClient(app) as client:
        r = client.post(
            "/admin/step-tokens",
            headers={"Authorization": f"Bearer {seed['token']}"},
            json={
                "team_id": str(uuid4()),   # not the trial's owner
                "trial_id": str(seed["trial_id"]),
                "step_id": "main",
                "ttl_sec": 60,
            },
        )
        assert r.status_code == 400


def test_round_trip_with_jwt_can_be_decoded(app, seed):  # type: ignore[no-untyped-def]
    """End-to-end smoke: mint a token, decode raw JWT body, verify claims."""
    with TestClient(app) as client:
        r = client.post(
            "/admin/step-tokens",
            headers={"Authorization": f"Bearer {seed['token']}"},
            json={
                "team_id": str(seed["team_id"]),
                "trial_id": str(seed["trial_id"]),
                "step_id": "phase-2",
                "ttl_sec": 30,
            },
        )
        assert r.status_code == 201
        token = r.json()["token"]
        body = token[len("loom_step_"):]
        signing_key = os.environ["LOOM_CP_STEP_JWT_SIGNING_KEY"]
        claims = jwt.decode(body, signing_key, algorithms=["HS256"])
        assert claims["sub"] == "step-session"
        assert claims["step_id"] == "phase-2"
        assert claims["scopes"] == ["llm:call"]


# ──────────────────────────────────────────────────────────────────────
# Issue #72 — JWT scope carries provider_connection_id from Trial row
# ──────────────────────────────────────────────────────────────────────


def test_step_token_omits_provider_connection_id_when_trial_has_none(
    app, seed,
):  # type: ignore[no-untyped-def]
    """Trial.provider_connection_id IS NULL (e.g. local adapter trial)
    ⇒ JWT must NOT carry a provider_connection_id claim, and
    ctx.provider_connection_id is None on verify."""
    with TestClient(app) as client:
        r = client.post(
            "/admin/step-tokens",
            headers={"Authorization": f"Bearer {seed['token']}"},
            json={
                "team_id": str(seed["team_id"]),
                "trial_id": str(seed["trial_id"]),
                "step_id": "main",
                "ttl_sec": 30,
            },
        )
        assert r.status_code == 201
        ctx = verify_step_jwt(
            r.json()["token"],
            signing_key=os.environ["LOOM_CP_STEP_JWT_SIGNING_KEY"],
        )
        assert ctx.provider_connection_id is None


def test_step_token_carries_trial_provider_connection_id(
    app, seed, postgres_url,
):  # type: ignore[no-untyped-def]
    """Issue #72: CP pulls provider_connection_id from the Trial row at
    mint time (defense against a compromised worker forging a different
    connection_id). Verify the JWT scope contains the right id."""
    from sqlalchemy import update as sa_update

    from loom.db.schema import ProviderConnection
    conn_id = uuid4()
    engine = create_engine(postgres_url)
    session_local = sessionmaker(engine)
    with session_local() as s:
        # FK trial → provider_connections requires the connection row
        # to exist. Seed a minimal one for this team.
        s.execute(insert(ProviderConnection).values(
            id=conn_id,
            team_id=seed["team_id"],
            provider_type="openai-compatible",
            display_name=f"test-conn-{conn_id}",
            base_url="https://api.openai.com/v1",
            upstream_host="api.openai.com",
            encrypted_api_key_ref=f"loom://team:{seed['team_id']}/{conn_id}",
            created_by="admin:fixture",
        ))
        s.execute(
            sa_update(Trial)
            .where(Trial.id == seed["trial_id"])
            .values(provider_connection_id=conn_id),
        )
        s.commit()
    engine.dispose()

    with TestClient(app) as client:
        r = client.post(
            "/admin/step-tokens",
            headers={"Authorization": f"Bearer {seed['token']}"},
            json={
                "team_id": str(seed["team_id"]),
                "trial_id": str(seed["trial_id"]),
                "step_id": "main",
                "ttl_sec": 30,
            },
        )
        assert r.status_code == 201
        ctx = verify_step_jwt(
            r.json()["token"],
            signing_key=os.environ["LOOM_CP_STEP_JWT_SIGNING_KEY"],
        )
        assert ctx.provider_connection_id == conn_id


def test_step_token_does_not_accept_provider_connection_id_in_payload(
    app, seed,
):  # type: ignore[no-untyped-def]
    """Defense in depth (#72): the worker MUST NOT be able to supply
    a different provider_connection_id in the request payload — the
    CP looks it up from the trial row regardless. Pydantic's
    extra="ignore" default silently drops the field; the issued JWT
    carries the trial's value (or None), not the payload's."""
    with TestClient(app) as client:
        r = client.post(
            "/admin/step-tokens",
            headers={"Authorization": f"Bearer {seed['token']}"},
            json={
                "team_id": str(seed["team_id"]),
                "trial_id": str(seed["trial_id"]),
                "step_id": "main",
                "ttl_sec": 30,
                # Attempt at forgery — trial has NULL connection_id.
                "provider_connection_id": str(uuid4()),
            },
        )
        # Either 201 (extras silently ignored — current Pydantic default
        # for the route) or 422 (extras forbidden). Both are safe; the
        # critical assertion is the JWT scope.
        assert r.status_code in (201, 422)
        if r.status_code == 201:
            ctx = verify_step_jwt(
                r.json()["token"],
                signing_key=os.environ["LOOM_CP_STEP_JWT_SIGNING_KEY"],
            )
            # MUST be None — trial has no connection_id; payload-supplied
            # value was ignored.
            assert ctx.provider_connection_id is None
