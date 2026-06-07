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
        with session_local() as s:
            s.execute(delete(Trial))
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
