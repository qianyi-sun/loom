import hashlib
from collections.abc import Iterator
from datetime import UTC, datetime
from uuid import UUID, uuid4

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, delete, insert
from sqlalchemy.orm import sessionmaker

from loom.db.schema import Task, Team, TeamQuota, Token, Trial
from loom_control_plane.app import create_app
from loom_control_plane.config import ControlPlaneSettings


@pytest.fixture
def seed_team(postgres_url: str) -> Iterator[tuple[UUID, str]]:
    engine = create_engine(postgres_url)
    session_factory = sessionmaker(engine)
    team_id = uuid4()
    raw = f"loom_team_{uuid4().hex}"
    with session_factory() as s:
        s.execute(insert(Team).values(id=team_id, name=f"sub-{team_id}"))
        s.execute(insert(TeamQuota).values(team_id=team_id))
        s.execute(insert(Token).values(
            token_hash=hashlib.sha256(raw.encode()).digest(),
            type="team", scopes=["submit"], team_id=team_id,
            issued_at=datetime.now(UTC), expires_at=None,
        ))
        s.execute(insert(Task).values(
            id="hello", checksum="0" * 64,
            config={
                "schema_version": "1",
                "task": {"id": "hello", "name": "hello"},
                "environment": {"os": "linux", "docker_image": "alpine"},
                "agent": {"name": "oracle"},
                "verifier": {"name": "pytest"},
                "steps": [{"name": "main"}],
            },
        ))
        s.execute(insert(Task).values(
            id="broken-config", checksum="1" * 64, config={},
        ))
        s.commit()
    try:
        yield team_id, raw
    finally:
        with session_factory() as s:
            s.execute(delete(Trial))
            s.execute(delete(Token))
            s.execute(delete(TeamQuota))
            s.execute(delete(Team))
            s.execute(delete(Task))
            s.commit()
        engine.dispose()


@pytest.fixture
def app(
    monkeypatch: pytest.MonkeyPatch, postgres_url: str,
    seed_team: tuple[UUID, str],
):
    for k, v in {
        "LOOM_CP_DB_URL": postgres_url,
        "LOOM_CP_MINIO_ENDPOINT": "http://minio:9000",
        "LOOM_CP_MINIO_ACCESS_KEY": "x",
        "LOOM_CP_MINIO_SECRET_KEY": "x",
        "LOOM_CP_LLM_GATEWAY_URL": "http://gw:9100/",
    }.items():
        monkeypatch.setenv(k, v)
    return create_app(ControlPlaneSettings(_env_file=None))


def test_submit_creates_trial(app, seed_team):  # type: ignore[no-untyped-def]
    _, raw = seed_team
    with TestClient(app) as client:
        r = client.post(
            "/trials",
            headers={"Authorization": f"Bearer {raw}"},
            json={"task_id": "hello", "config": {"agent_name": "oracle", "agent_model": None}},
        )
        assert r.status_code == 201, r.text
        body = r.json()
        assert "trial_id" in body
        assert body["state"] == "queued"
        assert "submitted_at" in body


def test_submit_rejects_unknown_task(app, seed_team):  # type: ignore[no-untyped-def]
    _, raw = seed_team
    with TestClient(app) as client:
        r = client.post(
            "/trials",
            headers={"Authorization": f"Bearer {raw}"},
            json={"task_id": "nope", "config": {"agent_name": "oracle", "agent_model": None}},
        )
        assert r.status_code == 404


def test_submit_rejects_invalid_task_config(app, seed_team):  # type: ignore[no-untyped-def]
    _, raw = seed_team
    with TestClient(app) as client:
        r = client.post(
            "/trials",
            headers={"Authorization": f"Bearer {raw}"},
            json={
                "task_id": "broken-config",
                "config": {"agent_name": "oracle", "agent_model": None},
            },
        )
        assert r.status_code == 400
        assert "invalid task config" in r.json()["detail"]
        assert "broken-config" in r.json()["detail"]


def test_submit_rejects_unauth(app, seed_team):  # type: ignore[no-untyped-def]
    with TestClient(app) as client:
        r = client.post("/trials", json={"task_id": "hello", "config": {"agent_name": "oracle", "agent_model": None}})
        assert r.status_code == 401


def test_submit_rejects_missing_task_id(app, seed_team):  # type: ignore[no-untyped-def]
    _, raw = seed_team
    with TestClient(app) as client:
        r = client.post(
            "/trials",
            headers={"Authorization": f"Bearer {raw}"},
            json={"config": {"agent_name": "oracle", "agent_model": None}},
        )
        assert r.status_code == 400
