import hashlib
from collections.abc import Iterator
from datetime import UTC, datetime
from uuid import UUID, uuid4

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, delete, insert
from sqlalchemy.orm import sessionmaker

from loom.db.schema import Task, Team, TeamQuota, Token, Trial, User
from loom_control_plane.app import create_app
from loom_control_plane.config import ControlPlaneSettings


@pytest.fixture
def seed_team(postgres_url: str) -> Iterator[tuple[UUID, str]]:
    engine = create_engine(postgres_url)
    session_factory = sessionmaker(engine)
    team_id = uuid4()
    user_id = uuid4()
    username = f"TrialSubmitter-{user_id.hex[:8]}"
    raw = f"loom_team_{uuid4().hex}"
    with session_factory() as s:
        s.execute(insert(Team).values(id=team_id, name=f"sub-{team_id}"))
        s.execute(insert(User).values(
            id=user_id,
            username=username,
            username_normalized=username.casefold(),
            status="active",
            is_platform_admin=False,
        ))
        s.execute(insert(TeamQuota).values(team_id=team_id))
        s.execute(insert(Token).values(
            token_hash=hashlib.sha256(raw.encode()).digest(),
            type="team", scopes=["submit"], team_id=team_id,
            created_by_user_id=user_id,
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
            s.execute(delete(User).where(User.id == user_id))
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


def test_submit_rejects_legacy_team_token_without_user_owner(
    app, seed_team, postgres_url: str,  # type: ignore[no-untyped-def]
) -> None:
    team_id, _raw = seed_team
    legacy_raw = f"loom_team_{uuid4().hex}"
    engine = create_engine(postgres_url)
    session_factory = sessionmaker(engine)
    with session_factory() as s:
        s.execute(insert(Token).values(
            token_hash=hashlib.sha256(legacy_raw.encode()).digest(),
            type="team", scopes=["submit"], team_id=team_id,
            issued_at=datetime.now(UTC), expires_at=None,
        ))
        s.commit()
    engine.dispose()

    with TestClient(app) as client:
        r = client.post(
            "/trials",
            headers={"Authorization": f"Bearer {legacy_raw}"},
            json={"task_id": "hello", "config": {"agent_name": "oracle", "agent_model": None}},
        )
        assert r.status_code == 403
        assert "legacy team token" in r.json()["detail"]


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


def _fetch_trial_config(postgres_url: str, trial_id: str) -> dict:
    engine = create_engine(postgres_url)
    try:
        with sessionmaker(engine)() as s:
            row = s.execute(
                Trial.__table__.select().where(Trial.id == UUID(trial_id)),
            ).mappings().one()
            return row["config"]
    finally:
        engine.dispose()


def test_submit_snapshots_retry_defaults_when_absent(
    app, seed_team, postgres_url: str,
):  # type: ignore[no-untyped-def]
    """#401: submitter omits `retry` → deployment defaults snapshotted."""
    _, raw = seed_team
    with TestClient(app) as client:
        r = client.post(
            "/trials",
            headers={"Authorization": f"Bearer {raw}"},
            json={
                "task_id": "hello",
                "config": {"agent_name": "oracle", "agent_model": None},
            },
        )
        assert r.status_code == 201, r.text
        cfg = _fetch_trial_config(postgres_url, r.json()["trial_id"])
    retry = cfg["retry"]
    assert retry["max_attempts"] == 3
    assert set(retry["retry_on"]) == {
        "gateway_error", "provider_transport_disconnect",
    }
    assert retry["backoff"] == {
        "base_sec": 30.0, "max_sec": 600.0,
        "multiplier": 2.0, "jitter": 0.2,
    }


def test_submit_clamps_max_attempts_to_team_ceiling(
    app, seed_team, postgres_url: str,
):  # type: ignore[no-untyped-def]
    """#401: submitter requests 10, team ceiling is 2 → snapshot stores 2."""
    team_id, raw = seed_team
    engine = create_engine(postgres_url)
    try:
        with sessionmaker(engine)() as s:
            s.execute(
                TeamQuota.__table__.update()
                .where(TeamQuota.team_id == team_id)
                .values(max_attempts_ceiling=2),
            )
            s.commit()
    finally:
        engine.dispose()

    with TestClient(app) as client:
        r = client.post(
            "/trials",
            headers={"Authorization": f"Bearer {raw}"},
            json={
                "task_id": "hello",
                "config": {
                    "agent_name": "oracle",
                    "agent_model": None,
                    "retry": {"max_attempts": 10},
                },
            },
        )
        assert r.status_code == 201, r.text
        cfg = _fetch_trial_config(postgres_url, r.json()["trial_id"])
    assert cfg["retry"]["max_attempts"] == 2


def test_submit_preserves_explicit_retry_below_ceiling(
    app, seed_team, postgres_url: str,
):  # type: ignore[no-untyped-def]
    """#401: submitter's explicit retry passes through when under ceiling."""
    _, raw = seed_team
    with TestClient(app) as client:
        r = client.post(
            "/trials",
            headers={"Authorization": f"Bearer {raw}"},
            json={
                "task_id": "hello",
                "config": {
                    "agent_name": "oracle",
                    "agent_model": None,
                    "retry": {
                        "max_attempts": 2,
                        "retry_on": ["agent_timeout"],
                    },
                },
            },
        )
        assert r.status_code == 201, r.text
        cfg = _fetch_trial_config(postgres_url, r.json()["trial_id"])
    assert cfg["retry"]["max_attempts"] == 2
    assert cfg["retry"]["retry_on"] == ["agent_timeout"]
