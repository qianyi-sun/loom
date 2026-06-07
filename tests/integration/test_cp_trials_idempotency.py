"""POST /trials honors `idempotency_key` (Plan 19 Task 2).

Same patterns as test_submit_trial.py (TestClient + postgres_url +
synchronous seed)."""

from __future__ import annotations

import hashlib
from collections.abc import Iterator
from datetime import UTC, datetime
from uuid import UUID, uuid4

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, delete, insert, select
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
    monkeypatch: pytest.MonkeyPatch,
    postgres_url: str,
    seed_team: tuple[UUID, str],
):  # type: ignore[no-untyped-def]
    for k, v in {
        "LOOM_CP_DB_URL": postgres_url,
        "LOOM_CP_MINIO_ENDPOINT": "http://minio:9000",
        "LOOM_CP_MINIO_ACCESS_KEY": "x",
        "LOOM_CP_MINIO_SECRET_KEY": "x",
        "LOOM_CP_LLM_GATEWAY_URL": "http://gw:9100/",
    }.items():
        monkeypatch.setenv(k, v)
    return create_app(ControlPlaneSettings(_env_file=None))


def test_same_idempotency_key_returns_same_trial(
    app,  # type: ignore[no-untyped-def]
    seed_team: tuple[UUID, str],
) -> None:
    _, raw = seed_team
    with TestClient(app) as client:
        r1 = client.post(
            "/trials",
            headers={"Authorization": f"Bearer {raw}"},
            json={
                "task_id": "hello", "config": {},
                "idempotency_key": "abc-123",
            },
        )
        r2 = client.post(
            "/trials",
            headers={"Authorization": f"Bearer {raw}"},
            json={
                "task_id": "hello", "config": {},
                "idempotency_key": "abc-123",
            },
        )
    assert r1.status_code == 201, r1.text
    assert r2.status_code == 201, r2.text
    assert r1.json()["trial_id"] == r2.json()["trial_id"]


def test_different_idempotency_keys_different_trials(
    app,  # type: ignore[no-untyped-def]
    seed_team: tuple[UUID, str],
) -> None:
    _, raw = seed_team
    with TestClient(app) as client:
        r1 = client.post(
            "/trials",
            headers={"Authorization": f"Bearer {raw}"},
            json={
                "task_id": "hello", "config": {},
                "idempotency_key": "k1",
            },
        )
        r2 = client.post(
            "/trials",
            headers={"Authorization": f"Bearer {raw}"},
            json={
                "task_id": "hello", "config": {},
                "idempotency_key": "k2",
            },
        )
    assert r1.status_code == 201
    assert r2.status_code == 201
    assert r1.json()["trial_id"] != r2.json()["trial_id"]


def test_no_idempotency_key_creates_distinct_trials(
    app,  # type: ignore[no-untyped-def]
    seed_team: tuple[UUID, str],
) -> None:
    """Hand-submitted trials (no idempotency_key) still get distinct ids."""
    _, raw = seed_team
    with TestClient(app) as client:
        r1 = client.post(
            "/trials",
            headers={"Authorization": f"Bearer {raw}"},
            json={"task_id": "hello", "config": {}},
        )
        r2 = client.post(
            "/trials",
            headers={"Authorization": f"Bearer {raw}"},
            json={"task_id": "hello", "config": {}},
        )
    assert r1.json()["trial_id"] != r2.json()["trial_id"]


def test_campaign_id_stored_on_trial(
    app,  # type: ignore[no-untyped-def]
    seed_team: tuple[UUID, str],
    postgres_url: str,
) -> None:
    """When `campaign_id` is present in the payload, it lands on the trial row."""
    _, raw = seed_team
    # Seed a campaign so the FK is satisfied.
    from loom.db.schema import Campaign
    engine = create_engine(postgres_url)
    sl = sessionmaker(engine)
    team_id, _ = seed_team
    campaign_id = uuid4()
    with sl() as s:
        s.add(Campaign(
            id=campaign_id, team_id=team_id, name="c",
            task_filter={}, trial_config={},
            state="submitted", created_by_token_prefix="abcdef12",
            expected_trial_count=1,
        ))
        s.commit()
    engine.dispose()
    try:
        with TestClient(app) as client:
            r = client.post(
                "/trials",
                headers={"Authorization": f"Bearer {raw}"},
                json={
                    "task_id": "hello", "config": {},
                    "campaign_id": str(campaign_id),
                    "idempotency_key": f"{campaign_id}::hello",
                },
            )
        assert r.status_code == 201, r.text
        trial_id = UUID(r.json()["trial_id"])
        engine = create_engine(postgres_url)
        sl = sessionmaker(engine)
        with sl() as s:
            trial = s.execute(
                select(Trial).where(Trial.id == trial_id),
            ).scalar_one()
        engine.dispose()
        assert trial.campaign_id == campaign_id
        assert trial.idempotency_key == f"{campaign_id}::hello"
    finally:
        engine = create_engine(postgres_url)
        sl = sessionmaker(engine)
        with sl() as s:
            s.execute(delete(Trial))
            s.execute(delete(Campaign))
            s.commit()
        engine.dispose()
