import hashlib
from collections.abc import Iterator
from datetime import UTC, datetime
from uuid import UUID, uuid4

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, delete, insert
from sqlalchemy.orm import sessionmaker

from loom.db.schema import Task, Team, Token, Trial, Worker
from loom_control_plane.app import create_app
from loom_control_plane.config import ControlPlaneSettings


@pytest.fixture
def traj_seed(postgres_url: str) -> Iterator[tuple[UUID, UUID, str]]:
    engine = create_engine(postgres_url)
    session_factory = sessionmaker(engine)
    team_id = uuid4()
    worker_id = uuid4()
    trial_id = uuid4()
    raw = f"w_{uuid4().hex}"
    with session_factory() as s:
        s.execute(insert(Team).values(id=team_id, name=f"x-{team_id}"))
        s.execute(insert(Token).values(
            token_hash=hashlib.sha256(raw.encode()).digest(),
            type="worker", scopes=["worker:index"], team_id=None,
            issued_at=datetime.now(UTC), expires_at=None,
        ))
        s.execute(insert(Worker).values(
            id=worker_id, hostname="h", version="v", capabilities=[],
            registered_at=datetime.now(UTC),
            last_seen_at=datetime.now(UTC), status="active",
        ))
        s.execute(insert(Task).values(id="t", checksum="0" * 64, config={}))
        s.execute(insert(Trial).values(
            id=trial_id, team_id=team_id, task_id="t",
            config={}, requires_caps={}, state="running", worker_id=worker_id,
        ))
        s.commit()
    try:
        yield trial_id, worker_id, raw
    finally:
        with session_factory() as s:
            s.execute(delete(Trial))
            s.execute(delete(Worker))
            s.execute(delete(Token))
            s.execute(delete(Team))
            s.execute(delete(Task))
            s.commit()
        engine.dispose()


@pytest.fixture
def app(
    monkeypatch: pytest.MonkeyPatch, postgres_url: str,
    traj_seed: tuple[UUID, UUID, str],
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


def test_index_patch(app, traj_seed):  # type: ignore[no-untyped-def]
    trial_id, worker_id, raw = traj_seed
    with TestClient(app) as client:
        r = client.patch(
            f"/trials/{trial_id}/trajectory_index",
            headers={"Authorization": f"Bearer {raw}"},
            json={
                "worker_id": str(worker_id),
                "trajectory_uri": f"s3://trajectories/x/{trial_id}/events.jsonl",
                "bytes_uploaded": 1024,
                "events_count": 25,
                "checksum_sha256": "abcd",
            },
        )
        assert r.status_code == 200


def test_index_patch_fenced(app, traj_seed):  # type: ignore[no-untyped-def]
    """A different worker_id → 409 (claim lost)."""
    trial_id, _worker_id, raw = traj_seed
    with TestClient(app) as client:
        r = client.patch(
            f"/trials/{trial_id}/trajectory_index",
            headers={"Authorization": f"Bearer {raw}"},
            json={
                "worker_id": str(uuid4()),
                "trajectory_uri": "s3://x",
            },
        )
        assert r.status_code == 409
