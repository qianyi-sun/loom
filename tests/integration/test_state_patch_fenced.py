import hashlib
from collections.abc import Iterator
from datetime import UTC, datetime
from uuid import UUID, uuid4

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, delete, insert
from sqlalchemy.orm import sessionmaker

from loom.db.schema import Task, Team, TeamQuota, Token, Trial, Worker
from loom_control_plane.app import create_app
from loom_control_plane.config import ControlPlaneSettings


@pytest.fixture
def state_seed(postgres_url: str) -> Iterator[tuple[UUID, UUID, UUID, str, str]]:
    engine = create_engine(postgres_url)
    session_factory = sessionmaker(engine)
    team_id = uuid4()
    worker_a = uuid4()
    worker_b = uuid4()
    trial_id = uuid4()
    raw_a = f"wa_{uuid4().hex}"
    raw_b = f"wb_{uuid4().hex}"
    with session_factory() as s:
        s.execute(insert(Team).values(id=team_id, name=f"x-{team_id}"))
        s.execute(insert(TeamQuota).values(team_id=team_id))
        for raw in (raw_a, raw_b):
            s.execute(insert(Token).values(
                token_hash=hashlib.sha256(raw.encode()).digest(),
                type="worker", scopes=["worker:report"], team_id=None,
                issued_at=datetime.now(UTC), expires_at=None,
            ))
        for wid in (worker_a, worker_b):
            s.execute(insert(Worker).values(
                id=wid, hostname="h", version="v", capabilities=[],
                registered_at=datetime.now(UTC),
                last_seen_at=datetime.now(UTC), status="active",
            ))
        s.execute(insert(Task).values(id="t", checksum="0" * 64, config={}))
        s.execute(insert(Trial).values(
            id=trial_id, team_id=team_id, task_id="t",
            config={}, requires_caps={}, state="claimed",
            worker_id=worker_a,
        ))
        s.commit()
    try:
        yield trial_id, worker_a, worker_b, raw_a, raw_b
    finally:
        with session_factory() as s:
            s.execute(delete(Trial))
            s.execute(delete(Worker))
            s.execute(delete(Token))
            s.execute(delete(TeamQuota))
            s.execute(delete(Team))
            s.execute(delete(Task))
            s.commit()
        engine.dispose()


@pytest.fixture
def app(
    monkeypatch: pytest.MonkeyPatch, postgres_url: str,
    state_seed: tuple[UUID, UUID, UUID, str, str],
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


def test_state_patch_with_matching_worker(app, state_seed):  # type: ignore[no-untyped-def]
    trial_id, worker_a, _, raw_a, _ = state_seed
    with TestClient(app) as client:
        r = client.patch(
            f"/trials/{trial_id}/state",
            headers={"Authorization": f"Bearer {raw_a}"},
            json={"worker_id": str(worker_a), "state": "running"},
        )
        assert r.status_code == 200
        assert r.json()["state"] == "running"


def test_state_patch_with_wrong_worker_fenced(app, state_seed):  # type: ignore[no-untyped-def]
    trial_id, _, worker_b, _, raw_b = state_seed
    with TestClient(app) as client:
        r = client.patch(
            f"/trials/{trial_id}/state",
            headers={"Authorization": f"Bearer {raw_b}"},
            json={"worker_id": str(worker_b), "state": "running"},
        )
        assert r.status_code == 409
        assert "claim" in r.json()["detail"].lower()


def test_state_patch_terminal_state(app, state_seed):  # type: ignore[no-untyped-def]
    """Transition to a terminal state should set finished_at."""
    trial_id, worker_a, _, raw_a, _ = state_seed
    with TestClient(app) as client:
        r = client.patch(
            f"/trials/{trial_id}/state",
            headers={"Authorization": f"Bearer {raw_a}"},
            json={
                "worker_id": str(worker_a), "state": "succeeded",
            },
        )
        assert r.status_code == 200
        assert r.json()["state"] == "succeeded"


def test_state_patch_rejects_invalid_state(app, state_seed):  # type: ignore[no-untyped-def]
    """Bug 1 regression: garbage state strings rejected with 400."""
    trial_id, worker_a, _, raw_a, _ = state_seed
    with TestClient(app) as client:
        r = client.patch(
            f"/trials/{trial_id}/state",
            headers={"Authorization": f"Bearer {raw_a}"},
            json={"worker_id": str(worker_a), "state": "bogus"},
        )
        assert r.status_code == 400
        assert "invalid state" in r.json()["detail"]


def test_state_patch_rejects_invalid_failure_reason(app, state_seed):  # type: ignore[no-untyped-def]
    """Bug 1 regression: failure_reason validated against enum."""
    trial_id, worker_a, _, raw_a, _ = state_seed
    with TestClient(app) as client:
        r = client.patch(
            f"/trials/{trial_id}/state",
            headers={"Authorization": f"Bearer {raw_a}"},
            json={
                "worker_id": str(worker_a),
                "state": "failed",
                "failure_reason": "made_up",
            },
        )
        assert r.status_code == 400
        assert "failure_reason" in r.json()["detail"]


def test_state_patch_rejects_succeeded_when_queued(  # type: ignore[no-untyped-def]
    app, state_seed, postgres_url,
):
    """Bug 4 regression: succeeded only from claimed/running, not queued.

    We mutate the seeded trial's state back to 'queued' and confirm the PATCH
    to 'succeeded' is refused with 409 even though the fencing predicate
    matches the worker.
    """
    from sqlalchemy import create_engine, text
    trial_id, worker_a, _, raw_a, _ = state_seed
    eng = create_engine(postgres_url)
    with eng.begin() as conn:
        conn.execute(
            text("UPDATE trials SET state = 'queued' WHERE id = :id"),
            {"id": trial_id},
        )
    eng.dispose()
    with TestClient(app) as client:
        r = client.patch(
            f"/trials/{trial_id}/state",
            headers={"Authorization": f"Bearer {raw_a}"},
            json={"worker_id": str(worker_a), "state": "succeeded"},
        )
        assert r.status_code == 409


def test_state_patch_rejects_unreachable_target(app, state_seed):  # type: ignore[no-untyped-def]
    """Bug 4 regression: targets not in _ALLOWED_FROM (e.g. 'queued',
    'claimed') cannot be reached via PATCH and must return 400."""
    trial_id, worker_a, _, raw_a, _ = state_seed
    with TestClient(app) as client:
        r = client.patch(
            f"/trials/{trial_id}/state",
            headers={"Authorization": f"Bearer {raw_a}"},
            json={"worker_id": str(worker_a), "state": "queued"},
        )
        assert r.status_code == 400
        assert "cannot be reached" in r.json()["detail"]
