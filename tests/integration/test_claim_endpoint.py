import hashlib
from collections.abc import Iterator
from datetime import UTC, datetime
from uuid import UUID, uuid4

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, delete, insert, select
from sqlalchemy.orm import sessionmaker

from loom.db.schema import Task, Team, TeamQuota, Token, Trial, Worker
from loom_control_plane.app import create_app
from loom_control_plane.config import ControlPlaneSettings


@pytest.fixture
def claim_seed(postgres_url: str) -> Iterator[tuple[UUID, str, UUID]]:
    engine = create_engine(postgres_url)
    session_factory = sessionmaker(engine)
    team_id = uuid4()
    worker_id = uuid4()
    raw_worker = f"loom_worker_{uuid4().hex}"
    with session_factory() as s:
        s.execute(insert(Team).values(id=team_id, name=f"x-{team_id}"))
        s.execute(insert(TeamQuota).values(team_id=team_id))
        s.execute(insert(Token).values(
            token_hash=hashlib.sha256(raw_worker.encode()).digest(),
            type="worker", scopes=["worker:claim", "worker:report"],
            team_id=None,
            issued_at=datetime.now(UTC), expires_at=None,
        ))
        s.execute(insert(Task).values(id="t", checksum="0" * 64, config={}))
        s.execute(insert(Trial).values(
            id=uuid4(), team_id=team_id, task_id="t",
            config={},
            requires_caps={
                "os": "linux", "gpu_vendor": "none",
                "network_policies": ["public"],
            },
            state="queued",
        ))
        s.execute(insert(Worker).values(
            id=worker_id, hostname="h", version="v",
            capabilities=[{
                "os": "linux", "gpu_vendor": "none",
                "network_policies": ["public"],
                "dynamic_network_policy": True, "mounted_fs": True,
                "resource_modes": ["auto"],
            }],
            registered_at=datetime.now(UTC),
            last_seen_at=datetime.now(UTC), status="active",
        ))
        s.commit()
    try:
        yield worker_id, raw_worker, team_id
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
    claim_seed: tuple[UUID, str, UUID],
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


_LINUX_PUBLIC_CAP = {
    "os": "linux", "gpu_vendor": "none",
    "network_policies": ["public"],
    "dynamic_network_policy": True, "mounted_fs": True,
    "resource_modes": ["auto"],
}


def test_claim_returns_trial(app, claim_seed):  # type: ignore[no-untyped-def]
    worker_id, raw_worker, _ = claim_seed
    with TestClient(app) as client:
        r = client.post(
            "/trials/claim",
            headers={"Authorization": f"Bearer {raw_worker}"},
            json={"worker_id": str(worker_id), "caps": [_LINUX_PUBLIC_CAP]},
        )
        assert r.status_code == 200, r.text
        body = r.json()
        assert "trial_id" in body
        assert body["state"] == "claimed"
        assert body["attempt_count"] == 1


def test_claim_no_match_returns_204(app, claim_seed):  # type: ignore[no-untyped-def]
    worker_id, raw_worker, _ = claim_seed
    with TestClient(app) as client:
        # First claim drains the only queued trial.
        client.post(
            "/trials/claim",
            headers={"Authorization": f"Bearer {raw_worker}"},
            json={"worker_id": str(worker_id), "caps": [_LINUX_PUBLIC_CAP]},
        )
        # Second claim has nothing.
        r = client.post(
            "/trials/claim",
            headers={"Authorization": f"Bearer {raw_worker}"},
            json={"worker_id": str(worker_id), "caps": [_LINUX_PUBLIC_CAP]},
        )
        assert r.status_code == 204


def test_claim_rejects_unauth(app, claim_seed):  # type: ignore[no-untyped-def]
    worker_id, _, _ = claim_seed
    with TestClient(app) as client:
        r = client.post(
            "/trials/claim",
            json={"worker_id": str(worker_id), "caps": [_LINUX_PUBLIC_CAP]},
        )
        assert r.status_code == 401


def test_heartbeat_updates_last_seen(app, claim_seed):  # type: ignore[no-untyped-def]
    worker_id, raw_worker, _ = claim_seed
    with TestClient(app) as client:
        r = client.post(
            f"/workers/{worker_id}/heartbeat",
            headers={"Authorization": f"Bearer {raw_worker}"},
        )
        assert r.status_code == 200
        assert r.json()["status"] == "ok"


def test_heartbeat_can_mark_intentional_idle_exit(
    app,
    claim_seed,
    postgres_url: str,
):  # type: ignore[no-untyped-def]
    worker_id, raw_worker, _ = claim_seed
    with TestClient(app) as client:
        r = client.post(
            f"/workers/{worker_id}/heartbeat",
            headers={"Authorization": f"Bearer {raw_worker}"},
            json={"status": "idle-exit"},
        )
        assert r.status_code == 200
        assert r.json()["status"] == "ok"

    engine = create_engine(postgres_url)
    session_factory = sessionmaker(engine)
    with session_factory() as session:
        status = session.execute(
            select(Worker.status).where(Worker.id == worker_id),
        ).scalar_one()
    engine.dispose()

    assert status == "idle-exit"


def test_heartbeat_rejects_unknown_status(app, claim_seed):  # type: ignore[no-untyped-def]
    worker_id, raw_worker, _ = claim_seed
    with TestClient(app) as client:
        r = client.post(
            f"/workers/{worker_id}/heartbeat",
            headers={"Authorization": f"Bearer {raw_worker}"},
            json={"status": "surprise"},
        )
        assert r.status_code == 400
