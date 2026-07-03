import hashlib
from collections.abc import Iterator
from datetime import UTC, datetime
from uuid import UUID, uuid4

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, delete, insert
from sqlalchemy.orm import sessionmaker

from loom.db.schema import Task, Team, Token, Trial, User
from loom_control_plane.app import create_app
from loom_control_plane.config import ControlPlaneSettings


@pytest.fixture
def cancel_seed(postgres_url: str) -> Iterator[tuple[str, UUID, UUID, UUID]]:
    engine = create_engine(postgres_url)
    session_factory = sessionmaker(engine)
    team_id = uuid4()
    user_id = uuid4()
    raw = f"t_{uuid4().hex}"
    queued = uuid4()
    running = uuid4()
    done = uuid4()
    with session_factory() as s:
        s.execute(insert(Team).values(id=team_id, name=f"x-{team_id}"))
        s.execute(insert(User).values(
            id=user_id,
            username=f"CancelUser-{user_id.hex[:8]}",
            username_normalized=f"cancel-user-{user_id.hex[:8]}",
            status="active",
            is_platform_admin=False,
        ))
        s.execute(insert(Token).values(
            token_hash=hashlib.sha256(raw.encode()).digest(),
            type="team", scopes=["submit"], team_id=team_id,
            created_by_user_id=user_id,
            issued_at=datetime.now(UTC), expires_at=None,
        ))
        s.execute(insert(Task).values(id="t", checksum="0" * 64, config={}))
        for tid, state in [
            (queued, "queued"), (running, "running"), (done, "succeeded"),
        ]:
            s.execute(insert(Trial).values(
                id=tid, team_id=team_id, task_id="t",
                config={}, requires_caps={}, state=state,
                result={} if state == "succeeded" else None,
            ))
        s.commit()
    try:
        yield raw, queued, running, done
    finally:
        with session_factory() as s:
            s.execute(delete(Trial))
            s.execute(delete(Token))
            s.execute(delete(User).where(User.id == user_id))
            s.execute(delete(Team))
            s.execute(delete(Task))
            s.commit()
        engine.dispose()


@pytest.fixture
def app(
    monkeypatch: pytest.MonkeyPatch, postgres_url: str,
    cancel_seed: tuple[str, UUID, UUID, UUID],
):
    for k, v in {
        "LOOM_CP_DB_URL": postgres_url,
        "LOOM_CP_MINIO_ENDPOINT": "http://minio:9000",
        "LOOM_CP_MINIO_ACCESS_KEY": "x",
        "LOOM_CP_MINIO_SECRET_KEY": "y",
        "LOOM_CP_LLM_GATEWAY_URL": "http://gw:9100/",
    }.items():
        monkeypatch.setenv(k, v)
    return create_app(ControlPlaneSettings(_env_file=None))


def test_cancel_queued(app, cancel_seed):  # type: ignore[no-untyped-def]
    raw, queued, _, _ = cancel_seed
    with TestClient(app) as client:
        r = client.post(
            f"/trials/{queued}/cancel",
            headers={"Authorization": f"Bearer {raw}"},
        )
        assert r.status_code == 200
        assert r.json()["state"] == "cancelled"


def test_cancel_running(app, cancel_seed):  # type: ignore[no-untyped-def]
    raw, _, running, _ = cancel_seed
    with TestClient(app) as client:
        r = client.post(
            f"/trials/{running}/cancel",
            headers={"Authorization": f"Bearer {raw}"},
        )
        assert r.status_code == 200


def test_cancel_terminal_returns_409(app, cancel_seed):  # type: ignore[no-untyped-def]
    raw, _, _, done = cancel_seed
    with TestClient(app) as client:
        r = client.post(
            f"/trials/{done}/cancel",
            headers={"Authorization": f"Bearer {raw}"},
        )
        assert r.status_code == 409
