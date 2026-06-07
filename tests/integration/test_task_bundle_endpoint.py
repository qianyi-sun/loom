"""Integration: GET /tasks/{id}/bundle."""

import hashlib
from collections.abc import Iterator
from datetime import UTC, datetime
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, delete, insert
from sqlalchemy.orm import sessionmaker

from loom.db.schema import Task, Token
from loom_control_plane.app import create_app
from loom_control_plane.config import ControlPlaneSettings


@pytest.fixture
def task_seed(postgres_url: str) -> Iterator[str]:
    engine = create_engine(postgres_url)
    session_local = sessionmaker(engine)
    raw = f"loom_w_{uuid4().hex}"
    with session_local() as s:
        s.execute(insert(Token).values(
            token_hash=hashlib.sha256(raw.encode()).digest(),
            type="worker", scopes=["worker:claim"], team_id=None,
            issued_at=datetime.now(UTC), expires_at=None,
        ))
        s.execute(insert(Task).values(
            id="hello", checksum="deadbeef" * 8,
            config={
                "schema_version": "1",
                "task": {"id": "hello", "name": "hello"},
                "environment": {"os": "linux", "docker_image": "alpine"},
                "agent": {"name": "oracle"},
                "verifier": {"name": "pytest"},
                "steps": [{"name": "main"}],
            },
            source="git+https://example.com/tasks/hello",
        ))
        s.commit()
    try:
        yield raw
    finally:
        with session_local() as s:
            s.execute(delete(Token))
            s.execute(delete(Task))
            s.commit()
        engine.dispose()


@pytest.fixture
def app(monkeypatch: pytest.MonkeyPatch, postgres_url: str, task_seed: str):
    for k, v in {
        "LOOM_CP_DB_URL": postgres_url,
        "LOOM_CP_MINIO_ENDPOINT": "http://minio:9000",
        "LOOM_CP_MINIO_ACCESS_KEY": "x",
        "LOOM_CP_MINIO_SECRET_KEY": "y",
        "LOOM_CP_LLM_GATEWAY_URL": "http://gw:9100/",
    }.items():
        monkeypatch.setenv(k, v)
    return create_app(ControlPlaneSettings(_env_file=None))


def test_get_task_bundle(app, task_seed):  # type: ignore[no-untyped-def]
    with TestClient(app) as client:
        r = client.get(
            "/tasks/hello/bundle",
            headers={"Authorization": f"Bearer {task_seed}"},
        )
        assert r.status_code == 200, r.text
        body = r.json()
        assert body["id"] == "hello"
        assert body["checksum"] == "deadbeef" * 8
        assert body["config"]["task"]["name"] == "hello"
        assert body["source"] == "git+https://example.com/tasks/hello"


def test_get_task_bundle_404(app, task_seed):  # type: ignore[no-untyped-def]
    with TestClient(app) as client:
        r = client.get(
            "/tasks/nope/bundle",
            headers={"Authorization": f"Bearer {task_seed}"},
        )
        assert r.status_code == 404


def test_get_task_bundle_unauthorized(app, task_seed):  # type: ignore[no-untyped-def]
    with TestClient(app) as client:
        r = client.get("/tasks/hello/bundle")
        assert r.status_code == 401
