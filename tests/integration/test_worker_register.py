"""Bug 5 regression: POST /workers/register validates `capabilities`
against the Capabilities Pydantic model (extra=forbid), so garbage like
typo'd OS or non-list payload is rejected at the boundary."""

import hashlib
from collections.abc import Iterator
from datetime import UTC, datetime
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, delete, insert, text
from sqlalchemy.orm import sessionmaker

from loom.db.schema import Token, Worker
from loom_control_plane.app import create_app
from loom_control_plane.config import ControlPlaneSettings


@pytest.fixture
def worker_token(postgres_url: str) -> Iterator[str]:
    engine = create_engine(postgres_url)
    session_factory = sessionmaker(engine)
    raw = f"w_{uuid4().hex}"
    with session_factory() as s:
        s.execute(insert(Token).values(
            token_hash=hashlib.sha256(raw.encode()).digest(),
            type="worker", scopes=["worker:report"], team_id=None,
            issued_at=datetime.now(UTC), expires_at=None,
        ))
        s.commit()
    try:
        yield raw
    finally:
        with session_factory() as s:
            s.execute(delete(Worker))
            s.execute(delete(Token))
            s.commit()
        engine.dispose()


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


_VALID_CAP = {
    "os": "linux",
    "gpu_vendor": "none",
    "network_policies": ["public"],
    "dynamic_network_policy": False,
    "mounted_fs": False,
    "resource_modes": ["auto"],
}


def test_register_with_valid_capabilities(app, worker_token):  # type: ignore[no-untyped-def]
    with TestClient(app) as client:
        r = client.post(
            "/workers/register",
            headers={"Authorization": f"Bearer {worker_token}"},
            json={
                "hostname": "host-1",
                "version": "0.1",
                "capabilities": [_VALID_CAP],
            },
        )
        assert r.status_code == 200, r.text
        body = r.json()
        assert "worker_id" in body
        assert body["heartbeat_interval_sec"] > 0


def test_register_persists_worker_capacity_and_pool(
    app,
    worker_token,
    postgres_url: str,
):  # type: ignore[no-untyped-def]
    with TestClient(app) as client:
        r = client.post(
            "/workers/register",
            headers={"Authorization": f"Bearer {worker_token}"},
            json={
                "hostname": "trt-gb10-7",
                "version": "0.1",
                "capabilities": [_VALID_CAP],
                "max_concurrent": 10,
                "pool_name": "gb10-arm64",
            },
        )
        assert r.status_code == 200, r.text
        worker_id = r.json()["worker_id"]

    engine = create_engine(postgres_url)
    try:
        with engine.connect() as conn:
            row = conn.execute(
                text(
                    "SELECT max_concurrent, pool_name "
                    "FROM workers WHERE id = :worker_id"
                ),
                {"worker_id": worker_id},
            ).one()
    finally:
        engine.dispose()

    assert row[0] == 10
    assert row[1] == "gb10-arm64"


def test_register_rejects_missing_capabilities(app, worker_token):  # type: ignore[no-untyped-def]
    with TestClient(app) as client:
        r = client.post(
            "/workers/register",
            headers={"Authorization": f"Bearer {worker_token}"},
            json={"hostname": "host-2", "version": "0.1"},
        )
        assert r.status_code == 400
        assert "capabilities" in r.json()["detail"]


def test_register_rejects_empty_capabilities(app, worker_token):  # type: ignore[no-untyped-def]
    with TestClient(app) as client:
        r = client.post(
            "/workers/register",
            headers={"Authorization": f"Bearer {worker_token}"},
            json={"hostname": "h", "version": "v", "capabilities": []},
        )
        assert r.status_code == 400


def test_register_rejects_non_list_capabilities(app, worker_token):  # type: ignore[no-untyped-def]
    with TestClient(app) as client:
        r = client.post(
            "/workers/register",
            headers={"Authorization": f"Bearer {worker_token}"},
            json={
                "hostname": "h", "version": "v",
                "capabilities": "linux",
            },
        )
        assert r.status_code == 400


def test_register_rejects_typo_os(app, worker_token):  # type: ignore[no-untyped-def]
    """Bug 5 regression: a typo'd OS value would silently never match any
    DRF claim queries — must be caught at the boundary."""
    bad = dict(_VALID_CAP, os="lunix")
    with TestClient(app) as client:
        r = client.post(
            "/workers/register",
            headers={"Authorization": f"Bearer {worker_token}"},
            json={"hostname": "h", "version": "v", "capabilities": [bad]},
        )
        assert r.status_code == 400
        assert "invalid capabilities" in r.json()["detail"]


def test_register_rejects_extra_keys(app, worker_token):  # type: ignore[no-untyped-def]
    """Bug 5 regression: Capabilities is extra='forbid', so unknown keys
    are caught — protects against future fields drifting silently."""
    bad = dict(_VALID_CAP, hax="yes")
    with TestClient(app) as client:
        r = client.post(
            "/workers/register",
            headers={"Authorization": f"Bearer {worker_token}"},
            json={"hostname": "h", "version": "v", "capabilities": [bad]},
        )
        assert r.status_code == 400
