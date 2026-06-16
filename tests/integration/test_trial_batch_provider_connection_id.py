"""Trial + Batch submission with optional provider_connection_id /
provider_model_id (cluster-deploy.md §Schema additions consumer wiring).

Validates:
- Trial POST validates provider_connection_id against the caller's team
  BEFORE forwarding to the Control Plane (saves a round-trip on bad
  input).
- Batch POST does the same validation in-process (no CP forward).
- NULL provider_connection_id is the back-compat path (existing trial
  submission code that doesn't pass the new fields keeps working).
- Cross-team / nonexistent / soft-deleted refs → 400 with a clear
  error message.
"""

from __future__ import annotations

import hashlib
from collections.abc import AsyncIterator
from datetime import UTC, datetime
from uuid import UUID, uuid4

import boto3
import httpx
import pytest
from botocore.config import Config
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, delete, insert
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.orm import sessionmaker

from loom.db.schema import (
    Batch,
    ProviderConnection,
    Secret,
    Task,
    Team,
    TeamQuota,
    Token,
    Trial,
    Worker,
)
from loom_service.app import create_app
from loom_service.config import LoomServiceSettings


@pytest.fixture
async def app_setup(
    monkeypatch: pytest.MonkeyPatch, postgres_url: str,
) -> AsyncIterator[tuple[FastAPI, dict[str, str], dict[str, UUID]]]:
    """Boot the app + seed two teams (A, B), each with a token + a
    provider_connection. Returns (app, tokens, ids) where ids holds
    team_a / team_b / conn_a / conn_b / conn_a_deleted (the
    soft-deleted one used by the rejection test) / task UUIDs."""
    for k, v in {
        "LOOM_SVC_DB_URL": postgres_url,
        "LOOM_SVC_MINIO_ENDPOINT": "http://minio:9000",
        "LOOM_SVC_MINIO_ACCESS_KEY": "x",
        "LOOM_SVC_MINIO_SECRET_KEY": "y",
        "LOOM_SVC_CONTROL_PLANE_URL": "http://cp:8080/",
        "LOOM_SVC_GATEWAY_URL": "http://gw:9100/",
    }.items():
        monkeypatch.setenv(k, v)
    settings = LoomServiceSettings(_env_file=None)
    app = create_app(settings)
    engine = create_async_engine(str(settings.db_url))
    app.state.settings = settings
    app.state.session_factory = async_sessionmaker(
        engine, expire_on_commit=False,
    )
    app.state.minio_client = boto3.client(
        "s3",
        endpoint_url=settings.minio_endpoint,
        aws_access_key_id=settings.minio_access_key.get_secret_value(),
        aws_secret_access_key=settings.minio_secret_key.get_secret_value(),
        region_name=settings.minio_region,
        config=Config(signature_version="s3v4"),
    )

    # Mock control-plane HTTP client so trial submission's forward
    # path returns a stub instead of hitting a real CP.
    def cp_handler(req: httpx.Request) -> httpx.Response:
        if req.url.path == "/trials" and req.method == "POST":
            return httpx.Response(
                201, json={
                    "trial_id": "00000000-0000-0000-0000-000000000099",
                    "state": "queued",
                },
            )
        return httpx.Response(404)

    app.state.http_client = httpx.AsyncClient(
        transport=httpx.MockTransport(cp_handler),
        base_url="http://cp",
    )
    app.state.gateway_client = httpx.AsyncClient(base_url="http://gw")

    # Seed.
    team_a = uuid4()
    team_b = uuid4()
    raw_a = f"loom_team_{uuid4().hex}"
    raw_b = f"loom_team_{uuid4().hex}"
    task_id = f"task-{uuid4().hex[:8]}"
    conn_a = uuid4()
    conn_a_deleted = uuid4()
    conn_b = uuid4()
    sync_engine = create_engine(postgres_url)
    sl = sessionmaker(sync_engine)
    with sl() as s:
        for tid, name in ((team_a, "team-a"), (team_b, "team-b")):
            s.execute(insert(Team).values(id=tid, name=f"{name}-{tid}"))
            s.execute(insert(TeamQuota).values(team_id=tid))
        s.execute(insert(Token).values(
            token_hash=hashlib.sha256(raw_a.encode()).digest(),
            type="team", scopes=["submit"], team_id=team_a,
            issued_at=datetime.now(UTC),
        ))
        s.execute(insert(Token).values(
            token_hash=hashlib.sha256(raw_b.encode()).digest(),
            type="team", scopes=["submit"], team_id=team_b,
            issued_at=datetime.now(UTC),
        ))
        s.execute(insert(Task).values(
            id=task_id, checksum="x" * 64, config={}, source="local",
        ))
        for cid, t, name in (
            (conn_a, team_a, "active-a"),
            (conn_b, team_b, "active-b"),
            (conn_a_deleted, team_a, "deleted-a"),
        ):
            s.execute(insert(ProviderConnection).values(
                id=cid, team_id=t, provider_type="openai-compatible",
                display_name=name, base_url="https://api.openai.com/v1",
                upstream_host="api.openai.com",
                encrypted_api_key_ref=f"loom://team:{t}/{cid}",
                created_by="admin:0",
            ))
        # Soft-delete one of team_a's connections.
        s.execute(
            ProviderConnection.__table__.update()
            .where(ProviderConnection.id == conn_a_deleted)
            .values(deleted_at=datetime.now(UTC)),
        )
        # Live worker advertising every backend Loom ships drivers for —
        # required by the POST /batches reject-when-no-worker check.
        s.execute(insert(Worker).values(
            id=uuid4(), hostname="fixture-worker", version="test",
            capabilities=[
                {"backend": "docker"}, {"backend": "fake"},
                {"backend": "daytona"}, {"backend": "modal"},
            ],
            registered_at=datetime.now(UTC),
            last_seen_at=datetime.now(UTC),
            status="active",
        ))
        s.commit()

    tokens = {"a": raw_a, "b": raw_b}
    ids = {
        "team_a": team_a, "team_b": team_b,
        "conn_a": conn_a, "conn_a_deleted": conn_a_deleted, "conn_b": conn_b,
        "task_id": task_id,
    }
    try:
        yield app, tokens, ids
    finally:
        await app.state.gateway_client.aclose()
        await app.state.http_client.aclose()
        await engine.dispose()
        with sl() as s:
            s.execute(delete(Trial))
            s.execute(delete(Batch))
            s.execute(delete(ProviderConnection))
            s.execute(delete(Secret))
            s.execute(delete(Token))
            s.execute(delete(Task))
            s.execute(delete(Worker))
            s.execute(delete(TeamQuota))
            s.execute(delete(Team))
            s.commit()
        sync_engine.dispose()


def _client(app: FastAPI) -> TestClient:
    return TestClient(app)


def _auth(token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}"}


# ──────────────────────────────────────────────────────────────────────
# Trial submission
# ──────────────────────────────────────────────────────────────────────


def test_trial_submit_with_valid_provider_succeeds(app_setup) -> None:
    """Valid provider_connection_id from caller's team passes
    validation + flows through to the (mocked) CP."""
    app, tokens, ids = app_setup
    c = _client(app)
    r = c.post(
        "/api/v1/trials",
        headers=_auth(tokens["a"]),
        json={
            "task_id": ids["task_id"],
            "config": {"agent_name": "oracle"},
            "provider_connection_id": str(ids["conn_a"]),
            "provider_model_id": "gpt-4o",
        },
    )
    assert r.status_code == 201, r.text
    assert r.json()["trial_id"] == "00000000-0000-0000-0000-000000000099"


def test_trial_submit_without_provider_succeeds(app_setup) -> None:
    """Back-compat: existing submission code that doesn't send the new
    fields keeps working."""
    app, tokens, ids = app_setup
    c = _client(app)
    r = c.post(
        "/api/v1/trials",
        headers=_auth(tokens["a"]),
        json={"task_id": ids["task_id"], "config": {"agent_name": "oracle"}},
    )
    assert r.status_code == 201


def test_trial_submit_with_cross_team_provider_returns_400(app_setup) -> None:
    """team_a submits with team_b's connection_id → 400 with a clear
    message (NOT 404 — the id was provided deliberately)."""
    app, tokens, ids = app_setup
    c = _client(app)
    r = c.post(
        "/api/v1/trials",
        headers=_auth(tokens["a"]),
        json={
            "task_id": ids["task_id"],
            "config": {"agent_name": "oracle"},
            "provider_connection_id": str(ids["conn_b"]),
        },
    )
    assert r.status_code == 400
    assert "different team" in r.json()["detail"]


def test_trial_submit_with_nonexistent_provider_returns_400(app_setup) -> None:
    app, tokens, ids = app_setup
    c = _client(app)
    r = c.post(
        "/api/v1/trials",
        headers=_auth(tokens["a"]),
        json={
            "task_id": ids["task_id"],
            "config": {"agent_name": "oracle"},
            "provider_connection_id": str(uuid4()),
        },
    )
    assert r.status_code == 400
    assert "not found" in r.json()["detail"]
    assert "loom providers list" in r.json()["detail"]


def test_trial_submit_with_deleted_provider_returns_400(app_setup) -> None:
    app, tokens, ids = app_setup
    c = _client(app)
    r = c.post(
        "/api/v1/trials",
        headers=_auth(tokens["a"]),
        json={
            "task_id": ids["task_id"],
            "config": {"agent_name": "oracle"},
            "provider_connection_id": str(ids["conn_a_deleted"]),
        },
    )
    assert r.status_code == 400
    assert "has been deleted" in r.json()["detail"]


# ──────────────────────────────────────────────────────────────────────
# Batch creation
# ──────────────────────────────────────────────────────────────────────


def test_batch_create_with_valid_provider_succeeds(app_setup) -> None:
    app, tokens, ids = app_setup
    c = _client(app)
    r = c.post(
        "/api/v1/batches",
        headers=_auth(tokens["a"]),
        json={
            "name": "batch-with-provider",
            "task_filter": {"task_ids": [ids["task_id"]]},
            "trial_config": {"agent_name": "oracle"},
            "provider_connection_id": str(ids["conn_a"]),
            "provider_model_id": "gpt-4o",
        },
    )
    assert r.status_code == 201, r.text
    body = r.json()
    batch_id = body["batch_id"]

    # Verify the row got the fields persisted.
    sync_engine = create_engine(str(app.state.settings.db_url))
    sl = sessionmaker(sync_engine)
    with sl() as s:
        row = s.execute(
            Batch.__table__.select().where(Batch.id == UUID(batch_id)),
        ).one()
    sync_engine.dispose()
    assert row.provider_connection_id == ids["conn_a"]
    assert row.provider_model_id == "gpt-4o"


def test_batch_create_without_provider_succeeds(app_setup) -> None:
    app, tokens, ids = app_setup
    c = _client(app)
    r = c.post(
        "/api/v1/batches",
        headers=_auth(tokens["a"]),
        json={
            "name": "no-provider",
            "task_filter": {"task_ids": [ids["task_id"]]},
            "trial_config": {"agent_name": "oracle"},
        },
    )
    assert r.status_code == 201


def test_batch_create_with_cross_team_provider_returns_400(app_setup) -> None:
    app, tokens, ids = app_setup
    c = _client(app)
    r = c.post(
        "/api/v1/batches",
        headers=_auth(tokens["a"]),
        json={
            "name": "cross-team",
            "task_filter": {"task_ids": [ids["task_id"]]},
            "trial_config": {"agent_name": "oracle"},
            "provider_connection_id": str(ids["conn_b"]),
        },
    )
    assert r.status_code == 400
    assert "different team" in r.json()["detail"]


def test_batch_create_with_nonexistent_provider_returns_400(app_setup) -> None:
    app, tokens, ids = app_setup
    c = _client(app)
    r = c.post(
        "/api/v1/batches",
        headers=_auth(tokens["a"]),
        json={
            "name": "nope",
            "task_filter": {"task_ids": [ids["task_id"]]},
            "trial_config": {"agent_name": "oracle"},
            "provider_connection_id": str(uuid4()),
        },
    )
    assert r.status_code == 400
    assert "not found" in r.json()["detail"]


def test_batch_create_with_deleted_provider_returns_400(app_setup) -> None:
    app, tokens, ids = app_setup
    c = _client(app)
    r = c.post(
        "/api/v1/batches",
        headers=_auth(tokens["a"]),
        json={
            "name": "deleted",
            "task_filter": {"task_ids": [ids["task_id"]]},
            "trial_config": {"agent_name": "oracle"},
            "provider_connection_id": str(ids["conn_a_deleted"]),
        },
    )
    assert r.status_code == 400
    assert "has been deleted" in r.json()["detail"]
