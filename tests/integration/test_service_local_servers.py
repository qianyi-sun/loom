"""GET /api/v1/local-servers — operator-configured local LLM servers.

Exercises the env-var → JSON-parsed → API-response path so a
mis-edited env doesn't silently degrade to an empty catalog.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import AsyncIterator
from datetime import UTC, datetime
from uuid import uuid4

import httpx
import pytest
from fastapi import FastAPI
from sqlalchemy import create_engine, delete, insert
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.orm import sessionmaker

from loom.db.schema import Team, TeamQuota, Token
from loom_service.app import create_app
from loom_service.config import LoomServiceSettings


@pytest.fixture
async def local_servers_setup(
    monkeypatch: pytest.MonkeyPatch, postgres_url: str,
) -> AsyncIterator[tuple[FastAPI, str]]:
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
    app.state.http_client = httpx.AsyncClient(
        base_url=str(settings.control_plane_url),
    )
    team_id = uuid4()
    raw = f"loom_team_{uuid4().hex}"
    sync_engine = create_engine(postgres_url)
    sl = sessionmaker(sync_engine)
    with sl() as s:
        s.execute(insert(Team).values(id=team_id, name=f"t-{team_id}"))
        s.execute(insert(Token).values(
            token_hash=hashlib.sha256(raw.encode()).digest(),
            type="team", scopes=["read:own"], team_id=team_id,
            issued_at=datetime.now(UTC),
        ))
        s.commit()
    try:
        yield app, raw
    finally:
        await app.state.http_client.aclose()
        await engine.dispose()
        with sl() as s:
            s.execute(delete(Token))
            s.execute(delete(TeamQuota))
            s.execute(delete(Team))
            s.commit()
        sync_engine.dispose()


async def test_empty_default_returns_no_items(
    local_servers_setup: tuple[FastAPI, str],
) -> None:
    """No env var set → empty catalog, not a 500."""
    app, raw = local_servers_setup
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://svc",
    ) as ac:
        r = await ac.get(
            "/api/v1/local-servers",
            headers={"Authorization": f"Bearer {raw}"},
        )
    assert r.status_code == 200
    assert r.json() == {"items": []}


async def test_configured_servers_surface(
    local_servers_setup: tuple[FastAPI, str],
) -> None:
    app, raw = local_servers_setup
    app.state.settings = app.state.settings.model_copy(update={
        "local_servers_json": json.dumps({
            "vllm-h100": {
                "base_url": "http://vllm:8000/v1",
                "kind": "vllm",
                "description": "Prod model pool",
            },
            "ollama-dev": {
                "base_url": "http://ollama:11434/v1",
                "kind": "ollama",
            },
        }),
    })
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://svc",
    ) as ac:
        r = await ac.get(
            "/api/v1/local-servers",
            headers={"Authorization": f"Bearer {raw}"},
        )
    body = r.json()
    assert r.status_code == 200
    # Sorted by name.
    assert [it["name"] for it in body["items"]] == [
        "ollama-dev", "vllm-h100",
    ]
    assert body["items"][1]["base_url"] == "http://vllm:8000/v1"
    assert body["items"][1]["description"] == "Prod model pool"
    assert body["items"][0]["description"] is None


async def test_malformed_json_returns_500(
    local_servers_setup: tuple[FastAPI, str],
) -> None:
    app, raw = local_servers_setup
    app.state.settings = app.state.settings.model_copy(update={
        "local_servers_json": "{not json",
    })
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://svc",
    ) as ac:
        r = await ac.get(
            "/api/v1/local-servers",
            headers={"Authorization": f"Bearer {raw}"},
        )
    assert r.status_code == 500
    assert "not valid JSON" in r.json()["detail"]


async def test_entry_without_base_url_silently_skipped(
    local_servers_setup: tuple[FastAPI, str],
) -> None:
    """One malformed entry shouldn't mask the others."""
    app, raw = local_servers_setup
    app.state.settings = app.state.settings.model_copy(update={
        "local_servers_json": json.dumps({
            "broken": {"description": "missing base_url"},
            "good": {"base_url": "http://x:8000/v1"},
        }),
    })
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://svc",
    ) as ac:
        r = await ac.get(
            "/api/v1/local-servers",
            headers={"Authorization": f"Bearer {raw}"},
        )
    body = r.json()
    assert [it["name"] for it in body["items"]] == ["good"]


async def test_unauthenticated_401(
    local_servers_setup: tuple[FastAPI, str],
) -> None:
    app, _ = local_servers_setup
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://svc",
    ) as ac:
        r = await ac.get("/api/v1/local-servers")
    assert r.status_code == 401
