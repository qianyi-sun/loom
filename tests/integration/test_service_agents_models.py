"""GET /agents and GET /models — the catalogs the SPA's
AgentModelPicker reads (Plan 25)."""

from __future__ import annotations

import hashlib
from collections.abc import AsyncIterator
from datetime import UTC, datetime
from uuid import uuid4

import boto3
import httpx
import pytest
from botocore.config import Config
from fastapi import FastAPI
from sqlalchemy import create_engine, delete, insert
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.orm import sessionmaker

from loom.db.schema import RateCard, Team, TeamQuota, Token
from loom_service.app import create_app
from loom_service.config import LoomServiceSettings


@pytest.fixture
async def setup(
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
    app.state.minio_client = boto3.client(
        "s3", endpoint_url=settings.minio_endpoint,
        aws_access_key_id=settings.minio_access_key.get_secret_value(),
        aws_secret_access_key=settings.minio_secret_key.get_secret_value(),
        region_name=settings.minio_region,
        config=Config(signature_version="s3v4"),
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
        # Two rate cards with overlapping provider+model entries —
        # /models must de-duplicate.
        s.execute(insert(RateCard).values(
            id="card-a", captured_at=datetime.now(UTC),
            table={
                "id": "card-a",
                "entries": [
                    {"provider": "anthropic", "model": "claude-opus-4-7",
                     "input_per_mtok": 15, "output_per_mtok": 75,
                     "cache_read_per_mtok": 0, "cache_write_per_mtok": 0},
                    {"provider": "openai", "model": "gpt-4o",
                     "input_per_mtok": 5, "output_per_mtok": 20,
                     "cache_read_per_mtok": 0, "cache_write_per_mtok": 0},
                ],
            },
        ))
        s.execute(insert(RateCard).values(
            id="card-b", captured_at=datetime.now(UTC),
            table={
                "id": "card-b",
                "entries": [
                    {"provider": "anthropic", "model": "claude-opus-4-7",
                     "input_per_mtok": 10, "output_per_mtok": 50,
                     "cache_read_per_mtok": 0, "cache_write_per_mtok": 0},
                    {"provider": "google", "model": "gemini-2.5-pro",
                     "input_per_mtok": 7, "output_per_mtok": 21,
                     "cache_read_per_mtok": 0, "cache_write_per_mtok": 0},
                ],
            },
        ))
        s.commit()
    try:
        yield app, raw
    finally:
        await app.state.http_client.aclose()
        await engine.dispose()
        with sl() as s:
            s.execute(delete(Token))
            s.execute(delete(RateCard))
            s.execute(delete(TeamQuota))
            s.execute(delete(Team))
            s.commit()
        sync_engine.dispose()


async def test_agents_includes_builtins_and_adapters(
    setup: tuple[FastAPI, str],
) -> None:
    app, raw = setup
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://svc",
    ) as ac:
        r = await ac.get(
            "/api/v1/agents",
            headers={"Authorization": f"Bearer {raw}"},
        )
    assert r.status_code == 200
    names = {a["name"] for a in r.json()["items"]}
    # Builtins.
    assert {"oracle", "litellm", "claude-code-inbox"}.issubset(names)
    # A representative adapter from loom-launcher.
    assert "claude-code" in names
    # Oracle should be flagged needs_model=False; everything else True.
    by_name = {a["name"]: a for a in r.json()["items"]}
    assert by_name["oracle"]["needs_model"] is False
    assert by_name["litellm"]["needs_model"] is True
    assert by_name["claude-code"]["needs_model"] is True
    assert by_name["oracle"]["kind"] == "builtin"
    assert by_name["claude-code"]["kind"] == "adapter"


async def test_models_deduplicates_across_rate_cards(
    setup: tuple[FastAPI, str],
) -> None:
    app, raw = setup
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://svc",
    ) as ac:
        r = await ac.get(
            "/api/v1/models",
            headers={"Authorization": f"Bearer {raw}"},
        )
    assert r.status_code == 200
    items = r.json()["items"]
    pairs = [(m["provider"], m["name"]) for m in items]
    # claude-opus-4-7 appears in both cards but only once here.
    assert pairs == [
        ("anthropic", "claude-opus-4-7"),
        ("google", "gemini-2.5-pro"),
        ("openai", "gpt-4o"),
    ]


async def test_agents_unauthenticated_401(
    setup: tuple[FastAPI, str],
) -> None:
    app, _raw = setup
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://svc",
    ) as ac:
        r = await ac.get("/api/v1/agents")
    assert r.status_code == 401


async def test_models_unauthenticated_401(
    setup: tuple[FastAPI, str],
) -> None:
    app, _raw = setup
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://svc",
    ) as ac:
        r = await ac.get("/api/v1/models")
    assert r.status_code == 401
