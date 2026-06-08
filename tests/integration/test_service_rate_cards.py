"""GET/POST /api/v1/rate-cards forwarders (Plan 20 Task 2)."""

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

from loom.db.schema import Team, TeamQuota, Token
from loom_service.app import create_app
from loom_service.config import LoomServiceSettings


@pytest.fixture
async def rc_setup(
    monkeypatch: pytest.MonkeyPatch, postgres_url: str,
) -> AsyncIterator[tuple[FastAPI, str, str, list[dict[str, str]]]]:
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

    captured: list[dict[str, str]] = []

    def gw_handler(req: httpx.Request) -> httpx.Response:
        captured.append({
            "method": req.method,
            "path": req.url.path,
            "auth": req.headers.get("authorization") or "",
            "body": req.content.decode() if req.content else "",
        })
        if req.method == "GET" and req.url.path == "/admin/rate-cards":
            return httpx.Response(
                200,
                json={"items": [
                    {"id": "rc1", "captured_at": "2026-06-07", "table_hash": "h"},
                ]},
            )
        if req.method == "GET" and req.url.path.startswith("/admin/rate-cards/"):
            rc_id = req.url.path.rsplit("/", 1)[-1]
            return httpx.Response(
                200,
                json={"id": rc_id, "captured_at": "2026-06-07", "table": {}},
            )
        if req.method == "POST" and req.url.path == "/admin/rate-cards":
            return httpx.Response(201, json={"id": "rc-new"})
        return httpx.Response(404)

    app.state.http_client = httpx.AsyncClient(base_url="http://cp")
    app.state.gateway_client = httpx.AsyncClient(
        transport=httpx.MockTransport(gw_handler),
        base_url="http://gw",
    )

    admin_raw = f"loom_admin_{uuid4().hex}"
    team_id = uuid4()
    team_raw = f"loom_team_{uuid4().hex}"
    sync_engine = create_engine(postgres_url)
    sl = sessionmaker(sync_engine)
    with sl() as s:
        s.execute(insert(Team).values(id=team_id, name=f"t-{team_id}"))
        s.execute(insert(Token).values(
            token_hash=hashlib.sha256(admin_raw.encode()).digest(),
            type="admin",
            scopes=["admin:tokens", "admin:rate_cards"],
            team_id=None, issued_at=datetime.now(UTC),
        ))
        s.execute(insert(Token).values(
            token_hash=hashlib.sha256(team_raw.encode()).digest(),
            type="team", scopes=["read:own"], team_id=team_id,
            issued_at=datetime.now(UTC),
        ))
        s.commit()
    try:
        yield app, admin_raw, team_raw, captured
    finally:
        await app.state.gateway_client.aclose()
        await app.state.http_client.aclose()
        await engine.dispose()
        with sl() as s:
            s.execute(delete(Token))
            s.execute(delete(TeamQuota))
            s.execute(delete(Team))
            s.commit()
        sync_engine.dispose()


async def test_admin_can_list(
    rc_setup: tuple[FastAPI, str, str, list[dict[str, str]]],
) -> None:
    app, admin_raw, _team_raw, captured = rc_setup
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://svc",
    ) as ac:
        r = await ac.get(
            "/api/v1/rate-cards",
            headers={"Authorization": f"Bearer {admin_raw}"},
        )
    assert r.status_code == 200, r.text
    assert r.json()["items"][0]["id"] == "rc1"
    assert captured[0]["path"] == "/admin/rate-cards"
    assert captured[0]["auth"] == f"Bearer {admin_raw}"


async def test_admin_can_get_detail(
    rc_setup: tuple[FastAPI, str, str, list[dict[str, str]]],
) -> None:
    app, admin_raw, _team_raw, captured = rc_setup
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://svc",
    ) as ac:
        r = await ac.get(
            "/api/v1/rate-cards/rc-foo",
            headers={"Authorization": f"Bearer {admin_raw}"},
        )
    assert r.status_code == 200
    assert r.json()["id"] == "rc-foo"
    assert captured[0]["path"] == "/admin/rate-cards/rc-foo"


async def test_team_token_403_on_list(
    rc_setup: tuple[FastAPI, str, str, list[dict[str, str]]],
) -> None:
    app, _admin_raw, team_raw, captured = rc_setup
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://svc",
    ) as ac:
        r = await ac.get(
            "/api/v1/rate-cards",
            headers={"Authorization": f"Bearer {team_raw}"},
        )
    assert r.status_code == 403
    # No upstream call was made.
    assert captured == []


async def test_admin_can_post(
    rc_setup: tuple[FastAPI, str, str, list[dict[str, str]]],
) -> None:
    app, admin_raw, _team_raw, captured = rc_setup
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://svc",
    ) as ac:
        r = await ac.post(
            "/api/v1/rate-cards",
            headers={"Authorization": f"Bearer {admin_raw}"},
            json={"id": "rc-new", "table": {"entries": []}},
        )
    assert r.status_code == 201
    assert captured[0]["method"] == "POST"
    assert "rc-new" in captured[0]["body"]


async def test_unauthenticated_401(
    rc_setup: tuple[FastAPI, str, str, list[dict[str, str]]],
) -> None:
    app, _admin_raw, _team_raw, _captured = rc_setup
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://svc",
    ) as ac:
        r = await ac.get("/api/v1/rate-cards")
    assert r.status_code == 401
