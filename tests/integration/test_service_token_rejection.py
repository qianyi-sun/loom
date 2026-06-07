"""Worker tokens cannot use the service layer (Plan 17 Task 4 / spec §4)."""

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

from loom.db.schema import Token
from loom_service.app import create_app
from loom_service.config import LoomServiceSettings


@pytest.fixture
async def svc_with_worker_token(
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
        "s3",
        endpoint_url=settings.minio_endpoint,
        aws_access_key_id=settings.minio_access_key.get_secret_value(),
        aws_secret_access_key=settings.minio_secret_key.get_secret_value(),
        region_name=settings.minio_region,
        config=Config(signature_version="s3v4"),
    )
    app.state.http_client = httpx.AsyncClient(
        base_url=str(settings.control_plane_url),
    )

    raw = f"loom_w_{uuid4().hex}"
    sync_engine = create_engine(postgres_url)
    sl = sessionmaker(sync_engine)
    with sl() as s:
        s.execute(insert(Token).values(
            token_hash=hashlib.sha256(raw.encode()).digest(),
            type="worker",
            scopes=["worker:claim", "worker:report"],
            team_id=None,
            issued_at=datetime.now(UTC),
            expires_at=None,
        ))
        s.commit()
    try:
        yield app, raw
    finally:
        await app.state.http_client.aclose()
        await engine.dispose()
        with sl() as s:
            s.execute(delete(Token))
            s.commit()
        sync_engine.dispose()


async def test_worker_token_403_on_tokens_list(
    svc_with_worker_token: tuple[FastAPI, str],
) -> None:
    app, raw = svc_with_worker_token
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://svc",
    ) as ac:
        r = await ac.get(
            "/api/v1/tokens", headers={"Authorization": f"Bearer {raw}"},
        )
    assert r.status_code == 403
    assert "worker" in r.json()["detail"]


async def test_worker_token_403_on_tokens_post(
    svc_with_worker_token: tuple[FastAPI, str],
) -> None:
    app, raw = svc_with_worker_token
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://svc",
    ) as ac:
        r = await ac.post(
            "/api/v1/tokens",
            headers={"Authorization": f"Bearer {raw}"},
            json={
                "type": "team",
                "scopes": ["read:own"],
                "expires_in_days": 1,
            },
        )
    assert r.status_code == 403


async def test_worker_token_403_on_tokens_delete(
    svc_with_worker_token: tuple[FastAPI, str],
) -> None:
    app, raw = svc_with_worker_token
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://svc",
    ) as ac:
        r = await ac.delete(
            "/api/v1/tokens/deadbeef",
            headers={"Authorization": f"Bearer {raw}"},
        )
    assert r.status_code == 403
