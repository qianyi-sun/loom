"""GET /api/v1/health returns 200 via ASGITransport (Plan 17 Task 2).

Bypasses the lifespan and populates `app.state` manually — mirrors the
existing CP / gateway integration test patterns."""

from __future__ import annotations

from collections.abc import AsyncIterator

import boto3
import httpx
import pytest
from botocore.config import Config
from fastapi import FastAPI
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from loom_service.app import create_app
from loom_service.config import LoomServiceSettings


@pytest.fixture
async def service_app(
    monkeypatch: pytest.MonkeyPatch, postgres_url: str,
) -> AsyncIterator[FastAPI]:
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
        base_url=str(settings.control_plane_url), timeout=10.0,
    )
    try:
        yield app
    finally:
        await app.state.http_client.aclose()
        await engine.dispose()


async def test_health_returns_200(service_app: FastAPI) -> None:
    transport = httpx.ASGITransport(app=service_app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://svc",
    ) as ac:
        r = await ac.get("/api/v1/health")
    assert r.status_code == 200
    assert r.json() == {"status": "ok"}
