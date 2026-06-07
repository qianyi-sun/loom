"""Benchmarks browse (Plan 18 Task 7)."""

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

from loom.db.schema import Benchmark, Team, TeamQuota, Token
from loom_service.app import create_app
from loom_service.config import LoomServiceSettings


@pytest.fixture
async def benchmarks_setup(
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
        for bid, dn, lic in (
            ("aime", "AIME", "proprietary-MAA"),
            ("humaneval", "HumanEval", "MIT"),
            ("mbpp", "MBPP", "CC-BY-4.0"),
        ):
            s.execute(insert(Benchmark).values(
                id=bid, display_name=dn, upstream_kind="huggingface",
                upstream_locator=f"upstream/{bid}", upstream_revision="",
                license_spdx=lic, license_url=f"https://example/{bid}",
                splits=["test"],
            ))
        s.commit()
    try:
        yield app, raw
    finally:
        await app.state.http_client.aclose()
        await engine.dispose()
        with sl() as s:
            s.execute(delete(Token))
            s.execute(delete(Benchmark))
            s.execute(delete(TeamQuota))
            s.execute(delete(Team))
            s.commit()
        sync_engine.dispose()


async def test_list_benchmarks(
    benchmarks_setup: tuple[FastAPI, str],
) -> None:
    app, raw = benchmarks_setup
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://svc",
    ) as ac:
        r = await ac.get(
            "/api/v1/benchmarks",
            headers={"Authorization": f"Bearer {raw}"},
        )
    assert r.status_code == 200
    items = r.json()["items"]
    assert len(items) == 3
    # Sorted by display_name: AIME < HumanEval < MBPP
    assert [it["display_name"] for it in items] == [
        "AIME", "HumanEval", "MBPP",
    ]


async def test_pagination(
    benchmarks_setup: tuple[FastAPI, str],
) -> None:
    app, raw = benchmarks_setup
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://svc",
    ) as ac:
        r1 = await ac.get(
            "/api/v1/benchmarks?limit=2",
            headers={"Authorization": f"Bearer {raw}"},
        )
        j1 = r1.json()
        assert len(j1["items"]) == 2
        assert j1["next_cursor"] == "HumanEval"
        r2 = await ac.get(
            f"/api/v1/benchmarks?limit=2&cursor={j1['next_cursor']}",
            headers={"Authorization": f"Bearer {raw}"},
        )
    j2 = r2.json()
    assert len(j2["items"]) == 1
    assert j2["items"][0]["id"] == "mbpp"
    assert j2["next_cursor"] is None


async def test_get_benchmark_detail(
    benchmarks_setup: tuple[FastAPI, str],
) -> None:
    app, raw = benchmarks_setup
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://svc",
    ) as ac:
        r = await ac.get(
            "/api/v1/benchmarks/humaneval",
            headers={"Authorization": f"Bearer {raw}"},
        )
    assert r.status_code == 200
    body = r.json()
    assert body["id"] == "humaneval"
    assert body["license_spdx"] == "MIT"
    assert body["upstream_kind"] == "huggingface"
    assert "imported_at" in body


async def test_get_benchmark_not_found(
    benchmarks_setup: tuple[FastAPI, str],
) -> None:
    app, raw = benchmarks_setup
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://svc",
    ) as ac:
        r = await ac.get(
            "/api/v1/benchmarks/no-such-bench",
            headers={"Authorization": f"Bearer {raw}"},
        )
    assert r.status_code == 404


async def test_unauthenticated_401(
    benchmarks_setup: tuple[FastAPI, str],
) -> None:
    app, _raw = benchmarks_setup
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://svc",
    ) as ac:
        r = await ac.get("/api/v1/benchmarks")
    assert r.status_code == 401
