"""Tasks browse (Plan 18 Task 6)."""

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

from loom.db.schema import Benchmark, Task, Team, TeamQuota, Token
from loom_service.app import create_app
from loom_service.config import LoomServiceSettings


@pytest.fixture
async def tasks_setup(
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
        s.execute(insert(Benchmark).values(
            id="humaneval", display_name="HumanEval",
            upstream_kind="huggingface",
            upstream_locator="openai_humaneval",
            upstream_revision="", license_spdx="MIT",
            license_url="https://example/license",
            splits=["test"],
        ))
        # Realistic config so the list route can surface name +
        # description + agent + verifier + step_count from the JSON.
        def _config(name: str, desc: str) -> dict:
            return {
                "task": {"id": name, "name": name, "description": desc},
                "agent": {"name": "oracle"},
                "verifier": {"name": "pytest"},
                "steps": [{"name": "main"}, {"name": "checkpoint"}],
            }
        for i, (tid, lic, bench, name, desc) in enumerate((
            ("humaneval/HumanEval/0", "MIT", "humaneval",
                "Two-sum", "Return indices of two numbers that sum to a target."),
            ("humaneval/HumanEval/1", "MIT", "humaneval",
                "Balanced parens", "Validate a balanced-parens expression."),
            ("local/hand-written", "Apache-2.0", None,
                "Hello world", "Smallest possible Loom task."),
        )):
            s.execute(insert(Task).values(
                id=tid, checksum="x" * 64, config=_config(name, desc),
                source="local", license=lic, benchmark_id=bench,
            ))
            del i
        s.commit()
    try:
        yield app, raw
    finally:
        await app.state.http_client.aclose()
        await engine.dispose()
        with sl() as s:
            s.execute(delete(Token))
            s.execute(delete(Task))
            s.execute(delete(Benchmark))
            s.execute(delete(TeamQuota))
            s.execute(delete(Team))
            s.commit()
        sync_engine.dispose()


async def test_list_tasks(tasks_setup: tuple[FastAPI, str]) -> None:
    app, raw = tasks_setup
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://svc",
    ) as ac:
        r = await ac.get(
            "/api/v1/tasks",
            headers={"Authorization": f"Bearer {raw}"},
        )
    assert r.status_code == 200
    items = r.json()["items"]
    assert len(items) == 3
    # Sorted by id ascending — HumanEval/0 < HumanEval/1 < local/.
    assert items[0]["id"] == "humaneval/HumanEval/0"


async def test_list_surfaces_name_description_agent_verifier_step_count(
    tasks_setup: tuple[FastAPI, str],
) -> None:
    """Plan 24: each list row carries the fields the SPA browse view
    needs to decide whether to submit a trial — name, description,
    agent, verifier, step_count — extracted from config JSON."""
    app, raw = tasks_setup
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://svc",
    ) as ac:
        r = await ac.get(
            "/api/v1/tasks?benchmark_id=humaneval",
            headers={"Authorization": f"Bearer {raw}"},
        )
    items = r.json()["items"]
    first = next(i for i in items if i["id"] == "humaneval/HumanEval/0")
    assert first["name"] == "Two-sum"
    assert first["description"].startswith("Return indices")
    assert first["agent_name"] == "oracle"
    assert first["verifier_name"] == "pytest"
    assert first["step_count"] == 2


async def test_q_substring_matches_task_id(
    tasks_setup: tuple[FastAPI, str],
) -> None:
    """Plan 24: substring search on task id replaces the dropped
    license filter — users locate tasks by typing a fragment."""
    app, raw = tasks_setup
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://svc",
    ) as ac:
        r = await ac.get(
            "/api/v1/tasks?q=hand",
            headers={"Authorization": f"Bearer {raw}"},
        )
    items = r.json()["items"]
    assert len(items) == 1
    assert items[0]["id"] == "local/hand-written"


async def test_q_is_case_insensitive(
    tasks_setup: tuple[FastAPI, str],
) -> None:
    app, raw = tasks_setup
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://svc",
    ) as ac:
        r = await ac.get(
            "/api/v1/tasks?q=HUMANEVAL",
            headers={"Authorization": f"Bearer {raw}"},
        )
    items = r.json()["items"]
    assert len(items) == 2


async def test_license_query_param_no_longer_filters(
    tasks_setup: tuple[FastAPI, str],
) -> None:
    """Plan 24: the legacy `license=` filter is gone. FastAPI silently
    ignores unknown query params, so the request still 200s — assert
    every row comes back rather than just MIT rows."""
    app, raw = tasks_setup
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://svc",
    ) as ac:
        r = await ac.get(
            "/api/v1/tasks?license=MIT",
            headers={"Authorization": f"Bearer {raw}"},
        )
    assert r.status_code == 200
    # All three tasks come back — the license param is now a no-op.
    assert len(r.json()["items"]) == 3


async def test_filter_by_benchmark_id(tasks_setup: tuple[FastAPI, str]) -> None:
    app, raw = tasks_setup
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://svc",
    ) as ac:
        r = await ac.get(
            "/api/v1/tasks?benchmark_id=humaneval",
            headers={"Authorization": f"Bearer {raw}"},
        )
    items = r.json()["items"]
    assert len(items) == 2
    assert all(it["benchmark_id"] == "humaneval" for it in items)


async def test_pagination(tasks_setup: tuple[FastAPI, str]) -> None:
    app, raw = tasks_setup
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://svc",
    ) as ac:
        r1 = await ac.get(
            "/api/v1/tasks?limit=2",
            headers={"Authorization": f"Bearer {raw}"},
        )
        j1 = r1.json()
        assert len(j1["items"]) == 2
        assert j1["next_cursor"] is not None

        r2 = await ac.get(
            f"/api/v1/tasks?limit=2&cursor={j1['next_cursor']}",
            headers={"Authorization": f"Bearer {raw}"},
        )
    j2 = r2.json()
    assert len(j2["items"]) == 1
    assert j2["next_cursor"] is None


async def test_get_task_detail(tasks_setup: tuple[FastAPI, str]) -> None:
    app, raw = tasks_setup
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://svc",
    ) as ac:
        r = await ac.get(
            "/api/v1/tasks/humaneval/HumanEval/0",
            headers={"Authorization": f"Bearer {raw}"},
        )
    assert r.status_code == 200
    body = r.json()
    assert body["id"] == "humaneval/HumanEval/0"
    assert body["license"] == "MIT"
    assert body["benchmark_id"] == "humaneval"
    assert "config" in body


async def test_get_task_not_found(tasks_setup: tuple[FastAPI, str]) -> None:
    app, raw = tasks_setup
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://svc",
    ) as ac:
        r = await ac.get(
            "/api/v1/tasks/no/such/task",
            headers={"Authorization": f"Bearer {raw}"},
        )
    assert r.status_code == 404


async def test_unauthenticated_401(tasks_setup: tuple[FastAPI, str]) -> None:
    app, _raw = tasks_setup
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://svc",
    ) as ac:
        r = await ac.get("/api/v1/tasks")
    assert r.status_code == 401
