"""Usage rollup: aggregates llm_calls JOIN trials by date_trunc
(Plan 20 Task 4)."""

from __future__ import annotations

import hashlib
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from uuid import UUID, uuid4

import boto3
import httpx
import pytest
from botocore.config import Config
from fastapi import FastAPI
from sqlalchemy import create_engine, delete, insert
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.orm import sessionmaker

from loom.db.schema import LlmCall, Task, Team, TeamQuota, Token, Trial
from loom_service.app import create_app
from loom_service.config import LoomServiceSettings


@pytest.fixture
async def usage_setup(
    monkeypatch: pytest.MonkeyPatch, postgres_url: str,
) -> AsyncIterator[tuple[FastAPI, str, str]]:
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
    app.state.http_client = httpx.AsyncClient(base_url="http://cp")
    app.state.gateway_client = httpx.AsyncClient(base_url="http://gw")

    team_id = uuid4()
    raw = f"loom_team_{uuid4().hex}"
    task_id = "local/usage-task"
    sync_engine = create_engine(postgres_url)
    sl = sessionmaker(sync_engine)
    with sl() as s:
        s.execute(insert(Team).values(id=team_id, name=f"t-{team_id}"))
        s.execute(insert(Token).values(
            token_hash=hashlib.sha256(raw.encode()).digest(),
            type="team", scopes=["read:own"], team_id=team_id,
            issued_at=datetime.now(UTC),
        ))
        s.execute(insert(Task).values(
            id=task_id, checksum="x" * 64, config={}, source="local",
        ))
        # Seed three trials across two days: day-1 succeeded + failed,
        # day-2 succeeded. Each has one LLM call.
        base = datetime(2026, 6, 1, 12, tzinfo=UTC)
        trials = [
            (uuid4(), 0, "succeeded"),
            (uuid4(), 0, "failed"),
            (uuid4(), 1, "succeeded"),
        ]
        for tid, day_off, state in trials:
            ts = base + timedelta(days=day_off)
            s.execute(insert(Trial).values(
                id=tid, task_id=task_id, team_id=team_id, state=state,
                config={}, requires_caps={}, submitted_at=ts,
                finished_at=ts,
            ))
            s.execute(insert(LlmCall).values(
                id=uuid4(),
                team_id=team_id,
                trial_id=tid,
                step_id="main",
                model="gpt-4",
                dialect="openai_chat",
                input_tokens=100,
                output_tokens=50,
                provider_extras={},
                cost_usd=Decimal("0.01"),
                rate_card_hash="h",
                captured_at=ts,
            ))
        s.commit()
    try:
        yield app, raw, str(team_id)
    finally:
        await app.state.gateway_client.aclose()
        await app.state.http_client.aclose()
        await engine.dispose()
        with sl() as s:
            s.execute(delete(LlmCall))
            s.execute(delete(Trial))
            s.execute(delete(Token))
            s.execute(delete(Task))
            s.execute(delete(TeamQuota))
            s.execute(delete(Team))
            s.commit()
        sync_engine.dispose()


async def test_usage_groups_by_day(
    usage_setup: tuple[FastAPI, str, str],
) -> None:
    app, raw, team_str = usage_setup
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://svc",
    ) as ac:
        r = await ac.get(
            f"/api/v1/usage?team_id={team_str}"
            f"&start=2026-06-01&end=2026-06-03&group_by=day",
            headers={"Authorization": f"Bearer {raw}"},
        )
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["degraded"] is False
    buckets = body["buckets"]
    assert len(buckets) == 2

    day1 = buckets[0]
    assert day1["trial_count"] == 2
    # Canonical field names introduced after the Plan 20 audit (H1).
    assert day1["trials_currently_succeeded"] == 1
    assert day1["trials_currently_failed"] == 1
    # Legacy aliases remain for the SPA's first-pass migration.
    assert day1["succeeded_count"] == 1
    assert day1["failed_count"] == 1
    assert day1["llm_input_tokens"] == 200
    assert day1["llm_output_tokens"] == 100
    assert day1["total_cost_usd"] == pytest.approx(0.02)

    day2 = buckets[1]
    assert day2["trial_count"] == 1
    assert day2["trials_currently_succeeded"] == 1
    assert day2["trials_currently_failed"] == 0


async def test_usage_groups_by_week(
    usage_setup: tuple[FastAPI, str, str],
) -> None:
    app, raw, team_str = usage_setup
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://svc",
    ) as ac:
        r = await ac.get(
            f"/api/v1/usage?team_id={team_str}"
            f"&start=2026-06-01&end=2026-06-30&group_by=week",
            headers={"Authorization": f"Bearer {raw}"},
        )
    assert r.status_code == 200
    buckets = r.json()["buckets"]
    # All 3 trials fall into the same week (June 1, 2026 is a Monday).
    assert len(buckets) == 1
    assert buckets[0]["trial_count"] == 3
    assert buckets[0]["llm_input_tokens"] == 300


async def test_usage_default_team_for_team_caller(
    usage_setup: tuple[FastAPI, str, str],
) -> None:
    """No team_id query param → scoped to caller's team_id."""
    app, raw, _team_str = usage_setup
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://svc",
    ) as ac:
        r = await ac.get(
            "/api/v1/usage?start=2026-06-01&end=2026-06-03",
            headers={"Authorization": f"Bearer {raw}"},
        )
    assert r.status_code == 200
    assert r.json()["buckets"][0]["trial_count"] == 2


async def test_cross_team_forbidden(
    usage_setup: tuple[FastAPI, str, str],
) -> None:
    app, raw, _team_str = usage_setup
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://svc",
    ) as ac:
        r = await ac.get(
            f"/api/v1/usage?team_id={uuid4()}"
            f"&start=2026-06-01&end=2026-06-03",
            headers={"Authorization": f"Bearer {raw}"},
        )
    assert r.status_code == 403


async def test_invalid_group_by_400(
    usage_setup: tuple[FastAPI, str, str],
) -> None:
    app, raw, _team_str = usage_setup
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://svc",
    ) as ac:
        r = await ac.get(
            "/api/v1/usage?start=2026-06-01&end=2026-06-03&group_by=hour",
            headers={"Authorization": f"Bearer {raw}"},
        )
    assert r.status_code == 400


async def test_end_before_start_400(
    usage_setup: tuple[FastAPI, str, str],
) -> None:
    app, raw, _team_str = usage_setup
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://svc",
    ) as ac:
        r = await ac.get(
            "/api/v1/usage?start=2026-06-10&end=2026-06-01",
            headers={"Authorization": f"Bearer {raw}"},
        )
    assert r.status_code == 400


async def test_degraded_when_llm_calls_missing(
    usage_setup: tuple[FastAPI, str, str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """If the llm_calls table somehow doesn't exist (operator drop,
    pre-Plan-9 schema), the route still 200s with empty buckets +
    degraded=True so the SPA can render a friendly state.

    Monkey-patches `_llm_calls_exists` directly rather than dropping
    the table — restoring the schema mid-session is brittle and
    Plan 9's table is canonical so we shouldn't actually remove it.
    """
    app, raw, team_str = usage_setup

    async def _absent(_session: object) -> bool:
        return False

    monkeypatch.setattr(
        "loom_service.routes.usage._llm_calls_exists", _absent,
    )

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://svc",
    ) as ac:
        r = await ac.get(
            f"/api/v1/usage?team_id={team_str}"
            f"&start=2026-06-01&end=2026-06-03",
            headers={"Authorization": f"Bearer {raw}"},
        )
    assert r.status_code == 200
    assert r.json() == {"buckets": [], "degraded": True}


_ = UUID  # keep import for typing
