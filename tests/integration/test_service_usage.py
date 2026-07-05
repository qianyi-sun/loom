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

from loom.admin_secret import AdminSecretVerifier
from loom.db.schema import (
    Batch,
    Benchmark,
    LlmCall,
    ProviderConnection,
    Task,
    Team,
    TeamQuota,
    Token,
    Trial,
    User,
)
from loom_service.app import create_app
from loom_service.config import LoomServiceSettings

RAW_ADMIN_TOKEN = "loom_admin_" + "U" * 43


@pytest.fixture
async def usage_setup(
    monkeypatch: pytest.MonkeyPatch,
    postgres_url: str,
) -> AsyncIterator[tuple[FastAPI, str, str, str, str]]:
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
        engine,
        expire_on_commit=False,
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
    app.state.admin_secret_verifier = AdminSecretVerifier.from_token(
        RAW_ADMIN_TOKEN,
    )

    team_id = uuid4()
    raw = f"loom_team_{uuid4().hex}"
    task_id = f"local/usage-task-{uuid4().hex[:8]}"
    batch_id = uuid4()
    sync_engine = create_engine(postgres_url)
    sl = sessionmaker(sync_engine)
    with sl() as s:
        s.execute(insert(Team).values(id=team_id, name=f"t-{team_id}"))
        s.execute(
            insert(Token).values(
                token_hash=hashlib.sha256(raw.encode()).digest(),
                type="team",
                scopes=["read:own"],
                team_id=team_id,
                issued_at=datetime.now(UTC),
            )
        )
        s.execute(
            insert(Task).values(
                id=task_id,
                checksum="x" * 64,
                config={},
                source="local",
            )
        )
        s.execute(
            insert(Batch).values(
                id=batch_id,
                team_id=team_id,
                name="priced usage batch",
                task_filter={"task_ids": [task_id], "subset_kind": "explicit"},
                trial_config={},
                state="finished",
                created_by_token_prefix="usage123",
                expected_trial_count=3,
            )
        )
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
            s.execute(
                insert(Trial).values(
                    id=tid,
                    task_id=task_id,
                    team_id=team_id,
                    state=state,
                    config={},
                    requires_caps={},
                    submitted_at=ts,
                    finished_at=ts,
                    batch_id=batch_id,
                    result=({"aggregate_reward": 1.0} if state == "succeeded" else None),
                )
            )
            s.execute(
                insert(LlmCall).values(
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
                )
            )
        s.commit()
    try:
        yield app, raw, RAW_ADMIN_TOKEN, str(team_id), task_id
    finally:
        await app.state.gateway_client.aclose()
        await app.state.http_client.aclose()
        await engine.dispose()
        with sl() as s:
            s.execute(delete(LlmCall))
            s.execute(delete(Trial))
            s.execute(delete(Batch))
            s.execute(delete(ProviderConnection))
            s.execute(delete(Token))
            s.execute(delete(Task))
            s.execute(delete(Benchmark))
            s.execute(delete(TeamQuota))
            s.execute(delete(Team))
            s.execute(delete(User))
            s.commit()
        sync_engine.dispose()


async def test_usage_groups_by_day(
    usage_setup: tuple[FastAPI, str, str, str, str],
) -> None:
    app, raw, _admin_raw, team_str, _task_id = usage_setup
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://svc",
    ) as ac:
        r = await ac.get(
            f"/api/v1/usage?team_id={team_str}&start=2026-06-01&end=2026-06-03&group_by=day",
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


async def test_usage_include_batches_distinguishes_token_only_cost(
    usage_setup: tuple[FastAPI, str, str, str, str],
    postgres_url: str,
) -> None:
    app, raw, _admin_raw, team_str, task_id = usage_setup
    team_id = UUID(team_str)
    batch_id = uuid4()
    trial_id = uuid4()
    ts = datetime(2026, 6, 2, 13, tzinfo=UTC)
    sync_engine = create_engine(postgres_url)
    with sync_engine.begin() as conn:
        conn.execute(
            insert(Batch).values(
                id=batch_id,
                team_id=team_id,
                name="self-deployed token-only batch",
                task_filter={"task_ids": [task_id], "subset_kind": "explicit"},
                trial_config={},
                state="finished",
                created_by_token_prefix="selfhost",
                expected_trial_count=1,
            )
        )
        conn.execute(
            insert(Trial).values(
                id=trial_id,
                task_id=task_id,
                team_id=team_id,
                state="failed",
                config={},
                requires_caps={},
                submitted_at=ts,
                finished_at=ts,
                batch_id=batch_id,
            )
        )
        conn.execute(
            insert(LlmCall).values(
                id=uuid4(),
                team_id=team_id,
                trial_id=trial_id,
                step_id="main",
                model="qwen3.6-35b-a3b",
                dialect="openai_responses",
                input_tokens=77,
                output_tokens=11,
                provider_extras={
                    "_loom_usage_status": "partial",
                    "_loom_missing_usage_keys": ["output_tokens"],
                    "_loom_provider_usage": {"input_tokens": 77},
                },
                cost_usd=Decimal("0"),
                rate_card_hash="facade:tokens-only",
                captured_at=ts,
            )
        )
    sync_engine.dispose()

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://svc",
    ) as ac:
        r = await ac.get(
            f"/api/v1/usage?team_id={team_str}"
            f"&start=2026-06-02&end=2026-06-02"
            f"&group_by=day&include_batches=true",
            headers={"Authorization": f"Bearer {raw}"},
        )
    assert r.status_code == 200, r.text
    bucket = r.json()["buckets"][0]
    assert bucket["cost_status"] == "mixed"
    assert bucket["pricing_modes"] == ["priced", "tokens-only"]
    by_name = {item["batch_name"]: item for item in bucket["batches"]}
    token_only = by_name["self-deployed token-only batch"]
    assert token_only["llm_input_tokens"] == 77
    assert token_only["llm_output_tokens"] == 11
    assert token_only["estimated_cost_usd"] is None
    assert token_only["cost_currency"] is None
    assert token_only["cost_status"] == "not_applicable"
    assert token_only["pricing_modes"] == ["tokens-only"]
    priced = by_name["priced usage batch"]
    assert priced["estimated_cost_usd"] == pytest.approx(0.01)
    assert priced["cost_status"] == "estimated"


async def test_usage_batch_family_includes_linked_supplemental_batches(
    usage_setup: tuple[FastAPI, str, str, str, str],
    postgres_url: str,
) -> None:
    app, raw, _admin_raw, team_str, task_id = usage_setup
    team_id = UUID(team_str)
    main_batch_id = uuid4()
    supplemental_batch_id = uuid4()
    main_trial_id = uuid4()
    supplemental_trial_id = uuid4()
    ts = datetime(2026, 6, 2, 14, tzinfo=UTC)

    sync_engine = create_engine(postgres_url)
    with sync_engine.begin() as conn:
        conn.execute(
            insert(Batch),
            [
                {
                    "id": main_batch_id,
                    "team_id": team_id,
                    "name": "main usage family batch",
                    "task_filter": {"task_ids": [task_id], "subset_kind": "explicit"},
                    "trial_config": {},
                    "state": "finished",
                    "created_by_token_prefix": "usagefam",
                    "expected_trial_count": 1,
                    "rerun_of_batch_id": None,
                },
                {
                    "id": supplemental_batch_id,
                    "team_id": team_id,
                    "name": "supplemental usage family batch",
                    "task_filter": {"task_ids": [task_id], "subset_kind": "explicit"},
                    "trial_config": {},
                    "state": "finished",
                    "created_by_token_prefix": "usagefam",
                    "expected_trial_count": 1,
                    "rerun_of_batch_id": main_batch_id,
                },
            ],
        )
        conn.execute(
            insert(Trial),
            [
                {
                    "id": main_trial_id,
                    "task_id": task_id,
                    "team_id": team_id,
                    "state": "failed",
                    "config": {},
                    "requires_caps": {},
                    "submitted_at": ts,
                    "finished_at": ts,
                    "batch_id": main_batch_id,
                    "result": None,
                },
                {
                    "id": supplemental_trial_id,
                    "task_id": task_id,
                    "team_id": team_id,
                    "state": "succeeded",
                    "config": {},
                    "requires_caps": {},
                    "submitted_at": ts,
                    "finished_at": ts,
                    "batch_id": supplemental_batch_id,
                    "result": {"aggregate_reward": 1.0},
                },
            ],
        )
        conn.execute(
            insert(LlmCall),
            [
                {
                    "id": uuid4(),
                    "team_id": team_id,
                    "trial_id": main_trial_id,
                    "step_id": "main",
                    "model": "glm-5.1-thinking",
                    "dialect": "openai_facade",
                    "input_tokens": 100,
                    "output_tokens": 10,
                    "provider_extras": {},
                    "cost_usd": Decimal("0.10"),
                    "rate_card_hash": "h",
                    "captured_at": ts,
                },
                {
                    "id": uuid4(),
                    "team_id": team_id,
                    "trial_id": supplemental_trial_id,
                    "step_id": "main",
                    "model": "glm-5.1-thinking",
                    "dialect": "openai_facade",
                    "input_tokens": 200,
                    "output_tokens": 20,
                    "provider_extras": {},
                    "cost_usd": Decimal("0.20"),
                    "rate_card_hash": "h",
                    "captured_at": ts,
                },
            ],
        )
    sync_engine.dispose()

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://svc") as ac:
        r = await ac.get(
            "/api/v1/usage",
            headers={"Authorization": f"Bearer {raw}"},
            params={
                "team_id": team_str,
                "start": "2026-06-02",
                "end": "2026-06-02",
                "group_by": "day",
                "batch_id": str(main_batch_id),
                "include_batch_family": "true",
                "include_batches": "true",
            },
        )

    assert r.status_code == 200, r.text
    body = r.json()
    assert len(body["buckets"]) == 1
    bucket = body["buckets"][0]
    assert bucket["llm_input_tokens"] == 300
    assert bucket["llm_output_tokens"] == 30
    assert bucket["estimated_cost_usd"] == pytest.approx(0.30)
    assert {item["batch_id"] for item in bucket["batches"]} == {
        str(main_batch_id),
        str(supplemental_batch_id),
    }


async def test_usage_failed_upstream_audit_rows_are_not_priced(
    usage_setup: tuple[FastAPI, str, str, str, str],
    postgres_url: str,
) -> None:
    app, raw, _admin_raw, team_str, task_id = usage_setup
    team_id = UUID(team_str)
    batch_id = uuid4()
    trial_id = uuid4()
    ts = datetime(2026, 6, 2, 15, tzinfo=UTC)
    sync_engine = create_engine(postgres_url)
    with sync_engine.begin() as conn:
        conn.execute(
            insert(Batch).values(
                id=batch_id,
                team_id=team_id,
                name="failed upstream audit batch",
                task_filter={"task_ids": [task_id], "subset_kind": "explicit"},
                trial_config={},
                state="finished",
                created_by_token_prefix="failed-upstream",
                expected_trial_count=1,
            )
        )
        conn.execute(
            insert(Trial).values(
                id=trial_id,
                task_id=task_id,
                team_id=team_id,
                state="failed",
                config={},
                requires_caps={},
                submitted_at=ts,
                finished_at=ts,
                batch_id=batch_id,
            )
        )
        conn.execute(
            insert(LlmCall).values(
                id=uuid4(),
                team_id=team_id,
                trial_id=trial_id,
                step_id="main",
                model="gpt-4",
                dialect="openai_chat",
                input_tokens=0,
                output_tokens=0,
                provider_extras={
                    "_loom_call_status": "failed",
                    "_loom_failure_category": "upstream_http_5xx",
                    "_loom_failure_status_code": 500,
                    "_loom_usage_status": "missing",
                },
                cost_usd=Decimal("0"),
                rate_card_hash="failed-upstream",
                captured_at=ts,
            )
        )
    sync_engine.dispose()

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://svc",
    ) as ac:
        filtered = await ac.get(
            "/api/v1/usage",
            params={
                "team_id": team_str,
                "start": "2026-06-02",
                "end": "2026-06-02",
                "pricing_mode": "failed-upstream",
                "include_batches": "true",
            },
            headers={"Authorization": f"Bearer {raw}"},
        )
        breakdown = await ac.get(
            "/api/v1/usage",
            params={
                "team_id": team_str,
                "start": "2026-06-02",
                "end": "2026-06-02",
                "breakdown_by": "pricing_mode",
            },
            headers={"Authorization": f"Bearer {raw}"},
        )

    assert filtered.status_code == 200, filtered.text
    buckets = filtered.json()["buckets"]
    assert len(buckets) == 1
    bucket = buckets[0]
    assert bucket["trial_count"] == 1
    assert bucket["llm_input_tokens"] == 0
    assert bucket["llm_output_tokens"] == 0
    assert bucket["estimated_cost_usd"] is None
    assert bucket["cost_currency"] is None
    assert bucket["cost_status"] == "failed_upstream"
    assert bucket["pricing_modes"] == ["failed-upstream"]
    assert bucket["priced_llm_calls_count"] == 0
    assert bucket["failed_upstream_llm_calls_count"] == 1
    assert bucket["missing_usage_llm_calls_count"] == 1
    assert bucket["usage_reporting_status"] == "missing"
    assert bucket["usage_estimate_confidence"] == "missing"
    assert bucket["batches"][0]["batch_name"] == "failed upstream audit batch"
    assert bucket["batches"][0]["failed_upstream_llm_calls_count"] == 1
    assert bucket["batches"][0]["cost_status"] == "failed_upstream"

    assert breakdown.status_code == 200, breakdown.text
    by_key = {
        item["breakdown_key"]: item
        for item in breakdown.json()["buckets"]
        if item.get("breakdown_by") == "pricing_mode"
    }
    assert by_key["failed-upstream"]["failed_upstream_llm_calls_count"] == 1
    assert by_key["failed-upstream"]["priced_llm_calls_count"] == 0
    assert by_key["priced"]["estimated_cost_usd"] == pytest.approx(0.01)


async def test_admin_usage_filters_and_breakdowns_cover_cost_dimensions(
    usage_setup: tuple[FastAPI, str, str, str, str],
    postgres_url: str,
) -> None:
    app, _team_raw, admin_raw, team_str, _task_id = usage_setup
    team_id = UUID(team_str)
    user_id = uuid4()
    token_raw = f"loom_team_{uuid4().hex}"
    token_hash = hashlib.sha256(token_raw.encode()).digest()
    token_prefix = token_hash.hex()[:8]
    provider_id = uuid4()
    batch_id = uuid4()
    trial_id = uuid4()
    task_id = f"skilllearnbench/task-{uuid4().hex[:8]}"
    ts = datetime(2026, 6, 2, 14, tzinfo=UTC)
    sync_engine = create_engine(postgres_url)
    with sync_engine.begin() as conn:
        conn.execute(
            insert(User).values(
                id=user_id,
                email="usage-owner@example.test",
                username="usage-owner",
                username_normalized="usage-owner",
                display_name="Usage Owner",
                status="active",
            )
        )
        conn.execute(
            insert(Token).values(
                token_hash=token_hash,
                name="usage owner token",
                type="team",
                scopes=["read:own", "submit"],
                team_id=team_id,
                created_by_user_id=user_id,
                issued_at=ts,
            )
        )
        conn.execute(
            insert(ProviderConnection).values(
                id=provider_id,
                team_id=team_id,
                provider_type="openai-compatible",
                display_name="YibuAPI hosted",
                base_url="https://api.yibuapi.com/v1",
                upstream_host="api.yibuapi.com",
                encrypted_api_key_ref="env:YIBUAPI_KEY",
                status="valid",
                pricing_source="tokens-only",
                rate_card_provider="yibuapi",
                created_by="test",
            )
        )
        conn.execute(
            insert(Benchmark).values(
                id="skilllearnbench",
                display_name="SkillLearnBench",
                upstream_kind="github",
                upstream_locator="cxcscmu/SkillLearnBench",
                upstream_revision="fixture",
                license_spdx="MIT",
                license_url="https://example.test/license",
                splits=["human_authored"],
            )
        )
        conn.execute(
            insert(Task).values(
                id=task_id,
                checksum="y" * 64,
                config={"task": {"id": task_id}},
                source="benchmark",
                license="MIT",
                benchmark_id="skilllearnbench",
            )
        )
        conn.execute(
            insert(Batch).values(
                id=batch_id,
                team_id=team_id,
                name="filtered yibuapi usage",
                task_filter={"task_ids": [task_id], "subset_kind": "explicit"},
                trial_config={},
                state="finished",
                created_by_token_prefix=token_prefix,
                expected_trial_count=1,
                provider_connection_id=provider_id,
                provider_model_id="qwen3.6-35b-a3b",
            )
        )
        conn.execute(
            insert(Trial).values(
                id=trial_id,
                task_id=task_id,
                team_id=team_id,
                state="failed",
                config={},
                requires_caps={},
                submitted_at=ts,
                finished_at=ts,
                batch_id=batch_id,
                provider_connection_id=provider_id,
                provider_model_id="qwen3.6-35b-a3b",
            )
        )
        conn.execute(
            insert(LlmCall).values(
                id=uuid4(),
                team_id=team_id,
                trial_id=trial_id,
                step_id="main",
                model="qwen3.6-35b-a3b",
                dialect="openai_responses",
                input_tokens=77,
                output_tokens=11,
                provider_extras={
                    "_loom_usage_status": "partial",
                    "_loom_missing_usage_keys": ["output_tokens"],
                    "_loom_provider_usage": {"input_tokens": 77},
                },
                cost_usd=Decimal("0"),
                rate_card_hash="facade:tokens-only",
                captured_at=ts,
            )
        )
    sync_engine.dispose()

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://svc",
    ) as ac:
        filtered = await ac.get(
            "/api/v1/usage",
            params={
                "team_id": team_str,
                "start": "2026-06-02",
                "end": "2026-06-02",
                "provider_connection_id": str(provider_id),
                "model": "qwen3.6-35b-a3b",
                "benchmark_id": "skilllearnbench",
                "status": "failed",
                "pricing_mode": "tokens-only",
                "user_id": str(user_id),
                "include_batches": "true",
            },
            headers={"Authorization": f"Bearer {admin_raw}"},
        )
        breakdown = await ac.get(
            "/api/v1/usage",
            params={
                "team_id": team_str,
                "start": "2026-06-02",
                "end": "2026-06-02",
                "breakdown_by": "pricing_mode",
            },
            headers={"Authorization": f"Bearer {admin_raw}"},
        )

    assert filtered.status_code == 200, filtered.text
    buckets = filtered.json()["buckets"]
    assert len(buckets) == 1
    bucket = buckets[0]
    assert bucket["trial_count"] == 1
    assert bucket["llm_input_tokens"] == 77
    assert bucket["llm_output_tokens"] == 11
    assert bucket["cost_status"] == "not_applicable"
    assert bucket["pricing_modes"] == ["tokens-only"]
    assert bucket["partial_usage_llm_calls_count"] == 1
    assert bucket["missing_usage_llm_calls_count"] == 0
    assert bucket["usage_reporting_status"] == "partial"
    assert bucket["usage_estimate_confidence"] == "partial"
    assert bucket["batches"][0]["batch_id"] == str(batch_id)
    assert bucket["batches"][0]["partial_usage_llm_calls_count"] == 1
    assert bucket["batches"][0]["usage_estimate_confidence"] == "partial"

    assert breakdown.status_code == 200, breakdown.text
    by_key = {
        item["breakdown_key"]: item
        for item in breakdown.json()["buckets"]
        if item.get("breakdown_by") == "pricing_mode"
    }
    assert by_key["tokens-only"]["llm_input_tokens"] == 77
    assert by_key["tokens-only"]["cost_status"] == "not_applicable"
    assert by_key["priced"]["estimated_cost_usd"] == pytest.approx(0.01)


async def test_usage_groups_by_week(
    usage_setup: tuple[FastAPI, str, str, str, str],
) -> None:
    app, raw, _admin_raw, team_str, _task_id = usage_setup
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://svc",
    ) as ac:
        r = await ac.get(
            f"/api/v1/usage?team_id={team_str}&start=2026-06-01&end=2026-06-30&group_by=week",
            headers={"Authorization": f"Bearer {raw}"},
        )
    assert r.status_code == 200
    buckets = r.json()["buckets"]
    # All 3 trials fall into the same week (June 1, 2026 is a Monday).
    assert len(buckets) == 1
    assert buckets[0]["trial_count"] == 3
    assert buckets[0]["llm_input_tokens"] == 300


async def test_usage_default_team_for_team_caller(
    usage_setup: tuple[FastAPI, str, str, str, str],
) -> None:
    """No team_id query param → scoped to caller's team_id."""
    app, raw, _admin_raw, _team_str, _task_id = usage_setup
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://svc",
    ) as ac:
        r = await ac.get(
            "/api/v1/usage?start=2026-06-01&end=2026-06-03",
            headers={"Authorization": f"Bearer {raw}"},
        )
    assert r.status_code == 200
    assert r.json()["buckets"][0]["trial_count"] == 2


async def test_cross_team_forbidden(
    usage_setup: tuple[FastAPI, str, str, str, str],
) -> None:
    app, raw, _admin_raw, _team_str, _task_id = usage_setup
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://svc",
    ) as ac:
        r = await ac.get(
            f"/api/v1/usage?team_id={uuid4()}&start=2026-06-01&end=2026-06-03",
            headers={"Authorization": f"Bearer {raw}"},
        )
    assert r.status_code == 403


async def test_invalid_group_by_400(
    usage_setup: tuple[FastAPI, str, str, str, str],
) -> None:
    app, raw, _admin_raw, _team_str, _task_id = usage_setup
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://svc",
    ) as ac:
        r = await ac.get(
            "/api/v1/usage?start=2026-06-01&end=2026-06-03&group_by=hour",
            headers={"Authorization": f"Bearer {raw}"},
        )
    assert r.status_code == 400


async def test_end_before_start_400(
    usage_setup: tuple[FastAPI, str, str, str, str],
) -> None:
    app, raw, _admin_raw, _team_str, _task_id = usage_setup
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://svc",
    ) as ac:
        r = await ac.get(
            "/api/v1/usage?start=2026-06-10&end=2026-06-01",
            headers={"Authorization": f"Bearer {raw}"},
        )
    assert r.status_code == 400


async def test_degraded_when_llm_calls_missing(
    usage_setup: tuple[FastAPI, str, str, str, str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """If the llm_calls table somehow doesn't exist (operator drop,
    pre-Plan-9 schema), the route still 200s with empty buckets +
    degraded=True so the SPA can render a friendly state.

    Monkey-patches `_llm_calls_exists` directly rather than dropping
    the table — restoring the schema mid-session is brittle and
    Plan 9's table is canonical so we shouldn't actually remove it.
    """
    app, raw, _admin_raw, team_str, _task_id = usage_setup

    async def _absent(_session: object) -> bool:
        return False

    monkeypatch.setattr(
        "loom_service.routes.usage._llm_calls_exists",
        _absent,
    )

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://svc",
    ) as ac:
        r = await ac.get(
            f"/api/v1/usage?team_id={team_str}&start=2026-06-01&end=2026-06-03",
            headers={"Authorization": f"Bearer {raw}"},
        )
    assert r.status_code == 200
    assert r.json() == {"buckets": [], "degraded": True}


_ = UUID  # keep import for typing
