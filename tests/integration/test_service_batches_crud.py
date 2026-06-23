"""Batches CRUD: POST creates with materialized expected count,
GET lists + detail with rollup, cancel cascades (Plan 19 Task 3)."""

from __future__ import annotations

import hashlib
from collections.abc import AsyncIterator
from datetime import UTC, datetime
from decimal import Decimal
from uuid import UUID, uuid4

import boto3
import httpx
import pytest
from botocore.config import Config
from fastapi import FastAPI
from sqlalchemy import create_engine, delete, insert, select, text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.orm import sessionmaker

from loom.db.schema import (
    Batch,
    Benchmark,
    LlmCall,
    Task,
    Team,
    TeamQuota,
    Token,
    Trial,
    Worker,
)
from loom_service.app import create_app
from loom_service.config import LoomServiceSettings


def _valid_task_config(task_id: str) -> dict[str, object]:
    return {
        "schema_version": "1",
        "task": {"id": task_id, "name": task_id},
        "environment": {"os": "linux", "docker_image": "alpine"},
        "agent": {"name": "oracle"},
        "verifier": {"name": "pytest"},
        "steps": [{"name": "main"}],
    }


def _counter_value(
    metric_name: str,
    sample_name: str,
    labels: dict[str, str],
) -> float:
    from prometheus_client import REGISTRY

    for metric in REGISTRY.collect():
        if metric.name != metric_name:
            continue
        for sample in metric.samples:
            if sample.name == sample_name and all(
                sample.labels.get(k) == v for k, v in labels.items()
            ):
                return float(sample.value)
    return 0.0


@pytest.fixture
async def camp_setup(
    monkeypatch: pytest.MonkeyPatch,
    postgres_url: str,
) -> AsyncIterator[tuple[FastAPI, str, UUID]]:
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
    # CRUD tests never reach the CP via http_client (only the runner does).
    app.state.http_client = httpx.AsyncClient(base_url="http://cp")

    team_id = uuid4()
    raw = f"loom_team_{uuid4().hex}"
    sync_engine = create_engine(postgres_url)
    sl = sessionmaker(sync_engine)
    with sl() as s:
        s.execute(insert(Team).values(id=team_id, name=f"t-{team_id}"))
        s.execute(
            insert(Token).values(
                token_hash=hashlib.sha256(raw.encode()).digest(),
                type="team",
                scopes=["submit", "read:own"],
                team_id=team_id,
                issued_at=datetime.now(UTC),
            )
        )
        # 3 MIT tasks + 2 Apache to test license-filter materialization.
        for i in range(3):
            tid = f"local/mit-{i}"
            s.execute(
                insert(Task).values(
                    id=tid,
                    checksum="x" * 64,
                    config=_valid_task_config(tid),
                    source="local",
                    license="MIT",
                )
            )
        for i in range(2):
            tid = f"local/apache-{i}"
            s.execute(
                insert(Task).values(
                    id=tid,
                    checksum="x" * 64,
                    config=_valid_task_config(tid),
                    source="local",
                    license="Apache-2.0",
                )
            )
        # Live worker advertising every backend Loom ships drivers for —
        # required by the POST /batches reject-when-no-worker check.
        # Individual tests that want to exercise the rejection path
        # delete this row before issuing their POST.
        s.execute(
            insert(Worker).values(
                id=uuid4(),
                hostname="fixture-worker",
                version="test",
                capabilities=[
                    {"backend": "docker"},
                    {"backend": "fake"},
                    {"backend": "daytona"},
                    {"backend": "modal"},
                ],
                registered_at=datetime.now(UTC),
                last_seen_at=datetime.now(UTC),
                status="active",
            )
        )
        s.commit()
    try:
        yield app, raw, team_id
    finally:
        await app.state.http_client.aclose()
        await engine.dispose()
        with sl() as s:
            s.execute(delete(LlmCall))
            s.execute(delete(Trial))
            s.execute(delete(Token))
            s.execute(delete(Batch))
            s.execute(delete(Task))
            s.execute(delete(Benchmark))
            s.execute(delete(Worker))
            s.execute(delete(TeamQuota))
            s.execute(delete(Team))
            s.commit()
        sync_engine.dispose()


async def test_post_batch_materializes_count(
    camp_setup: tuple[FastAPI, str, UUID],
) -> None:
    app, raw, _team_id = camp_setup
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://svc",
    ) as ac:
        r = await ac.post(
            "/api/v1/batches",
            headers={"Authorization": f"Bearer {raw}"},
            json={
                "name": "MIT slate",
                "description": "all MIT-licensed tasks",
                "task_filter": {"license": "MIT"},
                "trial_config": {"agent": {"name": "oracle"}},
            },
        )
    assert r.status_code == 201, r.text
    body = r.json()
    assert body["expected_trial_count"] == 3
    assert body["state"] == "submitted"
    UUID(body["batch_id"])  # parseable


async def test_post_batch_accepts_noncommercial_license_tasks(
    camp_setup: tuple[FastAPI, str, UUID],
    postgres_url: str,
) -> None:
    """Service-mode submit must not block tasks by source license."""
    app, raw, team_id = camp_setup
    sync_engine = create_engine(postgres_url)
    sl = sessionmaker(sync_engine)
    with sl() as s:
        s.execute(
            insert(TeamQuota).values(
                team_id=team_id,
                license_allowlist=["MIT"],
            )
        )
        s.execute(
            insert(Task).values(
                id="local/noncommercial",
                checksum="z" * 64,
                config=_valid_task_config("local/noncommercial"),
                source="local",
                license="CC-BY-NC-4.0",
            )
        )
        s.commit()
    sync_engine.dispose()

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://svc",
    ) as ac:
        r = await ac.post(
            "/api/v1/batches",
            headers={"Authorization": f"Bearer {raw}"},
            json={
                "name": "NC slate",
                "task_filter": {"license": "CC-BY-NC-4.0"},
                "trial_config": {"agent": {"name": "oracle"}},
            },
        )

    assert r.status_code == 201, r.text
    body = r.json()
    assert body["expected_trial_count"] == 1

    sync_engine = create_engine(postgres_url)
    sl = sessionmaker(sync_engine)
    with sl() as s:
        created = s.execute(
            select(Batch).where(Batch.name == "NC slate"),
        ).scalar_one_or_none()
    sync_engine.dispose()
    assert created is not None


async def test_post_batch_with_n_per_task_multiplies_count(
    camp_setup: tuple[FastAPI, str, UUID],
) -> None:
    """Plan 23: expected_trial_count = len(matched_tasks) * n_per_task."""
    app, raw, _team_id = camp_setup
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://svc",
    ) as ac:
        r = await ac.post(
            "/api/v1/batches",
            headers={"Authorization": f"Bearer {raw}"},
            json={
                "name": "MIT-x3",
                "task_filter": {"license": "MIT"},
                "trial_config": {},
                "n_per_task": 3,
            },
        )
    assert r.status_code == 201, r.text
    body = r.json()
    assert body["expected_trial_count"] == 9
    assert body["n_per_task"] == 3


async def test_paused_team_rejects_batch_and_records_reason(
    camp_setup: tuple[FastAPI, str, UUID],
) -> None:
    app, raw, team_id = camp_setup
    sync_engine = create_engine(str(app.state.settings.db_url))
    with sync_engine.begin() as conn:
        conn.execute(
            text(
                "UPDATE teams "
                "SET submissions_paused_at = NOW(), "
                "submissions_paused_reason = 'incident hold' "
                "WHERE id = :team_id",
            ),
            {"team_id": team_id},
        )
    sync_engine.dispose()

    before = _counter_value(
        "loom_svc_submission_rejects",
        "loom_svc_submission_rejects_total",
        {"reason": "team_paused"},
    )
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://svc",
    ) as ac:
        r = await ac.post(
            "/api/v1/batches",
            headers={"Authorization": f"Bearer {raw}"},
            json={
                "name": "paused-submit",
                "task_filter": {"license": "MIT"},
                "trial_config": {},
            },
        )

    assert r.status_code == 403, r.text
    assert "paused" in r.json()["detail"]
    after = _counter_value(
        "loom_svc_submission_rejects",
        "loom_svc_submission_rejects_total",
        {"reason": "team_paused"},
    )
    assert after == before + 1


async def test_post_batch_rejects_n_per_task_out_of_range(
    camp_setup: tuple[FastAPI, str, UUID],
) -> None:
    app, raw, _team_id = camp_setup
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://svc",
    ) as ac:
        r = await ac.post(
            "/api/v1/batches",
            headers={"Authorization": f"Bearer {raw}"},
            json={
                "name": "bad-n",
                "task_filter": {"license": "MIT"},
                "trial_config": {},
                "n_per_task": 0,
            },
        )
    assert r.status_code == 422


async def test_post_batch_rejects_unknown_agent_name(
    camp_setup: tuple[FastAPI, str, UUID],
) -> None:
    """Plan 25: a batch whose trial_config.agent_name isn't in the
    catalog is rejected at the API boundary so the batch runner
    doesn't fan out trials that would all 422."""
    app, raw, _team_id = camp_setup
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://svc",
    ) as ac:
        r = await ac.post(
            "/api/v1/batches",
            headers={"Authorization": f"Bearer {raw}"},
            json={
                "name": "phantom-agent",
                "task_filter": {"license": "MIT"},
                "trial_config": {
                    "agent_name": "not-an-agent",
                    "agent_model": None,
                },
            },
        )
    assert r.status_code == 400
    assert "agent" in r.json()["detail"].lower()


async def test_post_batch_rejects_agent_name_without_agent_model(
    camp_setup: tuple[FastAPI, str, UUID],
) -> None:
    app, raw, _team_id = camp_setup
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://svc",
    ) as ac:
        r = await ac.post(
            "/api/v1/batches",
            headers={"Authorization": f"Bearer {raw}"},
            json={
                "name": "missing-agent-model",
                "task_filter": {"license": "MIT"},
                "trial_config": {"agent_name": "oracle"},
            },
        )
        listed = await ac.get(
            "/api/v1/batches",
            headers={"Authorization": f"Bearer {raw}"},
        )
    assert r.status_code == 400
    assert "agent_model" in r.json()["detail"]
    assert listed.status_code == 200, listed.text
    assert listed.json()["items"] == []


async def test_post_batch_rejects_agent_without_service_runtime(
    camp_setup: tuple[FastAPI, str, UUID],
) -> None:
    """#289/#288: unsupported displayed adapters must fail before
    fan-out, not after every child trial hits `command not found`."""
    app, raw, _team_id = camp_setup
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://svc",
    ) as ac:
        r = await ac.post(
            "/api/v1/batches",
            headers={"Authorization": f"Bearer {raw}"},
            json={
                "name": "opencode-not-provisioned",
                "task_filter": {"license": "MIT"},
                "trial_config": {
                    "agent_name": "opencode",
                    "agent_model": {"provider": "openai", "name": "gpt-4o"},
                },
            },
        )
        listed = await ac.get(
            "/api/v1/batches",
            headers={"Authorization": f"Bearer {raw}"},
        )
    assert r.status_code == 400
    detail = r.json()["detail"]
    assert "opencode" in detail
    assert "runtime" in detail.lower()
    assert "GET /api/v1/agents" in detail
    assert listed.status_code == 200, listed.text
    assert listed.json()["items"] == []


async def test_post_rejects_unknown_filter_key(
    camp_setup: tuple[FastAPI, str, UUID],
) -> None:
    """Typo'd filter keys (`liscense`) get a 400 rather than silently
    matching zero tasks."""
    app, raw, _team_id = camp_setup
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://svc",
    ) as ac:
        r = await ac.post(
            "/api/v1/batches",
            headers={"Authorization": f"Bearer {raw}"},
            json={
                "name": "broken",
                "task_filter": {"liscense": "MIT"},
                "trial_config": {},
            },
        )
    assert r.status_code == 400
    assert "liscense" in r.json()["detail"]


async def test_post_rejects_empty_filter_match(
    camp_setup: tuple[FastAPI, str, UUID],
) -> None:
    """Audit M2: a filter that materializes to zero tasks would
    create a batch stuck in `submitted` forever
    (next_batch_state needs `expected > 0`). Reject up front."""
    app, raw, _team_id = camp_setup
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://svc",
    ) as ac:
        r = await ac.post(
            "/api/v1/batches",
            headers={"Authorization": f"Bearer {raw}"},
            json={
                "name": "empty",
                "task_filter": {"license": "no-such-license"},
                "trial_config": {},
            },
        )
    assert r.status_code == 400
    assert "zero tasks" in r.json()["detail"]


async def test_post_rejects_invalid_task_config(
    camp_setup: tuple[FastAPI, str, UUID],
    postgres_url: str,
) -> None:
    app, raw, _team_id = camp_setup
    sync_engine = create_engine(postgres_url)
    with sync_engine.begin() as conn:
        conn.execute(
            insert(Task).values(
                id="local/broken-config",
                checksum="b" * 64,
                config={},
                source="local",
                license="BrokenFixture",
            )
        )
    sync_engine.dispose()

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://svc",
    ) as ac:
        r = await ac.post(
            "/api/v1/batches",
            headers={"Authorization": f"Bearer {raw}"},
            json={
                "name": "broken-task",
                "task_filter": {"task_ids": ["local/broken-config"]},
                "trial_config": {},
            },
        )
    assert r.status_code == 400
    detail = r.json()["detail"]
    assert "invalid task config" in detail
    assert "local/broken-config" in detail


async def test_post_rejects_when_no_worker_advertises_backend(
    camp_setup: tuple[FastAPI, str, UUID],
    postgres_url: str,
) -> None:
    """cluster-deploy.md §POST /batches: reject when no live worker
    advertises the requested backend. Saves the operator from a batch
    that would stall in 'submitted' forever (no claim ever comes)."""
    app, raw, _team_id = camp_setup
    sync_engine = create_engine(postgres_url)
    sl = sessionmaker(sync_engine)
    # Tear down the fixture worker so no backend is live.
    with sl() as s:
        s.execute(delete(Worker))
        s.commit()

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://svc",
    ) as ac:
        r = await ac.post(
            "/api/v1/batches",
            headers={"Authorization": f"Bearer {raw}"},
            json={
                "name": "lonely",
                "task_filter": {"license": "MIT"},
                "trial_config": {},
                "backend": "docker",
            },
        )
    sync_engine.dispose()
    assert r.status_code == 400, r.text
    detail = r.json()["detail"]
    assert "no active worker advertises backend 'docker'" in detail
    assert "no active workers" in detail


async def test_post_rejects_when_no_worker_serves_specific_backend(
    camp_setup: tuple[FastAPI, str, UUID],
    postgres_url: str,
) -> None:
    """A worker exists, but it doesn't advertise the requested backend.
    The 400 detail names what IS available so operators can switch."""
    app, raw, _team_id = camp_setup
    sync_engine = create_engine(postgres_url)
    sl = sessionmaker(sync_engine)
    # Replace fixture worker with one that ONLY serves docker.
    with sl() as s:
        s.execute(delete(Worker))
        s.execute(
            insert(Worker).values(
                id=uuid4(),
                hostname="docker-only",
                version="test",
                capabilities=[{"backend": "docker"}],
                registered_at=datetime.now(UTC),
                last_seen_at=datetime.now(UTC),
                status="active",
            )
        )
        s.commit()

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://svc",
    ) as ac:
        r = await ac.post(
            "/api/v1/batches",
            headers={"Authorization": f"Bearer {raw}"},
            json={
                "name": "wants-modal",
                "task_filter": {"license": "MIT"},
                "trial_config": {},
                "backend": "modal",
            },
        )
    sync_engine.dispose()
    assert r.status_code == 400
    detail = r.json()["detail"]
    assert "'modal'" in detail
    assert "docker" in detail  # what IS available


async def test_post_rejects_when_only_worker_is_inactive(
    camp_setup: tuple[FastAPI, str, UUID],
    postgres_url: str,
) -> None:
    """An inactive worker doesn't count — `status='active'` is the
    predicate. Catches a regression to checking presence-only."""
    app, raw, _team_id = camp_setup
    sync_engine = create_engine(postgres_url)
    sl = sessionmaker(sync_engine)
    with sl() as s:
        # Demote the fixture worker to inactive.
        s.execute(Worker.__table__.update().values(status="shutting-down"))
        s.commit()

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://svc",
    ) as ac:
        r = await ac.post(
            "/api/v1/batches",
            headers={"Authorization": f"Bearer {raw}"},
            json={
                "name": "stale-only",
                "task_filter": {"license": "MIT"},
                "trial_config": {},
                "backend": "docker",
            },
        )
    sync_engine.dispose()
    assert r.status_code == 400
    assert "no active workers" in r.json()["detail"]


async def test_post_rejects_when_worker_heartbeat_is_stale(
    camp_setup: tuple[FastAPI, str, UUID],
    postgres_url: str,
) -> None:
    """Issue #68: a worker that crashed without SIGTERM keeps
    status='active' forever (no CP-side reaper flips it). The
    freshness predicate on `last_seen_at` ensures we don't keep
    handing batches to a dead worker. Heartbeat older than 30s
    ⇒ excluded from the catalog."""
    from datetime import timedelta

    app, raw, _team_id = camp_setup
    sync_engine = create_engine(postgres_url)
    sl = sessionmaker(sync_engine)
    # Age the fixture worker's heartbeat past the 30s freshness
    # window. `status` stays 'active' — that's the bug we're guarding
    # against (no reaper updates status today).
    with sl() as s:
        s.execute(
            Worker.__table__.update().values(
                last_seen_at=datetime.now(UTC) - timedelta(seconds=120),
            )
        )
        s.commit()

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://svc",
    ) as ac:
        r = await ac.post(
            "/api/v1/batches",
            headers={"Authorization": f"Bearer {raw}"},
            json={
                "name": "stale-hb",
                "task_filter": {"license": "MIT"},
                "trial_config": {},
                "backend": "docker",
            },
        )
    sync_engine.dispose()
    assert r.status_code == 400
    assert "no active workers" in r.json()["detail"]


async def test_post_requires_submit_scope(
    camp_setup: tuple[FastAPI, str, UUID],
    postgres_url: str,
) -> None:
    """A read:own-only token cannot create batches."""
    app, _raw, team_id = camp_setup
    no_submit_raw = f"loom_team_{uuid4().hex}"
    sync_engine = create_engine(postgres_url)
    sl = sessionmaker(sync_engine)
    with sl() as s:
        s.execute(
            insert(Token).values(
                token_hash=hashlib.sha256(no_submit_raw.encode()).digest(),
                type="team",
                scopes=["read:own"],
                team_id=team_id,
                issued_at=datetime.now(UTC),
            )
        )
        s.commit()
    sync_engine.dispose()
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://svc",
    ) as ac:
        r = await ac.post(
            "/api/v1/batches",
            headers={"Authorization": f"Bearer {no_submit_raw}"},
            json={
                "name": "X",
                "task_filter": {"license": "MIT"},
                "trial_config": {},
            },
        )
    assert r.status_code == 403


async def test_list_batches(
    camp_setup: tuple[FastAPI, str, UUID],
    postgres_url: str,
) -> None:
    app, raw, team_id = camp_setup
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://svc",
    ) as ac:
        await ac.post(
            "/api/v1/batches",
            headers={"Authorization": f"Bearer {raw}"},
            json={
                "name": "C1",
                "task_filter": {"license": "MIT"},
                "trial_config": {},
            },
        )
        c2 = await ac.post(
            "/api/v1/batches",
            headers={"Authorization": f"Bearer {raw}"},
            json={
                "name": "C2",
                "task_filter": {"license": "Apache-2.0"},
                "trial_config": {},
            },
        )
        assert c2.status_code == 201, c2.text
        c2_id = UUID(c2.json()["batch_id"])

        sync_engine = create_engine(postgres_url)
        with sync_engine.begin() as conn:
            trial_id = uuid4()
            conn.execute(
                insert(Trial).values(
                    id=trial_id,
                    task_id="local/apache-0",
                    team_id=team_id,
                    state="succeeded",
                    batch_id=c2_id,
                    sample_idx=0,
                    combination_idx=0,
                    config={},
                    requires_caps={},
                    result={"aggregate_reward": 1.0, "cost_usd": 99.0},
                )
            )
            conn.execute(
                insert(LlmCall).values(
                    id=uuid4(),
                    team_id=team_id,
                    trial_id=trial_id,
                    step_id="main",
                    model="openai/gpt-test",
                    dialect="openai",
                    input_tokens=4,
                    output_tokens=2,
                    provider_extras={},
                    cost_usd=Decimal("99.000000"),
                    rate_card_hash="stale-rate-card",
                )
            )
        sync_engine.dispose()

        r = await ac.get(
            "/api/v1/batches",
            headers={"Authorization": f"Bearer {raw}"},
        )
    assert r.status_code == 200
    items = r.json()["items"]
    assert len(items) == 2
    # Newest first.
    assert items[0]["name"] == "C2"
    assert "total_cost_usd" not in items[0]
    assert items[0]["total_prompt_tokens"] == 4
    assert items[0]["total_completion_tokens"] == 2
    assert items[0]["llm_calls_count"] == 1
    assert items[1]["total_prompt_tokens"] == 0
    assert items[1]["total_completion_tokens"] == 0
    assert items[1]["llm_calls_count"] == 0


async def test_get_batch_detail_with_rollup(
    camp_setup: tuple[FastAPI, str, UUID],
    postgres_url: str,
) -> None:
    """Detail surfaces per-state counts + reward/cost rollups extracted
    from Trial.result JSONB."""
    app, raw, team_id = camp_setup
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://svc",
    ) as ac:
        post = await ac.post(
            "/api/v1/batches",
            headers={"Authorization": f"Bearer {raw}"},
            json={
                "name": "rollup-test",
                "task_filter": {"license": "MIT"},
                "trial_config": {},
            },
        )
        cid = UUID(post.json()["batch_id"])

    # Seed 3 trial rows under this batch: 2 succeeded with rewards,
    # 1 still running. LLM usage should come from llm_calls, not the
    # stale/frozen cost_usd values in Trial.result.
    sync_engine = create_engine(postgres_url)
    sl = sessionmaker(sync_engine)
    with sl() as s:
        seeded_trial_ids: list[UUID] = []
        for i, (state, result) in enumerate(
            (
                ("succeeded", {"aggregate_reward": 1.0, "cost_usd": 0.05}),
                ("succeeded", {"aggregate_reward": 0.5, "cost_usd": 0.03}),
                ("running", None),
            )
        ):
            trial_id = uuid4()
            seeded_trial_ids.append(trial_id)
            s.execute(
                insert(Trial).values(
                    id=trial_id,
                    task_id=f"local/mit-{i}",
                    team_id=team_id,
                    state=state,
                    config={},
                    requires_caps={},
                    submitted_at=datetime.now(UTC),
                    batch_id=cid,
                    result=result,
                )
            )
        s.execute(
            insert(LlmCall),
            [
                {
                    "id": uuid4(),
                    "team_id": team_id,
                    "trial_id": seeded_trial_ids[0],
                    "step_id": "main",
                    "model": "openai/gpt-test",
                    "dialect": "openai",
                    "input_tokens": 10,
                    "output_tokens": 5,
                    "provider_extras": {},
                    "cost_usd": Decimal("9.990000"),
                    "rate_card_hash": "stale-rate-card",
                },
                {
                    "id": uuid4(),
                    "team_id": team_id,
                    "trial_id": seeded_trial_ids[1],
                    "step_id": "main",
                    "model": "openai/gpt-test",
                    "dialect": "openai",
                    "input_tokens": 7,
                    "output_tokens": 3,
                    "provider_extras": {},
                    "cost_usd": Decimal("0.000000"),
                    "rate_card_hash": "missing-rate-card",
                },
                {
                    "id": uuid4(),
                    "team_id": team_id,
                    "trial_id": seeded_trial_ids[2],
                    "step_id": "main",
                    "model": "openai/gpt-test",
                    "dialect": "openai",
                    "input_tokens": 1,
                    "output_tokens": 1,
                    "provider_extras": {},
                    "cost_usd": Decimal("0.000001"),
                    "rate_card_hash": "running-rate-card",
                },
            ],
        )
        s.commit()
    sync_engine.dispose()

    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://svc",
    ) as ac:
        r = await ac.get(
            f"/api/v1/batches/{cid}",
            headers={"Authorization": f"Bearer {raw}"},
        )
    assert r.status_code == 200
    body = r.json()
    assert body["trial_summary"]["succeeded"] == 2
    assert body["trial_summary"]["running"] == 1
    # avg of 1.0 + 0.5 = 0.75
    assert body["aggregate_reward"] == pytest.approx(0.75)
    assert "total_cost_usd" not in body
    assert body["total_prompt_tokens"] == 18
    assert body["total_completion_tokens"] == 9
    assert body["llm_calls_count"] == 3


async def test_get_batch_detail_includes_per_benchmark_rollup(
    camp_setup: tuple[FastAPI, str, UUID],
    postgres_url: str,
) -> None:
    app, raw, team_id = camp_setup
    sync_engine = create_engine(postgres_url)
    sl = sessionmaker(sync_engine)
    with sl() as s:
        s.execute(
            insert(Benchmark),
            [
                {
                    "id": "humaneval-420",
                    "display_name": "HumanEval 420",
                    "upstream_kind": "local",
                    "upstream_locator": "fixture",
                    "upstream_revision": "test",
                    "license_spdx": "MIT",
                    "license_url": "https://example.test/mit",
                    "splits": ["test"],
                },
                {
                    "id": "mbpp-420",
                    "display_name": "MBPP 420",
                    "upstream_kind": "local",
                    "upstream_locator": "fixture",
                    "upstream_revision": "test",
                    "license_spdx": "MIT",
                    "license_url": "https://example.test/mit",
                    "splits": ["test"],
                },
            ],
        )
        for tid, benchmark_id in (
            ("humaneval-420/task-0", "humaneval-420"),
            ("humaneval-420/task-1", "humaneval-420"),
            ("mbpp-420/task-0", "mbpp-420"),
        ):
            s.execute(
                insert(Task).values(
                    id=tid,
                    checksum="b" * 64,
                    config=_valid_task_config(tid),
                    source="local",
                    license="MIT",
                    benchmark_id=benchmark_id,
                )
            )
        s.commit()
    sync_engine.dispose()

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://svc",
    ) as ac:
        post = await ac.post(
            "/api/v1/batches",
            headers={"Authorization": f"Bearer {raw}"},
            json={
                "name": "mixed benchmarks",
                "task_filter": {
                    "subset_kind": "all",
                    "benchmark_ids": ["humaneval-420", "mbpp-420"],
                },
                "trial_config": {},
            },
        )
        assert post.status_code == 201, post.text
        batch_id = UUID(post.json()["batch_id"])

    sync_engine = create_engine(postgres_url)
    sl = sessionmaker(sync_engine)
    with sl() as s:
        for task_id, state, reward in (
            ("humaneval-420/task-0", "succeeded", 1.0),
            ("humaneval-420/task-1", "failed", 0.0),
            ("mbpp-420/task-0", "succeeded", 0.5),
        ):
            s.execute(
                insert(Trial).values(
                    id=uuid4(),
                    task_id=task_id,
                    team_id=team_id,
                    state=state,
                    config={},
                    requires_caps={},
                    submitted_at=datetime.now(UTC),
                    batch_id=batch_id,
                    result={"aggregate_reward": reward},
                )
            )
        s.commit()
    sync_engine.dispose()

    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://svc",
    ) as ac:
        r = await ac.get(
            f"/api/v1/batches/{batch_id}",
            headers={"Authorization": f"Bearer {raw}"},
        )

    assert r.status_code == 200, r.text
    body = r.json()
    assert body["aggregate_reward"] == pytest.approx(0.5)
    summary_by_id = {
        row["benchmark_id"]: row for row in body["benchmark_summary"]
    }
    assert set(summary_by_id) == {"humaneval-420", "mbpp-420"}

    humaneval = summary_by_id["humaneval-420"]
    assert humaneval["display_name"] == "HumanEval 420"
    assert humaneval["expected_trial_count"] == 2
    assert humaneval["completed_trial_count"] == 2
    assert humaneval["platform_failed_count"] == 1
    assert humaneval["trial_summary"]["succeeded"] == 1
    assert humaneval["trial_summary"]["failed"] == 1
    assert humaneval["aggregate_reward"] == pytest.approx(0.5)

    mbpp = summary_by_id["mbpp-420"]
    assert mbpp["display_name"] == "MBPP 420"
    assert mbpp["expected_trial_count"] == 1
    assert mbpp["completed_trial_count"] == 1
    assert mbpp["platform_failed_count"] == 0
    assert mbpp["trial_summary"]["succeeded"] == 1
    assert mbpp["aggregate_reward"] == pytest.approx(0.5)


async def test_get_batch_detail_exposes_fanout_failure(
    camp_setup: tuple[FastAPI, str, UUID],
    postgres_url: str,
) -> None:
    app, raw, team_id = camp_setup
    batch_id = uuid4()
    fanout_errors = [
        {
            "task_id": "local/mit-0",
            "sample_idx": 0,
            "combination_idx": None,
            "idempotency_key": f"{batch_id}::local/mit-0::0",
            "status_code": 403,
            "detail": "task license proprietary-MAA not in team allowlist",
            "seen_at": datetime.now(UTC).isoformat(),
        }
    ]

    sync_engine = create_engine(postgres_url)
    with sync_engine.begin() as conn:
        conn.execute(
            insert(Batch).values(
                id=batch_id,
                team_id=team_id,
                name="policy-blocked",
                task_filter={"license": "MIT"},
                trial_config={},
                state="finished",
                created_by_token_prefix="abcdef12",
                expected_trial_count=0,
                result_status="all_failed",
                fanout_errors=fanout_errors,
                finished_at=datetime.now(UTC),
            )
        )
    sync_engine.dispose()

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://svc",
    ) as ac:
        r = await ac.get(
            f"/api/v1/batches/{batch_id}",
            headers={"Authorization": f"Bearer {raw}"},
        )

    assert r.status_code == 200
    body = r.json()
    assert body["failure_reason"] == "fanout_submit_failed"
    assert "proprietary-MAA" in body["failure_message"]
    assert body["fanout_errors"] == fanout_errors


async def test_rerun_failed_batch_creates_linked_exact_targets(
    camp_setup: tuple[FastAPI, str, UUID],
    postgres_url: str,
) -> None:
    app, raw, team_id = camp_setup
    batch_id = uuid4()
    failed_trial_id = uuid4()

    sync_engine = create_engine(postgres_url)
    with sync_engine.begin() as conn:
        conn.execute(
            insert(Batch).values(
                id=batch_id,
                team_id=team_id,
                name="gateway-flaked",
                task_filter={"task_ids": ["local/mit-0"], "subset_kind": "explicit"},
                trial_config={
                    "agent_name": "litellm",
                    "agent_model": {"provider": "openai", "name": "qwen"},
                },
                state="finished",
                created_by_token_prefix="abcdef12",
                expected_trial_count=2,
                n_per_task=2,
                result_status="partial_failed",
                finished_at=datetime.now(UTC),
            )
        )
        conn.execute(
            insert(Trial).values(
                id=uuid4(),
                task_id="local/mit-0",
                team_id=team_id,
                state="succeeded",
                config={},
                requires_caps={},
                submitted_at=datetime.now(UTC),
                batch_id=batch_id,
                sample_idx=0,
                combination_idx=0,
                result={"aggregate_reward": 1.0, "cost_usd": 0.01},
            )
        )
        conn.execute(
            insert(Trial).values(
                id=failed_trial_id,
                task_id="local/mit-0",
                team_id=team_id,
                state="failed",
                failure_reason="gateway_error",
                failure_message="Loom gateway returned HTTP 503.",
                config={},
                requires_caps={},
                submitted_at=datetime.now(UTC),
                batch_id=batch_id,
                sample_idx=1,
                combination_idx=0,
                result=None,
            )
        )

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://svc") as ac:
        r = await ac.post(
            f"/api/v1/batches/{batch_id}/rerun-failed",
            headers={"Authorization": f"Bearer {raw}"},
        )

    assert r.status_code == 201, r.text
    body = r.json()
    rerun_batch_id = UUID(body["batch_id"])
    assert body["rerun_of_batch_id"] == str(batch_id)
    assert body["expected_trial_count"] == 1
    assert body["rerun_target_count"] == 1

    sl = sessionmaker(sync_engine)
    with sl() as s:
        row = s.execute(
            select(Batch).where(Batch.id == rerun_batch_id),
        ).scalar_one()
    sync_engine.dispose()

    assert row.rerun_of_batch_id == batch_id
    assert row.rerun_targets == [
        {
            "task_id": "local/mit-0",
            "sample_idx": 1,
            "combination_idx": 0,
            "original_trial_id": str(failed_trial_id),
            "failure_reason": "gateway_error",
        }
    ]


async def test_get_batch_detail_effective_rollup_uses_successful_rerun(
    camp_setup: tuple[FastAPI, str, UUID],
    postgres_url: str,
) -> None:
    app, raw, team_id = camp_setup
    batch_id = uuid4()
    rerun_batch_id = uuid4()
    failed_trial_id = uuid4()

    sync_engine = create_engine(postgres_url)
    original_success_trial_id = uuid4()
    rerun_success_trial_id = uuid4()
    with sync_engine.begin() as conn:
        conn.execute(
            insert(Batch).values(
                id=batch_id,
                team_id=team_id,
                name="original",
                task_filter={"task_ids": ["local/mit-0"], "subset_kind": "explicit"},
                trial_config={},
                state="finished",
                created_by_token_prefix="abcdef12",
                expected_trial_count=2,
                n_per_task=2,
                result_status="partial_failed",
                finished_at=datetime.now(UTC),
            )
        )
        conn.execute(
            insert(Trial).values(
                id=original_success_trial_id,
                task_id="local/mit-0",
                team_id=team_id,
                state="succeeded",
                config={},
                requires_caps={},
                submitted_at=datetime.now(UTC),
                batch_id=batch_id,
                sample_idx=0,
                combination_idx=0,
                result={"aggregate_reward": 1.0, "cost_usd": 0.01},
            )
        )
        conn.execute(
            insert(Trial).values(
                id=failed_trial_id,
                task_id="local/mit-0",
                team_id=team_id,
                state="failed",
                failure_reason="gateway_error",
                config={},
                requires_caps={},
                submitted_at=datetime.now(UTC),
                batch_id=batch_id,
                sample_idx=1,
                combination_idx=0,
            )
        )
        conn.execute(
            insert(Batch).values(
                id=rerun_batch_id,
                team_id=team_id,
                name="original failed-case rerun",
                task_filter={"task_ids": ["local/mit-0"], "subset_kind": "explicit"},
                trial_config={},
                state="finished",
                created_by_token_prefix="abcdef12",
                expected_trial_count=1,
                result_status="succeeded",
                finished_at=datetime.now(UTC),
                rerun_of_batch_id=batch_id,
                rerun_targets=[
                    {
                        "task_id": "local/mit-0",
                        "sample_idx": 1,
                        "combination_idx": 0,
                        "original_trial_id": str(failed_trial_id),
                        "failure_reason": "gateway_error",
                    }
                ],
            )
        )
        conn.execute(
            insert(Trial).values(
                id=rerun_success_trial_id,
                task_id="local/mit-0",
                team_id=team_id,
                state="succeeded",
                config={},
                requires_caps={},
                submitted_at=datetime.now(UTC),
                batch_id=rerun_batch_id,
                sample_idx=1,
                combination_idx=0,
                result={"aggregate_reward": 0.8, "cost_usd": 0.02},
            )
        )
        conn.execute(
            insert(LlmCall),
            [
                {
                    "id": uuid4(),
                    "team_id": team_id,
                    "trial_id": original_success_trial_id,
                    "step_id": "main",
                    "model": "openai/gpt-test",
                    "dialect": "openai",
                    "input_tokens": 5,
                    "output_tokens": 2,
                    "provider_extras": {},
                    "cost_usd": Decimal("0.010000"),
                    "rate_card_hash": "old-rate-card",
                },
                {
                    "id": uuid4(),
                    "team_id": team_id,
                    "trial_id": rerun_success_trial_id,
                    "step_id": "main",
                    "model": "openai/gpt-test",
                    "dialect": "openai",
                    "input_tokens": 11,
                    "output_tokens": 4,
                    "provider_extras": {},
                    "cost_usd": Decimal("0.020000"),
                    "rate_card_hash": "old-rate-card",
                },
            ],
        )
    sync_engine.dispose()

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://svc") as ac:
        r = await ac.get(
            f"/api/v1/batches/{batch_id}",
            headers={"Authorization": f"Bearer {raw}"},
        )

    assert r.status_code == 200
    body = r.json()
    assert body["trial_summary"]["succeeded"] == 1
    assert body["trial_summary"]["failed"] == 1
    assert body["effective_trial_summary"]["succeeded"] == 2
    assert body["effective_trial_summary"]["failed"] == 0
    assert body["effective_result_status"] == "succeeded"
    assert body["effective_aggregate_reward"] == pytest.approx(0.9)
    assert "total_cost_usd" not in body
    assert "effective_total_cost_usd" not in body
    assert body["total_prompt_tokens"] == 5
    assert body["total_completion_tokens"] == 2
    assert body["llm_calls_count"] == 1
    assert body["effective_total_prompt_tokens"] == 16
    assert body["effective_total_completion_tokens"] == 6
    assert body["effective_llm_calls_count"] == 2
    assert body["rerunnable_failed_count"] == 1
    assert body["rerun_batches"][0]["id"] == str(rerun_batch_id)


async def test_get_batch_not_found(
    camp_setup: tuple[FastAPI, str, UUID],
) -> None:
    app, raw, _team_id = camp_setup
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://svc",
    ) as ac:
        r = await ac.get(
            f"/api/v1/batches/{uuid4()}",
            headers={"Authorization": f"Bearer {raw}"},
        )
    assert r.status_code == 404


async def test_cancel_batch_cascades_to_active_trials(
    camp_setup: tuple[FastAPI, str, UUID],
    postgres_url: str,
) -> None:
    app, raw, team_id = camp_setup
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://svc",
    ) as ac:
        post = await ac.post(
            "/api/v1/batches",
            headers={"Authorization": f"Bearer {raw}"},
            json={
                "name": "to-cancel",
                "task_filter": {"license": "MIT"},
                "trial_config": {},
            },
        )
        cid = UUID(post.json()["batch_id"])

    # 1 queued, 1 succeeded — cancel should only touch the queued.
    sync_engine = create_engine(postgres_url)
    sl = sessionmaker(sync_engine)
    queued_id = uuid4()
    succ_id = uuid4()
    with sl() as s:
        s.execute(
            insert(Trial).values(
                id=queued_id,
                task_id="local/mit-0",
                team_id=team_id,
                state="queued",
                config={},
                requires_caps={},
                submitted_at=datetime.now(UTC),
                batch_id=cid,
            )
        )
        s.execute(
            insert(Trial).values(
                id=succ_id,
                task_id="local/mit-1",
                team_id=team_id,
                state="succeeded",
                config={},
                requires_caps={},
                submitted_at=datetime.now(UTC),
                batch_id=cid,
                result={"aggregate_reward": 1.0},
            )
        )
        s.commit()
    sync_engine.dispose()

    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://svc",
    ) as ac:
        cancel = await ac.post(
            f"/api/v1/batches/{cid}/cancel",
            headers={"Authorization": f"Bearer {raw}"},
        )
    assert cancel.status_code == 200
    assert cancel.json()["state"] == "cancelled"

    # Re-fetch trial states.
    from sqlalchemy import select

    sync_engine = create_engine(postgres_url)
    sl = sessionmaker(sync_engine)
    with sl() as s:
        queued_state = s.execute(
            select(Trial.state).where(Trial.id == queued_id),
        ).scalar_one()
        succ_state = s.execute(
            select(Trial.state).where(Trial.id == succ_id),
        ).scalar_one()
    sync_engine.dispose()
    assert queued_state == "cancelled"
    assert succ_state == "succeeded"  # terminal trial untouched
