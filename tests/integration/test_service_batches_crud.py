"""Batches CRUD: POST creates with materialized expected count,
GET lists + detail with rollup, cancel cascades (Plan 19 Task 3)."""

from __future__ import annotations

import hashlib
from collections.abc import AsyncIterator
from datetime import UTC, datetime
from uuid import UUID, uuid4

import boto3
import httpx
import pytest
from botocore.config import Config
from fastapi import FastAPI
from sqlalchemy import create_engine, delete, insert
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.orm import sessionmaker

from loom.db.schema import Batch, Task, Team, TeamQuota, Token, Trial
from loom_service.app import create_app
from loom_service.config import LoomServiceSettings


@pytest.fixture
async def camp_setup(
    monkeypatch: pytest.MonkeyPatch, postgres_url: str,
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
    # CRUD tests never reach the CP via http_client (only the runner does).
    app.state.http_client = httpx.AsyncClient(base_url="http://cp")

    team_id = uuid4()
    raw = f"loom_team_{uuid4().hex}"
    sync_engine = create_engine(postgres_url)
    sl = sessionmaker(sync_engine)
    with sl() as s:
        s.execute(insert(Team).values(id=team_id, name=f"t-{team_id}"))
        s.execute(insert(Token).values(
            token_hash=hashlib.sha256(raw.encode()).digest(),
            type="team", scopes=["submit", "read:own"], team_id=team_id,
            issued_at=datetime.now(UTC),
        ))
        # 3 MIT tasks + 2 Apache to test license-filter materialization.
        for i in range(3):
            s.execute(insert(Task).values(
                id=f"local/mit-{i}", checksum="x" * 64, config={},
                source="local", license="MIT",
            ))
        for i in range(2):
            s.execute(insert(Task).values(
                id=f"local/apache-{i}", checksum="x" * 64, config={},
                source="local", license="Apache-2.0",
            ))
        s.commit()
    try:
        yield app, raw, team_id
    finally:
        await app.state.http_client.aclose()
        await engine.dispose()
        with sl() as s:
            s.execute(delete(Trial))
            s.execute(delete(Token))
            s.execute(delete(Batch))
            s.execute(delete(Task))
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
        transport=transport, base_url="http://svc",
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


async def test_post_batch_with_n_per_task_multiplies_count(
    camp_setup: tuple[FastAPI, str, UUID],
) -> None:
    """Plan 23: expected_trial_count = len(matched_tasks) * n_per_task."""
    app, raw, _team_id = camp_setup
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://svc",
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


async def test_post_batch_rejects_n_per_task_out_of_range(
    camp_setup: tuple[FastAPI, str, UUID],
) -> None:
    app, raw, _team_id = camp_setup
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://svc",
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
        transport=transport, base_url="http://svc",
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


async def test_post_rejects_unknown_filter_key(
    camp_setup: tuple[FastAPI, str, UUID],
) -> None:
    """Typo'd filter keys (`liscense`) get a 400 rather than silently
    matching zero tasks."""
    app, raw, _team_id = camp_setup
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://svc",
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
        transport=transport, base_url="http://svc",
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
        s.execute(insert(Token).values(
            token_hash=hashlib.sha256(no_submit_raw.encode()).digest(),
            type="team", scopes=["read:own"], team_id=team_id,
            issued_at=datetime.now(UTC),
        ))
        s.commit()
    sync_engine.dispose()
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://svc",
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
) -> None:
    app, raw, _team_id = camp_setup
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://svc",
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
        await ac.post(
            "/api/v1/batches",
            headers={"Authorization": f"Bearer {raw}"},
            json={
                "name": "C2",
                "task_filter": {"license": "Apache-2.0"},
                "trial_config": {},
            },
        )
        r = await ac.get(
            "/api/v1/batches",
            headers={"Authorization": f"Bearer {raw}"},
        )
    assert r.status_code == 200
    items = r.json()["items"]
    assert len(items) == 2
    # Newest first.
    assert items[0]["name"] == "C2"


async def test_get_batch_detail_with_rollup(
    camp_setup: tuple[FastAPI, str, UUID],
    postgres_url: str,
) -> None:
    """Detail surfaces per-state counts + reward/cost rollups extracted
    from Trial.result JSONB."""
    app, raw, team_id = camp_setup
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://svc",
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
    # 1 still running.
    sync_engine = create_engine(postgres_url)
    sl = sessionmaker(sync_engine)
    with sl() as s:
        for i, (state, result) in enumerate((
            ("succeeded", {"aggregate_reward": 1.0, "cost_usd": 0.05}),
            ("succeeded", {"aggregate_reward": 0.5, "cost_usd": 0.03}),
            ("running", None),
        )):
            s.execute(insert(Trial).values(
                id=uuid4(),
                task_id=f"local/mit-{i}",
                team_id=team_id,
                state=state,
                config={},
                requires_caps={},
                submitted_at=datetime.now(UTC),
                batch_id=cid,
                result=result,
            ))
        s.commit()
    sync_engine.dispose()

    async with httpx.AsyncClient(
        transport=transport, base_url="http://svc",
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
    # sum 0.05 + 0.03
    assert body["total_cost_usd"] == pytest.approx(0.08)


async def test_get_batch_not_found(
    camp_setup: tuple[FastAPI, str, UUID],
) -> None:
    app, raw, _team_id = camp_setup
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://svc",
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
        transport=transport, base_url="http://svc",
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
        s.execute(insert(Trial).values(
            id=queued_id, task_id="local/mit-0", team_id=team_id,
            state="queued", config={}, requires_caps={},
            submitted_at=datetime.now(UTC), batch_id=cid,
        ))
        s.execute(insert(Trial).values(
            id=succ_id, task_id="local/mit-1", team_id=team_id,
            state="succeeded", config={}, requires_caps={},
            submitted_at=datetime.now(UTC), batch_id=cid,
            result={"aggregate_reward": 1.0},
        ))
        s.commit()
    sync_engine.dispose()

    async with httpx.AsyncClient(
        transport=transport, base_url="http://svc",
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
