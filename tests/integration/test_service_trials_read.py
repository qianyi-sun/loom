"""GET /api/v1/trials list with filters + cursor pagination (Plan 18 Task 2).

Plan-doc references several schema fields the v0.7 trials table
doesn't actually carry (`aggregate_reward`, `cost_usd`, `batch_id`,
UUID PK on tasks) — these tests target the actual schema instead.
Reward + cost come from `Trial.result`; agent name from
`Trial.config["agent"]`.
"""

from __future__ import annotations

import hashlib
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

import boto3
import httpx
import pytest
from botocore.config import Config
from fastapi import FastAPI
from sqlalchemy import create_engine, delete, insert, update
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.orm import sessionmaker

from loom.db.schema import Batch, Task, Team, TeamQuota, Token, Trial
from loom_service.app import create_app
from loom_service.config import LoomServiceSettings


@pytest.fixture
async def trials_setup(
    monkeypatch: pytest.MonkeyPatch, postgres_url: str,
) -> AsyncIterator[tuple[FastAPI, str, UUID, list[UUID]]]:
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
    task_id = f"local/task-{uuid4().hex[:8]}"
    trial_ids = [uuid4() for _ in range(3)]
    now = datetime.now(UTC)

    sync_engine = create_engine(postgres_url)
    sl = sessionmaker(sync_engine)
    with sl() as s:
        s.execute(insert(Team).values(id=team_id, name=f"t-{team_id}"))
        s.execute(insert(Token).values(
            token_hash=hashlib.sha256(raw.encode()).digest(),
            type="team", scopes=["read:own", "submit"], team_id=team_id,
            issued_at=now, expires_at=None,
        ))
        s.execute(insert(Task).values(
            id=task_id, checksum="x" * 64,
            config={"task": {"id": task_id, "name": "t"}},
            source="local", license="MIT",
        ))
        for i, tid in enumerate(trial_ids):
            s.execute(insert(Trial).values(
                id=tid, task_id=task_id, team_id=team_id,
                state="succeeded" if i % 2 == 0 else "running",
                config={"agent": {"name": "oracle", "model": None}},
                requires_caps={},
                submitted_at=now - timedelta(minutes=i),
                result=(
                    {"aggregate_reward": 1.0, "cost_usd": 0.05}
                    if i % 2 == 0 else None
                ),
            ))
        s.commit()
    try:
        yield app, raw, team_id, trial_ids
    finally:
        await app.state.http_client.aclose()
        await engine.dispose()
        with sl() as s:
            s.execute(delete(Trial))
            s.execute(delete(Token))
            s.execute(delete(Task))
            s.execute(delete(TeamQuota))
            s.execute(delete(Team))
            s.commit()
        sync_engine.dispose()


async def test_list_my_trials(
    trials_setup: tuple[FastAPI, str, UUID, list[UUID]],
) -> None:
    app, raw, _team_id, trial_ids = trials_setup
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://svc",
    ) as ac:
        r = await ac.get(
            "/api/v1/trials",
            headers={"Authorization": f"Bearer {raw}"},
        )
    assert r.status_code == 200, r.text
    items = r.json()["items"]
    assert len(items) == 3
    # Newest first (submitted_at desc). trial_ids[0] is newest in the fixture.
    assert items[0]["id"] == str(trial_ids[0])
    # Reward + cost extracted from result.
    assert items[0]["aggregate_reward"] == 1.0
    assert items[0]["cost_usd"] == 0.05
    # Running trial: no reward.
    assert items[1]["aggregate_reward"] is None
    # Agent name pulled from config.
    assert items[0]["agent_name"] == "oracle"


async def test_filter_by_state_succeeded_only(
    trials_setup: tuple[FastAPI, str, UUID, list[UUID]],
) -> None:
    app, raw, _team_id, _t = trials_setup
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://svc",
    ) as ac:
        r = await ac.get(
            "/api/v1/trials?state=succeeded",
            headers={"Authorization": f"Bearer {raw}"},
        )
    assert r.status_code == 200
    items = r.json()["items"]
    assert len(items) == 2
    assert all(i["state"] == "succeeded" for i in items)


async def test_filter_by_multi_state(
    trials_setup: tuple[FastAPI, str, UUID, list[UUID]],
) -> None:
    app, raw, _team_id, _t = trials_setup
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://svc",
    ) as ac:
        r = await ac.get(
            "/api/v1/trials?state=succeeded,running",
            headers={"Authorization": f"Bearer {raw}"},
        )
    items = r.json()["items"]
    assert len(items) == 3


async def test_pagination_cursor(
    trials_setup: tuple[FastAPI, str, UUID, list[UUID]],
) -> None:
    app, raw, _team_id, _t = trials_setup
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://svc",
    ) as ac:
        r1 = await ac.get(
            "/api/v1/trials?limit=2",
            headers={"Authorization": f"Bearer {raw}"},
        )
        j1 = r1.json()
        assert len(j1["items"]) == 2
        assert j1["next_cursor"] is not None

        r2 = await ac.get(
            f"/api/v1/trials?limit=2&cursor={j1['next_cursor']}",
            headers={"Authorization": f"Bearer {raw}"},
        )
    j2 = r2.json()
    assert len(j2["items"]) == 1
    assert j2["next_cursor"] is None


async def test_invalid_cursor_returns_400(
    trials_setup: tuple[FastAPI, str, UUID, list[UUID]],
) -> None:
    app, raw, _team_id, _t = trials_setup
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://svc",
    ) as ac:
        r = await ac.get(
            "/api/v1/trials?cursor=!!!not-a-cursor",
            headers={"Authorization": f"Bearer {raw}"},
        )
    assert r.status_code == 400


async def test_cross_team_forbidden_for_team(
    trials_setup: tuple[FastAPI, str, UUID, list[UUID]],
) -> None:
    app, raw, _team_id, _t = trials_setup
    other_team = uuid4()
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://svc",
    ) as ac:
        r = await ac.get(
            f"/api/v1/trials?team_id={other_team}",
            headers={"Authorization": f"Bearer {raw}"},
        )
    assert r.status_code == 403


async def test_no_read_own_scope_forbidden(
    trials_setup: tuple[FastAPI, str, UUID, list[UUID]],
    postgres_url: str,
) -> None:
    app, _raw, team_id, _t = trials_setup
    no_scope_raw = f"loom_team_{uuid4().hex}"
    sync_engine = create_engine(postgres_url)
    sl = sessionmaker(sync_engine)
    with sl() as s:
        s.execute(insert(Token).values(
            token_hash=hashlib.sha256(no_scope_raw.encode()).digest(),
            type="team", scopes=["submit"], team_id=team_id,
            issued_at=datetime.now(UTC),
        ))
        s.commit()
    sync_engine.dispose()
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://svc",
    ) as ac:
        r = await ac.get(
            "/api/v1/trials",
            headers={"Authorization": f"Bearer {no_scope_raw}"},
        )
    assert r.status_code == 403


async def test_trial_detail_returns_presigned_urls(
    trials_setup: tuple[FastAPI, str, UUID, list[UUID]],
) -> None:
    app, raw, _team_id, trial_ids = trials_setup
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://svc",
    ) as ac:
        r = await ac.get(
            f"/api/v1/trials/{trial_ids[0]}",
            headers={"Authorization": f"Bearer {raw}"},
        )
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["id"] == str(trial_ids[0])
    # boto3 presigned URLs include X-Amz-Signature in the query string.
    assert "X-Amz-Signature" in body["atif_url"]
    assert "X-Amz-Signature" in body["trajectory_url"]
    # URL anchors on the actual key shape (`{team_id}/{trial_id}/...`).
    assert f"/{trial_ids[0]}/atif.json" in body["atif_url"]
    assert f"/{trial_ids[0]}/events.jsonl" in body["trajectory_url"]
    assert body["artifacts"] == []


async def test_trial_detail_exposes_projected_artifacts(
    trials_setup: tuple[FastAPI, str, UUID, list[UUID]],
    postgres_url: str,
) -> None:
    app, raw, team_id, trial_ids = trials_setup
    artifact_key = f"{team_id}/{trial_ids[0]}/main/result.txt"
    sync_engine = create_engine(postgres_url)
    sl = sessionmaker(sync_engine)
    with sl() as s:
        s.execute(
            update(Trial)
            .where(Trial.id == trial_ids[0])
            .values(trajectory_index={
                "trajectory_uri": (
                    f"s3://trajectories/{team_id}/{trial_ids[0]}/events.jsonl"
                ),
                "atif_uri": f"s3://trajectories/{team_id}/{trial_ids[0]}/atif.json",
                "artifacts": [{
                    "step_name": "main",
                    "bucket": "artifacts",
                    "key": artifact_key,
                    "size": 5,
                }],
            }),
        )
        s.commit()
    sync_engine.dispose()

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://svc",
    ) as ac:
        r = await ac.get(
            f"/api/v1/trials/{trial_ids[0]}",
            headers={"Authorization": f"Bearer {raw}"},
        )
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["artifacts"] == [{
        "step_name": "main",
        "key": artifact_key,
        "size": 5,
        "download_url": body["artifacts"][0]["download_url"],
    }]
    assert "X-Amz-Signature" in body["artifacts"][0]["download_url"]


async def test_trial_detail_not_found(
    trials_setup: tuple[FastAPI, str, UUID, list[UUID]],
) -> None:
    app, raw, _team_id, _t = trials_setup
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://svc",
    ) as ac:
        r = await ac.get(
            f"/api/v1/trials/{uuid4()}",
            headers={"Authorization": f"Bearer {raw}"},
        )
    assert r.status_code == 404


async def test_trial_detail_cross_team_forbidden(
    trials_setup: tuple[FastAPI, str, UUID, list[UUID]],
    postgres_url: str,
) -> None:
    """A team-A caller can't read team-B's trial detail."""
    app, _raw_a, _team_a, trial_ids_a = trials_setup
    other_team = uuid4()
    other_raw = f"loom_team_{uuid4().hex}"
    sync_engine = create_engine(postgres_url)
    sl = sessionmaker(sync_engine)
    with sl() as s:
        s.execute(insert(Team).values(id=other_team, name=f"o-{other_team}"))
        s.execute(insert(Token).values(
            token_hash=hashlib.sha256(other_raw.encode()).digest(),
            type="team", scopes=["read:own"], team_id=other_team,
            issued_at=datetime.now(UTC),
        ))
        s.commit()
    sync_engine.dispose()
    try:
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(
            transport=transport, base_url="http://svc",
        ) as ac:
            r = await ac.get(
                f"/api/v1/trials/{trial_ids_a[0]}",
                headers={"Authorization": f"Bearer {other_raw}"},
            )
        assert r.status_code == 403
    finally:
        sync_engine = create_engine(postgres_url)
        sl = sessionmaker(sync_engine)
        with sl() as s:
            from loom.db.schema import Token as TokenModel
            s.execute(delete(TokenModel).where(
                TokenModel.team_id == other_team,
            ))
            s.execute(delete(TeamQuota).where(
                TeamQuota.team_id == other_team,
            ))
            s.execute(delete(Team).where(Team.id == other_team))
            s.commit()
        sync_engine.dispose()


async def test_trial_detail_carries_ready_flags(
    trials_setup: tuple[FastAPI, str, UUID, list[UUID]],
) -> None:
    """Audit M1: ready flags so the SPA can skip rendering a download
    link that would 404. trajectory_ready iff started_at is not null;
    atif_ready iff state terminal + finished_at not null."""
    app, raw, _team_id, trial_ids = trials_setup
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://svc",
    ) as ac:
        r = await ac.get(
            f"/api/v1/trials/{trial_ids[0]}",
            headers={"Authorization": f"Bearer {raw}"},
        )
    body = r.json()
    # Seeded trial: succeeded state, but no started_at/finished_at
    # were set on insert — so both flags should be False.
    assert body["atif_ready"] is False
    assert body["trajectory_ready"] is False


async def test_filter_by_task_id(
    trials_setup: tuple[FastAPI, str, UUID, list[UUID]],
) -> None:
    app, raw, _team_id, _t = trials_setup
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://svc",
    ) as ac:
        r = await ac.get(
            "/api/v1/trials",
            headers={"Authorization": f"Bearer {raw}"},
        )
        # Pull the task_id from one trial, then filter by it.
        task_id = r.json()["items"][0]["task_id"]
        r2 = await ac.get(
            f"/api/v1/trials?task_id={task_id}",
            headers={"Authorization": f"Bearer {raw}"},
        )
    assert r2.status_code == 200
    items = r2.json()["items"]
    assert all(it["task_id"] == task_id for it in items)


async def test_filter_by_batch_id(
    trials_setup: tuple[FastAPI, str, UUID, list[UUID]],
    postgres_url: str,
) -> None:
    """Inject a batch + back-link the first two trials; the third
    keeps `batch_id = NULL`. Filtering by the batch id must return
    exactly the two linked trials."""
    app, raw, team_id, trial_ids = trials_setup
    batch_id = uuid4()
    sync_engine = create_engine(postgres_url)
    sl = sessionmaker(sync_engine)
    with sl() as s:
        s.execute(insert(Batch).values(
            id=batch_id, team_id=team_id, name="qa-batch-161",
            task_filter={}, trial_config={},
        ))
        s.execute(
            update(Trial)
            .where(Trial.id.in_(trial_ids[:2]))
            .values(batch_id=batch_id),
        )
        s.commit()
    sync_engine.dispose()

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://svc",
    ) as ac:
        r = await ac.get(
            f"/api/v1/trials?batch_id={batch_id}",
            headers={"Authorization": f"Bearer {raw}"},
        )
    assert r.status_code == 200, r.text
    items = r.json()["items"]
    returned_ids = {it["id"] for it in items}
    assert returned_ids == {str(trial_ids[0]), str(trial_ids[1])}
