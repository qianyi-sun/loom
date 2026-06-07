"""Trajectory paginated read + download redirect (Plan 18 Task 4).

Uses a real MinIO container (testcontainers) so the boto3 client path
through `app.state.minio_client` exercises the same wire protocol the
production worker writes against.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import AsyncIterator, Iterator
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
from testcontainers.minio import MinioContainer

from loom.db.schema import Task, Team, TeamQuota, Token, Trial
from loom_service.app import create_app
from loom_service.config import LoomServiceSettings


@pytest.fixture(scope="module")
def minio() -> Iterator[MinioContainer]:
    with MinioContainer() as m:
        yield m


@pytest.fixture
async def traj_setup(
    monkeypatch: pytest.MonkeyPatch,
    postgres_url: str,
    minio: MinioContainer,
) -> AsyncIterator[tuple[FastAPI, str, UUID, UUID]]:
    cfg = minio.get_config()
    endpoint = f"http://{cfg['endpoint']}"
    for k, v in {
        "LOOM_SVC_DB_URL": postgres_url,
        "LOOM_SVC_MINIO_ENDPOINT": endpoint,
        "LOOM_SVC_MINIO_ACCESS_KEY": cfg["access_key"],
        "LOOM_SVC_MINIO_SECRET_KEY": cfg["secret_key"],
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
        endpoint_url=endpoint,
        aws_access_key_id=cfg["access_key"],
        aws_secret_access_key=cfg["secret_key"],
        region_name=settings.minio_region,
        config=Config(signature_version="s3v4"),
    )
    app.state.http_client = httpx.AsyncClient(
        base_url=str(settings.control_plane_url),
    )

    team_id = uuid4()
    raw = f"loom_team_{uuid4().hex}"
    task_id = f"local/task-{uuid4().hex[:8]}"
    trial_id = uuid4()
    now = datetime.now(UTC)

    sync_engine = create_engine(postgres_url)
    sl = sessionmaker(sync_engine)
    with sl() as s:
        s.execute(insert(Team).values(id=team_id, name=f"t-{team_id}"))
        s.execute(insert(Token).values(
            token_hash=hashlib.sha256(raw.encode()).digest(),
            type="team", scopes=["read:own"], team_id=team_id,
            issued_at=now, expires_at=None,
        ))
        s.execute(insert(Task).values(
            id=task_id, checksum="x" * 64, config={}, source="local",
        ))
        s.execute(insert(Trial).values(
            id=trial_id, task_id=task_id, team_id=team_id,
            state="succeeded", config={}, requires_caps={},
            submitted_at=now,
        ))
        s.commit()

    # Seed the trajectory bucket + events.jsonl object.
    if not minio.get_client().bucket_exists(settings.trajectories_bucket):
        minio.get_client().make_bucket(settings.trajectories_bucket)
    events = [
        {"kind": "trial_start", "trial_id": str(trial_id), "seq": 0},
        {"kind": "step_start", "trial_id": str(trial_id),
         "step_id": "main", "seq": 1},
        {"kind": "llm_call", "trial_id": str(trial_id),
         "step_id": "main", "seq": 2,
         "input_tokens": 100, "output_tokens": 50},
        {"kind": "step_end", "trial_id": str(trial_id),
         "step_id": "main", "seq": 3, "reward": 1.0},
        {"kind": "trial_end", "trial_id": str(trial_id), "seq": 4},
    ]
    body = ("\n".join(json.dumps(e) for e in events) + "\n").encode()
    app.state.minio_client.put_object(
        Bucket=settings.trajectories_bucket,
        Key=f"{team_id}/{trial_id}/events.jsonl",
        Body=body,
    )

    try:
        yield app, raw, team_id, trial_id
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


async def test_trajectory_paginates(
    traj_setup: tuple[FastAPI, str, UUID, UUID],
) -> None:
    app, raw, _team_id, trial_id = traj_setup
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://svc",
    ) as ac:
        r1 = await ac.get(
            f"/api/v1/trials/{trial_id}/trajectory?limit=2",
            headers={"Authorization": f"Bearer {raw}"},
        )
        j1 = r1.json()
        assert len(j1["events"]) == 2
        assert j1["events"][0]["kind"] == "trial_start"
        assert j1["next_cursor"] == 2

        r2 = await ac.get(
            f"/api/v1/trials/{trial_id}/trajectory?limit=10&"
            f"cursor={j1['next_cursor']}",
            headers={"Authorization": f"Bearer {raw}"},
        )
        j2 = r2.json()
    assert len(j2["events"]) == 3
    assert j2["next_cursor"] is None


async def test_trajectory_unknown_trial_404(
    traj_setup: tuple[FastAPI, str, UUID, UUID],
) -> None:
    app, raw, _team_id, _trial_id = traj_setup
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://svc",
    ) as ac:
        r = await ac.get(
            f"/api/v1/trials/{uuid4()}/trajectory",
            headers={"Authorization": f"Bearer {raw}"},
        )
    assert r.status_code == 404


async def test_trajectory_object_missing_404(
    traj_setup: tuple[FastAPI, str, UUID, UUID],
    postgres_url: str,
) -> None:
    """A trial row exists but the trajectory object was never written
    (e.g. crashed before first event); we 404 rather than 500."""
    app, raw, team_id, _trial_id = traj_setup
    bare_trial = uuid4()
    from sqlalchemy import select as sa_select
    sync_engine = create_engine(postgres_url)
    sl = sessionmaker(sync_engine)
    with sl() as s:
        # Reuse the existing task row to satisfy the FK.
        task_row_id = s.execute(
            sa_select(Task.id).limit(1),
        ).scalar_one()
        s.execute(insert(Trial).values(
            id=bare_trial, task_id=task_row_id, team_id=team_id,
            state="queued", config={}, requires_caps={},
            submitted_at=datetime.now(UTC),
        ))
        s.commit()
    sync_engine.dispose()
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://svc",
    ) as ac:
        r = await ac.get(
            f"/api/v1/trials/{bare_trial}/trajectory",
            headers={"Authorization": f"Bearer {raw}"},
        )
    assert r.status_code == 404


async def test_trajectory_download_302(
    traj_setup: tuple[FastAPI, str, UUID, UUID],
) -> None:
    app, raw, _team_id, trial_id = traj_setup
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://svc",
        follow_redirects=False,
    ) as ac:
        r = await ac.get(
            f"/api/v1/trials/{trial_id}/trajectory/download",
            headers={"Authorization": f"Bearer {raw}"},
        )
    assert r.status_code == 302
    assert "X-Amz-Signature" in r.headers["location"]
    assert "events.jsonl" in r.headers["location"]
