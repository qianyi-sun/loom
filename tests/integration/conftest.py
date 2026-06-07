"""Shared service-integration fixtures.

`minio` (session-scoped) brings up a single MinIO container for the
suite. `traj_setup` is the common service-app + seeded trial + trajectory
fixture used by trajectory + ATIF tests in this package.
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
def shared_minio() -> Iterator[MinioContainer]:
    """Module-scoped MinIO so we don't pay container-start cost per
    test. Routes that exercise the boto3 path (trajectory, atif)
    share this."""
    with MinioContainer() as m:
        yield m


@pytest.fixture
async def traj_setup(
    monkeypatch: pytest.MonkeyPatch,
    postgres_url: str,
    shared_minio: MinioContainer,
) -> AsyncIterator[tuple[FastAPI, str, UUID, UUID]]:
    cfg = shared_minio.get_config()
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

    # Seed events.jsonl + atif.json in the trajectories bucket.
    if not shared_minio.get_client().bucket_exists(
        settings.trajectories_bucket,
    ):
        shared_minio.get_client().make_bucket(settings.trajectories_bucket)
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
    prefix = f"{team_id}/{trial_id}"
    app.state.minio_client.put_object(
        Bucket=settings.trajectories_bucket,
        Key=f"{prefix}/events.jsonl",
        Body=body,
    )
    app.state.minio_client.put_object(
        Bucket=settings.trajectories_bucket,
        Key=f"{prefix}/atif.json",
        Body=b'{"version": "1.7", "trial_id": "x"}',
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
