"""Trajectory paginated read + download route (Plan 18 Task 4).

`traj_setup` lives in `tests/integration/conftest.py` so both
trajectory + ATIF tests share it (and the underlying MinIO container).
"""

from __future__ import annotations

from datetime import UTC, datetime
from uuid import UUID, uuid4

import httpx
from fastapi import FastAPI
from sqlalchemy import create_engine, insert, select, update
from sqlalchemy.orm import sessionmaker

from loom.db.schema import Task, Trial


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


async def test_trajectory_object_missing_returns_empty_page(
    traj_setup: tuple[FastAPI, str, UUID, UUID],
    postgres_url: str,
) -> None:
    """A trial row exists but the trajectory object was never written
    (queued/just-claimed, or crashed pre-first-event); we return an
    empty page so the SPA shows "no events yet" rather than a 404."""
    app, raw, team_id, _trial_id = traj_setup
    bare_trial = uuid4()
    sync_engine = create_engine(postgres_url)
    sl = sessionmaker(sync_engine)
    with sl() as s:
        task_row_id = s.execute(
            select(Task.id).limit(1),
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
    assert r.status_code == 200
    assert r.json() == {"events": [], "next_cursor": None}


async def test_trajectory_download_proxies_object_through_service(
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

    assert r.status_code == 200
    assert "location" not in r.headers
    assert b'"kind": "trial_start"' in r.content


async def test_artifact_download_proxies_object_through_service(
    traj_setup: tuple[FastAPI, str, UUID, UUID],
    postgres_url: str,
) -> None:
    app, raw, team_id, trial_id = traj_setup
    settings = app.state.settings
    artifact_key = f"{team_id}/{trial_id}/main/result.txt"
    existing = {
        bucket["Name"]
        for bucket in app.state.minio_client.list_buckets()["Buckets"]
    }
    if settings.artifacts_bucket not in existing:
        app.state.minio_client.create_bucket(Bucket=settings.artifacts_bucket)
    app.state.minio_client.put_object(
        Bucket=settings.artifacts_bucket,
        Key=artifact_key,
        Body=b"hello artifact",
    )

    sync_engine = create_engine(postgres_url)
    sl = sessionmaker(sync_engine)
    with sl() as s:
        s.execute(
            update(Trial)
            .where(Trial.id == trial_id)
            .values(
                trajectory_index={
                    "artifacts": [
                        {
                            "step_name": "main",
                            "bucket": settings.artifacts_bucket,
                            "key": artifact_key,
                            "size": 14,
                        }
                    ],
                },
            ),
        )
        s.commit()
    sync_engine.dispose()

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://svc",
        follow_redirects=False,
    ) as ac:
        r = await ac.get(
            f"/api/v1/trials/{trial_id}/artifacts/download",
            params={"key": artifact_key},
            headers={"Authorization": f"Bearer {raw}"},
        )

    assert r.status_code == 200
    assert "location" not in r.headers
    assert r.content == b"hello artifact"
