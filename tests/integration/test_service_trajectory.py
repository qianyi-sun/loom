"""Trajectory paginated read + download redirect (Plan 18 Task 4).

`traj_setup` lives in `tests/integration/conftest.py` so both
trajectory + ATIF tests share it (and the underlying MinIO container).
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from unittest.mock import MagicMock
from uuid import UUID, uuid4

import httpx
from fastapi import FastAPI
from sqlalchemy import create_engine, insert, select
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


async def test_trajectory_download_uses_public_presign_client(
    traj_setup: tuple[FastAPI, str, UUID, UUID],
) -> None:
    app, raw, _team_id, trial_id = traj_setup
    public_presign_client = MagicMock()

    def _public_url(
        _op: str, *, Params: dict[str, Any], ExpiresIn: int,  # noqa: N803
    ) -> str:
        assert ExpiresIn == 3600
        return (
            "http://localhost:9000/{}/{}"
            "?X-Amz-SignedHeaders=host&X-Amz-Signature=publicsig"
        ).format(
            Params["Bucket"],
            Params["Key"],
        )

    public_presign_client.generate_presigned_url.side_effect = _public_url
    app.state.minio_presign_client = public_presign_client

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
    loc = r.headers["location"]
    assert loc.startswith("http://localhost:9000/")
    assert "events.jsonl" in loc
    assert "X-Amz-Signature=publicsig" in loc
    assert "minio:9000" not in loc
    public_presign_client.generate_presigned_url.assert_called_once()
