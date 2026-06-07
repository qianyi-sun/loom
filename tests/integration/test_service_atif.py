"""ATIF redirect (Plan 18 Task 5)."""

from __future__ import annotations

from uuid import UUID, uuid4

import httpx
from fastapi import FastAPI


async def test_atif_redirects_to_presigned_url(
    traj_setup: tuple[FastAPI, str, UUID, UUID],
) -> None:
    app, raw, _team_id, trial_id = traj_setup
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://svc",
        follow_redirects=False,
    ) as ac:
        r = await ac.get(
            f"/api/v1/trials/{trial_id}/atif",
            headers={"Authorization": f"Bearer {raw}"},
        )
    assert r.status_code == 302
    loc = r.headers["location"]
    assert "X-Amz-Signature" in loc
    assert "atif.json" in loc
    assert str(trial_id) in loc


async def test_atif_unknown_trial_404(
    traj_setup: tuple[FastAPI, str, UUID, UUID],
) -> None:
    app, raw, _team_id, _trial_id = traj_setup
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://svc",
        follow_redirects=False,
    ) as ac:
        r = await ac.get(
            f"/api/v1/trials/{uuid4()}/atif",
            headers={"Authorization": f"Bearer {raw}"},
        )
    assert r.status_code == 404
