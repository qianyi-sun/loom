"""ATIF download route."""

from __future__ import annotations

from uuid import UUID, uuid4

import httpx
from fastapi import FastAPI


async def test_atif_download_proxies_object_through_service(
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

    assert r.status_code == 200
    assert "location" not in r.headers
    assert r.json()["trial_id"] == "x"


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
