"""ATIF redirect (Plan 18 Task 5)."""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock
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


async def test_atif_redirect_uses_public_presign_client(
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
            f"/api/v1/trials/{trial_id}/atif",
            headers={"Authorization": f"Bearer {raw}"},
        )
    assert r.status_code == 302
    loc = r.headers["location"]
    assert loc.startswith("http://localhost:9000/")
    assert "atif.json" in loc
    assert "X-Amz-Signature=publicsig" in loc
    assert "minio:9000" not in loc
    public_presign_client.generate_presigned_url.assert_called_once()


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
