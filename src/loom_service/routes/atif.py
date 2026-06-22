"""ATIF authenticated download (spec §5.2).

`finalize.py` writes the trial's ATIF document to the trajectories
bucket at `<team_id>/<trial_id>/atif.json`. This route proxies the
object through loom_service so browser clients only need API access,
not direct MinIO access.
"""

from __future__ import annotations

from uuid import UUID

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import StreamingResponse
from sqlalchemy import select

from loom.db.schema import Trial
from loom_service.auth_guards import (
    require_scope,
    require_team_or_admin,
)
from loom_service.dependencies import SessionAndCtx
from loom_service.routes.object_downloads import stream_object_response

router = APIRouter()


@router.get("/trials/{trial_id}/atif")
async def download_atif(
    request: Request,
    sc: SessionAndCtx,
    trial_id: UUID,
) -> StreamingResponse:
    settings = request.app.state.settings
    s, ctx = sc
    require_scope(ctx, "read:own")
    trial = (await s.execute(
        select(Trial).where(Trial.id == trial_id),
    )).scalar_one_or_none()
    if trial is None:
        raise HTTPException(status_code=404, detail="trial not found")
    require_team_or_admin(ctx, trial.team_id)

    return stream_object_response(
        client=request.app.state.minio_client,
        bucket=settings.trajectories_bucket,
        key=f"{trial.team_id}/{trial.id}/atif.json",
        filename=f"{trial.id}-atif.json",
        artifact_kind="atif",
        media_type="application/json",
    )
