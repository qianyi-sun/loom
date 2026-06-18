"""ATIF presigned redirect (spec §5.2).

`finalize.py` writes the trial's ATIF document to the trajectories
bucket at `<team_id>/<trial_id>/atif.json`. This route 302-redirects
the caller to a presigned GET URL for that object.
"""

from __future__ import annotations

from uuid import UUID

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import RedirectResponse
from sqlalchemy import select

from loom.db.schema import Trial
from loom_service.auth_guards import (
    require_scope,
    require_team_or_admin,
)
from loom_service.dependencies import SessionAndCtx
from loom_service.storage import get_minio_presign_client

router = APIRouter()


@router.get("/trials/{trial_id}/atif")
async def atif_redirect(
    request: Request,
    sc: SessionAndCtx,
    trial_id: UUID,
) -> RedirectResponse:
    settings = request.app.state.settings
    s, ctx = sc
    require_scope(ctx, "read:own")
    trial = (await s.execute(
        select(Trial).where(Trial.id == trial_id),
    )).scalar_one_or_none()
    if trial is None:
        raise HTTPException(status_code=404, detail="trial not found")
    require_team_or_admin(ctx, trial.team_id)

    url = get_minio_presign_client(
        request.app.state,
    ).generate_presigned_url(
        "get_object",
        Params={
            "Bucket": settings.trajectories_bucket,
            "Key": f"{trial.team_id}/{trial.id}/atif.json",
        },
        ExpiresIn=settings.signed_url_expiry_sec,
    )
    return RedirectResponse(url=url, status_code=302)
