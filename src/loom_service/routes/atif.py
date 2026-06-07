"""ATIF presigned redirect (spec §5.2).

`finalize.py` writes the trial's ATIF document to the trajectories
bucket at `<team_id>/<trial_id>/atif.json`. This route 302-redirects
the caller to a presigned GET URL for that object.
"""

from __future__ import annotations

from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Header, HTTPException, Request
from fastapi.responses import RedirectResponse
from sqlalchemy import select

from loom.auth import verify_bearer_token
from loom.db.schema import Trial
from loom_service.auth_guards import (
    require_human_or_admin,
    require_scope,
    require_team_or_admin,
)

router = APIRouter()


@router.get("/trials/{trial_id}/atif")
async def atif_redirect(
    request: Request,
    trial_id: UUID,
    authorization: Annotated[str | None, Header()] = None,
) -> RedirectResponse:
    settings = request.app.state.settings
    async with request.app.state.session_factory() as s:
        ctx = await verify_bearer_token(s, authorization)
        ctx = require_human_or_admin(ctx)
        require_scope(ctx, "read:own")
        trial = (await s.execute(
            select(Trial).where(Trial.id == trial_id),
        )).scalar_one_or_none()
        if trial is None:
            raise HTTPException(status_code=404, detail="trial not found")
        require_team_or_admin(ctx, trial.team_id)

    url = request.app.state.minio_client.generate_presigned_url(
        "get_object",
        Params={
            "Bucket": settings.trajectories_bucket,
            "Key": f"{trial.team_id}/{trial.id}/atif.json",
        },
        ExpiresIn=settings.signed_url_expiry_sec,
    )
    return RedirectResponse(url=url, status_code=302)
