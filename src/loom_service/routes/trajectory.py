"""Trajectory paginated read + download redirect (spec §5.2).

The events.jsonl object lives in the same `trajectories` bucket the
worker's TrajectoryWriter writes to, at the key
`<team_id>/<trial_id>/events.jsonl`. We fetch the whole object, split
on newlines, slice by integer cursor (line index), and return the
requested page. This is fine for v1 (trajectory files are bounded by
the trial wall budget and event size); future revisions could move
to byte-range reads + sidecar index.
"""

from __future__ import annotations

import json
from typing import Annotated, Any, cast
from uuid import UUID

from botocore.exceptions import ClientError
from fastapi import APIRouter, Header, HTTPException, Query, Request
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


def _key(team_id: UUID, trial_id: UUID) -> str:
    return f"{team_id}/{trial_id}/events.jsonl"


async def _load_trial(session: Any, trial_id: UUID, ctx: Any) -> Trial:
    trial = (await session.execute(
        select(Trial).where(Trial.id == trial_id),
    )).scalar_one_or_none()
    if trial is None:
        raise HTTPException(status_code=404, detail="trial not found")
    require_team_or_admin(ctx, trial.team_id)
    return cast(Trial, trial)


@router.get("/trials/{trial_id}/trajectory")
async def list_events(
    request: Request,
    trial_id: UUID,
    cursor: Annotated[int, Query(ge=0)] = 0,
    limit: Annotated[int, Query(gt=0, le=1000)] = 200,
    authorization: Annotated[str | None, Header()] = None,
) -> dict[str, Any]:
    settings = request.app.state.settings
    async with request.app.state.session_factory() as s:
        ctx = await verify_bearer_token(s, authorization)
        ctx = require_human_or_admin(ctx)
        require_scope(ctx, "read:own")
        trial = await _load_trial(s, trial_id, ctx)

    client = request.app.state.minio_client
    try:
        obj = client.get_object(
            Bucket=settings.trajectories_bucket,
            Key=_key(trial.team_id, trial.id),
        )
    except ClientError as exc:
        raise HTTPException(
            status_code=404, detail="trajectory not found",
        ) from exc
    body = obj["Body"].read().decode()
    lines = [ln for ln in body.split("\n") if ln.strip()]

    events: list[dict[str, Any]] = []
    next_cursor: int | None = None
    end = min(cursor + limit, len(lines))
    for i in range(cursor, end):
        try:
            events.append(json.loads(lines[i]))
        except json.JSONDecodeError:
            # Skip malformed lines; the writer is supposed to emit
            # one well-formed JSON object per line, but tolerate
            # truncation tails.
            continue
    if end < len(lines):
        next_cursor = end
    return {"events": events, "next_cursor": next_cursor}


@router.get("/trials/{trial_id}/trajectory/download")
async def download_trajectory(
    request: Request,
    trial_id: UUID,
    authorization: Annotated[str | None, Header()] = None,
) -> RedirectResponse:
    settings = request.app.state.settings
    async with request.app.state.session_factory() as s:
        ctx = await verify_bearer_token(s, authorization)
        ctx = require_human_or_admin(ctx)
        require_scope(ctx, "read:own")
        trial = await _load_trial(s, trial_id, ctx)

    url = request.app.state.minio_client.generate_presigned_url(
        "get_object",
        Params={
            "Bucket": settings.trajectories_bucket,
            "Key": _key(trial.team_id, trial.id),
        },
        ExpiresIn=settings.signed_url_expiry_sec,
    )
    return RedirectResponse(url=url, status_code=302)
