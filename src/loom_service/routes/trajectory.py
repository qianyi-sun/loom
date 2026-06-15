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
from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import RedirectResponse
from sqlalchemy import select

from loom.db.schema import Trial
from loom_service.auth_guards import (
    require_scope,
    require_team_or_admin,
)
from loom_service.dependencies import SessionAndCtx

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
    sc: SessionAndCtx,
    trial_id: UUID,
    cursor: Annotated[int, Query(ge=0)] = 0,
    limit: Annotated[int, Query(gt=0, le=1000)] = 200,
) -> dict[str, Any]:
    settings = request.app.state.settings
    s, ctx = sc
    require_scope(ctx, "read:own")
    trial = await _load_trial(s, trial_id, ctx)

    client = request.app.state.minio_client
    try:
        obj = client.get_object(
            Bucket=settings.trajectories_bucket,
            Key=_key(trial.team_id, trial.id),
        )
    except ClientError as exc:
        # A missing object means the trial hasn't written a first event
        # yet (queued/just-claimed) OR the worker crashed pre-first-event.
        # Either way we return an empty page rather than 404 — the trial
        # row exists (we already validated), so the UI's polling loop
        # should show "no events yet" not a scary 404. Other S3 errors
        # (perms, bucket missing) keep propagating.
        code = exc.response.get("Error", {}).get("Code")
        if code in ("NoSuchKey", "404"):
            return {"events": [], "next_cursor": None}
        raise
    # Stream-decode the JSONL line by line instead of materializing
    # the whole object into memory — a 100k-event trial would otherwise
    # cost ~200 MB raw + ~400 MB after split. `iter_lines()` lets us
    # skip lines up to `cursor`, decode `limit` events, and exit
    # early; remaining bytes stay on the wire (the response.close()
    # triggers a connection-close).
    body = obj["Body"]
    events: list[dict[str, Any]] = []
    next_cursor: int | None = None
    line_index = 0  # 1-based count of non-blank lines seen
    try:
        for raw in body.iter_lines():
            text = raw.decode("utf-8", errors="replace").strip()
            if not text:
                continue
            line_index += 1
            if line_index <= cursor:
                continue
            if len(events) >= limit:
                next_cursor = cursor + limit
                break
            try:
                events.append(json.loads(text))
            except json.JSONDecodeError:
                # Tolerate truncation tails — finalize crashes can
                # leave a partial last line.
                continue
    finally:
        body.close()
    return {"events": events, "next_cursor": next_cursor}


@router.get("/trials/{trial_id}/trajectory/download")
async def download_trajectory(
    request: Request,
    sc: SessionAndCtx,
    trial_id: UUID,
) -> RedirectResponse:
    settings = request.app.state.settings
    s, ctx = sc
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
