"""Trajectory index PATCH + read endpoints."""

from __future__ import annotations

from typing import Any
from uuid import UUID

from fastapi import APIRouter, Header, HTTPException, Request
from fastapi.responses import RedirectResponse
from sqlalchemy import bindparam, select
from sqlalchemy import text as sql_text
from sqlalchemy.dialects.postgresql import JSONB

from loom.auth import verify_bearer_token
from loom.db.schema import Trial as TrialRow

router = APIRouter()


_INDEX_PATCH = sql_text("""
UPDATE trials
   SET trajectory_index = :index_payload
 WHERE id = (:trial_id)::uuid AND worker_id = (:worker_id)::uuid
 RETURNING id;
""").bindparams(bindparam("index_payload", type_=JSONB))


@router.patch("/trials/{trial_id}/trajectory_index")
async def patch_trajectory_index(
    trial_id: UUID,
    request: Request,
    payload: dict[str, Any],
    authorization: str | None = Header(default=None),
) -> dict[str, str]:
    async with request.app.state.session_factory() as session:
        ctx = await verify_bearer_token(session, authorization)
    if ctx is None or "worker:index" not in ctx.scopes:
        raise HTTPException(status_code=401, detail="not authorized")

    try:
        worker_id = UUID(payload["worker_id"])
    except (KeyError, ValueError) as exc:
        raise HTTPException(
            status_code=400, detail=f"worker_id required: {exc}",
        ) from exc
    index_payload = {k: v for k, v in payload.items() if k != "worker_id"}

    async with request.app.state.session_factory() as session:
        row = (await session.execute(_INDEX_PATCH, {
            "trial_id": trial_id, "worker_id": worker_id,
            "index_payload": index_payload,
        })).mappings().one_or_none()
        await session.commit()
    if row is None:
        raise HTTPException(status_code=409, detail="worker lost claim")
    return {"trial_id": str(row["id"])}


@router.get("/trials/{trial_id}/trajectory")
async def get_trajectory_url(
    trial_id: UUID,
    request: Request,
    authorization: str | None = Header(default=None),
) -> RedirectResponse:
    async with request.app.state.session_factory() as session:
        ctx = await verify_bearer_token(session, authorization)
    if ctx is None:
        raise HTTPException(status_code=401, detail="not authorized")
    async with request.app.state.session_factory() as session:
        row = (await session.execute(
            select(TrialRow).where(TrialRow.id == trial_id),
        )).scalar_one_or_none()
    if row is None or not row.trajectory_index:
        raise HTTPException(status_code=404, detail="no trajectory recorded")
    if ctx.team_id is not None and row.team_id != ctx.team_id:
        raise HTTPException(
            status_code=403, detail="trajectory belongs to another team",
        )

    settings = request.app.state.settings
    url = request.app.state.minio_client.generate_presigned_url(
        "get_object",
        Params={
            "Bucket": "trajectories",
            "Key": f"{row.team_id}/{trial_id}/events.jsonl",
        },
        ExpiresIn=settings.signed_url_expiry_sec,
    )
    return RedirectResponse(url=url, status_code=302)
