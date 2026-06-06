"""Trajectory index PATCH + read endpoints."""

from __future__ import annotations

from typing import Any
from uuid import UUID

from fastapi import APIRouter, Header, HTTPException, Request
from sqlalchemy import bindparam
from sqlalchemy import text as sql_text
from sqlalchemy.dialects.postgresql import JSONB

from loom.auth import verify_bearer_token

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
