"""Fenced state PATCH (spec §2.8).

The fencing predicate (`worker_id = :worker_id` in the UPDATE WHERE clause)
is what makes this safe under crash-detector reclaim: if the trial has
been reassigned to a different worker, the UPDATE matches 0 rows → the
route returns 409 + `WorkerLostClaim` semantics.
"""

from __future__ import annotations

from typing import Any
from uuid import UUID

from fastapi import APIRouter, Header, HTTPException, Request
from sqlalchemy import text

from loom.auth import verify_bearer_token

router = APIRouter()

_PATCH_SQL = text("""
UPDATE trials
   SET state = (:new_state)::text,
       failure_reason = (:failure_reason)::text,
       finished_at = CASE WHEN (:is_terminal)::boolean
                           THEN NOW() ELSE finished_at END,
       started_at = CASE WHEN (:new_state)::text = 'running' AND started_at IS NULL
                          THEN NOW() ELSE started_at END
 WHERE id = (:trial_id)::uuid AND worker_id = (:worker_id)::uuid
 RETURNING id, state;
""")

_TERMINAL = {"succeeded", "failed", "cancelled"}


@router.patch("/trials/{trial_id}/state")
async def patch_state(
    trial_id: UUID,
    request: Request,
    payload: dict[str, Any],
    authorization: str | None = Header(default=None),
) -> dict[str, Any]:
    async with request.app.state.session_factory() as session:
        ctx = await verify_bearer_token(session, authorization)
    if ctx is None or "worker:report" not in ctx.scopes:
        raise HTTPException(status_code=401, detail="not authorized")

    try:
        new_state = payload["state"]
        worker_id = UUID(payload["worker_id"])
    except (KeyError, ValueError) as exc:
        raise HTTPException(
            status_code=400, detail=f"state + worker_id required: {exc}",
        ) from exc
    failure_reason = payload.get("failure_reason")

    async with request.app.state.session_factory() as session:
        row = (await session.execute(_PATCH_SQL, {
            "trial_id": trial_id,
            "worker_id": worker_id,
            "new_state": new_state,
            "failure_reason": failure_reason,
            "is_terminal": new_state in _TERMINAL,
        })).mappings().one_or_none()
        await session.commit()

    if row is None:
        raise HTTPException(
            status_code=409,
            detail="worker lost claim — trial no longer owned by this worker",
        )

    return {"trial_id": str(row["id"]), "state": row["state"]}
