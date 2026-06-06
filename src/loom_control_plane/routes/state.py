"""Fenced state PATCH (spec §2.8).

The fencing predicate (`worker_id = :worker_id` in the UPDATE WHERE clause)
makes this safe under crash-detector reclaim: if the trial has been
reassigned to a different worker, the UPDATE matches 0 rows → the route
returns 409 + `WorkerLostClaim` semantics.

Also enforces:
- Bug 1: state/failure_reason values are validated against TrialState +
  FailureReason enums (no arbitrary strings into the DB).
- Bug 4: source state is restricted per target — succeeded/failed only from
  claimed/running, etc. — no `succeeded → queued` reversals.
"""

from __future__ import annotations

from typing import Any
from uuid import UUID

from fastapi import APIRouter, Header, HTTPException, Request
from sqlalchemy import text

from loom.auth import verify_bearer_token
from loom.models.result import FailureReason, TrialState

router = APIRouter()

# Allowed source states for each target state. Targets not in this map
# (e.g., queued, claimed) cannot be reached via PATCH /state.
_ALLOWED_FROM: dict[TrialState, set[TrialState]] = {
    TrialState.RUNNING: {TrialState.CLAIMED, TrialState.RUNNING},
    TrialState.SUCCEEDED: {TrialState.CLAIMED, TrialState.RUNNING},
    TrialState.FAILED: {TrialState.CLAIMED, TrialState.RUNNING},
    TrialState.CANCELLED: {
        TrialState.QUEUED, TrialState.CLAIMED, TrialState.RUNNING,
    },
}

_PATCH_SQL = text("""
UPDATE trials
   SET state = (:new_state)::text,
       failure_reason = (:failure_reason)::text,
       finished_at = CASE WHEN (:is_terminal)::boolean
                           THEN NOW() ELSE finished_at END,
       started_at = CASE WHEN (:new_state)::text = 'running' AND started_at IS NULL
                          THEN NOW() ELSE started_at END
 WHERE id = (:trial_id)::uuid
   AND worker_id = (:worker_id)::uuid
   AND state = ANY(:allowed_from)
 RETURNING id, state;
""")

_TERMINAL = {
    TrialState.SUCCEEDED, TrialState.FAILED, TrialState.CANCELLED,
}


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
        new_state_str = payload["state"]
        worker_id = UUID(payload["worker_id"])
    except (KeyError, ValueError) as exc:
        raise HTTPException(
            status_code=400, detail=f"state + worker_id required: {exc}",
        ) from exc

    # Bug 1 fix: validate state against the TrialState enum upfront so we
    # never write garbage to the DB.
    try:
        new_state = TrialState(new_state_str)
    except ValueError as exc:
        raise HTTPException(
            status_code=400,
            detail=f"invalid state {new_state_str!r}; "
                   f"must be one of {sorted(s.value for s in TrialState)}",
        ) from exc
    if new_state not in _ALLOWED_FROM:
        raise HTTPException(
            status_code=400,
            detail=f"state {new_state_str!r} cannot be reached via PATCH",
        )

    failure_reason_str = payload.get("failure_reason")
    if failure_reason_str is not None:
        try:
            FailureReason(failure_reason_str)
        except ValueError as exc:
            raise HTTPException(
                status_code=400,
                detail=f"invalid failure_reason {failure_reason_str!r}",
            ) from exc

    allowed_from = sorted(s.value for s in _ALLOWED_FROM[new_state])

    async with request.app.state.session_factory() as session:
        row = (await session.execute(_PATCH_SQL, {
            "trial_id": trial_id,
            "worker_id": worker_id,
            "new_state": new_state.value,
            "failure_reason": failure_reason_str,
            "is_terminal": new_state in _TERMINAL,
            "allowed_from": allowed_from,
        })).mappings().one_or_none()
        await session.commit()

    if row is None:
        raise HTTPException(
            status_code=409,
            detail=(
                "worker lost claim, or trial is not in a state from which "
                f"the {new_state.value} transition is allowed"
            ),
        )

    return {"trial_id": str(row["id"]), "state": row["state"]}
