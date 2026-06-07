"""Trials read routes + write forwarders (spec §5.1).

Read surface:
- GET /api/v1/trials             — list with cursor pagination + filters
- GET /api/v1/trials/{id}        — detail + presigned ATIF + trajectory URLs

Write forwarders (Task 8):
- POST /api/v1/trials            — proxies to Control Plane /trials
- POST /api/v1/trials/{id}/cancel — proxies to Control Plane /trials/{id}/cancel

Field extraction notes: the v0.7 `trials` table does NOT carry
`aggregate_reward`, `cost_usd`, or `campaign_id` columns. Reward + cost
are extracted from `Trial.result` (the JSONB the worker writes at
finalize). Agent name + model are pulled from `Trial.config["agent"]`.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Annotated, Any
from uuid import UUID

from fastapi import APIRouter, Header, HTTPException, Query, Request
from sqlalchemy import and_, or_, select

from loom.auth import verify_bearer_token
from loom.db.schema import Trial
from loom_service.auth_guards import (
    is_admin,
    require_human_or_admin,
    require_scope,
    require_team_or_admin,
)
from loom_service.pagination import Cursor, decode_cursor, encode_cursor

router = APIRouter()


def _extract_reward(result: dict[str, Any] | None) -> float | None:
    """Pull aggregate reward out of the worker-written result JSONB.
    The worker stores the multi-step combined reward as
    `result["aggregate_reward"]` (Plan 3 contract); fall through to
    `result["reward"]` for single-step trials that predate that key."""
    if not result:
        return None
    val = result.get("aggregate_reward")
    if val is None:
        val = result.get("reward")
    return float(val) if val is not None else None


def _extract_cost(result: dict[str, Any] | None) -> float:
    """Total cost in USD across all LLM calls; 0.0 if absent."""
    if not result:
        return 0.0
    val = result.get("cost_usd", 0)
    if isinstance(val, Decimal):
        return float(val)
    return float(val or 0)


def _trial_row(t: Trial) -> dict[str, Any]:
    agent = (t.config or {}).get("agent") or {}
    return {
        "id": str(t.id),
        "task_id": t.task_id,
        "team_id": str(t.team_id),
        "state": t.state,
        "failure_reason": t.failure_reason,
        "submitted_at": t.submitted_at.isoformat(),
        "started_at": t.started_at.isoformat() if t.started_at else None,
        "finished_at": (
            t.finished_at.isoformat() if t.finished_at else None
        ),
        "attempt_count": t.attempt_count,
        "aggregate_reward": _extract_reward(t.result),
        "cost_usd": _extract_cost(t.result),
        "agent_name": agent.get("name"),
        "model": agent.get("model"),
    }


@router.get("/trials")
async def list_trials(
    request: Request,
    team_id: UUID | None = Query(default=None),
    task_id: str | None = Query(default=None),
    state: str | None = Query(
        default=None, description="comma-separated state filter",
    ),
    cursor: str | None = Query(default=None),
    limit: int = Query(default=50, gt=0, le=200),
    authorization: Annotated[str | None, Header()] = None,
) -> dict[str, Any]:
    async with request.app.state.session_factory() as s:
        ctx = await verify_bearer_token(s, authorization)
        ctx = require_human_or_admin(ctx)
        require_scope(ctx, "read:own")

        # Resolve the team filter:
        # - explicit `team_id` query → require_team_or_admin
        # - no filter + non-admin → scope to caller's own team
        # - no filter + admin → no team filter
        target_team = team_id
        if target_team is not None:
            require_team_or_admin(ctx, target_team)
        elif not is_admin(ctx):
            target_team = ctx.team_id

        stmt = select(Trial).order_by(
            Trial.submitted_at.desc(), Trial.id.desc(),
        )
        if target_team is not None:
            stmt = stmt.where(Trial.team_id == target_team)
        if task_id is not None:
            stmt = stmt.where(Trial.task_id == task_id)
        if state:
            wanted = [x.strip() for x in state.split(",") if x.strip()]
            if wanted:
                stmt = stmt.where(Trial.state.in_(wanted))
        if cursor:
            try:
                c = decode_cursor(cursor)
            except ValueError as exc:
                raise HTTPException(
                    status_code=400, detail=str(exc),
                ) from exc
            # Composite (submitted_at, id) key: rows strictly LESS than
            # the cursor (DESC ordering).
            stmt = stmt.where(
                or_(
                    Trial.submitted_at < c.submitted_at,
                    and_(
                        Trial.submitted_at == c.submitted_at,
                        Trial.id < c.id,
                    ),
                ),
            )
        stmt = stmt.limit(limit + 1)
        rows = list((await s.execute(stmt)).scalars().all())

    if len(rows) > limit:
        rows = rows[:limit]
        last = rows[-1]
        next_c = encode_cursor(
            Cursor(submitted_at=last.submitted_at, id=last.id),
        )
    else:
        next_c = None
    return {
        "items": [_trial_row(r) for r in rows],
        "next_cursor": next_c,
    }
