"""Campaigns CRUD (spec §5.3 / Plan 19).

Routes:
- POST /api/v1/campaigns          — create + immediately materialize
                                    expected_trial_count from task_filter
- GET  /api/v1/campaigns          — list with cursor pagination
- GET  /api/v1/campaigns/{id}     — detail + trial roll-up (state summary,
                                    aggregate reward, total cost) extracted
                                    from Trial.result JSONB
- POST /api/v1/campaigns/{id}/cancel — terminate the campaign + cascade-cancel
                                    its still-active trials
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from typing import Annotated, Any
from uuid import UUID

from fastapi import APIRouter, Header, HTTPException, Query, Request
from pydantic import BaseModel, Field
from sqlalchemy import and_, func, or_, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from loom.auth import verify_bearer_token
from loom.db.schema import Campaign, Task, Trial
from loom_service.auth_guards import (
    is_admin,
    require_human_or_admin,
    require_scope,
    require_team_or_admin,
)
from loom_service.pagination import Cursor, decode_cursor, encode_cursor

router = APIRouter()


class _CreateCampaign(BaseModel):
    name: str = Field(min_length=1, max_length=200)
    description: str | None = None
    task_filter: dict[str, Any]
    trial_config: dict[str, Any]


# Recognized task_filter keys. Anything else is rejected so a typo
# (`liscense` instead of `license`) doesn't silently match nothing.
_FILTER_KEYS: frozenset[str] = frozenset(
    {"license", "task_ids", "benchmark_id"},
)


async def _resolve_task_filter(
    session: AsyncSession, task_filter: Mapping[str, Any],
) -> list[str]:
    """Materialize a task_filter into a list of Task.id strings.

    Note: in the real v0.7 schema Task.id is a string (e.g.
    `humaneval/HumanEval/0`), not a UUID — the plan-doc drift.
    """
    unknown = set(task_filter.keys()) - _FILTER_KEYS
    if unknown:
        raise HTTPException(
            status_code=400,
            detail=f"unknown task_filter keys: {sorted(unknown)}",
        )
    stmt = select(Task.id)
    if "license" in task_filter:
        stmt = stmt.where(Task.license == task_filter["license"])
    if "task_ids" in task_filter:
        ids = [str(x) for x in task_filter["task_ids"]]
        stmt = stmt.where(Task.id.in_(ids))
    if "benchmark_id" in task_filter:
        stmt = stmt.where(Task.benchmark_id == task_filter["benchmark_id"])
    return [row[0] for row in (await session.execute(stmt)).all()]


def _serialize(
    c: Campaign,
    *,
    summary: dict[str, int] | None = None,
    aggregate_reward: float | None = None,
    total_cost_usd: float = 0.0,
) -> dict[str, Any]:
    out: dict[str, Any] = {
        "id": str(c.id),
        "team_id": str(c.team_id),
        "name": c.name,
        "description": c.description,
        "task_filter": c.task_filter,
        "trial_config": c.trial_config,
        "state": c.state,
        "created_at": c.created_at.isoformat(),
        "finished_at": (
            c.finished_at.isoformat() if c.finished_at else None
        ),
        "created_by_token_prefix": c.created_by_token_prefix,
        "expected_trial_count": c.expected_trial_count,
    }
    if summary is not None:
        out["trial_summary"] = summary
        out["aggregate_reward"] = aggregate_reward
        out["total_cost_usd"] = total_cost_usd
    return out


@router.post("/campaigns", status_code=201)
async def create_campaign(
    request: Request,
    payload: _CreateCampaign,
    authorization: Annotated[str | None, Header()] = None,
) -> dict[str, Any]:
    async with request.app.state.session_factory() as s:
        ctx = await verify_bearer_token(s, authorization)
        ctx = require_human_or_admin(ctx)
        require_scope(ctx, "submit")
        if ctx.team_id is None:
            raise HTTPException(
                status_code=400,
                detail="admin tokens must scope campaigns to a team — "
                       "use the service's per-team admin token",
            )

        task_ids = await _resolve_task_filter(s, payload.task_filter)
        token_prefix = (
            ctx.token_hash.hex()[:8] if ctx.token_hash else "00000000"
        )
        c = Campaign(
            team_id=ctx.team_id,
            name=payload.name,
            description=payload.description,
            task_filter=payload.task_filter,
            trial_config=payload.trial_config,
            state="submitted",
            created_by_token_prefix=token_prefix,
            expected_trial_count=len(task_ids),
        )
        s.add(c)
        await s.commit()
        await s.refresh(c)
        return {
            "campaign_id": str(c.id),
            "expected_trial_count": len(task_ids),
            "state": c.state,
            "created_at": c.created_at.isoformat(),
        }


@router.get("/campaigns")
async def list_campaigns(
    request: Request,
    team_id: Annotated[UUID | None, Query()] = None,
    state: Annotated[
        str | None,
        Query(description="comma-separated state filter"),
    ] = None,
    cursor: Annotated[str | None, Query()] = None,
    limit: Annotated[int, Query(gt=0, le=200)] = 50,
    authorization: Annotated[str | None, Header()] = None,
) -> dict[str, Any]:
    async with request.app.state.session_factory() as s:
        ctx = await verify_bearer_token(s, authorization)
        ctx = require_human_or_admin(ctx)
        require_scope(ctx, "read:own")

        target_team = team_id
        if target_team is not None:
            require_team_or_admin(ctx, target_team)
        elif not is_admin(ctx):
            target_team = ctx.team_id

        stmt = select(Campaign).order_by(
            Campaign.created_at.desc(), Campaign.id.desc(),
        )
        if target_team is not None:
            stmt = stmt.where(Campaign.team_id == target_team)
        if state:
            wanted = [x.strip() for x in state.split(",") if x.strip()]
            if wanted:
                stmt = stmt.where(Campaign.state.in_(wanted))
        if cursor:
            try:
                cur = decode_cursor(cursor)
            except ValueError as exc:
                raise HTTPException(
                    status_code=400, detail=str(exc),
                ) from exc
            stmt = stmt.where(
                or_(
                    Campaign.created_at < cur.submitted_at,
                    and_(
                        Campaign.created_at == cur.submitted_at,
                        Campaign.id < cur.id,
                    ),
                ),
            )
        stmt = stmt.limit(limit + 1)
        rows: Sequence[Campaign] = (
            await s.execute(stmt)
        ).scalars().all()

    items = list(rows)
    if len(items) > limit:
        items = items[:limit]
        last = items[-1]
        next_cursor: str | None = encode_cursor(
            Cursor(submitted_at=last.created_at, id=last.id),
        )
    else:
        next_cursor = None
    return {
        "items": [_serialize(r) for r in items],
        "next_cursor": next_cursor,
    }


def _rollup_from_result(result: dict[str, Any] | None) -> tuple[float | None, float]:
    """Pull (reward, cost) out of a Trial.result JSONB. Same logic as
    routes/trials.py — kept inline here so the rollup query can flow
    naturally."""
    if not result:
        return None, 0.0
    reward = result.get("aggregate_reward")
    if reward is None:
        reward = result.get("reward")
    try:
        reward_f = float(reward) if reward is not None else None
    except (TypeError, ValueError):
        reward_f = None
    cost = result.get("cost_usd", 0)
    try:
        cost_f = float(cost or 0)
    except (TypeError, ValueError):
        cost_f = 0.0
    return reward_f, cost_f


@router.get("/campaigns/{campaign_id}")
async def get_campaign(
    request: Request,
    campaign_id: UUID,
    authorization: Annotated[str | None, Header()] = None,
) -> dict[str, Any]:
    async with request.app.state.session_factory() as s:
        ctx = await verify_bearer_token(s, authorization)
        ctx = require_human_or_admin(ctx)
        require_scope(ctx, "read:own")
        c = (await s.execute(
            select(Campaign).where(Campaign.id == campaign_id),
        )).scalar_one_or_none()
        if c is None:
            raise HTTPException(
                status_code=404, detail="campaign not found",
            )
        require_team_or_admin(ctx, c.team_id)

        # Per-state counts come from a single GROUP BY query.
        state_counts = (await s.execute(
            select(Trial.state, func.count(Trial.id))
            .where(Trial.campaign_id == campaign_id)
            .group_by(Trial.state),
        )).all()
        summary: dict[str, int] = {
            k: 0 for k in (
                "queued", "claimed", "running",
                "succeeded", "failed", "cancelled",
            )
        }
        for st, n in state_counts:
            summary[str(st)] = int(n)

        # Reward + cost are inside Trial.result JSONB (no top-level
        # columns in v0.7). Pull every finished row's result and roll
        # up in Python — finished-trial count is bounded by
        # expected_trial_count which the campaign already knows.
        results = (await s.execute(
            select(Trial.result).where(
                and_(
                    Trial.campaign_id == campaign_id,
                    Trial.state.in_(["succeeded", "failed"]),
                ),
            ),
        )).scalars().all()
        reward_sum = 0.0
        reward_n = 0
        cost_total = 0.0
        for r in results:
            rew, cost = _rollup_from_result(r)
            cost_total += cost
            if rew is not None:
                reward_sum += rew
                reward_n += 1
        avg_reward = (reward_sum / reward_n) if reward_n > 0 else None

    return _serialize(
        c,
        summary=summary,
        aggregate_reward=avg_reward,
        total_cost_usd=cost_total,
    )


@router.post("/campaigns/{campaign_id}/cancel")
async def cancel_campaign(
    request: Request,
    campaign_id: UUID,
    authorization: Annotated[str | None, Header()] = None,
) -> dict[str, Any]:
    async with request.app.state.session_factory() as s:
        ctx = await verify_bearer_token(s, authorization)
        ctx = require_human_or_admin(ctx)
        require_scope(ctx, "submit")
        c = (await s.execute(
            select(Campaign).where(Campaign.id == campaign_id),
        )).scalar_one_or_none()
        if c is None:
            raise HTTPException(
                status_code=404, detail="campaign not found",
            )
        require_team_or_admin(ctx, c.team_id)
        now = datetime.now(UTC)
        await s.execute(
            update(Campaign)
            .where(Campaign.id == campaign_id)
            .values(state="cancelled", finished_at=now),
        )
        # Cascade-cancel still-active trials in this campaign. We do
        # NOT cancel queued trials whose worker may already be partway
        # through claim; the CP's existing cancel endpoint (Plan 5)
        # handles graceful interruption when called per-trial. Here we
        # just transition the rows to `cancelled` so the SPA stops
        # showing them as in-flight.
        await s.execute(
            update(Trial)
            .where(
                and_(
                    Trial.campaign_id == campaign_id,
                    Trial.state.in_(["queued", "claimed", "running"]),
                ),
            )
            .values(
                state="cancelled",
                cancellation_requested_at=now,
                finished_at=now,
            ),
        )
        await s.commit()
    return {"campaign_id": str(campaign_id), "state": "cancelled"}
