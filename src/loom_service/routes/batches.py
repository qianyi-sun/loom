"""Batches CRUD (spec §5.3 / Plan 19, renamed in Plan 28).

Routes:
- POST /api/v1/batches          — create + immediately materialize
                                  expected_trial_count from task_filter
- GET  /api/v1/batches          — list with cursor pagination
- GET  /api/v1/batches/{id}     — detail + trial roll-up (state summary,
                                  aggregate reward, total cost) extracted
                                  from Trial.result JSONB
- POST /api/v1/batches/{id}/cancel — terminate the batch + cascade-cancel
                                  its still-active trials
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import UTC, datetime
from typing import Annotated, Any
from uuid import UUID

from fastapi import APIRouter, HTTPException, Query, Request
from pydantic import BaseModel, Field
from sqlalchemy import and_, func, or_, select, update

from loom.db.schema import Batch, Trial
from loom.models.batch import Combination
from loom.models.types import ModelSpec
from loom_service.agent_catalog import (
    known_names,
    validate_agent_model_compat,
)
from loom_service.auth_guards import (
    is_admin,
    require_scope,
    require_team_or_admin,
)
from loom_service.dependencies import SessionAndCtx
from loom_service.pagination import Cursor, decode_cursor, encode_cursor
from loom_service.provider_connection_lookup import validate_provider_connection
from loom_service.task_filter import resolve_task_filter
from loom_service.worker_backends import get_active_backends

router = APIRouter()


class _CreateBatch(BaseModel):
    name: str = Field(min_length=1, max_length=200)
    description: str | None = None
    task_filter: dict[str, Any]
    trial_config: dict[str, Any]
    # Plan 23: n-sampling for single-combination batches. Ignored
    # when `combinations` is non-empty (each Combination carries
    # its own n_per_task).
    n_per_task: int = Field(default=1, ge=1, le=100)
    # Plan 28 PR-3: backend selection at the batch level. Optional;
    # defaults to "docker" so single-backend deployments don't have
    # to send it.
    backend: str = "docker"
    # Plan 28 PR-3: multi-(agent, model) combinations. Empty list
    # ⇒ single-combination behavior (agent + model come from
    # trial_config).
    combinations: list[Combination] = Field(default_factory=list)
    # cluster-deploy.md §Schema additions: batch-level provider
    # override. Carries through fan-out into Trial.provider_connection_id
    # for every trial spawned from this batch (unless overridden on
    # a per-trial basis at submission). Validated against the team
    # before insertion.
    provider_connection_id: UUID | None = None
    provider_model_id: str | None = None


def _serialize(
    b: Batch,
    *,
    summary: dict[str, int] | None = None,
    aggregate_reward: float | None = None,
    total_cost_usd: float = 0.0,
) -> dict[str, Any]:
    out: dict[str, Any] = {
        "id": str(b.id),
        "team_id": str(b.team_id),
        "name": b.name,
        "description": b.description,
        "task_filter": b.task_filter,
        "trial_config": b.trial_config,
        "state": b.state,
        "result_status": b.result_status,
        "created_at": b.created_at.isoformat(),
        "finished_at": (
            b.finished_at.isoformat() if b.finished_at else None
        ),
        "created_by_token_prefix": b.created_by_token_prefix,
        "expected_trial_count": b.expected_trial_count,
        "n_per_task": b.n_per_task,
        "backend": b.backend,
        "combinations": b.combinations,
    }
    if summary is not None:
        out["trial_summary"] = summary
        out["aggregate_reward"] = aggregate_reward
        out["total_cost_usd"] = total_cost_usd
    return out


@router.post("/batches", status_code=201)
async def create_batch(
    request: Request,
    sc: SessionAndCtx,
    payload: _CreateBatch,
) -> dict[str, Any]:
    s, ctx = sc
    require_scope(ctx, "submit")
    if ctx.team_id is None:
        raise HTTPException(
            status_code=400,
            detail="admin tokens must scope batches to a team — "
                   "use the service's per-team admin token",
        )

    catalog = known_names()

    if payload.combinations:
        # Multi-combination batch. trial_config MUST NOT carry
        # agent_name / agent_model / n_per_task in this shape —
        # those live on each Combination.
        for forbidden in ("agent_name", "agent_model"):
            if forbidden in payload.trial_config:
                raise HTTPException(
                    status_code=400,
                    detail=(
                        f"trial_config.{forbidden} must be absent when "
                        "`combinations` is supplied — each combination "
                        "carries its own agent/model"
                    ),
                )
        # Catalog membership + agent⇄model compatibility per
        # Combination.
        for i, combo in enumerate(payload.combinations):
            if combo.agent_name not in catalog:
                raise HTTPException(
                    status_code=400,
                    detail=(
                        f"combinations[{i}].agent_name "
                        f"{combo.agent_name!r} is not in the agent "
                        "catalog. GET /api/v1/agents."
                    ),
                )
            err = validate_agent_model_compat(
                combo.agent_name, combo.agent_model,
            )
            if err is not None:
                raise HTTPException(
                    status_code=400,
                    detail=f"combinations[{i}]: {err}",
                )
        # Labels unique within the batch (after computing the
        # derived label for those without one).
        seen_labels: set[str] = set()
        for i, combo in enumerate(payload.combinations):
            label = combo.label or _derive_combination_label(combo)
            if label in seen_labels:
                raise HTTPException(
                    status_code=400,
                    detail=(
                        f"combinations[{i}] label {label!r} is "
                        "duplicated within the batch"
                    ),
                )
            seen_labels.add(label)
    else:
        # Single-combination batch. Catalog check on the agent
        # embedded in trial_config + agent⇄model compatibility.
        agent_name = payload.trial_config.get("agent_name")
        if isinstance(agent_name, str) and agent_name:
            if agent_name not in catalog:
                raise HTTPException(
                    status_code=400,
                    detail=(
                        f"unknown agent_name {agent_name!r} in "
                        "trial_config. GET /api/v1/agents."
                    ),
                )
            model_raw = payload.trial_config.get("agent_model")
            model: ModelSpec | None
            if model_raw is None:
                model = None
            else:
                try:
                    model = ModelSpec.model_validate(model_raw)
                except Exception as exc:
                    raise HTTPException(
                        status_code=400,
                        detail=(
                            "trial_config.agent_model failed to "
                            f"validate: {exc}"
                        ),
                    ) from exc
            err = validate_agent_model_compat(agent_name, model)
            if err is not None:
                raise HTTPException(
                    status_code=400,
                    detail=f"trial_config: {err}",
                )

    # cluster-deploy.md §POST /batches: reject when no live worker
    # advertises the requested backend. Saves operators from creating
    # batches that would stall in 'submitted' forever (no worker will
    # ever claim them). Backend catalog is owned by /api/v1/backends;
    # this check uses the same predicate so a backend that shows
    # `available=false` there also rejects here.
    active_backends = await get_active_backends(s)
    if payload.backend not in active_backends:
        available_str = (
            ", ".join(sorted(active_backends)) if active_backends
            else "(none — no active workers)"
        )
        raise HTTPException(
            status_code=400,
            detail=(
                f"no active worker advertises backend "
                f"{payload.backend!r}. Currently available: "
                f"{available_str}. See `GET /api/v1/backends`."
            ),
        )

    task_ids = await resolve_task_filter(s, payload.task_filter)
    # Audit M2: a filter materializing to zero tasks creates a
    # batch stuck in `submitted` forever — reject up front.
    if not task_ids:
        raise HTTPException(
            status_code=400,
            detail=(
                f"task_filter {payload.task_filter} matched zero "
                "tasks; refusing to create empty batch"
            ),
        )

    token_prefix = (
        ctx.token_hash.hex()[:8] if ctx.token_hash else "00000000"
    )

    # expected_trial_count = sum over combinations × tasks.
    if payload.combinations:
        expected = sum(
            len(task_ids) * c.n_per_task for c in payload.combinations
        )
        combinations_jsonb = [
            c.model_dump(mode="json") for c in payload.combinations
        ]
    else:
        expected = len(task_ids) * payload.n_per_task
        combinations_jsonb = []

    # Validate provider_connection_id BEFORE constructing the Batch
    # row so we 400 on bad input rather than 500 on FK violation.
    if payload.provider_connection_id is not None and ctx.team_id is not None:
        await validate_provider_connection(
            s, payload.provider_connection_id, team_id=ctx.team_id,
        )

    b = Batch(
        team_id=ctx.team_id,
        name=payload.name,
        description=payload.description,
        task_filter=payload.task_filter,
        trial_config=payload.trial_config,
        state="submitted",
        created_by_token_prefix=token_prefix,
        expected_trial_count=expected,
        n_per_task=payload.n_per_task,
        backend=payload.backend,
        combinations=combinations_jsonb,
        provider_connection_id=payload.provider_connection_id,
        provider_model_id=payload.provider_model_id,
    )
    s.add(b)
    await s.commit()
    await s.refresh(b)
    return {
        "batch_id": str(b.id),
        "expected_trial_count": expected,
        "n_per_task": b.n_per_task,
        "backend": b.backend,
        "combinations": b.combinations,
        "state": b.state,
        "created_at": b.created_at.isoformat(),
    }


def _derive_combination_label(combo: Combination) -> str:
    """Default label `"{agent_name}"` or
    `"{agent_name}/{provider}/{name}"` when a model is set."""
    if combo.agent_model is None:
        return combo.agent_name
    return (
        f"{combo.agent_name}/{combo.agent_model.provider}/"
        f"{combo.agent_model.name}"
    )


@router.get("/batches")
async def list_batches(
    request: Request,
    sc: SessionAndCtx,
    team_id: Annotated[UUID | None, Query()] = None,
    state: Annotated[
        str | None,
        Query(description="comma-separated state filter"),
    ] = None,
    cursor: Annotated[str | None, Query()] = None,
    limit: Annotated[int, Query(gt=0, le=200)] = 50,
) -> dict[str, Any]:
    s, ctx = sc
    require_scope(ctx, "read:own")

    target_team = team_id
    if target_team is not None:
        require_team_or_admin(ctx, target_team)
    elif not is_admin(ctx):
        target_team = ctx.team_id

    stmt = select(Batch).order_by(
        Batch.created_at.desc(), Batch.id.desc(),
    )
    if target_team is not None:
        stmt = stmt.where(Batch.team_id == target_team)
    if state:
        wanted = [x.strip() for x in state.split(",") if x.strip()]
        if wanted:
            stmt = stmt.where(Batch.state.in_(wanted))
    if cursor:
        try:
            cur = decode_cursor(cursor)
        except ValueError as exc:
            raise HTTPException(
                status_code=400, detail=str(exc),
            ) from exc
        stmt = stmt.where(
            or_(
                Batch.created_at < cur.submitted_at,
                and_(
                    Batch.created_at == cur.submitted_at,
                    Batch.id < cur.id,
                ),
            ),
        )
    stmt = stmt.limit(limit + 1)
    rows: Sequence[Batch] = (
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


@router.get("/batches/{batch_id}")
async def get_batch(
    request: Request,
    sc: SessionAndCtx,
    batch_id: UUID,
) -> dict[str, Any]:
    s, ctx = sc
    require_scope(ctx, "read:own")
    b = (await s.execute(
        select(Batch).where(Batch.id == batch_id),
    )).scalar_one_or_none()
    if b is None:
        raise HTTPException(
            status_code=404, detail="batch not found",
        )
    require_team_or_admin(ctx, b.team_id)

    # Per-state counts come from a single GROUP BY query.
    state_counts = (await s.execute(
        select(Trial.state, func.count(Trial.id))
        .where(Trial.batch_id == batch_id)
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
    # expected_trial_count which the batch already knows.
    results = (await s.execute(
        select(Trial.result).where(
            and_(
                Trial.batch_id == batch_id,
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
        b,
        summary=summary,
        aggregate_reward=avg_reward,
        total_cost_usd=cost_total,
    )


@router.post("/batches/{batch_id}/cancel")
async def cancel_batch(
    request: Request,
    sc: SessionAndCtx,
    batch_id: UUID,
) -> dict[str, Any]:
    s, ctx = sc
    require_scope(ctx, "submit")
    b = (await s.execute(
        select(Batch).where(Batch.id == batch_id),
    )).scalar_one_or_none()
    if b is None:
        raise HTTPException(
            status_code=404, detail="batch not found",
        )
    require_team_or_admin(ctx, b.team_id)
    now = datetime.now(UTC)
    await s.execute(
        update(Batch)
        .where(Batch.id == batch_id)
        .values(state="cancelled", finished_at=now),
    )
    # Cascade-cancel still-active trials in this batch. We do
    # NOT cancel queued trials whose worker may already be partway
    # through claim; the CP's existing cancel endpoint (Plan 5)
    # handles graceful interruption when called per-trial. Here we
    # just transition the rows to `cancelled` so the SPA stops
    # showing them as in-flight.
    await s.execute(
        update(Trial)
        .where(
            and_(
                Trial.batch_id == batch_id,
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
    return {"batch_id": str(batch_id), "state": "cancelled"}
