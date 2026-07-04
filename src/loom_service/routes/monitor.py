"""Monitor summary API.

The SPA Monitor page lists paginated rows, but release/debug workflows also
need URL-scoped state counters and worker capacity at a glance.
"""

from __future__ import annotations

from typing import Annotated, Any, Literal
from uuid import UUID

from fastapi import APIRouter, Query
from sqlalchemy import func, select

from loom.db.schema import Batch, Trial
from loom.resource_pools import get_resource_pool_summary
from loom_service.auth_guards import require_scope
from loom_service.dependencies import SessionAndCtx
from loom_service.monitor_filters import (
    apply_batch_monitor_filters,
    apply_trial_monitor_filters,
    resolve_monitor_team_filter,
)
from loom_service.worker_backends import (
    _HEARTBEAT_FRESHNESS_SEC,
    get_active_backends,
    get_active_worker_count,
)

router = APIRouter()

View = Literal["batches", "trials"]

_BATCH_STATES = ("submitted", "running", "finished", "cancelled")
_TRIAL_STATES = (
    "queued",
    "claimed",
    "running",
    "succeeded",
    "failed",
    "cancelled",
)


async def _counts(
    session: Any,
    stmt: Any,
    *,
    state_col: Any,
    states: tuple[str, ...],
) -> dict[str, int]:
    counts = {state: 0 for state in states}
    rows = await session.execute(stmt.group_by(state_col))
    for state, count in rows.all():
        if state in counts:
            counts[str(state)] = int(count)
    return counts


def _queue_status(
    *,
    queued: int,
    claimed: int,
    running: int,
    active_workers: int,
) -> str:
    waiting = queued + claimed
    if active_workers <= 0 and (waiting > 0 or running > 0):
        return "blocked"
    if waiting > 0:
        return "waiting"
    if running > 0:
        return "running"
    return "idle"


def _resource_trials_stmt() -> Any:
    return (
        select(
            Trial.state,
            Trial.worker_id,
            Trial.requires_caps,
            Trial.claimed_at,
            Trial.pre_start_heartbeat_at,
        )
        .select_from(Trial)
        .where(Trial.state.in_(("queued", "claimed", "running")))
    )


@router.get("/monitor/summary")
async def get_monitor_summary(
    sc: SessionAndCtx,
    view: Annotated[View, Query()] = "batches",
    team_id: Annotated[UUID | None, Query()] = None,
    q: Annotated[str | None, Query()] = None,
    benchmark_id: Annotated[str | None, Query()] = None,
    agent_name: Annotated[str | None, Query()] = None,
    agent: Annotated[str | None, Query()] = None,
    model_provider: Annotated[str | None, Query()] = None,
    model_name: Annotated[str | None, Query()] = None,
    model: Annotated[str | None, Query()] = None,
    provider_connection_id: Annotated[UUID | None, Query()] = None,
    provider_model_id: Annotated[str | None, Query()] = None,
    batch_id: Annotated[UUID | None, Query()] = None,
    state: Annotated[
        str | None,
        Query(description="selected table state; summary still counts all states"),
    ] = None,
) -> dict[str, Any]:
    session, ctx = sc
    require_scope(ctx, "read:own")
    target_team = resolve_monitor_team_filter(ctx, team_id)
    agent_value = agent_name or agent
    model_value = model_name or model
    q_value = q.strip() if isinstance(q, str) and q.strip() else None

    batch_counts_stmt = select(Batch.state, func.count()).select_from(Batch)
    batch_counts_stmt = apply_batch_monitor_filters(
        batch_counts_stmt,
        target_team=target_team,
        q=q_value,
        benchmark_id=benchmark_id,
        agent_name=agent_value,
        model_provider=model_provider,
        model_name=model_value,
        provider_connection_id=provider_connection_id,
        provider_model_id=provider_model_id,
        state=None,
    )
    trial_counts_stmt = select(Trial.state, func.count()).select_from(Trial)
    trial_counts_stmt = apply_trial_monitor_filters(
        trial_counts_stmt,
        target_team=target_team,
        q=q_value,
        batch_id=batch_id,
        benchmark_id=benchmark_id,
        agent_name=agent_value,
        model_provider=model_provider,
        model_name=model_value,
        provider_connection_id=provider_connection_id,
        provider_model_id=provider_model_id,
        state=None,
    )
    resource_trials_stmt = _resource_trials_stmt()
    resource_trials_stmt = apply_trial_monitor_filters(
        resource_trials_stmt,
        target_team=target_team,
        q=q_value,
        batch_id=batch_id,
        benchmark_id=benchmark_id,
        agent_name=agent_value,
        model_provider=model_provider,
        model_name=model_value,
        provider_connection_id=provider_connection_id,
        provider_model_id=provider_model_id,
        state=None,
    )
    batch_counts = await _counts(
        session,
        batch_counts_stmt,
        state_col=Batch.state,
        states=_BATCH_STATES,
    )
    trial_counts = await _counts(
        session,
        trial_counts_stmt,
        state_col=Trial.state,
        states=_TRIAL_STATES,
    )

    active_backends = sorted(await get_active_backends(session))
    active_workers = await get_active_worker_count(session)
    resources = await get_resource_pool_summary(
        session,
        freshness_sec=_HEARTBEAT_FRESHNESS_SEC,
        trial_stmt=resource_trials_stmt,
    )
    queued = trial_counts["queued"]
    claimed = trial_counts["claimed"]
    running = trial_counts["running"]
    return {
        "scope": {
            "view": view,
            "team_id": str(target_team) if target_team else None,
            "q": q_value,
            "benchmark_id": benchmark_id,
            "agent_name": agent_value,
            "model_provider": model_provider,
            "model_name": model_value,
            "provider_connection_id": (
                str(provider_connection_id) if provider_connection_id else None
            ),
            "provider_model_id": provider_model_id,
            "batch_id": str(batch_id) if batch_id else None,
            "state": state,
        },
        "state_counts": {
            "batches": batch_counts,
            "trials": trial_counts,
        },
        "queue": {
            "queued": queued,
            "claimed": claimed,
            "running": running,
            "waiting": queued + claimed,
            "active_workers": active_workers,
            "available_backends": active_backends,
            "has_default_backend": "docker" in active_backends,
            "status": _queue_status(
                queued=queued,
                claimed=claimed,
                running=running,
                active_workers=active_workers,
            ),
        },
        "resources": resources,
    }
