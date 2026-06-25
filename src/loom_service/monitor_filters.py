"""Shared Monitor filter helpers.

The Monitor table routes and summary route need the same team, benchmark,
agent, model, batch, and state semantics so URL-backed views do not drift.
"""

from __future__ import annotations

from typing import Any
from uuid import UUID

from sqlalchemy import func, or_

from loom.auth import AuthContext
from loom.db.schema import Batch, Task, Trial
from loom_service.auth_guards import is_admin, require_team_or_admin


def resolve_monitor_team_filter(
    ctx: AuthContext,
    team_id: UUID | None,
) -> UUID | None:
    """Resolve optional Monitor team filters against caller permissions."""
    if team_id is not None:
        require_team_or_admin(ctx, team_id)
        return team_id
    if not is_admin(ctx):
        return ctx.team_id
    return None


def split_state_filter(state: str | None) -> list[str]:
    if not state:
        return []
    return [value.strip() for value in state.split(",") if value.strip()]


def apply_batch_monitor_filters(
    stmt: Any,
    *,
    target_team: UUID | None,
    benchmark_id: str | None = None,
    agent: str | None = None,
    model: str | None = None,
    state: str | None = None,
) -> Any:
    if target_team is not None:
        stmt = stmt.where(Batch.team_id == target_team)
    if benchmark_id:
        stmt = stmt.where(or_(
            Batch.task_filter.contains({"benchmark_id": benchmark_id}),
            Batch.task_filter.contains({"benchmark_ids": [benchmark_id]}),
        ))
    if agent:
        stmt = stmt.where(or_(
            Batch.trial_config.contains({"agent_name": agent}),
            Batch.trial_config.contains({"agent": {"name": agent}}),
            Batch.combinations.contains([{"agent_name": agent}]),
        ))
    if model:
        stmt = stmt.where(or_(
            Batch.trial_config.contains({"agent_model": {"name": model}}),
            Batch.trial_config.contains({"agent": {"model": {"name": model}}}),
            Batch.combinations.contains([{"agent_model": {"name": model}}]),
        ))
    wanted = split_state_filter(state)
    if wanted:
        stmt = stmt.where(Batch.state.in_(wanted))
    return stmt


def apply_trial_monitor_filters(
    stmt: Any,
    *,
    target_team: UUID | None,
    task_id: str | None = None,
    batch_id: UUID | None = None,
    benchmark_id: str | None = None,
    agent: str | None = None,
    model: str | None = None,
    state: str | None = None,
) -> Any:
    if target_team is not None:
        stmt = stmt.where(Trial.team_id == target_team)
    if task_id is not None:
        stmt = stmt.where(Trial.task_id == task_id)
    if benchmark_id is not None:
        stmt = stmt.join(Task, Task.id == Trial.task_id).where(
            Task.benchmark_id == benchmark_id,
        )
    if batch_id is not None:
        stmt = stmt.where(Trial.batch_id == batch_id)
    if agent:
        stmt = stmt.where(or_(
            func.jsonb_extract_path_text(Trial.config, "agent_name") == agent,
            func.jsonb_extract_path_text(Trial.config, "agent", "name")
            == agent,
        ))
    if model:
        stmt = stmt.where(or_(
            func.jsonb_extract_path_text(
                Trial.config, "agent_model", "name",
            ) == model,
            func.jsonb_extract_path_text(
                Trial.config, "agent", "model", "name",
            ) == model,
        ))
    wanted = split_state_filter(state)
    if wanted:
        stmt = stmt.where(Trial.state.in_(wanted))
    return stmt
