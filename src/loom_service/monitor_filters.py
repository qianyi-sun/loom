"""Shared Monitor filter helpers.

The Monitor table routes and summary route need the same team, benchmark,
agent, model, batch, and state semantics so URL-backed views do not drift.
"""

from __future__ import annotations

from typing import Any
from uuid import UUID

from sqlalchemy import String, cast, func, or_

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
    q: str | None = None,
    benchmark_id: str | None = None,
    agent_name: str | None = None,
    model_provider: str | None = None,
    model_name: str | None = None,
    provider_connection_id: UUID | None = None,
    provider_model_id: str | None = None,
    state: str | None = None,
) -> Any:
    if target_team is not None:
        stmt = stmt.where(Batch.team_id == target_team)
    if q:
        pattern = f"%{q.strip()}%"
        if q.strip():
            stmt = stmt.where(or_(
                Batch.name.ilike(pattern),
                Batch.description.ilike(pattern),
                cast(Batch.id, String).ilike(pattern),
            ))
    if benchmark_id:
        stmt = stmt.where(or_(
            Batch.task_filter.contains({"benchmark_id": benchmark_id}),
            Batch.task_filter.contains({"benchmark_ids": [benchmark_id]}),
        ))
    if agent_name:
        stmt = stmt.where(or_(
            Batch.trial_config.contains({"agent_name": agent_name}),
            Batch.trial_config.contains({"agent": {"name": agent_name}}),
            Batch.combinations.contains([{"agent_name": agent_name}]),
        ))
    if model_provider:
        stmt = stmt.where(or_(
            Batch.trial_config.contains({
                "agent_model": {"provider": model_provider},
            }),
            Batch.trial_config.contains({
                "agent": {"model": {"provider": model_provider}},
            }),
            Batch.combinations.contains([
                {"agent_model": {"provider": model_provider}},
            ]),
        ))
    if model_name:
        stmt = stmt.where(or_(
            Batch.trial_config.contains({"agent_model": {"name": model_name}}),
            Batch.trial_config.contains({
                "agent": {"model": {"name": model_name}},
            }),
            Batch.combinations.contains([{"agent_model": {"name": model_name}}]),
        ))
    if provider_connection_id is not None:
        stmt = stmt.where(or_(
            Batch.provider_connection_id == provider_connection_id,
            Batch.combinations.contains([
                {"provider_connection_id": str(provider_connection_id)},
            ]),
        ))
    if provider_model_id:
        stmt = stmt.where(or_(
            Batch.provider_model_id == provider_model_id,
            Batch.combinations.contains([
                {"provider_model_id": provider_model_id},
            ]),
        ))
    wanted = split_state_filter(state)
    if wanted:
        stmt = stmt.where(Batch.state.in_(wanted))
    return stmt


def apply_trial_monitor_filters(
    stmt: Any,
    *,
    target_team: UUID | None,
    q: str | None = None,
    task_id: str | None = None,
    batch_id: UUID | None = None,
    benchmark_id: str | None = None,
    agent_name: str | None = None,
    agent: str | None = None,
    model_provider: str | None = None,
    model_name: str | None = None,
    model: str | None = None,
    provider_connection_id: UUID | None = None,
    provider_model_id: str | None = None,
    state: str | None = None,
) -> Any:
    if target_team is not None:
        stmt = stmt.where(Trial.team_id == target_team)
    if q:
        needle = q.strip()
        if needle:
            pattern = f"%{needle}%"
            stmt = stmt.join(Batch, Batch.id == Trial.batch_id).where(or_(
                Batch.name.ilike(pattern),
                Batch.description.ilike(pattern),
                cast(Batch.id, String).ilike(pattern),
            ))
    if task_id is not None:
        stmt = stmt.where(Trial.task_id == task_id)
    if benchmark_id is not None:
        stmt = stmt.join(Task, Task.id == Trial.task_id).where(
            Task.benchmark_id == benchmark_id,
        )
    if batch_id is not None:
        stmt = stmt.where(Trial.batch_id == batch_id)
    agent_value = agent_name or agent
    if agent_value:
        stmt = stmt.where(or_(
            func.jsonb_extract_path_text(Trial.config, "agent_name") == agent_value,
            func.jsonb_extract_path_text(Trial.config, "agent", "name")
            == agent_value,
        ))
    if model_provider:
        stmt = stmt.where(or_(
            func.jsonb_extract_path_text(
                Trial.config, "agent_model", "provider",
            ) == model_provider,
            func.jsonb_extract_path_text(
                Trial.config, "agent", "model", "provider",
            ) == model_provider,
        ))
    model_value = model_name or model
    if model_value:
        stmt = stmt.where(or_(
            func.jsonb_extract_path_text(
                Trial.config, "agent_model", "name",
            ) == model_value,
            func.jsonb_extract_path_text(
                Trial.config, "agent", "model", "name",
            ) == model_value,
        ))
    if provider_connection_id is not None:
        stmt = stmt.where(Trial.provider_connection_id == provider_connection_id)
    if provider_model_id:
        stmt = stmt.where(Trial.provider_model_id == provider_model_id)
    wanted = split_state_filter(state)
    if wanted:
        stmt = stmt.where(Trial.state.in_(wanted))
    return stmt
