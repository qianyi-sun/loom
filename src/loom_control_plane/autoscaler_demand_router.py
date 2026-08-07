"""Durable routing for architecture-neutral autoscaler demand."""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import UTC, datetime
from fractions import Fraction
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from loom.db.schema import Trial, WorkerPoolAutoscalerPolicy


@dataclass(frozen=True)
class PoolDemandState:
    """Capacity facts used to assign one neutral queued trial."""

    pool_name: str
    enabled: bool
    max_slots: int
    active_slots: int
    occupied_slots: int
    pending_slots: int
    assigned_queued_slots: int
    blocked_reason: str | None = None
    error_message: str | None = None

    @property
    def unassigned_free_slots(self) -> int:
        return max(
            0,
            self.active_slots - self.occupied_slots - self.assigned_queued_slots,
        )

    @property
    def demand_headroom(self) -> int:
        committed_demand = self.occupied_slots + self.pending_slots + self.assigned_queued_slots
        return max(0, self.max_slots - committed_demand)

    @property
    def pressure(self) -> Fraction:
        if self.max_slots <= 0:
            return Fraction(1, 1)
        return Fraction(
            self.occupied_slots + self.pending_slots + self.assigned_queued_slots,
            self.max_slots,
        )


@dataclass(frozen=True)
class DemandRoutingSummary:
    assigned_count: int
    retained_count: int
    cleared_count: int
    unroutable_count: int


def requires_neutral_pool_assignment(requires_caps: object) -> bool:
    """Return whether queued demand may run in more than one worker pool."""
    if not isinstance(requires_caps, dict):
        return False
    worker_pool = requires_caps.get("worker_pool")
    if isinstance(worker_pool, str) and worker_pool.strip():
        return False
    cpu_arch: Any = requires_caps.get("cpu_arch")
    return isinstance(cpu_arch, str) and cpu_arch == "any"


def choose_neutral_pool(states: tuple[PoolDemandState, ...]) -> str | None:
    """Choose one healthy pool without duplicating neutral demand."""
    eligible = tuple(
        state
        for state in states
        if state.enabled
        and state.max_slots > 0
        and state.blocked_reason is None
        and state.error_message is None
    )
    free = tuple(state for state in eligible if state.unassigned_free_slots > 0)
    if free:
        return min(
            free,
            key=lambda state: (
                -state.unassigned_free_slots,
                state.pressure,
                state.pool_name,
            ),
        ).pool_name
    scalable = tuple(state for state in eligible if state.demand_headroom > 0)
    if not scalable:
        return None
    return min(
        scalable,
        key=lambda state: (state.pressure, state.pool_name),
    ).pool_name


def _pool_state(row: WorkerPoolAutoscalerPolicy) -> PoolDemandState:
    return PoolDemandState(
        pool_name=row.pool_name,
        enabled=bool(row.enabled),
        max_slots=max(0, int(row.max_slots)),
        active_slots=max(0, int(row.last_actual_slots or 0)),
        occupied_slots=max(0, int(row.last_occupied_slots or 0)),
        pending_slots=max(0, int(row.last_pending_slots or 0)),
        assigned_queued_slots=0,
        blocked_reason=row.last_blocked_reason,
        error_message=row.last_error,
    )


async def assign_neutral_queued_trials(
    session: AsyncSession,
    *,
    environment: str,
    now: datetime | None = None,
) -> DemandRoutingSummary:
    """Assign every unpinned neutral queued trial to exactly one pool."""
    now = now or datetime.now(UTC)
    policies = (
        (
            await session.execute(
                select(WorkerPoolAutoscalerPolicy)
                .where(WorkerPoolAutoscalerPolicy.environment == environment)
                .order_by(WorkerPoolAutoscalerPolicy.pool_name),
            )
        )
        .scalars()
        .all()
    )
    states = {row.pool_name: _pool_state(row) for row in policies}
    trials = (
        (
            await session.execute(
                select(Trial)
                .where(Trial.state == "queued")
                .order_by(Trial.submitted_at, Trial.id)
                .with_for_update(skip_locked=True),
            )
        )
        .scalars()
        .all()
    )

    for trial in trials:
        if not requires_neutral_pool_assignment(trial.requires_caps):
            continue
        assigned_pool = trial.autoscaler_pool_name
        state = states.get(assigned_pool) if assigned_pool is not None else None
        if state is not None and state.enabled and state.max_slots > 0:
            states[state.pool_name] = replace(
                state,
                assigned_queued_slots=state.assigned_queued_slots + 1,
            )

    assigned_count = 0
    retained_count = 0
    cleared_count = 0
    unroutable_count = 0
    for trial in trials:
        if not requires_neutral_pool_assignment(trial.requires_caps):
            if trial.autoscaler_pool_name is not None:
                trial.autoscaler_pool_name = None
                trial.autoscaler_pool_assigned_at = None
                cleared_count += 1
            continue

        assigned_pool = trial.autoscaler_pool_name
        assigned_state = states.get(assigned_pool) if assigned_pool is not None else None
        if assigned_state is not None and assigned_state.enabled and assigned_state.max_slots > 0:
            retained_count += 1
            continue
        if assigned_pool is not None:
            trial.autoscaler_pool_name = None
            trial.autoscaler_pool_assigned_at = None
            cleared_count += 1

        selected_pool = choose_neutral_pool(tuple(states.values()))
        if selected_pool is None:
            unroutable_count += 1
            continue
        trial.autoscaler_pool_name = selected_pool
        trial.autoscaler_pool_assigned_at = now
        selected_state = states[selected_pool]
        states[selected_pool] = replace(
            selected_state,
            assigned_queued_slots=selected_state.assigned_queued_slots + 1,
        )
        assigned_count += 1

    await session.flush()
    return DemandRoutingSummary(
        assigned_count=assigned_count,
        retained_count=retained_count,
        cleared_count=cleared_count,
        unroutable_count=unroutable_count,
    )
