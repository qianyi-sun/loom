"""Durable routing for architecture-neutral autoscaler demand."""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from fractions import Fraction
from typing import Any

from sqlalchemy import func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from loom.db.schema import Trial, WorkerPoolAutoscalerPolicy
from loom.execution_contract import (
    CapacityEvidenceKind,
    ExecutionAdapterKind,
    ExecutionRouteCandidateV1,
    ExecutionRoutingDecisionV1,
    ExecutionRoutingReason,
)
from loom.pipeline.keys import canonical_digest


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
    adapter_kind: ExecutionAdapterKind = ExecutionAdapterKind.LEGACY_WORKER_CLAIM
    target_id: str | None = None
    execution_class_id: str | None = None
    environment: str | None = None
    region: str | None = None
    data_residency: str | None = None
    operator_weight: int = 0
    budget_eligible: bool = True
    estimated_cost_microusd_per_slot_hour: int | None = None
    capacity_observed_at: datetime | None = None
    capacity_is_fresh: bool = True
    draining: bool = False

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


@dataclass(frozen=True)
class PoolSelection:
    pool_name: str
    reason: ExecutionRoutingReason


def requires_neutral_pool_assignment(requires_caps: object) -> bool:
    """Return whether queued demand may run in more than one worker pool."""
    if not isinstance(requires_caps, dict):
        return False
    worker_pool = requires_caps.get("worker_pool")
    if isinstance(worker_pool, str) and worker_pool.strip():
        return False
    cpu_arch: Any = requires_caps.get("cpu_arch")
    return isinstance(cpu_arch, str) and cpu_arch == "any"


def choose_neutral_pool_selection(
    states: tuple[PoolDemandState, ...],
) -> PoolSelection | None:
    """Choose one pool while distinguishing executable from configured capacity."""
    eligible = tuple(
        state
        for state in states
        if state.enabled
        and state.max_slots > 0
        and state.budget_eligible
        and not state.draining
        and state.blocked_reason is None
        and state.error_message is None
    )
    free = tuple(
        state for state in eligible if state.capacity_is_fresh and state.unassigned_free_slots > 0
    )
    if free:
        selected = min(
            free,
            key=lambda state: (
                -state.operator_weight,
                state.estimated_cost_microusd_per_slot_hour
                if state.estimated_cost_microusd_per_slot_hour is not None
                else 2**63 - 1,
                -state.unassigned_free_slots,
                state.pressure,
                state.pool_name,
            ),
        )
        return PoolSelection(
            pool_name=selected.pool_name,
            reason=ExecutionRoutingReason.FRESH_EXECUTABLE_CAPACITY,
        )
    scalable = tuple(state for state in eligible if state.demand_headroom > 0)
    if not scalable:
        return None
    selected = min(
        scalable,
        key=lambda state: (
            -state.operator_weight,
            state.estimated_cost_microusd_per_slot_hour
            if state.estimated_cost_microusd_per_slot_hour is not None
            else 2**63 - 1,
            state.pressure,
            state.pool_name,
        ),
    )
    return PoolSelection(
        pool_name=selected.pool_name,
        reason=ExecutionRoutingReason.CONFIGURED_SCALE_HEADROOM,
    )


def choose_neutral_pool(states: tuple[PoolDemandState, ...]) -> str | None:
    """Compatibility wrapper returning only the selected pool name."""
    selection = choose_neutral_pool_selection(states)
    return selection.pool_name if selection is not None else None


def pool_demand_state_from_policy(
    row: WorkerPoolAutoscalerPolicy,
    *,
    now: datetime,
    capacity_freshness_seconds: int,
    assigned_queued_slots: int = 0,
) -> PoolDemandState:
    """Normalize one persisted policy observation without upgrading stale capacity.

    ``last_actual_slots`` is backed by fresh worker heartbeats at observation
    time.  It is executable evidence only while the observation itself is
    fresh; configured scale headroom remains a separate planning fact.
    """
    observed_at = row.last_decision_at
    capacity_is_fresh = (
        observed_at is not None
        and observed_at + timedelta(seconds=capacity_freshness_seconds) > now
    )
    actuator_config = row.actuator_config or {}
    raw_weight = actuator_config.get("routing_weight", 0)
    operator_weight = (
        raw_weight if isinstance(raw_weight, int) and not isinstance(raw_weight, bool) else 0
    )
    operator_weight = max(-1_000, min(1_000, operator_weight))
    raw_cost = actuator_config.get("routing_cost_microusd_per_slot_hour")
    estimated_cost = (
        raw_cost
        if isinstance(raw_cost, int) and not isinstance(raw_cost, bool) and raw_cost >= 0
        else None
    )
    raw_budget_eligible = actuator_config.get("routing_budget_eligible", True)
    budget_eligible = raw_budget_eligible if isinstance(raw_budget_eligible, bool) else False
    return PoolDemandState(
        pool_name=row.pool_name,
        enabled=bool(row.enabled),
        max_slots=max(0, int(row.max_slots)),
        active_slots=max(0, int(row.last_actual_slots or 0)),
        occupied_slots=max(0, int(row.last_occupied_slots or 0)),
        pending_slots=max(0, int(row.last_pending_slots or 0)),
        assigned_queued_slots=max(0, int(assigned_queued_slots)),
        blocked_reason=row.last_blocked_reason,
        error_message=row.last_error,
        environment=row.environment,
        region=(
            str(actuator_config["routing_region"])
            if isinstance(actuator_config.get("routing_region"), str)
            else None
        ),
        data_residency=(
            str(actuator_config["routing_data_residency"])
            if isinstance(actuator_config.get("routing_data_residency"), str)
            else None
        ),
        operator_weight=operator_weight,
        budget_eligible=budget_eligible,
        estimated_cost_microusd_per_slot_hour=estimated_cost,
        capacity_observed_at=observed_at,
        capacity_is_fresh=capacity_is_fresh,
        draining=(row.prod_pressure_state or {}).get("state") == "draining",
    )


def route_candidate_from_pool_state(state: PoolDemandState) -> ExecutionRouteCandidateV1:
    blockers = []
    if not state.enabled:
        blockers.append("disabled")
    if state.draining:
        blockers.append("draining")
    if state.blocked_reason is not None:
        blockers.append("policy_blocked")
    if state.error_message is not None:
        blockers.append("policy_error")
    if state.max_slots <= 0:
        blockers.append("zero_configured_slots")
    if not state.budget_eligible:
        blockers.append("budget_ineligible")
    healthy = state.blocked_reason is None and state.error_message is None
    if not blockers and state.capacity_is_fresh and state.unassigned_free_slots > 0:
        evidence_kind = CapacityEvidenceKind.FRESH_EXECUTABLE
        available_slots = state.unassigned_free_slots
    elif not blockers and state.demand_headroom > 0:
        evidence_kind = CapacityEvidenceKind.CONFIGURED_SCALE_HEADROOM
        available_slots = state.demand_headroom
    else:
        evidence_kind = CapacityEvidenceKind.UNAVAILABLE
        available_slots = 0
        blockers.append("no_capacity_headroom")
    return ExecutionRouteCandidateV1(
        logical_pool_id=state.pool_name,
        adapter_kind=state.adapter_kind,
        target_id=state.target_id,
        execution_class_id=state.execution_class_id,
        environment=state.environment,
        region=state.region,
        data_residency=state.data_residency,
        operator_weight=state.operator_weight,
        budget_eligible=state.budget_eligible,
        estimated_cost_microusd_per_slot_hour=(state.estimated_cost_microusd_per_slot_hour),
        enabled=state.enabled,
        healthy=healthy,
        draining=state.draining,
        configured_slots=state.max_slots,
        active_slots=state.active_slots,
        occupied_slots=state.occupied_slots,
        pending_slots=state.pending_slots,
        assigned_queued_slots=state.assigned_queued_slots,
        available_slots=available_slots,
        capacity_evidence_kind=evidence_kind,
        capacity_observed_at=(state.capacity_observed_at if state.capacity_is_fresh else None),
        blockers=tuple(sorted(set(blockers))),
    )


def _routing_decision(
    *,
    trial: Trial,
    states: tuple[PoolDemandState, ...],
    selection: PoolSelection,
    generation: int,
    now: datetime,
) -> ExecutionRoutingDecisionV1:
    candidates = tuple(
        sorted(
            (route_candidate_from_pool_state(state) for state in states),
            key=lambda item: (item.logical_pool_id, item.target_id or ""),
        )
    )
    selected = next(item for item in candidates if item.logical_pool_id == selection.pool_name)
    return ExecutionRoutingDecisionV1(
        generation=generation,
        requirements_sha256=canonical_digest(trial.requires_caps),
        selected_pool_id=selected.logical_pool_id,
        selected_adapter_kind=selected.adapter_kind,
        selected_target_id=selected.target_id,
        selected_execution_class_id=selected.execution_class_id,
        reason=selection.reason,
        decided_at=now,
        candidates=candidates,
    )


async def assign_neutral_queued_trials(
    session: AsyncSession,
    *,
    environment: str,
    now: datetime | None = None,
    assignment_pool_names: frozenset[str] | None = None,
    capacity_freshness_seconds: int = 120,
) -> DemandRoutingSummary:
    """Assign unpinned neutral demand, optionally limited to witnessed pools.

    Foreign choices still reserve their planned slot in memory so one scoped
    pass preserves deterministic global balancing across the whole queue.
    """
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
    states = {
        row.pool_name: pool_demand_state_from_policy(
            row,
            now=now,
            capacity_freshness_seconds=capacity_freshness_seconds,
        )
        for row in policies
    }
    worker_pool_json = Trial.requires_caps.op("->")("worker_pool")
    worker_pool_text = Trial.requires_caps["worker_pool"].astext
    trials = (
        (
            await session.execute(
                select(Trial)
                .where(Trial.state == "queued")
                # Filter before taking row locks.  The claim path also uses
                # SKIP LOCKED, so locking concrete/pinned demand here can make
                # an otherwise claimable trial look transiently absent.
                .where(Trial.requires_caps["cpu_arch"].astext == "any")
                .where(
                    or_(
                        func.jsonb_typeof(worker_pool_json).is_distinct_from("string"),
                        func.btrim(worker_pool_text) == "",
                    ),
                )
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
                trial.execution_route_pool_name = None
                trial.execution_route_json = None
                trial.execution_route_sha256 = None
                cleared_count += 1
            continue

        assigned_pool = trial.autoscaler_pool_name
        assigned_state = states.get(assigned_pool) if assigned_pool is not None else None
        if (
            assigned_state is not None
            and assigned_state.enabled
            and assigned_state.max_slots > 0
            and not assigned_state.draining
            and assigned_state.blocked_reason is None
            and assigned_state.error_message is None
        ):
            retained_count += 1
            continue
        if assigned_pool is not None:
            trial.autoscaler_pool_name = None
            trial.autoscaler_pool_assigned_at = None
            trial.execution_route_pool_name = None
            trial.execution_route_json = None
            trial.execution_route_sha256 = None
            cleared_count += 1

        selection = choose_neutral_pool_selection(tuple(states.values()))
        if selection is None:
            unroutable_count += 1
            continue
        selected_pool = selection.pool_name
        selected_state = states[selected_pool]
        route_generation = trial.execution_route_generation + 1
        decision = (
            _routing_decision(
                trial=trial,
                states=tuple(states.values()),
                selection=selection,
                generation=route_generation,
                now=now,
            )
            if assignment_pool_names is None or selected_pool in assignment_pool_names
            else None
        )
        states[selected_pool] = replace(
            selected_state,
            assigned_queued_slots=selected_state.assigned_queued_slots + 1,
        )
        if decision is None:
            continue
        decision_json = decision.model_dump(mode="json")
        trial.autoscaler_pool_name = selected_pool
        trial.autoscaler_pool_assigned_at = now
        trial.execution_route_generation = route_generation
        trial.execution_route_pool_name = selected_pool
        trial.execution_route_json = decision_json
        trial.execution_route_sha256 = canonical_digest(decision_json)
        assigned_count += 1

    await session.flush()
    return DemandRoutingSummary(
        assigned_count=assigned_count,
        retained_count=retained_count,
        cleared_count=cleared_count,
        unroutable_count=unroutable_count,
    )
