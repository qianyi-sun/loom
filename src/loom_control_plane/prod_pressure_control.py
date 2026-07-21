"""Production-pressure signal and staging GB10 claim-control bridge.

The production and staging Control Planes have separate databases.  This
module exposes a secret-free production pressure snapshot and applies that
snapshot to staging's GB10 desired state plus worker registry.  Registry
drain state is the scheduler's existing fail-closed claim gate.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

from sqlalchemy import func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from loom.db.schema import (
    AdminAuditEvent,
    GB10WorkerNodeStatus,
    Trial,
    Worker,
    WorkerPoolAutoscalerPolicy,
)
from loom_control_plane.gb10_worker_lifecycle import (
    _reconcile_worker_registry_for_host_intents,
    get_desired_state,
    redact_status_text,
)

_CONTROL_KEY = "prod_pressure_control"


@dataclass(frozen=True)
class ProdPressureSignal:
    prod_pending_count: int
    prod_active_count: int
    prod_capacity_shortfall: int
    source: str = "control-plane prod queue summary"

    @property
    def has_pressure(self) -> bool:
        return any(
            value > 0
            for value in (
                self.prod_pending_count,
                self.prod_active_count,
                self.prod_capacity_shortfall,
            )
        )

    def public_dict(self) -> dict[str, object]:
        return {
            "has_pressure": self.has_pressure,
            "cause": "prod_capacity_pressure" if self.has_pressure else "none",
            "prod_pending_count": self.prod_pending_count,
            "prod_active_count": self.prod_active_count,
            "prod_capacity_shortfall": self.prod_capacity_shortfall,
            "source": self.source,
        }


def _clean_count(value: int, field: str) -> int:
    if isinstance(value, bool) or value < 0:
        raise ValueError(f"{field} must be a non-negative integer")
    return int(value)


def validate_signal(signal: ProdPressureSignal) -> ProdPressureSignal:
    source = str(signal.source).strip()
    if not source:
        raise ValueError("source must be a non-empty string")
    return ProdPressureSignal(
        prod_pending_count=_clean_count(signal.prod_pending_count, "prod_pending_count"),
        prod_active_count=_clean_count(signal.prod_active_count, "prod_active_count"),
        prod_capacity_shortfall=_clean_count(
            signal.prod_capacity_shortfall,
            "prod_capacity_shortfall",
        ),
        source=(redact_status_text(source[:240]) or "control-plane prod queue summary"),
    )


def _pool_match(pool_name: str) -> Any:
    required_pool = Trial.requires_caps["worker_pool"].astext
    return or_(required_pool.is_(None), required_pool == "", required_pool == pool_name)


async def fetch_prod_pressure_signal(
    session: AsyncSession,
    *,
    pool_name: str,
    now: datetime | None = None,
    freshness_sec: int = 120,
) -> ProdPressureSignal:
    """Compute a secret-free pressure signal from the production CP DB."""
    pool_name = str(pool_name).strip()
    if not pool_name:
        raise ValueError("pool_name must be a non-empty string")
    if freshness_sec <= 0:
        raise ValueError("freshness_sec must be positive")
    now = now or datetime.now(UTC)
    pool_match = _pool_match(pool_name)
    pending_count = int(
        (
            await session.execute(
                select(func.count())
                .select_from(Trial)
                .where(Trial.state == "queued", pool_match),
            )
        ).scalar_one()
    )
    active_count = int(
        (
            await session.execute(
                select(func.count())
                .select_from(Trial)
                .where(Trial.state.in_(("claimed", "running")), pool_match),
            )
        ).scalar_one()
    )

    workers = (
        (
            await session.execute(
                select(Worker).where(
                    Worker.pool_name == pool_name,
                    Worker.status == "active",
                    Worker.drain_state == "active",
                    Worker.last_seen_at >= now - timedelta(seconds=freshness_sec),
                ),
            )
        )
        .scalars()
        .all()
    )
    worker_ids = tuple(worker.id for worker in workers)
    in_flight_by_worker: dict[object, int] = {worker_id: 0 for worker_id in worker_ids}
    if worker_ids:
        rows = (
            await session.execute(
                select(Trial.worker_id, func.count())
                .where(
                    Trial.worker_id.in_(worker_ids),
                    Trial.state.in_(("claimed", "running")),
                )
                .group_by(Trial.worker_id),
            )
        ).all()
        in_flight_by_worker.update(
            {worker_id: int(count) for worker_id, count in rows if worker_id is not None},
        )
    claimable_free_slots = sum(
        max(0, int(worker.max_concurrent or 1) - in_flight_by_worker.get(worker.id, 0))
        for worker in workers
    )
    return ProdPressureSignal(
        prod_pending_count=pending_count,
        prod_active_count=active_count,
        prod_capacity_shortfall=max(0, pending_count - claimable_free_slots),
    )


def _parse_time(value: object) -> datetime | None:
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _grace_evidence(
    *,
    control: dict[str, Any],
    signal: ProdPressureSignal,
    preemptible: bool,
    grace_period_seconds: int,
    now: datetime,
) -> dict[str, object]:
    started_at = _parse_time(control.get("started_at"))
    cancel_after = (
        started_at + timedelta(seconds=grace_period_seconds)
        if signal.has_pressure and preemptible and started_at is not None
        else None
    )
    eligible = cancel_after is not None and now >= cancel_after
    if not signal.has_pressure:
        action = "none"
        reason = "prod_pressure_clear"
    elif not preemptible:
        action = "not_preemptible"
        reason = "staging work drains without cancellation"
    elif eligible:
        action = "cancel_retryable"
        reason = "prod_capacity_pressure_grace_period_elapsed"
    else:
        action = "wait"
        reason = "grace_period_active"
    return {
        "preemptible": preemptible,
        "grace_period_seconds": grace_period_seconds,
        "started_at": started_at.isoformat() if started_at else None,
        "cancel_after": cancel_after.isoformat() if cancel_after else None,
        "action": action,
        "retryable": bool(eligible),
        "reason": reason,
    }


async def _active_staging_trials_by_host(
    session: AsyncSession,
    *,
    pool_name: str,
    hostnames: set[str],
) -> list[tuple[Trial, str]]:
    if not hostnames:
        return []
    rows = (
        await session.execute(
            select(Trial, Worker.hostname)
            .join(Worker, Worker.id == Trial.worker_id)
            .where(
                Worker.pool_name == pool_name,
                Worker.hostname.in_(tuple(hostnames)),
                Trial.state.in_(("claimed", "running")),
            )
            .with_for_update(of=Trial),
        )
    ).all()
    return [(trial, str(hostname)) for trial, hostname in rows]


async def _apply_slurm_prod_pressure(
    session: AsyncSession,
    *,
    policy: WorkerPoolAutoscalerPolicy,
    signal: ProdPressureSignal,
    preemptible: bool,
    grace_period_seconds: int,
    now: datetime,
) -> dict[str, object]:
    """Record prod-pressure drain intent for a Slurm-actuated pool (#892).

    Single-writer contract: this only records intent into
    ``policy.prod_pressure_state``. The external autoscaler actor is the sole
    writer of the Slurm side effects (``scancel``, ``SlurmWorkerJob`` state,
    ``Worker.drain_state``); the scheduler claim path *reads* the intent to
    fence new claims. No worker or job rows are mutated here, so this composes
    with the in-cluster CP (which cannot reach ``scancel``).
    """
    raw = policy.prod_pressure_state if isinstance(policy.prod_pressure_state, dict) else {}
    state: dict[str, Any] = dict(raw)
    previous_signal = state.get("last_signal")
    previous_grace_action = state.get("last_grace_action")

    if signal.has_pressure:
        already_draining = state.get("state") == "draining"
        if not already_draining:
            state = {"state": "draining", "started_at": now.isoformat()}
        grace = _grace_evidence(
            control=state,
            signal=signal,
            preemptible=preemptible,
            grace_period_seconds=grace_period_seconds,
            now=now,
        )
        action = "pressure_held" if already_draining else "draining"
    else:
        grace = _grace_evidence(
            control=state,
            signal=signal,
            preemptible=preemptible,
            grace_period_seconds=grace_period_seconds,
            now=now,
        )
        action = "recovered" if state.get("state") == "draining" else "no_pressure"

    current_signal = signal.public_dict()
    state["last_signal"] = current_signal
    state["last_grace_action"] = grace["action"]
    state["preemptible"] = bool(preemptible)
    state["grace_period_seconds"] = int(grace_period_seconds)
    state["updated_at"] = now.isoformat()

    # Draining keeps the intent for the external actor + claim path to read;
    # recovery/no-pressure clears it to NULL so both see normal operation.
    drain_intent_active = action in {"draining", "pressure_held"}
    policy.prod_pressure_state = state if drain_intent_active else None
    policy.updated_at = now

    if (
        action in {"draining", "recovered"}
        or previous_signal != current_signal
        or previous_grace_action != grace["action"]
    ):
        session.add(
            AdminAuditEvent(
                actor="prod-pressure-controller",
                action="worker.capacity.prod_pressure",
                target_type="worker_pool",
                target_id=f"{policy.environment}/{policy.pool_name}",
                event_metadata={
                    "action": action,
                    "actuator": "slurm",
                    "cause": current_signal["cause"],
                    "prod_pending_count": signal.prod_pending_count,
                    "prod_active_count": signal.prod_active_count,
                    "prod_capacity_shortfall": signal.prod_capacity_shortfall,
                    "grace_action": grace["action"],
                },
            ),
        )
    await session.flush()
    return {
        "action": action,
        "actuator": "slurm",
        "prod_pressure": current_signal,
        "new_staging_claims_allowed": not drain_intent_active,
        "drain_intent_active": drain_intent_active,
        "grace": grace,
        "environment": policy.environment,
        "pool_name": policy.pool_name,
    }


async def apply_prod_pressure_signal(
    session: AsyncSession,
    *,
    environment: str,
    pool_name: str,
    signal: ProdPressureSignal,
    preemptible: bool,
    grace_period_seconds: int,
    now: datetime | None = None,
) -> dict[str, object]:
    """Apply production pressure to staging desired state and claimability."""
    signal = validate_signal(signal)
    if grace_period_seconds < 0:
        raise ValueError("grace_period_seconds must be non-negative")
    now = now or datetime.now(UTC)

    # Dispatch on the pool's actuator. Slurm pools record drain intent only;
    # the external autoscaler actor performs the scancel/release. GB10 pools
    # (and pools without a policy row, for backward compatibility) fall through
    # to the registry-fencing desired-state drain below.
    policy_row = (
        await session.execute(
            select(WorkerPoolAutoscalerPolicy).where(
                WorkerPoolAutoscalerPolicy.environment == environment,
                WorkerPoolAutoscalerPolicy.pool_name == pool_name,
            ),
        )
    ).scalar_one_or_none()
    if policy_row is not None and policy_row.actuator == "slurm":
        return await _apply_slurm_prod_pressure(
            session,
            policy=policy_row,
            signal=signal,
            preemptible=preemptible,
            grace_period_seconds=grace_period_seconds,
            now=now,
        )

    desired = await get_desired_state(
        session,
        environment=environment,
        pool_name=pool_name,
    )
    if desired is None:
        raise ValueError("GB10 desired state must exist before prod-pressure control")

    policy = dict(desired.rollout_policy or {})
    raw_control = policy.get(_CONTROL_KEY)
    control = dict(raw_control) if isinstance(raw_control, dict) else {}
    previous_signal = control.get("last_signal")
    previous_grace_action = control.get("last_grace_action")
    current_intents = {str(k): str(v) for k, v in dict(desired.host_intents or {}).items()}
    node_hosts = {
        hostname
        for (hostname,) in (
            await session.execute(
                select(GB10WorkerNodeStatus.hostname).where(
                    GB10WorkerNodeStatus.environment == environment,
                    GB10WorkerNodeStatus.pool_name == pool_name,
                ),
            )
        ).all()
    }
    worker_hosts = {
        hostname
        for (hostname,) in (
            await session.execute(
                select(Worker.hostname).where(Worker.pool_name == pool_name),
            )
        ).all()
    }
    known_hosts = set(current_intents) | node_hosts | worker_hosts
    running_staging_trials = 0
    retryable_preemption_trials = 0

    if signal.has_pressure:
        already_draining = control.get("state") == "draining"
        if not already_draining:
            control = {
                "state": "draining",
                "started_at": now.isoformat(),
                "previous_host_intents": current_intents,
                "previous_target_slots": desired.target_slots,
            }
        grace = _grace_evidence(
            control=control,
            signal=signal,
            preemptible=preemptible,
            grace_period_seconds=grace_period_seconds,
            now=now,
        )
        active_trials = await _active_staging_trials_by_host(
            session,
            pool_name=pool_name,
            hostnames=known_hosts,
        )
        running_staging_trials = len(active_trials)
        active_hosts = {hostname for _, hostname in active_trials}
        previous_intents = control.get("previous_host_intents")
        baseline_intents = (
            {str(k): str(v) for k, v in previous_intents.items()}
            if isinstance(previous_intents, dict)
            else current_intents
        )
        if grace["action"] == "cancel_retryable":
            for trial, _hostname in active_trials:
                trial.failure_reason = "prod_capacity_pressure"
                trial.failure_message = (
                    "preemptible staging trial selected for worker drain after "
                    "production capacity pressure grace period elapsed; crash "
                    "reclaim returns it to queued with retry backoff"
                )
            retryable_preemption_trials = len(active_trials)
            control["last_preemption_at"] = now.isoformat()
            control["last_retryable_preemption_trial_count"] = (
                retryable_preemption_trials
            )
        next_intents = {
            host: (
                "stopped"
                if baseline_intents.get(host, "active") == "stopped"
                or host not in active_hosts
                else (
                    "stopped"
                    if grace["action"] == "cancel_retryable"
                    else (
                        "draining"
                        if baseline_intents.get(host) == "draining"
                        else "active"
                    )
                )
            )
            for host in sorted(known_hosts)
        }
        registry_intents = {
            host: "stopped" if intent == "stopped" else "draining"
            for host, intent in next_intents.items()
        }
        desired.target_slots = 0
        if retryable_preemption_trials:
            action = "preempting_after_grace"
        else:
            action = "pressure_held" if already_draining else "draining"
    else:
        grace = _grace_evidence(
            control=control,
            signal=signal,
            preemptible=preemptible,
            grace_period_seconds=grace_period_seconds,
            now=now,
        )
        previous = control.get("previous_host_intents")
        if control.get("state") == "draining" and isinstance(previous, dict):
            next_intents = {str(k): str(v) for k, v in previous.items()}
            previous_target_slots = control.get("previous_target_slots")
            desired.target_slots = (
                int(previous_target_slots)
                if isinstance(previous_target_slots, int)
                and not isinstance(previous_target_slots, bool)
                else desired.target_slots
            )
            control["state"] = "recovered"
            control["recovered_at"] = now.isoformat()
            action = "recovered"
        else:
            next_intents = current_intents
            action = "no_pressure"
        registry_intents = next_intents

    control["last_signal"] = signal.public_dict()
    control["preemptible"] = bool(preemptible)
    control["grace_period_seconds"] = int(grace_period_seconds)
    control["updated_at"] = now.isoformat()
    policy[_CONTROL_KEY] = control
    desired.host_intents = next_intents
    desired.rollout_policy = policy
    desired.updated_at = now

    registry_changes = await _reconcile_worker_registry_for_host_intents(
        session,
        environment=environment,
        pool_name=pool_name,
        host_intents=registry_intents,
        owner="prod-pressure-controller",
        now=now,
    )
    control["last_grace_action"] = grace["action"]
    desired.rollout_policy = {**policy, _CONTROL_KEY: control}
    current_signal = signal.public_dict()
    if (
        action in {"draining", "recovered"}
        or previous_signal != current_signal
        or previous_grace_action != grace["action"]
    ):
        session.add(
            AdminAuditEvent(
                actor="prod-pressure-controller",
                action="worker.capacity.prod_pressure",
                target_type="worker_pool",
                target_id=f"{environment}/{pool_name}",
                event_metadata={
                    "action": action,
                    "cause": current_signal["cause"],
                    "prod_pending_count": signal.prod_pending_count,
                    "prod_active_count": signal.prod_active_count,
                    "prod_capacity_shortfall": signal.prod_capacity_shortfall,
                    "registry_changes": registry_changes,
                    "grace_action": grace["action"],
                    "running_staging_trials": running_staging_trials,
                    "retryable_preemption_trials": retryable_preemption_trials,
                },
            ),
        )

    desired_active_hosts = tuple(
        host for host, intent in next_intents.items() if intent == "active"
    )
    claimable_workers = 0
    if not signal.has_pressure and desired_active_hosts:
        claimable_workers = int(
            (
                await session.execute(
                    select(func.count())
                    .select_from(Worker)
                    .where(
                        Worker.pool_name == pool_name,
                        Worker.hostname.in_(desired_active_hosts),
                        Worker.status == "active",
                        Worker.drain_state == "active",
                        Worker.last_seen_at >= now - timedelta(seconds=120),
                    ),
                )
            ).scalar_one()
        )
    await session.flush()
    return {
        "action": action,
        "prod_pressure": signal.public_dict(),
        "new_staging_claims_allowed": claimable_workers > 0,
        "claimable_worker_count": claimable_workers,
        "target_slots": desired.target_slots,
        "host_intents": next_intents,
        "registry_changes": registry_changes,
        "grace": grace,
        "running_staging_trials": running_staging_trials,
        "retryable_preemption_trials": retryable_preemption_trials,
        "draining_host_count": sum(
            1 for intent in next_intents.values() if intent == "draining"
        ),
    }
