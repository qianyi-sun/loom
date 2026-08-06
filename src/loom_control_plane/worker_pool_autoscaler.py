"""Shared worker-pool autoscaler policy and decision helpers."""

from __future__ import annotations

import asyncio
import logging
import math
from collections.abc import Mapping
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from sqlalchemy import or_, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from loom.db.schema import (
    GB10WorkerNodeStatus,
    GB10WorkerPoolDesiredState,
    SlurmWorkerJob,
    Trial,
    Worker,
    WorkerPoolAutoscalerPolicy,
)
from loom.dev_instance import (
    RequestedPolicy,
    dev_pool_instance_name,
    validate_dev_instance,
)
from loom.worker_token import (
    WORKER_AUTH_FINGERPRINT_ENV_KEY,
    worker_token_fingerprint_from_env_file,
)
from loom_control_plane.elastic_slurm_worker_controller import (
    ElasticSlurmWorkerControllerConfig,
    SlurmWorkerCapacitySnapshot,
    SlurmWorkerCommandRunner,
    SubprocessSlurmCommandRunner,
    build_controller_config,
    compute_controller_decision,
    slurm_compose_project_identity,
    slurm_sandbox_identity,
    slurm_submission_config_for_node,
)
from loom_control_plane.shared_capacity_broker import AutoscalerGrantHandoff
from loom_control_plane.slurm_worker_jobs import (
    ACTIVE_STATES,
    reconcile_slurm_worker_jobs,
    record_slurm_worker_job,
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class AutoscalerPolicyConfig:
    environment: str
    pool_name: str
    actuator: str
    enabled: bool
    min_slots: int
    max_slots: int
    scale_up_threshold_slots: int
    scale_down_idle_seconds: int
    scale_up_cooldown_seconds: int
    scale_down_cooldown_seconds: int
    drain_timeout_seconds: int
    force: bool = False
    disabled_reason: str | None = None
    idle_since_at: datetime | None = None
    last_scale_up_at: datetime | None = None
    last_scale_down_at: datetime | None = None
    actuator_config: dict[str, Any] | None = None
    qos_boost: str = ""
    qos_normal: str = ""


@dataclass(frozen=True)
class AutoscalerObservation:
    active_slots: int
    pending_slots: int
    draining_slots: int
    occupied_slots: int
    queued_slots: int
    idle_worker_ids: tuple[str, ...]
    drained_worker_ids: tuple[str, ...]
    release_drift_slots: int = 0
    release_drift_job_ids: tuple[str, ...] = ()
    release_drift_worker_ids_to_drain: tuple[str, ...] = ()
    release_drift_worker_ids_to_release: tuple[str, ...] = ()

    @property
    def claimable_free_slots(self) -> int:
        return max(0, self.active_slots - self.occupied_slots)


@dataclass(frozen=True)
class AutoscalerDecision:
    action: str
    reason: str
    desired_slots: int
    actual_slots: int
    pending_slots: int
    draining_slots: int
    occupied_slots: int
    queued_slots: int
    idle_since_at: datetime | None = None
    worker_ids_to_drain: tuple[str, ...] = ()
    worker_ids_to_release: tuple[str, ...] = ()
    blocked_reason: str | None = None
    blocked_details: dict[str, Any] | None = None
    error_message: str | None = None


@dataclass(frozen=True)
class SlurmScaleUpActuatorResult:
    error: str | None = None
    blocked_reason: str | None = None
    blocked_details: dict[str, Any] | None = None


def apply_global_dev_capacity_grant(
    policy: AutoscalerPolicyConfig,
    grant: AutoscalerGrantHandoff | None,
    *,
    deployment_generation: int | None,
    now: datetime | None = None,
) -> AutoscalerPolicyConfig:
    """Apply one exact global grant as a hard local autoscaler ceiling.

    Any missing, stale, expired, or differently bound grant clamps scaling to
    zero for this tick. Existing workers then follow the normal drain-first path;
    the global ledger keeps their slots committed until termination is observed.
    """
    now = now or datetime.now(UTC)
    actor_config = policy.actuator_config or {}
    candidate_sha = str(actor_config.get("candidate_sha") or "")
    reason: str | None = None
    if grant is None:
        reason = "missing"
    elif deployment_generation is None or deployment_generation <= 0:
        reason = "deployment_generation_missing"
    elif grant.environment != policy.environment or grant.pool_name != policy.pool_name:
        reason = "scope_mismatch"
    elif grant.deployment_generation != deployment_generation:
        reason = "deployment_generation_mismatch"
    elif grant.candidate_sha != candidate_sha:
        reason = "candidate_mismatch"
    else:
        try:
            expires_at = datetime.fromisoformat(grant.expires_at.replace("Z", "+00:00"))
        except ValueError:
            reason = "expiry_invalid"
        else:
            if expires_at.tzinfo is None or expires_at.astimezone(UTC) <= now.astimezone(UTC):
                reason = "expired"
    if reason is not None:
        return replace(
            policy,
            # Keep the decision engine enabled at a zero ceiling: queued work
            # is blocked from scale-up, while idle workers can still drain.
            enabled=policy.enabled,
            min_slots=0,
            max_slots=0,
            disabled_reason=f"global_dev_capacity_grant_{reason}",
        )
    assert grant is not None
    effective_max = min(policy.max_slots, grant.max_slots)
    return replace(
        policy,
        enabled=policy.enabled,
        min_slots=min(policy.min_slots, effective_max),
        max_slots=effective_max,
        disabled_reason=(
            policy.disabled_reason
            if grant.enabled and effective_max > 0
            else "global_dev_capacity_grant_zero"
        ),
    )


def select_slurm_qos(
    *,
    active_plus_pending: int,
    min_slots: int,
    qos_boost: str,
    qos_normal: str,
) -> str:
    """Pick the Slurm QoS for a scale-up submission.

    When the pool's DB-sourced ``active_plus_pending`` slot sum is below
    ``min_slots`` the pool is under its warm floor and submissions use the
    higher-priority ``qos_boost``; at or above the floor they use
    ``qos_normal``. ``qos_boost`` is only honoured when configured; otherwise
    ``qos_normal`` (which may itself be empty) is always returned.
    """
    if qos_boost and active_plus_pending < min_slots:
        return qos_boost
    return qos_normal


def _cooldown_active(
    last_at: datetime | None,
    *,
    cooldown_seconds: int,
    now: datetime,
) -> bool:
    if last_at is None:
        return False
    return last_at + timedelta(seconds=cooldown_seconds) > now


def _base_decision(
    *,
    action: str,
    reason: str,
    policy: AutoscalerPolicyConfig,
    observation: AutoscalerObservation,
    desired_slots: int | None = None,
    idle_since_at: datetime | None = None,
    worker_ids_to_drain: tuple[str, ...] = (),
    worker_ids_to_release: tuple[str, ...] = (),
    blocked_reason: str | None = None,
    blocked_details: dict[str, Any] | None = None,
    error_message: str | None = None,
) -> AutoscalerDecision:
    return AutoscalerDecision(
        action=action,
        reason=reason,
        desired_slots=policy.min_slots if desired_slots is None else desired_slots,
        actual_slots=observation.active_slots,
        pending_slots=observation.pending_slots,
        draining_slots=observation.draining_slots,
        occupied_slots=observation.occupied_slots,
        queued_slots=observation.queued_slots,
        idle_since_at=idle_since_at,
        worker_ids_to_drain=worker_ids_to_drain,
        worker_ids_to_release=worker_ids_to_release,
        blocked_reason=blocked_reason,
        blocked_details=blocked_details,
        error_message=error_message,
    )


def compute_autoscaler_decision(
    policy: AutoscalerPolicyConfig,
    observation: AutoscalerObservation,
    *,
    now: datetime | None = None,
) -> AutoscalerDecision:
    now = now or datetime.now(UTC)
    if not policy.enabled:
        return _base_decision(
            action="noop",
            reason=policy.disabled_reason or "disabled",
            policy=policy,
            observation=observation,
            desired_slots=policy.min_slots,
            idle_since_at=None,
        )

    if observation.release_drift_job_ids:
        job_ids = ", ".join(observation.release_drift_job_ids)
        if observation.release_drift_worker_ids_to_release:
            return _base_decision(
                action="release_drained",
                reason="release_state_drift",
                policy=policy,
                observation=observation,
                desired_slots=max(policy.min_slots, observation.active_slots),
                worker_ids_to_release=(observation.release_drift_worker_ids_to_release),
                idle_since_at=None,
                error_message=f"release-state drift in Slurm job(s): {job_ids}",
            )
        if observation.release_drift_worker_ids_to_drain:
            return _base_decision(
                action="request_drain",
                reason="release_state_drift",
                policy=policy,
                observation=observation,
                desired_slots=max(policy.min_slots, observation.active_slots),
                worker_ids_to_drain=observation.release_drift_worker_ids_to_drain,
                blocked_reason="release_state_drift",
                idle_since_at=None,
                error_message=f"release-state drift in Slurm job(s): {job_ids}",
            )
        return _base_decision(
            action="blocked",
            reason="release_state_drift",
            policy=policy,
            observation=observation,
            desired_slots=max(policy.min_slots, observation.active_slots),
            blocked_reason="release_state_drift",
            idle_since_at=None,
            error_message=f"release-state drift in Slurm job(s): {job_ids}",
        )

    # A zero global-dev grant is an active revocation, not merely a scale-up
    # ceiling. It must win over queued demand and cooldowns so pending Slurm
    # submissions are cancelled and live workers stop claiming new trials.
    # The actuator keeps running work drain-first; the broker retains its slot
    # commitment until the matching terminal observation arrives.
    if policy.max_slots == 0 and (
        observation.active_slots > 0
        or observation.pending_slots > 0
        or observation.draining_slots > 0
    ):
        if observation.drained_worker_ids:
            return _base_decision(
                action="release_drained",
                reason="capacity_authority_zero",
                policy=policy,
                observation=observation,
                desired_slots=0,
                worker_ids_to_release=observation.drained_worker_ids,
            )
        if observation.draining_slots > 0 and observation.pending_slots == 0:
            return _base_decision(
                action="noop",
                reason="capacity_authority_drain_in_progress",
                policy=policy,
                observation=observation,
                desired_slots=0,
            )
        return _base_decision(
            action="drain_capacity",
            reason="capacity_authority_zero",
            policy=policy,
            observation=observation,
            desired_slots=0,
            worker_ids_to_drain=observation.idle_worker_ids,
        )

    active_plus_pending = observation.active_slots + observation.pending_slots
    free_plus_pending = observation.claimable_free_slots + observation.pending_slots
    queue_deficit = observation.queued_slots - free_plus_pending
    min_deficit = policy.min_slots - active_plus_pending
    scale_up_deficit = max(queue_deficit, min_deficit, 0)
    if scale_up_deficit >= policy.scale_up_threshold_slots and scale_up_deficit > 0:
        desired_slots = min(policy.max_slots, active_plus_pending + scale_up_deficit)
        if desired_slots <= active_plus_pending:
            return _base_decision(
                action="blocked",
                reason="max_slots_reached",
                policy=policy,
                observation=observation,
                desired_slots=policy.max_slots,
                blocked_reason="max_slots_reached",
                idle_since_at=None,
            )
        if _cooldown_active(
            policy.last_scale_up_at,
            cooldown_seconds=policy.scale_up_cooldown_seconds,
            now=now,
        ):
            return _base_decision(
                action="blocked",
                reason="scale_up_cooldown",
                policy=policy,
                observation=observation,
                desired_slots=desired_slots,
                blocked_reason="scale_up_cooldown",
                idle_since_at=None,
            )
        return _base_decision(
            action="scale_up",
            reason="queued_deficit" if queue_deficit > 0 else "min_warm_capacity",
            policy=policy,
            observation=observation,
            desired_slots=desired_slots,
            idle_since_at=None,
        )

    if observation.drained_worker_ids:
        return _base_decision(
            action="release_drained",
            reason="drain_complete",
            policy=policy,
            observation=observation,
            desired_slots=policy.min_slots,
            idle_since_at=policy.idle_since_at,
            worker_ids_to_release=observation.drained_worker_ids,
        )

    if observation.draining_slots > 0:
        return _base_decision(
            action="noop",
            reason="waiting_for_drain",
            policy=policy,
            observation=observation,
            desired_slots=policy.min_slots,
            idle_since_at=policy.idle_since_at,
        )

    if observation.queued_slots > 0 or observation.occupied_slots > 0:
        return _base_decision(
            action="noop",
            reason="busy",
            policy=policy,
            observation=observation,
            desired_slots=max(policy.min_slots, active_plus_pending),
            idle_since_at=None,
        )

    excess_slots = max(0, observation.active_slots - policy.min_slots)
    if excess_slots <= 0:
        return _base_decision(
            action="noop",
            reason="at_min_capacity",
            policy=policy,
            observation=observation,
            desired_slots=policy.min_slots,
            idle_since_at=None,
        )

    idle_since_at = policy.idle_since_at or now
    if policy.idle_since_at is None:
        return _base_decision(
            action="noop",
            reason="idle_window_started",
            policy=policy,
            observation=observation,
            desired_slots=policy.min_slots,
            idle_since_at=idle_since_at,
        )

    if idle_since_at + timedelta(seconds=policy.scale_down_idle_seconds) > now:
        return _base_decision(
            action="noop",
            reason="idle_window_waiting",
            policy=policy,
            observation=observation,
            desired_slots=policy.min_slots,
            idle_since_at=idle_since_at,
        )

    if _cooldown_active(
        policy.last_scale_down_at,
        cooldown_seconds=policy.scale_down_cooldown_seconds,
        now=now,
    ):
        return _base_decision(
            action="blocked",
            reason="scale_down_cooldown",
            policy=policy,
            observation=observation,
            desired_slots=policy.min_slots,
            idle_since_at=idle_since_at,
            blocked_reason="scale_down_cooldown",
        )

    if not observation.idle_worker_ids:
        return _base_decision(
            action="blocked",
            reason="waiting_for_idle_workers",
            policy=policy,
            observation=observation,
            desired_slots=policy.min_slots,
            idle_since_at=idle_since_at,
            blocked_reason="waiting_for_idle_workers",
        )

    return _base_decision(
        action="request_drain",
        reason="idle_excess_capacity",
        policy=policy,
        observation=observation,
        desired_slots=policy.min_slots,
        idle_since_at=idle_since_at,
        worker_ids_to_drain=observation.idle_worker_ids,
    )


def _clean_nonempty(value: str, field: str) -> str:
    cleaned = str(value).strip()
    if not cleaned:
        raise ValueError(f"{field} must be a non-empty string")
    return cleaned


def _dt(value: datetime | None) -> str | None:
    return value.isoformat() if value is not None else None


def autoscaler_policy_to_dict(
    row: WorkerPoolAutoscalerPolicy,
) -> dict[str, object]:
    return {
        "id": str(row.id),
        "environment": row.environment,
        "pool_name": row.pool_name,
        "actuator": row.actuator,
        "enabled": row.enabled,
        "min_slots": row.min_slots,
        "max_slots": row.max_slots,
        "scale_up_threshold_slots": row.scale_up_threshold_slots,
        "scale_down_idle_seconds": row.scale_down_idle_seconds,
        "scale_up_cooldown_seconds": row.scale_up_cooldown_seconds,
        "scale_down_cooldown_seconds": row.scale_down_cooldown_seconds,
        "drain_timeout_seconds": row.drain_timeout_seconds,
        "force": row.force,
        "disabled_reason": row.disabled_reason,
        "actuator_config": row.actuator_config,
        "idle_since_at": _dt(row.idle_since_at),
        "last_decision": row.last_decision,
        "last_decision_reason": row.last_decision_reason,
        "last_desired_slots": row.last_desired_slots,
        "last_actual_slots": row.last_actual_slots,
        "last_pending_slots": row.last_pending_slots,
        "last_draining_slots": row.last_draining_slots,
        "last_occupied_slots": row.last_occupied_slots,
        "last_queued_slots": row.last_queued_slots,
        "last_blocked_reason": row.last_blocked_reason,
        "last_blocked_details": row.last_blocked_details,
        "last_error": row.last_error,
        "last_scale_up_at": _dt(row.last_scale_up_at),
        "last_scale_down_at": _dt(row.last_scale_down_at),
        "last_decision_at": _dt(row.last_decision_at),
        "created_at": _dt(row.created_at),
        "updated_at": _dt(row.updated_at),
    }


def _validate_policy_fields(
    *,
    actuator: str,
    min_slots: int,
    max_slots: int,
    scale_up_threshold_slots: int,
    scale_down_idle_seconds: int,
    scale_up_cooldown_seconds: int,
    scale_down_cooldown_seconds: int,
    drain_timeout_seconds: int,
) -> None:
    if actuator not in {"slurm", "gb10"}:
        raise ValueError("actuator must be one of: slurm, gb10")
    if min_slots < 0:
        raise ValueError("min_slots must be >= 0")
    if max_slots < min_slots:
        raise ValueError("max_slots must be >= min_slots")
    if scale_up_threshold_slots < 0:
        raise ValueError("scale_up_threshold_slots must be >= 0")
    if scale_down_idle_seconds < 0:
        raise ValueError("scale_down_idle_seconds must be >= 0")
    if scale_up_cooldown_seconds < 0:
        raise ValueError("scale_up_cooldown_seconds must be >= 0")
    if scale_down_cooldown_seconds < 0:
        raise ValueError("scale_down_cooldown_seconds must be >= 0")
    if drain_timeout_seconds <= 0:
        raise ValueError("drain_timeout_seconds must be > 0")


async def get_autoscaler_policy(
    session: AsyncSession,
    *,
    environment: str,
    pool_name: str,
) -> WorkerPoolAutoscalerPolicy | None:
    environment = _clean_nonempty(environment, "environment")
    pool_name = _clean_nonempty(pool_name, "pool_name")
    return (
        await session.execute(
            select(WorkerPoolAutoscalerPolicy).where(
                WorkerPoolAutoscalerPolicy.environment == environment,
                WorkerPoolAutoscalerPolicy.pool_name == pool_name,
            ),
        )
    ).scalar_one_or_none()


async def delete_autoscaler_policy_if_drained(
    session: AsyncSession,
    *,
    environment: str,
    pool_name: str,
) -> bool:
    """Delete an autoscaler policy only after every owned capacity row drained."""
    environment = _clean_nonempty(environment, "environment")
    pool_name = _clean_nonempty(pool_name, "pool_name")
    row = (
        await session.execute(
            select(WorkerPoolAutoscalerPolicy)
            .where(
                WorkerPoolAutoscalerPolicy.environment == environment,
                WorkerPoolAutoscalerPolicy.pool_name == pool_name,
            )
            .with_for_update(),
        )
    ).scalar_one_or_none()
    if row is None:
        return False
    active_job = (
        await session.execute(
            select(SlurmWorkerJob.id)
            .where(
                SlurmWorkerJob.environment == environment,
                SlurmWorkerJob.pool_name == pool_name,
                SlurmWorkerJob.state.in_(ACTIVE_STATES),
            )
            .limit(1),
        )
    ).scalar_one_or_none()
    live_worker = (
        await session.execute(
            select(Worker.id)
            .where(
                Worker.pool_name == pool_name,
                Worker.status == "active",
                Worker.drain_state != "drained",
            )
            .limit(1),
        )
    ).scalar_one_or_none()
    counters = (row.last_actual_slots, row.last_pending_slots, row.last_draining_slots)
    if (
        active_job is not None
        or live_worker is not None
        or any(int(value or 0) for value in counters)
    ):
        raise ValueError("autoscaler policy still owns active, pending, or draining capacity")
    await session.delete(row)
    await session.flush()
    return True


async def _enforce_dev_pool_envelope(
    session: AsyncSession,
    *,
    pool_name: str,
    actuator: str,
    min_slots: int,
    max_slots: int,
) -> None:
    """Reject a ``dev-<name>`` pool policy that falls outside the dev envelope.

    No-op for non-dev pools (base pools like ``oldlab``/``gb10`` are untouched).
    For a dev pool, delegates to :func:`validate_dev_instance` — the single
    source of truth the guarded dev-instances endpoint also uses. Policy maxima
    are demand ceilings, not static reservations; the global grant authority
    enforces the aggregate fleet budget transactionally at runtime. Raises
    ``ValueError`` (→ HTTP 400) on any envelope violation.
    """
    name = dev_pool_instance_name(pool_name)
    if name is None:
        return
    errors = validate_dev_instance(
        name,
        RequestedPolicy(actuator=actuator, min_slots=min_slots, max_slots=max_slots),
        (),
    )
    if errors:
        raise ValueError(
            f"dev pool {pool_name!r} policy is outside the dev envelope: " + "; ".join(errors),
        )


async def upsert_autoscaler_policy(
    session: AsyncSession,
    *,
    environment: str,
    pool_name: str,
    actuator: str,
    enabled: bool,
    min_slots: int,
    max_slots: int,
    scale_up_threshold_slots: int,
    scale_down_idle_seconds: int,
    scale_up_cooldown_seconds: int,
    scale_down_cooldown_seconds: int,
    drain_timeout_seconds: int,
    force: bool = False,
    disabled_reason: str | None = None,
    actuator_config: dict[str, Any] | None = None,
    now: datetime | None = None,
) -> WorkerPoolAutoscalerPolicy:
    environment = _clean_nonempty(environment, "environment")
    pool_name = _clean_nonempty(pool_name, "pool_name")
    actuator = _clean_nonempty(actuator, "actuator")
    _validate_policy_fields(
        actuator=actuator,
        min_slots=min_slots,
        max_slots=max_slots,
        scale_up_threshold_slots=scale_up_threshold_slots,
        scale_down_idle_seconds=scale_down_idle_seconds,
        scale_up_cooldown_seconds=scale_up_cooldown_seconds,
        scale_down_cooldown_seconds=scale_down_cooldown_seconds,
        drain_timeout_seconds=drain_timeout_seconds,
    )
    # Dev-instance admission (design phase 4 / #1166): a ``dev-<name>`` pool
    # policy must stay inside the dev envelope — slurm actuator, per-instance
    # cap, and the fleet-wide Σ(max_slots) budget across all live dev pools.
    # Enforced here (fail-closed, defense-in-depth) so it holds no matter which
    # caller writes the policy — the guarded dev-instances endpoint AND any
    # direct admin API call route through this one function.
    await _enforce_dev_pool_envelope(
        session,
        pool_name=pool_name,
        actuator=actuator,
        min_slots=min_slots,
        max_slots=max_slots,
    )
    now = now or datetime.now(UTC)
    row = await get_autoscaler_policy(
        session,
        environment=environment,
        pool_name=pool_name,
    )
    if row is None:
        row = WorkerPoolAutoscalerPolicy(
            environment=environment,
            pool_name=pool_name,
            actuator=actuator,
            max_slots=max_slots,
            updated_at=now,
        )
        session.add(row)
    row.actuator = actuator
    row.enabled = bool(enabled)
    row.min_slots = int(min_slots)
    row.max_slots = int(max_slots)
    row.scale_up_threshold_slots = int(scale_up_threshold_slots)
    row.scale_down_idle_seconds = int(scale_down_idle_seconds)
    row.scale_up_cooldown_seconds = int(scale_up_cooldown_seconds)
    row.scale_down_cooldown_seconds = int(scale_down_cooldown_seconds)
    row.drain_timeout_seconds = int(drain_timeout_seconds)
    row.force = bool(force)
    row.disabled_reason = disabled_reason
    row.actuator_config = dict(actuator_config or {})
    row.updated_at = now
    await session.flush()
    return row


async def fetch_autoscaler_status(
    session: AsyncSession,
    *,
    environment: str | None = None,
    pool_name: str | None = None,
) -> dict[str, list[dict[str, object]]]:
    stmt = select(WorkerPoolAutoscalerPolicy)
    if environment:
        stmt = stmt.where(WorkerPoolAutoscalerPolicy.environment == environment)
    if pool_name:
        stmt = stmt.where(WorkerPoolAutoscalerPolicy.pool_name == pool_name)
    rows = (
        (
            await session.execute(
                stmt.order_by(
                    WorkerPoolAutoscalerPolicy.environment,
                    WorkerPoolAutoscalerPolicy.pool_name,
                ),
            )
        )
        .scalars()
        .all()
    )
    return {"policies": [autoscaler_policy_to_dict(row) for row in rows]}


def _policy_to_config(row: WorkerPoolAutoscalerPolicy) -> AutoscalerPolicyConfig:
    return AutoscalerPolicyConfig(
        environment=row.environment,
        pool_name=row.pool_name,
        actuator=row.actuator,
        enabled=row.enabled,
        min_slots=row.min_slots,
        max_slots=row.max_slots,
        scale_up_threshold_slots=row.scale_up_threshold_slots,
        scale_down_idle_seconds=row.scale_down_idle_seconds,
        scale_up_cooldown_seconds=row.scale_up_cooldown_seconds,
        scale_down_cooldown_seconds=row.scale_down_cooldown_seconds,
        drain_timeout_seconds=row.drain_timeout_seconds,
        force=row.force,
        disabled_reason=row.disabled_reason,
        idle_since_at=row.idle_since_at,
        last_scale_up_at=row.last_scale_up_at,
        last_scale_down_at=row.last_scale_down_at,
        actuator_config=dict(row.actuator_config or {}),
        qos_boost=str((row.actuator_config or {}).get("qos_boost") or ""),
        qos_normal=str((row.actuator_config or {}).get("qos_normal") or ""),
    )


def _policy_uses_external_runner(row: WorkerPoolAutoscalerPolicy) -> bool:
    actor_config = row.actuator_config or {}
    return bool(actor_config.get("external_runner"))


def _optional_bool(value: object, *, default: bool) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"0", "false", "no", "off"}:
            return False
        if normalized in {"1", "true", "yes", "on"}:
            return True
    return bool(value)


def _queued_trial_matches_policy(
    requires_caps: object,
    row: WorkerPoolAutoscalerPolicy,
) -> bool:
    if not isinstance(requires_caps, dict) or not requires_caps:
        return True
    actor_config = row.actuator_config or {}
    policy_backend = actor_config.get("backend", "docker")
    policy_arch = actor_config.get(
        "cpu_arch",
        "arm64" if row.actuator == "gb10" else "x86_64",
    )
    backend = requires_caps.get("backend")
    if isinstance(backend, str) and backend != policy_backend:
        return False
    worker_pool = requires_caps.get("worker_pool")
    if (
        isinstance(worker_pool, str)
        and worker_pool.strip()
        and worker_pool.strip() != row.pool_name
    ):
        return False
    cpu_arch = requires_caps.get("cpu_arch")
    if isinstance(cpu_arch, str) and cpu_arch not in {policy_arch, "any"}:
        return False
    return True


def _field(obj: Any, name: str, default: Any = None) -> Any:
    if isinstance(obj, dict):
        return obj.get(name, default)
    return getattr(obj, name, default)


def _allowed_slurm_nodes(row: WorkerPoolAutoscalerPolicy) -> frozenset[str]:
    raw_nodes = (row.actuator_config or {}).get("allowed_nodes", ())
    values: list[object]
    if isinstance(raw_nodes, str):
        values = list(raw_nodes.split(","))
    elif isinstance(raw_nodes, list | tuple | set | frozenset):
        values = list(raw_nodes)
    else:
        values = []
    return frozenset(node for node in (str(raw_node).strip() for raw_node in values) if node)


def _expected_slurm_worker_token_fingerprint(
    row: WorkerPoolAutoscalerPolicy,
) -> str | None:
    env_file = (row.actuator_config or {}).get("env_file")
    if not isinstance(env_file, str) or not env_file:
        return None
    try:
        return worker_token_fingerprint_from_env_file(Path(env_file))
    except OSError:
        return None


def _slurm_release_state_drift(
    row: WorkerPoolAutoscalerPolicy,
    job: Any,
    *,
    expected_worker_token_fingerprint: str | None,
) -> bool:
    actor_config = row.actuator_config or {}
    nodelist = str(_field(job, "nodelist", "") or "").strip()
    if not nodelist or nodelist not in _allowed_slurm_nodes(row):
        return True

    redacted_env = _field(job, "redacted_env", {}) or {}
    if not isinstance(redacted_env, dict):
        return True

    expected_env_file = actor_config.get("env_file")
    if (
        isinstance(expected_env_file, str)
        and expected_env_file
        and redacted_env.get("LOOM_REMOTE_WORKER_ENV_FILE") != expected_env_file
    ):
        return True

    expected_repo_dir = actor_config.get("repo_dir")
    if (
        isinstance(expected_repo_dir, str)
        and expected_repo_dir
        and redacted_env.get("LOOM_REMOTE_WORKER_REPO_DIR") != expected_repo_dir
    ):
        return True

    if expected_worker_token_fingerprint:
        return (
            redacted_env.get(WORKER_AUTH_FINGERPRINT_ENV_KEY) != expected_worker_token_fingerprint
        )
    return False


async def _load_observation(
    session: AsyncSession,
    row: WorkerPoolAutoscalerPolicy,
    *,
    now: datetime,
    freshness_sec: int,
) -> AutoscalerObservation:
    cutoff = now - timedelta(seconds=freshness_sec)
    workers = (
        (
            await session.execute(
                select(Worker)
                .where(
                    Worker.pool_name == row.pool_name,
                    Worker.status == "active",
                    Worker.last_seen_at >= cutoff,
                )
                .order_by(Worker.hostname, Worker.id),
            )
        )
        .scalars()
        .all()
    )
    worker_ids = [worker.id for worker in workers]
    in_flight_by_worker = {worker_id: 0 for worker_id in worker_ids}
    if worker_ids:
        trial_rows = (
            await session.execute(
                select(Trial.worker_id).where(
                    Trial.worker_id.in_(worker_ids),
                    Trial.state.in_(("claimed", "running")),
                ),
            )
        ).all()
        for (worker_id,) in trial_rows:
            if worker_id is not None:
                in_flight_by_worker[worker_id] = (
                    in_flight_by_worker.get(
                        worker_id,
                        0,
                    )
                    + 1
                )

    pending_slots = int(row.last_pending_slots or 0)
    release_drift_slots = 0
    release_drift_job_ids: list[str] = []
    release_drift_worker_ids: set[Any] = set()
    release_drift_hostnames: set[str] = set()
    release_drift_worker_ids_to_drain: set[str] = set()
    release_drift_worker_ids_to_release: set[str] = set()
    # #1021: for the Slurm actuator, only workers this autoscaler actually
    # launched (linked to an active Slurm job by worker_id or by hostname) are
    # release/drain candidates. A fresh, unlinked, static worker must never be
    # drained by the Slurm actuator's idle release.
    slurm_owned_worker_ids: set[str] = set()
    if row.actuator == "slurm":
        pending_slots = 0
        expected_worker_token_fingerprint = _expected_slurm_worker_token_fingerprint(row)
        worker_by_id = {worker.id: worker for worker in workers}
        slurm_jobs = (
            (
                await session.execute(
                    select(SlurmWorkerJob).where(
                        SlurmWorkerJob.environment == row.environment,
                        SlurmWorkerJob.pool_name == row.pool_name,
                        SlurmWorkerJob.state.in_(ACTIVE_STATES),
                    ),
                )
            )
            .scalars()
            .all()
        )
        active_job_count_by_worker_id: dict[Any, int] = {}
        for job in slurm_jobs:
            job_worker_id = _field(job, "worker_id")
            if job_worker_id is not None:
                active_job_count_by_worker_id[job_worker_id] = (
                    active_job_count_by_worker_id.get(job_worker_id, 0) + 1
                )
        owned_hostnames = {
            str(_field(job, "nodelist")) for job in slurm_jobs if _field(job, "nodelist")
        }
        owned_worker_ids = {
            _field(job, "worker_id") for job in slurm_jobs if _field(job, "worker_id") is not None
        }
        for worker in workers:
            if (
                worker.id in owned_worker_ids
                or str(getattr(worker, "hostname", "")) in owned_hostnames
            ):
                slurm_owned_worker_ids.add(str(worker.id))
        for job in slurm_jobs:
            slots = max(0, int(_field(job, "requested_concurrency", 0) or 0))
            if _slurm_release_state_drift(
                row,
                job,
                expected_worker_token_fingerprint=expected_worker_token_fingerprint,
            ):
                release_drift_slots += slots
                job_id = str(_field(job, "job_id") or _field(job, "id") or "unknown")
                release_drift_job_ids.append(job_id)
                worker_id = _field(job, "worker_id")
                if worker_id is not None:
                    release_drift_worker_ids.add(worker_id)
                nodelist = _field(job, "nodelist")
                if nodelist:
                    release_drift_hostnames.add(str(nodelist))
                linked_worker = (
                    worker_by_id.get(worker_id)
                    if worker_id is not None and active_job_count_by_worker_id.get(worker_id) == 1
                    else None
                )
                if linked_worker is not None and str(linked_worker.hostname) != str(nodelist):
                    linked_worker = None
                job_state = str(_field(job, "state", "")).strip().lower()
                if (
                    job_state == "running"
                    and linked_worker is not None
                    and linked_worker.drain_state == "active"
                ):
                    release_drift_worker_ids_to_drain.add(str(linked_worker.id))
                elif (
                    job_state == "running"
                    and linked_worker is not None
                    and linked_worker.drain_state in {"draining", "drained"}
                ):
                    if in_flight_by_worker.get(linked_worker.id, 0) == 0:
                        release_drift_worker_ids_to_release.add(str(linked_worker.id))
                continue
            if str(_field(job, "state", "")).strip().lower() == "pending":
                pending_slots += slots

    active_slots = 0
    draining_slots = 0
    active_idle: list[tuple[str, int]] = []
    drained_worker_ids: list[str] = []
    for worker in workers:
        if (
            worker.id in release_drift_worker_ids
            or str(
                getattr(worker, "hostname", ""),
            )
            in release_drift_hostnames
        ):
            continue
        slots = max(1, int(worker.max_concurrent or 1))
        in_flight = in_flight_by_worker.get(worker.id, 0)
        if worker.drain_state == "active":
            active_slots += slots
            # #1021: static/unlinked workers still count toward capacity, but the
            # Slurm actuator may only release workers it owns.
            drain_eligible = row.actuator != "slurm" or str(worker.id) in slurm_owned_worker_ids
            if in_flight == 0 and drain_eligible:
                active_idle.append((str(worker.id), slots))
        elif worker.drain_state == "draining":
            draining_slots += slots
            if in_flight == 0:
                drained_worker_ids.append(str(worker.id))

    excess_slots = max(0, active_slots - row.min_slots)
    selected_idle: list[str] = []
    selected_slots = 0
    for worker_id, slots in active_idle:
        if selected_slots >= excess_slots:
            break
        selected_idle.append(worker_id)
        selected_slots += slots

    queued_rows = (
        await session.execute(
            select(Trial.requires_caps).where(Trial.state == "queued"),
        )
    ).all()
    queued_slots = sum(
        1 for (requires_caps,) in queued_rows if _queued_trial_matches_policy(requires_caps, row)
    )

    return AutoscalerObservation(
        active_slots=active_slots,
        pending_slots=pending_slots,
        draining_slots=draining_slots,
        occupied_slots=sum(in_flight_by_worker.values()),
        queued_slots=queued_slots,
        idle_worker_ids=tuple(selected_idle),
        drained_worker_ids=tuple(drained_worker_ids),
        release_drift_slots=release_drift_slots,
        release_drift_job_ids=tuple(release_drift_job_ids),
        release_drift_worker_ids_to_drain=tuple(
            sorted(release_drift_worker_ids_to_drain),
        ),
        release_drift_worker_ids_to_release=tuple(
            sorted(release_drift_worker_ids_to_release),
        ),
    )


async def _refresh_slurm_job_registry(
    session: AsyncSession,
    row: WorkerPoolAutoscalerPolicy,
    *,
    runner: SlurmWorkerCommandRunner | None,
    now: datetime,
) -> str | None:
    if row.actuator != "slurm":
        return None
    job_ids = tuple(
        str(job_id)
        for (job_id,) in (
            await session.execute(
                select(SlurmWorkerJob.job_id).where(
                    SlurmWorkerJob.environment == row.environment,
                    SlurmWorkerJob.pool_name == row.pool_name,
                    SlurmWorkerJob.state.in_(ACTIVE_STATES),
                    SlurmWorkerJob.job_id.is_not(None),
                ),
            )
        ).all()
        if job_id is not None
    )
    if not job_ids:
        return None
    config = _slurm_config_from_policy(row)
    runner = runner or SubprocessSlurmCommandRunner().bind_config(config)
    try:
        observations = await runner.query_jobs(job_ids)
        await reconcile_slurm_worker_jobs(
            session,
            observations,
            stale_after_seconds=config.stale_after_seconds,
            now=now,
        )
    except Exception as exc:
        logger.warning(
            "worker_pool_autoscaler_slurm_query_failed",
            extra={
                "environment": row.environment,
                "pool_name": row.pool_name,
                "err": str(exc),
            },
        )
        return str(exc)
    return None


async def _request_worker_drain(
    session: AsyncSession,
    *,
    worker_ids: tuple[str, ...],
    now: datetime,
    reason: str,
) -> None:
    if not worker_ids:
        return
    await session.execute(
        update(Worker)
        .where(Worker.id.in_(worker_ids))
        .where(Worker.drain_state == "active")
        .values(
            drain_state="draining",
            drain_requested_at=now,
            drain_reason=reason,
            drain_owner="worker-pool-autoscaler",
        ),
    )


def _persist_decision(
    row: WorkerPoolAutoscalerPolicy,
    decision: AutoscalerDecision,
    *,
    now: datetime,
) -> None:
    row.idle_since_at = decision.idle_since_at
    row.last_decision = decision.action
    row.last_decision_reason = decision.reason
    row.last_desired_slots = decision.desired_slots
    row.last_actual_slots = decision.actual_slots
    row.last_pending_slots = decision.pending_slots
    row.last_draining_slots = decision.draining_slots
    row.last_occupied_slots = decision.occupied_slots
    row.last_queued_slots = decision.queued_slots
    row.last_blocked_reason = decision.blocked_reason
    row.last_blocked_details = decision.blocked_details
    row.last_error = decision.error_message
    row.last_decision_at = now
    row.updated_at = now
    if decision.action == "scale_up":
        row.last_scale_up_at = now
    if decision.action in {"request_drain", "release_drained"}:
        row.last_scale_down_at = now


def _decision_with_observation(
    decision: AutoscalerDecision,
    observation: AutoscalerObservation,
) -> AutoscalerDecision:
    return replace(
        decision,
        actual_slots=observation.active_slots,
        pending_slots=observation.pending_slots,
        draining_slots=observation.draining_slots,
        occupied_slots=observation.occupied_slots,
        queued_slots=observation.queued_slots,
    )


def _slurm_config_from_policy(
    row: WorkerPoolAutoscalerPolicy,
) -> ElasticSlurmWorkerControllerConfig:
    actor_config = dict(row.actuator_config or {})
    allowed_nodes_raw = actor_config.get("allowed_nodes", ())
    if isinstance(allowed_nodes_raw, str):
        allowed_nodes_csv = allowed_nodes_raw
        allowed_nodes = tuple(
            node for node in (part.strip() for part in allowed_nodes_raw.split(",")) if node
        )
    else:
        allowed_nodes = tuple(
            node for node in (str(raw_node).strip() for raw_node in allowed_nodes_raw) if node
        )
        allowed_nodes_csv = ",".join(allowed_nodes)
    requested_concurrency = int(actor_config.get("requested_concurrency") or 1)
    resource_aware = _optional_bool(actor_config.get("resource_aware"), default=False)
    if actor_config.get("max_jobs") is not None:
        max_jobs = int(actor_config["max_jobs"])
    elif resource_aware:
        max_jobs = max(1, len(allowed_nodes))
    else:
        max_jobs = max(1, math.ceil(row.max_slots / requested_concurrency))
    pending_job_cap = int(actor_config.get("pending_job_cap") or max_jobs)
    config = build_controller_config(
        enabled=True,
        environment=row.environment,
        pool_name=row.pool_name,
        allowed_nodes_csv=allowed_nodes_csv,
        env_file=str(actor_config.get("env_file") or ""),
        repo_dir=str(actor_config.get("repo_dir") or ""),
        partition=str(actor_config.get("partition") or ""),
        time_limit=str(actor_config.get("time_limit") or "7-00:00:00"),
        requested_cpus=int(actor_config.get("requested_cpus") or 1),
        requested_memory_mib=int(actor_config.get("requested_memory_mib") or 1),
        requested_concurrency=requested_concurrency,
        max_jobs=max_jobs,
        pending_job_cap=pending_job_cap,
        min_queued_trials=1,
        stale_after_seconds=int(actor_config.get("stale_after_seconds") or 300),
        sbatch_path=str(actor_config.get("sbatch_path") or "sbatch"),
        squeue_path=str(actor_config.get("squeue_path") or "squeue"),
        sacct_path=str(actor_config.get("sacct_path") or "sacct"),
        scancel_path=str(actor_config.get("scancel_path") or "scancel"),
        command_timeout_seconds=float(
            actor_config.get("command_timeout_seconds") or 20.0,
        ),
        exclusive=_optional_bool(actor_config.get("exclusive"), default=False),
        slurm_account=str(actor_config.get("slurm_account") or ""),
        slurm_qos=str(actor_config.get("qos_normal") or actor_config.get("slurm_qos") or ""),
        slurm_reservation=str(actor_config.get("slurm_reservation") or ""),
        sinfo_path=str(actor_config.get("sinfo_path") or "sinfo"),
        resource_aware=resource_aware,
        cpu_per_slot=int(actor_config.get("cpu_per_slot") or 2),
        memory_mib_per_slot=int(actor_config.get("memory_mib_per_slot") or 8192),
        reserved_cpus=int(actor_config.get("reserved_cpus") or 4),
        reserved_memory_mib=int(actor_config.get("reserved_memory_mib") or 24_576),
        max_concurrency_per_node=int(
            actor_config.get("max_concurrency_per_node") or 8,
        ),
        max_cpu_load_ratio=float(actor_config.get("max_cpu_load_ratio") or 1.0),
        # #896 per-container caps (0/unset = unbounded).
        container_cpus=float(actor_config.get("container_cpus") or 0.0),
        container_memory_mib=int(actor_config.get("container_memory_mib") or 0),
        container_pids=int(actor_config.get("container_pids") or 0),
        job_pids_max=actor_config.get("job_pids_max", 0),
        candidate_sha=str(actor_config.get("candidate_sha") or ""),
        gpu_tres=str(actor_config.get("gpu_tres") or ""),
        job_output_dir=str(actor_config.get("job_output_dir") or ""),
    )
    if config is None:
        raise ValueError("Slurm autoscaler policy unexpectedly disabled")
    return config


def _worker_env_from_slurm_config(
    config: ElasticSlurmWorkerControllerConfig,
) -> dict[str, str]:
    sandbox_identity = slurm_sandbox_identity(config)
    env = {
        "LOOM_WORKER_MAX_CONCURRENT": str(config.requested_concurrency),
        "LOOM_WORKER_POOL_NAME": config.pool_name,
        "LOOM_REMOTE_WORKER_ENV_FILE": config.env_file,
        "LOOM_REMOTE_WORKER_REPO_DIR": config.repo_dir,
        "LOOM_WORKER_SANDBOX_IDENTITY": sandbox_identity,
        "LOOM_WORKER_CANDIDATE_SHA": config.candidate_sha,
        "LOOM_WORKER_SLURM_ALLOCATED_GPUS": str(config.requested_gpus),
    }
    # #896: propagate per-container caps when configured (0/unset = unbounded).
    if config.container_cpus > 0:
        env["LOOM_WORKER_CONTAINER_CPUS"] = str(config.container_cpus)
    if config.container_memory_mib > 0:
        env["LOOM_WORKER_CONTAINER_MEMORY_MIB"] = str(config.container_memory_mib)
    if config.container_pids > 0:
        env["LOOM_WORKER_CONTAINER_PIDS"] = str(config.container_pids)
    try:
        fingerprint = worker_token_fingerprint_from_env_file(Path(config.env_file))
    except OSError as exc:
        logger.warning(
            "slurm_worker_token_fingerprint_unavailable",
            extra={
                "environment": config.environment,
                "pool_name": config.pool_name,
                "env_file": config.env_file,
                "err": str(exc),
            },
        )
    else:
        if fingerprint:
            env[WORKER_AUTH_FINGERPRINT_ENV_KEY] = fingerprint
    return env


def _slurm_node_exclusion_details(
    slurm_decision: Any,
    *,
    node_resources: dict[str, Any] | None,
) -> list[dict[str, object]]:
    resources = node_resources or {}
    details: list[dict[str, object]] = []
    for node, plan in slurm_decision.node_capacity.items():
        resource = resources.get(node)
        row: dict[str, object] = {
            "hostname": node,
            "reason": plan.reason,
            "safe_slots": plan.safe_slots,
        }
        if resource is not None:
            row.update(
                {
                    "state": resource.state,
                    "cpus_total": resource.cpus_total,
                    "idle_cpus": resource.idle_cpus,
                    "cpu_load": resource.cpu_load,
                    "free_memory_mib": resource.free_memory_mib,
                },
            )
        details.append(row)
    return details


def _no_safe_slurm_nodes_details(
    config: ElasticSlurmWorkerControllerConfig,
    slurm_decision: Any,
    *,
    node_resources: dict[str, Any] | None,
) -> dict[str, object]:
    return {
        "reason": "no_safe_slurm_nodes",
        "slurm_decision_reason": slurm_decision.reason,
        "resource_aware": config.resource_aware,
        "allowed_nodes": list(config.allowed_nodes),
        "node_exclusions": _slurm_node_exclusion_details(
            slurm_decision,
            node_resources=node_resources,
        ),
    }


def _slurm_config_blocker(
    row: WorkerPoolAutoscalerPolicy, exc: ValueError
) -> tuple[str, dict[str, object]]:
    message = str(exc)
    actor_config = row.actuator_config or {}
    allowed_nodes = actor_config.get("allowed_nodes")
    if "allowed nodes are required" in message:
        return (
            "missing_slurm_allowed_nodes",
            {
                "reason": "missing_slurm_allowed_nodes",
                "message": message,
                "resource_aware": bool(actor_config.get("resource_aware")),
                "allowed_nodes": [] if allowed_nodes is None else allowed_nodes,
            },
        )
    return (
        "slurm_autoscaler_config_invalid",
        {
            "reason": "slurm_autoscaler_config_invalid",
            "message": message,
        },
    )


async def _apply_slurm_scale_up(
    session: AsyncSession,
    row: WorkerPoolAutoscalerPolicy,
    decision: AutoscalerDecision,
    *,
    runner: SlurmWorkerCommandRunner | None,
    now: datetime,
    max_slots: int | None = None,
) -> SlurmScaleUpActuatorResult:
    try:
        config = _slurm_config_from_policy(row)
    except ValueError as exc:
        blocked_reason, blocked_details = _slurm_config_blocker(row, exc)
        return SlurmScaleUpActuatorResult(
            error=str(exc),
            blocked_reason=blocked_reason,
            blocked_details=blocked_details,
        )
    runner = runner or SubprocessSlurmCommandRunner().bind_config(config)
    recorded_active_jobs = (
        (
            await session.execute(
                select(SlurmWorkerJob).where(
                    SlurmWorkerJob.environment == row.environment,
                    SlurmWorkerJob.pool_name == row.pool_name,
                    SlurmWorkerJob.state.in_(ACTIVE_STATES),
                ),
            )
        )
        .scalars()
        .all()
    )
    expected_worker_token_fingerprint = _expected_slurm_worker_token_fingerprint(row)
    release_drift_jobs = [
        job
        for job in recorded_active_jobs
        if _slurm_release_state_drift(
            row,
            job,
            expected_worker_token_fingerprint=expected_worker_token_fingerprint,
        )
    ]
    if release_drift_jobs:
        drift_job_ids = sorted(
            str(_field(job, "job_id") or _field(job, "id") or "unknown")
            for job in release_drift_jobs
        )
        return SlurmScaleUpActuatorResult(
            error=f"release-state drift in Slurm job(s): {', '.join(drift_job_ids)}",
            blocked_reason="release_state_drift",
            blocked_details={
                "reason": "release_state_drift",
                "job_ids": drift_job_ids,
                "nodes": sorted(
                    {
                        str(_field(job, "nodelist"))
                        for job in release_drift_jobs
                        if _field(job, "nodelist")
                    },
                ),
            },
        )
    active_jobs = recorded_active_jobs
    active_nodes = {job.nodelist for job in active_jobs}
    pending_jobs = sum(1 for job in active_jobs if job.state == "pending")
    running_jobs = sum(1 for job in active_jobs if job.state == "running")
    active_job_ids = tuple(
        str(job.job_id) for job in active_jobs if getattr(job, "job_id", None) is not None
    )
    node_resources = None
    if config.resource_aware:
        try:
            node_resources = await runner.query_node_resources(config.allowed_nodes)
        except Exception as exc:
            return SlurmScaleUpActuatorResult(error=str(exc))

    slurm_decision = compute_controller_decision(
        config,
        SlurmWorkerCapacitySnapshot(
            queued_trials=decision.desired_slots,
            running_trials=decision.occupied_slots,
            pending_jobs=pending_jobs,
            running_jobs=running_jobs,
            active_slots=decision.actual_slots,
            pending_slots=decision.pending_slots,
            active_nodes=active_nodes,
            cancellable_pending_job_ids=(),
            active_job_ids=active_job_ids,
            node_resources=node_resources,
        ),
    )
    if (
        config.resource_aware
        and slurm_decision.reason == "no_safe_nodes"
        and not slurm_decision.submit_nodes
    ):
        return SlurmScaleUpActuatorResult(
            blocked_reason="no_safe_slurm_nodes",
            blocked_details=_no_safe_slurm_nodes_details(
                config,
                slurm_decision,
                node_resources=node_resources,
            ),
        )
    actor_config = dict(row.actuator_config or {})
    active_plus_pending = decision.actual_slots + decision.pending_slots
    # Strip like build_controller_config does, so operator-authored trailing
    # whitespace never leaks into a submitted --qos=<value>.
    qos_boost_value = str(actor_config.get("qos_boost") or "").strip()
    # Contract: qos_normal is primary; legacy slurm_qos is only the fallback.
    qos_normal_value = str(
        actor_config.get("qos_normal") or actor_config.get("slurm_qos") or "",
    ).strip()
    # Stateful budget clamp + PER-SUBMISSION QoS. Submissions must never push the
    # pool's committed slot sum past ``max_slots``. QoS is chosen per submission
    # from the committed slot sum at the moment each job enters the pool, so a
    # reconcile that starts below ``min_slots`` and crosses it mid-loop gives the
    # boost QoS only to the jobs submitted while still below the floor.
    committed_slots = active_plus_pending
    remaining_budget = (row.max_slots if max_slots is None else max_slots) - active_plus_pending
    actuator_error: str | None = None
    for node in slurm_decision.submit_nodes:
        if remaining_budget <= 0:
            break
        node_config = slurm_submission_config_for_node(
            config,
            slurm_decision,
            node=node,
        )
        effective_concurrency = node_config.requested_concurrency
        per_worker = min(effective_concurrency, remaining_budget)
        if per_worker <= 0:
            break
        submission_qos = select_slurm_qos(
            active_plus_pending=committed_slots,
            min_slots=row.min_slots,
            qos_boost=qos_boost_value,
            qos_normal=qos_normal_value,
        )
        if per_worker < effective_concurrency:
            # Scale CPU/memory proportionally from the effective pre-clamp
            # request, NOT from the independent per-slot defaults: a
            # 10-slot/115000 MiB worker clamped to 4 slots must request
            # 46000 MiB (115000 * 4 / 10), not 4 * memory_mib_per_slot.
            # Use ceil, never round: round() is banker's rounding, so e.g.
            # 5 CPU / 2 slots clamped to 1 would give round(2.5)=2 -- below the
            # proportional requirement. ceil(2.5)=3 never under-requests.
            node_config = replace(
                node_config,
                requested_concurrency=per_worker,
                requested_cpus=max(
                    1,
                    math.ceil(node_config.requested_cpus * per_worker / effective_concurrency),
                ),
                requested_memory_mib=max(
                    1,
                    math.ceil(
                        node_config.requested_memory_mib * per_worker / effective_concurrency
                    ),
                ),
            )
        if submission_qos != node_config.slurm_qos:
            node_config = replace(node_config, slurm_qos=submission_qos)
        remaining_budget -= per_worker
        committed_slots += per_worker
        try:
            job_id = await runner.submit_worker(node=node, config=node_config)
            await record_slurm_worker_job(
                session,
                environment=row.environment,
                pool_name=row.pool_name,
                nodelist=node,
                requested_cpus=node_config.requested_cpus,
                requested_memory_mib=node_config.requested_memory_mib,
                requested_pids=node_config.container_pids or None,
                requested_gpu_tres=node_config.gpu_tres or None,
                requested_gpus=node_config.requested_gpus,
                requested_concurrency=node_config.requested_concurrency,
                sandbox_identity=slurm_sandbox_identity(node_config),
                candidate_sha=node_config.candidate_sha or None,
                compose_project=slurm_compose_project_identity(node_config, job_id),
                job_id=job_id,
                slurm_state="PENDING",
                pending_reason=None,
                env=_worker_env_from_slurm_config(node_config),
                submitted_at=now,
            )
        except Exception as exc:
            actuator_error = str(exc)
            await record_slurm_worker_job(
                session,
                environment=row.environment,
                pool_name=row.pool_name,
                nodelist=node,
                requested_cpus=node_config.requested_cpus,
                requested_memory_mib=node_config.requested_memory_mib,
                requested_pids=node_config.container_pids or None,
                requested_gpu_tres=node_config.gpu_tres or None,
                requested_gpus=node_config.requested_gpus,
                requested_concurrency=node_config.requested_concurrency,
                sandbox_identity=slurm_sandbox_identity(node_config),
                candidate_sha=node_config.candidate_sha or None,
                job_id=None,
                slurm_state="FAILED",
                pending_reason=None,
                env=_worker_env_from_slurm_config(node_config),
                submitted_at=now,
                submission_error=str(exc),
            )
    return SlurmScaleUpActuatorResult(error=actuator_error)


async def _apply_slurm_release_drained(
    session: AsyncSession,
    row: WorkerPoolAutoscalerPolicy,
    decision: AutoscalerDecision,
    *,
    runner: SlurmWorkerCommandRunner | None,
    now: datetime,
    freshness_sec: int = 120,
) -> SlurmScaleUpActuatorResult:
    if not decision.worker_ids_to_release:
        return SlurmScaleUpActuatorResult()
    policy_id = row.id
    expected_environment = row.environment
    expected_pool_name = row.pool_name
    current_row = (
        await session.execute(
            select(WorkerPoolAutoscalerPolicy)
            .where(WorkerPoolAutoscalerPolicy.id == policy_id)
            .execution_options(populate_existing=True)
            .with_for_update(),
        )
    ).scalar_one_or_none()
    if (
        current_row is None
        or not current_row.enabled
        or current_row.actuator != "slurm"
        or current_row.environment != expected_environment
        or current_row.pool_name != expected_pool_name
    ):
        return SlurmScaleUpActuatorResult(
            error="release-state drift release blocked: autoscaler policy changed",
            blocked_reason="release_state_drift",
            blocked_details={
                "reason": "release_state_drift",
                "worker_ids": sorted(decision.worker_ids_to_release),
                "guard_errors": ["autoscaler policy changed"],
            },
        )
    config = _slurm_config_from_policy(current_row)
    runner = runner or SubprocessSlurmCommandRunner().bind_config(config)
    released_worker_ids = set(decision.worker_ids_to_release)
    release_workers = (
        (
            await session.execute(
                select(Worker).where(Worker.id.in_(released_worker_ids)).with_for_update(),
            )
        )
        .scalars()
        .all()
    )
    worker_by_id = {str(worker.id): worker for worker in release_workers}
    release_hostnames = {
        str(worker.hostname) for worker in release_workers if worker.hostname
    }
    in_flight_rows = (
        await session.execute(
            select(Trial.worker_id)
            .where(
                Trial.worker_id.in_(released_worker_ids),
                Trial.state.in_(("claimed", "running")),
            )
            .with_for_update(),
        )
    ).all()
    jobs = (
        (
            await session.execute(
                select(SlurmWorkerJob)
                .where(
                    or_(
                        SlurmWorkerJob.worker_id.in_(released_worker_ids),
                        (
                            SlurmWorkerJob.worker_id.is_(None)
                            & SlurmWorkerJob.nodelist.in_(release_hostnames)
                        ),
                    ),
                    SlurmWorkerJob.environment == current_row.environment,
                    SlurmWorkerJob.pool_name == current_row.pool_name,
                    SlurmWorkerJob.state.in_(ACTIVE_STATES),
                )
                .with_for_update(),
            )
        )
        .scalars()
        .all()
    )
    jobs_by_worker_id: dict[str, list[SlurmWorkerJob]] = {}
    unlinked_jobs_by_hostname: dict[str, list[SlurmWorkerJob]] = {}
    for job in jobs:
        if job.worker_id is not None:
            jobs_by_worker_id.setdefault(str(job.worker_id), []).append(job)
        elif job.nodelist:
            unlinked_jobs_by_hostname.setdefault(str(job.nodelist), []).append(job)

    expected_worker_ids = {str(worker_id) for worker_id in released_worker_ids}
    in_flight_worker_ids = {
        str(worker_id) for (worker_id,) in in_flight_rows if worker_id is not None
    }
    guard_errors: list[str] = []
    expected_worker_token_fingerprint = _expected_slurm_worker_token_fingerprint(
        current_row,
    )
    jobs_to_cancel: list[SlurmWorkerJob] = []
    for worker_id in sorted(expected_worker_ids):
        worker = worker_by_id.get(worker_id)
        if worker is None:
            guard_errors.append(f"{worker_id}: fresh active worker missing")
            continue
        worker_jobs = [
            *jobs_by_worker_id.get(worker_id, []),
            *unlinked_jobs_by_hostname.get(str(worker.hostname), []),
        ]
        if worker.status != "active" or worker.last_seen_at < now - timedelta(
            seconds=freshness_sec
        ):
            guard_errors.append(f"{worker_id}: worker is not fresh and active")
        if worker.drain_state not in {"draining", "drained"}:
            guard_errors.append(f"{worker_id}: worker is not draining")
        if worker_id in in_flight_worker_ids:
            guard_errors.append(f"{worker_id}: worker still has in-flight trials")
        if len(worker_jobs) > 1:
            guard_errors.append(
                f"{worker_id}: expected at most one active Slurm job, found {len(worker_jobs)}",
            )
            continue
        if not worker_jobs:
            continue
        job = worker_jobs[0]
        if job.environment != current_row.environment or job.pool_name != current_row.pool_name:
            guard_errors.append(f"{worker_id}: Slurm job belongs to another policy")
        if job.state != "running":
            guard_errors.append(f"{worker_id}: Slurm job is not running")
        if job.nodelist != worker.hostname:
            guard_errors.append(f"{worker_id}: Slurm job hostname does not match worker")
        if not job.job_id:
            guard_errors.append(f"{worker_id}: Slurm job id is missing")
        if decision.reason == "release_state_drift" and not _slurm_release_state_drift(
            current_row,
            job,
            expected_worker_token_fingerprint=expected_worker_token_fingerprint,
        ):
            guard_errors.append(f"{worker_id}: Slurm job no longer has release-state drift")
        jobs_to_cancel.append(job)

    if guard_errors:
        message = "; ".join(guard_errors)
        return SlurmScaleUpActuatorResult(
            error=f"release-state drift release blocked: {message}",
            blocked_reason="release_state_drift",
            blocked_details={
                "reason": "release_state_drift",
                "worker_ids": sorted(expected_worker_ids),
                "guard_errors": guard_errors,
            },
        )

    for job in jobs_to_cancel:
        if job.job_id:
            await runner.cancel_job(job.job_id)
        job.state = "cancelled"
        job.slurm_state = "CANCELLED"
        job.pending_reason = "cancelled after autoscaler drain"
        job.finished_at = now
        job.updated_at = now
    await session.execute(
        update(Worker)
        .where(Worker.id.in_(released_worker_ids))
        .where(Worker.drain_state.in_(("draining", "drained")))
        .values(
            drain_state="drained",
            drain_reason="autoscaler release completed",
            drain_owner="worker-pool-autoscaler",
        ),
    )
    return SlurmScaleUpActuatorResult()


async def _apply_slurm_capacity_authority_drain(
    session: AsyncSession,
    row: WorkerPoolAutoscalerPolicy,
    *,
    runner: SlurmWorkerCommandRunner | None,
    now: datetime,
) -> SlurmScaleUpActuatorResult:
    """Converge an externally revoked dev grant toward zero capacity.

    Pending jobs have executed no user work and are safe to cancel immediately.
    Running workers are fenced to ``draining`` so in-flight trials finish but
    no new claims begin. Their Slurm jobs remain owned by the normal guarded
    ``release_drained`` path, which verifies freshness, ownership, and zero
    in-flight trials before calling ``scancel``.
    """
    try:
        config = _slurm_config_from_policy(row)
    except ValueError as exc:
        blocked_reason, blocked_details = _slurm_config_blocker(row, exc)
        return SlurmScaleUpActuatorResult(
            error=str(exc),
            blocked_reason=blocked_reason,
            blocked_details=blocked_details,
        )
    runner = runner or SubprocessSlurmCommandRunner().bind_config(config)
    pending_jobs = (
        (
            await session.execute(
                select(SlurmWorkerJob)
                .where(
                    SlurmWorkerJob.environment == row.environment,
                    SlurmWorkerJob.pool_name == row.pool_name,
                    SlurmWorkerJob.state == "pending",
                )
                .with_for_update(),
            )
        )
        .scalars()
        .all()
    )
    try:
        for job in pending_jobs:
            if job.job_id:
                await runner.cancel_job(job.job_id)
            job.state = "cancelled"
            job.slurm_state = "CANCELLED"
            job.pending_reason = "cancelled after global capacity grant revocation"
            job.finished_at = now
            job.updated_at = now
    except Exception as exc:
        return SlurmScaleUpActuatorResult(error=str(exc))

    await session.execute(
        update(Worker)
        .where(
            Worker.pool_name == row.pool_name,
            Worker.status == "active",
            Worker.drain_state == "active",
        )
        .values(
            drain_state="draining",
            drain_requested_at=now,
            drain_reason="global development capacity grant revoked",
            drain_owner="global-dev-fleet-autoscaler",
        ),
    )
    return SlurmScaleUpActuatorResult()


async def _apply_slurm_prod_pressure_drain(
    session: AsyncSession,
    row: WorkerPoolAutoscalerPolicy,
    *,
    runner: SlurmWorkerCommandRunner | None,
    now: datetime,
) -> dict[str, Any] | None:
    """Consume the prod-pressure drain intent recorded by the CP handler (#892).

    Single-writer: this is the ONLY place a prod-pressure drain scancels
    ``SlurmWorkerJob``s and flips ``Worker.drain_state``. Returns a summary when
    a drain intent is active (so the caller skips normal scaling this tick), or
    ``None`` when there is no active intent. Only ``cancel_retryable``
    (preemptible + grace elapsed) reclaims jobs now; ``wait`` /
    ``not_preemptible`` holds -- running jobs finish naturally while the
    scheduler claim path (which reads the same intent) fences new claims.
    """
    raw = row.prod_pressure_state if isinstance(row.prod_pressure_state, dict) else None
    if not raw or raw.get("state") != "draining":
        return None
    grace_action = str(raw.get("last_grace_action") or "wait")
    if grace_action != "cancel_retryable":
        return {
            "action": "prod_pressure_hold",
            "grace_action": grace_action,
            "cancelled_job_ids": [],
        }
    config = _slurm_config_from_policy(row)
    runner = runner or SubprocessSlurmCommandRunner().bind_config(config)
    jobs = (
        (
            await session.execute(
                select(SlurmWorkerJob)
                .where(
                    SlurmWorkerJob.environment == row.environment,
                    SlurmWorkerJob.pool_name == row.pool_name,
                    SlurmWorkerJob.state.in_(ACTIVE_STATES),
                )
                .with_for_update(),
            )
        )
        .scalars()
        .all()
    )
    cancelled_job_ids: list[str] = []
    worker_ids: set[Any] = set()
    for job in jobs:
        if job.job_id:
            await runner.cancel_job(job.job_id)
            cancelled_job_ids.append(job.job_id)
        job.state = "cancelled"
        job.slurm_state = "CANCELLED"
        job.pending_reason = "cancelled by prod-pressure reclaim"
        job.finished_at = now
        job.updated_at = now
        if job.worker_id is not None:
            worker_ids.add(job.worker_id)
    if worker_ids:
        # Fence the reclaimed workers; scancel kills the job process, so the
        # crash reclaimer requeues any orphaned in-flight trials.
        await session.execute(
            update(Worker)
            .where(Worker.id.in_(worker_ids))
            .values(
                drain_state="drained",
                drain_reason="prod-pressure reclaim",
                drain_owner="prod-pressure-controller",
            ),
        )
    return {
        "action": "prod_pressure_drain",
        "grace_action": grace_action,
        "cancelled_job_ids": cancelled_job_ids,
    }


async def _apply_gb10_host_intent(
    session: AsyncSession,
    row: WorkerPoolAutoscalerPolicy,
    decision: AutoscalerDecision,
    *,
    worker_ids: tuple[str, ...],
    intent: str,
    now: datetime,
) -> None:
    if not worker_ids:
        return
    release_workers = (
        await session.execute(
            select(Worker.id, Worker.hostname).where(Worker.id.in_(worker_ids)),
        )
    ).all()
    worker_id_by_hostname = {str(hostname): worker_id for worker_id, hostname in release_workers}
    hostname_matches = tuple(worker_id_by_hostname)
    desired = (
        await session.execute(
            select(GB10WorkerPoolDesiredState).where(
                GB10WorkerPoolDesiredState.environment == row.environment,
                GB10WorkerPoolDesiredState.pool_name == row.pool_name,
            ),
        )
    ).scalar_one_or_none()
    if desired is None:
        actor_config = dict(row.actuator_config or {})
        desired = GB10WorkerPoolDesiredState(
            environment=row.environment,
            pool_name=row.pool_name,
            image_tag=str(actor_config.get("image_tag") or "dev"),
            max_concurrent=int(actor_config.get("max_concurrent") or 1),
            env_config_version=str(actor_config.get("env_config_version") or "autoscaler"),
            rollout_policy={},
            env={},
            updated_at=now,
        )
        session.add(desired)
    node_match_filters: list[Any] = [
        GB10WorkerNodeStatus.worker_id.in_(worker_ids),
    ]
    if hostname_matches:
        node_match_filters.append(
            GB10WorkerNodeStatus.hostname.in_(hostname_matches),
        )
    nodes = (
        (
            await session.execute(
                select(GB10WorkerNodeStatus).where(
                    GB10WorkerNodeStatus.environment == row.environment,
                    GB10WorkerNodeStatus.pool_name == row.pool_name,
                    or_(*node_match_filters),
                ),
            )
        )
        .scalars()
        .all()
    )
    host_intents = dict(desired.host_intents or {})
    for node in nodes:
        if node.worker_id is None:
            node.worker_id = worker_id_by_hostname.get(node.hostname)
        host_intents[node.hostname] = intent
        node.desired_intent = intent
        node.updated_at = now
    desired.host_intents = host_intents
    desired.target_slots = decision.desired_slots
    desired.updated_at = now


async def _get_or_create_gb10_desired_state(
    session: AsyncSession,
    row: WorkerPoolAutoscalerPolicy,
    *,
    now: datetime,
) -> GB10WorkerPoolDesiredState:
    desired = (
        await session.execute(
            select(GB10WorkerPoolDesiredState).where(
                GB10WorkerPoolDesiredState.environment == row.environment,
                GB10WorkerPoolDesiredState.pool_name == row.pool_name,
            ),
        )
    ).scalar_one_or_none()
    if desired is not None:
        return desired
    actor_config = dict(row.actuator_config or {})
    desired = GB10WorkerPoolDesiredState(
        environment=row.environment,
        pool_name=row.pool_name,
        image_tag=str(actor_config.get("image_tag") or "dev"),
        max_concurrent=int(actor_config.get("max_concurrent") or 1),
        env_config_version=str(actor_config.get("env_config_version") or "autoscaler"),
        target_slots=0,
        host_intents={},
        rollout_policy={},
        env={},
        updated_at=now,
    )
    session.add(desired)
    await session.flush()
    return desired


async def _apply_gb10_scale_up(
    session: AsyncSession,
    row: WorkerPoolAutoscalerPolicy,
    decision: AutoscalerDecision,
    *,
    now: datetime,
) -> None:
    desired = await _get_or_create_gb10_desired_state(session, row, now=now)
    actor_config = dict(row.actuator_config or {})
    configured_hosts = actor_config.get("hosts")
    if isinstance(configured_hosts, list):
        hosts = [str(host) for host in configured_hosts]
    else:
        hosts = list((desired.host_intents or {}).keys())
    if not hosts:
        nodes = (
            await session.execute(
                select(GB10WorkerNodeStatus.hostname)
                .where(
                    GB10WorkerNodeStatus.environment == row.environment,
                    GB10WorkerNodeStatus.pool_name == row.pool_name,
                )
                .order_by(GB10WorkerNodeStatus.hostname),
            )
        ).all()
        hosts = [hostname for (hostname,) in nodes]

    node_rows = (
        (
            await session.execute(
                select(GB10WorkerNodeStatus).where(
                    GB10WorkerNodeStatus.environment == row.environment,
                    GB10WorkerNodeStatus.pool_name == row.pool_name,
                    GB10WorkerNodeStatus.hostname.in_(hosts),
                ),
            )
        )
        .scalars()
        .all()
    )
    current_intent_by_host = {
        node.hostname: node.current_intent
        for node in node_rows
        if node.current_intent in {"active", "draining", "stopped"}
    }

    max_concurrent = int(
        actor_config.get("max_concurrent") or desired.max_concurrent or 1,
    )
    host_intents = dict(desired.host_intents or {})
    for host in hosts:
        host_intents.setdefault(host, current_intent_by_host.get(host, "stopped"))

    active_capacity = sum(max_concurrent for host in hosts if host_intents.get(host) == "active")
    for host in hosts:
        if active_capacity >= decision.desired_slots:
            break
        if host_intents.get(host) == "active":
            continue
        host_intents[host] = "active"
        active_capacity += max_concurrent
    desired.host_intents = host_intents
    desired.target_slots = decision.desired_slots
    desired.updated_at = now
    for intent in ("active", "draining", "stopped"):
        intent_hosts = tuple(host for host in hosts if host_intents.get(host) == intent)
        if not intent_hosts:
            continue
        await session.execute(
            update(GB10WorkerNodeStatus)
            .where(GB10WorkerNodeStatus.environment == row.environment)
            .where(GB10WorkerNodeStatus.pool_name == row.pool_name)
            .where(GB10WorkerNodeStatus.hostname.in_(intent_hosts))
            .values(desired_intent=intent, updated_at=now),
        )


def _exact_autoscaler_environment(environment: str) -> str:
    if (
        not isinstance(environment, str)
        or not environment
        or environment != environment.strip()
        or any(ord(character) < 32 or ord(character) == 127 for character in environment)
    ):
        raise ValueError(
            "worker pool autoscaler environment must be an exact non-empty value "
            "without surrounding whitespace"
        )
    return environment


async def reconcile_worker_pool_autoscaler_once(
    session: AsyncSession,
    *,
    environment: str,
    now: datetime | None = None,
    freshness_sec: int = 120,
    slurm_runner: SlurmWorkerCommandRunner | None = None,
    include_external_policies: bool = False,
    external_only: bool = False,
    pool_names: tuple[str, ...] | None = None,
    capacity_grants: Mapping[tuple[str, str], AutoscalerGrantHandoff] | None = None,
    deployment_generation: int | None = None,
) -> list[AutoscalerDecision]:
    now = now or datetime.now(UTC)
    scoped_environment = _exact_autoscaler_environment(environment)
    stmt = select(WorkerPoolAutoscalerPolicy).where(
        WorkerPoolAutoscalerPolicy.enabled.is_(True),
        WorkerPoolAutoscalerPolicy.environment == scoped_environment,
    )
    if pool_names:
        cleaned_pool_names = tuple(
            dict.fromkeys(name.strip() for name in pool_names if name.strip()),
        )
        if cleaned_pool_names:
            stmt = stmt.where(WorkerPoolAutoscalerPolicy.pool_name.in_(cleaned_pool_names))
    policies = (
        (
            await session.execute(
                stmt.order_by(
                    WorkerPoolAutoscalerPolicy.environment,
                    WorkerPoolAutoscalerPolicy.pool_name,
                ),
            )
        )
        .scalars()
        .all()
    )
    decisions: list[AutoscalerDecision] = []
    for row in policies:
        effective_policy = _policy_to_config(row)
        if capacity_grants is not None and dev_pool_instance_name(row.pool_name) is not None:
            effective_policy = apply_global_dev_capacity_grant(
                effective_policy,
                capacity_grants.get((row.environment, row.pool_name)),
                deployment_generation=deployment_generation,
                now=now,
            )
        uses_external_runner = _policy_uses_external_runner(row)
        if uses_external_runner and not include_external_policies:
            continue
        if external_only and not uses_external_runner:
            continue
        actuator_error: str | None = None
        actuator_blocked_reason: str | None = None
        actuator_blocked_details: dict[str, Any] | None = None
        if row.actuator == "slurm":
            actuator_error = await _refresh_slurm_job_registry(
                session,
                row,
                runner=slurm_runner,
                now=now,
            )
            # Prod-pressure reclaim takes precedence over normal scaling: if the
            # CP handler recorded an active drain intent, reclaim (or hold) this
            # tick and skip the scale-up/down decision entirely.
            prod_pressure_summary = await _apply_slurm_prod_pressure_drain(
                session,
                row,
                runner=slurm_runner,
                now=now,
            )
            if prod_pressure_summary is not None:
                observation = await _load_observation(
                    session,
                    row,
                    now=now,
                    freshness_sec=freshness_sec,
                )
                decision = _base_decision(
                    action=str(prod_pressure_summary["action"]),
                    reason=f"prod_pressure grace={prod_pressure_summary['grace_action']}",
                    policy=effective_policy,
                    observation=observation,
                    desired_slots=0,
                )
                _persist_decision(row, decision, now=now)
                if actuator_error is not None:
                    row.last_error = actuator_error
                decisions.append(decision)
                continue
        observation = await _load_observation(
            session,
            row,
            now=now,
            freshness_sec=freshness_sec,
        )
        decision = compute_autoscaler_decision(
            effective_policy,
            observation,
            now=now,
        )
        if decision.action == "drain_capacity" and row.actuator == "slurm":
            slurm_result = await _apply_slurm_capacity_authority_drain(
                session,
                row,
                runner=slurm_runner,
                now=now,
            )
            actuator_error = slurm_result.error
            actuator_blocked_reason = slurm_result.blocked_reason
            actuator_blocked_details = slurm_result.blocked_details
            if actuator_blocked_reason is not None:
                decision = replace(
                    decision,
                    action="blocked",
                    reason=actuator_blocked_reason,
                    blocked_reason=actuator_blocked_reason,
                    blocked_details=actuator_blocked_details,
                )
        elif decision.action == "request_drain":
            await _request_worker_drain(
                session,
                worker_ids=decision.worker_ids_to_drain,
                now=now,
                reason=decision.reason,
            )
            if row.actuator == "gb10":
                await _apply_gb10_host_intent(
                    session,
                    row,
                    decision,
                    worker_ids=decision.worker_ids_to_drain,
                    intent="draining",
                    now=now,
                )
        elif decision.action == "scale_up" and row.actuator == "slurm":
            slurm_result = await _apply_slurm_scale_up(
                session,
                row,
                decision,
                runner=slurm_runner,
                now=now,
                max_slots=effective_policy.max_slots,
            )
            actuator_error = slurm_result.error
            actuator_blocked_reason = slurm_result.blocked_reason
            actuator_blocked_details = slurm_result.blocked_details
            if actuator_blocked_reason is not None:
                decision = replace(
                    decision,
                    action="blocked",
                    reason=actuator_blocked_reason,
                    blocked_reason=actuator_blocked_reason,
                    blocked_details=actuator_blocked_details,
                )
        elif decision.action == "scale_up" and row.actuator == "gb10":
            await _apply_gb10_scale_up(
                session,
                row,
                decision,
                now=now,
            )
        elif decision.action == "release_drained" and row.actuator == "slurm":
            slurm_result = await _apply_slurm_release_drained(
                session,
                row,
                decision,
                runner=slurm_runner,
                now=now,
                freshness_sec=freshness_sec,
            )
            actuator_error = slurm_result.error
            actuator_blocked_reason = slurm_result.blocked_reason
            actuator_blocked_details = slurm_result.blocked_details
            if actuator_blocked_reason is not None:
                decision = replace(
                    decision,
                    action="blocked",
                    reason=actuator_blocked_reason,
                    blocked_reason=actuator_blocked_reason,
                    blocked_details=actuator_blocked_details,
                )
        elif decision.action == "release_drained" and row.actuator == "gb10":
            await _apply_gb10_host_intent(
                session,
                row,
                decision,
                worker_ids=decision.worker_ids_to_release,
                intent="stopped",
                now=now,
            )
            await session.execute(
                update(Worker)
                .where(Worker.id.in_(decision.worker_ids_to_release))
                .where(Worker.drain_state.in_(("draining", "drained")))
                .values(
                    drain_state="drained",
                    drain_reason="autoscaler release completed",
                    drain_owner="worker-pool-autoscaler",
                ),
            )
        if decision.action == "release_drained" or (
            decision.action in {"scale_up", "drain_capacity"} and row.actuator == "slurm"
        ):
            observation = await _load_observation(
                session,
                row,
                now=now,
                freshness_sec=freshness_sec,
            )
            decision = _decision_with_observation(decision, observation)
        _persist_decision(row, decision, now=now)
        if actuator_error is not None:
            row.last_error = actuator_error
        decisions.append(decision)
    await session.flush()
    return decisions


async def run_worker_pool_autoscaler_loop(
    *,
    session_factory: Any,
    environment: str,
    interval_sec: int = 30,
    freshness_sec: int = 120,
    include_external_policies: bool = False,
    external_only: bool = False,
    pool_names: tuple[str, ...] | None = None,
) -> None:
    while True:
        try:
            async with session_factory() as session:
                await reconcile_worker_pool_autoscaler_once(
                    session,
                    environment=environment,
                    freshness_sec=freshness_sec,
                    include_external_policies=include_external_policies,
                    external_only=external_only,
                    pool_names=pool_names,
                )
                await session.commit()
        except asyncio.CancelledError:
            return
        except Exception as exc:
            logger.warning(
                "worker_pool_autoscaler_loop_error",
                extra={"err": str(exc)},
            )
        await asyncio.sleep(interval_sec)
