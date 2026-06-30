"""Shared worker-pool autoscaler policy and decision helpers."""

from __future__ import annotations

import asyncio
import logging
import math
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
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
from loom_control_plane.elastic_slurm_worker_controller import (
    ElasticSlurmWorkerControllerConfig,
    SlurmWorkerCapacitySnapshot,
    SlurmWorkerCommandRunner,
    SubprocessSlurmCommandRunner,
    build_controller_config,
    compute_controller_decision,
    slurm_submission_config_for_node,
)
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


@dataclass(frozen=True)
class AutoscalerObservation:
    active_slots: int
    pending_slots: int
    draining_slots: int
    occupied_slots: int
    queued_slots: int
    idle_worker_ids: tuple[str, ...]
    drained_worker_ids: tuple[str, ...]

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
    error_message: str | None = None


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
    cpu_arch = requires_caps.get("cpu_arch")
    if isinstance(cpu_arch, str) and cpu_arch not in {policy_arch, "any"}:
        return False
    return True


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

    active_slots = 0
    draining_slots = 0
    active_idle: list[tuple[str, int]] = []
    drained_worker_ids: list[str] = []
    for worker in workers:
        slots = max(1, int(worker.max_concurrent or 1))
        in_flight = in_flight_by_worker.get(worker.id, 0)
        if worker.drain_state == "active":
            active_slots += slots
            if in_flight == 0:
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

    pending_slots = int(row.last_pending_slots or 0)
    if row.actuator == "slurm":
        pending_slots = sum(
            int(slots or 0)
            for (slots,) in (
                await session.execute(
                    select(SlurmWorkerJob.requested_concurrency).where(
                        SlurmWorkerJob.environment == row.environment,
                        SlurmWorkerJob.pool_name == row.pool_name,
                        SlurmWorkerJob.state == "pending",
                    ),
                )
            ).all()
        )

    return AutoscalerObservation(
        active_slots=active_slots,
        pending_slots=pending_slots,
        draining_slots=draining_slots,
        occupied_slots=sum(in_flight_by_worker.values()),
        queued_slots=queued_slots,
        idle_worker_ids=tuple(selected_idle),
        drained_worker_ids=tuple(drained_worker_ids),
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
        exclusive=_optional_bool(actor_config.get("exclusive"), default=True),
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
    )
    if config is None:
        raise ValueError("Slurm autoscaler policy unexpectedly disabled")
    return config


def _worker_env_from_slurm_config(
    config: ElasticSlurmWorkerControllerConfig,
) -> dict[str, str]:
    return {
        "LOOM_WORKER_MAX_CONCURRENT": str(config.requested_concurrency),
        "LOOM_WORKER_POOL_NAME": config.pool_name,
        "LOOM_REMOTE_WORKER_ENV_FILE": config.env_file,
        "LOOM_REMOTE_WORKER_REPO_DIR": config.repo_dir,
    }


async def _apply_slurm_scale_up(
    session: AsyncSession,
    row: WorkerPoolAutoscalerPolicy,
    decision: AutoscalerDecision,
    *,
    runner: SlurmWorkerCommandRunner | None,
    now: datetime,
) -> str | None:
    config = _slurm_config_from_policy(row)
    runner = runner or SubprocessSlurmCommandRunner().bind_config(config)
    active_jobs = (
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
            return str(exc)

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
    actuator_error: str | None = None
    for node in slurm_decision.submit_nodes:
        node_config = slurm_submission_config_for_node(
            config,
            slurm_decision,
            node=node,
        )
        try:
            job_id = await runner.submit_worker(node=node, config=node_config)
            await record_slurm_worker_job(
                session,
                environment=row.environment,
                pool_name=row.pool_name,
                nodelist=node,
                requested_cpus=node_config.requested_cpus,
                requested_memory_mib=node_config.requested_memory_mib,
                requested_concurrency=node_config.requested_concurrency,
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
                requested_concurrency=node_config.requested_concurrency,
                job_id=None,
                slurm_state="FAILED",
                pending_reason=None,
                env=_worker_env_from_slurm_config(node_config),
                submitted_at=now,
                submission_error=str(exc),
            )
    return actuator_error


async def _apply_slurm_release_drained(
    session: AsyncSession,
    row: WorkerPoolAutoscalerPolicy,
    decision: AutoscalerDecision,
    *,
    runner: SlurmWorkerCommandRunner | None,
    now: datetime,
) -> None:
    if not decision.worker_ids_to_release:
        return
    config = _slurm_config_from_policy(row)
    runner = runner or SubprocessSlurmCommandRunner().bind_config(config)
    released_worker_ids = set(decision.worker_ids_to_release)
    release_workers = (
        await session.execute(
            select(Worker.id, Worker.hostname).where(Worker.id.in_(released_worker_ids)),
        )
    ).all()
    worker_id_by_hostname = {str(hostname): worker_id for worker_id, hostname in release_workers}
    hostname_matches = tuple(worker_id_by_hostname)
    job_match_filters: list[Any] = [
        SlurmWorkerJob.worker_id.in_(released_worker_ids),
    ]
    if hostname_matches:
        job_match_filters.append(
            SlurmWorkerJob.worker_id.is_(None) & SlurmWorkerJob.nodelist.in_(hostname_matches)
        )
    jobs = (
        (
            await session.execute(
                select(SlurmWorkerJob).where(
                    SlurmWorkerJob.environment == row.environment,
                    SlurmWorkerJob.pool_name == row.pool_name,
                    SlurmWorkerJob.state == "running",
                    or_(*job_match_filters),
                ),
            )
        )
        .scalars()
        .all()
    )
    for job in jobs:
        if job.worker_id is None:
            job.worker_id = worker_id_by_hostname.get(job.nodelist)
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


async def reconcile_worker_pool_autoscaler_once(
    session: AsyncSession,
    *,
    now: datetime | None = None,
    freshness_sec: int = 120,
    slurm_runner: SlurmWorkerCommandRunner | None = None,
    include_external_policies: bool = False,
    external_only: bool = False,
    pool_names: tuple[str, ...] | None = None,
) -> list[AutoscalerDecision]:
    now = now or datetime.now(UTC)
    stmt = select(WorkerPoolAutoscalerPolicy).where(
        WorkerPoolAutoscalerPolicy.enabled.is_(True),
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
        uses_external_runner = _policy_uses_external_runner(row)
        if uses_external_runner and not include_external_policies:
            continue
        if external_only and not uses_external_runner:
            continue
        actuator_error: str | None = None
        if row.actuator == "slurm":
            actuator_error = await _refresh_slurm_job_registry(
                session,
                row,
                runner=slurm_runner,
                now=now,
            )
        observation = await _load_observation(
            session,
            row,
            now=now,
            freshness_sec=freshness_sec,
        )
        decision = compute_autoscaler_decision(
            _policy_to_config(row),
            observation,
            now=now,
        )
        if decision.action == "request_drain":
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
            actuator_error = await _apply_slurm_scale_up(
                session,
                row,
                decision,
                runner=slurm_runner,
                now=now,
            )
        elif decision.action == "scale_up" and row.actuator == "gb10":
            await _apply_gb10_scale_up(
                session,
                row,
                decision,
                now=now,
            )
        elif decision.action == "release_drained" and row.actuator == "slurm":
            await _apply_slurm_release_drained(
                session,
                row,
                decision,
                runner=slurm_runner,
                now=now,
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
            decision.action == "scale_up" and row.actuator == "slurm"
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
