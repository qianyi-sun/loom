"""Worker resource-pool slot summaries.

The UI and CLI should reason about execution slots, not worker-process count.
This module keeps that accounting in one place so Monitor, CLI-facing API, and
Prometheus refreshers do not drift.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, TypedDict
from uuid import UUID

from sqlalchemy import Select, select
from sqlalchemy.ext.asyncio import AsyncSession

from loom.db.schema import Trial, Worker, WorkerPoolAutoscalerPolicy


@dataclass(frozen=True)
class ResourcePoolKey:
    pool_name: str
    backend: str
    cpu_arch: str


class ResourcePoolSnapshot(TypedDict):
    pool_name: str
    backend: str
    cpu_arch: str
    autoscaler_environment: str | None
    autoscaler_actuator: str | None
    autoscaler_enabled: bool
    autoscaler_idle_since_at: str | None
    autoscaler_idle_seconds: int | None
    desired_slots: int
    pending_slots: int
    active_workers: int
    draining_workers: int
    total_slots: int
    draining_slots: int
    occupied_slots: int
    free_slots: int
    running_tasks: int
    starting_tasks: int
    queued_tasks: int
    last_autoscaler_decision: str | None
    last_autoscaler_reason: str | None
    last_autoscaler_blocked_reason: str | None
    last_autoscaler_error: str | None


class ResourcePoolAggregate(TypedDict):
    desired_slots: int
    pending_slots: int
    active_workers: int
    draining_workers: int
    total_slots: int
    draining_slots: int
    occupied_slots: int
    free_slots: int
    running_tasks: int
    starting_tasks: int
    queued_tasks: int


class ResourcePoolSummary(TypedDict):
    aggregate: ResourcePoolAggregate
    pools: list[ResourcePoolSnapshot]


@dataclass
class _MutablePool:
    key: ResourcePoolKey
    autoscaler_environment: str | None = None
    autoscaler_actuator: str | None = None
    autoscaler_enabled: bool = False
    autoscaler_idle_since_at: str | None = None
    autoscaler_idle_seconds: int | None = None
    desired_slots: int = 0
    pending_slots: int = 0
    active_workers: int = 0
    draining_workers: int = 0
    total_slots: int = 0
    draining_slots: int = 0
    occupied_slots: int = 0
    claimable_occupied_slots: int = 0
    running_tasks: int = 0
    starting_tasks: int = 0
    queued_tasks: int = 0
    last_autoscaler_decision: str | None = None
    last_autoscaler_reason: str | None = None
    last_autoscaler_blocked_reason: str | None = None
    last_autoscaler_error: str | None = None

    @property
    def free_slots(self) -> int:
        return max(0, self.total_slots - self.claimable_occupied_slots)

    def as_dict(self) -> ResourcePoolSnapshot:
        return {
            "pool_name": self.key.pool_name,
            "backend": self.key.backend,
            "cpu_arch": self.key.cpu_arch,
            "autoscaler_environment": self.autoscaler_environment,
            "autoscaler_actuator": self.autoscaler_actuator,
            "autoscaler_enabled": self.autoscaler_enabled,
            "autoscaler_idle_since_at": self.autoscaler_idle_since_at,
            "autoscaler_idle_seconds": self.autoscaler_idle_seconds,
            "desired_slots": self.desired_slots,
            "pending_slots": self.pending_slots,
            "active_workers": self.active_workers,
            "draining_workers": self.draining_workers,
            "total_slots": self.total_slots,
            "draining_slots": self.draining_slots,
            "occupied_slots": self.occupied_slots,
            "free_slots": self.free_slots,
            "running_tasks": self.running_tasks,
            "starting_tasks": self.starting_tasks,
            "queued_tasks": self.queued_tasks,
            "last_autoscaler_decision": self.last_autoscaler_decision,
            "last_autoscaler_reason": self.last_autoscaler_reason,
            "last_autoscaler_blocked_reason": self.last_autoscaler_blocked_reason,
            "last_autoscaler_error": self.last_autoscaler_error,
        }


def _primary_capability(capabilities: object) -> dict[str, Any]:
    if isinstance(capabilities, list):
        for cap in capabilities:
            if isinstance(cap, dict):
                return cap
    return {}


def _pool_key(
    *,
    pool_name: str,
    capabilities: object,
) -> ResourcePoolKey:
    cap = _primary_capability(capabilities)
    backend = cap.get("backend", "docker")
    cpu_arch = cap.get("cpu_arch", "x86_64")
    return ResourcePoolKey(
        pool_name=pool_name.strip() or "default",
        backend=backend if isinstance(backend, str) else "docker",
        cpu_arch=cpu_arch if isinstance(cpu_arch, str) else "x86_64",
    )


def _policy_pool_key(row: WorkerPoolAutoscalerPolicy) -> ResourcePoolKey:
    actor_config = row.actuator_config or {}
    backend = actor_config.get("backend", "docker")
    default_arch = "arm64" if row.actuator == "gb10" else "x86_64"
    cpu_arch = actor_config.get("cpu_arch", default_arch)
    return ResourcePoolKey(
        pool_name=row.pool_name.strip() or "default",
        backend=backend if isinstance(backend, str) else "docker",
        cpu_arch=cpu_arch if isinstance(cpu_arch, str) else default_arch,
    )


def _trial_matches_pool(requires_caps: object, key: ResourcePoolKey) -> bool:
    if not isinstance(requires_caps, dict) or not requires_caps:
        return True
    backend = requires_caps.get("backend")
    if isinstance(backend, str) and backend != key.backend:
        return False
    cpu_arch = requires_caps.get("cpu_arch")
    if isinstance(cpu_arch, str) and cpu_arch not in {key.cpu_arch, "any"}:
        return False
    return True


def _aggregate(
    pools: list[ResourcePoolSnapshot],
    *,
    queued_tasks: int,
) -> ResourcePoolAggregate:
    desired_slots = sum(pool["desired_slots"] for pool in pools)
    pending_slots = sum(pool["pending_slots"] for pool in pools)
    active_workers = sum(pool["active_workers"] for pool in pools)
    draining_workers = sum(pool["draining_workers"] for pool in pools)
    total_slots = sum(pool["total_slots"] for pool in pools)
    draining_slots = sum(pool["draining_slots"] for pool in pools)
    occupied_slots = sum(pool["occupied_slots"] for pool in pools)
    return {
        "desired_slots": desired_slots,
        "pending_slots": pending_slots,
        "active_workers": active_workers,
        "draining_workers": draining_workers,
        "total_slots": total_slots,
        "draining_slots": draining_slots,
        "occupied_slots": occupied_slots,
        "free_slots": sum(pool["free_slots"] for pool in pools),
        "running_tasks": sum(pool["running_tasks"] for pool in pools),
        "starting_tasks": sum(pool["starting_tasks"] for pool in pools),
        "queued_tasks": queued_tasks,
    }


async def get_resource_pool_summary(
    session: AsyncSession,
    *,
    freshness_sec: int,
    trial_stmt: Select[Any] | None = None,
) -> ResourcePoolSummary:
    """Return aggregate and per-pool slot state.

    `trial_stmt` lets Monitor apply the same URL-scope filters it uses for
    state counters. When omitted, global queued/claimed/running trials are used,
    which is the correct shape for metrics refreshers and CLI default output.
    """
    now = datetime.now(UTC)
    cutoff = now - timedelta(seconds=freshness_sec)
    worker_rows = (await session.execute(
        select(
            Worker.id,
            Worker.capabilities,
            Worker.max_concurrent,
            Worker.pool_name,
            Worker.drain_state,
        ).where(
            Worker.status == "active",
            Worker.last_seen_at >= cutoff,
        ),
    )).all()

    pools_by_key: dict[ResourcePoolKey, _MutablePool] = {}
    worker_to_key: dict[UUID, ResourcePoolKey] = {}
    worker_is_claimable: dict[UUID, bool] = {}
    for worker_id, capabilities, max_concurrent, pool_name, drain_state in worker_rows:
        key = _pool_key(
            pool_name=str(pool_name or "default"),
            capabilities=capabilities,
        )
        worker_to_key[worker_id] = key
        is_claimable = str(drain_state or "active") == "active"
        worker_is_claimable[worker_id] = is_claimable
        pool = pools_by_key.setdefault(key, _MutablePool(key=key))
        slots = max(1, int(max_concurrent or 1))
        if is_claimable:
            pool.active_workers += 1
            pool.total_slots += slots
        else:
            pool.draining_workers += 1
            pool.draining_slots += slots

    policy_rows = (await session.execute(
        select(WorkerPoolAutoscalerPolicy).order_by(
            WorkerPoolAutoscalerPolicy.environment,
            WorkerPoolAutoscalerPolicy.pool_name,
        ),
    )).scalars().all()
    for policy in policy_rows:
        key = _policy_pool_key(policy)
        pool = pools_by_key.setdefault(key, _MutablePool(key=key))
        pool.autoscaler_environment = policy.environment
        pool.autoscaler_actuator = policy.actuator
        pool.autoscaler_enabled = bool(policy.enabled)
        if policy.idle_since_at is None:
            pool.autoscaler_idle_since_at = None
            pool.autoscaler_idle_seconds = None
        else:
            pool.autoscaler_idle_since_at = policy.idle_since_at.isoformat()
            pool.autoscaler_idle_seconds = max(
                0,
                int((now - policy.idle_since_at).total_seconds()),
            )
        pool.desired_slots = int(policy.last_desired_slots or policy.min_slots or 0)
        pool.pending_slots = int(policy.last_pending_slots or 0)
        if policy.last_draining_slots is not None:
            pool.draining_slots = max(pool.draining_slots, int(policy.last_draining_slots))
        pool.last_autoscaler_decision = policy.last_decision
        pool.last_autoscaler_reason = policy.last_decision_reason
        pool.last_autoscaler_blocked_reason = policy.last_blocked_reason
        pool.last_autoscaler_error = policy.last_error

    if trial_stmt is None:
        trial_stmt = select(
            Trial.state,
            Trial.worker_id,
            Trial.requires_caps,
        ).where(Trial.state.in_(("queued", "claimed", "running")))

    trial_rows = (await session.execute(trial_stmt)).all()
    queued_tasks = 0
    for state, worker_id, requires_caps in trial_rows:
        if state == "queued":
            queued_tasks += 1
            for key, pool in pools_by_key.items():
                if _trial_matches_pool(requires_caps, key):
                    pool.queued_tasks += 1
            continue

        if state not in {"claimed", "running"} or worker_id is None:
            continue
        worker_key = worker_to_key.get(worker_id)
        if worker_key is None:
            continue
        pool = pools_by_key[worker_key]
        pool.occupied_slots += 1
        if worker_is_claimable.get(worker_id, False):
            pool.claimable_occupied_slots += 1
        if state == "claimed":
            pool.starting_tasks += 1
        elif state == "running":
            pool.running_tasks += 1

    pools = [
        pool.as_dict()
        for pool in sorted(
            pools_by_key.values(),
            key=lambda p: (p.key.pool_name, p.key.backend, p.key.cpu_arch),
        )
    ]
    return {
        "aggregate": _aggregate(pools, queued_tasks=queued_tasks),
        "pools": pools,
    }
