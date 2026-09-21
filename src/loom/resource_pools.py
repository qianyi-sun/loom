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

from loom.db.schema import Trial, Worker


@dataclass(frozen=True)
class ResourcePoolKey:
    pool_name: str
    backend: str
    cpu_arch: str


class ResourcePoolSnapshot(TypedDict):
    pool_name: str
    backend: str
    cpu_arch: str
    current_active_slots: int
    active_workers: int
    draining_workers: int
    total_slots: int
    draining_slots: int
    occupied_slots: int
    free_slots: int
    running_tasks: int
    starting_tasks: int
    pre_start_heartbeat_fresh_tasks: int
    oldest_starting_task_age_sec: int | None
    queued_tasks: int


class ResourcePoolAggregate(TypedDict):
    current_active_slots: int
    active_workers: int
    draining_workers: int
    total_slots: int
    draining_slots: int
    occupied_slots: int
    free_slots: int
    running_tasks: int
    starting_tasks: int
    pre_start_heartbeat_fresh_tasks: int
    oldest_starting_task_age_sec: int | None
    queued_tasks: int


class ResourcePoolSummary(TypedDict):
    aggregate: ResourcePoolAggregate
    pools: list[ResourcePoolSnapshot]


@dataclass
class _MutablePool:
    key: ResourcePoolKey
    active_workers: int = 0
    draining_workers: int = 0
    total_slots: int = 0
    draining_slots: int = 0
    occupied_slots: int = 0
    claimable_occupied_slots: int = 0
    running_tasks: int = 0
    starting_tasks: int = 0
    pre_start_heartbeat_fresh_tasks: int = 0
    oldest_starting_task_age_sec: int | None = None
    queued_tasks: int = 0

    @property
    def current_active_slots(self) -> int:
        return self.total_slots

    @property
    def free_slots(self) -> int:
        return max(0, self.current_active_slots - self.claimable_occupied_slots)

    def as_dict(self) -> ResourcePoolSnapshot:
        return {
            "pool_name": self.key.pool_name,
            "backend": self.key.backend,
            "cpu_arch": self.key.cpu_arch,
            "current_active_slots": self.current_active_slots,
            "active_workers": self.active_workers,
            "draining_workers": self.draining_workers,
            "total_slots": self.total_slots,
            "draining_slots": self.draining_slots,
            "occupied_slots": self.occupied_slots,
            "free_slots": self.free_slots,
            "running_tasks": self.running_tasks,
            "starting_tasks": self.starting_tasks,
            "pre_start_heartbeat_fresh_tasks": self.pre_start_heartbeat_fresh_tasks,
            "oldest_starting_task_age_sec": self.oldest_starting_task_age_sec,
            "queued_tasks": self.queued_tasks,
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
    current_active_slots = sum(pool["current_active_slots"] for pool in pools)
    active_workers = sum(pool["active_workers"] for pool in pools)
    draining_workers = sum(pool["draining_workers"] for pool in pools)
    total_slots = sum(pool["total_slots"] for pool in pools)
    draining_slots = sum(pool["draining_slots"] for pool in pools)
    occupied_slots = sum(pool["occupied_slots"] for pool in pools)
    oldest_starting_task_age_sec = max(
        (
            age
            for age in (pool["oldest_starting_task_age_sec"] for pool in pools)
            if age is not None
        ),
        default=None,
    )
    return {
        "current_active_slots": current_active_slots,
        "active_workers": active_workers,
        "draining_workers": draining_workers,
        "total_slots": total_slots,
        "draining_slots": draining_slots,
        "occupied_slots": occupied_slots,
        "free_slots": sum(pool["free_slots"] for pool in pools),
        "running_tasks": sum(pool["running_tasks"] for pool in pools),
        "starting_tasks": sum(pool["starting_tasks"] for pool in pools),
        "pre_start_heartbeat_fresh_tasks": sum(
            pool["pre_start_heartbeat_fresh_tasks"] for pool in pools
        ),
        "oldest_starting_task_age_sec": oldest_starting_task_age_sec,
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
    state counters. When omitted, global queued/protected-pending/claimed/running
    trials are used, which is the correct shape for metrics refreshers and CLI
    default output.
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
        drain_state_name = str(drain_state or "active")
        is_claimable = drain_state_name == "active"
        worker_is_claimable[worker_id] = is_claimable
        pool = pools_by_key.setdefault(key, _MutablePool(key=key))
        slots = max(1, int(max_concurrent or 1))
        if is_claimable:
            pool.active_workers += 1
            pool.total_slots += slots
        elif drain_state_name == "draining":
            pool.draining_workers += 1
            pool.draining_slots += slots

    if trial_stmt is None:
        trial_stmt = select(
            Trial.state,
            Trial.worker_id,
            Trial.requires_caps,
            Trial.claimed_at,
            Trial.pre_start_heartbeat_at,
        ).where(
            Trial.state.in_(("queued", "protected-pending", "claimed", "running"))
        )

    trial_rows = (await session.execute(trial_stmt)).all()
    queued_tasks = 0
    for state, worker_id, requires_caps, claimed_at, pre_start_heartbeat_at in trial_rows:
        if state in {"queued", "protected-pending"}:
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
            if claimed_at is not None:
                age_sec = max(0, int((now - claimed_at).total_seconds()))
                if (
                    pool.oldest_starting_task_age_sec is None
                    or age_sec > pool.oldest_starting_task_age_sec
                ):
                    pool.oldest_starting_task_age_sec = age_sec
            if (
                pre_start_heartbeat_at is not None
                and pre_start_heartbeat_at >= cutoff
            ):
                pool.pre_start_heartbeat_fresh_tasks += 1
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
