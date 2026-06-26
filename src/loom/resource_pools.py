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
    active_workers: int
    total_slots: int
    occupied_slots: int
    free_slots: int
    running_tasks: int
    starting_tasks: int
    queued_tasks: int


class ResourcePoolAggregate(TypedDict):
    active_workers: int
    total_slots: int
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
    active_workers: int = 0
    total_slots: int = 0
    occupied_slots: int = 0
    running_tasks: int = 0
    starting_tasks: int = 0
    queued_tasks: int = 0

    @property
    def free_slots(self) -> int:
        return max(0, self.total_slots - self.occupied_slots)

    def as_dict(self) -> ResourcePoolSnapshot:
        return {
            "pool_name": self.key.pool_name,
            "backend": self.key.backend,
            "cpu_arch": self.key.cpu_arch,
            "active_workers": self.active_workers,
            "total_slots": self.total_slots,
            "occupied_slots": self.occupied_slots,
            "free_slots": self.free_slots,
            "running_tasks": self.running_tasks,
            "starting_tasks": self.starting_tasks,
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
    active_workers = sum(pool["active_workers"] for pool in pools)
    total_slots = sum(pool["total_slots"] for pool in pools)
    occupied_slots = sum(pool["occupied_slots"] for pool in pools)
    return {
        "active_workers": active_workers,
        "total_slots": total_slots,
        "occupied_slots": occupied_slots,
        "free_slots": max(0, total_slots - occupied_slots),
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
    cutoff = datetime.now(UTC) - timedelta(seconds=freshness_sec)
    worker_rows = (await session.execute(
        select(
            Worker.id,
            Worker.capabilities,
            Worker.max_concurrent,
            Worker.pool_name,
        ).where(
            Worker.status == "active",
            Worker.last_seen_at >= cutoff,
        ),
    )).all()

    pools_by_key: dict[ResourcePoolKey, _MutablePool] = {}
    worker_to_key: dict[UUID, ResourcePoolKey] = {}
    for worker_id, capabilities, max_concurrent, pool_name in worker_rows:
        key = _pool_key(
            pool_name=str(pool_name or "default"),
            capabilities=capabilities,
        )
        worker_to_key[worker_id] = key
        pool = pools_by_key.setdefault(key, _MutablePool(key=key))
        pool.active_workers += 1
        pool.total_slots += max(1, int(max_concurrent or 1))

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
