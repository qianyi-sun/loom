"""Shared lookup: which backends do currently-active workers serve?

Used by:
- `GET /api/v1/backends` — render the catalog with `available=true|false`.
- `POST /api/v1/batches` — admit against a fresh worker or separately
  identified compatible autoscaler cold-start authority.

A backend is "active" iff at least one row in `workers` satisfies BOTH:
1. `status = 'active'` — set on register, flipped to 'shutting-down' on
   SIGTERM via the worker's own teardown path.
2. `last_seen_at >= now() - 30 seconds` — heartbeat freshness check.
   The worker beats every 5s; this freshness window is intentionally shorter
   than CP's crash-detector reclaim expiry. Without this predicate, a worker that
   crashes without SIGTERM keeps `status='active'` forever, defeating
   PR #63's reject-when-no-worker check (issue #68).

Workers registered before Plan 28 PR-3 omit the `backend` key in each
capability dict — those rows fall back to "docker" since that was the
only backend the worker pool shipped before that PR.
"""

from __future__ import annotations

import os
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from loom.db.schema import (
    ServiceExecutionClass,
    ServiceExecutionTarget,
    Worker,
    WorkerPoolAutoscalerPolicy,
)
from loom.models.task import TaskConfig
from loom.service_execution_backend import NEBIUS_BACKEND, NEBIUS_LOGICAL_POOL_ID
from loom_control_plane.scheduler.requires_caps import derive_requires_caps

# Freshness window — 30s = 6 heartbeat intervals. Generous enough to
# ride out network blips without keeping a dead worker visible to users.
# This must remain shorter than the CP crash-detector expiry so new
# submissions stop using a stale worker before in-flight trials are reclaimed.
# Bump if heartbeat interval changes (`loom_worker.config.heartbeat_interval_sec`).
_HEARTBEAT_FRESHNESS_SEC = 30

# A pool supervisor normally reconciles every 30 seconds.  Four missed ticks
# are enough to stop treating its configured maximum as cold-start authority.
# This is deliberately separate from worker heartbeat freshness: a configured
# maximum is planning headroom, never proof of currently executable capacity.
_AUTOSCALER_POLICY_FRESHNESS_SEC = 120

_LEGACY_DOCKER_NETWORK_POLICIES = frozenset(
    {"public", "no-network", "allowlist"},
)
_EFFECTIVE_ZERO_REASON_PREFIXES = (
    "global_execution_fence_",
    "global_dev_capacity_grant_",
    "pipeline_policy_activation_",
)


@dataclass(frozen=True)
class ColdStartPool:
    """One fresh autoscaler policy that may create legacy worker capacity."""

    pool_name: str
    backend: str
    cpu_arch: str


@dataclass(frozen=True)
class ServiceExecutionBackendPool:
    """One fresh target that can accept explicitly selected service execution."""

    pool_name: str
    backend: str
    execution_class_id: str


def runtime_environment() -> str:
    """Return the exact DB autoscaler environment for this service process."""
    value = os.environ.get("LOOM_ENV", "development").strip().lower()
    return value or "development"


def _policy_backend(policy: WorkerPoolAutoscalerPolicy) -> str:
    config = policy.actuator_config or {}
    value = config.get("backend") if isinstance(config, Mapping) else None
    return value.strip() if isinstance(value, str) and value.strip() else "docker"


def _policy_cpu_arch(policy: WorkerPoolAutoscalerPolicy) -> str:
    config = policy.actuator_config or {}
    value = config.get("cpu_arch") if isinstance(config, Mapping) else None
    if isinstance(value, str) and value.strip():
        return value.strip()
    return "arm64" if policy.actuator == "gb10" else "x86_64"


def _policy_is_cold_start_healthy(
    policy: WorkerPoolAutoscalerPolicy,
    *,
    now: datetime,
) -> bool:
    observed_at = policy.last_decision_at
    if observed_at is None:
        return False
    if observed_at.tzinfo is None:
        observed_at = observed_at.replace(tzinfo=UTC)
    if observed_at + timedelta(seconds=_AUTOSCALER_POLICY_FRESHNESS_SEC) <= now:
        return False
    if not policy.enabled or policy.max_slots <= 0 or policy.disabled_reason:
        return False
    if policy.last_blocked_reason or policy.last_error:
        return False
    if (policy.prod_pressure_state or {}).get("state") == "draining":
        return False
    reason = policy.last_decision_reason or ""
    return not reason.startswith(_EFFECTIVE_ZERO_REASON_PREFIXES)


async def get_cold_start_pools(
    session: AsyncSession,
    *,
    environment: str | None = None,
    now: datetime | None = None,
) -> tuple[ColdStartPool, ...]:
    """Return fresh, healthy pool policies without upgrading them to workers.

    These rows authorize batch persistence so the autoscaler can observe queued
    demand.  They do not make ``GET /backends.available`` true and do not count
    as executable slots.
    """
    scoped_environment = environment or runtime_environment()
    observed_now = now or datetime.now(UTC)
    policies = (
        (
            await session.execute(
                select(WorkerPoolAutoscalerPolicy)
                .where(WorkerPoolAutoscalerPolicy.environment == scoped_environment)
                .order_by(WorkerPoolAutoscalerPolicy.pool_name),
            )
        )
        .scalars()
        .all()
    )
    return tuple(
        ColdStartPool(
            pool_name=policy.pool_name,
            backend=_policy_backend(policy),
            cpu_arch=_policy_cpu_arch(policy),
        )
        for policy in policies
        if _policy_is_cold_start_healthy(policy, now=observed_now)
    )


async def get_service_execution_backend_pools(
    session: AsyncSession,
    *,
    environment: str | None = None,
    now: datetime | None = None,
) -> tuple[ServiceExecutionBackendPool, ...]:
    """Return fresh Nebius targets without claiming that a node is already live.

    The user-facing ``nebius`` backend maps only to the durable
    ``nebius-cpu`` service-execution pool. A healthy target is cold-start
    authority; quota and provisioning admission still run transactionally
    before the actuator creates a Job.
    """

    scoped_environment = environment or runtime_environment()
    observed_now = now or datetime.now(UTC)
    targets = (
        (
            await session.execute(
                select(ServiceExecutionTarget)
                .join(
                    ServiceExecutionClass,
                    ServiceExecutionClass.id == ServiceExecutionTarget.execution_class_id,
                )
                .where(
                    ServiceExecutionTarget.environment == scoped_environment,
                    ServiceExecutionTarget.provider == NEBIUS_BACKEND,
                    ServiceExecutionTarget.logical_pool_id == NEBIUS_LOGICAL_POOL_ID,
                    ServiceExecutionTarget.desired_state == "active",
                    ServiceExecutionTarget.observed_state == "ready",
                    ServiceExecutionTarget.health_status == "healthy",
                    ServiceExecutionClass.enabled.is_(True),
                )
                .order_by(ServiceExecutionTarget.region, ServiceExecutionTarget.id)
            )
        )
        .scalars()
        .all()
    )
    pools: dict[str, ServiceExecutionBackendPool] = {}
    for target in targets:
        observed_at = target.health_observed_at
        if observed_at is None:
            continue
        if observed_at.tzinfo is None:
            observed_at = observed_at.replace(tzinfo=UTC)
        stale_after = target.spec_json.get("health_stale_after_seconds")
        if not isinstance(stale_after, int) or stale_after <= 0:
            continue
        if observed_at + timedelta(seconds=stale_after) <= observed_now:
            continue
        pools.setdefault(
            target.logical_pool_id,
            ServiceExecutionBackendPool(
                pool_name=target.logical_pool_id,
                backend=NEBIUS_BACKEND,
                execution_class_id=target.execution_class_id,
            ),
        )
    return tuple(pools.values())


def compatible_cold_start_pool_names(
    pools: Sequence[ColdStartPool],
    *,
    backend: str,
    task_configs: Sequence[TaskConfig],
) -> tuple[str, ...]:
    """Return pools that collectively cover every selected legacy task."""
    eligible = tuple(pool for pool in pools if pool.backend == backend)
    if not eligible or not task_configs:
        return ()

    matched: set[str] = set()
    for task_config in task_configs:
        required = derive_requires_caps(task_config)
        task_matches = tuple(
            pool
            for pool in eligible
            if required.os == "linux"
            and required.gpu_vendor == "none"
            and required.network_policies <= _LEGACY_DOCKER_NETWORK_POLICIES
            and required.cpu_arch in {"any", pool.cpu_arch}
        )
        if not task_matches:
            return ()
        matched.update(pool.pool_name for pool in task_matches)
    return tuple(sorted(matched))


def parse_backends_from_capabilities(
    rows: list[object],
) -> set[str]:
    """Pure helper: collapse a list of `workers.capabilities` JSONB
    rows into the set of advertised backend names.

    Each row is the raw JSONB list one worker stored (typically
    `[{"backend": "docker"}, {"backend": "fake"}]`). Rows of the wrong
    shape are skipped silently — corrupt rows shouldn't keep the
    catalog from rendering.
    """
    out: set[str] = set()
    for caps_list in rows:
        if not isinstance(caps_list, list):
            continue
        for cap in caps_list:
            if not isinstance(cap, dict):
                continue
            # Pre-Plan-28-PR-3 workers omit the backend key — they only
            # served docker, so that's the safe default.
            backend_name = cap.get("backend", "docker")
            if isinstance(backend_name, str):
                out.add(backend_name)
    return out


async def get_active_backends(session: AsyncSession) -> set[str]:
    """Return the set of backend names served by at least one active
    worker. Empty set means no active workers (or none advertising any
    backend), which the batch route translates to a 400.

    "Active" = `status='active'` AND heartbeat within the last
    `_HEARTBEAT_FRESHNESS_SEC` seconds. The status-only predicate is
    insufficient because workers that crash without SIGTERM leave the
    row at 'active' (issue #68); using `last_seen_at` ensures stale
    workers stop counting toward the catalog within ~6 heartbeats.
    """
    cutoff = datetime.now(UTC) - timedelta(seconds=_HEARTBEAT_FRESHNESS_SEC)
    rows = (await session.execute(
        select(Worker.capabilities).where(
            Worker.status == "active",
            Worker.last_seen_at >= cutoff,
        ),
    )).scalars().all()
    return parse_backends_from_capabilities(list(rows))


async def get_active_worker_count(session: AsyncSession) -> int:
    """Return the count of currently active, fresh-heartbeat workers."""
    cutoff = datetime.now(UTC) - timedelta(seconds=_HEARTBEAT_FRESHNESS_SEC)
    return int((await session.execute(
        select(func.count()).select_from(Worker).where(
            Worker.status == "active",
            Worker.last_seen_at >= cutoff,
        ),
    )).scalar_one())
