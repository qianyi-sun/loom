"""Fresh local workers and native hosted execution targets."""
from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from loom.db.schema import (
    ServiceExecutionClass,
    ServiceExecutionTarget,
    Worker,
)
from loom.service_execution_backend import NEBIUS_BACKEND, NEBIUS_LOGICAL_POOL_ID

# Freshness window — 30s = 6 heartbeat intervals. Generous enough to
# ride out network blips without keeping a dead worker visible to users.
# This must remain shorter than the CP crash-detector expiry so new
# submissions stop using a stale worker before in-flight trials are reclaimed.
# Bump if heartbeat interval changes (`loom_worker.config.heartbeat_interval_sec`).
_HEARTBEAT_FRESHNESS_SEC = 30

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
