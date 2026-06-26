"""Shared lookup: which backends do currently-active workers serve?

Used by:
- `GET /api/v1/backends` — render the catalog with `available=true|false`.
- `POST /api/v1/batches` — reject when the requested backend has no
  active worker (cluster-deploy.md §POST /batches).

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

from datetime import UTC, datetime, timedelta

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from loom.db.schema import Worker

# Freshness window — 30s = 6 heartbeat intervals. Generous enough to
# ride out network blips without keeping a dead worker visible to users.
# This must remain shorter than the CP crash-detector expiry so new
# submissions stop using a stale worker before in-flight trials are reclaimed.
# Bump if heartbeat interval changes (`loom_worker.config.heartbeat_interval_sec`).
_HEARTBEAT_FRESHNESS_SEC = 30


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
