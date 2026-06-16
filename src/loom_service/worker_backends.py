"""Shared lookup: which backends do currently-active workers serve?

Used by:
- `GET /api/v1/backends` — render the catalog with `available=true|false`.
- `POST /api/v1/batches` — reject when the requested backend has no
  active worker (cluster-deploy.md §POST /batches).

A backend is "active" iff at least one row in `workers` with
`status='active'` advertises that backend in its capabilities JSONB.
Workers registered before Plan 28 PR-3 omit the `backend` key in each
capability dict — those rows fall back to "docker" since that was the
only backend the worker pool shipped before that PR.

Stale heartbeats are not considered here — the schema field
`status` is the source of truth (the worker flips itself to
'shutting-down' on SIGTERM; a CP-side reaper transitions long-stale
rows to 'dead'). This matches `GET /backends` behavior exactly so
the SPA's catalog and the batch-creation check stay consistent.
"""

from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from loom.db.schema import Worker


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
    backend), which the batch route translates to a 400."""
    rows = (await session.execute(
        select(Worker.capabilities).where(Worker.status == "active"),
    )).scalars().all()
    return parse_backends_from_capabilities(list(rows))
