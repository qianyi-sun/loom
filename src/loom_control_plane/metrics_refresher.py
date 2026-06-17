"""Periodic refresher for gauge metrics — DB queries that turn into
`Gauge.set()` calls on the shared `metrics` registry.

The CP emits counters and histograms inline (state PATCH, claim
latency, worker reclaim). The gauges that drive the platform-health
alerts — `loom_workers_active`, `loom_queue_depth`,
`loom_trials_inflight` — are computed by SELECTs that would be too
expensive to run on every API call. Instead a background task
refreshes them at a fixed cadence (default 30s).

Cadence trade-off: 30s is short enough that the alerts react to real
issues within their `for:` windows (the shortest alert is 2 minutes —
plenty of refresh ticks), and long enough that the queries don't
hammer Postgres. Scale up for very large clusters where the
`trials` aggregation gets slow.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

from sqlalchemy import text

from loom_control_plane.metrics import (
    QUEUE_DEPTH,
    TRIALS_INFLIGHT,
    WORKERS_ACTIVE,
)

logger = logging.getLogger(__name__)

# `loom_workers_active` is one global gauge — the count of workers
# whose last_seen_at is within the heartbeat-expiry window. The
# expiry value is supplied by the lifespan (matches
# crash_detector's `expiry_sec`).
_WORKERS_ACTIVE_SQL = text("""
SELECT count(*) FROM workers
 WHERE last_seen_at > NOW() - INTERVAL '1 second' * (:expiry_sec)::int
""")

# Queue depth + in-flight: per-team gauges. We label by team_id so
# the per-team breakdown alerts work (sum by team_id, individual
# alerts).
_QUEUE_DEPTH_SQL = text("""
SELECT team_id::text, count(*) AS depth
  FROM trials
 WHERE state = 'queued'
 GROUP BY team_id
""")

_TRIALS_INFLIGHT_SQL = text("""
SELECT team_id::text, state, count(*) AS cnt
  FROM trials
 WHERE state IN ('queued', 'claimed', 'running')
 GROUP BY team_id, state
""")


async def refresh_once(session: Any, *, expiry_sec: int) -> None:
    """Run all three refresh queries and update the gauges.

    Per-team gauges are RESET to 0 for every previously-observed
    label set, then SET to the current counts. Without the reset
    step, a team that drops to 0 queued trials would keep its old
    nonzero value indefinitely (prometheus_client's Gauge labels
    are sticky)."""
    # workers_active is a scalar — no labels to clear.
    workers = (await session.execute(
        _WORKERS_ACTIVE_SQL, {"expiry_sec": expiry_sec},
    )).scalar_one_or_none() or 0
    WORKERS_ACTIVE.set(workers)

    # Per-team gauges: clear-then-set. The clear is per-label-set
    # via the internal `_metrics` dict — prometheus_client doesn't
    # expose a clean API for "reset all labels," so we just zero
    # out anything we previously set and overwrite with the
    # current scan. New label sets are picked up automatically; an
    # empty result row at the next tick zeroes them via the same
    # mechanism.
    queue_rows = (await session.execute(_QUEUE_DEPTH_SQL)).all()
    inflight_rows = (await session.execute(_TRIALS_INFLIGHT_SQL)).all()

    # Zero previously-seen labels (best-effort; clear() on the
    # underlying _metrics dict is not part of the public API but
    # is stable).
    QUEUE_DEPTH.clear()
    TRIALS_INFLIGHT.clear()
    for team_id, depth in queue_rows:
        QUEUE_DEPTH.labels(team_id=team_id).set(int(depth))
    for team_id, state, cnt in inflight_rows:
        TRIALS_INFLIGHT.labels(team_id=team_id, state=state).set(int(cnt))


async def run_metrics_refresher_loop(
    *,
    session_factory: Any,
    expiry_sec: int,
    interval_sec: int = 30,
) -> None:
    """Long-running task — refreshes every `interval_sec` seconds.
    Cancellation is the only stop signal."""
    while True:
        try:
            async with session_factory() as session:
                await refresh_once(session, expiry_sec=expiry_sec)
        except asyncio.CancelledError:
            return
        except Exception as exc:
            logger.warning("metrics_refresher_error", extra={"err": str(exc)})
        await asyncio.sleep(interval_sec)
