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

from loom.resource_pools import get_resource_pool_summary
from loom_control_plane.metrics import (
    EXECUTION_ATTEMPTS,
    PIPELINE_CHECKPOINT_OLDEST_AGE_SECONDS,
    PIPELINE_RUNS,
    PIPELINE_STAGE_DEADLINE_OVERRUN_SECONDS,
    PIPELINE_STAGE_QUEUE_AGE_SECONDS,
    PIPELINE_STAGE_RUNS,
    QUEUE_DEPTH,
    SLURM_WORKER_ACTIVE_SLOTS,
    SLURM_WORKER_CANCELLED_PENDING_JOBS,
    SLURM_WORKER_DESIRED_SLOTS,
    SLURM_WORKER_FAILED_SUBMISSIONS,
    SLURM_WORKER_IDLE_EXITS,
    SLURM_WORKER_PENDING_JOBS,
    SLURM_WORKER_PENDING_SLOTS,
    SLURM_WORKER_RUNNING_JOBS,
    SLURM_WORKER_STALE_JOBS,
    SLURM_WORKER_STALE_SLOTS,
    TRIALS_INFLIGHT,
    WORKER_POOL_AUTOSCALER_DECISION,
    WORKER_POOL_AUTOSCALER_ERROR,
    WORKER_POOL_AUTOSCALER_IDLE_SECONDS,
    WORKER_POOL_DESIRED_SLOTS,
    WORKER_POOL_DRAINING_SLOTS,
    WORKER_POOL_DRAINING_WORKERS,
    WORKER_POOL_FREE_SLOTS,
    WORKER_POOL_OCCUPIED_SLOTS,
    WORKER_POOL_PENDING_SLOTS,
    WORKER_POOL_TOTAL_SLOTS,
    WORKER_POOL_WORKERS,
    WORKER_TOKENS_STALE_COUNT,
    WORKERS_ACTIVE,
)
from loom_control_plane.slurm_worker_jobs import (
    fetch_slurm_worker_metric_rows,
    summarize_jobs,
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

# Worker-token staleness audit. Counts live worker tokens that have
# either gone unused for >30 days (last_seen_at falls back to issued_at
# for never-used tokens, so a token minted yesterday is NOT flagged) or
# are >90 days old since mint (rotation overdue). Filters out revoked
# and naturally-expired rows since those can't authenticate anyway.
_WORKER_TOKENS_STALENESS_SQL = text("""
SELECT
    COUNT(*) FILTER (
        WHERE COALESCE(last_seen_at, issued_at)
              < NOW() - INTERVAL '30 days'
    ) AS unused_30d,
    COUNT(*) FILTER (
        WHERE issued_at < NOW() - INTERVAL '90 days'
    ) AS aged_90d
  FROM tokens
 WHERE type = 'worker'
   AND revoked_at IS NULL
   AND (expires_at IS NULL OR expires_at > NOW())
""")

_PIPELINE_RUNS_SQL = text("""
SELECT state, COALESCE(result, 'none') AS result_status, count(*) AS cnt
  FROM pipeline_runs
 GROUP BY state, COALESCE(result, 'none')
""")

# A gate is controller-owned.  Container stages use a bounded cpu/gpu class;
# the immutable resolved profile is the authority, never a user-provided label.
_PIPELINE_STAGE_RESOURCE_CLASS_SQL = """
CASE
  WHEN stage.node_kind = 'gate' THEN 'controller'
  WHEN EXISTS (
    SELECT 1
      FROM jsonb_array_elements(
        COALESCE(stage.resource_profile_json->'execution_variants', '[]'::jsonb)
      ) AS variant
     WHERE COALESCE((variant->>'gpu_count_exact')::int, 0) > 0
  ) THEN 'gpu'
  ELSE 'cpu'
END
"""

_PIPELINE_STAGE_RUNS_SQL = text(f"""
SELECT stage.state, {_PIPELINE_STAGE_RESOURCE_CLASS_SQL} AS resource_class,
       count(*) AS cnt
  FROM pipeline_stage_runs AS stage
 GROUP BY stage.state, resource_class
""")

_PIPELINE_ATTEMPTS_SQL = text(f"""
SELECT attempt.state, {_PIPELINE_STAGE_RESOURCE_CLASS_SQL} AS resource_class,
       count(*) AS cnt
  FROM execution_attempts AS attempt
  JOIN pipeline_stage_runs AS stage ON stage.id = attempt.stage_run_id
 GROUP BY attempt.state, resource_class
""")

_PIPELINE_QUEUE_AGE_SQL = text(f"""
SELECT stage.state, {_PIPELINE_STAGE_RESOURCE_CLASS_SQL} AS resource_class,
       max(EXTRACT(EPOCH FROM (
         NOW() - COALESCE(
           CASE
             WHEN stage.state = 'retry_wait' THEN stage.next_attempt_at
             WHEN stage.state = 'claimed' THEN stage.claimed_at
             ELSE stage.ready_at
           END,
           stage.created_at
         )
       ))) AS age_seconds
  FROM pipeline_stage_runs AS stage
 WHERE stage.state IN ('ready', 'queued', 'claimed', 'retry_wait')
 GROUP BY stage.state, resource_class
""")

_PIPELINE_DEADLINE_OVERRUN_SQL = text(f"""
SELECT {_PIPELINE_STAGE_RESOURCE_CLASS_SQL} AS resource_class,
       max(GREATEST(EXTRACT(EPOCH FROM (
         NOW() - (
           attempt.started_at
           + make_interval(
               secs => ((stage.resolved_execution_spec_json->>'timeout_seconds')::int + 35)
             )
         )
       )), 0)) AS age_seconds
  FROM execution_attempts AS attempt
  JOIN pipeline_stage_runs AS stage ON stage.id = attempt.stage_run_id
 WHERE attempt.state = 'running'
   AND attempt.started_at IS NOT NULL
 GROUP BY resource_class
""")

_PIPELINE_CHECKPOINT_AGE_SQL = text(f"""
SELECT {_PIPELINE_STAGE_RESOURCE_CLASS_SQL} AS resource_class,
       max(EXTRACT(EPOCH FROM (
         NOW() - COALESCE(artifact.created_at, attempt.started_at)
       ))) AS age_seconds
  FROM execution_attempts AS attempt
  JOIN pipeline_stage_runs AS stage ON stage.id = attempt.stage_run_id
  LEFT JOIN artifacts AS artifact ON artifact.id = stage.latest_checkpoint_artifact_id
 WHERE attempt.state = 'running'
   AND attempt.started_at IS NOT NULL
   AND stage.resolved_execution_spec_json->'checkpoint' IS NOT NULL
   AND stage.resolved_execution_spec_json->'checkpoint' <> 'null'::jsonb
 GROUP BY resource_class
""")

_SLURM_WORKER_GAUGES = (
    SLURM_WORKER_DESIRED_SLOTS,
    SLURM_WORKER_ACTIVE_SLOTS,
    SLURM_WORKER_PENDING_SLOTS,
    SLURM_WORKER_STALE_SLOTS,
    SLURM_WORKER_RUNNING_JOBS,
    SLURM_WORKER_PENDING_JOBS,
    SLURM_WORKER_STALE_JOBS,
    SLURM_WORKER_FAILED_SUBMISSIONS,
    SLURM_WORKER_CANCELLED_PENDING_JOBS,
    SLURM_WORKER_IDLE_EXITS,
)

_WORKER_POOL_GAUGES = (
    WORKER_POOL_TOTAL_SLOTS,
    WORKER_POOL_OCCUPIED_SLOTS,
    WORKER_POOL_FREE_SLOTS,
    WORKER_POOL_WORKERS,
    WORKER_POOL_DESIRED_SLOTS,
    WORKER_POOL_PENDING_SLOTS,
    WORKER_POOL_DRAINING_SLOTS,
    WORKER_POOL_DRAINING_WORKERS,
    WORKER_POOL_AUTOSCALER_DECISION,
    WORKER_POOL_AUTOSCALER_ERROR,
    WORKER_POOL_AUTOSCALER_IDLE_SECONDS,
)


async def refresh_pipeline_gauges(session: Any) -> None:
    """Refresh bounded Pipeline gauges and remove disappeared label tuples."""

    run_rows = (await session.execute(_PIPELINE_RUNS_SQL)).all()
    stage_rows = (await session.execute(_PIPELINE_STAGE_RUNS_SQL)).all()
    attempt_rows = (await session.execute(_PIPELINE_ATTEMPTS_SQL)).all()
    queue_rows = (await session.execute(_PIPELINE_QUEUE_AGE_SQL)).all()
    deadline_rows = (await session.execute(_PIPELINE_DEADLINE_OVERRUN_SQL)).all()
    checkpoint_rows = (await session.execute(_PIPELINE_CHECKPOINT_AGE_SQL)).all()

    for gauge in (
        PIPELINE_RUNS,
        PIPELINE_STAGE_RUNS,
        EXECUTION_ATTEMPTS,
        PIPELINE_STAGE_QUEUE_AGE_SECONDS,
        PIPELINE_STAGE_DEADLINE_OVERRUN_SECONDS,
        PIPELINE_CHECKPOINT_OLDEST_AGE_SECONDS,
    ):
        gauge.clear()
    for state, result_status, count in run_rows:
        PIPELINE_RUNS.labels(state=state, result_status=result_status).set(int(count))
    for state, resource_class, count in stage_rows:
        PIPELINE_STAGE_RUNS.labels(state=state, resource_class=resource_class).set(int(count))
    for state, resource_class, count in attempt_rows:
        EXECUTION_ATTEMPTS.labels(state=state, resource_class=resource_class).set(int(count))
    for state, resource_class, age_seconds in queue_rows:
        PIPELINE_STAGE_QUEUE_AGE_SECONDS.labels(state=state, resource_class=resource_class).set(
            max(float(age_seconds or 0), 0)
        )
    for resource_class, age_seconds in deadline_rows:
        PIPELINE_STAGE_DEADLINE_OVERRUN_SECONDS.labels(resource_class=resource_class).set(
            max(float(age_seconds or 0), 0)
        )
    for resource_class, age_seconds in checkpoint_rows:
        PIPELINE_CHECKPOINT_OLDEST_AGE_SECONDS.labels(resource_class=resource_class).set(
            max(float(age_seconds or 0), 0)
        )


async def refresh_once(session: Any, *, expiry_sec: int) -> None:
    """Run all three refresh queries and update the gauges.

    Per-team gauges are RESET to 0 for every previously-observed
    label set, then SET to the current counts. Without the reset
    step, a team that drops to 0 queued trials would keep its old
    nonzero value indefinitely (prometheus_client's Gauge labels
    are sticky)."""
    # workers_active is a scalar — no labels to clear.
    workers = (
        await session.execute(
            _WORKERS_ACTIVE_SQL,
            {"expiry_sec": expiry_sec},
        )
    ).scalar_one_or_none() or 0
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

    resource_summary = await get_resource_pool_summary(
        session,
        freshness_sec=expiry_sec,
    )
    for gauge in _WORKER_POOL_GAUGES:
        gauge.clear()
    for pool in resource_summary["pools"]:
        labels = {
            "pool_name": str(pool["pool_name"]),
            "backend": str(pool["backend"]),
            "cpu_arch": str(pool["cpu_arch"]),
        }
        WORKER_POOL_TOTAL_SLOTS.labels(**labels).set(int(pool["total_slots"]))
        WORKER_POOL_OCCUPIED_SLOTS.labels(**labels).set(
            int(pool["occupied_slots"]),
        )
        WORKER_POOL_FREE_SLOTS.labels(**labels).set(int(pool["free_slots"]))
        WORKER_POOL_WORKERS.labels(**labels).set(int(pool["active_workers"]))
        WORKER_POOL_DESIRED_SLOTS.labels(**labels).set(
            int(pool["desired_slots"]),
        )
        WORKER_POOL_PENDING_SLOTS.labels(**labels).set(
            int(pool["pending_slots"]),
        )
        WORKER_POOL_DRAINING_SLOTS.labels(**labels).set(
            int(pool["draining_slots"]),
        )
        WORKER_POOL_DRAINING_WORKERS.labels(**labels).set(
            int(pool["draining_workers"]),
        )
        decision = pool.get("last_autoscaler_decision")
        if decision:
            WORKER_POOL_AUTOSCALER_DECISION.labels(
                **labels,
                action=str(decision),
                reason=str(pool.get("last_autoscaler_reason") or ""),
            ).set(1)
        WORKER_POOL_AUTOSCALER_ERROR.labels(**labels).set(
            1 if pool.get("last_autoscaler_error") else 0,
        )
        WORKER_POOL_AUTOSCALER_IDLE_SECONDS.labels(**labels).set(
            int(pool.get("autoscaler_idle_seconds") or 0),
        )

    # Worker-token staleness audit. Re-publishes the gauge each tick;
    # the alert fires when either reason has a non-zero count for >1h.
    staleness_row = (await session.execute(_WORKER_TOKENS_STALENESS_SQL)).one()
    WORKER_TOKENS_STALE_COUNT.labels(reason="unused_30d").set(
        int(staleness_row.unused_30d),
    )
    WORKER_TOKENS_STALE_COUNT.labels(reason="aged_90d").set(
        int(staleness_row.aged_90d),
    )

    slurm_summary = summarize_jobs(await fetch_slurm_worker_metric_rows(session))
    for gauge in _SLURM_WORKER_GAUGES:
        gauge.clear()
    for capacity in slurm_summary.by_pool.values():
        labels = {
            "environment": capacity.environment,
            "pool_name": capacity.pool_name,
        }
        SLURM_WORKER_DESIRED_SLOTS.labels(**labels).set(capacity.desired_slots)
        SLURM_WORKER_ACTIVE_SLOTS.labels(**labels).set(capacity.active_slots)
        SLURM_WORKER_PENDING_SLOTS.labels(**labels).set(capacity.pending_slots)
        SLURM_WORKER_STALE_SLOTS.labels(**labels).set(capacity.stale_slots)
        SLURM_WORKER_RUNNING_JOBS.labels(**labels).set(capacity.running_jobs)
        SLURM_WORKER_PENDING_JOBS.labels(**labels).set(capacity.pending_jobs)
        SLURM_WORKER_STALE_JOBS.labels(**labels).set(capacity.stale_jobs)
        SLURM_WORKER_FAILED_SUBMISSIONS.labels(**labels).set(
            capacity.failed_submissions,
        )
        SLURM_WORKER_CANCELLED_PENDING_JOBS.labels(**labels).set(
            capacity.cancelled_pending_jobs,
        )
        SLURM_WORKER_IDLE_EXITS.labels(**labels).set(capacity.idle_exits)

    await refresh_pipeline_gauges(session)


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
