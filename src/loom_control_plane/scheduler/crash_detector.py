"""Background sweep that reclaims trials from workers whose heartbeat has
lapsed (spec §2.8 + §3.10).

Runs every `interval_sec`. Trials in claimed/running state owned by a worker
whose `last_seen_at` is more than `expiry_sec` old go back to queued with a
30-second backoff, so the scheduler doesn't immediately re-hand them to the
same dead worker.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from loom_control_plane.metrics import WORKER_RECLAIM_TOTAL

logger = logging.getLogger(__name__)

_RECLAIM_SQL = text("""
UPDATE trials
   SET state = 'queued',
       worker_id = NULL,
       failure_reason = CASE
           WHEN state = 'claimed' AND started_at IS NULL
           THEN 'worker_lost_claim'
           ELSE failure_reason
       END,
       failure_message = CASE
           WHEN state = 'claimed' AND started_at IS NULL
           THEN CONCAT(
               'claimed_without_started_reclaimed trial_id=', id::text,
               ' worker_id=', worker_id::text,
               ' claimed_at=', claimed_at::text,
               ' expiry_sec=', (:expiry_sec)::int::text,
               ' started_at=NULL'
           )
           ELSE failure_message
       END,
       next_attempt_at = NOW() + INTERVAL '1 second' * 30
 WHERE state IN ('claimed', 'running')
   AND worker_id IN (
       SELECT id FROM workers
        WHERE last_seen_at < NOW() - INTERVAL '1 second' * (:expiry_sec)::int
   )
 RETURNING id;
""")

_STALE_CLAIM_SQL = text("""
UPDATE trials
   SET state = 'queued',
       worker_id = NULL,
       failure_reason = 'worker_lost_claim',
       failure_message = CONCAT(
           'claimed_without_started_reclaimed trial_id=', id::text,
           ' worker_id=', worker_id::text,
           ' claimed_at=', claimed_at::text,
           ' expiry_sec=', (:expiry_sec)::int::text,
           ' started_at=NULL'
       ),
       next_attempt_at = NOW() + INTERVAL '1 second' * 30
 WHERE state = 'claimed'
   AND started_at IS NULL
   AND claimed_at IS NOT NULL
   AND claimed_at < NOW() - INTERVAL '1 second' * (:expiry_sec)::int
 RETURNING id;
""")


async def reclaim_expired_workers(
    session: AsyncSession,
    *,
    expiry_sec: int,
    claimed_without_start_expiry_sec: int | None = None,
) -> int:
    """Run a single sweep. Returns the number of trials reclaimed."""
    expired_rows = (
        await session.execute(
            _RECLAIM_SQL,
            {
                "expiry_sec": expiry_sec,
            },
        )
    ).all()
    stale_count = 0
    if claimed_without_start_expiry_sec is not None:
        stale_count = len(
            (
                await session.execute(
                    _STALE_CLAIM_SQL,
                    {
                        "expiry_sec": claimed_without_start_expiry_sec,
                    },
                )
            ).all()
        )
    return len(expired_rows) + stale_count


async def run_crash_detector_loop(
    *,
    session_factory: Any,
    expiry_sec: int,
    interval_sec: int,
    claimed_without_start_expiry_sec: int | None = None,
) -> None:
    """Long-running task — sweeps every `interval_sec` seconds. Cancellation
    is the only way to stop it; lifespan teardown awaits the cancellation."""
    while True:
        try:
            async with session_factory() as session:
                count = await reclaim_expired_workers(
                    session,
                    expiry_sec=expiry_sec,
                    claimed_without_start_expiry_sec=(claimed_without_start_expiry_sec),
                )
                await session.commit()
            if count:
                logger.info("crash_detector_reclaimed", extra={"count": count})
                WORKER_RECLAIM_TOTAL.inc(count)
        except asyncio.CancelledError:
            return
        except Exception as exc:
            logger.warning("crash_detector_error", extra={"err": str(exc)})
        await asyncio.sleep(interval_sec)
