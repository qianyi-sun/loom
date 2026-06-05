"""Backoff scheduling for trial retries (spec §5.3)."""

from __future__ import annotations

import random
from datetime import datetime, timedelta

from loom.models.trial import BackoffSpec


def next_attempt_at(
    *,
    attempt_count: int,
    backoff: BackoffSpec,
    now: datetime,
) -> datetime:
    """Compute when a trial may next be retried.

    `attempt_count` is the number of attempts already made (1 for the first retry).
    Result is `now + min(base × multiplier^(attempt_count-1), max_sec) × jitter`.
    """
    if attempt_count < 1:
        raise ValueError("attempt_count must be >= 1")

    raw_delay = backoff.base_sec * (backoff.multiplier ** (attempt_count - 1))
    capped = min(raw_delay, backoff.max_sec)
    jittered = capped * random.uniform(1 - backoff.jitter, 1 + backoff.jitter)
    return now + timedelta(seconds=jittered)
