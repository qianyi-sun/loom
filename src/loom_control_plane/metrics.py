"""Prometheus metric definitions for Control Plane (spec §7.3).

Cardinality is bounded by design — `team_id` is the only high-cardinality
label and is only attached where strictly necessary.
"""

from __future__ import annotations

from prometheus_client import Counter, Gauge, Histogram

TRIALS_STATE_TOTAL = Counter(
    "loom_trials_state_total",
    "Trial state transitions",
    labelnames=("from_state", "to_state", "team_id"),
)

TRIALS_INFLIGHT = Gauge(
    "loom_trials_inflight",
    "Trials in claimed/running",
    labelnames=("team_id", "state"),
)

QUEUE_DEPTH = Gauge(
    "loom_queue_depth",
    "Queued trials per team",
    labelnames=("team_id",),
)

CLAIM_LATENCY_SEC = Histogram(
    "loom_claim_latency_sec",
    "Time for POST /trials/claim",
    labelnames=("result",),  # 'hit' | 'miss'
)

STATE_PATCH_TOTAL = Counter(
    "loom_state_patch_total",
    "Trial state PATCH outcomes",
    # endpoint: 'state' / 'trajectory' / ...; result: 'ok' / 'fenced' / 'timeout'
    labelnames=("endpoint", "result"),
)

WORKERS_ACTIVE = Gauge(
    "loom_workers_active",
    "Workers with fresh heartbeat",
)

WORKER_RECLAIM_TOTAL = Counter(
    "loom_worker_reclaim_total",
    "Trials reclaimed by crash detector",
)

RETRY_EXHAUSTED_TOTAL = Counter(
    "loom_retry_exhausted_total",
    "Trials transitioned to failed because attempt_count >= max_attempts",
)
