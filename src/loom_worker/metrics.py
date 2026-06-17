"""Prometheus metrics for loom_worker (#81 slice B-4).

Unlike the FastAPI services (CP / gateway / loom_service), the worker
is a long-running asyncio process — there's no app to mount /metrics
on. We spin up `prometheus_client.start_http_server(port)` at startup
so the worker can be scraped on a known port (default 9101, configurable
via `LOOM_WORKER_METRICS_PORT`).

Cardinality bounded:
- `result` is a small enum per metric.
- `backend` is a fixed enum (docker, fake, daytona, modal).
"""

from __future__ import annotations

from prometheus_client import Counter, Gauge, Histogram

# Per-worker gauges. Each worker process serves its own /metrics, so
# the prometheus job scrape picks up the instance label naturally —
# no need for an explicit worker_id label.
TRIALS_INFLIGHT = Gauge(
    "loom_worker_trials_inflight",
    "Trials currently running on this worker (in-flight in the RunnerPool)",
)

TRIALS_STARTED_TOTAL = Counter(
    "loom_worker_trials_started_total",
    "Trials this worker has claimed + started",
    labelnames=("backend",),
)

TRIALS_COMPLETED_TOTAL = Counter(
    "loom_worker_trials_completed_total",
    "Trials this worker has run to a terminal state",
    # result: succeeded / failed / cancelled / crashed
    labelnames=("backend", "result"),
)

TRIAL_DURATION_SEC = Histogram(
    "loom_worker_trial_duration_sec",
    "Wall-clock duration of trial runs on this worker",
    labelnames=("backend", "result"),
    # Trial durations span seconds → tens of minutes. Match the alert
    # threshold buckets (which key off claim → terminal time).
    buckets=(1.0, 5.0, 15.0, 60.0, 300.0, 900.0, 1800.0, 3600.0, 7200.0),
)

CLAIM_LOOP_ITERATIONS_TOTAL = Counter(
    "loom_worker_claim_loop_iterations_total",
    "Number of claim-loop iterations the worker has run",
    # result: hit (got a trial) / miss (no work) / error (CP unreachable)
    labelnames=("result",),
)

HEARTBEAT_FAILURES_TOTAL = Counter(
    "loom_worker_heartbeat_failures_total",
    "Heartbeat thread ticks that failed to PATCH the CP",
)
