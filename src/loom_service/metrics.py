"""Prometheus metrics for loom_service (#81 slice B-3).

Cardinality bounded:
- `route` is a fixed set (the FastAPI route templates, ~25 paths).
- `status_class` is 2xx / 4xx / 5xx (3 values) instead of raw status.
- `result` is small per metric.

Naming: `loom_svc_*` prefix.
"""

from __future__ import annotations

from prometheus_client import Counter, Histogram

HTTP_REQUESTS_TOTAL = Counter(
    "loom_svc_http_requests_total",
    "HTTP requests handled by loom_service",
    # status_class folded down to 2xx/4xx/5xx; raw status would be
    # unbounded if /metrics scraped infrequently.
    labelnames=("route", "method", "status_class"),
)

HTTP_REQUEST_LATENCY_SEC = Histogram(
    "loom_svc_http_request_latency_sec",
    "HTTP request latency by route",
    labelnames=("route", "method"),
    # Service is mostly synchronous DB+forwarder operations; the long
    # tail comes from gateway forwards.
    buckets=(0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0),
)

BATCH_RUNNER_TICKS_TOTAL = Counter(
    "loom_svc_batch_runner_ticks_total",
    "Batch runner background-task ticks",
    # result: ok / skipped_no_token / error
    labelnames=("result",),
)

BATCH_RUNNER_TRIALS_DISPATCHED = Counter(
    "loom_svc_batch_runner_trials_dispatched_total",
    "Trials the batch runner has fanned out from queued batches",
)

TOKENS_ISSUED_TOTAL = Counter(
    "loom_svc_tokens_issued_total",
    "Tokens minted via /api/v1/tokens",
    # token_type: team / admin
    labelnames=("token_type",),
)

TOKENS_REVOKED_TOTAL = Counter(
    "loom_svc_tokens_revoked_total",
    "Tokens revoked via DELETE /api/v1/tokens/{prefix}",
)
