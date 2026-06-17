"""Prometheus metrics for the LLM Gateway (#81 slice B-2).

Cardinality is bounded by design:
- `provider` is a fixed set (openai, anthropic, google, openai-compatible,
  custom — 5 values).
- `dialect` is a fixed set (openai, anthropic, google, responses,
  gemini — 5 values).
- `result` is a small enum per metric.
- `team_id` is high-cardinality — we attach it ONLY where the
  per-team breakdown is critical (`COST_TOTAL`), never on rates or
  latencies.

Naming: `loom_gateway_*` prefix keeps gateway series distinguishable
from CP / service / worker series in shared dashboards.
"""

from __future__ import annotations

from prometheus_client import Counter, Histogram

LLM_CALLS_TOTAL = Counter(
    "loom_gateway_llm_calls_total",
    "LLM calls forwarded through the gateway",
    # provider: the connection's provider_type (openai, anthropic, ...)
    # dialect: which facade routed the call (openai, anthropic, ...,
    #          may differ from provider for openai-compatible)
    # result: ok / upstream_error / ssrf_blocked / auth_error /
    #         rate_limited / timeout
    labelnames=("provider", "dialect", "result"),
)

LLM_CALL_LATENCY_SEC = Histogram(
    "loom_gateway_llm_call_latency_sec",
    "Wall-clock latency of LLM calls forwarded through the gateway",
    labelnames=("provider", "dialect"),
    # Standard prometheus-client default buckets are tuned for sub-second
    # web requests; LLM calls span 100ms (cache hit) → 60s (long
    # completion). Explicit buckets surface the long tail.
    buckets=(0.1, 0.25, 0.5, 1.0, 2.0, 5.0, 10.0, 20.0, 60.0),
)

COST_USD_TOTAL = Counter(
    "loom_gateway_cost_usd_total",
    "Cumulative cost of LLM calls in USD, by team + provider",
    # team_id is high-cardinality but bounded by team count
    # (typically dozens, not thousands). Acceptable for cost
    # attribution dashboards.
    labelnames=("team_id", "provider"),
)

PROVIDER_VALIDATION_TOTAL = Counter(
    "loom_gateway_provider_validation_total",
    "Provider connection test results (POST /provider-connections/{id}/test)",
    # result: valid / invalid / unreachable
    labelnames=("provider", "result"),
)
