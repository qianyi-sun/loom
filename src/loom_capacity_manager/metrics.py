"""Private-registry, bounded-cardinality metrics for shadow capacity evidence."""

from __future__ import annotations

from prometheus_client import CollectorRegistry, Counter, Gauge, generate_latest

RUN_RESULTS = frozenset({"committed", "input-contention", "failed"})
RUN_REASONS = frozenset(
    {
        "none",
        "timeout",
        "invalid-input",
        "input-contention",
        "writer-fenced",
        "transaction-failed",
        "unexpected",
    }
)
FRESHNESS_STATES = frozenset({"valid", "stale", "missing", "invalid", "equivocal"})
REPORT_KINDS = frozenset({"demand", "pool"})
POOL_SLOT_STATES = frozenset({"configured", "desired", "committed"})


class CapacityMetrics:
    """Per-app collectors with no subject or environment-name labels."""

    def __init__(self) -> None:
        self.registry = CollectorRegistry(auto_describe=True)
        self.ready = Gauge(
            "loom_capacity_manager_ready",
            "Whether the manager holds a valid writer fence.",
            registry=self.registry,
        )
        self.executable_new_capacity_ceiling = Gauge(
            "loom_capacity_manager_executable_new_capacity_ceiling",
            "Executable capacity ceiling; Package 1 requires zero.",
            registry=self.registry,
        )
        self.increase_freeze = Gauge(
            "loom_capacity_manager_increase_freeze",
            "Whether capacity increases are globally frozen.",
            registry=self.registry,
        )
        self.report_freshness = Gauge(
            "loom_capacity_manager_report_freshness",
            "Bounded reporter freshness counts.",
            ("report_kind", "state"),
            registry=self.registry,
        )
        self.pool_slots = Gauge(
            "loom_capacity_manager_pool_slots",
            "Configured or desired pool slots.",
            ("pool_id", "state"),
            registry=self.registry,
        )
        self.shadow_runs = Counter(
            "loom_capacity_manager_shadow_runs_total",
            "Completed shadow reconciliation calls.",
            ("result", "reason"),
            registry=self.registry,
        )
        self.executable_new_capacity_ceiling.set(0)
        self.ready.set(0)
        self.increase_freeze.set(1)

    def observe_run(self, result: str, reason: str) -> None:
        safe_result = result if result in RUN_RESULTS else "failed"
        safe_reason = reason if reason in RUN_REASONS else "unexpected"
        self.shadow_runs.labels(result=safe_result, reason=safe_reason).inc()

    def render(self) -> bytes:
        return generate_latest(self.registry)


__all__ = [
    "FRESHNESS_STATES",
    "POOL_SLOT_STATES",
    "REPORT_KINDS",
    "RUN_REASONS",
    "RUN_RESULTS",
    "CapacityMetrics",
]
