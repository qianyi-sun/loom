"""Bounded Pipeline controller metrics; identities stay in structured logs."""

from __future__ import annotations

from prometheus_client import CollectorRegistry, Counter, Gauge


class PipelineOrchestratorMetrics:
    def __init__(self, registry: CollectorRegistry | None = None) -> None:
        self.runs = Gauge(
            "loom_pipeline_orchestrator_runs",
            "Pipeline runs by bounded state",
            ("state",),
            registry=registry,
        )
        self.transitions = Counter(
            "loom_pipeline_orchestrator_transitions_total",
            "Durable transitions by bounded kind and reason",
            ("kind", "reason"),
            registry=registry,
        )
        self.reservations = Gauge(
            "loom_pipeline_orchestrator_reservations",
            "Budget reservations by bounded kind and state",
            ("kind", "state"),
            registry=registry,
        )
