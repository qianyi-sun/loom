"""Shared positive teardown acknowledgement contracts for executable work."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Literal
from uuid import UUID

from prometheus_client import Counter, Histogram

from loom.pipeline.keys import canonical_digest
from loom.pipeline.work_protocol import WorkerCleanupProofV1

CANCEL_ACKS = Counter(
    "loom_execution_cancellation_ack_total",
    "Positive execution cleanup acknowledgements",
    ("work_kind", "outcome"),
)
CANCEL_LATENCY = Histogram(
    "loom_execution_cancellation_latency_seconds",
    "Seconds from cancellation request to positive cleanup acknowledgement",
    ("work_kind", "outcome"),
)


@dataclass(frozen=True, slots=True)
class ExecutionCancellationAck:
    work_kind: Literal["pipeline", "trial", "batch"]
    execution_id: UUID
    requested_at: datetime
    observed_at: datetime
    outcome: Literal["not_started", "graceful", "forced", "worker_lost_cleanup"]
    resources: WorkerCleanupProofV1
    version: int

    def __post_init__(self) -> None:
        if self.requested_at.tzinfo is None or self.observed_at.tzinfo is None:
            raise ValueError("cancellation timestamps must be timezone-aware")
        if self.observed_at < self.requested_at:
            raise ValueError("cancellation observation precedes its request")
        if self.version < 0:
            raise ValueError("cancellation version cannot be negative")

    @property
    def resource_digest(self) -> str:
        return canonical_digest(self.resources)

    def observe_metrics(self) -> None:
        CANCEL_ACKS.labels(self.work_kind, self.outcome).inc()
        CANCEL_LATENCY.labels(self.work_kind, self.outcome).observe(
            (self.observed_at - self.requested_at).total_seconds()
        )


__all__ = ["ExecutionCancellationAck"]
