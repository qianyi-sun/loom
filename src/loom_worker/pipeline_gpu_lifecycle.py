"""Worker-owned lifecycle signal for allocated GPUs without expected processes."""

from __future__ import annotations

import time
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Literal
from uuid import UUID

from loom_worker.metrics import PIPELINE_GPU_ALLOCATED_IDLE_SECONDS

PipelineGpuIdleReason = Literal["pre_start", "process_absent", "cleanup_pending"]


@dataclass(frozen=True)
class _Lifecycle:
    reason: PipelineGpuIdleReason
    since: float


@dataclass
class PipelineGpuLifecycleTracker:
    """Track process-group lifecycle; GPU utilization is deliberately absent."""

    clock: Callable[[], float] = time.monotonic
    _attempts: dict[UUID, _Lifecycle] = field(default_factory=dict)

    def mark(
        self,
        attempt_id: UUID,
        *,
        reason: PipelineGpuIdleReason,
    ) -> None:
        current = self._attempts.get(attempt_id)
        if current is None or current.reason != reason:
            self._attempts[attempt_id] = _Lifecycle(reason, self.clock())
        self.refresh()

    def process_present(self, attempt_id: UUID) -> None:
        self._attempts.pop(attempt_id, None)
        self.refresh()

    def clear(self, attempt_id: UUID) -> None:
        self._attempts.pop(attempt_id, None)
        self.refresh()

    def refresh(self) -> None:
        now = self.clock()
        maxima: dict[PipelineGpuIdleReason, float] = {}
        for lifecycle in self._attempts.values():
            key = lifecycle.reason
            maxima[key] = max(maxima.get(key, 0), max(now - lifecycle.since, 0))
        PIPELINE_GPU_ALLOCATED_IDLE_SECONDS.clear()
        for reason, age in maxima.items():
            PIPELINE_GPU_ALLOCATED_IDLE_SECONDS.labels(
                reason=reason,
            ).set(age)
