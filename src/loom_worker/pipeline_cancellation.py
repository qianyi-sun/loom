"""Worker-side sticky cancellation and teardown ordering."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Protocol
from uuid import UUID

from loom.pipeline.work_protocol import WorkerCleanupProofV1


class CancellationBackend(Protocol):
    async def term(self, *, attempt_id: UUID) -> None: ...
    async def wait_empty(self, *, attempt_id: UUID, timeout_seconds: int) -> bool: ...
    async def kill(self, *, attempt_id: UUID) -> None: ...
    async def teardown(self, *, attempt_id: UUID) -> WorkerCleanupProofV1: ...


@dataclass(frozen=True, slots=True)
class CancellationObservation:
    observed_at: datetime
    outcome: str
    resources: WorkerCleanupProofV1


@dataclass(slots=True)
class PipelineCancellationCoordinator:
    backend: CancellationBackend
    grace_seconds: int = 30
    poll_seconds: int = 5

    def __post_init__(self) -> None:
        if self.grace_seconds != 30 or self.poll_seconds != 5:
            raise ValueError("Pipeline cancellation cadence is fixed at 5s/30s")

    async def observe_and_teardown(self, *, attempt_id: UUID) -> CancellationObservation:
        observed_at = datetime.now(UTC)
        await self.backend.term(attempt_id=attempt_id)
        graceful = await self.backend.wait_empty(
            attempt_id=attempt_id, timeout_seconds=self.grace_seconds
        )
        if not graceful:
            await self.backend.kill(attempt_id=attempt_id)
            empty = await self.backend.wait_empty(attempt_id=attempt_id, timeout_seconds=0)
            if not empty:
                raise RuntimeError("execution resources remain after SIGKILL")
        resources = await self.backend.teardown(attempt_id=attempt_id)
        return CancellationObservation(
            observed_at=observed_at,
            outcome="graceful" if graceful else "forced",
            resources=resources,
        )

__all__ = ["CancellationObservation", "PipelineCancellationCoordinator"]
