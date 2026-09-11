"""Bounded management-only final recovery of native build allocations."""

import asyncio
from dataclasses import dataclass
from typing import Literal, Protocol
from uuid import UUID

from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from loom_capacity_agent.admission import PublishableExecutableProtectedReleaseV2
from loom_capacity_agent.client import DemandPublishError
from loom_capacity_build_guard.hold_retirement import (
    BuildGuardHoldRetirementStore,
    RetiredBuildHoldV1,
)
from loom_capacity_build_guard.installation_store import RetainedBuildInstallation
from loom_capacity_build_guard.terminal_store import BuildGuardTerminalStore
from loom_capacity_manager.executable_contracts import ExecutableFinalReleaseWitnessV2
from loom_capacity_manager.typed_inventory_contracts import ExecutableTerminalInventoryEvidenceV3


class BuildRecoveryManager(Protocol):
    async def get_final_release_witness(self, intent_id: UUID) -> ExecutableFinalReleaseWitnessV2 | None: ...

    async def get_build_terminal_inventory_evidence(self, intent_id: UUID) -> ExecutableTerminalInventoryEvidenceV3 | None: ...


@dataclass(frozen=True, slots=True)
class BuildRecoveryResult:
    event_id: int
    intent_id: UUID
    state: Literal["retired", "unavailable", "failed"]
    receipt: RetiredBuildHoldV1 | None = None
    failure: Literal["timeout", "transport", "authority", "database"] | None = None


class BuildRecoveryCoordinator:
    """Sweep a finite event range without network locks or durable cursor claims.

    The cursor is process-local scan progress, never an acknowledgement. Restart
    begins again at zero; immutable retirements make repetition safe. A finite
    upper bound ensures continuous arrivals cannot starve earlier retries.
    """

    def __init__(self, *, session_factory: async_sessionmaker[AsyncSession], installation: RetainedBuildInstallation,
        manager: BuildRecoveryManager, batch_size: int = 16, item_timeout_seconds: float = 5,
    ) -> None:
        if type(batch_size) is not int or not 1 <= batch_size <= 64:
            raise ValueError("build recovery batch size must be between one and 64")
        if (isinstance(item_timeout_seconds, bool) or not isinstance(item_timeout_seconds, (int, float))
            or not 0.05 <= item_timeout_seconds <= 30):
            raise ValueError("build recovery item timeout must be between 0.05 and 30 seconds")
        self._sessions = session_factory
        self._installation = installation
        self._manager = manager
        self._batch_size = batch_size
        self._timeout = item_timeout_seconds
        self._after = 0
        self._through: int | None = None
        self._lock = asyncio.Lock()

    async def reconcile(self) -> tuple[BuildRecoveryResult, ...]:
        async with self._lock:
            # Discovery errors propagate: there is no trusted batch to report.
            async with asyncio.timeout(self._timeout):
                async with self._sessions.begin() as session:
                    page = await BuildGuardHoldRetirementStore(session, installation=self._installation).read_pending(
                        after_event_id=self._after, through_event_id=self._through, limit=self._batch_size)
                if not page.publications and self._through is not None:
                    self._after, self._through = 0, None
                    async with self._sessions.begin() as session:
                        page = await BuildGuardHoldRetirementStore(session, installation=self._installation).read_pending(limit=self._batch_size)
            self._through = page.through_event_id
            results: list[BuildRecoveryResult] = []
            for publication in page.publications:
                failure: Literal["timeout", "transport", "authority", "database"]
                try:
                    async with asyncio.timeout(self._timeout):
                        receipt = await self._retire(publication)
                    result = BuildRecoveryResult(publication.event_id, publication.release.binding.intent_id,
                        "retired" if receipt is not None else "unavailable", receipt)
                except TimeoutError:
                    failure = "timeout"
                    result = BuildRecoveryResult(publication.event_id, publication.release.binding.intent_id, "failed", failure=failure)
                except (DemandPublishError, ValueError, DBAPIError) as exc:
                    failure = "transport" if isinstance(exc, DemandPublishError) else "database" if isinstance(exc, DBAPIError) else "authority"
                    result = BuildRecoveryResult(publication.event_id, publication.release.binding.intent_id, "failed", failure=failure)
                # Cancellation propagates. A restart still rediscovers this event.
                self._after = publication.event_id
                results.append(result)
            if not page.publications or self._after == self._through:
                self._after, self._through = 0, None
            return tuple(results)

    async def _retire(self, publication: PublishableExecutableProtectedReleaseV2) -> RetiredBuildHoldV1 | None:
        witness = await self._manager.get_final_release_witness(publication.release.binding.intent_id)
        if witness is None:
            return None
        if not isinstance(witness, ExecutableFinalReleaseWitnessV2):
            raise ValueError("build recovery requires a typed manager release witness")
        witness = ExecutableFinalReleaseWitnessV2.model_validate_json(witness.model_dump_json())
        if (witness.release.binding != publication.release.binding
            or witness.protected_release != publication.release
            or witness.protected_acknowledgement_sha256 != publication.publication_digest):
            raise ValueError("build recovery manager witness does not match pending authority")
        if publication.event_kind in {"withdrawn", "released"}:
            terminal = await self._manager.get_build_terminal_inventory_evidence(publication.release.binding.intent_id)
            if terminal is None:
                return None
            if not isinstance(terminal, ExecutableTerminalInventoryEvidenceV3):
                raise ValueError("build recovery requires typed native terminal evidence")
            terminal = ExecutableTerminalInventoryEvidenceV3.model_validate_json(terminal.model_dump_json())
            if (terminal.binding != witness.release.binding or terminal.inventory_sequence != witness.release.inventory_sequence
                or terminal.record.physical_kind != "slurm-job" or witness.release.terminal_kind != "slurm-job"
                or terminal.record.physical_identity != witness.release.terminal_identity
                or terminal.record.terminal_evidence_sha256 != witness.release.terminal_evidence_sha256):
                raise ValueError("build recovery terminal evidence differs from exact manager release")
            async with self._sessions.begin() as session:
                await BuildGuardTerminalStore(session, installation=self._installation).import_evidence(terminal)
        async with self._sessions.begin() as session:
            return await BuildGuardHoldRetirementStore(session, installation=self._installation).retire(witness)
