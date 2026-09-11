"""Settle disappeared registered builders before protected/final release."""

import asyncio
from dataclasses import dataclass
from typing import Literal, Protocol
from uuid import UUID, uuid5

from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from loom_capacity_agent.admission import ExecutableDrainRequestV2, ExecutableReleaseRequestV2
from loom_capacity_agent.client import DemandPublishError
from loom_capacity_build_guard.execution_store import BuildGuardExecutionStore
from loom_capacity_build_guard.installation_store import RetainedBuildInstallation
from loom_capacity_build_guard.terminal_discovery import (
    BuildGuardTerminalDiscovery,
    PendingNativeWorkerV1,
)
from loom_capacity_build_guard.terminal_store import BuildGuardTerminalStore
from loom_capacity_manager.executable_contracts import canonical_executable_digest
from loom_capacity_manager.typed_inventory_contracts import ExecutableTerminalInventoryEvidenceV3


class BuildTerminalManager(Protocol):
    async def get_build_terminal_inventory_evidence(self, intent_id: UUID) -> ExecutableTerminalInventoryEvidenceV3 | None: ...


@dataclass(frozen=True, slots=True)
class BuildTerminalRecoveryResult:
    event_id: int
    intent_id: UUID
    state: Literal["released", "unavailable", "failed"]
    failure: Literal["timeout", "transport", "authority", "database"] | None = None


class BuildTerminalRecoveryCoordinator:
    """Finite retry sweep; neither deadlines nor scan cursors prove worker death."""

    def __init__(self, *, session_factory: async_sessionmaker[AsyncSession], installation: RetainedBuildInstallation,
        manager: BuildTerminalManager, batch_size: int = 16, item_timeout_seconds: float = 5,
    ) -> None:
        if type(batch_size) is not int or not 1 <= batch_size <= 64:
            raise ValueError("native terminal recovery batch size must be between one and 64")
        if (isinstance(item_timeout_seconds, bool) or not isinstance(item_timeout_seconds, (int, float))
            or not 0.05 <= item_timeout_seconds <= 30):
            raise ValueError("native terminal recovery timeout must be between 0.05 and 30 seconds")
        self._sessions = session_factory
        self._installation = installation
        self._manager = manager
        self._batch_size = batch_size
        self._timeout = item_timeout_seconds
        self._after = 0
        self._through: int | None = None
        self._lock = asyncio.Lock()

    async def reconcile(self) -> tuple[BuildTerminalRecoveryResult, ...]:
        async with self._lock:
            async with asyncio.timeout(self._timeout):
                async with self._sessions.begin() as session:
                    page = await BuildGuardTerminalDiscovery(session, installation=self._installation).read_pending(
                        after_event_id=self._after, through_event_id=self._through, limit=self._batch_size)
                if not page.workers and self._through is not None:
                    self._after, self._through = 0, None
                    async with self._sessions.begin() as session:
                        page = await BuildGuardTerminalDiscovery(session, installation=self._installation).read_pending(limit=self._batch_size)
            self._through = page.through_event_id
            results = []
            for worker in page.workers:
                try:
                    async with asyncio.timeout(self._timeout):
                        released = await self._settle(worker)
                    result = BuildTerminalRecoveryResult(worker.event_id, worker.binding.intent_id,
                        "released" if released else "unavailable")
                except TimeoutError:
                    result = BuildTerminalRecoveryResult(worker.event_id, worker.binding.intent_id, "failed", "timeout")
                except (DemandPublishError, ValueError, DBAPIError) as exc:
                    failure: Literal["transport", "authority", "database"] = (
                        "transport" if isinstance(exc, DemandPublishError) else "database" if isinstance(exc, DBAPIError) else "authority")
                    result = BuildTerminalRecoveryResult(worker.event_id, worker.binding.intent_id, "failed", failure)
                # Cancellation propagates; cursor progress is never authority.
                self._after = worker.event_id
                results.append(result)
            if not page.workers or self._after == self._through:
                self._after, self._through = 0, None
            return tuple(results)

    async def _settle(self, worker: PendingNativeWorkerV1) -> bool:
        terminal = await self._manager.get_build_terminal_inventory_evidence(worker.binding.intent_id)
        if terminal is None:
            return False
        if type(terminal) is not ExecutableTerminalInventoryEvidenceV3:
            raise ValueError("native recovery requires typed terminal evidence")
        terminal = ExecutableTerminalInventoryEvidenceV3.model_validate_json(terminal.model_dump_json())
        if terminal.binding != worker.binding:
            raise ValueError("native recovery terminal binding changed")
        digest = canonical_executable_digest(terminal)
        async with self._sessions.begin() as session:
            await BuildGuardTerminalStore(session, installation=self._installation).import_evidence(terminal)
        if worker.claim is not None:
            async with self._sessions.begin() as session:
                await BuildGuardTerminalStore(session, installation=self._installation).settle_interrupted(
                    worker.claim, terminal_inventory_sha256=digest)
        async with self._sessions.begin() as session:
            store = BuildGuardExecutionStore(session, installation=self._installation)
            observed = await store.observe_intent(worker.binding)
            if observed.release is not None:
                return True
            if (observed.worker_id != worker.worker_id or observed.worker_incarnation != worker.worker_incarnation
                or observed.claim_high_water != int(worker.claim is not None)):
                raise ValueError("native recovery registered worker changed")
            if observed.drain is None:
                await store.begin_drain(ExecutableDrainRequestV2(binding=worker.binding,
                    operation_id=uuid5(worker.operation_id, "native-terminal-drain"),
                    worker_id=worker.worker_id, worker_incarnation=worker.worker_incarnation,
                    expected_claim_high_water=observed.claim_high_water, drain_epoch=3))
        # Re-observe after drain commit: a worker may have won release meanwhile.
        async with self._sessions.begin() as session:
            store = BuildGuardExecutionStore(session, installation=self._installation)
            observed = await store.observe_intent(worker.binding)
            if observed.release is not None:
                return True
            await BuildGuardTerminalStore(session, installation=self._installation).release_terminal_worker(
                ExecutableReleaseRequestV2(binding=worker.binding,
                    operation_id=uuid5(worker.operation_id, "native-terminal-release"),
                    reporter_incarnation=self._installation.document.reporter_incarnation,
                    bootstrap_registration_epoch=1, protected_registration_epoch=2,
                    expected_claim_high_water=observed.claim_high_water, release_epoch=4),
                terminal_inventory_sha256=digest)
        return True
