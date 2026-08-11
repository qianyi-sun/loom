"""Journal-first, zero-execution pool executor for protocol validation."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import TypeVar
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession

from loom_capacity_executor.journal import ExecutorJournal, JournalRegressionError
from loom_capacity_manager.contracts import StrictV1Model
from loom_capacity_manager.grant_contracts import (
    DryRunBootstrapRegistrationV1,
    DryRunExecutorHeartbeatV1,
    DryRunExecutorInventoryV1,
    DryRunIntentCloseV1,
    DryRunPartialReleaseV1,
    DryRunPermitConsumptionV1,
    DryRunReservationAcceptanceV1,
    canonical_grant_digest,
)
from loom_capacity_manager.grant_store import (
    AcceptedReservation,
    CapacityGrantStore,
    ClosingIntent,
    ConsumedLaunchPermit,
    HeartbeatedExecutor,
    IngestedExecutorInventory,
    IntentReady,
    ReleasedReservationShapes,
)
from loom_capacity_manager.store import CapacityStoreError

_ResultT = TypeVar("_ResultT")


@dataclass(frozen=True, slots=True)
class DryRunExecutorBinding:
    """The exact central authority and pool generation owned by one process."""

    authority_incarnation: UUID
    writer_epoch: int
    executor_id: str
    executor_incarnation: UUID
    pool_id: str
    pool_generation: int

    def __post_init__(self) -> None:
        if self.writer_epoch <= 0 or self.pool_generation <= 0:
            raise ValueError("executor binding epochs must be positive")


class DryRunPoolExecutor:
    """Drive central protocol methods without exposing any scheduler operation."""

    def __init__(
        self,
        binding: DryRunExecutorBinding,
        journal: ExecutorJournal,
        grant_store: CapacityGrantStore,
    ) -> None:
        self.binding = binding
        self.journal = journal
        self.grant_store = grant_store

    def _assert_executor(self, contract: object) -> None:
        if (
            getattr(contract, "executor_id", None) != self.binding.executor_id
            or getattr(contract, "executor_incarnation", None) != self.binding.executor_incarnation
            or getattr(contract, "executable", None) is not False
        ):
            raise ValueError("dry-run command executor binding changed")

    async def _journaled_command(
        self,
        *,
        session: AsyncSession,
        contract: StrictV1Model,
        event: str,
        object_kind: str,
        object_id: str,
        operation: Callable[[], Awaitable[_ResultT]],
    ) -> _ResultT:
        self._assert_executor(contract)
        await self._assert_central_checkpoint(session)
        digest = canonical_grant_digest(contract)
        requested_event = f"{event}-requested"
        confirmed_event = f"{event}-confirmed"
        pending = self.journal.pending_requests()
        same_pending = (
            len(pending) == 1
            and pending[0].event_kind == requested_event
            and pending[0].object_kind == object_kind
            and pending[0].object_id == object_id
            and pending[0].payload_digest == digest
        )
        if pending and not same_pending:
            raise JournalRegressionError(
                "another journal-first executor command remains unresolved"
            )
        latest = self.journal.latest(object_kind, object_id)
        if not same_pending and not (
            latest is not None
            and latest.event_kind == confirmed_event
            and latest.payload_digest == digest
        ):
            self.journal.append(
                requested_event,
                digest,
                object_kind=object_kind,
                object_id=object_id,
            )
        try:
            result = await operation()
        except CapacityStoreError:
            latest = self.journal.latest(object_kind, object_id)
            if not (
                latest is not None
                and latest.event_kind == f"{event}-rejected"
                and latest.payload_digest == digest
            ):
                self.journal.append(
                    f"{event}-rejected",
                    digest,
                    object_kind=object_kind,
                    object_id=object_id,
                )
            raise
        latest = self.journal.latest(object_kind, object_id)
        if not (
            latest is not None
            and latest.event_kind == confirmed_event
            and latest.payload_digest == digest
        ):
            self.journal.append(
                confirmed_event,
                digest,
                object_kind=object_kind,
                object_id=object_id,
            )
        return result

    async def _assert_central_checkpoint(self, session: AsyncSession) -> None:
        checkpoint = await self.grant_store.executor_checkpoint(
            session,
            authority_incarnation=self.binding.authority_incarnation,
            writer_epoch=self.binding.writer_epoch,
            executor_id=self.binding.executor_id,
            executor_incarnation=self.binding.executor_incarnation,
            pool_id=self.binding.pool_id,
            pool_generation=self.binding.pool_generation,
        )
        self.journal.assert_covers(
            checkpoint.journal_sequence,
            checkpoint.journal_digest,
        )
        if self.journal.head.sequence < checkpoint.command_sequence:
            raise JournalRegressionError("local journal is behind central command high-water")

    async def accept_reservation(
        self,
        session: AsyncSession,
        acceptance: DryRunReservationAcceptanceV1,
    ) -> AcceptedReservation:
        return await self._journaled_command(
            session=session,
            contract=acceptance,
            event="reservation-accept",
            object_kind="tranche",
            object_id=str(acceptance.tranche_id),
            operation=lambda: self.grant_store.accept_reservation(session, acceptance),
        )

    async def register_bootstrap(
        self,
        session: AsyncSession,
        registration: DryRunBootstrapRegistrationV1,
    ) -> IntentReady:
        return await self._journaled_command(
            session=session,
            contract=registration,
            event="bootstrap-register",
            object_kind="intent",
            object_id=str(registration.intent_id),
            operation=lambda: self.grant_store.register_bootstrap(session, registration),
        )

    async def consume_launch_permit(
        self,
        session: AsyncSession,
        consumption: DryRunPermitConsumptionV1,
    ) -> ConsumedLaunchPermit:
        return await self._journaled_command(
            session=session,
            contract=consumption,
            event="permit-consume",
            object_kind="intent",
            object_id=str(consumption.intent_id),
            operation=lambda: self.grant_store.consume_launch_permit(session, consumption),
        )

    async def begin_intent_close(
        self,
        session: AsyncSession,
        close: DryRunIntentCloseV1,
    ) -> ClosingIntent:
        return await self._journaled_command(
            session=session,
            contract=close,
            event="intent-close",
            object_kind="intent",
            object_id=str(close.intent_id),
            operation=lambda: self.grant_store.begin_intent_close(session, close),
        )

    async def release_shapes(
        self,
        session: AsyncSession,
        release: DryRunPartialReleaseV1,
    ) -> ReleasedReservationShapes:
        return await self._journaled_command(
            session=session,
            contract=release,
            event="reservation-release",
            object_kind="tranche",
            object_id=str(release.tranche_id),
            operation=lambda: self.grant_store.release_shapes(session, release),
        )

    async def heartbeat(
        self,
        session: AsyncSession,
        *,
        heartbeat_sequence: int,
    ) -> HeartbeatedExecutor:
        checkpoint = await self.grant_store.executor_checkpoint(
            session,
            authority_incarnation=self.binding.authority_incarnation,
            writer_epoch=self.binding.writer_epoch,
            executor_id=self.binding.executor_id,
            executor_incarnation=self.binding.executor_incarnation,
            pool_id=self.binding.pool_id,
            pool_generation=self.binding.pool_generation,
        )
        self.journal.assert_covers(
            checkpoint.journal_sequence,
            checkpoint.journal_digest,
        )
        if self.journal.head.sequence < checkpoint.command_sequence:
            raise JournalRegressionError("local journal is behind central command high-water")
        head = self.journal.head
        heartbeat = DryRunExecutorHeartbeatV1(
            authority_incarnation=self.binding.authority_incarnation,
            writer_epoch=self.binding.writer_epoch,
            executor_id=self.binding.executor_id,
            executor_incarnation=self.binding.executor_incarnation,
            pool_id=self.binding.pool_id,
            pool_generation=self.binding.pool_generation,
            heartbeat_sequence=heartbeat_sequence,
            journal_sequence=head.sequence,
            journal_digest=head.digest,
            journal_checkpoint_sequence=checkpoint.journal_sequence,
            journal_checkpoint_digest=checkpoint.journal_digest,
        )
        return await self.grant_store.heartbeat_executor(session, heartbeat)

    async def ingest_inventory(
        self,
        session: AsyncSession,
        inventory: DryRunExecutorInventoryV1,
    ) -> IngestedExecutorInventory:
        self._assert_executor(inventory)
        checkpoint = await self.grant_store.executor_checkpoint(
            session,
            authority_incarnation=self.binding.authority_incarnation,
            writer_epoch=self.binding.writer_epoch,
            executor_id=self.binding.executor_id,
            executor_incarnation=self.binding.executor_incarnation,
            pool_id=self.binding.pool_id,
            pool_generation=self.binding.pool_generation,
        )
        self.journal.assert_covers(
            checkpoint.journal_sequence,
            checkpoint.journal_digest,
        )
        if self.journal.head.sequence < checkpoint.command_sequence:
            raise JournalRegressionError("local journal is behind central command high-water")
        head = self.journal.head
        if (
            inventory.authority_incarnation != self.binding.authority_incarnation
            or inventory.writer_epoch != self.binding.writer_epoch
            or inventory.pool_id != self.binding.pool_id
            or inventory.pool_generation != self.binding.pool_generation
            or inventory.journal_sequence != head.sequence
            or inventory.journal_digest != head.digest
        ):
            raise ValueError("dry-run inventory executor binding changed")
        return await self.grant_store.ingest_executor_inventory(
            session,
            inventory.model_copy(
                update={
                    "journal_checkpoint_sequence": checkpoint.journal_sequence,
                    "journal_checkpoint_digest": checkpoint.journal_digest,
                }
            ),
        )


__all__ = ["DryRunExecutorBinding", "DryRunPoolExecutor"]
