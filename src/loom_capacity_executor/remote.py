"""Journal-first remote executor for zero-capacity controller rehearsals."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import TypeVar

from loom_capacity_executor.client import (
    AcceptedReservationReceiptV1,
    CapacityExecutorClient,
    ClosingIntentReceiptV1,
    ConsumedPermitReceiptV1,
    ExecutorCheckpointReceiptV1,
    ExecutorHeartbeatReceiptV1,
    ExecutorInventoryReceiptV1,
    ExecutorRejectedError,
    IntentReadyReceiptV1,
    ReleasedShapesReceiptV1,
)
from loom_capacity_executor.dry_run import DryRunExecutorBinding
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

_ResultT = TypeVar("_ResultT")


class RemoteDryRunPoolExecutor:
    """Exercise the real manager boundary without any scheduler capability."""

    def __init__(
        self,
        binding: DryRunExecutorBinding,
        journal: ExecutorJournal,
        client: CapacityExecutorClient,
    ) -> None:
        if client.binding != binding:
            raise ValueError("executor client binding differs from local binding")
        self.binding = binding
        self.journal = journal
        self.client = client

    def _assert_executor(self, contract: object) -> None:
        if (
            getattr(contract, "executor_id", None) != self.binding.executor_id
            or getattr(contract, "executor_incarnation", None) != self.binding.executor_incarnation
            or getattr(contract, "executable", None) is not False
        ):
            raise ValueError("dry-run command executor binding changed")

    async def _assert_central_checkpoint(self) -> ExecutorCheckpointReceiptV1:
        checkpoint = await self.client.checkpoint()
        self.journal.assert_covers(
            checkpoint.journal_sequence,
            checkpoint.journal_digest,
        )
        if self.journal.head.sequence < checkpoint.command_sequence:
            raise JournalRegressionError("local journal is behind central command high-water")
        return checkpoint

    async def _journaled_command(
        self,
        *,
        contract: StrictV1Model,
        event: str,
        object_kind: str,
        object_id: str,
        operation: Callable[[], Awaitable[_ResultT]],
    ) -> _ResultT:
        self._assert_executor(contract)
        await self._assert_central_checkpoint()
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
        except ExecutorRejectedError:
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

    async def accept_reservation(
        self,
        acceptance: DryRunReservationAcceptanceV1,
    ) -> AcceptedReservationReceiptV1:
        return await self._journaled_command(
            contract=acceptance,
            event="reservation-accept",
            object_kind="tranche",
            object_id=str(acceptance.tranche_id),
            operation=lambda: self.client.accept_reservation(acceptance),
        )

    async def register_bootstrap(
        self,
        registration: DryRunBootstrapRegistrationV1,
    ) -> IntentReadyReceiptV1:
        return await self._journaled_command(
            contract=registration,
            event="bootstrap-register",
            object_kind="intent",
            object_id=str(registration.intent_id),
            operation=lambda: self.client.register_bootstrap(registration),
        )

    async def consume_launch_permit(
        self,
        consumption: DryRunPermitConsumptionV1,
    ) -> ConsumedPermitReceiptV1:
        return await self._journaled_command(
            contract=consumption,
            event="permit-consume",
            object_kind="intent",
            object_id=str(consumption.intent_id),
            operation=lambda: self.client.consume_launch_permit(consumption),
        )

    async def begin_intent_close(
        self,
        close: DryRunIntentCloseV1,
    ) -> ClosingIntentReceiptV1:
        return await self._journaled_command(
            contract=close,
            event="intent-close",
            object_kind="intent",
            object_id=str(close.intent_id),
            operation=lambda: self.client.begin_intent_close(close),
        )

    async def release_shapes(
        self,
        release: DryRunPartialReleaseV1,
    ) -> ReleasedShapesReceiptV1:
        return await self._journaled_command(
            contract=release,
            event="reservation-release",
            object_kind="tranche",
            object_id=str(release.tranche_id),
            operation=lambda: self.client.release_shapes(release),
        )

    async def heartbeat(
        self,
        *,
        heartbeat_sequence: int,
    ) -> ExecutorHeartbeatReceiptV1:
        checkpoint = await self._assert_central_checkpoint()
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
        return await self.client.heartbeat(heartbeat)

    async def ingest_inventory(
        self,
        inventory: DryRunExecutorInventoryV1,
    ) -> ExecutorInventoryReceiptV1:
        self._assert_executor(inventory)
        checkpoint = await self._assert_central_checkpoint()
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
        return await self.client.inventory(
            inventory.model_copy(
                update={
                    "journal_checkpoint_sequence": checkpoint.journal_sequence,
                    "journal_checkpoint_digest": checkpoint.journal_digest,
                }
            )
        )


__all__ = ["RemoteDryRunPoolExecutor"]
