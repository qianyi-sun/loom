"""Journal-first remote driver for the executable-v2 manager queue."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import TypeVar

from loom_capacity_executor.client import (
    AcceptedExecutableReservationReceiptV2,
    ClosingExecutableIntentReceiptV2,
    ConsumedExecutablePermitReceiptV2,
    ExecutableCapacityExecutorClient,
    ExecutableCheckpointReceiptV2,
    ExecutablePoolWorkV2,
    ExecutorRejectedError,
    ProposedExecutableBootstrapReceiptV2,
    RecoveredExecutableSubmissionReceiptV2,
    ReleasedExecutableShapesReceiptV2,
)
from loom_capacity_executor.journal import ExecutorJournal, JournalRegressionError
from loom_capacity_manager.executable_contracts import (
    ExecutableBootstrapProposalV2,
    ExecutableExecutorRegistrationV2,
    ExecutableIntentCloseV2,
    ExecutablePartialReleaseV2,
    ExecutablePermitConsumptionV2,
    ExecutableReservationAcceptanceV2,
    ExecutableSubmissionRecoveryV2,
    StrictV2Model,
    canonical_executable_digest,
)

_ResultT = TypeVar("_ResultT")


class RemoteExecutablePoolExecutor:
    """Journal manager commands without embedding any scheduler mutation capability."""

    def __init__(
        self,
        registration: ExecutableExecutorRegistrationV2,
        journal: ExecutorJournal,
        client: ExecutableCapacityExecutorClient,
    ) -> None:
        if client.registration != registration:
            raise ValueError("executor client registration differs from local registration")
        self.registration = registration
        self.journal = journal
        self.client = client

    async def _assert_central_checkpoint(self) -> ExecutableCheckpointReceiptV2:
        checkpoint = await self.client.executable_checkpoint()
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
        contract: StrictV2Model,
        event: str,
        object_kind: str,
        object_id: str,
        operation: Callable[[], Awaitable[_ResultT]],
    ) -> _ResultT:
        await self._assert_central_checkpoint()
        digest = canonical_executable_digest(contract)
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
                "another journal-first executable command remains unresolved"
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

    async def next_work(self) -> ExecutablePoolWorkV2 | None:
        checkpoint = await self._assert_central_checkpoint()
        return await self.client.next_executable_work(checkpoint.command_sequence)

    async def accept_reservation(
        self,
        acceptance: ExecutableReservationAcceptanceV2,
    ) -> AcceptedExecutableReservationReceiptV2:
        return await self._journaled_command(
            contract=acceptance,
            event="reservation-accept",
            object_kind="tranche",
            object_id=str(acceptance.tranche_id),
            operation=lambda: self.client.accept_executable_reservation(acceptance),
        )

    async def propose_bootstrap(
        self,
        proposal: ExecutableBootstrapProposalV2,
    ) -> ProposedExecutableBootstrapReceiptV2:
        return await self._journaled_command(
            contract=proposal,
            event="bootstrap-propose",
            object_kind="intent",
            object_id=str(proposal.binding.intent_id),
            operation=lambda: self.client.propose_executable_bootstrap(proposal),
        )

    async def consume_launch_permit(
        self,
        consumption: ExecutablePermitConsumptionV2,
    ) -> ConsumedExecutablePermitReceiptV2:
        return await self._journaled_command(
            contract=consumption,
            event="permit-consume",
            object_kind="intent",
            object_id=str(consumption.binding.intent_id),
            operation=lambda: self.client.consume_executable_permit(consumption),
        )

    async def recover_submission(
        self,
        recovery: ExecutableSubmissionRecoveryV2,
    ) -> RecoveredExecutableSubmissionReceiptV2:
        return await self._journaled_command(
            contract=recovery,
            event="submission-recover",
            object_kind="intent",
            object_id=str(recovery.binding.intent_id),
            operation=lambda: self.client.recover_executable_submission(recovery),
        )

    async def close_intent(
        self,
        close: ExecutableIntentCloseV2,
    ) -> ClosingExecutableIntentReceiptV2:
        return await self._journaled_command(
            contract=close,
            event="intent-close",
            object_kind="intent",
            object_id=str(close.binding.intent_id),
            operation=lambda: self.client.close_executable_intent(close),
        )

    async def release_shapes(
        self,
        release: ExecutablePartialReleaseV2,
    ) -> ReleasedExecutableShapesReceiptV2:
        return await self._journaled_command(
            contract=release,
            event="reservation-release",
            object_kind="tranche",
            object_id=str(release.tranche_id),
            operation=lambda: self.client.release_executable_shapes(release),
        )


__all__ = ["RemoteExecutablePoolExecutor"]
