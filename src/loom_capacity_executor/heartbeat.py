"""Journal-first executable executor heartbeat loop."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Annotated, Any, Literal, cast

from pydantic import Field, field_validator, model_validator

from loom_capacity_executor.journal import ExecutorJournal, JournalRecord, JournalRegressionError
from loom_capacity_manager.contracts import Digest
from loom_capacity_manager.executable_contracts import (
    ExecutableExecutorHeartbeatV2,
    ExecutableExecutorRegistrationV2,
    StrictV2Model,
    canonical_executable_bytes,
    canonical_executable_digest,
    retained_prepared_activation_matches,
)

_ZERO_DIGEST = "0" * 64


class ExecutableHeartbeatError(RuntimeError):
    """A heartbeat request, replay, or receipt failed an exact executor fence."""


class ExecutableHeartbeatReceiptEvidenceV2(StrictV2Model):
    """The replay-stable manager receipt fields for one heartbeat."""

    heartbeat_sequence: Annotated[int, Field(gt=0, le=(1 << 63) - 1)]
    lease_expires_at: datetime
    replayed: bool
    executable: Literal[True]

    @field_validator("lease_expires_at")
    @classmethod
    def _lease_expires_at(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("heartbeat receipt lease must be timezone-aware")
        return value.astimezone(UTC)


class ExecutableHeartbeatConfirmationV2(StrictV2Model):
    """Durable receipt evidence for an accepted executable heartbeat."""

    heartbeat: ExecutableExecutorHeartbeatV2
    heartbeat_sha256: Digest
    receipt: ExecutableHeartbeatReceiptEvidenceV2
    receipt_sha256: Digest

    @model_validator(mode="after")
    def _digests_match(self) -> ExecutableHeartbeatConfirmationV2:
        if (
            self.heartbeat_sha256 != canonical_executable_digest(self.heartbeat)
            or self.receipt_sha256 != canonical_executable_digest(self.receipt)
            or self.receipt.heartbeat_sequence != self.heartbeat.heartbeat_sequence
        ):
            raise ValueError("heartbeat confirmation digest binding changed")
        return self


class ExecutableHeartbeatLoop:
    """Durably renew one executable executor lease with byte-for-byte replay."""

    def __init__(
        self,
        registration: ExecutableExecutorRegistrationV2,
        journal: ExecutorJournal,
        client: Any,
    ) -> None:
        if not isinstance(registration, ExecutableExecutorRegistrationV2):
            raise TypeError("heartbeat loop requires an executable registration")
        if not isinstance(journal, ExecutorJournal):
            raise TypeError("heartbeat loop requires a locked executor journal")
        self.registration = registration
        self.journal = journal
        self.client = client

    @property
    def _object_id(self) -> str:
        return str(self.registration.executor_incarnation)

    async def heartbeat(self, checkpoint: Any | None = None) -> ExecutableExecutorHeartbeatV2:
        """Send or replay one journaled heartbeat request."""

        record = self.journal.latest("heartbeat", self._object_id)
        if record is not None and record.event_kind == "heartbeat-requested":
            heartbeat = self._heartbeat_from_record(record)
            payload = record.durable_payload()
            if payload is None:
                raise JournalRegressionError("heartbeat request is absent from journal")
            digest = record.payload_digest
            expected_receipt: ExecutableHeartbeatReceiptEvidenceV2 | None = None
        elif record is not None and record.event_kind == "heartbeat-received":
            confirmation = self._confirmation_from_record(record)
            heartbeat = confirmation.heartbeat
            payload = canonical_executable_bytes(heartbeat)
            digest = confirmation.heartbeat_sha256
            expected_receipt = confirmation.receipt
        else:
            if record is not None and record.event_kind != "heartbeat-confirmed":
                raise JournalRegressionError("heartbeat journal state is invalid")
            pending = tuple(
                item
                for item in self.journal.pending_requests()
                if not (
                    item.object_kind == "heartbeat"
                    and item.object_id == self._object_id
                    and item.event_kind == "heartbeat-requested"
                )
            )
            if pending:
                raise JournalRegressionError("another executable command remains unresolved")
            sequence = (
                1
                if record is None
                else self._confirmation_from_record(record).heartbeat.heartbeat_sequence + 1
            )
            if checkpoint is None and sequence != 1:
                checkpoint = await self.client.executable_checkpoint()
            if checkpoint is not None:
                self.journal.assert_covers(
                    checkpoint.journal_sequence,
                    checkpoint.journal_digest,
                )
            selected = self.journal.head
            heartbeat = ExecutableExecutorHeartbeatV2(
                execution=self.registration.execution,
                executor_id=self.registration.executor_id,
                executor_incarnation=self.registration.executor_incarnation,
                pool_id=self.registration.pool_id,
                pool_generation=self.registration.pool_generation,
                heartbeat_sequence=sequence,
                journal_sequence=selected.sequence,
                journal_digest=selected.digest,
                journal_checkpoint_sequence=(
                    0 if checkpoint is None else checkpoint.journal_sequence
                ),
                journal_checkpoint_digest=(
                    _ZERO_DIGEST if checkpoint is None else checkpoint.journal_digest
                ),
            )
            payload = canonical_executable_bytes(heartbeat)
            digest = canonical_executable_digest(heartbeat)
            self.journal.append(
                "heartbeat-requested",
                digest,
                object_kind="heartbeat",
                object_id=self._object_id,
                payload=payload,
            )
            expected_receipt = None
        receipt = await self.client.heartbeat_executable_executor(heartbeat)
        receipt_evidence = self._assert_receipt(
            heartbeat,
            receipt,
            expected=expected_receipt,
        )
        confirmation = ExecutableHeartbeatConfirmationV2(
            heartbeat=heartbeat,
            heartbeat_sha256=digest,
            receipt=receipt_evidence,
            receipt_sha256=canonical_executable_digest(receipt_evidence),
        )
        confirmation_payload = canonical_executable_bytes(confirmation)
        confirmation_digest = canonical_executable_digest(confirmation)
        if expected_receipt is None:
            self.journal.append(
                "heartbeat-received",
                confirmation_digest,
                object_kind="heartbeat",
                object_id=self._object_id,
                payload=confirmation_payload,
            )
        self.journal.append(
            "heartbeat-confirmed",
            confirmation_digest,
            object_kind="heartbeat",
            object_id=self._object_id,
            payload=confirmation_payload,
        )
        return heartbeat

    def _heartbeat_from_record(self, record: JournalRecord) -> ExecutableExecutorHeartbeatV2:
        if record.object_kind != "heartbeat" or record.object_id != self._object_id:
            raise JournalRegressionError("heartbeat request object binding changed")
        payload = record.durable_payload()
        if payload is None:
            raise JournalRegressionError("heartbeat request is absent from journal")
        heartbeat = ExecutableExecutorHeartbeatV2.model_validate_json(payload)
        self._assert_heartbeat_binding(heartbeat)
        if record.payload_digest != canonical_executable_digest(heartbeat):
            raise JournalRegressionError("heartbeat request digest changed")
        return heartbeat

    def _confirmation_from_record(
        self,
        record: JournalRecord,
    ) -> ExecutableHeartbeatConfirmationV2:
        if record.object_kind != "heartbeat" or record.object_id != self._object_id:
            raise JournalRegressionError("heartbeat confirmation object binding changed")
        payload = record.durable_payload()
        if payload is None:
            raise JournalRegressionError("heartbeat confirmation is absent from journal")
        try:
            confirmation = ExecutableHeartbeatConfirmationV2.model_validate_json(payload)
        except ValueError as exc:
            raise JournalRegressionError("heartbeat confirmation is invalid") from exc
        self._assert_heartbeat_binding(confirmation.heartbeat)
        if record.payload_digest != canonical_executable_digest(confirmation):
            raise JournalRegressionError("heartbeat confirmation digest changed")
        return confirmation

    def _assert_heartbeat_binding(self, heartbeat: ExecutableExecutorHeartbeatV2) -> None:
        if (
            (
                self._execution_context(heartbeat.execution)
                != self._execution_context(self.registration.execution)
                and not retained_prepared_activation_matches(
                    heartbeat.execution,
                    self.registration.execution,
                )
            )
            or heartbeat.executor_id != self.registration.executor_id
            or heartbeat.executor_incarnation != self.registration.executor_incarnation
            or heartbeat.pool_id != self.registration.pool_id
            or heartbeat.pool_generation != self.registration.pool_generation
        ):
            raise JournalRegressionError("heartbeat request binding changed")

    @staticmethod
    def _execution_context(value: Any) -> dict[str, Any]:
        return cast(
            dict[str, Any],
            value.model_dump(
                mode="json",
                exclude={"allocation_epoch", "executable"},
                exclude_none=False,
            ),
        )

    @staticmethod
    def _assert_receipt(
        heartbeat: ExecutableExecutorHeartbeatV2,
        receipt: Any,
        *,
        expected: ExecutableHeartbeatReceiptEvidenceV2 | None,
    ) -> ExecutableHeartbeatReceiptEvidenceV2:
        try:
            evidence = ExecutableHeartbeatReceiptEvidenceV2.model_validate(
                {
                    "heartbeat_sequence": getattr(receipt, "heartbeat_sequence", None),
                    "lease_expires_at": getattr(receipt, "lease_expires_at", None),
                    "replayed": getattr(receipt, "replayed", None),
                    "executable": getattr(receipt, "executable", None),
                }
            )
        except ValueError as exc:
            raise ExecutableHeartbeatError("heartbeat receipt changed") from exc
        if evidence.heartbeat_sequence != heartbeat.heartbeat_sequence:
            raise ExecutableHeartbeatError("heartbeat receipt changed")
        if expected is not None and (
            evidence.heartbeat_sequence != expected.heartbeat_sequence
            or evidence.lease_expires_at != expected.lease_expires_at
            or evidence.executable != expected.executable
            or (expected.replayed and not evidence.replayed)
        ):
            raise ExecutableHeartbeatError("heartbeat receipt changed")
        return evidence


__all__ = [
    "ExecutableHeartbeatConfirmationV2",
    "ExecutableHeartbeatError",
    "ExecutableHeartbeatLoop",
    "ExecutableHeartbeatReceiptEvidenceV2",
]
