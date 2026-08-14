"""Journal-first executable executor heartbeat loop."""

from __future__ import annotations

from datetime import datetime
from typing import Any, cast

from loom_capacity_executor.journal import ExecutorJournal, JournalRecord, JournalRegressionError
from loom_capacity_manager.executable_contracts import (
    ExecutableExecutorHeartbeatV2,
    ExecutableExecutorRegistrationV2,
    canonical_executable_bytes,
    canonical_executable_digest,
)

_ZERO_DIGEST = "0" * 64


class ExecutableHeartbeatError(RuntimeError):
    """A heartbeat request, replay, or receipt failed an exact executor fence."""


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
                1 if record is None else self._heartbeat_from_record(record).heartbeat_sequence + 1
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
        receipt = await self.client.heartbeat_executable_executor(heartbeat)
        self._assert_receipt(heartbeat, receipt)
        self.journal.append(
            "heartbeat-confirmed",
            digest,
            object_kind="heartbeat",
            object_id=self._object_id,
            payload=payload,
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

    def _assert_heartbeat_binding(self, heartbeat: ExecutableExecutorHeartbeatV2) -> None:
        if (
            self._execution_context(heartbeat.execution)
            != self._execution_context(self.registration.execution)
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
    def _assert_receipt(heartbeat: ExecutableExecutorHeartbeatV2, receipt: Any) -> None:
        lease = getattr(receipt, "lease_expires_at", None)
        if (
            getattr(receipt, "heartbeat_sequence", None) != heartbeat.heartbeat_sequence
            or not isinstance(lease, datetime)
            or lease.tzinfo is None
            or lease.utcoffset() is None
        ):
            raise ExecutableHeartbeatError("heartbeat receipt changed")


__all__ = ["ExecutableHeartbeatError", "ExecutableHeartbeatLoop"]
