from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import UUID

import pytest

from loom_capacity_executor.dry_run import DryRunExecutorBinding, DryRunPoolExecutor
from loom_capacity_executor.journal import ExecutorJournal, JournalRegressionError
from loom_capacity_manager.grant_contracts import (
    DryRunExecutorHeartbeatV1,
    DryRunReservationAcceptanceV1,
    canonical_grant_digest,
)
from loom_capacity_manager.grant_store import (
    AcceptedReservation,
    ExecutorCheckpoint,
    GrantConflictError,
    HeartbeatedExecutor,
)


class _FakeGrantStore:
    def __init__(self) -> None:
        self.accept_calls = 0
        self.fail_accept_once = False
        self.reject_accept_once = False
        self.heartbeat: DryRunExecutorHeartbeatV1 | None = None
        self.checkpoint_sequence = 0
        self.checkpoint_digest = "0" * 64
        self.checkpoint_command_sequence = 0

    async def accept_reservation(
        self,
        session: Any,
        acceptance: DryRunReservationAcceptanceV1,
    ) -> AcceptedReservation:
        del session
        self.accept_calls += 1
        if self.fail_accept_once:
            self.fail_accept_once = False
            raise RuntimeError("simulated central outage")
        if self.reject_accept_once:
            self.reject_accept_once = False
            raise GrantConflictError("simulated definitive rejection")
        return AcceptedReservation(acceptance.tranche_id, (UUID(int=2),), False)

    async def heartbeat_executor(
        self,
        session: Any,
        heartbeat: DryRunExecutorHeartbeatV1,
    ) -> HeartbeatedExecutor:
        del session
        self.heartbeat = heartbeat
        return HeartbeatedExecutor(
            UUID(int=3),
            heartbeat.heartbeat_sequence,
            heartbeat.journal_sequence,
            None,  # type: ignore[arg-type]
            False,
        )

    async def executor_checkpoint(self, session: Any, **_binding: object) -> ExecutorCheckpoint:
        del session
        return ExecutorCheckpoint(
            executor_row_id=UUID(int=3),
            authority_incarnation=UUID(int=10),
            writer_epoch=4,
            executor_id="oldlab-executor",
            executor_incarnation=UUID(int=11),
            pool_id="oldlab",
            pool_generation=2,
            command_sequence=self.checkpoint_command_sequence,
            journal_sequence=self.checkpoint_sequence,
            journal_digest=self.checkpoint_digest,
            inventory_sequence=0,
            lease_expires_at=datetime.now(UTC),
        )


def _binding() -> DryRunExecutorBinding:
    return DryRunExecutorBinding(
        authority_incarnation=UUID(int=10),
        writer_epoch=4,
        executor_id="oldlab-executor",
        executor_incarnation=UUID(int=11),
        pool_id="oldlab",
        pool_generation=2,
    )


def _acceptance() -> DryRunReservationAcceptanceV1:
    return DryRunReservationAcceptanceV1(
        tranche_id=UUID(int=1),
        proposal_digest="a" * 64,
        executor_id="oldlab-executor",
        executor_incarnation=UUID(int=11),
        command_sequence=1,
    )


async def test_journal_first_command_recovers_without_duplicate_prepare(
    tmp_path: Path,
) -> None:
    path = tmp_path / "oldlab.journal"
    store = _FakeGrantStore()
    store.fail_accept_once = True
    acceptance = _acceptance()
    with ExecutorJournal(path) as journal:
        executor = DryRunPoolExecutor(_binding(), journal, store)  # type: ignore[arg-type]
        with pytest.raises(RuntimeError, match="central outage"):
            await executor.accept_reservation(object(), acceptance)  # type: ignore[arg-type]
        prepared = journal.latest("tranche", str(acceptance.tranche_id))
        assert prepared is not None
        assert prepared.event_kind == "reservation-accept-requested"
        assert prepared.payload_digest == canonical_grant_digest(acceptance)
        assert journal.head.sequence == 1

    with ExecutorJournal(path) as recovered:
        executor = DryRunPoolExecutor(_binding(), recovered, store)  # type: ignore[arg-type]
        result = await executor.accept_reservation(object(), acceptance)  # type: ignore[arg-type]
        assert result.tranche_id == acceptance.tranche_id
        assert recovered.head.sequence == 2
        assert (
            recovered.latest("tranche", str(acceptance.tranche_id)).event_kind
            == "reservation-accept-confirmed"
        )
    assert store.accept_calls == 2


async def test_journal_first_command_records_definitive_protocol_rejection(
    tmp_path: Path,
) -> None:
    store = _FakeGrantStore()
    store.reject_accept_once = True
    acceptance = _acceptance()
    with ExecutorJournal(tmp_path / "oldlab.journal") as journal:
        executor = DryRunPoolExecutor(_binding(), journal, store)  # type: ignore[arg-type]
        with pytest.raises(GrantConflictError, match="definitive rejection"):
            await executor.accept_reservation(object(), acceptance)  # type: ignore[arg-type]
        assert journal.pending_requests() == ()
        rejected = journal.latest("tranche", str(acceptance.tranche_id))
        assert rejected is not None
        assert rejected.event_kind == "reservation-accept-rejected"
        assert rejected.payload_digest == canonical_grant_digest(acceptance)


async def test_executor_binding_and_heartbeat_are_fail_closed(tmp_path: Path) -> None:
    path = tmp_path / "oldlab.journal"
    store = _FakeGrantStore()
    with ExecutorJournal(path) as journal:
        executor = DryRunPoolExecutor(_binding(), journal, store)  # type: ignore[arg-type]
        wrong = _acceptance().model_copy(update={"executor_id": "gb10-executor"})
        with pytest.raises(ValueError, match="executor binding"):
            await executor.accept_reservation(object(), wrong)  # type: ignore[arg-type]
        assert journal.head.sequence == 0

        await executor.accept_reservation(object(), _acceptance())  # type: ignore[arg-type]
        result = await executor.heartbeat(object(), heartbeat_sequence=1)  # type: ignore[arg-type]
        assert result.journal_sequence == journal.head.sequence
        assert store.heartbeat is not None
        assert store.heartbeat.journal_sequence == journal.head.sequence
        assert store.heartbeat.journal_digest == journal.head.digest


def test_permanent_v1_executor_modules_have_no_scheduler_or_process_execution_surface() -> None:
    executor_package = Path(__file__).parents[2] / "src/loom_capacity_executor"
    sources = tuple(
        executor_package / name for name in ("dry_run.py", "remote.py", "client.py", "journal.py")
    )
    forbidden = ("import subprocess", "create_subprocess", "sbatch", "squeue", "scancel")
    assert not any(
        token in source.read_text(encoding="utf-8") for source in sources for token in forbidden
    )


async def test_executor_refuses_a_local_journal_behind_central_checkpoint(
    tmp_path: Path,
) -> None:
    store = _FakeGrantStore()
    store.checkpoint_sequence = 1
    store.checkpoint_digest = "a" * 64
    with ExecutorJournal(tmp_path / "oldlab.journal") as journal:
        executor = DryRunPoolExecutor(_binding(), journal, store)  # type: ignore[arg-type]
        with pytest.raises(JournalRegressionError, match="behind central"):
            await executor.heartbeat(object(), heartbeat_sequence=1)  # type: ignore[arg-type]


async def test_executor_refuses_missing_command_records_above_checkpoint(
    tmp_path: Path,
) -> None:
    store = _FakeGrantStore()
    store.checkpoint_command_sequence = 1
    with ExecutorJournal(tmp_path / "oldlab.journal") as journal:
        executor = DryRunPoolExecutor(_binding(), journal, store)  # type: ignore[arg-type]
        with pytest.raises(JournalRegressionError, match="command high-water"):
            await executor.accept_reservation(object(), _acceptance())  # type: ignore[arg-type]
    assert store.accept_calls == 0
