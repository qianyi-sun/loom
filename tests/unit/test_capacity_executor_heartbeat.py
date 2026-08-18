from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest

from loom_capacity_executor.heartbeat import ExecutableHeartbeatError, ExecutableHeartbeatLoop
from loom_capacity_executor.journal import ExecutorJournal, JournalRegressionError
from loom_capacity_manager.executable_contracts import (
    ExecutableExecutorHeartbeatV2,
    ExecutableExecutorRegistrationV2,
    ExecutionAuthorityV2,
)
from tests.unit.test_capacity_executor_launch_renderer import launch_context_fixture


class RecordingHeartbeatClient:
    def __init__(self) -> None:
        self.heartbeats: list[ExecutableExecutorHeartbeatV2] = []
        self.journal_sequence = 0
        self.journal_digest = "0" * 64

    async def executable_checkpoint(self) -> SimpleNamespace:
        return SimpleNamespace(
            journal_sequence=self.journal_sequence,
            journal_digest=self.journal_digest,
            command_sequence=0,
        )

    async def heartbeat_executable_executor(
        self,
        heartbeat: ExecutableExecutorHeartbeatV2,
    ) -> SimpleNamespace:
        self.heartbeats.append(heartbeat)
        self.journal_sequence = heartbeat.journal_sequence
        self.journal_digest = heartbeat.journal_digest
        return SimpleNamespace(
            heartbeat_sequence=heartbeat.heartbeat_sequence,
            lease_expires_at=datetime.now(UTC) + timedelta(minutes=5),
            replayed=False,
            executable=True,
        )


def _registration() -> ExecutableExecutorRegistrationV2:
    launch = launch_context_fixture()
    active = ExecutionAuthorityV2.model_validate(
        launch.binding.execution.model_dump(exclude={"allocation_epoch"})
    )
    return ExecutableExecutorRegistrationV2(
        execution=active,
        executor_id=launch.binding.executor_id,
        executor_incarnation=launch.binding.executor_incarnation,
        pool_id=launch.binding.pool_id,
        pool_generation=launch.binding.pool_generation,
        signing_key_id=launch.ownership_key.signing_key_id,
        signing_key_sha256=launch.ownership_key.public_key_sha256,
        local_authority_sha256="a" * 64,
        controller_authority_sha256=launch.controller_authority.controller_authority_sha256,
    )


@pytest.mark.asyncio
async def test_heartbeat_replays_after_active_authority_json_round_trip(
    tmp_path: Path,
) -> None:
    registration = _registration()
    client = RecordingHeartbeatClient()
    journal = ExecutorJournal(tmp_path / "executor.journal")
    with journal:
        loop = ExecutableHeartbeatLoop(registration, journal, client)

        first = await loop.heartbeat()
        second = await loop.heartbeat()

    assert (first.heartbeat_sequence, second.heartbeat_sequence) == (1, 2)
    assert [item.heartbeat_sequence for item in client.heartbeats] == [1, 2]


@pytest.mark.asyncio
async def test_prepared_heartbeat_journal_continues_at_exact_active_epoch(
    tmp_path: Path,
) -> None:
    active = _registration()
    prepared = active.model_copy(
        update={
            "execution": active.execution.model_copy(
                update={
                    "execution_state": "prepared",
                    "executable_new_capacity_ceiling": 0,
                    "executable_new_capacity_rate_per_minute": 0,
                }
            )
        }
    )
    client = RecordingHeartbeatClient()
    journal = ExecutorJournal(tmp_path / "executor.journal")
    with journal:
        first = await ExecutableHeartbeatLoop(prepared, journal, client).heartbeat()
        second = await ExecutableHeartbeatLoop(active, journal, client).heartbeat()

    assert first.execution.execution_state == "prepared"
    assert second.execution == active.execution
    assert (first.heartbeat_sequence, second.heartbeat_sequence) == (1, 2)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("field", "changed"),
    (
        ("writer_epoch", 3),
        ("configuration_epoch", 2),
        ("execution_epoch", 2),
        ("execution_manifest_sha256", "f" * 64),
        ("trusted_fleet_release_sha256", "e" * 64),
    ),
)
async def test_prepared_heartbeat_journal_rejects_changed_active_epoch(
    tmp_path: Path,
    field: str,
    changed: object,
) -> None:
    active = _registration()
    prepared = active.model_copy(
        update={
            "execution": active.execution.model_copy(
                update={
                    "execution_state": "prepared",
                    "executable_new_capacity_ceiling": 0,
                    "executable_new_capacity_rate_per_minute": 0,
                }
            )
        }
    )
    changed_active = active.model_copy(
        update={"execution": active.execution.model_copy(update={field: changed})}
    )
    client = RecordingHeartbeatClient()
    journal = ExecutorJournal(tmp_path / f"executor-{field}.journal")
    with journal:
        await ExecutableHeartbeatLoop(prepared, journal, client).heartbeat()
        with pytest.raises(JournalRegressionError, match="binding changed"):
            await ExecutableHeartbeatLoop(changed_active, journal, client).heartbeat()


@pytest.mark.asyncio
async def test_heartbeat_confirmation_journals_exact_receipt_evidence(
    tmp_path: Path,
) -> None:
    lease = datetime(2026, 8, 13, 16, 5, tzinfo=UTC)

    class FixedReceiptClient(RecordingHeartbeatClient):
        async def heartbeat_executable_executor(
            self,
            heartbeat: ExecutableExecutorHeartbeatV2,
        ) -> SimpleNamespace:
            self.heartbeats.append(heartbeat)
            return SimpleNamespace(
                heartbeat_sequence=heartbeat.heartbeat_sequence,
                lease_expires_at=lease,
                replayed=False,
                executable=True,
            )

    registration = _registration()
    client = FixedReceiptClient()
    journal = ExecutorJournal(tmp_path / "executor.journal")
    with journal:
        loop = ExecutableHeartbeatLoop(registration, journal, client)

        heartbeat = await loop.heartbeat()
        latest = journal.latest("heartbeat", str(registration.executor_incarnation))

    assert latest is not None
    assert latest.event_kind == "heartbeat-confirmed"
    payload = latest.durable_payload()
    assert payload is not None
    document = json.loads(payload)
    assert document["heartbeat"]["heartbeat_sequence"] == heartbeat.heartbeat_sequence
    assert document["receipt"]["heartbeat_sequence"] == heartbeat.heartbeat_sequence
    assert (
        datetime.fromisoformat(document["receipt"]["lease_expires_at"].replace("Z", "+00:00"))
        == lease
    )
    assert document["receipt"]["replayed"] is False
    assert document["receipt"]["executable"] is True


@pytest.mark.asyncio
async def test_heartbeat_replay_fences_changed_lease_after_receipt_journaled_crash(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    first_lease = datetime(2026, 8, 13, 16, 5, tzinfo=UTC)
    changed_lease = datetime(2026, 8, 13, 16, 10, tzinfo=UTC)

    class ChangedReplayClient(RecordingHeartbeatClient):
        def __init__(self) -> None:
            super().__init__()
            self.receipts = [
                SimpleNamespace(
                    heartbeat_sequence=1,
                    lease_expires_at=first_lease,
                    replayed=False,
                    executable=True,
                ),
                SimpleNamespace(
                    heartbeat_sequence=1,
                    lease_expires_at=changed_lease,
                    replayed=True,
                    executable=True,
                ),
            ]

        async def heartbeat_executable_executor(
            self,
            heartbeat: ExecutableExecutorHeartbeatV2,
        ) -> SimpleNamespace:
            self.heartbeats.append(heartbeat)
            receipt = self.receipts.pop(0)
            return SimpleNamespace(
                heartbeat_sequence=heartbeat.heartbeat_sequence,
                lease_expires_at=receipt.lease_expires_at,
                replayed=receipt.replayed,
                executable=receipt.executable,
            )

    registration = _registration()
    client = ChangedReplayClient()
    journal = ExecutorJournal(tmp_path / "executor.journal")
    with journal:
        loop = ExecutableHeartbeatLoop(registration, journal, client)
        original_append = journal.append

        def crash_before_confirm(
            event_kind: str,
            payload_digest: str,
            *,
            object_kind: str,
            object_id: str,
            payload: bytes | None = None,
        ):
            if event_kind == "heartbeat-confirmed":
                raise RuntimeError("crash before heartbeat confirmation")
            return original_append(
                event_kind,
                payload_digest,
                object_kind=object_kind,
                object_id=object_id,
                payload=payload,
            )

        monkeypatch.setattr(journal, "append", crash_before_confirm)
        with pytest.raises(RuntimeError, match="heartbeat confirmation"):
            await loop.heartbeat()
        monkeypatch.setattr(journal, "append", original_append)

        with pytest.raises(ExecutableHeartbeatError, match="receipt changed"):
            await loop.heartbeat()
