from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest

from loom_capacity_executor.heartbeat import ExecutableHeartbeatLoop
from loom_capacity_executor.journal import ExecutorJournal
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
        )


@pytest.mark.asyncio
async def test_heartbeat_replays_after_active_authority_json_round_trip(
    tmp_path: Path,
) -> None:
    launch = launch_context_fixture()
    active = ExecutionAuthorityV2.model_validate(
        launch.binding.execution.model_dump(exclude={"allocation_epoch"})
    )
    registration = ExecutableExecutorRegistrationV2(
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
    client = RecordingHeartbeatClient()
    journal = ExecutorJournal(tmp_path / "executor.journal")
    with journal:
        loop = ExecutableHeartbeatLoop(registration, journal, client)

        first = await loop.heartbeat()
        second = await loop.heartbeat()

    assert (first.heartbeat_sequence, second.heartbeat_sequence) == (1, 2)
    assert [item.heartbeat_sequence for item in client.heartbeats] == [1, 2]
