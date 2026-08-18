from __future__ import annotations

import asyncio
import hashlib
import json
import os
import signal
import subprocess
import sys
from dataclasses import asdict, dataclass, replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from uuid import UUID, uuid5

import pytest
from tests.support.fake_slurm import FakeSlurm
from tests.unit.test_capacity_executor_config import executor_files
from tests.unit.test_capacity_executor_executable import executor_fixture

from loom_capacity_executor.config import PoolExecutorConfig
from loom_capacity_executor.executable import ExecutablePoolExecutor
from loom_capacity_executor.journal import ExecutorJournal
from loom_capacity_manager.contracts import NodeEnvelopeV1, ResourceVectorV1
from loom_capacity_manager.executable_contracts import (
    ExecutableExecutorHeartbeatV2,
    ExecutableExecutorInventoryV2,
    ExecutionAuthorityV2,
    ExecutionContextV2,
    canonical_executable_bytes,
    canonical_executable_digest,
)
from loom_capacity_pool_controller import runtime as once
from loom_capacity_pool_controller.runtime import (
    ExecutorConfigError,
    run_daemon_once,
    run_executor_once,
)
from loom_capacity_pool_executor.slurm_inventory import SlurmInventoryPolicy

_REGISTRATION_NAMESPACE = UUID("0dbdb949-f40e-5ae4-92ac-ee986992a3a2")


@dataclass
class InventoryClient:
    inventory_sequence: int = 0

    def __post_init__(self) -> None:
        self.inventories: list[ExecutableExecutorInventoryV2] = []
        self.heartbeats: list[ExecutableExecutorHeartbeatV2] = []
        self.events: list[tuple[str, object]] = []
        self.journal_sequence = 0
        self.journal_digest = "0" * 64

    async def executable_checkpoint(self) -> SimpleNamespace:
        return SimpleNamespace(
            journal_sequence=self.journal_sequence,
            journal_digest=self.journal_digest,
            inventory_sequence=self.inventory_sequence,
            command_sequence=0,
        )

    async def heartbeat_executable_executor(
        self,
        heartbeat: ExecutableExecutorHeartbeatV2,
    ) -> SimpleNamespace:
        self.heartbeats.append(heartbeat)
        self.events.append(("heartbeat", heartbeat.heartbeat_sequence))
        self.journal_sequence = heartbeat.journal_sequence
        self.journal_digest = heartbeat.journal_digest
        return SimpleNamespace(
            heartbeat_sequence=heartbeat.heartbeat_sequence,
            lease_expires_at=datetime.now(UTC) + timedelta(seconds=90),
            replayed=False,
            executable=True,
        )

    async def ingest_executable_inventory(
        self, inventory: ExecutableExecutorInventoryV2
    ) -> SimpleNamespace:
        self.inventories.append(inventory)
        self.events.append(("inventory", inventory.inventory_sequence))
        self.inventory_sequence = inventory.inventory_sequence
        return SimpleNamespace()


class PreparedInventoryClient(InventoryClient):
    def __init__(self, execution: ExecutionContextV2) -> None:
        super().__init__()
        self.execution = execution
        self.registration_keys: list[UUID] = []

    async def current_execution_context(self) -> ExecutionContextV2:
        self.events.append(("context", self.execution.execution_epoch))
        return self.execution

    async def register_execution_executor(self, *, idempotency_key: UUID) -> ExecutionContextV2:
        self.registration_keys.append(idempotency_key)
        self.events.append(("registration", idempotency_key))
        return self.execution

    async def executable_checkpoint(self) -> SimpleNamespace:
        self.events.append(("checkpoint", self.inventory_sequence))
        return await super().executable_checkpoint()


def _prepared_policy(config: PoolExecutorConfig) -> SlurmInventoryPolicy:
    return SlurmInventoryPolicy(
        pool_id=config.pool_id,
        pool_generation=config.pool_generation,
        reporter_incarnation=UUID("10000000-0000-4000-8000-000000000001"),
        nodes=(
            NodeEnvelopeV1(
                node_id=f"{config.pool_id}-node-a",
                allocatable=ResourceVectorV1(
                    slots=2,
                    cpu_millicores=4_000,
                    memory_bytes=16 * 1024**3,
                ),
            ),
        ),
        relevant_partitions=(config.partition,),
        slot_resources=ResourceVectorV1(
            slots=1,
            cpu_millicores=2_000,
            memory_bytes=8 * 1024**3,
        ),
        controller_cluster=config.slurm_cluster,
        slurm_version=(23, 11, 4),
        data_parser="data_parser/v0.0.40",
        query_principal="loom-capacity-slurm-reader",
        query_uid=os.geteuid(),
        job_visibility_evidence_sha256="a" * 64,
        scontrol_sha256="b" * 64,
        squeue_sha256="c" * 64,
        slurm_conf_sha256="d" * 64,
    )


class FixedReadOnlyInventoryRunner:
    def __init__(self, policy: SlurmInventoryPolicy, events: list[tuple[str, object]]) -> None:
        self.policy = policy
        self.events = events

    async def run(self, command: str) -> bytes:
        self.events.append(("query", command))
        common: dict[str, object] = {
            "errors": [],
            "warnings": [],
            "meta": {
                "slurm": {
                    "cluster": self.policy.controller_cluster,
                    "version": {"major": "23", "minor": "11", "micro": "4"},
                },
                "plugin": {"data_parser": "data_parser/v0.0.40"},
            },
            "last_update": {"set": True, "infinite": False, "number": 77},
        }
        if command == "jobs":
            document = common | {"jobs": []}
        elif command == "nodes":
            node = self.policy.nodes[0]
            document = common | {
                "nodes": [
                    {
                        "name": node.node_id,
                        "partitions": list(self.policy.relevant_partitions),
                        "state": ["DOWN"],
                        "cpus": node.allocatable.cpu_millicores // 1_000,
                        "effective_cpus": node.allocatable.cpu_millicores // 1_000,
                        "real_memory": node.allocatable.memory_bytes // 1024**2,
                        "alloc_cpus": 0,
                        "alloc_memory": 0,
                        "gres": "",
                        "gres_used": "",
                    }
                ]
            }
        else:
            raise AssertionError("only fixed jobs and nodes queries are allowed")
        return json.dumps(document).encode("utf-8")


@pytest.mark.asyncio
async def test_prepared_inventory_registers_and_journals_complete_physical_snapshot(
    tmp_path: Path,
) -> None:
    config = PoolExecutorConfig.from_files(executor_files(tmp_path).config)
    policy = _prepared_policy(config)
    client = PreparedInventoryClient(config.execution)

    def runner_factory(*, policy: SlurmInventoryPolicy) -> FixedReadOnlyInventoryRunner:
        return FixedReadOnlyInventoryRunner(policy, client.events)

    result = await once.run_prepared_inventory_once(
        config,
        policy,
        client=client,
        runner_factory=runner_factory,
    )

    registration_key = uuid5(
        _REGISTRATION_NAMESPACE,
        canonical_executable_digest(config.registration),
    )
    assert result.mode == "inventory-only"
    assert client.events == [
        ("context", config.execution.execution_epoch),
        ("registration", registration_key),
        ("heartbeat", 1),
        ("checkpoint", 0),
        ("query", "jobs"),
        ("query", "nodes"),
        ("query", "jobs"),
        ("inventory", 1),
        ("checkpoint", 1),
        ("heartbeat", 2),
    ]
    inventory = client.inventories[0]
    assert inventory.execution == config.execution
    assert inventory.executor_id == config.executor_id
    assert inventory.executor_incarnation == config.executor_incarnation
    assert inventory.pool_id == config.pool_id
    assert inventory.pool_generation == config.pool_generation
    assert inventory.inventory_sequence == 1
    assert inventory.journal_sequence == 3
    assert inventory.journal_checkpoint_sequence == 0
    assert inventory.journal_checkpoint_digest == "0" * 64
    assert tuple(record.physical_identity for record in inventory.records) == (
        f"slurm-node-{config.pool_id}-node-a-unavailable",
    )
    assert inventory.records[0].authority_scope == "foreign"
    with ExecutorJournal(config.journal_file) as journal:
        latest = journal.latest("inventory", str(config.executor_incarnation))
        assert latest is not None
        assert latest.event_kind == "inventory-publish-confirmed"


@pytest.mark.asyncio
@pytest.mark.parametrize("field", ("pool_id", "pool_generation", "query_uid"))
async def test_prepared_inventory_rejects_policy_binding_drift_before_network_or_runner(
    tmp_path: Path,
    field: str,
) -> None:
    config = PoolExecutorConfig.from_files(executor_files(tmp_path).config)
    policy = _prepared_policy(config)
    changed = {
        "pool_id": "gb10",
        "pool_generation": config.pool_generation + 1,
        "query_uid": os.geteuid() + 1,
    }
    policy = replace(policy, **{field: changed[field]})
    client = PreparedInventoryClient(config.execution)
    constructed = False

    def runner_factory(*, policy: SlurmInventoryPolicy) -> FixedReadOnlyInventoryRunner:
        nonlocal constructed
        constructed = True
        return FixedReadOnlyInventoryRunner(policy, client.events)

    with pytest.raises(ExecutorConfigError, match="inventory policy"):
        await once.run_prepared_inventory_once(
            config,
            policy,
            client=client,
            runner_factory=runner_factory,
        )

    assert not constructed
    assert client.events == []


@pytest.mark.asyncio
@pytest.mark.parametrize("context_kind", ("active", "drain-only", "changed", "invalid"))
async def test_prepared_inventory_rejects_nonexact_manager_context_before_registration_or_query(
    tmp_path: Path,
    context_kind: str,
) -> None:
    config = PoolExecutorConfig.from_files(executor_files(tmp_path).config)
    policy = _prepared_policy(config)
    context: object
    if context_kind in {"active", "drain-only"}:
        context = config.execution.model_copy(update={"execution_state": context_kind})
    elif context_kind == "changed":
        context = config.execution.model_copy(
            update={"writer_epoch": config.execution.writer_epoch + 1}
        )
    else:
        context = None
    client = PreparedInventoryClient(config.execution)

    async def current_execution_context() -> object:
        client.events.append(("context", context_kind))
        return context

    client.current_execution_context = current_execution_context  # type: ignore[method-assign]

    def runner_factory(*, policy: SlurmInventoryPolicy) -> FixedReadOnlyInventoryRunner:
        return FixedReadOnlyInventoryRunner(policy, client.events)

    with pytest.raises(ExecutorConfigError, match="prepared-only manager context"):
        await once.run_prepared_inventory_once(
            config,
            policy,
            client=client,
            runner_factory=runner_factory,
        )

    assert client.registration_keys == []
    assert not any(event == "query" for event, _value in client.events)


@pytest.mark.asyncio
async def test_prepared_inventory_stops_after_registration_rejection(tmp_path: Path) -> None:
    config = PoolExecutorConfig.from_files(executor_files(tmp_path).config)
    policy = _prepared_policy(config)

    class RejectedRegistrationClient(PreparedInventoryClient):
        async def register_execution_executor(
            self,
            *,
            idempotency_key: UUID,
        ) -> ExecutionContextV2:
            await super().register_execution_executor(idempotency_key=idempotency_key)
            raise RuntimeError("registration rejected")

    client = RejectedRegistrationClient(config.execution)

    def runner_factory(*, policy: SlurmInventoryPolicy) -> FixedReadOnlyInventoryRunner:
        return FixedReadOnlyInventoryRunner(policy, client.events)

    with pytest.raises(RuntimeError, match="registration rejected"):
        await once.run_prepared_inventory_once(
            config,
            policy,
            client=client,
            runner_factory=runner_factory,
        )

    assert [event for event, _value in client.events] == ["context", "registration"]


@pytest.mark.asyncio
async def test_prepared_inventory_query_failure_leaves_no_inventory_request(
    tmp_path: Path,
) -> None:
    config = PoolExecutorConfig.from_files(executor_files(tmp_path).config)
    policy = _prepared_policy(config)
    client = PreparedInventoryClient(config.execution)

    class FailedQueryRunner(FixedReadOnlyInventoryRunner):
        async def run(self, command: str) -> bytes:
            self.events.append(("query", command))
            raise TimeoutError("read-only Slurm query timed out")

    def runner_factory(*, policy: SlurmInventoryPolicy) -> FailedQueryRunner:
        return FailedQueryRunner(policy, client.events)

    with pytest.raises(TimeoutError, match="query timed out"):
        await once.run_prepared_inventory_once(
            config,
            policy,
            client=client,
            runner_factory=runner_factory,
        )

    assert client.inventories == []
    with ExecutorJournal(config.journal_file) as journal:
        assert journal.latest("inventory", str(config.executor_incarnation)) is None


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ("warning", "race", "oversize"))
async def test_prepared_inventory_rejects_incomplete_or_unstable_controller_snapshot(
    tmp_path: Path,
    failure: str,
) -> None:
    config = PoolExecutorConfig.from_files(executor_files(tmp_path).config)
    policy = _prepared_policy(config)
    client = PreparedInventoryClient(config.execution)

    class InvalidSnapshotRunner(FixedReadOnlyInventoryRunner):
        def __init__(self, policy: SlurmInventoryPolicy) -> None:
            super().__init__(policy, client.events)
            self.job_queries = 0

        async def run(self, command: str) -> bytes:
            if failure == "oversize":
                self.events.append(("query", command))
                return b"x" * (8 * 1024 * 1024 + 1)
            encoded = await super().run(command)
            document = json.loads(encoded)
            if failure == "warning":
                document["warnings"] = ["controller visibility warning"]
            elif command == "jobs":
                self.job_queries += 1
                if self.job_queries % 2 == 0:
                    document["last_update"]["number"] = 78
            return json.dumps(document).encode("utf-8")

    def runner_factory(*, policy: SlurmInventoryPolicy) -> InvalidSnapshotRunner:
        return InvalidSnapshotRunner(policy)

    with pytest.raises(ValueError):
        await once.run_prepared_inventory_once(
            config,
            policy,
            client=client,
            runner_factory=runner_factory,
        )

    assert client.inventories == []
    with ExecutorJournal(config.journal_file) as journal:
        assert journal.latest("inventory", str(config.executor_incarnation)) is None


@pytest.mark.asyncio
@pytest.mark.parametrize("manager_applied", (False, True))
async def test_prepared_inventory_replays_byte_identical_request_without_requery(
    tmp_path: Path,
    manager_applied: bool,
) -> None:
    config = PoolExecutorConfig.from_files(executor_files(tmp_path).config)
    policy = _prepared_policy(config)

    class ResponseLostClient(PreparedInventoryClient):
        async def ingest_executable_inventory(
            self,
            inventory: ExecutableExecutorInventoryV2,
        ) -> SimpleNamespace:
            self.inventories.append(inventory)
            self.events.append(("inventory", inventory.inventory_sequence))
            if manager_applied:
                self.inventory_sequence = inventory.inventory_sequence
                raise TimeoutError("inventory response lost")
            raise RuntimeError("inventory rejected")

    first = ResponseLostClient(config.execution)

    def first_runner(*, policy: SlurmInventoryPolicy) -> FixedReadOnlyInventoryRunner:
        return FixedReadOnlyInventoryRunner(policy, first.events)

    expected_error = TimeoutError if manager_applied else RuntimeError
    expected_message = "response lost" if manager_applied else "rejected"
    with pytest.raises(expected_error, match=expected_message):
        await once.run_prepared_inventory_once(
            config,
            policy,
            client=first,
            runner_factory=first_runner,
        )
    requested = canonical_executable_bytes(first.inventories[0])

    replay = PreparedInventoryClient(config.execution)
    replay.inventory_sequence = 1 if manager_applied else 0

    def replay_runner(*, policy: SlurmInventoryPolicy) -> FixedReadOnlyInventoryRunner:
        return FixedReadOnlyInventoryRunner(policy, replay.events)

    result = await once.run_prepared_inventory_once(
        config,
        policy,
        client=replay,
        runner_factory=replay_runner,
    )

    assert result.mode == "inventory-only"
    assert canonical_executable_bytes(replay.inventories[0]) == requested
    assert not any(event == "query" for event, _value in replay.events)
    assert replay.registration_keys == first.registration_keys


@pytest.mark.asyncio
async def test_prepared_inventory_rejects_noncanonical_durable_replay_bytes(
    tmp_path: Path,
) -> None:
    config = PoolExecutorConfig.from_files(executor_files(tmp_path).config)
    policy = _prepared_policy(config)
    inventory = ExecutableExecutorInventoryV2(
        execution=config.execution,
        executor_id=config.executor_id,
        executor_incarnation=config.executor_incarnation,
        pool_id=config.pool_id,
        pool_generation=config.pool_generation,
        inventory_sequence=1,
        journal_sequence=0,
        journal_digest="0" * 64,
    )
    noncanonical = json.dumps(inventory.model_dump(mode="json")).encode("ascii")
    assert noncanonical != canonical_executable_bytes(inventory)
    with ExecutorJournal(config.journal_file) as journal:
        journal.append(
            "inventory-publish-requested",
            hashlib.sha256(noncanonical).hexdigest(),
            object_kind="inventory",
            object_id=str(config.executor_incarnation),
            payload=noncanonical,
        )
    client = PreparedInventoryClient(config.execution)

    def runner_factory(*, policy: SlurmInventoryPolicy) -> FixedReadOnlyInventoryRunner:
        return FixedReadOnlyInventoryRunner(policy, client.events)

    with pytest.raises(ExecutorConfigError, match="not canonical"):
        await once.run_prepared_inventory_once(
            config,
            policy,
            client=client,
            runner_factory=runner_factory,
        )

    assert client.inventories == []
    assert not any(event == "query" for event, _value in client.events)


@pytest.mark.asyncio
async def test_prepared_inventory_redacts_invalid_durable_replay_payload(
    tmp_path: Path,
) -> None:
    config = PoolExecutorConfig.from_files(executor_files(tmp_path).config)
    policy = _prepared_policy(config)
    secret = "durable-payload-must-not-escape"
    payload = json.dumps({"leaked_secret": secret}, separators=(",", ":")).encode("ascii")
    with ExecutorJournal(config.journal_file) as journal:
        journal.append(
            "inventory-publish-requested",
            hashlib.sha256(payload).hexdigest(),
            object_kind="inventory",
            object_id=str(config.executor_incarnation),
            payload=payload,
        )
    client = PreparedInventoryClient(config.execution)

    def runner_factory(*, policy: SlurmInventoryPolicy) -> FixedReadOnlyInventoryRunner:
        return FixedReadOnlyInventoryRunner(policy, client.events)

    with pytest.raises(ExecutorConfigError) as caught:
        await once.run_prepared_inventory_once(
            config,
            policy,
            client=client,
            runner_factory=runner_factory,
        )

    assert str(caught.value) == "inventory journal payload is invalid"
    assert secret not in str(caught.value)
    assert client.inventories == []


@pytest.mark.asyncio
async def test_prepared_inventory_rejects_unknown_inventory_journal_state_before_network_work(
    tmp_path: Path,
) -> None:
    config = PoolExecutorConfig.from_files(executor_files(tmp_path).config)
    policy = _prepared_policy(config)
    inventory = ExecutableExecutorInventoryV2(
        execution=config.execution,
        executor_id=config.executor_id,
        executor_incarnation=config.executor_incarnation,
        pool_id=config.pool_id,
        pool_generation=config.pool_generation,
        inventory_sequence=1,
        journal_sequence=0,
        journal_digest="0" * 64,
    )
    payload = canonical_executable_bytes(inventory)
    with ExecutorJournal(config.journal_file) as journal:
        journal.append(
            "inventory-published",
            canonical_executable_digest(inventory),
            object_kind="inventory",
            object_id=str(config.executor_incarnation),
            payload=payload,
        )
    client = PreparedInventoryClient(config.execution)
    client.inventory_sequence = 1

    def runner_factory(*, policy: SlurmInventoryPolicy) -> FixedReadOnlyInventoryRunner:
        return FixedReadOnlyInventoryRunner(policy, client.events)

    with pytest.raises(ExecutorConfigError, match="inventory journal state is invalid"):
        await once.run_prepared_inventory_once(
            config,
            policy,
            client=client,
            runner_factory=runner_factory,
        )

    assert [event for event, _value in client.events] == ["context", "registration"]


@pytest.mark.asyncio
async def test_prepared_inventory_does_not_publish_when_journal_request_append_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = PoolExecutorConfig.from_files(executor_files(tmp_path).config)
    policy = _prepared_policy(config)
    client = PreparedInventoryClient(config.execution)
    real_append = ExecutorJournal.append

    def append(
        journal: ExecutorJournal,
        event_kind: str,
        payload_digest: str,
        **kwargs: object,
    ) -> object:
        if event_kind == "inventory-publish-requested":
            raise OSError("inventory journal append failed")
        return real_append(journal, event_kind, payload_digest, **kwargs)  # type: ignore[arg-type]

    def runner_factory(*, policy: SlurmInventoryPolicy) -> FixedReadOnlyInventoryRunner:
        return FixedReadOnlyInventoryRunner(policy, client.events)

    monkeypatch.setattr(ExecutorJournal, "append", append)

    with pytest.raises(OSError, match="journal append failed"):
        await once.run_prepared_inventory_once(
            config,
            policy,
            client=client,
            runner_factory=runner_factory,
        )

    assert client.inventories == []
    with ExecutorJournal(config.journal_file) as journal:
        assert journal.latest("inventory", str(config.executor_incarnation)) is None


@pytest.mark.asyncio
async def test_prepared_inventory_replays_after_confirmation_append_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = PoolExecutorConfig.from_files(executor_files(tmp_path).config)
    policy = _prepared_policy(config)
    first = PreparedInventoryClient(config.execution)
    real_append = ExecutorJournal.append

    def append(
        journal: ExecutorJournal,
        event_kind: str,
        payload_digest: str,
        **kwargs: object,
    ) -> object:
        if event_kind == "inventory-publish-confirmed":
            raise OSError("inventory confirmation append failed")
        return real_append(journal, event_kind, payload_digest, **kwargs)  # type: ignore[arg-type]

    def first_runner(*, policy: SlurmInventoryPolicy) -> FixedReadOnlyInventoryRunner:
        return FixedReadOnlyInventoryRunner(policy, first.events)

    monkeypatch.setattr(ExecutorJournal, "append", append)
    with pytest.raises(OSError, match="confirmation append failed"):
        await once.run_prepared_inventory_once(
            config,
            policy,
            client=first,
            runner_factory=first_runner,
        )
    published = canonical_executable_bytes(first.inventories[0])
    monkeypatch.setattr(ExecutorJournal, "append", real_append)

    replay = PreparedInventoryClient(config.execution)
    replay.inventory_sequence = 1

    def replay_runner(*, policy: SlurmInventoryPolicy) -> FixedReadOnlyInventoryRunner:
        return FixedReadOnlyInventoryRunner(policy, replay.events)

    await once.run_prepared_inventory_once(
        config,
        policy,
        client=replay,
        runner_factory=replay_runner,
    )

    assert canonical_executable_bytes(replay.inventories[0]) == published
    assert not any(event == "query" for event, _value in replay.events)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("manager_sequence", "failure"),
    ((0, "regressed"), (2, "advanced")),
)
async def test_prepared_inventory_rejects_divergent_manager_inventory_high_water_before_query(
    tmp_path: Path,
    manager_sequence: int,
    failure: str,
) -> None:
    config = PoolExecutorConfig.from_files(executor_files(tmp_path).config)
    policy = _prepared_policy(config)
    client = PreparedInventoryClient(config.execution)

    def runner_factory(*, policy: SlurmInventoryPolicy) -> FixedReadOnlyInventoryRunner:
        return FixedReadOnlyInventoryRunner(policy, client.events)

    await once.run_prepared_inventory_once(
        config,
        policy,
        client=client,
        runner_factory=runner_factory,
    )
    client.events.clear()
    client.inventory_sequence = manager_sequence

    with pytest.raises(ExecutorConfigError, match=f"inventory high-water {failure}"):
        await once.run_prepared_inventory_once(
            config,
            policy,
            client=client,
            runner_factory=runner_factory,
        )

    assert not any(event == "query" for event, _value in client.events)
    assert len(client.inventories) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "interruption",
    (
        "context",
        "registration",
        "heartbeat-1",
        "checkpoint-1",
        "query-1",
        "query-2",
        "query-3",
        "inventory",
        "checkpoint-2",
        "heartbeat-2",
    ),
)
async def test_prepared_inventory_recovers_after_cancellation_at_every_external_await(
    tmp_path: Path,
    interruption: str,
) -> None:
    config = PoolExecutorConfig.from_files(executor_files(tmp_path).config)
    policy = _prepared_policy(config)
    entered = asyncio.Event()

    class InterruptibleClient(PreparedInventoryClient):
        def __init__(self) -> None:
            super().__init__(config.execution)
            self.interruption: str | None = interruption
            self.heartbeat_calls = 0
            self.checkpoint_calls = 0

        async def block(self, stage: str) -> None:
            if self.interruption == stage:
                entered.set()
                await asyncio.Event().wait()

        async def current_execution_context(self) -> ExecutionContextV2:
            await self.block("context")
            return await super().current_execution_context()

        async def register_execution_executor(
            self,
            *,
            idempotency_key: UUID,
        ) -> ExecutionContextV2:
            await self.block("registration")
            return await super().register_execution_executor(idempotency_key=idempotency_key)

        async def heartbeat_executable_executor(
            self,
            heartbeat: ExecutableExecutorHeartbeatV2,
        ) -> SimpleNamespace:
            self.heartbeat_calls += 1
            await self.block(f"heartbeat-{self.heartbeat_calls}")
            return await super().heartbeat_executable_executor(heartbeat)

        async def executable_checkpoint(self) -> SimpleNamespace:
            self.checkpoint_calls += 1
            await self.block(f"checkpoint-{self.checkpoint_calls}")
            return await super().executable_checkpoint()

        async def ingest_executable_inventory(
            self,
            inventory: ExecutableExecutorInventoryV2,
        ) -> SimpleNamespace:
            await self.block("inventory")
            return await super().ingest_executable_inventory(inventory)

    client = InterruptibleClient()

    class InterruptibleRunner(FixedReadOnlyInventoryRunner):
        def __init__(self, policy: SlurmInventoryPolicy) -> None:
            super().__init__(policy, client.events)
            self.query_calls = 0

        async def run(self, command: str) -> bytes:
            self.query_calls += 1
            await client.block(f"query-{self.query_calls}")
            return await super().run(command)

    def runner_factory(*, policy: SlurmInventoryPolicy) -> InterruptibleRunner:
        return InterruptibleRunner(policy)

    task = asyncio.create_task(
        once.run_prepared_inventory_once(
            config,
            policy,
            client=client,
            runner_factory=runner_factory,
        )
    )
    await asyncio.wait_for(entered.wait(), timeout=1)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    client.interruption = None
    result = await once.run_prepared_inventory_once(
        config,
        policy,
        client=client,
        runner_factory=runner_factory,
    )

    assert result.mode == "inventory-only"
    with ExecutorJournal(config.journal_file) as journal:
        latest = journal.latest("inventory", str(config.executor_incarnation))
        assert latest is not None
        assert latest.event_kind == "inventory-publish-confirmed"


@pytest.mark.asyncio
async def test_prepared_inventory_never_constructs_executable_scheduler_runtime(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = PoolExecutorConfig.from_files(executor_files(tmp_path).config)
    policy = _prepared_policy(config)
    client = PreparedInventoryClient(config.execution)

    def forbidden_runtime(*_args: object, **_kwargs: object) -> object:
        raise AssertionError("prepared inventory must not construct executable scheduler runtime")

    def runner_factory(*, policy: SlurmInventoryPolicy) -> FixedReadOnlyInventoryRunner:
        return FixedReadOnlyInventoryRunner(policy, client.events)

    monkeypatch.setattr(once, "build_executable_runtime", forbidden_runtime)
    monkeypatch.setattr(once, "AsyncSlurmBackend", MutatingBackendMustNotConstruct)

    result = await once.run_prepared_inventory_once(
        config,
        policy,
        client=client,
        runner_factory=runner_factory,
    )

    assert result.mode == "inventory-only"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("validate_only", "prepared_only", "policy", "activation"),
    (
        (True, True, "present", None),
        (False, True, None, None),
        (False, True, "present", "present"),
        (True, False, "present", None),
        (True, False, None, "present"),
        (False, False, "present", "present"),
        (False, False, None, None),
    ),
)
async def test_daemon_mode_combinations_fail_before_client_construction(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    validate_only: bool,
    prepared_only: bool,
    policy: str | None,
    activation: str | None,
) -> None:
    config = PoolExecutorConfig.from_files(executor_files(tmp_path).config)
    inventory_policy = _prepared_policy(config) if policy is not None else None
    activation_path = tmp_path / "activation.json" if activation is not None else None

    def unexpected_client(_config: PoolExecutorConfig) -> object:
        raise AssertionError("invalid mode must fail before client construction")

    monkeypatch.setattr(once, "build_executable_client", unexpected_client)

    with pytest.raises(ExecutorConfigError, match=r"mode|requires|refuses"):
        await run_daemon_once(
            config,
            validate_only=validate_only,
            prepared_only=prepared_only,
            inventory_policy=inventory_policy,
            activation_runtime_artifact=activation_path,
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "change",
    (
        {"pool_id": 0},
        {"validate_only": 1},
        {"prepared_only": 1},
        {"prepared_only": True, "inventory_policy": object()},
        {"activation_runtime_artifact": "activation.json"},
    ),
)
async def test_daemon_rejects_invalid_mode_argument_types_before_client_construction(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    change: dict[str, object],
) -> None:
    config = PoolExecutorConfig.from_files(executor_files(tmp_path).config)
    arguments: dict[str, object] = {
        "validate_only": False,
        "prepared_only": False,
        "inventory_policy": None,
        "activation_runtime_artifact": tmp_path / "activation.json",
    }
    arguments.update(change)

    def unexpected_client(_config: PoolExecutorConfig) -> object:
        raise AssertionError("invalid runtime argument must fail before client construction")

    monkeypatch.setattr(once, "build_executable_client", unexpected_client)

    with pytest.raises(TypeError, match="argument"):
        await run_daemon_once(config, **arguments)  # type: ignore[arg-type]


@pytest.mark.asyncio
async def test_daemon_prepared_mode_routes_only_to_physical_inventory_runtime(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = PoolExecutorConfig.from_files(executor_files(tmp_path).config)
    policy = _prepared_policy(config)
    client = PreparedInventoryClient(config.execution)
    captured: dict[str, object] = {}

    class ManagedClient:
        async def __aenter__(self) -> PreparedInventoryClient:
            return client

        async def __aexit__(self, *_args: object) -> None:
            return None

    def client_factory(_config: PoolExecutorConfig) -> ManagedClient:
        return ManagedClient()

    async def prepared_once(
        runtime_config: PoolExecutorConfig,
        runtime_policy: SlurmInventoryPolicy,
        *,
        client: object,
    ) -> once.ExecutorOnceResult:
        captured["config"] = runtime_config
        captured["policy"] = runtime_policy
        captured["client"] = client
        return once.ExecutorOnceResult("inventory-only")

    monkeypatch.setattr(once, "build_executable_client", client_factory)
    monkeypatch.setattr(once, "run_prepared_inventory_once", prepared_once)

    result = await run_daemon_once(
        config,
        prepared_only=True,
        inventory_policy=policy,
    )

    assert result.mode == "inventory-only"
    assert captured == {"config": config, "policy": policy, "client": client}


@pytest.mark.asyncio
async def test_daemon_prepared_mode_rejects_cross_loaded_pool_before_client_construction(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = PoolExecutorConfig.from_files(executor_files(tmp_path, pool_id="gb10").config)
    policy = _prepared_policy(config)

    def unexpected_client(_config: PoolExecutorConfig) -> object:
        raise AssertionError("cross-loaded pool must fail before client construction")

    monkeypatch.setattr(once, "build_executable_client", unexpected_client)

    with pytest.raises(ExecutorConfigError, match="pool binding"):
        await run_daemon_once(
            config,
            pool_id="oldlab",
            prepared_only=True,
            inventory_policy=policy,
        )


@pytest.mark.asyncio
async def test_daemon_rejects_explicit_empty_pool_before_client_construction(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = PoolExecutorConfig.from_files(executor_files(tmp_path).config)

    def unexpected_client(_config: PoolExecutorConfig) -> object:
        raise AssertionError("empty pool binding must fail before credential loading")

    monkeypatch.setattr(once, "build_executable_client", unexpected_client)

    with pytest.raises(ExecutorConfigError, match="pool binding"):
        await run_daemon_once(
            config,
            pool_id="",
            validate_only=True,
        )


@pytest.mark.asyncio
async def test_daemon_prepared_mode_rejects_policy_drift_before_client_construction(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = PoolExecutorConfig.from_files(executor_files(tmp_path).config)
    policy = replace(_prepared_policy(config), controller_cluster="another-cluster")

    def unexpected_client(_config: PoolExecutorConfig) -> object:
        raise AssertionError("policy drift must fail before credential loading")

    monkeypatch.setattr(once, "build_executable_client", unexpected_client)

    with pytest.raises(ExecutorConfigError, match="inventory policy"):
        await run_daemon_once(
            config,
            prepared_only=True,
            inventory_policy=policy,
        )


@pytest.mark.asyncio
async def test_daemon_executable_mode_rejects_prepared_context_before_activation_runtime(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = PoolExecutorConfig.from_files(executor_files(tmp_path).config)
    client = PreparedInventoryClient(config.execution)

    class ManagedClient:
        async def __aenter__(self) -> PreparedInventoryClient:
            return client

        async def __aexit__(self, *_args: object) -> None:
            return None

    def forbidden(*_args: object, **_kwargs: object) -> object:
        raise AssertionError("prepared context must fail before activation runtime loading")

    monkeypatch.setattr(once, "build_executable_client", lambda _config: ManagedClient())
    monkeypatch.setattr(once, "load_activation_runtime_artifact", forbidden)
    monkeypatch.setattr(once, "build_executable_runtime", forbidden)

    with pytest.raises(ExecutorConfigError, match="prepared-only"):
        await run_daemon_once(
            config,
            activation_runtime_artifact=tmp_path / "activation.json",
        )


@pytest.mark.asyncio
async def test_executor_once_heartbeats_before_and_after_complete_inventory(
    tmp_path: Path,
) -> None:
    config = PoolExecutorConfig.from_files(executor_files(tmp_path).config)
    client = InventoryClient()

    result = await run_executor_once(config, client=client, validate_only=True)

    assert result.mode == "validate-only"
    assert client.events == [("heartbeat", 1), ("inventory", 1), ("heartbeat", 2)]
    assert len(client.heartbeats) == 2
    first, second = client.heartbeats
    inventory = client.inventories[0]
    assert first.journal_checkpoint_sequence == 0
    assert first.journal_checkpoint_digest == "0" * 64
    assert first.journal_sequence == 0
    assert first.journal_digest == "0" * 64
    assert inventory.journal_sequence == 3
    assert second.journal_checkpoint_sequence == 0
    assert second.journal_checkpoint_digest == "0" * 64
    assert second.journal_sequence == inventory.journal_sequence + 2


@pytest.mark.asyncio
async def test_executor_once_rejects_changed_heartbeat_receipt(tmp_path: Path) -> None:
    config = PoolExecutorConfig.from_files(executor_files(tmp_path).config)

    class BadHeartbeatReceiptClient(InventoryClient):
        async def heartbeat_executable_executor(
            self,
            heartbeat: ExecutableExecutorHeartbeatV2,
        ) -> SimpleNamespace:
            await super().heartbeat_executable_executor(heartbeat)
            return SimpleNamespace(
                heartbeat_sequence=heartbeat.heartbeat_sequence + 1,
                lease_expires_at=datetime.now(UTC) + timedelta(seconds=90),
                replayed=False,
                executable=True,
            )

    with pytest.raises(Exception, match="heartbeat receipt"):
        await run_executor_once(
            config,
            client=BadHeartbeatReceiptClient(),
            validate_only=True,
        )


@pytest.mark.asyncio
async def test_response_lost_inventory_is_replayed_byte_for_byte_before_new_inventory(
    tmp_path: Path,
) -> None:
    config = PoolExecutorConfig.from_files(executor_files(tmp_path).config)
    client = InventoryClient()

    class ResponseLostClient(InventoryClient):
        async def ingest_executable_inventory(
            self, inventory: ExecutableExecutorInventoryV2
        ) -> SimpleNamespace:
            self.inventory_sequence = inventory.inventory_sequence
            raise TimeoutError("manager response lost")

    with pytest.raises(TimeoutError, match="response lost"):
        await run_executor_once(
            config,
            client=ResponseLostClient(),
            validate_only=True,
        )
    result = await run_executor_once(config, client=client, validate_only=True)
    assert result.mode == "validate-only"
    assert client.inventory_sequence == 1
    assert client.events == [("inventory", 1), ("heartbeat", 2)]
    assert client.inventories[0].journal_sequence == 3


@pytest.mark.asyncio
async def test_cancellation_after_journal_persistence_leaves_exact_inventory_replay(
    tmp_path: Path,
) -> None:
    config = PoolExecutorConfig.from_files(executor_files(tmp_path).config)
    entered = asyncio.Event()

    class BlockingClient(InventoryClient):
        async def ingest_executable_inventory(
            self, inventory: ExecutableExecutorInventoryV2
        ) -> SimpleNamespace:
            entered.set()
            await asyncio.Event().wait()
            raise AssertionError("unreachable")

    task = asyncio.create_task(
        run_executor_once(config, client=BlockingClient(), validate_only=True)
    )
    await entered.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    with ExecutorJournal(config.journal_file) as journal:
        latest = journal.latest("inventory", str(config.executor_incarnation))
        assert latest is not None
        assert latest.event_kind == "inventory-publish-requested"


@pytest.mark.asyncio
async def test_bad_manager_journal_checkpoint_is_rejected_before_inventory(tmp_path: Path) -> None:
    config = PoolExecutorConfig.from_files(executor_files(tmp_path).config)

    class BadCheckpointClient(InventoryClient):
        async def executable_checkpoint(self) -> SimpleNamespace:
            return SimpleNamespace(
                journal_sequence=1,
                journal_digest="a" * 64,
                inventory_sequence=0,
                command_sequence=0,
            )

    with pytest.raises(Exception, match="journal"):
        await run_executor_once(
            config,
            client=BadCheckpointClient(),
            validate_only=True,
        )


@pytest.mark.asyncio
async def test_validate_only_rejects_concurrent_journal_owner(tmp_path: Path) -> None:
    config = PoolExecutorConfig.from_files(executor_files(tmp_path).config)
    with ExecutorJournal(config.journal_file):
        with pytest.raises(Exception, match="journal lock"):
            await run_executor_once(config, client=InventoryClient(), validate_only=True)


@pytest.mark.asyncio
async def test_daemon_entry_constructs_client_and_executes_inert_journal_path(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = PoolExecutorConfig.from_files(executor_files(tmp_path).config)
    client = InventoryClient()
    constructed = False

    class ManagedClient(InventoryClient):
        async def __aenter__(self) -> InventoryClient:
            return client

        async def __aexit__(self, *_args: object) -> None:
            return None

    def factory(_config: PoolExecutorConfig) -> ManagedClient:
        nonlocal constructed
        constructed = True
        return ManagedClient()

    monkeypatch.setattr(once, "build_executable_client", factory)
    result = await run_daemon_once(config, validate_only=True)
    assert constructed
    assert result.mode == "validate-only"


@pytest.mark.asyncio
async def test_daemon_entry_fetches_current_context_loads_artifact_and_assembles_runtime(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = PoolExecutorConfig.from_files(executor_files(tmp_path).config)
    active = config.execution.model_copy(
        update={
            "execution_state": "active",
            "executable_new_capacity_ceiling": 1,
            "executable_new_capacity_rate_per_minute": 1,
        }
    )
    artifact_path = tmp_path / "activation-runtime.json"
    artifact = object()
    executor = object()

    class ManagedClient(InventoryClient):
        async def __aenter__(self) -> ManagedClient:
            return self

        async def __aexit__(self, *_args: object) -> None:
            return None

        async def current_execution_context(self) -> object:
            self.events.append(("context", 0))
            return active

    managed = ManagedClient()
    captured: dict[str, object] = {}

    def client_factory(_config: PoolExecutorConfig) -> InventoryClient:
        return managed

    def load_artifact(path: Path) -> object:
        captured["artifact_path"] = path
        return artifact

    def build_runtime(
        runtime_config: PoolExecutorConfig,
        loaded_artifact: object,
        *,
        manager_client: object,
        current_context: object,
    ) -> object:
        captured["runtime_config"] = runtime_config
        captured["artifact"] = loaded_artifact
        captured["manager_client"] = manager_client
        captured["current_context"] = current_context
        return executor

    async def run_once(
        runtime_config: PoolExecutorConfig,
        *,
        pool_id: str | None = None,
        client: object,
        validate_only: bool = False,
        authority: ExecutionAuthorityV2 | None = None,
        executor: object | None = None,
    ) -> once.ExecutorOnceResult:
        captured["run_config"] = runtime_config
        captured["pool_id"] = pool_id
        captured["client"] = client
        captured["validate_only"] = validate_only
        captured["authority"] = authority
        captured["executor"] = executor
        return once.ExecutorOnceResult("scale-up")

    monkeypatch.setattr(once, "build_executable_client", client_factory)
    monkeypatch.setattr(once, "load_activation_runtime_artifact", load_artifact, raising=False)
    monkeypatch.setattr(once, "build_executable_runtime", build_runtime, raising=False)
    monkeypatch.setattr(once, "run_executor_once", run_once)

    result = await run_daemon_once(config, activation_runtime_artifact=artifact_path)

    assert result.mode == "scale-up"
    assert managed.events == [("context", 0)]
    assert captured["artifact_path"] == artifact_path
    assert captured["artifact"] is artifact
    assert captured["manager_client"] is managed
    assert captured["current_context"] == active
    assert captured["authority"] == ExecutionAuthorityV2.model_validate(active.model_dump())
    assert captured["executor"] is executor


@pytest.mark.asyncio
async def test_sigterm_cancels_async_daemon_after_requested_record_is_durable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = PoolExecutorConfig.from_files(executor_files(tmp_path).config)
    started = asyncio.Event()
    callbacks: dict[int, object] = {}
    loop = asyncio.get_running_loop()

    async def blocked_daemon(*_args: object, **_kwargs: object) -> object:
        started.set()
        await asyncio.Event().wait()
        raise AssertionError("unreachable")

    def add_handler(value: int, callback: object) -> None:
        callbacks[value] = callback

    monkeypatch.setattr(once, "run_daemon_once", blocked_daemon)
    monkeypatch.setattr(loop, "add_signal_handler", add_handler)
    runner = asyncio.create_task(once._run_with_signals(config, pool_id=None, validate_only=True))
    await started.wait()
    callback = callbacks[int(signal.SIGTERM)]
    assert callable(callback)
    callback()
    with pytest.raises(asyncio.CancelledError):
        await runner


def test_module_entrypoint_exposes_real_daemon_arguments() -> None:
    result = subprocess.run(
        (sys.executable, "-m", "loom_capacity_executor", "--help"),
        check=False,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0
    assert "--validate-only" in result.stdout
    assert "--prepared-only" in result.stdout
    assert "--config" in result.stdout
    assert "--activation-runtime-artifact" in result.stdout
    assert "--inventory-policy" in result.stdout
    assert "--expected-inventory-policy-sha256" in result.stdout


def test_module_entrypoint_loads_and_forwards_exact_prepared_inventory_policy(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = PoolExecutorConfig.from_files(executor_files(tmp_path).config)
    policy = _prepared_policy(config)
    policy_path = tmp_path / "inventory-policy.json"
    digest = "e" * 64
    captured: dict[str, object] = {}

    def load_config(path: Path, *, expected_manifest_sha256: str) -> PoolExecutorConfig:
        captured["config_path"] = path
        captured["manifest_digest"] = expected_manifest_sha256
        return config

    def load_policy(path: Path, *, expected_sha256: str) -> SlurmInventoryPolicy:
        captured["policy_path"] = path
        captured["policy_digest"] = expected_sha256
        return policy

    async def run_with_signals(
        runtime_config: PoolExecutorConfig,
        *,
        pool_id: str | None,
        validate_only: bool,
        prepared_only: bool,
        inventory_policy: SlurmInventoryPolicy | None,
        activation_runtime_artifact: Path | None,
    ) -> once.ExecutorOnceResult:
        captured["runtime_config"] = runtime_config
        captured["pool_id"] = pool_id
        captured["validate_only"] = validate_only
        captured["prepared_only"] = prepared_only
        captured["inventory_policy"] = inventory_policy
        captured["activation"] = activation_runtime_artifact
        return once.ExecutorOnceResult("inventory-only")

    monkeypatch.setattr(once.PoolExecutorConfig, "from_files", load_config)
    monkeypatch.setattr(once, "load_slurm_inventory_policy", load_policy, raising=False)
    monkeypatch.setattr(once, "_run_with_signals", run_with_signals)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "loom-capacity-executor",
            "--config",
            str(tmp_path / "executor.json"),
            "--expected-manifest-sha256",
            "f" * 64,
            "--pool",
            config.pool_id,
            "--prepared-only",
            "--inventory-policy",
            str(policy_path),
            "--expected-inventory-policy-sha256",
            digest,
        ],
    )

    assert once.main() == 0
    assert captured == {
        "config_path": str(tmp_path / "executor.json"),
        "manifest_digest": "f" * 64,
        "policy_path": policy_path,
        "policy_digest": digest,
        "runtime_config": config,
        "pool_id": config.pool_id,
        "validate_only": False,
        "prepared_only": True,
        "inventory_policy": policy,
        "activation": None,
    }


@pytest.mark.parametrize(
    "mode_arguments",
    (
        ("--prepared-only",),
        ("--prepared-only", "--inventory-policy", "policy.json"),
        ("--prepared-only", "--expected-inventory-policy-sha256", "e" * 64),
        (
            "--prepared-only",
            "--inventory-policy",
            "policy.json",
            "--expected-inventory-policy-sha256",
            "e" * 64,
            "--activation-runtime-artifact",
            "activation.json",
        ),
        (
            "--inventory-policy",
            "policy.json",
            "--expected-inventory-policy-sha256",
            "e" * 64,
            "--activation-runtime-artifact",
            "activation.json",
        ),
        ("--validate-only", "--activation-runtime-artifact", "activation.json"),
        (),
    ),
)
def test_module_entrypoint_rejects_invalid_modes_before_reading_configuration(
    monkeypatch: pytest.MonkeyPatch,
    mode_arguments: tuple[str, ...],
) -> None:
    def unexpected_load(*_args: object, **_kwargs: object) -> object:
        raise AssertionError("invalid mode must fail before reading configuration")

    monkeypatch.setattr(once.PoolExecutorConfig, "from_files", unexpected_load)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "loom-capacity-executor",
            "--config",
            "do-not-open-this-config",
            "--expected-manifest-sha256",
            "f" * 64,
            *mode_arguments,
        ],
    )

    with pytest.raises(SystemExit) as stopped:
        once.main()

    assert stopped.value.code == 2


class MutatingBackendMustNotConstruct:
    def __init__(self) -> None:
        raise AssertionError("mutating Slurm backend must remain unconstructed at zero ceiling")

    async def tick(self) -> object:
        raise AssertionError("mutating Slurm backend must remain unconstructed at zero ceiling")


class UntrustedTick:
    async def tick(self) -> object:
        return object()


def test_executor_result_does_not_claim_an_unobserved_scheduler_mutation_count() -> None:
    assert asdict(once.ExecutorOnceResult("inventory-only")) == {"mode": "inventory-only"}


@pytest.mark.asyncio
async def test_zero_ceiling_never_constructs_mutating_backend(tmp_path: Path) -> None:
    config = PoolExecutorConfig.from_files(executor_files(tmp_path).config)
    with pytest.raises(ExecutorConfigError, match="prepared-only"):
        await run_executor_once(
            config,
            client=InventoryClient(),
            executor=None,
        )


@pytest.mark.asyncio
async def test_validate_only_does_not_construct_mutating_backend(tmp_path: Path) -> None:
    config = PoolExecutorConfig.from_files(executor_files(tmp_path).config)
    result = await run_executor_once(
        config,
        client=InventoryClient(),
        validate_only=True,
        executor=None,
    )
    assert result.mode == "validate-only"


@pytest.mark.asyncio
@pytest.mark.parametrize("pool_id", ("oldlab", ""))
async def test_pool_argument_rejects_cross_loaded_or_empty_configuration(
    tmp_path: Path,
    pool_id: str,
) -> None:
    config = PoolExecutorConfig.from_files(executor_files(tmp_path, pool_id="gb10").config)
    with pytest.raises(Exception, match="pool binding"):
        await run_executor_once(config, pool_id=pool_id, client=InventoryClient())


@pytest.mark.asyncio
async def test_current_drain_only_authority_rejects_an_arbitrary_tick_object(
    tmp_path: Path,
) -> None:
    config = PoolExecutorConfig.from_files(executor_files(tmp_path).config)
    authority = ExecutionAuthorityV2(
        **config.execution.model_dump(exclude={"execution_state"}),
        execution_state="drain-only",
    )
    with pytest.raises(Exception, match="executable runtime"):
        await run_executor_once(
            config,
            client=InventoryClient(),
            authority=authority,
            executor=UntrustedTick(),  # type: ignore[arg-type]
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("writer_increment", (0, 1))
async def test_current_drain_only_authority_executes_drain_boundary(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    writer_increment: int,
) -> None:
    executor, journal, manager, _admission, _slurm, launch = executor_fixture(tmp_path, work=None)
    called = False
    typed = FakeSlurm(tmp_path / "typed-drain")
    backend = typed.backend()
    executor.slurm = backend
    executor.expected_slurm_authority = backend.authority

    async def drain_only_tick() -> SimpleNamespace:
        nonlocal called
        called = True
        return SimpleNamespace(status="idle")

    class NoopHeartbeatLoop:
        def __init__(self, *_args: object) -> None:
            pass

        async def heartbeat(self) -> None:
            return None

    try:
        config = PoolExecutorConfig.from_files(executor_files(tmp_path).config)
        executable_paths = tuple(
            sorted(
                (name, Path(getattr(backend.authority.executables, name).path))
                for name in ("scontrol", "sacctmgr", "squeue", "sbatch", "scancel", "sacct")
            )
        )
        config = replace(
            config,
            pool_generation=executor.registration.pool_generation,
            executor_id=executor.registration.executor_id,
            executor_incarnation=executor.registration.executor_incarnation,
            controller_authority_sha256=executor.controller_authority.controller_authority_sha256,
            local_authority_sha256=executor.registration.local_authority_sha256,
            signing_key_id=executor.ownership_key.signing_key_id,
            signing_key_sha256=executor.ownership_key.public_key_sha256,
            ownership_key=executor.ownership_key,
            journal_file=journal.path,
            slurm_cluster=launch.profile.slurm_cluster,
            controller_host=launch.profile.controller_host,
            partition=launch.profile.partition,
            association=launch.profile.association,
            submitter=launch.profile.submitter,
            qos=launch.profile.qos,
            profile_id=launch.profile.profile_id,
            profile_generation=launch.profile.profile_generation,
            profile_digest=launch.profile.profile_digest,
            execution=executor.registration.execution,
            manifest=replace(
                config.manifest,
                slurm_cluster=launch.profile.slurm_cluster,
                controller_host=launch.profile.controller_host,
                partition=launch.profile.partition,
                association=launch.profile.association,
                submitter=launch.profile.submitter,
                qos=launch.profile.qos,
                local_uid=backend.authority.local_uid,
                slurm_executables=executable_paths,
            ),
        )
        authority = ExecutionAuthorityV2.model_validate(
            executor.registration.execution.model_dump()
            | {
                "writer_epoch": (executor.registration.execution.writer_epoch + writer_increment),
                "execution_state": "drain-only",
                "executable_new_capacity_ceiling": 0,
                "executable_new_capacity_rate_per_minute": 0,
            }
        )
        executor.tick_drain_only = drain_only_tick  # type: ignore[method-assign]
        monkeypatch.setattr(once, "ExecutableHeartbeatLoop", NoopHeartbeatLoop)

        result = await run_executor_once(
            config,
            client=manager,
            authority=authority,
            executor=executor,
        )
    finally:
        journal.close()

    assert result.mode == "drain-only"
    assert called


@pytest.mark.asyncio
async def test_current_drain_only_authority_rejects_non_boundary_writer_epoch(
    tmp_path: Path,
) -> None:
    executor, journal, manager, _admission, _slurm, launch = executor_fixture(tmp_path, work=None)
    try:
        config = PoolExecutorConfig.from_files(executor_files(tmp_path).config)
        config = replace(
            config,
            pool_generation=executor.registration.pool_generation,
            executor_id=executor.registration.executor_id,
            executor_incarnation=executor.registration.executor_incarnation,
            controller_authority_sha256=executor.controller_authority.controller_authority_sha256,
            local_authority_sha256=executor.registration.local_authority_sha256,
            signing_key_id=executor.ownership_key.signing_key_id,
            signing_key_sha256=executor.ownership_key.public_key_sha256,
            ownership_key=executor.ownership_key,
            journal_file=journal.path,
            slurm_cluster=launch.profile.slurm_cluster,
            controller_host=launch.profile.controller_host,
            partition=launch.profile.partition,
            association=launch.profile.association,
            submitter=launch.profile.submitter,
            qos=launch.profile.qos,
            profile_id=launch.profile.profile_id,
            profile_generation=launch.profile.profile_generation,
            profile_digest=launch.profile.profile_digest,
            execution=executor.registration.execution,
        )
        authority = ExecutionAuthorityV2.model_validate(
            executor.registration.execution.model_dump()
            | {
                "writer_epoch": executor.registration.execution.writer_epoch + 2,
                "execution_state": "drain-only",
                "executable_new_capacity_ceiling": 0,
                "executable_new_capacity_rate_per_minute": 0,
            }
        )

        with pytest.raises(
            ExecutorConfigError,
            match="current execution authority differs from local binding",
        ):
            await run_executor_once(
                config,
                client=manager,
                authority=authority,
                executor=executor,
            )
    finally:
        journal.close()


@pytest.mark.asyncio
async def test_active_authority_rejects_task8_executor_without_typed_slurm_backend(
    tmp_path: Path,
) -> None:
    executor, journal, manager, _admission, _slurm, launch = executor_fixture(tmp_path, work=None)
    try:
        config = PoolExecutorConfig.from_files(executor_files(tmp_path).config)
        config = replace(
            config,
            pool_generation=executor.registration.pool_generation,
            executor_id=executor.registration.executor_id,
            executor_incarnation=executor.registration.executor_incarnation,
            controller_authority_sha256=executor.controller_authority.controller_authority_sha256,
            local_authority_sha256=executor.registration.local_authority_sha256,
            signing_key_id=executor.ownership_key.signing_key_id,
            signing_key_sha256=executor.ownership_key.public_key_sha256,
            ownership_key=executor.ownership_key,
            journal_file=journal.path,
            slurm_cluster=launch.profile.slurm_cluster,
            controller_host=launch.profile.controller_host,
            partition=launch.profile.partition,
            association=launch.profile.association,
            submitter=launch.profile.submitter,
            qos=launch.profile.qos,
            profile_id=launch.profile.profile_id,
            profile_generation=launch.profile.profile_generation,
            profile_digest=launch.profile.profile_digest,
            execution=executor.registration.execution,
        )
        authority = ExecutionAuthorityV2.model_validate(
            executor.registration.execution.model_dump()
        )
        with pytest.raises(Exception, match="typed Slurm backend"):
            await run_executor_once(
                config,
                client=manager,
                authority=authority,
                executor=executor,
            )
    finally:
        journal.close()


@pytest.mark.asyncio
async def test_typed_slurm_backend_partition_mismatch_is_rejected(tmp_path: Path) -> None:
    executor, journal, manager, _admission, _slurm, launch = executor_fixture(tmp_path, work=None)
    fake = FakeSlurm(tmp_path / "typed-slurm")
    try:
        backend = fake.backend(partition="other")
        executor.slurm = backend
        executor.expected_slurm_authority = backend.authority
        config = PoolExecutorConfig.from_files(executor_files(tmp_path).config)
        executable_paths = tuple(
            sorted(
                (name, Path(getattr(backend.authority.executables, name).path))
                for name in ("scontrol", "sacctmgr", "squeue", "sbatch", "scancel", "sacct")
            )
        )
        config = replace(
            config,
            pool_generation=executor.registration.pool_generation,
            executor_id=executor.registration.executor_id,
            executor_incarnation=executor.registration.executor_incarnation,
            controller_authority_sha256=executor.controller_authority.controller_authority_sha256,
            local_authority_sha256=executor.registration.local_authority_sha256,
            signing_key_id=executor.ownership_key.signing_key_id,
            signing_key_sha256=executor.ownership_key.public_key_sha256,
            ownership_key=executor.ownership_key,
            journal_file=journal.path,
            slurm_cluster=launch.profile.slurm_cluster,
            controller_host=launch.profile.controller_host,
            partition=launch.profile.partition,
            association=launch.profile.association,
            submitter=launch.profile.submitter,
            qos=launch.profile.qos,
            profile_id=launch.profile.profile_id,
            profile_generation=launch.profile.profile_generation,
            profile_digest=launch.profile.profile_digest,
            execution=executor.registration.execution,
            manifest=replace(
                config.manifest,
                slurm_cluster=launch.profile.slurm_cluster,
                controller_host=launch.profile.controller_host,
                partition=launch.profile.partition,
                association=launch.profile.association,
                submitter=launch.profile.submitter,
                qos=launch.profile.qos,
                local_uid=backend.authority.local_uid,
                slurm_executables=executable_paths,
            ),
        )
        authority = ExecutionAuthorityV2.model_validate(
            executor.registration.execution.model_dump()
        )
        with pytest.raises(Exception, match="exact controller-local binding"):
            await run_executor_once(config, client=manager, authority=authority, executor=executor)
    finally:
        journal.close()


@pytest.mark.asyncio
async def test_full_slurm_authority_envelope_mismatch_is_rejected(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original, journal, manager, admission, _slurm, launch = executor_fixture(tmp_path, work=None)
    fake = FakeSlurm(tmp_path / "typed-slurm-envelope")
    baseline = fake.backend()
    changed = fake.backend(max_stdout_bytes=baseline.authority.max_stdout_bytes + 128)
    try:
        executor = ExecutablePoolExecutor(
            original.registration,
            journal,
            manager,
            admission,
            baseline,
            profile=launch.profile,
            controller_authority=original.controller_authority,
            ownership_key=original.ownership_key,
            now=lambda: datetime(2026, 8, 13, 16, 0, tzinfo=UTC),
            bootstrap_handoff_store=original._bootstrap_handoff_store,
        )
        executor.slurm = changed
        config = PoolExecutorConfig.from_files(executor_files(tmp_path).config)
        executable_paths = tuple(
            sorted(
                (name, Path(getattr(baseline.authority.executables, name).path))
                for name in ("scontrol", "sacctmgr", "squeue", "sbatch", "scancel", "sacct")
            )
        )
        config = replace(
            config,
            pool_generation=executor.registration.pool_generation,
            executor_id=executor.registration.executor_id,
            executor_incarnation=executor.registration.executor_incarnation,
            controller_authority_sha256=executor.controller_authority.controller_authority_sha256,
            local_authority_sha256=executor.registration.local_authority_sha256,
            signing_key_id=executor.ownership_key.signing_key_id,
            signing_key_sha256=executor.ownership_key.public_key_sha256,
            ownership_key=executor.ownership_key,
            journal_file=journal.path,
            slurm_cluster=launch.profile.slurm_cluster,
            controller_host=launch.profile.controller_host,
            partition=launch.profile.partition,
            association=launch.profile.association,
            submitter=launch.profile.submitter,
            qos=launch.profile.qos,
            profile_id=launch.profile.profile_id,
            profile_generation=launch.profile.profile_generation,
            profile_digest=launch.profile.profile_digest,
            execution=executor.registration.execution,
            manifest=replace(
                config.manifest,
                slurm_cluster=launch.profile.slurm_cluster,
                controller_host=launch.profile.controller_host,
                partition=launch.profile.partition,
                association=launch.profile.association,
                submitter=launch.profile.submitter,
                qos=launch.profile.qos,
                local_uid=baseline.authority.local_uid,
                slurm_executables=executable_paths,
            ),
        )
        authority = ExecutionAuthorityV2.model_validate(
            executor.registration.execution.model_dump()
        )

        class NoopHeartbeatLoop:
            def __init__(self, *_args: object) -> None:
                pass

            async def heartbeat(self) -> None:
                return None

        async def unexpected_tick() -> SimpleNamespace:
            return SimpleNamespace(status="idle")

        executor.tick = unexpected_tick  # type: ignore[method-assign]
        monkeypatch.setattr(once, "ExecutableHeartbeatLoop", NoopHeartbeatLoop)

        with pytest.raises(Exception, match=r"Slurm authority|exact controller-local binding"):
            await run_executor_once(config, client=manager, authority=authority, executor=executor)
    finally:
        journal.close()
