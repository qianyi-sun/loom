from __future__ import annotations

import asyncio
import signal
import subprocess
import sys
from dataclasses import asdict, dataclass, replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest
from scripts.ops import global_fleet_pool_executor_once as once
from scripts.ops.global_fleet_pool_executor_once import (
    ExecutorConfigError,
    run_daemon_once,
    run_executor_once,
)
from tests.support.fake_slurm import FakeSlurm
from tests.unit.test_capacity_executor_config import executor_files
from tests.unit.test_capacity_executor_executable import executor_fixture

from loom_capacity_executor.config import PoolExecutorConfig
from loom_capacity_executor.executable import ExecutablePoolExecutor
from loom_capacity_executor.journal import ExecutorJournal
from loom_capacity_manager.executable_contracts import (
    ExecutableExecutorHeartbeatV2,
    ExecutableExecutorInventoryV2,
    ExecutionAuthorityV2,
)


@dataclass
class InventoryClient:
    inventory_sequence: int = 0

    def __post_init__(self) -> None:
        self.inventories: list[ExecutableExecutorInventoryV2] = []
        self.heartbeats: list[ExecutableExecutorHeartbeatV2] = []
        self.events: list[tuple[str, int]] = []
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


@pytest.mark.asyncio
async def test_executor_once_heartbeats_before_and_after_complete_inventory(
    tmp_path: Path,
) -> None:
    config = PoolExecutorConfig.from_files(executor_files(tmp_path).config)
    client = InventoryClient()

    result = await run_executor_once(config, client=client)

    assert result.mode == "inventory-only"
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
        await run_executor_once(config, client=BadHeartbeatReceiptClient())


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
        await run_executor_once(config, client=ResponseLostClient())
    result = await run_executor_once(config, client=client)
    assert result.mode == "inventory-only"
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

    task = asyncio.create_task(run_executor_once(config, client=BlockingClient()))
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
        await run_executor_once(config, client=BadCheckpointClient())


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
    assert "--config" in result.stdout
    assert "--activation-runtime-artifact" in result.stdout


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
    result = await run_executor_once(
        config,
        client=InventoryClient(),
        executor=None,
    )
    assert result.mode == "inventory-only"


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
async def test_pool_argument_rejects_cross_loaded_configuration(tmp_path: Path) -> None:
    config = PoolExecutorConfig.from_files(executor_files(tmp_path, pool_id="gb10").config)
    with pytest.raises(Exception, match="pool binding"):
        await run_executor_once(config, pool_id="oldlab", client=InventoryClient())


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
