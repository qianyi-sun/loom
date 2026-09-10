"""Runtime retention keeps recovery evidence while reclaiming repeated telemetry."""

from hashlib import sha256
from importlib import import_module
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest

from loom_capacity_executor.inventory_journal import load_journal_inventory
from loom_capacity_executor.journal import JournalRegressionError
from tests.unit.test_capacity_executor_executable import executor_fixture


async def test_idle_runtime_checkpoint_retains_only_latest_inventory_batch(tmp_path):
    runtime, journal, manager, _, _, _ = executor_fixture(tmp_path, work=None)
    try:
        for _ in range(8):
            await runtime._publish_inventory(await manager.executable_checkpoint())
        latest = journal.latest("inventory", str(runtime.registration.executor_incarnation))
        inventory = load_journal_inventory(journal, latest)
        module = import_module("loom_capacity_executor.journal_retention")
        plan = module.plan_runtime_checkpoint(runtime, await manager.executable_checkpoint())
        assert plan.retained_sequences == (latest.sequence - 1, latest.sequence)
        checkpoint = plan.prepare(journal)
        journal.commit_checkpoint(central_sequence=checkpoint.sequence, central_digest=checkpoint.record_digest)
        assert load_journal_inventory(journal, latest) == inventory
        for _ in range(4):
            await runtime._publish_inventory(await manager.executable_checkpoint())
        plan = module.plan_runtime_checkpoint(runtime, await manager.executable_checkpoint())
        assert len(plan.retained_sequences) == 2
        plan.prepare(journal)
    finally:
        journal.close()


@pytest.mark.parametrize("pending", (False, True))
async def test_runtime_checkpoint_rejects_unknown_dependencies_and_pending_work(tmp_path, pending):
    runtime, journal, manager, _, _, _ = executor_fixture(tmp_path, work=None)
    try:
        module = import_module("loom_capacity_executor.journal_retention")
        journal.append("intent-close-requested" if pending else "future-operation-confirmed", sha256(b"x").hexdigest(),
            object_kind="executor", object_id="unknown", payload=b"x")
        with pytest.raises(JournalRegressionError, match="unresolved" if pending else "unsupported"):
            module.plan_runtime_checkpoint(runtime, await manager.executable_checkpoint())
    finally:
        journal.close()


async def test_runtime_checkpoint_plan_cannot_outlive_its_selected_head(tmp_path):
    runtime, journal, manager, _, _, _ = executor_fixture(tmp_path, work=None)
    try:
        module = import_module("loom_capacity_executor.journal_retention")
        plan = module.plan_runtime_checkpoint(runtime, await manager.executable_checkpoint())
        await runtime._publish_inventory(await manager.executable_checkpoint())
        with pytest.raises(JournalRegressionError, match="head changed"):
            plan.prepare(journal)
    finally:
        journal.close()


@pytest.mark.parametrize("event", ("heartbeat-confirmed", "inventory-publish-confirmed", "inventory-chunk-retained"))
async def test_runtime_checkpoint_rejects_known_telemetry_with_unknown_object_binding(tmp_path, event):
    runtime, journal, manager, _, _, _ = executor_fixture(tmp_path, work=None)
    try:
        journal.append(event, sha256(b"x").hexdigest(),
            object_kind="executor", object_id="future-binding", payload=b"x")
        module = import_module("loom_capacity_executor.journal_retention")
        with pytest.raises(JournalRegressionError, match=r"unsupported.*binding"):
            module.plan_runtime_checkpoint(runtime, await manager.executable_checkpoint())
    finally:
        journal.close()


async def test_runtime_checkpoint_maintenance_republishes_retirement_inventory(tmp_path, monkeypatch):
    runtime, journal, manager, _, _, _ = executor_fixture(tmp_path, work=None)
    try:
        module = import_module("loom_capacity_executor.journal_retention")
        monkeypatch.setattr(module, "CHECKPOINT_TRIGGER_BYTES", 0, raising=False)
        heartbeats = []

        async def heartbeat(value):
            heartbeats.append(value)
            manager.journal_sequence = value.journal_sequence
            manager.journal_digest = value.journal_digest
            return SimpleNamespace(heartbeat_sequence=value.heartbeat_sequence,
                lease_expires_at=datetime.now(UTC) + timedelta(minutes=5), replayed=False, executable=True)

        manager.heartbeat_executable_executor = heartbeat
        for _ in range(6):
            await runtime._publish_inventory(await manager.executable_checkpoint())
        for _ in range(3):
            previous_sequence = manager.inventory_sequence
            assert await module.maintain_runtime_journal(runtime) == "compacted"
            latest = journal.latest("inventory", str(runtime.registration.executor_incarnation))
            assert manager.inventory_sequence == previous_sequence + 1
            assert heartbeats[-1].journal_sequence == latest.sequence
            assert journal.pending_checkpoint() is None
            assert len(tuple(tmp_path.glob("*.snapshot-*"))) == 1
        assert not manager.releases
    finally:
        journal.close()


async def test_runtime_checkpoint_capacity_pressure_preserves_history_for_drain(tmp_path, monkeypatch):
    runtime, journal, manager, _, _, _ = executor_fixture(tmp_path, work=None)
    try:
        module = import_module("loom_capacity_executor.journal_retention")
        for _ in range(4):
            await runtime._publish_inventory(await manager.executable_checkpoint())
        monkeypatch.setattr(module, "CHECKPOINT_TRIGGER_BYTES", 0, raising=False)
        monkeypatch.setattr("loom_capacity_executor.journal._MAX_JOURNAL_BYTES", journal.path.stat().st_size + 1024)
        before = journal.head
        assert await module.maintain_runtime_journal(runtime) == "capacity-constrained"
        assert journal.head == before
        assert not tuple(tmp_path.glob("*.snapshot-*"))
    finally:
        journal.close()
