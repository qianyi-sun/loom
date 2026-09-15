"""Runtime retention keeps recovery evidence while reclaiming repeated telemetry."""

from datetime import UTC, datetime, timedelta
from hashlib import sha256
from importlib import import_module
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


@pytest.mark.parametrize("lost_inventory_response", (False, True))
async def test_runtime_checkpoint_maintenance_republishes_retirement_inventory(tmp_path, monkeypatch, lost_inventory_response):
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
        if lost_inventory_response:
            publish = manager.ingest_executable_inventory

            async def lose_reply(value):
                await publish(value)
                raise ConnectionError("inventory response lost")

            manager.ingest_executable_inventory = lose_reply
            with pytest.raises(ConnectionError):
                await module.maintain_runtime_journal(runtime)
            manager.ingest_executable_inventory = publish
            assert journal.pending_checkpoint() is None
            # The normal size trigger is no reason to skip unfinished final
            # inventory replay after the checkpoint itself has been published.
            monkeypatch.setattr(module, "CHECKPOINT_TRIGGER_BYTES", 64 * 1024 * 1024)
            assert await module.maintain_runtime_journal(runtime) == "compacted"
            monkeypatch.setattr(module, "CHECKPOINT_TRIGGER_BYTES", 0)
        for _ in range(3):
            previous_sequence = manager.inventory_sequence
            assert await module.maintain_runtime_journal(runtime) == "compacted"
            latest = journal.latest("inventory", str(runtime.registration.executor_incarnation))
            assert manager.inventory_sequence == previous_sequence + 1
            assert heartbeats[-1].journal_sequence == latest.sequence
            assert journal.pending_checkpoint() is None
            assert len(tuple(tmp_path.glob("*.snapshot-*"))) == 1
        assert not manager.releases
        # Retained snapshot bytes remain part of the capacity budget, but are
        # not fresh journal growth that can justify another maintenance-only tick.
        monkeypatch.setattr(module, "CHECKPOINT_TRIGGER_BYTES", journal.path.stat().st_size + 1)
        assert journal._footprint() > module.CHECKPOINT_TRIGGER_BYTES
        assert await module.maintain_runtime_journal(runtime) == "not-needed"
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


async def test_pressure_drain_poll_publishes_inventory_without_requesting_new_admission(tmp_path):
    runtime, journal, manager, _, slurm, _ = executor_fixture(tmp_path, work=None)
    try:
        calls = []

        async def select(command_sequence, *, cleanup_only=False):
            calls.append(cleanup_only)
            if not cleanup_only:
                raise AssertionError("new admission would hide cleanup inventory")
            return None

        manager.next_executable_work = select
        result = await runtime.tick_drain_only()
        assert result.status == "inventory-published"
        assert calls == [True]
        assert len(manager.inventories) == 1
        assert slurm.submit_count == 0
    finally:
        journal.close()


@pytest.mark.parametrize("confirmed", [False, True])
async def test_native_delivery_survives_checkpoint_until_exact_release(tmp_path, confirmed):
    from loom_capacity_executor.native_bootstrap_delivery import expected_native_delivery_receipt
    from loom_capacity_executor.native_bootstrap_outbox import NativeBootstrapOutbox
    from loom_capacity_manager.executable_contracts import canonical_executable_bytes
    from tests.unit.test_capacity_executor_executable import (
        _release_work,
        launch_context_fixture,
        permit_fixture,
    )

    context = launch_context_fixture()
    runtime, journal, manager, _, _, _ = executor_fixture(tmp_path, work=permit_fixture(context.binding))
    try:
        await runtime.tick()
        physical = runtime._physical_binding(runtime._load_launch(context.binding.intent_id))

        class Client:
            async def deliver(self, raw):
                if not confirmed:
                    raise ConnectionError("receiver offline")
                return expected_native_delivery_receipt(raw)

            async def observe_receipt(self, raw):
                raise AssertionError("first delivery only")

        owner = NativeBootstrapOutbox(journal=journal, store=runtime._bootstrap_handoff_store,
            clients={physical.binding.node_ids[0]: Client()}, configuration_sha256="a" * 64, now=runtime._now)
        if confirmed:
            await owner.deliver(physical)
        else:
            with pytest.raises(ConnectionError):
                await owner.deliver(physical)
        # A local delivery intent must not prevent heartbeat/cleanup RPCs or
        # compaction while the receiver is offline.
        assert journal.pending_requests() == ()
        module = import_module("loom_capacity_executor.journal_retention")
        plan = module.plan_runtime_checkpoint(runtime, await manager.executable_checkpoint())
        delivery_records = journal.records("executor", "native-delivery:" + str(context.binding.intent_id))
        assert all(record.sequence in plan.retained_sequences for record in delivery_records)
        anchor = plan.prepare(journal)
        journal.commit_checkpoint(central_sequence=anchor.sequence, central_digest=anchor.record_digest)
        manager.journal_sequence, manager.journal_digest = anchor.sequence, anchor.record_digest
        journal.close()
        journal.__enter__()
        assert journal.records("executor", delivery_records[0].object_id) == delivery_records
        # Retention consumes a manager-acknowledged exact release, never an empty
        # inventory or a delivery receipt standing in for worker completion.
        release = _release_work(context.binding, command_sequence=manager.command_sequence + 1,
            inventory_sequence=1, terminal_evidence_sha256="b" * 64, protected_release_sha256="c" * 64)
        payload = canonical_executable_bytes(release)
        journal.append("reservation-release-confirmed", sha256(payload).hexdigest(),
            object_kind="tranche", object_id=str(release.tranche_id), payload=payload)
        manager.command_sequence = release.command_sequence
        plan = module.plan_runtime_checkpoint(runtime, await manager.executable_checkpoint())
        assert not any(record.sequence in plan.retained_sequences for record in delivery_records)
        # A delivery written after release is reopened lifecycle history.
        record = delivery_records[-1]
        journal.append(record.event_kind, record.payload_digest, object_kind=record.object_kind,
            object_id=record.object_id, payload=record.durable_payload())
        with pytest.raises(JournalRegressionError, match="reopened"):
            module.plan_runtime_checkpoint(runtime, await manager.executable_checkpoint())
    finally:
        journal.close()
