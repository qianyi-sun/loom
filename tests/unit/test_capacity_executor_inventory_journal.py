"""Complete inventories survive bounded journal framing and ambiguous publication."""

from hashlib import sha256
from importlib import import_module
from unittest.mock import AsyncMock

import pytest

from loom_capacity_executor.journal import ExecutorJournal, JournalError
from loom_capacity_manager.executable_contracts import canonical_executable_bytes
from loom_capacity_manager.typed_inventory_contracts import inventory_confirmation_journal_head
from tests.unit.test_capacity_executor_executable import _job_from_launch, _terminal_from_job
from tests.unit.test_capacity_executor_typed_journal import facts, typed_executor
from tests.unit.test_capacity_typed_inventory_contracts import typed_inventory


def large_inventory():
    value = typed_inventory()
    return value.model_copy(update={"records": tuple(
        value.records[0].model_copy(update={"physical_identity": f"job-{index:05d}"})
        for index in range(200)
    )})


def test_large_inventory_retention_reopens_and_matches_manager_confirmation(tmp_path):
    module = import_module("loom_capacity_executor.inventory_journal")
    value = large_inventory()
    assert len(canonical_executable_bytes(value)) > 64 * 1024
    path = tmp_path / "journal"
    with ExecutorJournal(path) as journal:
        module.retain_inventory_request(journal, value)
        requested = journal.latest("inventory", str(value.executor_incarnation))
        assert module.load_journal_inventory(journal, requested) == value
        module.complete_inventory_request(journal, value, rejected=False)
        assert (journal.head.sequence, journal.head.digest) == inventory_confirmation_journal_head(value)
    assert max(map(len, path.read_bytes().splitlines())) < 64 * 1024
    with ExecutorJournal(path) as journal:
        assert module.load_journal_inventory(
            journal, journal.latest("inventory", str(value.executor_incarnation))
        ) == value
        assert not journal.pending_requests()


def test_inventory_capacity_refusal_leaves_no_partial_request(tmp_path, monkeypatch):
    module = import_module("loom_capacity_executor.inventory_journal")
    monkeypatch.setattr("loom_capacity_executor.journal._MAX_JOURNAL_BYTES", 70_000)
    with ExecutorJournal(tmp_path / "journal") as journal:
        with pytest.raises(JournalError, match="capacity"):
            module.retain_inventory_request(journal, large_inventory())
        assert journal.head.sequence == 0
        assert journal.path.read_bytes() == b""


@pytest.mark.parametrize("ambiguous", (False, True))
async def test_runtime_large_inventory_terminal_readback_and_exact_replay(tmp_path, ambiguous):
    runtime, journal, context = typed_executor(tmp_path)
    try:
        subject = facts(context)
        rendered = runtime.render_launch(context.binding, launch_subject=subject)
        runtime._remember_launch(rendered, launch_subject=subject,
            bootstrap_registration_epoch=1, event="slurm-submit-confirmed")
        job = _job_from_launch(context, rendered_request=rendered.request, job_id="101")
        runtime.slurm.terminal_jobs = (_terminal_from_job(job),)
        runtime.slurm.jobs = [job.model_copy(update={"job_id": str(1000 + index)}) for index in range(300)]
        checkpoint = await runtime.client.executable_checkpoint()
        original = runtime.client.ingest_executable_inventory
        sent = []

        async def lose_response(value):
            sent.append(value)
            await original(value)
            raise TimeoutError("response lost after durable ingestion")

        if ambiguous:
            runtime.client.ingest_executable_inventory = lose_response
            with pytest.raises(TimeoutError):
                await runtime._publish_inventory(checkpoint)
            runtime.client.ingest_executable_inventory = original
            runtime.slurm.inventory = AsyncMock(side_effect=AssertionError("must replay, not resample"))
            result = await runtime._replay_inventory_request(await runtime.client.executable_checkpoint())
            assert runtime.client.inventories[-1] == sent[0]
        else:
            result = await runtime._publish_inventory(checkpoint)
        assert result.status == "inventory-published"
        inventory = runtime.client.inventories[-1]
        assert len(canonical_executable_bytes(inventory)) > 64 * 1024
        assert runtime._confirmed_terminal_inventory_record(context.binding).state == "terminal"
        assert (journal.head.sequence, journal.head.digest) == inventory_confirmation_journal_head(inventory)
        assert not journal.pending_requests()
    finally:
        journal.close()


def test_small_typed_inventory_keeps_existing_inline_chain(tmp_path):
    module = import_module("loom_capacity_executor.inventory_journal")
    value = typed_inventory()
    payload = canonical_executable_bytes(value)
    with ExecutorJournal(tmp_path / "journal") as journal:
        module.retain_inventory_request(journal, value)
        module.complete_inventory_request(journal, value, rejected=False)
        records = journal.records("inventory", str(value.executor_incarnation))
        assert len(records) == 2
        assert all(record.durable_payload() == payload and record.payload_digest == sha256(payload).hexdigest()
            for record in records)
