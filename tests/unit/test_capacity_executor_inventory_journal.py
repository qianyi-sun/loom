"""Complete inventories survive bounded journal framing and ambiguous publication."""

from hashlib import sha256
from importlib import import_module
from unittest.mock import AsyncMock

import pytest

from loom_capacity_executor.journal import ExecutorJournal, JournalError, JournalRegressionError
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


def test_near_maximum_inventory_can_spend_recovery_reserve(tmp_path, monkeypatch):
    from loom_capacity_manager.contracts import MAX_CONTRACT_BYTES

    module = import_module("loom_capacity_executor.inventory_journal")
    value = typed_inventory()
    record = value.records[0].model_copy(update={"physical_identity": "job-00000"})
    empty = value.model_copy(update={"records": ()})
    count = (MAX_CONTRACT_BYTES - len(canonical_executable_bytes(empty))) // (
        len(canonical_executable_bytes(record)) + 1) - 1
    value = value.model_copy(update={"records": tuple(record.model_copy(update={
        "physical_identity": f"job-{index:05d}"}) for index in range(count))})
    payload = canonical_executable_bytes(value)
    assert MAX_CONTRACT_BYTES - 16_384 < len(payload) <= MAX_CONTRACT_BYTES
    # New admission cannot use this budget, but terminal publication can.
    monkeypatch.setattr("loom_capacity_executor.journal._MAX_JOURNAL_BYTES", 12 * 1024 * 1024)
    with ExecutorJournal(tmp_path / "journal") as journal:
        module.retain_inventory_request(journal, value)
        module.complete_inventory_request(journal, value, rejected=False)
        assert (journal.head.sequence, journal.head.digest) == inventory_confirmation_journal_head(value)


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


@pytest.mark.parametrize("crash_after", (1, 3))
def test_partial_chunks_cannot_publish_and_fresh_sample_uses_new_anchor(tmp_path, monkeypatch, crash_after):
    module = import_module("loom_capacity_executor.inventory_journal")
    path = tmp_path / "journal"
    value = large_inventory()
    with ExecutorJournal(path) as journal:
        original = journal.append

        def interrupted(*args, **kwargs):
            if journal.head.sequence == crash_after:
                raise OSError("crashed before request")
            return original(*args, **kwargs)

        monkeypatch.setattr(journal, "append", interrupted)
        with pytest.raises(OSError):
            module.retain_inventory_request(journal, value)
        assert journal.pending_requests() == ()
    with ExecutorJournal(path) as journal:
        value = value.model_copy(update={"journal_sequence": journal.head.sequence,
            "journal_digest": journal.head.digest})
        module.retain_inventory_request(journal, value)
        module.complete_inventory_request(journal, value, rejected=False)
        assert (journal.head.sequence, journal.head.digest) == inventory_confirmation_journal_head(value)


@pytest.mark.parametrize("tamper", ("missing", "order", "payload", "reference"))
def test_forged_chunk_batch_is_rejected_even_with_valid_journal_hashes(tmp_path, tamper):
    from loom_capacity_manager.typed_inventory_contracts import typed_inventory_journal_frames

    module = import_module("loom_capacity_executor.inventory_journal")
    frames = list(typed_inventory_journal_frames(large_inventory())[:-1])
    if tamper == "missing":
        del frames[0]
    elif tamper == "order":
        frames[0], frames[1] = frames[1], frames[0]
    elif tamper == "payload":
        event, kind, object_id, payload = frames[0]
        frames[0] = (event, kind, object_id, b"x" * len(payload))
    else:
        event, kind, object_id, payload = frames[-1]
        frames[-1] = (event, kind, object_id, payload.replace(b'"byte_count":', b'"extra":0,"byte_count":'))
    with ExecutorJournal(tmp_path / "journal") as journal:
        for event, kind, object_id, payload in frames:
            retained = journal.append(event, sha256(payload).hexdigest(), object_kind=kind,
                object_id=object_id, payload=payload)
        with pytest.raises(JournalRegressionError):
            module.load_journal_inventory(journal, retained)


def test_large_rejection_is_durable_and_resolves_request(tmp_path):
    module = import_module("loom_capacity_executor.inventory_journal")
    value = large_inventory()
    with ExecutorJournal(tmp_path / "journal") as journal:
        module.retain_inventory_request(journal, value)
        module.complete_inventory_request(journal, value, rejected=True)
        record = journal.latest("inventory", str(value.executor_incarnation))
        assert record.event_kind == "inventory-publish-rejected"
        assert module.load_journal_inventory(journal, record) == value
        assert not journal.pending_requests()


async def test_interleaved_request_refuses_rpc_before_unconfirmable_publication(tmp_path):
    module = import_module("loom_capacity_executor.inventory_journal")
    runtime, journal, _context = typed_executor(tmp_path)
    try:
        value = large_inventory().model_copy(update={"execution": runtime.registration.execution,
            "executor_id": runtime.registration.executor_id,
            "executor_incarnation": runtime.registration.executor_incarnation})
        module.retain_inventory_request(journal, value)
        journal.append("unrelated-retained", "a" * 64, object_kind="executor", object_id="other")
        runtime.client.ingest_executable_inventory = AsyncMock()
        with pytest.raises(JournalRegressionError, match="pending request"):
            await runtime._send_inventory(value)
        runtime.client.ingest_executable_inventory.assert_not_awaited()
    finally:
        journal.close()
