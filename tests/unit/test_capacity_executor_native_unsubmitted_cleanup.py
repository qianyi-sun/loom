"""Cancelled native work can revoke before first preparation, never after submit.

Exercise the close consumer directly: public typed runtime remains interlocked
until the remaining claim, release and contained-launch consumers are complete.
"""

from datetime import timedelta
from hashlib import sha256
from unittest.mock import AsyncMock

import pytest

from loom_capacity_executor.bootstrap_handoff import BootstrapHandoffError
from loom_capacity_executor.journal import ExecutorJournal, JournalRegressionError
from loom_capacity_executor.journal_retention import plan_runtime_checkpoint
from loom_capacity_manager.executable_contracts import (
    ExecutableBootstrapRegistrationV2,
    canonical_executable_bytes,
    canonical_executable_digest,
)
from tests.unit.test_capacity_executor_executable import close_fixture
from tests.unit.test_capacity_executor_typed_journal import typed_executor


@pytest.mark.parametrize("pool", ["oldlab", "gb10"])
@pytest.mark.parametrize("lost_reply", [False, True])
@pytest.mark.parametrize("pending_preparation", [False, True])
async def test_native_close_revokes_without_fresh_preparation_after_expiry(tmp_path,pool,lost_reply,pending_preparation):
    runtime,journal,context = typed_executor(tmp_path,pool=pool)
    try:
        binding = context.binding
        runtime.admission.purpose = lambda supplied: "personal-build-worker"
        await runtime._propose_bootstrap(binding,command_sequence=1)
        runtime._now = lambda: context.submitted_at + timedelta(days=1)
        runtime.admission.prepare_worker = AsyncMock(side_effect=RuntimeError("source cancelled or expired"))
        close = close_fixture(binding,command_sequence=2)
        if pending_preparation:
            with pytest.raises(RuntimeError,match="source cancelled"):
                await runtime._prepare_protected_bootstrap(binding,bootstrap_registration_epoch=1,
                    bootstrap_evidence_sha256=close.bootstrap_evidence_sha256)
            runtime.admission.prepare_worker.reset_mock()
        runtime.client.work = close
        revoke = runtime.admission.revoke_prepared_bootstrap
        calls = []

        async def revoke_with_lost_reply(request):
            calls.append(request)
            receipt = await revoke(request)
            if lost_reply and len(calls) == 1:
                raise RuntimeError("revocation committed but reply lost")
            return receipt

        runtime.admission.revoke_prepared_bootstrap = revoke_with_lost_reply
        if lost_reply:
            with pytest.raises(RuntimeError,match="reply lost"):
                await runtime._close(close,await runtime._checkpoint())
            assert runtime.client.work == close
            record = journal.latest("prepared-revocation",str(binding.intent_id))
            assert record.event_kind == "protected-prepared-revocation-requested"
            journal.close()
            journal = ExecutorJournal(journal.path)
            journal.__enter__()
            runtime.journal = journal
            assert (await runtime._replay_local_request(await runtime._checkpoint())).status == "draining"
        result = await runtime._close(close,await runtime._checkpoint())
        assert result.status == "draining"
        assert runtime.client.work is None
        runtime.admission.prepare_worker.assert_not_awaited()
        assert runtime.slurm.submit_count == 0
        assert len(calls) == (2 if lost_reply else 1)
        assert calls[0] == calls[-1]
        revocation = journal.latest("prepared-revocation",str(binding.intent_id))
        central = journal.latest("intent",str(binding.intent_id))
        assert revocation.event_kind == "prepared-handoff-deleted"
        assert revocation.sequence < central.sequence
        assert central.event_kind == "intent-close-confirmed"
        assert journal.pending_requests() == ()
        if pending_preparation:
            assert journal.latest("bootstrap",str(binding.intent_id)).event_kind == "protected-bootstrap-revoked"
    finally:
        journal.close()


@pytest.mark.parametrize("stage", ["before-delete", "before-supersede", "after-supersede"])
async def test_native_cleanup_restarts_across_local_revocation_steps(tmp_path,stage):
    runtime,journal,context = typed_executor(tmp_path)
    try:
        binding = context.binding
        runtime.admission.purpose = lambda supplied: "personal-build-worker"
        await runtime._propose_bootstrap(binding,command_sequence=1)
        close = close_fixture(binding,command_sequence=2)
        runtime.admission.prepare_worker = AsyncMock(side_effect=RuntimeError("cancelled"))
        with pytest.raises(RuntimeError,match="cancelled"):
            await runtime._prepare_protected_bootstrap(binding,bootstrap_registration_epoch=1,
                bootstrap_evidence_sha256=close.bootstrap_evidence_sha256)
        runtime.admission.prepare_worker.reset_mock()
        runtime.client.work = close
        method = "_delete_prepared_handoff" if stage == "before-delete" else "_supersede_preparation"
        original = getattr(runtime,method)

        def crash(*args,**kwargs):
            if stage == "after-supersede":
                original(*args,**kwargs)
            raise RuntimeError("simulated process loss")

        setattr(runtime,method,crash)
        with pytest.raises(RuntimeError,match="process loss"):
            await runtime._close(close,await runtime._checkpoint())
        setattr(runtime,method,original)
        journal.close()
        journal = ExecutorJournal(journal.path)
        journal.__enter__()
        runtime.journal = journal
        await runtime._replay_local_request(await runtime._checkpoint())
        assert (await runtime._close(close,await runtime._checkpoint())).status == "draining"
        assert journal.pending_requests() == ()
        assert runtime.client.work is None
        assert len(runtime.admission.prepared_revocation_requests) == 1
        runtime.admission.prepare_worker.assert_not_awaited()
    finally:
        journal.close()


async def test_native_revoked_preparation_survives_checkpoint_and_reopen(tmp_path):
    runtime,journal,context = typed_executor(tmp_path)
    try:
        binding = context.binding
        runtime.admission.purpose = lambda supplied: "personal-build-worker"
        await runtime._publish_inventory(await runtime._checkpoint())
        await runtime._propose_bootstrap(binding,command_sequence=1)
        close = close_fixture(binding,command_sequence=2)
        runtime.admission.prepare_worker = AsyncMock(side_effect=RuntimeError("cancelled"))
        with pytest.raises(RuntimeError,match="cancelled"):
            await runtime._prepare_protected_bootstrap(binding,bootstrap_registration_epoch=1,
                bootstrap_evidence_sha256=close.bootstrap_evidence_sha256)
        runtime.client.work = close
        await runtime._close(close,await runtime._checkpoint())
        retained = [record for kind in ("bootstrap","prepared-revocation")
            for record in journal.records(kind,str(binding.intent_id))]
        plan = plan_runtime_checkpoint(runtime,await runtime._checkpoint())
        assert all(record.sequence in plan.retained_sequences for record in retained)
        checkpoint = plan.prepare(journal)
        journal.commit_checkpoint(central_sequence=checkpoint.sequence,central_digest=checkpoint.record_digest)
        journal.close()
        journal = ExecutorJournal(journal.path)
        journal.__enter__()
        runtime.journal = journal
        assert all(record in journal.records(record.object_kind,record.object_id) for record in retained)
        assert journal.pending_requests() == ()
    finally:
        journal.close()


@pytest.mark.parametrize("boundary", ["missing-proposal", "unknown-job", "orphan-physical", "ownership-sidecar", "missing-store"])
async def test_native_close_requires_durable_no_submission_evidence(tmp_path,boundary):
    runtime,journal,context = typed_executor(tmp_path)
    try:
        binding = context.binding
        runtime.admission.purpose = lambda supplied: "personal-build-worker"
        if boundary != "missing-proposal":
            await runtime._propose_bootstrap(binding,command_sequence=1)
        if boundary == "unknown-job":
            journal.append("unexpected-job-state",sha256(b"{}").hexdigest(),object_kind="job",
                object_id=str(binding.intent_id),payload=b"{}")
        elif boundary == "orphan-physical":
            journal.append("physical-bind-requested",sha256(b"{}").hexdigest(),object_kind="executor",
                object_id=f"physical-bind:{binding.intent_id}",payload=b"{}")
        elif boundary == "ownership-sidecar":
            store = runtime._bootstrap_handoff_store
            path = store.directory / store.reference_for(binding)
            path.with_suffix(".ownership").write_bytes(b"physical-evidence")
        elif boundary == "missing-store":
            runtime._bootstrap_handoff_store = None
        runtime.admission.prepare_worker = AsyncMock(side_effect=AssertionError("must reject before preparation"))
        close = close_fixture(binding,command_sequence=2)
        runtime.client.work = close
        with pytest.raises((JournalRegressionError,BootstrapHandoffError)):
            await runtime._close(close,await runtime._checkpoint())
        assert runtime.admission.prepared_revocation_requests == []
        assert runtime.client.work == close
        assert runtime.slurm.submit_count == 0
    finally:
        journal.close()


@pytest.mark.parametrize("boundary", ["binding", "epoch", "command"])
async def test_native_close_rejects_malformed_pending_pair_before_revocation(tmp_path,boundary):
    from uuid import uuid4

    runtime,journal,context = typed_executor(tmp_path)
    try:
        binding = context.binding
        runtime.admission.purpose = lambda supplied: "personal-build-worker"
        await runtime._propose_bootstrap(binding,command_sequence=1)
        close = close_fixture(binding,command_sequence=2)
        registration = ExecutableBootstrapRegistrationV2(
            binding=binding if boundary != "binding" else binding.model_copy(update={"intent_id":uuid4()}),
            command_sequence=99 if boundary == "command" else 1,
            bootstrap_registration_epoch=99 if boundary == "epoch" else 1,
            bootstrap_evidence_sha256=close.bootstrap_evidence_sha256)
        journal.append("protected-bootstrap-requested",canonical_executable_digest(registration),
            object_kind="bootstrap",object_id=str(binding.intent_id),payload=canonical_executable_bytes(registration))
        runtime.client.work = close
        with pytest.raises(JournalRegressionError):
            await runtime._close(close,await runtime._checkpoint())
        assert runtime.admission.prepared_revocation_requests == []
        assert runtime.client.work == close
        store = runtime._bootstrap_handoff_store
        assert (store.directory/store.reference_for(binding)).exists()
    finally:
        journal.close()
