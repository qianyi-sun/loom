"""Cancelled native work can revoke before first preparation, never after submit.

Exercise the close consumer directly: public typed runtime remains interlocked
until the remaining claim, release and contained-launch consumers are complete.
"""

from datetime import timedelta
from hashlib import sha256
from unittest.mock import AsyncMock

import pytest

from loom_capacity_executor.journal import JournalRegressionError
from tests.unit.test_capacity_executor_executable import close_fixture
from tests.unit.test_capacity_executor_typed_journal import typed_executor


@pytest.mark.parametrize("pool", ["oldlab", "gb10"])
@pytest.mark.parametrize("lost_reply", [False, True])
async def test_native_close_revokes_without_fresh_preparation_after_expiry(tmp_path,pool,lost_reply):
    runtime,journal,context = typed_executor(tmp_path,pool=pool)
    try:
        binding = context.binding
        runtime.admission.purpose = lambda supplied: "personal-build-worker"
        await runtime._propose_bootstrap(binding,command_sequence=1)
        runtime._now = lambda: context.submitted_at + timedelta(days=1)
        runtime.admission.prepare_worker = AsyncMock(side_effect=RuntimeError("source cancelled or expired"))
        close = close_fixture(binding,command_sequence=2)
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
    finally:
        journal.close()


@pytest.mark.parametrize("boundary", ["missing-proposal", "unknown-job", "orphan-physical"])
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
        runtime.admission.prepare_worker = AsyncMock(side_effect=AssertionError("must reject before preparation"))
        close = close_fixture(binding,command_sequence=2)
        runtime.client.work = close
        with pytest.raises(JournalRegressionError):
            await runtime._close(close,await runtime._checkpoint())
        assert runtime.admission.prepared_revocation_requests == []
        assert runtime.client.work == close
        assert runtime.slurm.submit_count == 0
    finally:
        journal.close()
