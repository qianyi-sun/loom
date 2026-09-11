"""Pinned routing and authenticated manager purpose agree before positive work."""

from unittest.mock import AsyncMock

import pytest

from loom_capacity_executor.journal import JournalRegressionError
from tests.unit.test_capacity_executor_executable import close_fixture
from tests.unit.test_capacity_executor_typed_journal import facts, typed_executor


@pytest.mark.parametrize("pool", ["oldlab", "gb10"])
@pytest.mark.parametrize("purpose", ["application-worker", "personal-build-worker"])
@pytest.mark.parametrize("operation", ["render", "prepare"])
async def test_purpose_mismatch_rejects_before_positive_side_effects(tmp_path, pool, purpose, operation):
    runtime, journal, context = typed_executor(tmp_path, pool=pool, purpose=purpose)
    try:
        if operation == "prepare":
            await runtime._propose_bootstrap(context.binding, command_sequence=1)
        runtime.admission.purpose = lambda binding: (
            "application-worker" if purpose == "personal-build-worker" else "personal-build-worker"
        )
        runtime.admission.prepare_worker = AsyncMock(side_effect=AssertionError("must not prepare"))
        head = journal.head
        with pytest.raises((ValueError, JournalRegressionError), match="purpose"):
            if operation == "render":
                runtime.render_launch(context.binding, launch_subject=facts(context))
            else:
                await runtime._prepare_protected_bootstrap(context.binding, bootstrap_registration_epoch=1,
                    bootstrap_evidence_sha256="a" * 64)
        assert journal.head == head
        runtime.admission.prepare_worker.assert_not_awaited()
        assert runtime.slurm.submit_count == 0
    finally:
        journal.close()


async def test_retained_launch_rejects_changed_admission_purpose_without_manager_lookup(tmp_path):
    runtime, journal, context = typed_executor(tmp_path)
    try:
        subject = facts(context)
        rendered = runtime.render_launch(context.binding, launch_subject=subject)
        runtime._remember_launch(rendered, bootstrap_registration_epoch=1,
            event="slurm-submit-requested", launch_subject=subject)
        runtime.admission.purpose = lambda binding: "application-worker"
        runtime.client.launch_subject = AsyncMock(side_effect=AssertionError("no fresh lookup"))
        with pytest.raises(JournalRegressionError):
            runtime._load_launch(context.binding.intent_id)
        runtime.client.launch_subject.assert_not_awaited()
    finally:
        journal.close()


@pytest.mark.parametrize("cleanup", [False, True])
async def test_pending_preparation_revalidates_purpose_but_cleanup_needs_no_launch_facts(tmp_path, cleanup):
    runtime, journal, context = typed_executor(tmp_path)
    try:
        binding = context.binding
        await runtime._propose_bootstrap(binding, command_sequence=1)
        close = close_fixture(binding, command_sequence=2)
        runtime.admission.prepare_worker = AsyncMock(side_effect=RuntimeError("lost reply"))
        with pytest.raises(RuntimeError, match="lost reply"):
            await runtime._prepare_protected_bootstrap(binding, bootstrap_registration_epoch=1,
                bootstrap_evidence_sha256=close.bootstrap_evidence_sha256)
        runtime.admission.prepare_worker.reset_mock()
        runtime.client.launch_subject.reset_mock()
        if cleanup:
            runtime.client.work = close
            runtime.client.launch_subject.side_effect = AssertionError("expired permit")
            result = await runtime._replay_local_request(await runtime._checkpoint())
            assert result.status == "draining"
            runtime.client.launch_subject.assert_not_awaited()
        else:
            runtime.admission.purpose = lambda binding: "application-worker"
            with pytest.raises((ValueError, JournalRegressionError), match="purpose"):
                await runtime._replay_local_request(await runtime._checkpoint())
        runtime.admission.prepare_worker.assert_not_awaited()
        assert runtime.slurm.submit_count == 0
    finally:
        journal.close()
