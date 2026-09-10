"""Delayed cleanup uses exact durable terminal proof after Slurm accounting expires."""

from uuid import UUID

import pytest

from loom_capacity_executor.executable import ProtectedIntentObservationV2
from tests.unit.test_capacity_executor_executable import (
    _job_from_launch,
    _protected_release_receipt,
    _release_work,
    _terminal_from_job,
    executor_fixture,
    launch_context_fixture,
    permit_fixture,
)
from tests.unit.test_capacity_executor_typed_journal import facts, typed_executor


@pytest.mark.parametrize("typed", (False, True))
@pytest.mark.parametrize("tamper", ("none", "sequence", "digest", "live", "binding", "duplicate-terminal"))
async def test_delayed_release_requires_exact_historical_terminal_and_protected_fences(tmp_path, typed, tamper):
    if typed:
        runtime, journal, context = typed_executor(tmp_path)
        subject = facts(context)
        rendered = runtime.render_launch(context.binding, launch_subject=subject)
        runtime._remember_launch(rendered, launch_subject=subject,
            bootstrap_registration_epoch=1, event="slurm-submit-confirmed")
        job = _job_from_launch(context, rendered_request=rendered.request, job_id="101")
    else:
        context = launch_context_fixture()
        runtime, journal, _manager, _admission, _slurm, _launch = executor_fixture(
            tmp_path, work=permit_fixture(context.binding))
        await runtime.tick()
        job = runtime.slurm.jobs.pop()
    try:
        terminal = _terminal_from_job(job)
        runtime.slurm.terminal_jobs = (terminal,)
        await runtime._publish_inventory(await runtime.client.executable_checkpoint())
        inventory = runtime.client.inventories[-1]
        assert inventory.records[0].state == "terminal"
        runtime.slurm.terminal_jobs = ()
        await runtime._publish_inventory(await runtime.client.executable_checkpoint())
        assert runtime.client.inventories[-1].records == ()
        protected_digest = "1" * 64
        runtime.admission.observations[context.binding.intent_id] = ProtectedIntentObservationV2(
            binding=context.binding, bootstrap_registration_epoch=1,
            worker_id=UUID(int=701), worker_incarnation=UUID(int=702),
            protected_registration_epoch=2, claim_high_water=0,
            release=_protected_release_receipt(context.binding, digest=protected_digest))
        release = _release_work(context.binding,
            command_sequence=runtime.client.command_sequence + 1,
            inventory_sequence=inventory.inventory_sequence + (tamper == "sequence"),
            terminal_evidence_sha256="b" * 64 if tamper == "digest" else runtime._slurm_evidence_digest(terminal),
            protected_release_sha256=protected_digest)
        if tamper == "live":
            runtime.slurm.jobs = [job]
        if tamper == "duplicate-terminal":
            runtime.slurm.terminal_jobs = (terminal, terminal)
        if tamper == "binding":
            item = release.releases[0]
            release = release.model_copy(update={"releases": (item.model_copy(update={
                "binding": item.binding.model_copy(update={"deployment_generation": 999})}),)})
        if tamper == "duplicate-terminal":
            with pytest.raises(ValueError, match="duplicate Slurm terminal job"):
                await runtime._release(release)
            assert not runtime.client.releases
            return
        result = await runtime._release(release)
        assert result.status == ("released" if tamper == "none" else "quarantined")
        assert len(runtime.client.releases) == (tamper == "none")
    finally:
        journal.close()
