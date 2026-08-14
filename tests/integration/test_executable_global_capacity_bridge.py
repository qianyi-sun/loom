"""Real two-pool, multi-owner proof for executable global capacity."""

from __future__ import annotations

import json

import httpx
import pytest

from loom_capacity_agent.executable_admission import ExecutableAdmissionError
from loom_capacity_agent.store import CapacityAgentStoreError
from loom_capacity_executor.client import ExecutorRejectedError, ExecutorTransportError
from loom_capacity_executor.slurm_backend import (
    SlurmCommandError,
    SlurmSubmissionUncertainError,
)
from loom_capacity_manager.store import ExecutionConflictError
from tests.support.executable_capacity_harness import ExecutableCapacityHarness

pytestmark = pytest.mark.asyncio


async def test_two_owners_share_both_pools_without_cross_binding(
    executable_capacity_harness: ExecutableCapacityHarness,
) -> None:
    alice = await executable_capacity_harness.add_owner("alice", "a" * 64)
    bob = await executable_capacity_harness.add_owner("bob", "b" * 64)
    await alice.publish_x86_demand(1)
    await bob.publish_arm_demand(1)
    await executable_capacity_harness.converge()
    assert executable_capacity_harness.oldlab.owner_slots(alice.subject_id) == 1
    assert executable_capacity_harness.gb10.owner_slots(bob.subject_id) == 1
    assert executable_capacity_harness.cross_owner_bindings() == []


async def test_neutral_fairness_is_stable_and_complete_work_scales_to_zero(
    executable_capacity_harness: ExecutableCapacityHarness,
) -> None:
    alice = await executable_capacity_harness.add_owner("alice", "a" * 64)
    bob = await executable_capacity_harness.add_owner("bob", "b" * 64)
    await alice.publish_neutral_demand(2)
    await bob.publish_neutral_demand(2)

    await executable_capacity_harness.converge()
    first = await executable_capacity_harness.canonical_digests()
    order = executable_capacity_harness.progressive_owner_order()

    assert executable_capacity_harness.owner_slots(alice.subject_id) == 2
    assert executable_capacity_harness.owner_slots(bob.subject_id) == 2
    assert len(order) == 2
    assert set(order) == {"alice", "bob"}
    commitments = await executable_capacity_harness.manager_commitments()
    assert {item.subject_id for item in commitments} == {
        alice.subject_id,
        bob.subject_id,
    }
    assert {item.pool_id for item in commitments} == {"oldlab", "gb10"}
    assert {item.state for item in commitments} == {"observed"}
    assert sum(item.concurrency_slots for item in commitments) == 4
    protected = await executable_capacity_harness.protected_worker_registrations()
    assert {item.binding.subject_id for item in protected} == {
        alice.subject_id,
        bob.subject_id,
    }
    assert {item.slurm_job_id for item in protected} == {
        executable_capacity_harness.oldlab.latest_loom_job_id(),
        executable_capacity_harness.gb10.latest_loom_job_id(),
    }
    assert len(executable_capacity_harness.static_subject_ids) == 2

    await executable_capacity_harness.converge()
    second = await executable_capacity_harness.canonical_digests()
    assert second.allocation == first.allocation
    assert second.inventory == first.inventory
    assert second.evidence == first.evidence
    assert await executable_capacity_harness.inventory_pipeline_is_exact("oldlab")
    assert await executable_capacity_harness.inventory_pipeline_is_exact("gb10")
    assert executable_capacity_harness.total_loom_jobs() == 2

    await executable_capacity_harness.claim_all()
    assert await executable_capacity_harness.protected_live_claim_count() == 4
    await executable_capacity_harness.complete_all_claims()
    assert await executable_capacity_harness.protected_live_claim_count() == 0
    await executable_capacity_harness.scale_to_zero()
    final = await executable_capacity_harness.canonical_digests()

    assert executable_capacity_harness.execution_state == "shadow"
    assert executable_capacity_harness.total_loom_jobs() == 0
    assert await executable_capacity_harness.manager_commitments() == ()
    assert executable_capacity_harness.oldlab.inventory_records() == ()
    assert executable_capacity_harness.gb10.inventory_records() == ()
    assert final.inventory != first.inventory
    assert final.evidence != first.evidence
    print(
        "CANONICAL_DIGEST fairness "
        f"allocation={first.allocation} inventory={final.inventory} "
        f"evidence={final.evidence}"
    )


async def test_failure_matrix_keeps_uncertainty_pool_and_owner_scoped(
    executable_capacity_harness: ExecutableCapacityHarness,
) -> None:
    alice = await executable_capacity_harness.add_owner("alice", "a" * 64)
    bob = await executable_capacity_harness.add_owner("bob", "b" * 64)
    await alice.publish_x86_demand(1)
    await bob.publish_arm_demand(1)
    await executable_capacity_harness.activate_next_epoch()
    await executable_capacity_harness.reconcile()
    await executable_capacity_harness.drive_pool("gb10")

    assert executable_capacity_harness.distinct_controller_authorities()
    foreign_job = executable_capacity_harness.oldlab.add_foreign_job("9001")
    foreign_before = executable_capacity_harness.oldlab.job_snapshot(foreign_job)
    assert foreign_job in executable_capacity_harness.oldlab.unscoped_query_job_ids()
    assert foreign_job not in await executable_capacity_harness.oldlab.scoped_inventory_job_ids()
    gb10_before = executable_capacity_harness.gb10.loom_job_snapshot()

    submissions_before = len(executable_capacity_harness.oldlab.sbatch_calls)
    executable_capacity_harness.oldlab.fail_submission_after_mutation()
    with pytest.raises(SlurmSubmissionUncertainError):
        await executable_capacity_harness.drive_pool("oldlab")
    executable_capacity_harness.oldlab.clear_command_fault("sbatch")
    uncertain_job = executable_capacity_harness.oldlab.latest_loom_job_id()

    await executable_capacity_harness.restart_executor("oldlab")
    recovered = await executable_capacity_harness.recover_pool("oldlab")
    assert recovered.status == "adopted"
    assert len(executable_capacity_harness.oldlab.sbatch_calls) == submissions_before + 1
    assert executable_capacity_harness.gb10.loom_job_snapshot() == gb10_before

    executable_capacity_harness.oldlab.fail_command("squeue")
    with pytest.raises(SlurmCommandError):
        await executable_capacity_harness.oldlab.tick()
    await executable_capacity_harness.gb10.tick()
    executable_capacity_harness.oldlab.clear_command_fault("squeue")
    assert executable_capacity_harness.gb10.loom_job_snapshot() == gb10_before
    assert executable_capacity_harness.cross_owner_bindings() == []

    exact_job = executable_capacity_harness.oldlab.job_snapshot(uncertain_job)
    executable_capacity_harness.oldlab.replace_job(
        uncertain_job,
        cpus=exact_job["cpus"] + 1,
    )
    published = await executable_capacity_harness.oldlab.tick()
    assert published.status == "inventory-published"
    mismatch = await executable_capacity_harness.manager_intent_evidence(
        "oldlab",
        uncertain_job,
    )
    assert mismatch.state == "quarantined"
    assert mismatch.concurrency_slots == 1
    assert await executable_capacity_harness.charged_slots() == 2
    assert executable_capacity_harness.oldlab.owner_slots(alice.subject_id) == 1
    assert executable_capacity_harness.gb10.owner_slots(bob.subject_id) == 1
    assert executable_capacity_harness.cross_owner_bindings() == []
    executable_capacity_harness.oldlab.restore_job(uncertain_job, exact_job)

    with pytest.raises(ExecutableAdmissionError):
        await executable_capacity_harness.prepare_with_wrong_candidate(alice)

    charged_before = await executable_capacity_harness.charged_slots()
    oldlab_before = executable_capacity_harness.oldlab.loom_job_snapshot()
    await executable_capacity_harness.stop_manager()
    with pytest.raises(ExecutorTransportError):
        await executable_capacity_harness.gb10.tick()
    assert await executable_capacity_harness.charged_slots() == charged_before
    assert executable_capacity_harness.oldlab.loom_job_snapshot() == oldlab_before
    assert executable_capacity_harness.oldlab.job_snapshot(foreign_job) == foreign_before
    assert foreign_job not in executable_capacity_harness.oldlab.cancelled_job_ids()


async def test_redeploy_delete_and_drain_require_full_epoch_turnover(
    executable_capacity_harness: ExecutableCapacityHarness,
) -> None:
    alice = await executable_capacity_harness.add_owner("alice", "a" * 64)
    bob = await executable_capacity_harness.add_owner("bob", "b" * 64)
    await alice.publish_x86_demand(1)
    await bob.publish_arm_demand(1)
    await executable_capacity_harness.converge()
    foreign_job = executable_capacity_harness.oldlab.add_foreign_job("9002")
    foreign_before = executable_capacity_harness.oldlab.job_snapshot(foreign_job)
    projections_before = await executable_capacity_harness.development_projection_count()

    with pytest.raises(httpx.HTTPStatusError) as active_redeploy:
        await executable_capacity_harness.redeploy_owner(alice, "c" * 64)
    assert active_redeploy.value.response.status_code == 503
    assert active_redeploy.value.request.url.path == (
        f"/v1/development-projections/{alice.subject_id}"
    )
    assert json.loads(active_redeploy.value.request.content)["operation_kind"] == "update"
    assert await executable_capacity_harness.development_projection_count() == projections_before
    with pytest.raises(httpx.HTTPStatusError) as active_delete:
        await executable_capacity_harness.delete_owner(bob)
    assert active_delete.value.response.status_code == 503
    assert active_delete.value.request.url.path == (f"/v1/development-projections/{bob.subject_id}")
    assert json.loads(active_delete.value.request.content)["operation_kind"] == "destroy"
    assert await executable_capacity_harness.development_projection_count() == projections_before

    await executable_capacity_harness.begin_drain()
    with pytest.raises(ExecutionConflictError):
        await executable_capacity_harness.retire()
    assert await executable_capacity_harness.charged_slots() == 2
    await executable_capacity_harness.finish_drain_and_retire()

    old_candidate = alice.candidate
    old_agent_registration = alice.registration
    old_agent_configuration = old_agent_registration.configuration_generation
    old_reporter = alice.reporter_incarnation
    old_executor = executable_capacity_harness.executor_identity("oldlab")
    await executable_capacity_harness.redeploy_owner(alice, "c" * 64)
    with pytest.raises(CapacityAgentStoreError):
        await executable_capacity_harness.reconfigure_stale_agent(
            alice,
            old_agent_registration,
            expected_configuration_generation=old_agent_configuration,
        )
    assert (
        await executable_capacity_harness.protected_agent_registration(alice) == alice.registration
    )
    await bob.publish_zero_demand()
    await alice.publish_x86_demand(1)
    await executable_capacity_harness.activate_next_epoch()
    await executable_capacity_harness.reconcile()
    next_binding = await executable_capacity_harness.accept_next_binding("oldlab")

    with pytest.raises(ExecutableAdmissionError):
        await executable_capacity_harness.prepare_binding_with_candidate(
            alice,
            next_binding,
            old_candidate,
        )
    with pytest.raises(ExecutorRejectedError):
        await executable_capacity_harness.heartbeat_stale_executor(old_executor)

    await executable_capacity_harness.converge()
    await executable_capacity_harness.scale_to_zero()
    assert alice.reporter_incarnation != old_reporter

    await executable_capacity_harness.delete_owner(bob)
    await alice.publish_zero_demand()
    await executable_capacity_harness.activate_next_epoch()
    assert bob.subject_id not in await executable_capacity_harness.active_subject_ids()
    with pytest.raises(httpx.HTTPStatusError) as stale_reporter:
        await bob.publish_zero_demand()
    assert stale_reporter.value.response.status_code == 401 or (
        stale_reporter.value.response.status_code == 403
    )

    await executable_capacity_harness.scale_to_zero()
    final = await executable_capacity_harness.canonical_digests()
    assert executable_capacity_harness.oldlab.job_snapshot(foreign_job) == foreign_before
    assert foreign_job not in executable_capacity_harness.oldlab.cancelled_job_ids()
    assert executable_capacity_harness.cross_owner_bindings() == []
    print(
        "CANONICAL_DIGEST lifecycle "
        f"allocation={final.allocation} inventory={final.inventory} "
        f"evidence={final.evidence}"
    )
