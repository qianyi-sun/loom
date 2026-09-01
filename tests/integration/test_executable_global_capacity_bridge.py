"""Real two-pool, multi-owner proof for executable global capacity."""

from __future__ import annotations

import hashlib
import json
import shutil
from pathlib import Path

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


async def test_harness_uses_public_runtime_and_trusted_process_entry(
    executable_capacity_harness: ExecutableCapacityHarness,
) -> None:
    alice = await executable_capacity_harness.add_owner("alice", "a" * 64)
    await alice.publish_x86_demand(1)
    await executable_capacity_harness.activate_next_epoch()

    assert executable_capacity_harness.pool_runtime_entry_components("oldlab") == {
        "runtime_artifact": "ActivationRuntimeArtifactV2",
        "admission_client": "RoutedExecutableAdmissionClient",
        "profile_count": 2,
        "trusted_launcher_config_verified": True,
    }

    await executable_capacity_harness.reconcile()
    submitted = await executable_capacity_harness.drive_pool("oldlab")
    assert submitted.operation_id is not None
    assert executable_capacity_harness.trusted_launcher_process_entry(
        "oldlab",
        submitted.operation_id,
    ) == {
        "process_argv_matches_submitted_slurm_argv": True,
        "candidate_exec_received_worker_credential": True,
    }


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
    executor_status = await executable_capacity_harness.executable_executor_status()
    alice_status = await executable_capacity_harness.executable_subject_status(alice.subject_id)
    bob_status = await executable_capacity_harness.executable_subject_status(bob.subject_id)
    assert executor_status["execution_state"] == "active"
    assert executor_status["blockers"] == []
    assert {item["pool_id"] for item in executor_status["items"]} == {
        "oldlab",
        "gb10",
    }
    assert all(item["blockers"] == [] for item in executor_status["items"])
    for status in (alice_status, bob_status):
        assert status["capacity_status"] == "waiting"
        assert status["worker_available"] is False
        assert status["active_capacity_intent_count"] == 1
        assert status["active_capacity_slots"] == 1
        assert status["blockers"] == ["worker-registration-pending"]


@pytest.mark.timeout(120)
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


async def test_prepared_unused_retirement_uses_protected_release_reporter_runtime(
    executable_capacity_harness: ExecutableCapacityHarness,
) -> None:
    alice = await executable_capacity_harness.add_owner("alice", "a" * 64)
    await alice.publish_x86_demand(1)
    await executable_capacity_harness.activate_next_epoch()
    await executable_capacity_harness.reconcile()

    binding = await executable_capacity_harness.prepare_unused_intent("oldlab")
    handoff_path = executable_capacity_harness.bootstrap_handoff_path("oldlab", binding)
    assert handoff_path.exists()
    assert executable_capacity_harness.oldlab.sbatch_calls == ()

    await executable_capacity_harness.begin_drain()
    drained = await executable_capacity_harness.drain_prepared_unused("oldlab", binding)
    assert drained.event_kind == "prepared-revoked"
    assert drained.executor_status == "draining"
    assert drained.manager_state == "closing"
    assert drained.terminal_kind == "unused"
    assert not handoff_path.exists()
    assert executable_capacity_harness.oldlab.sbatch_calls == ()
    assert executable_capacity_harness.oldlab.cancelled_job_ids() == ()

    await executable_capacity_harness.restart_executor("oldlab")
    outage = await executable_capacity_harness.publish_next_protected_release_with_replay(
        alice,
        manager_outage_before_first_publish=True,
        lose_response_after_manager_ack=True,
    )
    assert outage.event_kind == "prepared-revoked"
    assert outage.publish_attempts == 3
    assert len(set(outage.idempotency_keys)) == 1
    assert outage.idempotency_keys[1] == outage.idempotency_keys[2]
    assert outage.manager_replayed_flags == (False, True)
    assert outage.guard_publication_count == 1

    released = await executable_capacity_harness.release_retired_intent("oldlab", binding)
    final = await executable_capacity_harness.canonical_digests()
    assert released.status == "released"
    assert outage.release_digest == final.release
    assert executable_capacity_harness.execution_state == "shadow"
    assert executable_capacity_harness.total_loom_jobs() == 0
    assert await executable_capacity_harness.manager_commitments() == ()
    assert executable_capacity_harness.oldlab.inventory_records() == ()
    print(
        "CANONICAL_DIGEST prepared_unused "
        f"allocation={final.allocation} inventory={final.inventory} "
        f"release={final.release}"
    )


async def test_accepted_never_prepared_close_uses_cleanup_bootstrap_before_retirement(
    executable_capacity_harness: ExecutableCapacityHarness,
) -> None:
    alice = await executable_capacity_harness.add_owner("alice", "a" * 64)
    await alice.publish_x86_demand(1)
    await executable_capacity_harness.activate_next_epoch()
    await executable_capacity_harness.reconcile()

    binding = await executable_capacity_harness.accept_next_binding("oldlab")
    handoff_path = executable_capacity_harness.bootstrap_handoff_path("oldlab", binding)
    assert not handoff_path.exists()
    assert executable_capacity_harness.oldlab.sbatch_calls == ()

    await executable_capacity_harness.begin_drain()
    drained = await executable_capacity_harness.drain_accepted_cleanup_unused("oldlab", binding)
    assert drained.event_kind == "prepared-revoked"
    assert drained.executor_status == "draining"
    assert drained.manager_state == "closing"
    assert drained.terminal_kind == "unused"
    assert not handoff_path.exists()
    assert executable_capacity_harness.oldlab.sbatch_calls == ()
    assert executable_capacity_harness.oldlab.cancelled_job_ids() == ()

    published = await executable_capacity_harness.publish_next_protected_release_with_replay(alice)
    assert published.event_kind == "prepared-revoked"
    released = await executable_capacity_harness.release_retired_intent("oldlab", binding)

    assert released.status == "released"
    assert executable_capacity_harness.execution_state == "shadow"
    assert executable_capacity_harness.total_loom_jobs() == 0
    assert await executable_capacity_harness.manager_commitments() == ()
    assert executable_capacity_harness.oldlab.inventory_records() == ()


async def test_unregistered_withdrawn_retirement_cancels_only_exact_pending_job(
    executable_capacity_harness: ExecutableCapacityHarness,
) -> None:
    alice = await executable_capacity_harness.add_owner("alice", "a" * 64)
    await alice.publish_x86_demand(1)
    await executable_capacity_harness.activate_next_epoch()
    await executable_capacity_harness.reconcile()
    foreign_job = executable_capacity_harness.oldlab.add_foreign_job("9003")
    foreign_before = executable_capacity_harness.oldlab.job_snapshot(foreign_job)

    binding, job_id = await executable_capacity_harness.submit_unregistered_intent("oldlab")
    assert not executable_capacity_harness.worker_registered(binding.intent_id)
    assert executable_capacity_harness.trusted_launcher_process_count() == 0

    await executable_capacity_harness.begin_drain()
    drained = await executable_capacity_harness.drain_unregistered_withdrawn(
        "oldlab",
        binding,
        job_id,
    )
    assert drained.event_kind == "withdrawn"
    assert drained.executor_status == "pending-cancelled"
    assert drained.manager_state == "closing"
    assert drained.terminal_kind == "slurm-job"
    assert executable_capacity_harness.oldlab.cancelled_job_ids() == (job_id,)
    assert executable_capacity_harness.oldlab.job_snapshot(foreign_job) == foreign_before
    assert foreign_job not in executable_capacity_harness.oldlab.cancelled_job_ids()
    assert not executable_capacity_harness.worker_registered(binding.intent_id)

    await executable_capacity_harness.restart_executor("oldlab")
    published = await executable_capacity_harness.publish_next_protected_release_with_replay(
        alice,
        manager_outage_before_first_publish=True,
        lose_response_after_manager_ack=True,
    )
    assert published.event_kind == "withdrawn"
    assert published.publish_attempts == 3
    assert len(set(published.idempotency_keys)) == 1
    assert published.manager_replayed_flags == (False, True)
    assert published.guard_publication_count == 1

    released = await executable_capacity_harness.release_retired_intent("oldlab", binding)
    final = await executable_capacity_harness.canonical_digests()
    assert released.status == "released"
    assert published.release_digest == final.release
    assert executable_capacity_harness.execution_state == "shadow"
    assert executable_capacity_harness.oldlab.job_snapshot(foreign_job) == foreign_before
    assert await executable_capacity_harness.manager_commitments() == ()
    print(
        "CANONICAL_DIGEST unregistered_withdrawn "
        f"allocation={final.allocation} inventory={final.inventory} "
        f"release={final.release}"
    )


async def test_protected_release_reporter_delay_keeps_pools_isolated(
    executable_capacity_harness: ExecutableCapacityHarness,
) -> None:
    alice = await executable_capacity_harness.add_owner("alice", "a" * 64)
    bob = await executable_capacity_harness.add_owner("bob", "b" * 64)
    await alice.publish_x86_demand(1)
    await bob.publish_arm_demand(1)
    await executable_capacity_harness.activate_next_epoch()
    await executable_capacity_harness.reconcile()

    bob_binding, bob_job_id = await executable_capacity_harness.submit_unregistered_intent("gb10")
    alice_binding = await executable_capacity_harness.prepare_unused_intent("oldlab")
    await executable_capacity_harness.begin_drain()
    alice_drained = await executable_capacity_harness.drain_prepared_unused("oldlab", alice_binding)
    bob_drained = await executable_capacity_harness.drain_unregistered_withdrawn(
        "gb10",
        bob_binding,
        bob_job_id,
    )
    assert alice_drained.event_kind == "prepared-revoked"
    assert bob_drained.event_kind == "withdrawn"

    bob_delayed = await executable_capacity_harness.protected_intent_snapshot(
        "gb10",
        bob_binding,
    )
    assert bob_delayed.owner_name == "bob"
    assert bob_delayed.pool_id == "gb10"
    assert bob_delayed.manager_state == "closing"
    assert bob_delayed.protected_release_digest is None
    assert bob_delayed.terminal_kind == "slurm-job"
    assert not bob_delayed.worker_registered
    assert bob_delayed.live_job_states == ()
    assert bob_delayed.cancelled_job_ids == (bob_job_id,)

    alice_publication = (
        await executable_capacity_harness.publish_next_protected_release_with_replay(
            alice,
            manager_outage_before_first_publish=True,
            lose_response_after_manager_ack=True,
        )
    )
    assert alice_publication.guard_publication_count == 1
    assert alice_publication.publish_attempts == 3
    assert alice_publication.manager_replayed_flags == (False, True)
    assert (
        await executable_capacity_harness.protected_intent_snapshot("gb10", bob_binding)
        == bob_delayed
    )
    alice_published = await executable_capacity_harness.protected_intent_snapshot(
        "oldlab",
        alice_binding,
    )
    assert alice_published.owner_name == "alice"
    assert alice_published.pool_id == "oldlab"
    assert alice_published.manager_state == "closing"
    assert alice_published.protected_release_digest == alice_publication.release_digest

    alice_released = await executable_capacity_harness.release_retired_intent(
        "oldlab",
        alice_binding,
    )
    assert alice_released.status == "released"
    alice_released_snapshot = await executable_capacity_harness.protected_intent_snapshot(
        "oldlab",
        alice_binding,
    )
    assert alice_released_snapshot.manager_state == "released"
    assert alice_released_snapshot.protected_release_digest == alice_publication.release_digest
    assert (
        await executable_capacity_harness.protected_intent_snapshot("gb10", bob_binding)
        == bob_delayed
    )
    await executable_capacity_harness.publish_retirement_safe_executor_evidence("oldlab")
    assert (
        await executable_capacity_harness.protected_intent_snapshot("gb10", bob_binding)
        == bob_delayed
    )
    executor_status = await executable_capacity_harness.executable_executor_status()
    executor_items = {item["pool_id"]: item for item in executor_status["items"]}
    assert executor_items["oldlab"]["retirement_safe"] is True
    assert executor_items["oldlab"]["blockers"] == []
    assert executor_items["oldlab"]["inventory_record_counts"] == {}
    assert {
        (item.subject_id, item.pool_id, item.state)
        for item in await executable_capacity_harness.manager_commitments()
    } == {(bob.subject_id, "gb10", "closing")}
    assert await executable_capacity_harness.retirement_is_blocked()
    assert executable_capacity_harness.execution_state == "drain-only"

    bob_publication = await executable_capacity_harness.publish_next_protected_release_with_replay(
        bob,
        manager_outage_before_first_publish=True,
        lose_response_after_manager_ack=True,
    )
    assert bob_publication.event_kind == "withdrawn"
    assert bob_publication.guard_publication_count == 1
    assert bob_publication.publish_attempts == 3
    assert bob_publication.manager_replayed_flags == (False, True)
    bob_released = await executable_capacity_harness.release_retired_intent("gb10", bob_binding)
    assert bob_released.status == "released"
    bob_released_snapshot = await executable_capacity_harness.protected_intent_snapshot(
        "gb10",
        bob_binding,
    )
    assert bob_released_snapshot.manager_state == "released"
    assert bob_released_snapshot.protected_release_digest == bob_publication.release_digest
    assert (
        await executable_capacity_harness.protected_intent_snapshot("oldlab", alice_binding)
        == alice_released_snapshot
    )
    assert executable_capacity_harness.execution_state == "shadow"
    assert await executable_capacity_harness.manager_commitments() == ()
    assert executable_capacity_harness.cross_owner_bindings() == []


async def _prepared_unused_release_digests(
    root: Path,
    postgres_url: str,
    capacity_guard_template_database: dict[str, object],
    *,
    database_suffix: str,
) -> tuple[str, str, str]:
    if root.exists():
        shutil.rmtree(root)
    root.mkdir(parents=True, exist_ok=False)
    harness = await ExecutableCapacityHarness.create(
        root,
        postgres_url,
        capacity_guard_template_database,
        database_suffix=database_suffix,
    )
    try:
        alice = await harness.add_owner("alice", "a" * 64)
        await alice.publish_x86_demand(1)
        await harness.activate_next_epoch()
        await harness.reconcile()
        binding = await harness.prepare_unused_intent("oldlab")
        await harness.begin_drain()
        await harness.drain_prepared_unused("oldlab", binding)
        await harness.restart_executor("oldlab")
        published = await harness.publish_next_protected_release_with_replay(
            alice,
            manager_outage_before_first_publish=True,
            lose_response_after_manager_ack=True,
        )
        await harness.release_retired_intent("oldlab", binding)
        final = await harness.canonical_digests()
        assert published.release_digest == final.release
        return final.allocation, final.inventory, final.release
    finally:
        await harness.aclose()


async def _unregistered_withdrawn_release_digests(
    root: Path,
    postgres_url: str,
    capacity_guard_template_database: dict[str, object],
    *,
    database_suffix: str,
) -> tuple[str, str, str]:
    if root.exists():
        shutil.rmtree(root)
    root.mkdir(parents=True, exist_ok=False)
    harness = await ExecutableCapacityHarness.create(
        root,
        postgres_url,
        capacity_guard_template_database,
        database_suffix=database_suffix,
    )
    try:
        alice = await harness.add_owner("alice", "a" * 64)
        await alice.publish_x86_demand(1)
        await harness.activate_next_epoch()
        await harness.reconcile()
        binding, job_id = await harness.submit_unregistered_intent("oldlab")
        await harness.begin_drain()
        await harness.drain_unregistered_withdrawn("oldlab", binding, job_id)
        await harness.restart_executor("oldlab")
        published = await harness.publish_next_protected_release_with_replay(
            alice,
            manager_outage_before_first_publish=True,
            lose_response_after_manager_ack=True,
        )
        await harness.release_retired_intent("oldlab", binding)
        final = await harness.canonical_digests()
        assert published.release_digest == final.release
        return final.allocation, final.inventory, final.release
    finally:
        await harness.aclose()


@pytest.mark.timeout(180)
async def test_protected_release_canonical_digests_are_stable_across_fresh_runs(
    tmp_path: Path,
    postgres_url: str,
    capacity_guard_template_database: dict[str, object],
) -> None:
    repeat_root = tmp_path / "protected-release-repeat"
    pair_namespace = hashlib.sha256(str(tmp_path).encode("utf-8")).hexdigest()[:12]
    prepared_suffix = f"task4_{pair_namespace}_prep"
    withdrawn_suffix = f"task4_{pair_namespace}_withd"
    assert repeat_root.is_relative_to(tmp_path)
    assert prepared_suffix != withdrawn_suffix
    first_prepared = await _prepared_unused_release_digests(
        repeat_root / "prepared",
        postgres_url,
        capacity_guard_template_database,
        database_suffix=prepared_suffix,
    )
    second_prepared = await _prepared_unused_release_digests(
        repeat_root / "prepared",
        postgres_url,
        capacity_guard_template_database,
        database_suffix=prepared_suffix,
    )
    first_withdrawn = await _unregistered_withdrawn_release_digests(
        repeat_root / "withdrawn",
        postgres_url,
        capacity_guard_template_database,
        database_suffix=withdrawn_suffix,
    )
    second_withdrawn = await _unregistered_withdrawn_release_digests(
        repeat_root / "withdrawn",
        postgres_url,
        capacity_guard_template_database,
        database_suffix=withdrawn_suffix,
    )

    assert first_prepared == second_prepared
    assert first_withdrawn == second_withdrawn
    print(
        "CANONICAL_DIGEST prepared_unused_repeat "
        f"allocation={first_prepared[0]} inventory={first_prepared[1]} "
        f"release={first_prepared[2]}"
    )
    print(
        "CANONICAL_DIGEST unregistered_withdrawn_repeat "
        f"allocation={first_withdrawn[0]} inventory={first_withdrawn[1]} "
        f"release={first_withdrawn[2]}"
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


@pytest.mark.timeout(120)
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
