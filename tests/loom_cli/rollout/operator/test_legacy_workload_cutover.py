"""A distinct cutover retires writers without reopening the handoff restore journal."""

import copy
import os
from dataclasses import replace

import pytest

from loom_cli.rollout.operator.protected_legacy_workload_cutover import (
    LegacyWorkloadCutover,
    LegacyWorkloadCutoverJournal,
)
from tests.loom_cli.rollout.operator.test_application_workload_runtime import _context


def fixture(tmp_path):
    plan, handoff, guard, _, runner, _ = _context(tmp_path)
    journal = LegacyWorkloadCutoverJournal(tmp_path / "state", plan.request_id, plan.attempt_number, os.geteuid())
    checks = []
    def fence():
        checks.append("fence")
        return "d" * 64
    owner = LegacyWorkloadCutover(plan, journal, runner, guard, lambda: None, fence)
    return owner, handoff, runner, checks


@pytest.mark.parametrize("lost_reply", [False, True])
def test_retirement_has_its_own_journal_and_reconciles_partial_shutdown(tmp_path, lost_reply):
    owner, handoff, runner, checks = fixture(tmp_path)
    original = copy.deepcopy(runner.objects)
    handoff_files = {str(path): path.read_bytes() for path in handoff.attempt_root.rglob("*") if path.is_file()}
    if lost_reply:
        runner.fail_after = 2
        with pytest.raises(RuntimeError, match="lost workload patch"):
            owner.retire()
    evidence = owner.retire()
    assert len(evidence["workloads"]) == 8
    assert {str(path): path.read_bytes() for path in handoff.attempt_root.rglob("*") if path.is_file()} == handoff_files
    assert all(obj["spec"].get("replicas", 0) == 0 for obj in runner.objects if obj["kind"] == "Deployment")
    assert next(obj for obj in runner.objects if obj["kind"] == "CronJob")["metadata"]["annotations"]["loom.dev/legacy-writer-retirement"] == owner.plan.plan_digest
    count = len(runner.patch_calls)
    assert owner.retire() == evidence
    assert len(runner.patch_calls) == count and checks
    # A later caller cannot use this retirement journal to restore old replicas.
    runner.objects = original
    with pytest.raises(RuntimeError, match="retired workload"):
        owner.retire()
    assert len(runner.patch_calls) == count


def test_late_owned_job_is_retained_and_suspended_before_terminal(tmp_path):
    owner, _, runner, _ = fixture(tmp_path)
    runner.late_job = True
    evidence = owner.retire()
    assert len(evidence["workloads"]) == 9
    assert next(obj for obj in runner.objects if obj["kind"] == "Job")["spec"]["suspend"] is True


@pytest.mark.parametrize("authority", ["guard", "fence"])
def test_missing_live_authority_refuses_before_workload_changes(tmp_path, authority):
    owner, _, runner, _ = fixture(tmp_path)
    def refuse():
        raise RuntimeError("authority unavailable")
    owner = replace(owner, **{"guard_check" if authority == "guard" else "fence_check": refuse})
    with pytest.raises(RuntimeError, match="authority unavailable"):
        owner.retire()
    assert runner.patch_calls == []


def test_changed_saved_spec_is_not_adopted_after_lost_reply(tmp_path):
    owner, _, runner, _ = fixture(tmp_path)
    runner.fail_after = 2
    with pytest.raises(RuntimeError, match="lost workload patch"):
        owner.retire()
    runner.objects[0]["spec"]["template"]["spec"]["containers"][0]["image"] = "foreign:latest"
    calls = len(runner.patch_calls)
    with pytest.raises(ValueError, match="saved identity or spec"):
        owner.retire()
    assert len(runner.patch_calls) == calls


def test_latent_unowned_replica_set_with_database_credentials_blocks_cutover(tmp_path):
    owner, _, runner, _ = fixture(tmp_path)
    latent = copy.deepcopy(runner.objects[0])
    latent["kind"] = "ReplicaSet"
    latent["metadata"]["name"] = "foreign-latent-writer"
    from uuid import uuid4
    latent["metadata"]["uid"] = str(uuid4())
    latent["spec"]["replicas"] = 0
    latent["spec"]["template"]["spec"]["containers"][0]["envFrom"] = [
        {"secretRef": {"name": "loom-secrets"}},
    ]
    runner.objects.append(latent)
    with pytest.raises(RuntimeError, match=r"unowned.*ReplicaSet"):
        owner.retire()
    assert runner.patch_calls == []


def test_fresh_guard_for_same_plan_can_finish_a_partial_retirement(tmp_path):
    from dataclasses import asdict

    from loom_cli.rollout.operator.staging_mutation_guard import MutationGuardEvidence

    owner, _, runner, _ = fixture(tmp_path)
    runner.fail_after = 2
    with pytest.raises(RuntimeError, match="lost workload patch"):
        owner.retire()
    values = asdict(owner.guard)
    values.pop("schema_version")
    values.pop("evidence_digest")
    values.update(generation="c" * 32, database_backend_pid=owner.guard.database_backend_pid + 1,
        mutation_epoch=owner.plan.starting_mutation_epoch + 1)
    successor = MutationGuardEvidence.build(**values)
    checks = []
    resumed = replace(owner, guard=successor, guard_check=lambda: checks.append(successor.generation))
    assert len(resumed.retire()["workloads"]) == 8
    assert checks and set(checks) == {successor.generation}
