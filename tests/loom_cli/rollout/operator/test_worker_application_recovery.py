"""Worker recovers only the original acknowledged operation before DB admission."""

import os
from dataclasses import replace
from types import SimpleNamespace

import pytest

from loom_cli.rollout.operator import worker
from loom_cli.rollout.operator.protected_apply_journal import ComponentObservation, ComponentState
from tests.loom_cli.rollout.operator.test_application_admission_recovery import _component
from tests.loom_cli.rollout.operator.test_application_guard_retention import _pending_resume
from tests.loom_cli.rollout.operator.test_protected_apply_journal import _Backend
from tests.loom_cli.rollout.operator.test_worker import valid_envelope


@pytest.mark.parametrize("component_id", ["application-ownership-handoff", "database-migration", "staging-capacity-database"])
@pytest.mark.parametrize("drift", [None, "no-ack", "candidate", "not-resume", "interrupted"])
def test_worker_recovers_original_plan_before_any_new_attempt(tmp_path, component_id, drift):
    plan, journal, saved_guard = _pending_resume(tmp_path, component_id=component_id, acknowledge=drift != "no-ack")
    candidate_sha = "d" * 40 if drift == "candidate" else plan.candidate_sha
    envelope = replace(valid_envelope(), request_id=plan.request_id,
        attempt_number=plan.attempt_number + (0 if drift == "not-resume" else 1),
        resume=drift != "not-resume", resolved_sha=candidate_sha, image_tag=f"staging-{candidate_sha[:7]}",
        resolved_tree=plan.candidate_tree, preflight_attestation_sha256=plan.attestation_digest)
    attestation = SimpleNamespace(bindings=SimpleNamespace(staging_mutation_epoch=plan.starting_mutation_epoch))
    calls = []

    def recover(candidate, *, guard):
        assert candidate == plan and guard == saved_guard
        calls.append(candidate.attempt_number)
        if drift == "interrupted":
            raise RuntimeError("cleanup interrupted")
        backend = _Backend()
        backend.states["mutation-epoch-claim"] = ComponentState.EXACT
        components = [backend.component("mutation-epoch-claim", 0),
            replace(_component(lambda _: None, exact=True), component_id=component_id,
                classify=lambda _: ComponentObservation(ComponentState.EXACT, "3" * 64, plan.starting_mutation_epoch + 1))]
        return journal.recover_pending_application_operation(candidate, components, guard=guard)

    def invoke():
        return worker._recover_pending_application_before_admission(envelope, attestation=attestation,
            state_root=tmp_path / "state", service_uid=os.geteuid(), recover=recover)

    if drift:
        with pytest.raises((RuntimeError, ValueError)):
            invoke()
        assert calls == ([plan.attempt_number] if drift == "interrupted" else [])
    else:
        invoke()
        invoke()
        assert calls == [plan.attempt_number]
    assert not (journal.attempt_root.parent / str(plan.attempt_number + 1)).exists()


@pytest.mark.parametrize("failed_recovery", [False, True])
def test_final_admission_waits_for_cleanup_and_propagates_failure(tmp_path, monkeypatch, failed_recovery):
    calls = []
    attestation = SimpleNamespace(bindings=SimpleNamespace(staging_mutation_epoch=7))
    monkeypatch.setattr(worker, "FinalGateExecutionStore", lambda *a, **k: SimpleNamespace(read_all=lambda: {}))
    monkeypatch.setattr(worker, "FinalAdmissionStore", lambda *a, **k: SimpleNamespace(publish=lambda _: calls.append("publish")))

    def recover(envelope, observed):
        assert observed is attestation
        calls.append("recover")
        if failed_recovery:
            raise RuntimeError("cleanup interrupted")

    def admit(*args, **kwargs):
        assert calls == ["recover"]
        calls.append("admit")
        return object()

    def invoke():
        return worker._admit_final_attempt(valid_envelope(), deep_preflight=SimpleNamespace(admit_final=admit),
            attestation_store=SimpleNamespace(read=lambda _: attestation), state_root=tmp_path,
            service_uid=os.geteuid(), recover_pending_application=recover)

    if failed_recovery:
        with pytest.raises(RuntimeError, match="cleanup interrupted"):
            invoke()
        assert calls == ["recover"]
    else:
        invoke()
        assert calls == ["recover", "admit", "publish"]
