from __future__ import annotations

import json
import os
from pathlib import Path
from uuid import UUID

import pytest

from loom_cli.rollout.operator.final_gate_plan import FinalGatePlanStore
from loom_cli.rollout.operator.protected_apply_journal import (
    ComponentFailure,
    ComponentFailureDiagnostic,
    ComponentObservation,
    ComponentState,
    ProtectedApplyComponent,
    ProtectedApplyJournal,
    ProtectedApplyJournalError,
    read_component_failure,
    read_component_failure_diagnostic,
)
from loom_cli.rollout.operator.protected_apply_recovery import (
    find_advanced_epoch_attempt,
)
from loom_cli.rollout.operator.protected_capacity_manager_configuration_compensation import (
    CapacityManagerConfigurationCompensationIntentRecord,
    CapacityManagerConfigurationCompensationRecord,
    CapacityManagerConfigurationCompensationStore,
)
from loom_cli.rollout.operator.protected_execution_preparation_journal import (
    ExecutionPreparationOperationIntent,
    ExecutionPreparationOperationJournal,
    ExecutionPreparationOperationTerminal,
)
from loom_cli.rollout.operator.protected_external_supervisor_transport import (
    ExternalSupervisorApplyError,
    ExternalSupervisorCompensationError,
)
from loom_cli.rollout.operator.protected_gb10_transport import GB10FleetApplyError
from tests.loom_cli.rollout.operator.test_final_gate_plan import _execution_plan, _plan


class _Backend:
    def __init__(self) -> None:
        self.states: dict[str, ComponentState] = {}
        self.apply_calls: list[str] = []

    def component(self, component_id: str, ordinal: int) -> ProtectedApplyComponent:
        def classify(_plan):
            state = self.states.get(component_id, ComponentState.READY)
            return ComponentObservation(
                state=state,
                evidence_digest=f"{ordinal + 1:064x}",
                observed_epoch=8 if state is ComponentState.EXACT else 7,
            )

        def apply(_plan):
            self.apply_calls.append(component_id)
            self.states[component_id] = ComponentState.EXACT

        return ProtectedApplyComponent(
            component_id=component_id,
            implementation_digest=f"{ordinal + 11:064x}",
            input_fingerprint=f"{ordinal + 21:064x}",
            classify=classify,
            apply=apply,
        )


def _journal(tmp_path: Path) -> ProtectedApplyJournal:
    attempt = tmp_path / "state/requests/req-alpha/attempts/1"
    attempt.mkdir(parents=True, mode=0o700)
    return ProtectedApplyJournal(
        tmp_path / "state",
        request_id="req-alpha",
        attempt_number=1,
        service_uid=os.geteuid(),
    )


def _compensation_store(tmp_path: Path) -> CapacityManagerConfigurationCompensationStore:
    return CapacityManagerConfigurationCompensationStore(
        (
            tmp_path / "state/protected-capacity/capacity-manager-configuration-compensations"
        ).resolve(),
        service_uid=os.geteuid(),
    )


def _compensation_intent(
    plan,
) -> CapacityManagerConfigurationCompensationIntentRecord:
    assert plan.manager_configuration_epoch is not None
    assert plan.manager_configuration_digest is not None
    return CapacityManagerConfigurationCompensationIntentRecord.build(
        request_id=plan.request_id,
        attempt_number=plan.attempt_number,
        plan_digest=plan.plan_digest,
        activation_idempotency_key=UUID("00000000-0000-4000-8000-000000000301"),
        activation_request_digest="1" * 64,
        target_configuration_epoch=10,
        target_configuration_digest="2" * 64,
        target_configuration_evidence_digest="3" * 64,
        predecessor_configuration_epoch=plan.manager_configuration_epoch,
        predecessor_configuration_digest=plan.manager_configuration_digest,
        predecessor_configuration_evidence_digest="4" * 64,
        backup_lease_digest=plan.backup_lease_digest,
        rollback_idempotency_key=UUID("00000000-0000-4000-8000-000000000302"),
        rollback_request_digest="5" * 64,
        rollback_evidence_sha256="6" * 64,
    )


def _compensation_record(plan) -> CapacityManagerConfigurationCompensationRecord:
    intent = _compensation_intent(plan)
    return CapacityManagerConfigurationCompensationRecord.build(
        request_id=intent.request_id,
        attempt_number=intent.attempt_number,
        plan_digest=intent.plan_digest,
        activation_idempotency_key=intent.activation_idempotency_key,
        activation_request_digest=intent.activation_request_digest,
        target_configuration_epoch=intent.target_configuration_epoch,
        target_configuration_digest=intent.target_configuration_digest,
        target_configuration_evidence_digest=intent.target_configuration_evidence_digest,
        predecessor_configuration_epoch=intent.predecessor_configuration_epoch,
        predecessor_configuration_digest=intent.predecessor_configuration_digest,
        predecessor_configuration_evidence_digest=intent.predecessor_configuration_evidence_digest,
        backup_lease_digest=intent.backup_lease_digest,
        rollback_idempotency_key=intent.rollback_idempotency_key,
        rollback_request_digest=intent.rollback_request_digest,
        rollback_evidence_sha256=intent.rollback_evidence_sha256,
        resulting_configuration_epoch=11,
        resulting_configuration_digest="7" * 64,
        resulting_configuration_evidence_digest="8" * 64,
    )


def _publish_recovery_plan_and_epoch_terminal(
    tmp_path: Path,
    *,
    plan=None,  # type: ignore[no-untyped-def]
):
    journal = _journal(tmp_path)
    plan = _plan(tmp_path) if plan is None else plan
    FinalGatePlanStore(
        tmp_path / "state",
        request_id="req-alpha",
        attempt_number=1,
        service_uid=os.geteuid(),
    ).publish(plan)
    backend = _Backend()
    journal.execute(plan, (backend.component("mutation-epoch-claim", 0),))
    return plan


def _record_execution_preparation_compensation(tmp_path: Path, plan) -> None:  # type: ignore[no-untyped-def]
    (tmp_path / "state").chmod(0o700)
    journal = ExecutionPreparationOperationJournal(
        (tmp_path / "state").resolve(),
        request_id=plan.request_id,
        attempt_number=plan.attempt_number,
        service_uid=os.geteuid(),
    )
    preparation = ExecutionPreparationOperationIntent.build(
        plan=plan,
        artifact_sha256=plan.execution_prerequisite_artifact_sha256,
        operation="manager-preparation",
        request_sha256="1" * 64,
        prepared_execution_epoch=None,
        prepared_execution_manifest_sha256=None,
    )
    journal.record_intent(preparation)
    journal.record_terminal(
        ExecutionPreparationOperationTerminal.build(
            intent=preparation,
            evidence_sha256="2" * 64,
            prepared_execution_epoch=1,
            prepared_execution_manifest_sha256="3" * 64,
            result_state="prepared",
        )
    )
    abort = ExecutionPreparationOperationIntent.build(
        plan=plan,
        artifact_sha256=plan.execution_prerequisite_artifact_sha256,
        operation="manager-abort",
        request_sha256="4" * 64,
        prepared_execution_epoch=1,
        prepared_execution_manifest_sha256="3" * 64,
    )
    journal.record_intent(abort)
    journal.record_terminal(
        ExecutionPreparationOperationTerminal.build(
            intent=abort,
            evidence_sha256="5" * 64,
            prepared_execution_epoch=1,
            prepared_execution_manifest_sha256="3" * 64,
            result_state="shadow",
        )
    )


def test_protected_apply_journal_publishes_intent_before_apply_and_reuses_terminal(
    tmp_path: Path,
) -> None:
    journal = _journal(tmp_path)
    backend = _Backend()
    components = (backend.component("epoch-claim", 0), backend.component("manifest-apply", 1))

    first = journal.execute(_plan(tmp_path), components)
    second = journal.execute(_plan(tmp_path), components)

    assert backend.apply_calls == ["epoch-claim", "manifest-apply"]
    assert list(first) == ["epoch-claim", "manifest-apply"]
    assert first == second
    root = tmp_path / "state/requests/req-alpha/attempts/1/protected-apply"
    for ordinal, component in enumerate(components):
        component_root = root / f"{ordinal:02d}-{component.component_id}"
        assert json.loads((component_root / "intent.json").read_text())["component_id"] == (
            component.component_id
        )
        assert json.loads((component_root / "terminal.json").read_text())["applied"] is True


def test_protected_apply_journal_recovers_intent_after_apply_without_repeating(
    tmp_path: Path,
) -> None:
    journal = _journal(tmp_path)
    backend = _Backend()
    component = backend.component("manifest-apply", 0)
    original_publish = journal._publish_or_match
    failed = False

    def crash_after_apply(path, value):
        nonlocal failed
        if path.name == "terminal.json" and not failed:
            failed = True
            raise RuntimeError("simulated worker crash")
        original_publish(path, value)

    journal._publish_or_match = crash_after_apply  # type: ignore[method-assign]
    with pytest.raises(RuntimeError, match="simulated worker crash"):
        journal.execute(_plan(tmp_path), (component,))
    assert backend.apply_calls == ["manifest-apply"]

    journal._publish_or_match = original_publish  # type: ignore[method-assign]
    terminal = journal.execute(_plan(tmp_path), (component,))["manifest-apply"]
    assert backend.apply_calls == ["manifest-apply"]
    assert terminal.applied is False


def test_protected_apply_recovery_requires_exact_plan_epoch_intent_and_terminal(
    tmp_path: Path,
) -> None:
    journal = _journal(tmp_path)
    plan = _plan(tmp_path)
    FinalGatePlanStore(
        tmp_path / "state",
        request_id="req-alpha",
        attempt_number=1,
        service_uid=os.geteuid(),
    ).publish(plan)
    backend = _Backend()
    epoch = backend.component("mutation-epoch-claim", 0)

    assert not journal.has_advanced_epoch_terminal(plan)
    journal.execute(plan, (epoch,))

    assert journal.has_advanced_epoch_terminal(plan)
    assert (
        find_advanced_epoch_attempt(
            tmp_path / "state",
            request_id="req-alpha",
            through_attempt=1,
            candidate_sha=plan.candidate_sha,
            attestation_digest=plan.attestation_digest,
            starting_mutation_epoch=plan.starting_mutation_epoch,
            service_uid=os.geteuid(),
        )
        == 1
    )
    with pytest.raises(ValueError, match="plan binding drifted"):
        find_advanced_epoch_attempt(
            tmp_path / "state",
            request_id="req-alpha",
            through_attempt=1,
            candidate_sha="f" * 40,
            attestation_digest=plan.attestation_digest,
            starting_mutation_epoch=plan.starting_mutation_epoch,
            service_uid=os.geteuid(),
        )


def test_protected_apply_recovery_rejects_changed_execution_prerequisite(
    tmp_path: Path,
) -> None:
    journal = _journal(tmp_path)
    original = _execution_plan(tmp_path, access_metadata_override="a" * 64)
    changed = _execution_plan(tmp_path, access_metadata_override="b" * 64)
    FinalGatePlanStore(
        tmp_path / "state",
        request_id=original.request_id,
        attempt_number=original.attempt_number,
        service_uid=os.geteuid(),
    ).publish(original)
    journal.execute(original, (_Backend().component("mutation-epoch-claim", 0),))

    assert original.execution_prerequisite_artifact_sha256 != (
        changed.execution_prerequisite_artifact_sha256
    )
    assert original.attestation_digest != changed.attestation_digest
    with pytest.raises(ValueError, match="plan binding drifted"):
        find_advanced_epoch_attempt(
            tmp_path / "state",
            request_id=original.request_id,
            through_attempt=original.attempt_number,
            candidate_sha=changed.candidate_sha,
            attestation_digest=changed.attestation_digest,
            starting_mutation_epoch=changed.starting_mutation_epoch,
            service_uid=os.geteuid(),
        )


def test_protected_apply_recovery_rejects_exact_compensated_attempt(tmp_path: Path) -> None:
    plan = _publish_recovery_plan_and_epoch_terminal(tmp_path)
    store = _compensation_store(tmp_path)
    intent = _compensation_intent(plan)
    store.record_intent(intent)
    store.record(_compensation_record(plan))

    assert (
        find_advanced_epoch_attempt(
            tmp_path / "state",
            request_id="req-alpha",
            through_attempt=1,
            candidate_sha=plan.candidate_sha,
            attestation_digest=plan.attestation_digest,
            starting_mutation_epoch=plan.starting_mutation_epoch,
            service_uid=os.geteuid(),
        )
        is None
    )


def test_schema_seven_recovery_rejects_durably_compensated_execution_preparation(
    tmp_path: Path,
) -> None:
    plan = _execution_plan(tmp_path)
    plan = _publish_recovery_plan_and_epoch_terminal(tmp_path, plan=plan)
    _record_execution_preparation_compensation(tmp_path, plan)

    assert (
        find_advanced_epoch_attempt(
            tmp_path / "state",
            request_id="req-alpha",
            through_attempt=1,
            candidate_sha=plan.candidate_sha,
            attestation_digest=plan.attestation_digest,
            starting_mutation_epoch=plan.starting_mutation_epoch,
            service_uid=os.geteuid(),
        )
        is None
    )


def test_schema_seven_final_gate_recovery_fails_closed_on_malformed_preparation_journal(
    tmp_path: Path,
) -> None:
    """Break caught: final-gate recovery skips malformed execution authority."""

    plan = _execution_plan(tmp_path)
    plan = _publish_recovery_plan_and_epoch_terminal(tmp_path, plan=plan)
    state_root = (tmp_path / "state").resolve()
    state_root.chmod(0o700)
    journal = ExecutionPreparationOperationJournal(
        state_root,
        request_id=plan.request_id,
        attempt_number=plan.attempt_number,
        service_uid=os.geteuid(),
    )
    preparation = ExecutionPreparationOperationIntent.build(
        plan=plan,
        artifact_sha256=plan.execution_prerequisite_artifact_sha256,
        operation="manager-preparation",
        request_sha256="1" * 64,
        prepared_execution_epoch=None,
        prepared_execution_manifest_sha256=None,
    )
    journal.record_intent(preparation)
    malformed = journal.root / "manager-preparation.intent.json"
    malformed.write_bytes(b"{}\n")
    malformed.chmod(0o600)

    with pytest.raises(ValueError, match="intent fields are invalid"):
        find_advanced_epoch_attempt(
            state_root,
            request_id=plan.request_id,
            through_attempt=plan.attempt_number,
            candidate_sha=plan.candidate_sha,
            attestation_digest=plan.attestation_digest,
            starting_mutation_epoch=plan.starting_mutation_epoch,
            service_uid=os.geteuid(),
        )


def test_protected_apply_recovery_fails_closed_on_malformed_compensation_inventory(
    tmp_path: Path,
) -> None:
    plan = _publish_recovery_plan_and_epoch_terminal(tmp_path)
    root = _compensation_store(tmp_path).root
    root.mkdir(parents=True, mode=0o700)
    malformed = root / ".capacity-manager-configuration-compensation-garbage.json"
    malformed.write_bytes(b"{}\n")
    malformed.chmod(0o600)

    with pytest.raises(RuntimeError, match="filename is invalid"):
        find_advanced_epoch_attempt(
            tmp_path / "state",
            request_id="req-alpha",
            through_attempt=1,
            candidate_sha=plan.candidate_sha,
            attestation_digest=plan.attestation_digest,
            starting_mutation_epoch=plan.starting_mutation_epoch,
            service_uid=os.geteuid(),
        )


def test_protected_apply_recovery_fails_closed_on_incomplete_matching_compensation(
    tmp_path: Path,
) -> None:
    plan = _publish_recovery_plan_and_epoch_terminal(tmp_path)
    store = _compensation_store(tmp_path)
    store.record_intent(_compensation_intent(plan))

    with pytest.raises(RuntimeError, match="record is incomplete"):
        find_advanced_epoch_attempt(
            tmp_path / "state",
            request_id="req-alpha",
            through_attempt=1,
            candidate_sha=plan.candidate_sha,
            attestation_digest=plan.attestation_digest,
            starting_mutation_epoch=plan.starting_mutation_epoch,
            service_uid=os.geteuid(),
        )


def test_protected_apply_recovery_fails_closed_on_matching_compensation_plan_drift(
    tmp_path: Path,
) -> None:
    plan = _publish_recovery_plan_and_epoch_terminal(tmp_path)
    store = _compensation_store(tmp_path)
    intent = CapacityManagerConfigurationCompensationIntentRecord.build(
        request_id=plan.request_id,
        attempt_number=plan.attempt_number,
        plan_digest=plan.plan_digest,
        activation_idempotency_key=UUID("00000000-0000-4000-8000-000000000399"),
        activation_request_digest="1" * 64,
        target_configuration_epoch=10,
        target_configuration_digest="2" * 64,
        target_configuration_evidence_digest="3" * 64,
        predecessor_configuration_epoch=plan.manager_configuration_epoch,  # type: ignore[arg-type]
        predecessor_configuration_digest=plan.manager_configuration_digest,  # type: ignore[arg-type]
        predecessor_configuration_evidence_digest="4" * 64,
        backup_lease_digest="9" * 64,
        rollback_idempotency_key=UUID("00000000-0000-4000-8000-000000000302"),
        rollback_request_digest="5" * 64,
        rollback_evidence_sha256="6" * 64,
    )
    store.record_intent(intent)
    store.record(
        CapacityManagerConfigurationCompensationRecord.build(
            request_id=intent.request_id,
            attempt_number=intent.attempt_number,
            plan_digest=intent.plan_digest,
            activation_idempotency_key=intent.activation_idempotency_key,
            activation_request_digest=intent.activation_request_digest,
            target_configuration_epoch=intent.target_configuration_epoch,
            target_configuration_digest=intent.target_configuration_digest,
            target_configuration_evidence_digest=intent.target_configuration_evidence_digest,
            predecessor_configuration_epoch=intent.predecessor_configuration_epoch,
            predecessor_configuration_digest=intent.predecessor_configuration_digest,
            predecessor_configuration_evidence_digest=intent.predecessor_configuration_evidence_digest,
            backup_lease_digest=intent.backup_lease_digest,
            rollback_idempotency_key=intent.rollback_idempotency_key,
            rollback_request_digest=intent.rollback_request_digest,
            rollback_evidence_sha256=intent.rollback_evidence_sha256,
            resulting_configuration_epoch=11,
            resulting_configuration_digest="7" * 64,
            resulting_configuration_evidence_digest="8" * 64,
        )
    )

    with pytest.raises(RuntimeError, match="plan binding drifted"):
        find_advanced_epoch_attempt(
            tmp_path / "state",
            request_id="req-alpha",
            through_attempt=1,
            candidate_sha=plan.candidate_sha,
            attestation_digest=plan.attestation_digest,
            starting_mutation_epoch=plan.starting_mutation_epoch,
            service_uid=os.geteuid(),
        )


def test_protected_apply_recovery_fails_closed_on_ambiguous_matching_compensations(
    tmp_path: Path,
) -> None:
    plan = _publish_recovery_plan_and_epoch_terminal(tmp_path)
    store = _compensation_store(tmp_path)
    first = _compensation_intent(plan)
    store.record_intent(first)
    store.record(_compensation_record(plan))
    second = CapacityManagerConfigurationCompensationIntentRecord.build(
        request_id=plan.request_id,
        attempt_number=plan.attempt_number,
        plan_digest=plan.plan_digest,
        activation_idempotency_key=UUID("00000000-0000-4000-8000-000000000399"),
        activation_request_digest="1" * 64,
        target_configuration_epoch=10,
        target_configuration_digest="2" * 64,
        target_configuration_evidence_digest="3" * 64,
        predecessor_configuration_epoch=plan.manager_configuration_epoch,  # type: ignore[arg-type]
        predecessor_configuration_digest=plan.manager_configuration_digest,  # type: ignore[arg-type]
        predecessor_configuration_evidence_digest="4" * 64,
        backup_lease_digest=plan.backup_lease_digest,
        rollback_idempotency_key=UUID("00000000-0000-4000-8000-000000000302"),
        rollback_request_digest="5" * 64,
        rollback_evidence_sha256="6" * 64,
    )
    store.record_intent(second)
    store.record(
        CapacityManagerConfigurationCompensationRecord.build(
            request_id=second.request_id,
            attempt_number=second.attempt_number,
            plan_digest=second.plan_digest,
            activation_idempotency_key=second.activation_idempotency_key,
            activation_request_digest=second.activation_request_digest,
            target_configuration_epoch=second.target_configuration_epoch,
            target_configuration_digest=second.target_configuration_digest,
            target_configuration_evidence_digest=second.target_configuration_evidence_digest,
            predecessor_configuration_epoch=second.predecessor_configuration_epoch,
            predecessor_configuration_digest=second.predecessor_configuration_digest,
            predecessor_configuration_evidence_digest=second.predecessor_configuration_evidence_digest,
            backup_lease_digest=second.backup_lease_digest,
            rollback_idempotency_key=second.rollback_idempotency_key,
            rollback_request_digest=second.rollback_request_digest,
            rollback_evidence_sha256=second.rollback_evidence_sha256,
            resulting_configuration_epoch=11,
            resulting_configuration_digest="7" * 64,
            resulting_configuration_evidence_digest="8" * 64,
        )
    )

    with pytest.raises(RuntimeError, match="match is ambiguous"):
        find_advanced_epoch_attempt(
            tmp_path / "state",
            request_id="req-alpha",
            through_attempt=1,
            candidate_sha=plan.candidate_sha,
            attestation_digest=plan.attestation_digest,
            starting_mutation_epoch=plan.starting_mutation_epoch,
            service_uid=os.geteuid(),
        )


def test_protected_apply_journal_refuses_partial_or_drifted_live_state(tmp_path: Path) -> None:
    journal = _journal(tmp_path)
    backend = _Backend()
    backend.states["migration-apply"] = ComponentState.DRIFTED

    with pytest.raises(ProtectedApplyJournalError, match="live state drifted"):
        journal.execute(_plan(tmp_path), (backend.component("migration-apply", 0),))
    assert backend.apply_calls == []
    root = tmp_path / "state/requests/req-alpha/attempts/1/protected-apply"
    assert (root / "00-migration-apply/intent.json").exists()
    assert not (root / "00-migration-apply/terminal.json").exists()


def test_protected_apply_journal_persists_only_structured_gb10_failure(
    tmp_path: Path,
) -> None:
    journal = _journal(tmp_path)

    def classify(_plan):
        return ComponentObservation(
            state=ComponentState.READY,
            evidence_digest="1" * 64,
            observed_epoch=7,
        )

    def apply(_plan):
        raise GB10FleetApplyError(("trt-gb10-10", "trt-gb10-2"))

    component = ProtectedApplyComponent(
        component_id="gb10-candidate",
        implementation_digest="2" * 64,
        input_fingerprint="3" * 64,
        classify=classify,
        apply=apply,
    )

    with pytest.raises(GB10FleetApplyError):
        journal.execute(_plan(tmp_path), (component,))

    failure_path = (
        tmp_path
        / "state/requests/req-alpha/attempts/1/protected-apply"
        / "00-gb10-candidate/failure.json"
    )
    failure = read_component_failure(failure_path, service_uid=os.geteuid())
    assert failure == ComponentFailure(
        schema_version=1,
        component_id="gb10-candidate",
        failure_code="gb10-convergence-failed",
        failed_hosts=("trt-gb10-10", "trt-gb10-2"),
    )
    assert set(json.loads(failure_path.read_text())) == {
        "component_id",
        "failed_hosts",
        "failure_code",
        "schema_version",
    }


def test_protected_apply_journal_rejects_plan_or_component_drift(tmp_path: Path) -> None:
    journal = _journal(tmp_path)
    backend = _Backend()
    component = backend.component("epoch-claim", 0)
    journal.execute(_plan(tmp_path), (component,))
    drifted = ProtectedApplyComponent(
        component_id=component.component_id,
        implementation_digest="f" * 64,
        input_fingerprint=component.input_fingerprint,
        classify=component.classify,
        apply=component.apply,
    )

    with pytest.raises(ProtectedApplyJournalError, match="cannot be replaced"):
        journal.execute(_plan(tmp_path), (drifted,))

    backend.states["epoch-claim"] = ComponentState.DRIFTED
    with pytest.raises(ProtectedApplyJournalError, match="terminal state drifted"):
        journal.execute(_plan(tmp_path), (component,))


def test_protected_apply_journal_rejects_unsafe_record_authority(tmp_path: Path) -> None:
    journal = _journal(tmp_path)
    backend = _Backend()
    component = backend.component("epoch-claim", 0)
    journal.execute(_plan(tmp_path), (component,))
    intent = (
        tmp_path / "state/requests/req-alpha/attempts/1/protected-apply/00-epoch-claim/intent.json"
    )
    intent.chmod(0o644)

    with pytest.raises(ProtectedApplyJournalError, match="file authority is unsafe"):
        journal.execute(_plan(tmp_path), (component,))


def test_records_secret_safe_failure_diagnostic_when_a_component_apply_raises(
    tmp_path: Path,
) -> None:
    # #1085 phase 1b: a non-gb10 component whose apply raises previously left
    # only intent.json (masked dead-end, #1081). It now records a coded,
    # secret-safe reason — type + raise-site, never the message.
    journal = _journal(tmp_path)

    def classify(_plan):
        return ComponentObservation(
            state=ComponentState.READY, evidence_digest="1" * 64, observed_epoch=7
        )

    def apply(_plan):
        raise ValueError("secret-bearing detail must never be recorded")

    component = ProtectedApplyComponent(
        component_id="environment-state",
        implementation_digest="2" * 64,
        input_fingerprint="3" * 64,
        classify=classify,
        apply=apply,
    )

    with pytest.raises(ValueError, match="secret-bearing"):  # real failure still propagates
        journal.execute(_plan(tmp_path), (component,))

    root = tmp_path / "state/requests/req-alpha/attempts/1/protected-apply/00-environment-state"
    record = json.loads((root / "failure-diagnostic.json").read_text())
    assert record["component_id"] == "environment-state"
    assert record["failure_code"] == "apply-failed"
    assert record["diagnostic"].startswith("unclassified environment-state failure: ValueError at ")
    assert "secret-bearing" not in (root / "failure-diagnostic.json").read_text()
    assert not (root / "terminal.json").exists()


def test_records_typed_external_supervisor_apply_and_compensation_failures(
    tmp_path: Path,
) -> None:
    journal = _journal(tmp_path)

    def classify(_plan):
        return ComponentObservation(
            state=ComponentState.READY,
            evidence_digest="1" * 64,
            observed_epoch=7,
        )

    def apply(_plan):
        raise ExternalSupervisorApplyError(
            "service-activation-failed",
            compensation_failure_code="transition-validation-failed",
        )

    component = ProtectedApplyComponent(
        component_id="external-supervisors-gb10",
        implementation_digest="2" * 64,
        input_fingerprint="3" * 64,
        classify=classify,
        apply=apply,
    )

    with pytest.raises(ExternalSupervisorApplyError):
        journal.execute(_plan(tmp_path), (component,))

    root = (
        tmp_path
        / "state/requests/req-alpha/attempts/1/protected-apply/00-external-supervisors-gb10"
    )
    diagnostic = read_component_failure_diagnostic(
        root / "failure-diagnostic.json",
        service_uid=os.geteuid(),
    )
    assert diagnostic.failure_code == "apply-failed"
    assert diagnostic.primary_failure_code == "service-activation-failed"
    assert diagnostic.compensation_failure_code == "transition-validation-failed"
    assert diagnostic.diagnostic == "classified external-supervisor apply failure"
    assert set(json.loads((root / "failure-diagnostic.json").read_text())) == {
        "component_id",
        "compensation_failure_code",
        "diagnostic",
        "failure_code",
        "ordinal",
        "primary_failure_code",
        "schema_version",
    }


def test_typed_external_supervisor_diagnostic_rejects_free_form_text() -> None:
    with pytest.raises(ValueError, match="failure diagnostic is invalid"):
        ComponentFailureDiagnostic.from_dict(
            {
                "schema_version": 2,
                "component_id": "external-supervisors-gb10",
                "ordinal": 10,
                "failure_code": "apply-failed",
                "diagnostic": "secret-bearing free-form remote detail",
                "primary_failure_code": "service-activation-failed",
                "compensation_failure_code": None,
            }
        )


def test_schema_v1_diagnostic_rejects_typed_compensation_classification() -> None:
    with pytest.raises(ValueError, match="failure diagnostic is invalid"):
        ComponentFailureDiagnostic.from_dict(
            {
                "schema_version": 1,
                "component_id": "external-supervisor-reconciliation",
                "ordinal": 0,
                "failure_code": "compensation-reconciliation-failed",
                "diagnostic": "free-form value",
            }
        )


def test_reconciliation_outcomes_advance_after_failure_then_success(
    tmp_path: Path,
) -> None:
    journal = _journal(tmp_path)
    reconcile_calls = 0

    def exact(_plan):
        return ComponentObservation(
            state=ComponentState.EXACT,
            evidence_digest="1" * 64,
            observed_epoch=7,
        )

    def reconcile(_plan):
        nonlocal reconcile_calls
        reconcile_calls += 1
        if reconcile_calls == 1:
            raise ExternalSupervisorCompensationError("transition-validation-failed")

    def fail_later(_plan):
        raise RuntimeError("later component failed")

    reconciliation = ProtectedApplyComponent(
        component_id="external-supervisor-reconciliation",
        implementation_digest="2" * 64,
        input_fingerprint="3" * 64,
        classify=exact,
        apply=reconcile,
        reconcile_before_apply=True,
    )
    later = ProtectedApplyComponent(
        component_id="staging-manifests",
        implementation_digest="4" * 64,
        input_fingerprint="5" * 64,
        classify=lambda _plan: ComponentObservation(
            state=ComponentState.READY,
            evidence_digest="6" * 64,
            observed_epoch=7,
        ),
        apply=fail_later,
    )

    with pytest.raises(ExternalSupervisorCompensationError):
        journal.execute(_plan(tmp_path), (reconciliation, later))

    outcomes = (
        tmp_path
        / "state/requests/req-alpha/attempts/1/protected-apply"
        / "00-external-supervisor-reconciliation/reconciliation-outcomes"
    )
    assert json.loads((outcomes / "00000000.json").read_text()) == {
        "component_id": "external-supervisor-reconciliation",
        "compensation_failure_code": "transition-validation-failed",
        "diagnostic": "classified external-supervisor compensation reconciliation failure",
        "failure_code": "compensation-reconciliation-failed",
        "sequence": 0,
        "schema_version": 1,
        "status": "failed",
    }

    with pytest.raises(RuntimeError, match="later component failed"):
        journal.execute(_plan(tmp_path), (reconciliation, later))

    assert json.loads((outcomes / "00000001.json").read_text()) == {
        "component_id": "external-supervisor-reconciliation",
        "compensation_failure_code": None,
        "diagnostic": None,
        "failure_code": None,
        "sequence": 1,
        "schema_version": 1,
        "status": "succeeded",
    }
    assert not (outcomes.parent / "terminal.json").exists()


def test_reconciliation_outcome_publication_does_not_need_post_publish_unlink(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    journal = _journal(tmp_path)
    real_unlink = os.unlink

    def reject_outcome_unlink(path, *args, dir_fd=None, **kwargs):
        if dir_fd is not None:
            directory = Path(os.readlink(f"/proc/self/fd/{dir_fd}"))
            if directory.name == "reconciliation-outcomes":
                raise RuntimeError("simulated crash before temporary unlink")
        return real_unlink(path, *args, dir_fd=dir_fd, **kwargs)

    monkeypatch.setattr(os, "unlink", reject_outcome_unlink)
    reconciliation = ProtectedApplyComponent(
        component_id="external-supervisor-reconciliation",
        implementation_digest="2" * 64,
        input_fingerprint="3" * 64,
        classify=lambda _plan: ComponentObservation(
            state=ComponentState.EXACT,
            evidence_digest="1" * 64,
            observed_epoch=7,
        ),
        apply=lambda _plan: None,
        reconcile_before_apply=True,
    )

    journal.execute(_plan(tmp_path), (reconciliation,))

    outcome = (
        tmp_path
        / "state/requests/req-alpha/attempts/1/protected-apply"
        / "00-external-supervisor-reconciliation/reconciliation-outcomes/00000000.json"
    )
    assert outcome.stat().st_nlink == 1


def test_reconciliation_terminal_is_deferred_until_the_component_chain_completes(
    tmp_path: Path,
) -> None:
    journal = _journal(tmp_path)
    reconcile_calls = 0

    def exact(_plan):
        return ComponentObservation(
            state=ComponentState.EXACT,
            evidence_digest="1" * 64,
            observed_epoch=7,
        )

    def reconcile(_plan):
        nonlocal reconcile_calls
        reconcile_calls += 1
        if reconcile_calls == 2:
            raise ExternalSupervisorCompensationError("transition-validation-failed")

    def fail_later(_plan):
        raise RuntimeError("later component failed")

    reconciliation = ProtectedApplyComponent(
        component_id="external-supervisor-reconciliation",
        implementation_digest="2" * 64,
        input_fingerprint="3" * 64,
        classify=exact,
        apply=reconcile,
        reconcile_before_apply=True,
    )
    later = ProtectedApplyComponent(
        component_id="staging-manifests",
        implementation_digest="4" * 64,
        input_fingerprint="5" * 64,
        classify=lambda _plan: ComponentObservation(
            state=ComponentState.READY,
            evidence_digest="6" * 64,
            observed_epoch=7,
        ),
        apply=fail_later,
    )

    with pytest.raises(RuntimeError, match="later component failed"):
        journal.execute(_plan(tmp_path), (reconciliation, later))

    root = tmp_path / "state/requests/req-alpha/attempts/1/protected-apply"
    assert not (root / "00-external-supervisor-reconciliation/terminal.json").exists()

    with pytest.raises(ExternalSupervisorCompensationError):
        journal.execute(_plan(tmp_path), (reconciliation, later))

    diagnostic = read_component_failure_diagnostic(
        root / "00-external-supervisor-reconciliation/failure-diagnostic.json",
        service_uid=os.geteuid(),
    )
    assert diagnostic.compensation_failure_code == "transition-validation-failed"


def test_records_failure_diagnostic_when_a_component_does_not_converge(
    tmp_path: Path,
) -> None:
    journal = _journal(tmp_path)

    def classify(_plan):  # READY before and after apply → never reaches EXACT
        return ComponentObservation(
            state=ComponentState.READY, evidence_digest="4" * 64, observed_epoch=7
        )

    def apply(_plan):  # applies but does not converge
        return None

    component = ProtectedApplyComponent(
        component_id="external-supervisors",
        implementation_digest="5" * 64,
        input_fingerprint="6" * 64,
        classify=classify,
        apply=apply,
    )

    with pytest.raises(ProtectedApplyJournalError, match="did not converge"):
        journal.execute(_plan(tmp_path), (component,))

    root = tmp_path / "state/requests/req-alpha/attempts/1/protected-apply/00-external-supervisors"
    record = json.loads((root / "failure-diagnostic.json").read_text())
    assert record["failure_code"] == "did-not-converge"
    assert record["diagnostic"] == "component classified ready after apply"


def test_records_secret_safe_failure_diagnostic_when_pre_classification_raises(
    tmp_path: Path,
) -> None:
    journal = _journal(tmp_path)

    def classify(_plan):
        raise RuntimeError("pre-classification secret must never be recorded")

    component = ProtectedApplyComponent(
        component_id="environment-state",
        implementation_digest="7" * 64,
        input_fingerprint="8" * 64,
        classify=classify,
        apply=lambda _plan: None,
    )

    with pytest.raises(RuntimeError, match="pre-classification secret"):
        journal.execute(_plan(tmp_path), (component,))

    root = tmp_path / "state/requests/req-alpha/attempts/1/protected-apply/00-environment-state"
    record = json.loads((root / "failure-diagnostic.json").read_text())
    assert record["failure_code"] == "pre-classify-failed"
    assert record["diagnostic"].startswith(
        "unclassified environment-state failure: RuntimeError at "
    )
    assert "pre-classification secret" not in (root / "failure-diagnostic.json").read_text()


def test_records_secret_safe_failure_diagnostic_when_post_classification_raises(
    tmp_path: Path,
) -> None:
    journal = _journal(tmp_path)
    classify_calls = 0

    def classify(_plan):
        nonlocal classify_calls
        classify_calls += 1
        if classify_calls == 1:
            return ComponentObservation(
                state=ComponentState.READY,
                evidence_digest="9" * 64,
                observed_epoch=7,
            )
        raise RuntimeError("post-classification secret must never be recorded")

    component = ProtectedApplyComponent(
        component_id="environment-state",
        implementation_digest="a" * 64,
        input_fingerprint="b" * 64,
        classify=classify,
        apply=lambda _plan: None,
    )

    with pytest.raises(RuntimeError, match="post-classification secret"):
        journal.execute(_plan(tmp_path), (component,))

    root = tmp_path / "state/requests/req-alpha/attempts/1/protected-apply/00-environment-state"
    record = json.loads((root / "failure-diagnostic.json").read_text())
    assert record["failure_code"] == "post-classify-failed"
    assert record["diagnostic"].startswith(
        "unclassified environment-state failure: RuntimeError at "
    )
    assert "post-classification secret" not in (root / "failure-diagnostic.json").read_text()


def test_records_secret_safe_failure_diagnostic_when_terminal_reclassification_raises(
    tmp_path: Path,
) -> None:
    journal = _journal(tmp_path)
    backend = _Backend()
    component = backend.component("environment-state", 0)
    journal.execute(_plan(tmp_path), (component,))

    def classify(_plan):
        raise RuntimeError("terminal-classification secret must never be recorded")

    drifted_component = ProtectedApplyComponent(
        component_id=component.component_id,
        implementation_digest=component.implementation_digest,
        input_fingerprint=component.input_fingerprint,
        classify=classify,
        apply=component.apply,
    )

    with pytest.raises(RuntimeError, match="terminal-classification secret"):
        journal.execute(_plan(tmp_path), (drifted_component,))

    root = tmp_path / "state/requests/req-alpha/attempts/1/protected-apply/00-environment-state"
    record = json.loads((root / "failure-diagnostic.json").read_text())
    assert record["failure_code"] == "terminal-classify-failed"
    assert record["diagnostic"].startswith(
        "unclassified environment-state failure: RuntimeError at "
    )
    assert "terminal-classification secret" not in (root / "failure-diagnostic.json").read_text()


@pytest.mark.parametrize("second_failure", ["drifted", "raise"])
def test_preapply_group_classifies_every_member_before_any_group_apply(
    tmp_path: Path,
    second_failure: str,
) -> None:
    journal = _journal(tmp_path)
    states = {
        "credential-gb10": ComponentState.READY,
        "credential-oldlab": ComponentState.READY,
    }
    apply_calls: list[str] = []

    def grouped(component_id: str, ordinal: int) -> ProtectedApplyComponent:
        def classify(_plan):
            if component_id == "credential-oldlab" and second_failure == "raise":
                raise RuntimeError("credential classification failed")
            state = (
                ComponentState.DRIFTED
                if component_id == "credential-oldlab" and second_failure == "drifted"
                else states[component_id]
            )
            return ComponentObservation(
                state=state,
                evidence_digest=f"{ordinal + 1:064x}",
                observed_epoch=8 if state is ComponentState.EXACT else 7,
            )

        def apply(_plan):
            apply_calls.append(component_id)
            states[component_id] = ComponentState.EXACT

        return ProtectedApplyComponent(
            component_id=component_id,
            implementation_digest=f"{ordinal + 11:064x}",
            input_fingerprint=f"{ordinal + 21:064x}",
            classify=classify,
            apply=apply,
            preapply_group="external-supervisor-credentials",
        )

    components = (grouped("credential-gb10", 0), grouped("credential-oldlab", 1))

    with pytest.raises((ProtectedApplyJournalError, RuntimeError)):
        journal.execute(_plan(tmp_path), components)

    assert apply_calls == []


def test_preapply_group_resume_rechecks_all_members_and_keeps_first_terminal(
    tmp_path: Path,
) -> None:
    journal = _journal(tmp_path)
    states = {
        "credential-gb10": ComponentState.READY,
        "credential-oldlab": ComponentState.READY,
    }
    apply_calls: list[str] = []
    fail_second = True

    def grouped(component_id: str, ordinal: int) -> ProtectedApplyComponent:
        def classify(_plan):
            state = states[component_id]
            return ComponentObservation(
                state=state,
                evidence_digest=f"{ordinal + 1:064x}",
                observed_epoch=8 if state is ComponentState.EXACT else 7,
            )

        def apply(_plan):
            nonlocal fail_second
            apply_calls.append(component_id)
            if component_id == "credential-oldlab" and fail_second:
                fail_second = False
                raise RuntimeError("unexpected second publication failure")
            states[component_id] = ComponentState.EXACT

        return ProtectedApplyComponent(
            component_id=component_id,
            implementation_digest=f"{ordinal + 31:064x}",
            input_fingerprint=f"{ordinal + 41:064x}",
            classify=classify,
            apply=apply,
            preapply_group="external-supervisor-credentials",
        )

    components = (grouped("credential-gb10", 0), grouped("credential-oldlab", 1))
    with pytest.raises(RuntimeError, match="unexpected second publication failure"):
        journal.execute(_plan(tmp_path), components)

    terminals = journal.execute(_plan(tmp_path), components)

    assert apply_calls == ["credential-gb10", "credential-oldlab", "credential-oldlab"]
    assert terminals["credential-gb10"].applied is True
    assert terminals["credential-oldlab"].applied is True


def test_partial_failed_journal_rejects_reordered_component_chain(
    tmp_path: Path,
) -> None:
    journal = _journal(tmp_path)
    states = {
        "credential-gb10": ComponentState.READY,
        "credential-oldlab": ComponentState.READY,
    }
    apply_calls: list[str] = []

    def grouped(
        component_id: str,
        *,
        implementation_digest: str,
        fail_apply: bool = False,
    ) -> ProtectedApplyComponent:
        def classify(_plan):
            state = states[component_id]
            return ComponentObservation(
                state=state,
                evidence_digest=("1" if component_id == "credential-gb10" else "2") * 64,
                observed_epoch=8 if state is ComponentState.EXACT else 7,
            )

        def apply(_plan):
            apply_calls.append(component_id)
            if fail_apply:
                raise RuntimeError("legacy credential publication failed")
            states[component_id] = ComponentState.EXACT

        return ProtectedApplyComponent(
            component_id=component_id,
            implementation_digest=implementation_digest,
            input_fingerprint=("3" if component_id == "credential-gb10" else "4") * 64,
            classify=classify,
            apply=apply,
            preapply_group="external-supervisor-credentials",
        )

    legacy = (
        grouped("credential-gb10", implementation_digest="5" * 64),
        grouped(
            "credential-oldlab",
            implementation_digest="5" * 64,
            fail_apply=True,
        ),
    )
    with pytest.raises(RuntimeError, match="legacy credential publication failed"):
        journal.execute(_plan(tmp_path), legacy)

    reordered = (
        grouped("credential-oldlab", implementation_digest="6" * 64),
        grouped("credential-gb10", implementation_digest="6" * 64),
    )
    with pytest.raises(ProtectedApplyJournalError, match="chain identity drifted"):
        journal.execute(_plan(tmp_path), reordered)

    assert apply_calls == ["credential-gb10", "credential-oldlab"]
