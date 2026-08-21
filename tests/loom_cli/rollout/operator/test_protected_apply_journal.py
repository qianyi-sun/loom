from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

from loom_cli.rollout.operator.final_gate_plan import FinalGatePlanStore
from loom_cli.rollout.operator.protected_apply_journal import (
    ComponentFailure,
    ComponentObservation,
    ComponentState,
    ProtectedApplyComponent,
    ProtectedApplyJournal,
    ProtectedApplyJournalError,
    read_component_failure,
)
from loom_cli.rollout.operator.protected_apply_recovery import (
    find_advanced_epoch_attempt,
)
from loom_cli.rollout.operator.protected_gb10_transport import GB10FleetApplyError
from tests.loom_cli.rollout.operator.test_final_gate_plan import _plan


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
