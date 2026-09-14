"""Bounded history selection around independently validated handoff journals."""

import os
from dataclasses import replace
from pathlib import Path

import pytest

from loom_cli.rollout.operator.final_gate_plan import FinalGatePlan, _hash_json
from loom_cli.rollout.operator.protected_application_restoration import _bound_evidence
from loom_cli.rollout.operator.protected_apply_journal import (
    ComponentIntent,
    ComponentObservation,
    ComponentState,
    ComponentTerminal,
    ProtectedApplyJournal,
)
from tests.loom_cli.rollout.operator.test_application_admission_recovery import _component
from tests.loom_cli.rollout.operator.test_application_restoration import _inputs


def _later(plan):
    payload = {k: v for k, v in plan.to_dict().items() if k != "plan_digest"}
    payload.update(request_id="req-next-owner-rollout", starting_mutation_epoch=plan.starting_mutation_epoch + 1)
    return FinalGatePlan.from_dict({**payload, "plan_digest": _hash_json(payload)})


def _saved(tmp_path, monkeypatch):
    plan, view, *_ = _inputs(tmp_path)
    component = _component(lambda _: None)
    intent = ComponentIntent.build(plan, component, 2)
    view = replace(view, intent=intent, restoration=_bound_evidence(view), fences_retiring=True)
    terminal = ComponentTerminal.build(intent, ComponentObservation(ComponentState.EXACT, "a" * 64,
        plan.starting_mutation_epoch + 1), applied=True)
    root = tmp_path / "history"
    journal = ProtectedApplyJournal(root, request_id=plan.request_id, attempt_number=plan.attempt_number)
    for directory in (root, root / "requests", journal.attempt_root.parent.parent, journal.attempt_root.parent,
                      journal.attempt_root, journal.root, journal.root / "02-application-ownership-handoff"):
        directory.mkdir(mode=0o700, exist_ok=True)
    journal._publish_or_match(journal.attempt_root / "final-gate-plan.json", plan.to_dict())
    journal._publish_or_match(journal.root / "02-application-ownership-handoff" / "intent.json", intent.to_dict())
    journal._publish_or_match(journal.root / "02-application-ownership-handoff" / "terminal.json", terminal.to_dict())
    state = {"view": view, "terminal": terminal}
    def read(self, candidate, bound, *, ordinal):
        assert candidate == plan and bound == component and ordinal == 2
        return state["terminal"]
    monkeypatch.setattr(ProtectedApplyJournal, "read_application_handoff_terminal", read)
    monkeypatch.setattr(ProtectedApplyJournal, "read_application_recovery_view", lambda *args, **kwargs: state["view"])
    return plan, _later(plan), component, journal, root, state


@pytest.mark.parametrize("drift", [None, "terminal", "view"])
def test_history_retains_only_the_original_completed_source_and_rechecks_it(tmp_path, monkeypatch, drift):
    from loom_cli.rollout.operator.protected_application_handoff_history import (
        select_completed_handoff,
    )

    original, current, component, journal, root, state = _saved(tmp_path, monkeypatch)
    calls = []
    def build(plan, *, journal, ordinal):
        assert plan == original and ordinal == 2
        calls.append(journal)
        return component
    source = select_completed_handoff(current, state_root=root, service_uid=os.getuid(), build_component=build)
    assert source is not None and source.plan == original and source.journal.root == journal.root
    assert len(calls) == 1
    assert source.read() == (state["view"], state["terminal"])
    if drift:
        state[drift] = None
        with pytest.raises(RuntimeError, match="historical handoff"):
            source.read()


def test_history_does_not_read_current_request_or_pending_terminal(tmp_path, monkeypatch):
    from loom_cli.rollout.operator.protected_application_handoff_history import (
        select_completed_handoff,
    )

    original, current, _component_unused, journal, root, _ = _saved(tmp_path, monkeypatch)
    def never(*args, **kwargs):
        pytest.fail("selected a current or unfinished operation")
    assert select_completed_handoff(original, state_root=root, service_uid=os.getuid(), build_component=never) is None
    (journal.root / "02-application-ownership-handoff" / "terminal.json").unlink()
    assert select_completed_handoff(current, state_root=root, service_uid=os.getuid(), build_component=never) is None


def test_history_rejects_unsafe_request_directory(tmp_path, monkeypatch):
    from loom_cli.rollout.operator.protected_application_handoff_history import (
        select_completed_handoff,
    )

    _, current, component, _, root, _ = _saved(tmp_path, monkeypatch)
    (root / "requests" / "unsafe-request").symlink_to(Path("/tmp"))
    with pytest.raises((ValueError, RuntimeError), match="historical handoff"):
        select_completed_handoff(current, state_root=root, service_uid=os.getuid(), build_component=lambda *args, **kwargs: component)
