"""Classification may inspect exact saved recovery, but cannot publish authority."""

import hashlib
import json
from dataclasses import replace

import pytest

from loom_cli.rollout.operator.final_gate_plan import FinalGatePlan
from loom_cli.rollout.operator.protected_apply_journal import ProtectedApplyJournal
from tests.loom_cli.rollout.operator.test_application_admission_recovery import (
    _component,
    _guard,
    _handoff,
    _target,
)
from tests.loom_cli.rollout.operator.test_cnpg_manager_replacement import _manager
from tests.loom_cli.rollout.operator.test_final_gate_plan import _plan
from tests.loom_cli.rollout.operator.test_protected_apply_journal import _journal


def test_absent_recovery_view_does_not_create_a_journal(tmp_path):
    plan = _plan(tmp_path)
    journal = ProtectedApplyJournal(
        tmp_path / "absent", request_id=plan.request_id, attempt_number=plan.attempt_number,
    )
    assert journal.read_application_recovery_view(plan, _component(lambda _: None), ordinal=0) is None
    assert not (tmp_path / "absent").exists()


def _interrupted(tmp_path):
    plan, journal = _plan(tmp_path), _journal(tmp_path)

    def apply(_):
        journal.record_application_admission_recovery(
            target=_target(), handoff_backend=_handoff(), coordination_guard=_guard(),
        )
        journal.prepare_application_handoff_recovery(ordinal=1)
        journal.prepare_application_manager_replacement(identity=_manager())
        journal.begin_application_manager_replacement()
        raise RuntimeError("interrupted PUT")

    component = _component(apply)
    with pytest.raises(RuntimeError, match="interrupted PUT"):
        journal.execute(plan, [component])
    return plan, journal, component


def test_readonly_recovery_view_preserves_pending_work_without_active_apply(tmp_path, monkeypatch):
    plan, journal, component = _interrupted(tmp_path)

    def forbid(*_args, **_kwargs):
        pytest.fail("read-only recovery must not write, fsync or acquire apply authority")

    monkeypatch.setattr(journal, "_publish_or_match", forbid)
    monkeypatch.setattr(journal, "_sync_application_recovery", forbid)
    view = journal.read_application_recovery_view(plan, component, ordinal=0)
    assert view.admission.handoff_backend == _handoff()
    assert view.admission.coordination_guard == _guard()
    assert len(view.handoff_recoveries) == 1
    assert view.handoff_recoveries[0][1] is None
    intent, dispatched, receipt = view.manager_replacement
    assert intent.identity == _manager()
    assert dispatched and receipt is None
    with pytest.raises(RuntimeError, match="active component"):
        journal.begin_application_manager_replacement()
    with pytest.raises(RuntimeError, match="active component"):
        journal.prepare_application_handoff_recovery(ordinal=1)


@pytest.mark.parametrize("changed", ["plan", "implementation", "input", "ordinal", "component"])
def test_recovery_view_refuses_changed_component_binding(tmp_path, changed):
    plan, journal, component = _interrupted(tmp_path)
    ordinal = 0
    if changed == "plan":
        payload = plan.to_dict()
        payload.pop("plan_digest")
        payload["migration_plan_digest"] = "b" * 64
        payload["plan_digest"] = hashlib.sha256(
            json.dumps(payload, sort_keys=True, separators=(",", ":")).encode(),
        ).hexdigest()
        plan = FinalGatePlan.from_dict(payload)
    elif changed == "implementation":
        component = replace(component, implementation_digest="9" * 64)
    elif changed == "input":
        component = replace(component, input_fingerprint="9" * 64)
    elif changed == "component":
        component = replace(component, component_id="another-component")
    else:
        ordinal = 1
    with pytest.raises(RuntimeError, match="recovery view"):
        journal.read_application_recovery_view(plan, component, ordinal=ordinal)


def test_classification_recovery_view_does_not_enable_mutation(tmp_path):
    plan, journal, component = _interrupted(tmp_path)
    original_classify = component.classify
    seen = []

    def classify(candidate):
        seen.append(journal.read_application_recovery_view(candidate, component, ordinal=0))
        with pytest.raises(RuntimeError, match="active component"):
            journal.record_application_manager_replacement(
                identity=replace(_manager(), executable_inode=101),
            )
        return original_classify(candidate)

    component = replace(component, classify=classify, apply=lambda _: None)
    with pytest.raises(RuntimeError, match="did not converge"):
        journal.execute(plan, [component])
    assert len(seen) == 2 and seen[0] == seen[1]
