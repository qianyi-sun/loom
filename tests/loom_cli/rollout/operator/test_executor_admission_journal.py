"""Executor issuance retains one private credential before the SQL effect."""

import os
from dataclasses import replace

import pytest

from loom_cli.rollout.operator.protected_application_guard_retention import (
    application_guard_is_retained,
)
from tests.loom_cli.rollout.operator.test_application_admission_recovery import _component
from tests.loom_cli.rollout.operator.test_application_guard_retention import _guard, _setup
from tests.loom_cli.rollout.operator.test_application_migration_journal import _authority


def _record(plan, component, guard):
    from loom_cli.rollout.operator.protected_executor_admission_journal import (
        ExecutorAdmissionRecord,
    )

    authority = _authority(plan, component, guard)
    return ExecutorAdmissionRecord.from_dict({
        "schema_version": 1, "admission": authority["admission"], "guard": guard.to_dict(),
        "bootstrap_terminal_digest": "a" * 64, "bootstrap_event_digest": "b" * 64,
        "inputs_digest": "c" * 64, "seed_digest": "d" * 64,
        "identity": {"role_oid": 93, "privilege_sha256": "e" * 64}, "password": "p" * 64,
    })


@pytest.mark.parametrize("lost_reply", [False, True])
def test_executor_issuance_retains_same_credential_on_interruption(tmp_path, lost_reply):
    from loom_cli.rollout.operator.protected_executor_admission_journal import (
        ExecutorAdmissionJournal,
    )

    plan, journal = _setup(tmp_path)
    guard = _guard(plan)
    captured = []
    def apply(_):
        saved = ExecutorAdmissionJournal(journal, plan, component, 0)
        journal.retain_application_guard(plan, guard=guard)
        with pytest.raises(RuntimeError, match="acknowledgement"):
            saved.retain(_record(plan, component, guard), guard=guard)
        application_guard_is_retained(tmp_path / "state", request_id=plan.request_id,
            service_uid=os.getuid(), guard=guard, acknowledge=True)
        record = _record(plan, component, guard)
        saved.retain(record, guard=guard)
        assert saved.read() == (record, False)
        assert record.password not in repr(record)
        assert (saved.root / "executor-admission.json").stat().st_mode & 0o777 == 0o600
        with pytest.raises(RuntimeError, match="changed"):
            saved.retain(replace(record, password="x" * 64), guard=guard)
        if lost_reply:
            saved.mark_issued(record, guard=guard)
            saved.mark_issued(record, guard=guard)
        captured.append(record)
        raise RuntimeError("lost reply")
    component = replace(_component(apply), component_id="executor-database-admission")
    saved = ExecutorAdmissionJournal(journal, plan, component, 0)
    assert saved.read() is None
    with pytest.raises(RuntimeError, match="active component"):
        saved.retain(_record(plan, component, guard), guard=guard)
    with pytest.raises(RuntimeError, match="lost reply"):
        journal.execute(plan, [component])
    assert saved.read() == (captured[0], lost_reply)
    with pytest.raises(RuntimeError, match="active component"):
        saved.mark_issued(captured[0], guard=guard)
    # A completion marker cannot stand in for the credential if its file is lost.
    (saved.root / "executor-admission.json").unlink()
    if lost_reply:
        with pytest.raises(RuntimeError, match="issuance"):
            saved.read()


@pytest.mark.parametrize("drift", ["guard", "intent", "role", "target", "password", "extra"])
def test_executor_admission_record_rejects_unbound_authority(tmp_path, drift):
    from loom_cli.rollout.operator.protected_executor_admission_journal import (
        ExecutorAdmissionRecord,
    )

    plan, _ = _setup(tmp_path)
    component = replace(_component(lambda _: None), component_id="executor-database-admission")
    record = _record(plan, component, _guard(plan)).to_dict()
    if drift == "guard":
        record["admission"]["coordination_guard"]["backend"]["pid"] += 1
    elif drift == "intent":
        record["admission"]["intent_digest"] = "bad"
    elif drift == "role":
        record["identity"]["role_oid"] = True
    elif drift == "target":
        record["admission"]["target"]["database"] = "postgres"
    elif drift == "password":
        record["password"] = "short"
    else:
        record["extra"] = True
    with pytest.raises((ValueError, RuntimeError), match=r"admission|recovery"):
        ExecutorAdmissionRecord.from_dict(record)
