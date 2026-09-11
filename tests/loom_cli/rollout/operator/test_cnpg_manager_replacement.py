"""Same-process replacement is single-dispatch and retains immutable authority."""

from dataclasses import replace

import pytest

from tests.loom_cli.rollout.operator.test_application_admission_recovery import (
    _component,
    _guard,
    _handoff,
    _target,
)
from tests.loom_cli.rollout.operator.test_final_gate_plan import _plan
from tests.loom_cli.rollout.operator.test_protected_apply_journal import _journal


def _manager():
    from loom_cli.rollout.operator.protected_cnpg_manager_replacement import CNPGManagerIdentity

    return CNPGManagerIdentity(
        pod_name="loom-postgres-1",
        pod_uid="22222222-2222-4222-8222-222222222222",
        container_id="containerd://" + "a" * 64,
        node_name="trt-eai-oldlab-4",
        restart_count=6,
        process_started_ticks=12345,
        executable_device=24,
        executable_inode=100,
    )


def _admit(journal):
    journal.record_application_admission_recovery(
        target=_target(), handoff_backend=_handoff(), coordination_guard=_guard(),
    )


def test_manager_replacement_requires_active_original_guard_and_durable_single_dispatch(tmp_path):
    plan, journal = _plan(tmp_path), _journal(tmp_path)
    with pytest.raises(RuntimeError, match="active component"):
        journal.prepare_application_manager_replacement(identity=_manager())

    def apply(_):
        with pytest.raises(RuntimeError, match="original.*guard"):
            journal.prepare_application_manager_replacement(identity=_manager())
        _admit(journal)
        intent = journal.prepare_application_manager_replacement(identity=_manager())
        assert journal.prepare_application_manager_replacement(identity=_manager()) == intent
        assert journal.read_application_manager_replacement() == (intent, False, None)
        assert journal.begin_application_manager_replacement() is True
        assert journal.begin_application_manager_replacement() is False
        assert journal.read_application_manager_replacement() == (intent, True, None)
        with pytest.raises(RuntimeError, match="cannot be replaced"):
            journal.prepare_application_manager_replacement(
                identity=replace(_manager(), executable_inode=101),
            )
        raise RuntimeError("interrupted after possible PUT")

    with pytest.raises(RuntimeError, match="possible PUT"):
        journal.execute(plan, [_component(apply)])
    intent_path = journal.root / "00-application-ownership-handoff/application-manager-intent.json"
    saved = intent_path.read_bytes()
    assert intent_path.stat().st_mode & 0o777 == 0o600

    journal = _journal(tmp_path)

    def recover(_):
        assert journal.begin_application_manager_replacement() is False
        with pytest.raises(ValueError, match="executable transition"):
            journal.record_application_manager_replacement(identity=_manager())
        successor = replace(_manager(), executable_inode=101)
        receipt = journal.record_application_manager_replacement(identity=successor)
        assert journal.record_application_manager_replacement(identity=successor) == receipt
        assert journal.read_application_manager_replacement()[2] == receipt
        with pytest.raises(RuntimeError, match="cannot be replaced"):
            journal.record_application_manager_replacement(
                identity=replace(successor, executable_inode=102),
            )
        assert intent_path.read_bytes() == saved
        raise RuntimeError("recovered exact successor")

    with pytest.raises(RuntimeError, match="exact successor"):
        journal.execute(plan, [_component(recover)])


@pytest.mark.parametrize("change", [
    {"pod_uid": "33333333-3333-4333-8333-333333333333"},
    {"container_id": "containerd://" + "b" * 64},
    {"node_name": "trt-eai-oldlab-5"},
    {"restart_count": 7},
    {"process_started_ticks": 12346},
    {"executable_device": 25},
])
def test_replacement_cannot_adopt_another_process_or_volume(tmp_path, change):
    plan, journal = _plan(tmp_path), _journal(tmp_path)

    def apply(_):
        _admit(journal)
        journal.prepare_application_manager_replacement(identity=_manager())
        assert journal.begin_application_manager_replacement()
        with pytest.raises(ValueError, match="executable transition"):
            journal.record_application_manager_replacement(
                identity=replace(_manager(), executable_inode=101, **change),
            )
        raise RuntimeError("foreign successor refused")

    with pytest.raises(RuntimeError, match="successor refused"):
        journal.execute(plan, [_component(apply)])


@pytest.mark.parametrize("change", [
    {"node_name": "trt-eai-oldlab-2"}, {"node_name": "trt-eai-oldlab-1"},
    {"pod_name": "other"}, {"restart_count": True}, {"process_started_ticks": 0},
    {"container_id": "unknown"}, {"executable_inode": -1},
])
def test_manager_identity_refuses_unsupported_targets(change):
    with pytest.raises(ValueError, match="manager identity"):
        replace(_manager(), **change)


def test_receipt_cannot_precede_dispatch(tmp_path):
    plan, journal = _plan(tmp_path), _journal(tmp_path)

    def apply(_):
        _admit(journal)
        journal.prepare_application_manager_replacement(identity=_manager())
        with pytest.raises(RuntimeError, match="dispatch"):
            journal.record_application_manager_replacement(
                identity=replace(_manager(), executable_inode=101),
            )
        raise RuntimeError("unissued transition refused")

    with pytest.raises(RuntimeError, match="unissued transition"):
        journal.execute(plan, [_component(apply)])
