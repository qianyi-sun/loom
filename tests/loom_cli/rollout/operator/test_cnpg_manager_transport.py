"""The fixed replacement transport never repeats uncertain dispatch."""

from contextlib import contextmanager
from dataclasses import replace
from types import SimpleNamespace

import pytest

from loom_cli.rollout.operator.protected_apply_executor import SubprocessProtectedApplyCommandRunner
from tests.loom_cli.rollout.operator.test_application_admission_recovery import _component
from tests.loom_cli.rollout.operator.test_cnpg_manager_replacement import _admit, _manager
from tests.loom_cli.rollout.operator.test_final_gate_plan import _plan
from tests.loom_cli.rollout.operator.test_protected_apply_journal import _journal


def test_manager_transport_requires_active_intent_before_any_io(tmp_path, monkeypatch):
    def forbidden(*args, **kwargs):
        pytest.fail("unadmitted replacement reached a process")

    monkeypatch.setattr("subprocess.Popen", forbidden)
    runner, journal = SubprocessProtectedApplyCommandRunner(), _journal(tmp_path)
    with pytest.raises(RuntimeError, match="active component"):
        runner.issue_staging_manager_replacement(journal=journal)


@pytest.mark.parametrize("upload_error", [None, TimeoutError("private diagnostic"), OSError("private diagnostic")])
def test_manager_transport_dispatches_once_and_keeps_ambiguous_result(tmp_path, monkeypatch, upload_error):
    from loom_cli.rollout.operator import protected_cnpg_manager_transport as transport

    plan, journal = _plan(tmp_path), _journal(tmp_path)
    runner = SubprocessProtectedApplyCommandRunner()
    calls = []

    def issue():
        assert journal.read_application_manager_replacement()[1] is True
        calls.append("upload")
        if upload_error:
            raise upload_error

    @contextmanager
    def channel(*args, **kwargs):
        calls.append("open")
        try:
            yield SimpleNamespace(issue=issue)
        finally:
            calls.append("close")

    monkeypatch.setattr(transport, "_prepared_update", channel)

    def apply(_):
        _admit(journal)
        journal.prepare_application_manager_replacement(identity=_manager())
        assert runner.issue_staging_manager_replacement(journal=journal) is True
        assert runner.issue_staging_manager_replacement(journal=journal) is False
        assert journal.read_application_manager_replacement()[2] is None
        raise RuntimeError("reconciliation still required")

    with pytest.raises(RuntimeError, match="reconciliation still required"):
        journal.execute(plan, [_component(apply)])
    assert calls == ["open", "upload", "close"]


def test_preparation_failure_does_not_consume_dispatch(tmp_path, monkeypatch):
    from loom_cli.rollout.operator import protected_cnpg_manager_transport as transport

    plan, journal = _plan(tmp_path), _journal(tmp_path)

    @contextmanager
    def unavailable(*args, **kwargs):
        raise RuntimeError("CNPG manager channel unavailable")
        yield  # pragma: no cover

    monkeypatch.setattr(transport, "_prepared_update", unavailable)

    def apply(_):
        _admit(journal)
        journal.prepare_application_manager_replacement(identity=_manager())
        with pytest.raises(RuntimeError, match="channel unavailable"):
            SubprocessProtectedApplyCommandRunner().issue_staging_manager_replacement(journal=journal)
        assert journal.read_application_manager_replacement()[1:] == (False, None)
        raise RuntimeError("still undispatched")

    with pytest.raises(RuntimeError, match="still undispatched"):
        journal.execute(plan, [_component(apply)])


@pytest.mark.parametrize("change", [
    {"pod_uid": "33333333-3333-4333-8333-333333333333"},
    {"container_id": "containerd://" + "b" * 64},
    {"process_started_ticks": 12346}, {"executable_inode": 101},
])
def test_transport_refuses_changed_identity_before_binary_or_tunnel(tmp_path, monkeypatch, change):
    from loom_cli.rollout.operator import protected_cnpg_manager_transport as transport

    def forbidden(*args, **kwargs):
        pytest.fail("changed manager reached binary or tunnel subprocess")

    monkeypatch.setattr(transport, "_observe_identity", lambda *a, **k: replace(_manager(), **change))
    monkeypatch.setattr("subprocess.Popen", forbidden)
    plan, journal = _plan(tmp_path), _journal(tmp_path)

    def apply(_):
        _admit(journal)
        journal.prepare_application_manager_replacement(identity=_manager())
        with pytest.raises(RuntimeError, match="identity changed"):
            SubprocessProtectedApplyCommandRunner().issue_staging_manager_replacement(journal=journal)
        assert journal.read_application_manager_replacement()[1:] == (False, None)
        raise RuntimeError("identity refused")

    with pytest.raises(RuntimeError, match="identity refused"):
        journal.execute(plan, [_component(apply)])
