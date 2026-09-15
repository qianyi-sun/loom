"""Installed database completion cannot invent credentials, peers or guard authority."""

import os
from contextlib import contextmanager
from dataclasses import replace

import pytest

from loom.staging_mutation_coordination import rollout_guard_application_name
from loom_cli.rollout.operator.protected_application_guard_retention import (
    application_guard_is_retained,
)
from tests.loom_cli.rollout.operator.test_application_admission_recovery import (
    _component,
    _handoff,
    _target,
)
from tests.loom_cli.rollout.operator.test_application_admission_recovery import (
    _guard as _database_guard,
)
from tests.loom_cli.rollout.operator.test_application_credential_recovery import (
    _PASSWORD,
    _Runner,
    _sources,
)
from tests.loom_cli.rollout.operator.test_application_guard_retention import _guard
from tests.loom_cli.rollout.operator.test_cnpg_manager_replacement import _manager
from tests.loom_cli.rollout.operator.test_protected_apply_journal import _journal


@pytest.mark.parametrize("schema_revision", ["0142/guard_0035", "0134/guard_0030"])
@pytest.mark.parametrize("missing", [None, "successor", "ack", "manager-receipt", "guard-identity", "pending-peer"])
def test_database_completion_requires_original_journal_authority(tmp_path, monkeypatch, missing, schema_revision):
    from loom_cli.rollout.operator import protected_application_database_completion as module
    from loom_cli.rollout.operator.protected_application_database_completion import (
        complete_protected_application_database,
    )

    plan, live = _sources(tmp_path, schema_revision=schema_revision)
    journal, runner = _journal(tmp_path), _Runner(live)
    guard = _guard(plan)
    for path in (tmp_path / "state", tmp_path / "state/requests", journal.attempt_root.parent.parent, journal.attempt_root.parent):
        path.chmod(0o700)
    database_guard = replace(
        _database_guard(), backend=replace(_handoff(), pid=guard.database_backend_pid),
        application_name=rollout_guard_application_name(
            request_id=guard.request_id, candidate_sha=guard.candidate_sha,
            candidate_tree=guard.candidate_tree, generation=guard.generation,
        ),
    )
    if missing == "guard-identity":
        database_guard = replace(database_guard, backend=replace(database_guard.backend, pid=999))
    expected_backend = replace(_handoff(), pid=125, started_at="2026-09-09T01:02:04+00:00") if missing == "successor" else _handoff()
    peer, maintenance, calls = object(), object(), []
    @contextmanager
    def open_maintenance():
        calls.append("maintenance")
        yield maintenance
    runner.open_staging_peer_maintenance_database = open_maintenance
    def complete(connection, **kwargs):
        assert connection is peer and kwargs["maintenance"] is maintenance
        assert kwargs["password"] == _PASSWORD
        assert kwargs["handoff_backend"] == expected_backend
        assert kwargs["coordination_guard"] == database_guard
        assert kwargs["schema_acl_profile"] == "cnpg-staging"
        assert kwargs["schema_revision"] == schema_revision
        calls.append("sql-complete")
        return "database-result-only"
    monkeypatch.setattr(module, "complete_application_handoff_database", complete)
    def apply(_):
        journal.retain_application_guard(plan, guard=guard)
        if missing != "ack":
            application_guard_is_retained(tmp_path / "state", request_id=plan.request_id,
                                          service_uid=os.getuid(), guard=guard, acknowledge=True)
        journal.record_application_admission_recovery(
            target=_target(), handoff_backend=_handoff(), coordination_guard=database_guard,
        )
        journal.prepare_application_manager_replacement(identity=_manager())
        journal.begin_application_manager_replacement()
        if missing != "manager-receipt":
            journal.record_application_manager_replacement(identity=replace(_manager(), executable_inode=101))
        if missing in {"pending-peer", "successor"}:
            journal.prepare_application_handoff_recovery(ordinal=1)
        if missing == "successor":
            journal.record_application_handoff_replacement(ordinal=1, handoff_backend=expected_backend)
        result = complete_protected_application_database(
            plan, journal=journal, runner=runner, connection=peer, guard=guard,
        )
        assert result == "database-result-only"
        raise RuntimeError("workload recovery still pending")
    with pytest.raises(RuntimeError):
        journal.execute(plan, [_component(apply)])
    if missing not in {None, "successor"}:
        assert calls == []
        assert runner.calls == []  # Refuse before even reading original credentials.
    else:
        assert calls == ["maintenance", "sql-complete"]
    assert not list(journal.root.rglob("terminal.json"))
    assert application_guard_is_retained(tmp_path / "state", request_id=plan.request_id, service_uid=os.getuid())
