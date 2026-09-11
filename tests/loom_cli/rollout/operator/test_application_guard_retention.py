"""An interrupted handoff must keep its original supervised database guard."""

import os
from contextlib import contextmanager
from dataclasses import replace

import pytest

from loom_cli.rollout.operator import staging_mutation_guard as guard_module
from loom_cli.rollout.operator.protected_application_guard_retention import (
    application_guard_is_retained,
)
from loom_cli.rollout.operator.protected_apply_journal import ComponentObservation, ComponentState
from loom_cli.rollout.operator.staging_mutation_guard import MutationGuardEvidence
from tests.loom_cli.rollout.operator.test_application_admission_recovery import _component
from tests.loom_cli.rollout.operator.test_final_gate_plan import _plan
from tests.loom_cli.rollout.operator.test_protected_apply_journal import _journal
from tests.loom_cli.rollout.operator.test_staging_mutation_guard import (
    _CANDIDATE_SHA,
    _CANDIDATE_TREE,
    _Cluster,
    _GENERATION,
    _config,
    _query_context,
)


def _guard(plan):
    return MutationGuardEvidence.build(
        request_id=plan.request_id, candidate_sha=plan.candidate_sha,
        candidate_tree=plan.candidate_tree, generation=_GENERATION,
        mutation_epoch=plan.starting_mutation_epoch, guard_pid=os.getpid(),
        database_backend_pid=4321, deadline_unix_seconds=2_000_000_000,
        cronjob_uid="50de34f1-f12b-4dce-9f1c-e049f066bc54",
        suspended_resource_version="11", state="ready",
    )


def _setup(tmp_path):
    plan, journal = _plan(tmp_path), _journal(tmp_path)
    for path in (tmp_path / "state", tmp_path / "state/requests", journal.attempt_root.parent.parent,
                 journal.attempt_root.parent):
        path.chmod(0o700)
    return plan, journal


def test_no_database_mutation_authority_before_guard_acknowledges_retention(tmp_path):
    plan, journal = _setup(tmp_path)
    guard = _guard(plan)
    with pytest.raises(RuntimeError, match="active component"):
        journal.retain_application_guard(plan, guard=guard)

    def apply(_):
        journal.retain_application_guard(plan, guard=guard)
        with pytest.raises(RuntimeError, match="acknowledgement"):
            journal.require_application_guard_retained(plan, guard=guard)
        assert application_guard_is_retained(
            tmp_path / "state", request_id=plan.request_id, service_uid=os.getuid(),
            guard=guard, acknowledge=True,
        )
        journal.require_application_guard_retained(plan, guard=guard)
        raise RuntimeError("interrupted after acknowledgement")

    with pytest.raises(RuntimeError, match="interrupted after acknowledgement"):
        journal.execute(plan, [_component(apply)])
    assert application_guard_is_retained(
        tmp_path / "state", request_id=plan.request_id, service_uid=os.getuid(), guard=guard,
    )


def test_worker_exit_keeps_same_guard_until_exact_component_terminal(tmp_path, monkeypatch):
    plan, journal = _setup(tmp_path)
    config = replace(_config(tmp_path), state_root=tmp_path / "state")
    cluster = _Cluster()
    saved = []
    complete = False
    original_publish = guard_module._publish_evidence

    def apply(_):
        journal.retain_application_guard(plan, guard=saved[0])
        raise RuntimeError("worker interrupted")

    component = replace(_component(apply), classify=lambda _: ComponentObservation(
        ComponentState.EXACT if complete else ComponentState.READY, "3" * 64,
        plan.starting_mutation_epoch + 1,
    ))

    def publish(configuration, evidence, *, service_uid):
        original_publish(configuration, evidence, service_uid=service_uid)
        if evidence.state == "ready":
            saved.append(evidence)
            with pytest.raises(RuntimeError, match="worker interrupted"):
                journal.execute(plan, [component])

    monkeypatch.setattr(guard_module, "_publish_evidence", publish)
    sleeps = []

    def sleep(seconds):
        nonlocal complete
        sleeps.append(seconds)
        assert len(sleeps) == 1, "guard did not observe the safe terminal"
        assert "restore" not in cluster.events and "unlock" not in cluster.events
        assert cluster.cronjob["spec"]["suspend"] is True
        assert application_guard_is_retained(
            config.state_root, request_id=plan.request_id, service_uid=os.getuid(), guard=saved[0],
        )
        # The test's stand-in component reaches its exact terminal. Production
        # must supply the complete live database/workload/fence classification.
        complete = True
        journal.execute(plan, [component])

    @contextmanager
    def query_context(**_kwargs):
        with _query_context([True], cluster.events) as query:
            yield lambda statement: (
                ({"mutation_epoch": plan.starting_mutation_epoch},)
                if statement == guard_module._READ_EPOCH_SQL else query(statement)
            )

    evidence = guard_module.hold_request_guard(
        config=config, request_id=plan.request_id, generation=_GENERATION,
        service_uid=os.getuid(), run=cluster, query_context=query_context,
        resolve_candidate=lambda _: (_CANDIDATE_SHA, _CANDIDATE_TREE),
        stop_requested=lambda: False, owner_running=lambda _: False,
        owner_launch_grace_seconds=0, sleep=sleep,
    )
    assert sleeps == [1.0]
    assert evidence.database_backend_pid == saved[0].database_backend_pid
    assert evidence.generation == saved[0].generation and evidence.state == "released"
    assert cluster.events.count("try-lock") == 1
    assert cluster.events[-2:] == ["restore", "unlock"]


@pytest.mark.parametrize("drift", ["guard", "terminal", "intent", "symlink"])
def test_changed_recovery_records_never_allow_guard_release(tmp_path, drift):
    plan, journal = _setup(tmp_path)
    guard = _guard(plan)

    def apply(_):
        journal.retain_application_guard(plan, guard=guard)
        raise RuntimeError("retained")

    with pytest.raises(RuntimeError, match="retained"):
        journal.execute(plan, [_component(apply)])
    root = journal.root / "00-application-ownership-handoff"
    if drift == "guard":
        payload = guard.to_dict()
        payload.pop("evidence_digest")
        guard = MutationGuardEvidence.build(**{key: value for key, value in payload.items()
                                              if key != "schema_version"} | {"database_backend_pid": 4322})
    elif drift == "terminal":
        (root / "terminal.json").write_text('{}')
        (root / "terminal.json").chmod(0o600)
    elif drift == "intent":
        (root / "intent.json").write_text('{}')
    else:
        root.rename(root.with_name("original"))
        root.symlink_to(root.with_name("original"), target_is_directory=True)
    with pytest.raises((RuntimeError, ValueError)):
        application_guard_is_retained(
            tmp_path / "state", request_id=plan.request_id, service_uid=os.getuid(), guard=guard,
        )
