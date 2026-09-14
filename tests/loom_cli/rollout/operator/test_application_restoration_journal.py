"""Only fresh combined observation under retained authority can publish restoration."""

import json
import os

import pytest

from loom_cli.rollout.operator import protected_application_restoration as restoration
from loom_cli.rollout.operator.protected_application_guard_retention import (
    application_guard_is_retained,
)
from loom_cli.rollout.operator.protected_apply_journal import ProtectedApplyJournal
from tests.loom_cli.rollout.operator.test_application_admission_recovery import _component
from tests.loom_cli.rollout.operator.test_application_restoration import _inputs


def _seed(plan, journal, view, guard, *, acknowledge=True):
    journal.retain_application_guard(plan, guard=guard)
    if acknowledge:
        assert application_guard_is_retained(journal.attempt_root.parents[3], request_id=plan.request_id,
            service_uid=os.getuid(), guard=guard, acknowledge=True)
    journal.record_application_cnpg_runtime(plan, runtime=view.cnpg_runtime)
    journal.record_application_credential_recovery(plan, binding=view.credential_binding)
    journal.record_application_cnpg_configuration(plan, binding=view.cnpg_configuration)
    owner = view.owner_creations[0][0]
    journal.prepare_application_owner_creation(plan, backend=owner.backend, coordination_guard=owner.coordination_guard)
    journal.record_application_owner_oid(plan, ordinal=1, role_oid=view.admission.target.successor_oid)
    journal.record_application_admission_recovery(target=view.admission.target,
        handoff_backend=view.admission.handoff_backend, coordination_guard=view.admission.coordination_guard)
    journal.prepare_application_manager_replacement(identity=view.cnpg_runtime.manager)
    journal.begin_application_manager_replacement()
    journal.record_application_manager_replacement(identity=view.manager_replacement[2].identity)
    journal.record_application_workloads(plan, workloads=view.workloads)
    journal.begin_application_workload_restoration(plan)


@pytest.mark.parametrize('failure', [None, 'unacknowledged', 'observation', 'late-guard', 'fsync'])
def test_restoration_publication_requires_observation_retention_and_durable_readback(tmp_path, monkeypatch, failure):
    plan, view, guard, runner, _, _ = _inputs(tmp_path)
    journal = ProtectedApplyJournal(tmp_path / "state", request_id=plan.request_id, attempt_number=plan.attempt_number)
    observed = []
    def observe(candidate, *, view, **kwargs):
        observed.append(view)
        assert candidate == plan and kwargs['guard'] == guard
        if failure == 'observation':
            raise RuntimeError('live restoration drift')
        if failure == 'late-guard':
            (journal.attempt_root.parent.parent / 'application-guard-retention-ack.json').unlink()
        return restoration._bound_evidence(view)
    monkeypatch.setattr(restoration, 'observe_application_restoration', observe)
    original_sync = journal._sync_application_recovery
    def sync(root, filename):
        if failure == 'fsync' and filename == 'application-restoration.json':
            raise OSError('lost restoration fsync')
        original_sync(root, filename)
    monkeypatch.setattr(journal, '_sync_application_recovery', sync)
    result = []
    def apply(_):
        if journal.read_application_admission_recovery() is None:
            _seed(plan, journal, view, guard, acknowledge=failure != 'unacknowledged')
        result.append(journal.observe_and_record_application_restoration(plan, runner=runner, guard=guard))
        assert journal.read_active_application_recovery_view(plan).restoration == result[0]
        raise RuntimeError('after restoration')
    component = _component(apply)
    with pytest.raises((RuntimeError, OSError)):
        journal.execute(plan, [component])
    assert bool(result) == (failure is None)
    assert bool(observed) == (failure != 'unacknowledged')
    path = journal.root / '00-application-ownership-handoff/application-restoration.json'
    assert path.exists() == (failure in {None, 'fsync'})
    if failure == 'fsync':
        previous = path.read_bytes()
        failure = None
        with pytest.raises(RuntimeError, match='after restoration'):
            journal.execute(plan, [component])
        assert len(observed) == 2 and len(result) == 1
        assert path.read_bytes() == previous
    if failure is None:
        # A durable old outcome cannot skip a new live observation on resume.
        failure = 'observation'
        with pytest.raises(RuntimeError, match='live restoration drift'):
            journal.execute(plan, [component])
        assert len(result) == 1
        monkeypatch.setattr(journal, '_sync_application_recovery', lambda *_: pytest.fail('classification fsync'))
        recovered = journal.read_application_recovery_view(plan, component, ordinal=0)
        assert recovered.restoration == result[0]
        record = json.loads(path.read_text())
        record['workloads_sha256'] = 'f' * 64
        path.write_text(json.dumps(record))
        with pytest.raises(RuntimeError, match='restoration'):
            journal.read_application_recovery_view(plan, component, ordinal=0)


def test_active_view_and_publication_refuse_outside_original_apply(tmp_path):
    plan, _, guard, runner, _, _ = _inputs(tmp_path)
    journal = ProtectedApplyJournal(tmp_path / "state", request_id=plan.request_id, attempt_number=plan.attempt_number)
    with pytest.raises(RuntimeError, match='active component'):
        journal.read_active_application_recovery_view(plan)
    with pytest.raises(RuntimeError, match='active component'):
        journal.observe_and_record_application_restoration(plan, runner=runner, guard=guard)
