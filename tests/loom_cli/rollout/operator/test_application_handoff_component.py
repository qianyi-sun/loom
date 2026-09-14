"""Full phase ordering/recovery with real immutable journals and isolated observations.

SQL/Kubernetes effects are controlled here; their separate integration suites
exercise the mechanisms. This is not installed or live migration evidence.
"""

import json
import os
from contextlib import contextmanager, nullcontext
from dataclasses import replace
from uuid import uuid4

import pytest

from loom_cli.rollout.operator.protected_application_guard_retention import (
    application_guard_is_retained,
)
from loom_cli.rollout.operator.protected_apply_journal import (
    ComponentObservation,
    ComponentState,
    ProtectedApplyComponent,
    ProtectedApplyJournal,
)
from tests.loom_cli.rollout.operator.test_application_restoration import _inputs


@pytest.mark.parametrize('interrupt', [None, 'prepare', 'drain', 'dispatch', 'database-complete', 'restore', 'retire'])
def test_handoff_component_resumes_original_phases_without_resealing_or_redispatch(tmp_path, monkeypatch, interrupt):
    from loom_cli.rollout.operator import protected_application_handoff_component as module
    from loom_cli.rollout.operator import protected_application_restoration as restoration

    plan, baseline, guard, _, credential, replaced = _inputs(tmp_path)
    journal = ProtectedApplyJournal(tmp_path / 'state', request_id=plan.request_id, attempt_number=plan.attempt_number)
    current = [baseline.cnpg_runtime]
    events = []
    failure = [interrupt]
    def event(name):
        events.append(name)
        if failure[0] == name:
            failure[0] = None
            raise RuntimeError('lost ' + name + ' acknowledgement')
    class Runner:
        @property
        def environment(self):
            return {}
        def capture_stdout(self, *args, **kwargs):
            return json.dumps({'status': {'currentPrimary': baseline.cnpg_runtime.manager.pod_name}}).encode()
        def open_staging_peer_database(self):
            return nullcontext(object())
        def prepare_staging_application_database(self, candidate, *, journal, connection, guard):
            owner = baseline.owner_creations[0][0]
            journal.prepare_application_owner_creation(candidate, backend=owner.backend, coordination_guard=owner.coordination_guard)
            journal.record_application_owner_oid(candidate, ordinal=1, role_oid=baseline.admission.target.successor_oid)
            journal.record_application_admission_recovery(target=baseline.admission.target,
                handoff_backend=baseline.admission.handoff_backend, coordination_guard=baseline.admission.coordination_guard)
            event('prepare')
        @contextmanager
        def recover_staging_peer_database(self, candidate, *, journal, ordinal, runtime_password):
            journal.prepare_application_handoff_recovery(ordinal=ordinal)
            journal.record_application_handoff_replacement(ordinal=ordinal,
                handoff_backend=replace(baseline.admission.handoff_backend, pid=8000 + ordinal))
            yield object()
        def issue_staging_manager_replacement(self, *, journal, runtime_password):
            assert journal.begin_application_manager_replacement()
            current[0] = replaced
            event('dispatch')
            return True
    retain = journal.retain_application_guard
    def retained(candidate, *, guard):
        retain(candidate, guard=guard)
        assert application_guard_is_retained(tmp_path / 'state', request_id=plan.request_id,
            service_uid=os.geteuid(), guard=guard, acknowledge=True)
    monkeypatch.setattr(journal, 'retain_application_guard', retained)
    monkeypatch.setattr(module, 'observe_application_runtime_credential', lambda *args, **kwargs: credential)
    monkeypatch.setattr(module, 'observe_cnpg_primary_runtime', lambda *args, **kwargs: current[0])
    monkeypatch.setattr('loom_cli.rollout.operator.protected_cnpg_runtime_admission.observe_cnpg_primary_runtime',
                        lambda *args, **kwargs: current[0])
    monkeypatch.setattr(module, '_admit_sql_profiles', lambda *args, **kwargs: events.append('sql-profile'))
    def acquire(candidate, *, journal, runner):
        request = journal.read_application_cnpg_fence(candidate)
        for ordinal, _ in enumerate(request.documents()):
            journal.prepare_application_cnpg_fence_create(candidate, ordinal=ordinal)
            if journal.read_application_cnpg_fence_object(candidate, ordinal=ordinal) is None:
                journal.record_application_cnpg_fence_object(candidate, ordinal=ordinal, uid=str(uuid4()))
        events.append('fence')
    monkeypatch.setattr(module, 'acquire_cnpg_input_fence', acquire)
    def pause(candidate, *, journal, **kwargs):
        journal.record_application_workloads(candidate, workloads=baseline.workloads)
        event('drain')
    monkeypatch.setattr(module, 'pause_application_workloads', pause)
    def restore(candidate, *, journal, **kwargs):
        assert journal.read_application_manager_replacement()[2] is not None
        journal.begin_application_workload_restoration(candidate)
        event('database-complete')
        event('restore')
    monkeypatch.setattr(module, 'restore_application_workloads', restore)
    monkeypatch.setattr(restoration, 'observe_application_restoration',
        lambda *args, view, **kwargs: restoration._bound_evidence(view))
    monkeypatch.setattr(module, 'observe_application_restoration', restoration.observe_application_restoration)
    monkeypatch.setattr(module, 'observe_application_cnpg_fence_retirement', lambda *args, **kwargs: 'f' * 64)
    def retire(candidate, *, journal, runner, guard):
        journal.observe_and_record_application_restoration(candidate, runner=runner, guard=guard)
        journal.begin_application_cnpg_fence_retirement(candidate, guard=guard)
        event('retire')
    monkeypatch.setattr(module, 'retire_cnpg_input_fence', retire)
    component = module.ProtectedApplicationAuthorityHandoffComponent(
        journal=journal, runner=Runner(), ordinal=1, guard_source=lambda _: guard,
        epoch_source=lambda _: plan.starting_mutation_epoch + 1,
        observe_external_authority=lambda *args: 'e' * 64,
    ).component(plan)
    epoch = ProtectedApplyComponent('mutation-epoch-claim', '1' * 64, '2' * 64,
        lambda _: ComponentObservation(ComponentState.EXACT, '3' * 64, plan.starting_mutation_epoch + 1),
        lambda _: pytest.fail('reapplied epoch'))
    if interrupt:
        with pytest.raises(RuntimeError, match='lost ' + interrupt):
            journal.execute(plan, [epoch, component])
        assert application_guard_is_retained(tmp_path / 'state', request_id=plan.request_id, service_uid=os.getuid(), guard=guard)
    result = journal.execute(plan, [epoch, component])
    assert result['application-ownership-handoff'].observed_epoch == plan.starting_mutation_epoch + 1
    assert events.count('prepare') == events.count('dispatch') == 1
    assert events.index('sql-profile') < events.index('prepare') < events.index('drain') < events.index('dispatch') < events.index('restore') < events.index('retire')
    before = list(events)
    assert journal.execute(plan, [epoch, component]) == result
    assert events == before


@pytest.mark.parametrize('boundary', ['classify', 'checkpoint'])
@pytest.mark.parametrize('drift', ['guard', 'epoch'])
def test_observation_cannot_hide_original_guard_or_epoch_loss(tmp_path, monkeypatch, boundary, drift):
    from loom_cli.rollout.operator import protected_application_handoff_component as module

    plan, baseline, guard, _, credential, _ = _inputs(tmp_path)
    journal = ProtectedApplyJournal(tmp_path / 'state', request_id=plan.request_id, attempt_number=plan.attempt_number)
    lost = [False]
    def guard_source(_):
        if lost[0] and drift == 'guard':
            raise RuntimeError('supervised guard lost')
        return guard
    def epoch_source(_):
        return plan.starting_mutation_epoch + (2 if lost[0] and drift == 'epoch' else 1)
    def observe(*args):
        lost[0] = True
        return credential, baseline.cnpg_runtime, 'e' * 64
    component = module.ProtectedApplicationAuthorityHandoffComponent(
        journal=journal, runner=object(), ordinal=1, guard_source=guard_source,
        epoch_source=epoch_source, observe_external_authority=lambda *args: 'e' * 64,
    )
    monkeypatch.setattr(type(component), '_inputs', observe)
    monkeypatch.setattr(journal, 'read_active_application_recovery_view', lambda _: baseline)
    with pytest.raises(RuntimeError, match='guard|epoch'):
        if boundary == 'classify':
            component.classify(plan)
        else:
            component._checkpoint(plan, guard)
    assert not journal.root.exists()
