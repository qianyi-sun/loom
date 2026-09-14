"""Retirement recovers ambiguous patches without deleting or recreating names."""

import copy
import json

import pytest

from loom_cli.rollout.operator import protected_application_restoration as restoration
from loom_cli.rollout.operator.protected_apply_journal import ProtectedApplyJournal
from loom_cli.rollout.operator.protected_cnpg_fence_acquisition import acquire_cnpg_input_fence
from tests.loom_cli.rollout.operator.test_application_admission_recovery import _component
from tests.loom_cli.rollout.operator.test_application_restoration import _inputs
from tests.loom_cli.rollout.operator.test_application_restoration_journal import _seed
from tests.loom_cli.rollout.operator.test_application_workload_runtime import (
    Runner as WorkloadRunner,
)
from tests.loom_cli.rollout.operator.test_cnpg_fence_acquisition import FenceRunner


class RetirementRunner(FenceRunner):
    def __init__(self, guard, coordination, journal, plan):
        super().__init__()
        self.peer = WorkloadRunner(guard, coordination)
        self.journal, self.plan = journal, plan
        self.patches = []
        self.fail_after = None
        self.before_patch = lambda: None
        self.probes = []
        self.propagated = True

    def capture_stdout(self, argv, *, env, timeout_seconds):
        if '--dry-run=server' in argv:
            self.probes.append(tuple(argv))
            if not self.propagated:
                raise RuntimeError('retirement has not propagated')
            return b'{}'
        return super().capture_stdout(argv, env=env, timeout_seconds=timeout_seconds)

    def open_staging_peer_maintenance_database(self):
        return self.peer.open_staging_peer_maintenance_database()

    def capture_stdout_with_input(self, argv, *, env, input_payload, timeout_seconds):
        if '--dry-run=server' in argv:
            return self.capture_stdout(argv, env=env, timeout_seconds=timeout_seconds)
        if argv[1] == 'create':
            return super().capture_stdout_with_input(argv, env=env, input_payload=input_payload, timeout_seconds=timeout_seconds)
        assert argv[1] == 'patch' and '--patch-file=/dev/stdin' in argv
        assert '--field-manager=loom-cnpg-fence' in argv and '--type=json' in argv
        assert self.journal.read_active_application_recovery_view(self.plan).restoration is not None
        assert self.journal.read_active_application_recovery_view(self.plan).fences_retiring
        self.before_patch()
        value = self.objects[argv[2], argv[3]]
        patch = json.loads(input_payload)
        assert patch[:3] == [
            {'op': 'test', 'path': '/metadata/uid', 'value': value['metadata']['uid']},
            {'op': 'test', 'path': '/metadata/resourceVersion', 'value': value['metadata']['resourceVersion']},
            {'op': 'test', 'path': '/spec', 'value': value['spec']},
        ]
        assert patch[3:] == [{'op': 'replace', 'path': '/spec/matchConditions',
                             'value': [{'name': 'retired-handoff', 'expression': 'false'}]}]
        value['spec']['matchConditions'] = patch[3]['value']
        value['metadata'].update(generation=2, resourceVersion='102')
        self.patches.append(argv[3])
        if len(self.patches) == self.fail_after:
            raise RuntimeError('lost retirement reply')
        return json.dumps(value).encode()


def _case(tmp_path, monkeypatch):
    plan, view, guard, _, _, _ = _inputs(tmp_path)
    journal = ProtectedApplyJournal(tmp_path / 'state', request_id=plan.request_id, attempt_number=plan.attempt_number)
    runner = RetirementRunner(guard, view.admission.coordination_guard, journal, plan)
    monkeypatch.setattr(restoration, 'observe_application_restoration', lambda _, *, view, **kwargs: restoration._bound_evidence(view))
    def initialize(_):
        _seed(plan, journal, view, guard)
        journal.prepare_application_cnpg_fence(plan, target_pooler_names=())
        acquire_cnpg_input_fence(plan, journal=journal, runner=runner)
        raise RuntimeError('acquired')
    component = _component(initialize)
    with pytest.raises(RuntimeError, match='acquired'):
        journal.execute(plan, [component])
    return plan, journal, guard, runner


@pytest.mark.parametrize('lost_reply', [None, 1, 3, 5])
def test_retirement_retains_every_uid_and_resumes_exact_partial_state(tmp_path, monkeypatch, lost_reply):
    from loom_cli.rollout.operator.protected_cnpg_fence_retirement import retire_cnpg_input_fence
    plan, journal, guard, runner = _case(tmp_path, monkeypatch)
    original = copy.deepcopy(runner.objects)
    runner.fail_after = lost_reply
    results = []
    def apply(_):
        results.append(retire_cnpg_input_fence(plan, journal=journal, runner=runner, guard=guard))
        raise RuntimeError('retired')
    component = _component(apply)
    with pytest.raises(RuntimeError, match='lost retirement reply' if lost_reply else 'retired'):
        journal.execute(plan, [component])
    if lost_reply:
        runner.fail_after = None
        with pytest.raises(RuntimeError, match='retired'):
            journal.execute(plan, [component])
    assert len(results) == 1 and len(results[0]) == len(original)
    assert len(runner.patches) == len(set(runner.patches)) == 5
    assert len(runner.creates) == len(original) == 10
    assert len(runner.probes) == 6
    for key, value in original.items():
        current = runner.objects[key]
        assert current['metadata']['uid'] == value['metadata']['uid']
        if value['kind'] == 'ValidatingAdmissionPolicyBinding':
            assert current == value
    with pytest.raises(RuntimeError, match='retired'):
        journal.execute(plan, [component])
    assert len(runner.patches) == 5


@pytest.mark.parametrize('failure', ['restoration', 'guard', 'foreign-binding', 'missing-policy'])
def test_retirement_refuses_before_patch_when_live_authority_or_complete_inventory_fails(tmp_path, monkeypatch, failure):
    from loom_cli.rollout.operator.protected_cnpg_fence_retirement import retire_cnpg_input_fence
    plan, journal, guard, runner = _case(tmp_path, monkeypatch)
    if failure == 'restoration':
        def fail(*args, **kwargs):
            raise RuntimeError('restoration not ready')
        monkeypatch.setattr(restoration, 'observe_application_restoration', fail)
    elif failure == 'guard':
        runner.peer.guard_alive = False
    elif failure == 'foreign-binding':
        list(runner.objects.values())[9]['spec']['validationActions'] = ['Warn']
    else:
        del runner.objects[next(iter(runner.objects))]
    with pytest.raises((ValueError, RuntimeError)):
        journal.execute(plan, [_component(lambda _: retire_cnpg_input_fence(plan, journal=journal, runner=runner, guard=guard))])
    assert runner.patches == []


def test_durable_retirement_decision_prevents_acquisition_before_first_patch(tmp_path, monkeypatch):
    from loom_cli.rollout.operator.protected_cnpg_fence_retirement import retire_cnpg_input_fence
    plan, journal, guard, runner = _case(tmp_path, monkeypatch)
    def crash():
        raise RuntimeError('before first patch')
    runner.before_patch = crash
    with pytest.raises(RuntimeError, match='before first patch'):
        journal.execute(plan, [_component(lambda _: retire_cnpg_input_fence(plan, journal=journal, runner=runner, guard=guard))])
    assert runner.patches == []
    calls = len(runner.calls)
    with pytest.raises(RuntimeError, match='retir'):
        journal.execute(plan, [_component(lambda _: acquire_cnpg_input_fence(plan, journal=journal, runner=runner))])
    assert len(runner.calls) == calls


def test_retirement_requires_all_scope_propagation_and_retries_without_repatching(tmp_path, monkeypatch):
    from loom_cli.rollout.operator.protected_cnpg_fence_retirement import retire_cnpg_input_fence
    plan, journal, guard, runner = _case(tmp_path, monkeypatch)
    runner.propagated = False
    def apply(_):
        retire_cnpg_input_fence(plan, journal=journal, runner=runner, guard=guard)
        raise RuntimeError('retired')
    with pytest.raises(RuntimeError, match='not propagated'):
        journal.execute(plan, [_component(apply)])
    assert len(runner.patches) == 5
    runner.propagated = True
    with pytest.raises(RuntimeError, match='retired'):
        journal.execute(plan, [_component(apply)])
    assert len(runner.patches) == 5 and len(runner.probes) == 7


@pytest.mark.parametrize('drift', [None, 'policy', 'binding', 'propagation', 'record'])
def test_retired_classification_observes_exact_inventory_without_active_apply(tmp_path, monkeypatch, drift):
    from loom_cli.rollout.operator.protected_cnpg_fence_retirement import (
        observe_application_cnpg_fence_retirement,
        retire_cnpg_input_fence,
    )
    plan, journal, guard, runner = _case(tmp_path, monkeypatch)
    def apply(_):
        retire_cnpg_input_fence(plan, journal=journal, runner=runner, guard=guard)
        raise RuntimeError('retired')
    component = _component(apply)
    with pytest.raises(RuntimeError, match='retired'):
        journal.execute(plan, [component])
    if drift == 'policy':
        next(iter(runner.objects.values()))['spec']['matchConditions'][0]['expression'] = 'true'
    elif drift == 'binding':
        list(runner.objects.values())[1]['spec']['validationActions'] = ['Warn']
    elif drift == 'propagation':
        runner.propagated = False
    elif drift == 'record':
        path = journal.root / '00-application-ownership-handoff/application-cnpg-fence-retirement.json'
        record = json.loads(path.read_text())
        record['inventory_sha256'] = 'f' * 64
        path.write_text(json.dumps(record))
    def forbid(*args, **kwargs):
        pytest.fail('retired classification attempted journal mutation or fsync')
    monkeypatch.setattr(journal, '_publish_or_match', forbid)
    monkeypatch.setattr(journal, '_sync_application_recovery', forbid)
    if drift:
        with pytest.raises((RuntimeError, ValueError)):
            observe_application_cnpg_fence_retirement(plan, journal=journal, component=component, ordinal=0, runner=runner)
    else:
        result = observe_application_cnpg_fence_retirement(plan, journal=journal, component=component, ordinal=0, runner=runner)
        assert len(result) == 64
        assert result == observe_application_cnpg_fence_retirement(plan, journal=journal, component=component, ordinal=0, runner=runner)
    assert len(runner.patches) == 5 and len(runner.creates) == 10
