"""The installed handoff keeps the original supervised guard through closed admission."""

import os
from types import SimpleNamespace

import pytest

from loom_cli.rollout.operator.protected_apply_journal import (
    ComponentObservation,
    ComponentState,
    ProtectedApplyJournal,
)
from tests.loom_cli.rollout.operator.test_application_credential_recovery import _sources
from tests.loom_cli.rollout.operator.test_application_guard_retention import _guard
from tests.loom_cli.rollout.operator.test_final_gate_plan import _plan as _unsupported_plan
from tests.loom_cli.rollout.operator.test_staging_mutation_guard import _config


def _plan(tmp_path):
    return _sources(tmp_path, schema_revision="0134/guard_0030")[0]


@pytest.mark.parametrize('retention', ['absent', 'awaiting-ack', 'acknowledged'])
@pytest.mark.parametrize('drift', [None, 'epoch', 'guard'])
def test_installed_handoff_uses_original_guard_probe_only_after_retention_ack(tmp_path, monkeypatch, retention, drift):
    from loom_cli.rollout.operator import installed_application_handoff as module

    plan, config = _plan(tmp_path), _config(tmp_path)
    original = _guard(plan)
    changed = [False]
    calls = []
    class Guard:
        def assert_ready(self, request_id, *, candidate_config):
            assert request_id == plan.request_id and candidate_config == config
            calls.append('guard')
            if changed[0] and drift == 'guard':
                raise RuntimeError('supervised original guard lost')
            return original
        def observe_retained_epoch(self, guard, *, candidate_config):
            assert retention == 'acknowledged' and guard == original and candidate_config == config
            calls.append('probe')
            changed[0] = True
            return plan.starting_mutation_epoch + (2 if drift == 'epoch' else 1)
    monkeypatch.setattr(module, 'MutationGuardManager', lambda **kwargs: Guard())
    monkeypatch.setattr(module, '_read_pending_retention', lambda *args, **kwargs:
        None if retention == 'absent' else SimpleNamespace(acknowledged=retention == 'acknowledged'))
    def classify(candidate):
        assert retention != 'acknowledged', 'opened an ordinary database connection while admission can be closed'
        calls.append('sql')
        changed[0] = True
        return ComponentObservation(ComponentState.EXACT, 'e' * 64, plan.starting_mutation_epoch + (2 if drift == 'epoch' else 1))
    monkeypatch.setattr(module, 'KubernetesProtectedEpochComponent', lambda **kwargs: SimpleNamespace(classify=classify))
    factory = module.InstalledApplicationHandoffFactory(config=config, service_uid=os.getuid(), successor_source=lambda *args: None,
        runner=SimpleNamespace(environment={'KUBECONFIG': '/fixture'}))
    journal = ProtectedApplyJournal(config.state_root, request_id=plan.request_id, attempt_number=plan.attempt_number)
    component = factory(plan, journal=journal, ordinal=2)
    assert component.component_id == 'application-ownership-handoff'
    assert calls == [], 'building the full chain observed or mutated a component'
    if drift:
        with pytest.raises((ValueError, RuntimeError), match=r'guard|epoch'):
            factory.epoch(plan)
    else:
        assert factory.epoch(plan) == plan.starting_mutation_epoch + 1
        assert calls == ['guard', 'probe' if retention == 'acknowledged' else 'sql', 'guard']
    assert not journal.root.exists()


def test_installed_handoff_refuses_wrong_journal_before_constructing_callbacks(tmp_path, monkeypatch):
    from loom_cli.rollout.operator import installed_application_handoff as module

    plan, config = _plan(tmp_path), _config(tmp_path)
    factory = module.InstalledApplicationHandoffFactory(config=config, service_uid=os.getuid(), successor_source=lambda *args: None,
        runner=SimpleNamespace(environment={'KUBECONFIG': '/fixture'}))
    journal = ProtectedApplyJournal(config.state_root, request_id=plan.request_id, attempt_number=plan.attempt_number + 1)
    with pytest.raises(ValueError, match=r'handoff.*journal'):
        factory(plan, journal=journal, ordinal=2)
    assert not journal.root.exists()


def test_installed_handoff_requires_exact_epoch_claim_identity(tmp_path, monkeypatch):
    from loom_cli.rollout.operator import installed_application_handoff as module

    plan, config = _plan(tmp_path), _config(tmp_path)
    guard = _guard(plan)
    monkeypatch.setattr(module, 'MutationGuardManager', lambda **kwargs: SimpleNamespace(assert_ready=lambda *args, **kw: guard))
    monkeypatch.setattr(module, '_read_pending_retention', lambda *args, **kwargs: None)
    monkeypatch.setattr(module, 'KubernetesProtectedEpochComponent', lambda **kwargs: SimpleNamespace(classify=lambda _: 
        ComponentObservation(ComponentState.DRIFTED, 'f' * 64, plan.starting_mutation_epoch + 1)))
    factory = module.InstalledApplicationHandoffFactory(config=config, service_uid=os.getuid(), successor_source=lambda *args: None,
        runner=SimpleNamespace(environment={'KUBECONFIG': '/fixture'}))
    with pytest.raises(ValueError, match='epoch'):
        factory.epoch(plan)


def test_installed_handoff_rejects_unsupported_checkpoint_before_observation(tmp_path, monkeypatch):
    from loom_cli.rollout.operator import installed_application_handoff as module
    plan, config = _unsupported_plan(tmp_path), _config(tmp_path)
    factory = module.InstalledApplicationHandoffFactory(config=config, service_uid=os.getuid(), successor_source=lambda *args: None,
        runner=SimpleNamespace(environment={}))
    journal = ProtectedApplyJournal(config.state_root, request_id=plan.request_id, attempt_number=plan.attempt_number)
    with pytest.raises(RuntimeError, match="revision"):
        factory(plan, journal=journal, ordinal=2)
    assert not journal.root.exists()


@pytest.mark.parametrize('offset', [1, 2])
def test_only_completed_epoch_reader_accepts_a_bound_advanced_guard(tmp_path, monkeypatch, offset):
    from loom_cli.rollout.operator import installed_application_handoff as module
    from loom_cli.rollout.operator.staging_mutation_guard import MutationGuardEvidence

    plan, config = _plan(tmp_path), _config(tmp_path)
    original = _guard(plan)
    data = {k: v for k, v in original.to_dict().items() if k not in {'schema_version', 'evidence_digest'}}
    data.update(generation='d' * 32, database_backend_pid=999, mutation_epoch=original.mutation_epoch + offset)
    current = MutationGuardEvidence.build(**data)
    monkeypatch.setattr(module, 'MutationGuardManager', lambda **kwargs: SimpleNamespace(assert_ready=lambda *args, **kw: current))
    monkeypatch.setattr(module, 'KubernetesProtectedEpochComponent', lambda **kwargs: SimpleNamespace(classify=lambda _:
        ComponentObservation(ComponentState.EXACT, 'f' * 64, plan.starting_mutation_epoch + 1)))
    factory = module.InstalledApplicationHandoffFactory(config=config, service_uid=os.getuid(),
        successor_source=lambda *args: None, runner=SimpleNamespace(environment={'KUBECONFIG': '/fixture'}))
    with pytest.raises(ValueError, match='original guard'):
        factory.epoch(plan)
    if offset == 1:
        assert factory.completed_epoch(plan) == plan.starting_mutation_epoch + 1
    else:
        with pytest.raises(ValueError, match='current guard'):
            factory.completed_epoch(plan)
