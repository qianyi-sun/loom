"""Completed replay observes current authority without reusing the old guard or schema."""

from dataclasses import replace

import pytest

from loom.application_completed_authority import ApplicationOwnerSuccessor
from loom_cli.rollout.operator.protected_application_restoration import _bound_evidence
from loom_cli.rollout.operator.protected_apply_journal import (
    ComponentObservation,
    ComponentState,
    ComponentTerminal,
)
from loom_cli.rollout.operator.staging_mutation_guard import MutationGuardEvidence
from tests.loom_cli.rollout.operator.test_application_restoration import _inputs


@pytest.mark.parametrize('guard_epoch', ['original', 'advanced', 'new-request'])
@pytest.mark.parametrize('drift', [None, 'credential-uid', 'credential-body', 'cluster', 'late-inputs', 'late-guard', 'late-epoch', 'successor'])
def test_completed_effect_brackets_current_inputs_and_restricted_runtime(tmp_path, monkeypatch, guard_epoch, drift):
    from loom_cli.rollout.operator import protected_application_completed as module

    plan, view, guard, runner, credential, runtime = _inputs(tmp_path)
    view = replace(view, restoration=_bound_evidence(view), fences_retiring=True)
    terminal = ComponentTerminal.build(view.intent, ComponentObservation(ComponentState.EXACT, 'a' * 64, plan.starting_mutation_epoch + 1), applied=True)
    original_plan = plan
    if guard_epoch == 'new-request':
        from loom_cli.rollout.operator.final_gate_plan import FinalGatePlan, _hash_json
        payload = {k: v for k, v in plan.to_dict().items() if k != "plan_digest"}
        payload.update(request_id="req-next-owner-rollout", starting_mutation_epoch=plan.starting_mutation_epoch + 1)
        plan = FinalGatePlan.from_dict({**payload, "plan_digest": _hash_json(payload)})
        data = {k: v for k, v in guard.to_dict().items() if k not in {"schema_version", "evidence_digest"}}
        guard = MutationGuardEvidence.build(**{**data, "request_id": plan.request_id, "mutation_epoch": plan.starting_mutation_epoch,
            "generation": "d" * 32, "database_backend_pid": 999})
    if guard_epoch == 'advanced':
        data = {k: v for k, v in guard.to_dict().items() if k not in {'schema_version', 'evidence_digest'}}
        data.update(generation='d' * 32, database_backend_pid=999, mutation_epoch=guard.mutation_epoch + 1)
        guard = MutationGuardEvidence.build(**data)
    # Metadata versions and postmaster identity legitimately change after the
    # completed handoff. Current observation must still be stable across use.
    credential = replace(credential, binding=replace(credential.binding,
        application_resource_version='105', cnpg_resource_version='106'))
    if drift == 'credential-uid':
        credential = replace(credential, binding=replace(credential.binding, application_uid='aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa'))
    if drift == 'credential-body':
        credential = replace(credential, binding=replace(credential.binding, application_sha256='e' * 64))
    if drift == 'cluster':
        runtime = replace(runtime, cluster_uid='aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa')
    later = [False]
    calls = []
    successor = ApplicationOwnerSuccessor('app_successor', 902)
    def observe_inputs():
        calls.append('inputs')
        return credential, runtime, ('f' if later[0] and drift == 'late-inputs' else 'e') * 64
    def read_guard():
        if later[0] and drift == 'late-guard':
            raise RuntimeError('current guard lost')
        return guard
    def epoch():
        return plan.starting_mutation_epoch + (2 if later[0] and drift == 'late-epoch' else 1)
    def read_successor():
        return replace(successor, role_oid=903) if later[0] and drift == 'successor' else successor
    def sql_observation(connection, **kwargs):
        calls.append('sql')
        assert kwargs['target'] == view.admission.target
        assert kwargs['successor'] == successor
        assert kwargs['runtime_password'] == credential.credential.password
        later[0] = True
        return 'd' * 64
    monkeypatch.setattr(module, 'observe_completed_application_authority', sql_observation)
    arguments = dict(view=view, terminal=terminal, runner=runner,
        guard_source=read_guard, epoch_source=epoch, observe_inputs=observe_inputs,
        successor_source=read_successor, admit_sql_profile=lambda *args: calls.append('sql-profile'),
        observe_retired_fences=lambda: 'c' * 64)
    if guard_epoch == 'new-request':
        arguments['historical_plan'] = original_plan
    if drift:
        with pytest.raises(RuntimeError, match=r'completed application|guard'):
            module.observe_completed_application_handoff(plan, **arguments)
        if drift in {'credential-uid', 'credential-body', 'cluster'}:
            assert 'sql' not in calls
    else:
        observed = module.observe_completed_application_handoff(plan, **arguments)
        if guard_epoch == 'new-request':
            assert observed.state == ComponentState.EXACT and observed.observed_epoch == plan.starting_mutation_epoch + 1
            assert observed.evidence_digest != terminal.evidence_digest
        else:
            assert observed == ComponentObservation(ComponentState.EXACT, terminal.evidence_digest, terminal.observed_epoch)
        assert calls == ['inputs', 'sql-profile', 'sql', 'inputs']
