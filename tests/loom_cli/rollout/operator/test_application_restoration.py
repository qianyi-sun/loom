"""Restoration evidence requires all original bindings and fresh live observations."""

import os
from contextlib import nullcontext
from dataclasses import replace

import pytest

from loom.application_runtime_login import ApplicationRuntimeLoginState
from loom_cli.rollout.operator.protected_application_admission_recovery import (
    ApplicationAdmissionRecoveryRecord,
    admission_record_digest,
)
from loom_cli.rollout.operator.protected_application_credential_recovery import (
    observe_application_runtime_credential,
)
from loom_cli.rollout.operator.protected_application_owner_preparation import (
    APPLICATION_OWNER_ROLE,
    ApplicationOwnerCreationIntent,
)
from loom_cli.rollout.operator.protected_application_workloads import (
    ApplicationWorkload,
    _digest,
    validate_workload_inventory,
)
from loom_cli.rollout.operator.protected_apply_journal import (
    ApplicationRecoveryView,
    ComponentIntent,
)
from loom_cli.rollout.operator.protected_cnpg_manager_replacement import (
    CNPGManagerReplacementIntent,
    CNPGManagerReplacementReceipt,
)
from tests.loom_cli.rollout.operator.test_application_admission_recovery import (
    _component,
    _handoff,
    _target,
)
from tests.loom_cli.rollout.operator.test_application_credential_recovery import _Runner, _sources
from tests.loom_cli.rollout.operator.test_application_workload_runtime import _context
from tests.loom_cli.rollout.operator.test_cnpg_runtime_admission import _runtime


def _inputs(tmp_path):
    _, _, guard, saved_guard, workload_runner, _ = _context(tmp_path)
    plan, live = _sources(tmp_path)
    credentials = _Runner(live)
    observed = observe_application_runtime_credential(plan, runner=credentials, service_uid=os.getuid())
    intent = ComponentIntent.build(plan, _component(lambda _: None), 4)
    target = replace(_target(), successor_role=APPLICATION_OWNER_ROLE)
    admission = ApplicationAdmissionRecoveryRecord(intent.intent_digest, target, _handoff(), saved_guard)
    runtime = replace(_runtime(), cluster_uid=observed.configuration.cluster_uid)
    replacement = CNPGManagerReplacementIntent(intent.intent_digest, admission_record_digest(admission.to_dict()), runtime.manager)
    receipt = CNPGManagerReplacementReceipt.validate(replacement, replace(runtime.manager, executable_inode=101))
    owner = ApplicationOwnerCreationIntent(1, intent.intent_digest, admission.handoff_backend, saved_guard)
    view = ApplicationRecoveryView(intent, admission, (), (replacement, True, receipt),
        tuple(ApplicationWorkload.capture(obj) for obj in workload_runner.objects), True,
        ((owner, target.successor_oid),), runtime, observed.binding, observed.configuration)
    class Runner:
        environment = credentials.environment
        def capture_stdout(self, *args, **kwargs):
            return credentials.capture_stdout(*args, **kwargs)
        def open_staging_peer_database(self):
            return nullcontext(object())
    return plan, view, guard, Runner(), observed, replace(runtime, manager=receipt.identity)


@pytest.mark.parametrize('missing', [None, 'admission', 'cnpg_runtime', 'credential_binding', 'cnpg_configuration',
                                     'manager_replacement', 'workloads', 'workloads_restoring', 'owner_creations'])
def test_restoration_observer_never_infers_completion_from_partial_records(tmp_path, monkeypatch, missing):
    from loom_cli.rollout.operator import protected_application_restoration as module

    plan, view, guard, runner, credential, runtime = _inputs(tmp_path)
    calls = []
    def read(name, value):
        def result(*args, **kwargs):
            calls.append(name)
            return value
        return result
    monkeypatch.setattr(module, 'observe_cnpg_primary_runtime', read('runtime', runtime))
    monkeypatch.setattr(module, 'observe_application_runtime_credential', read('credential', credential))
    monkeypatch.setattr(module, 'observe_recovered_application_workloads', read('workload', _digest([item.to_dict() for item in validate_workload_inventory(view.workloads)])))
    monkeypatch.setattr(module, 'observe_application_runtime_login', read('sql', ApplicationRuntimeLoginState.RESTORED))
    if missing:
        view = replace(view, **{missing: () if missing in {'workloads', 'owner_creations'} else False if missing == 'workloads_restoring' else None})
        with pytest.raises(RuntimeError):
            module.observe_application_restoration(plan, view=view, runner=runner, guard=guard, service_uid=os.getuid())
        assert calls == []
    else:
        monkeypatch.setattr(os, 'fsync', lambda *_: pytest.fail('read-only restoration published a record'))
        evidence = module.observe_application_restoration(plan, view=view, runner=runner, guard=guard, service_uid=os.getuid())
        assert evidence.intent_digest == view.intent.intent_digest
        assert evidence == module.ApplicationRestorationEvidence.from_dict(evidence.to_dict())
        assert credential.credential.password not in repr(evidence.to_dict())
        assert calls.count('sql') == 2 and calls.count('runtime') == 2 and calls.count('workload') == 1


@pytest.mark.parametrize('drift', ['postmaster', 'manager', 'credential', 'configuration', 'sealed', 'workload', 'late-runtime', 'late-sql'])
def test_restoration_observer_refuses_live_drift_including_after_workload_check(tmp_path, monkeypatch, drift):
    from loom_cli.rollout.operator import protected_application_restoration as module

    plan, view, guard, runner, credential, runtime = _inputs(tmp_path)
    seen = []
    def runtime_read(*args, **kwargs):
        seen.append('runtime')
        if drift == 'postmaster' or (drift == 'late-runtime' and seen.count('runtime') > 1):
            return replace(runtime, postgres_started_ticks=runtime.postgres_started_ticks + 1)
        if drift == 'manager':
            return view.cnpg_runtime
        return runtime
    def credential_read(*args, **kwargs):
        if drift == 'credential':
            return replace(credential, binding=replace(credential.binding, application_resource_version='999'))
        if drift == 'configuration':
            return replace(credential, configuration=replace(credential.configuration, cluster_generation=999))
        return credential
    def sql_read(*args, **kwargs):
        seen.append('sql')
        return (ApplicationRuntimeLoginState.SEALED if drift == 'sealed' or (drift == 'late-sql' and seen.count('sql') > 1)
                else ApplicationRuntimeLoginState.RESTORED)
    def workloads(*args, **kwargs):
        if drift == 'workload':
            raise RuntimeError('original workload is not ready')
        return _digest([item.to_dict() for item in validate_workload_inventory(view.workloads)])
    monkeypatch.setattr(module, 'observe_cnpg_primary_runtime', runtime_read)
    monkeypatch.setattr(module, 'observe_application_runtime_credential', credential_read)
    monkeypatch.setattr(module, 'observe_application_runtime_login', sql_read)
    monkeypatch.setattr(module, 'observe_recovered_application_workloads', workloads)
    with pytest.raises(RuntimeError):
        module.observe_application_restoration(plan, view=view, runner=runner, guard=guard, service_uid=os.getuid())
