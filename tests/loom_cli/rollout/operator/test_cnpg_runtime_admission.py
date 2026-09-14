"""Reject CNPG Pod inputs that could replace the admitted executable profile."""

from copy import deepcopy

import pytest

from loom_cli.rollout.operator.protected_cnpg_manager_replacement import CNPG_MANAGER_IMAGE


@pytest.fixture
def pod():
    pg_image = 'ghcr.io/cloudnative-pg/postgresql@sha256:3c0ba08ea353c9705a755c113e4ae395be76553e0ed68076e5410cb09b9d17d9'
    security = {'allowPrivilegeEscalation': False, 'capabilities': {'drop': ['ALL']},
                'privileged': False, 'readOnlyRootFilesystem': True, 'runAsNonRoot': True,
                'seccompProfile': {'type': 'RuntimeDefault'}}
    mounts = [{'name': 'pgdata', 'mountPath': '/var/lib/postgresql/data'},
              {'name': 'scratch-data', 'mountPath': '/run'},
              {'name': 'scratch-data', 'mountPath': '/controller'},
              {'name': 'shm', 'mountPath': '/dev/shm'}]
    container = {'name': 'postgres', 'image': pg_image, 'securityContext': security,
                 'command': ['/controller/manager', 'instance', 'run', '--status-port-tls', '--log-level=info'],
                 'env': [{'name': k, 'value': v} for k, v in {
                     'PGDATA': '/var/lib/postgresql/data/pgdata', 'POD_NAME': 'loom-postgres-1',
                     'NAMESPACE': 'loom-staging', 'CLUSTER_NAME': 'loom-postgres',
                     'PSQL_HISTORY': '/controller/tmp/.psql_history', 'PGPORT': '5432',
                     'PGHOST': '/controller/run', 'TMPDIR': '/controller/tmp'}.items()],
                 'volumeMounts': mounts}
    return {'apiVersion': 'v1', 'kind': 'Pod',
            'metadata': {'name': 'loom-postgres-1', 'namespace': 'loom-staging',
                         'uid': '22222222-2222-4222-8222-222222222222', 'resourceVersion': '123',
                         'ownerReferences': [{'apiVersion': 'postgresql.cnpg.io/v1', 'kind': 'Cluster',
                                              'name': 'loom-postgres', 'uid': '11111111-1111-4111-8111-111111111111',
                                              'controller': True, 'blockOwnerDeletion': True}]},
            'spec': {'nodeName': 'trt-eai-oldlab-4', 'containers': [container],
                     'initContainers': [{'name': 'bootstrap-controller', 'image': CNPG_MANAGER_IMAGE,
                        'command': ['/manager', 'bootstrap', '/controller/manager', '--log-level=info'],
                        'volumeMounts': deepcopy(mounts), 'securityContext': deepcopy(security)}],
                     'securityContext': {'fsGroup': 26, 'runAsGroup': 26, 'runAsUser': 26,
                                         'runAsNonRoot': True, 'seccompProfile': {'type': 'RuntimeDefault'}},
                     'serviceAccountName': 'loom-postgres',
                     'volumes': [{'name': 'pgdata', 'persistentVolumeClaim': {'claimName': 'loom-postgres-1'}},
                                 {'name': 'scratch-data', 'emptyDir': {}},
                                 {'name': 'shm', 'emptyDir': {'medium': 'Memory'}}]},
            'status': {'phase': 'Running', 'containerStatuses': [{'name': 'postgres', 'imageID': pg_image,
                       'containerID': 'containerd://' + 'a' * 64, 'restartCount': 0,
                       'state': {'running': {'startedAt': '2026-09-11T00:00:00Z'}}}],
                       'initContainerStatuses': [{'name': 'bootstrap-controller', 'imageID': CNPG_MANAGER_IMAGE,
                                                 'state': {'terminated': {'exitCode': 0}}}]}}


def test_supported_primary_pod(pod):
    from loom_cli.rollout.operator.protected_cnpg_runtime_admission import admit_cnpg_primary_pod
    identity, digest = admit_cnpg_primary_pod(pod, cluster_uid='11111111-1111-4111-8111-111111111111')
    assert identity.pod_name == 'loom-postgres-1' and identity.node_name == 'trt-eai-oldlab-4'
    assert len(digest) == 64


@pytest.mark.parametrize('change', ['env', 'init-env', 'host', 'ephemeral', 'sidecar', 'image',
                                   'init-image', 'mount', 'volume', 'lifecycle', 'security', 'owner',
                                   'node', 'args', 'duplicate-env', 'device'])
def test_primary_rejects_executable_or_volume_injection(pod, change):
    from loom_cli.rollout.operator.protected_cnpg_runtime_admission import admit_cnpg_primary_pod
    spec = pod['spec']
    container = spec['containers'][0]
    if change == 'env':
        container['env'].append({'name': 'LD_PRELOAD', 'value': '/controller/hook.so'})
    elif change == 'init-env':
        spec['initContainers'][0]['env'] = [{'name': 'LD_PRELOAD', 'value': '/controller/hook.so'}]
    elif change == 'host':
        spec['hostPID'] = True
    elif change == 'ephemeral':
        spec['ephemeralContainers'] = [{'name': 'foreign'}]
    elif change == 'sidecar':
        spec['containers'].append(deepcopy(container))
    elif change == 'image':
        pod['status']['containerStatuses'][0]['imageID'] = 'foreign:latest'
    elif change == 'init-image':
        pod['status']['initContainerStatuses'][0]['imageID'] = 'foreign:latest'
    elif change == 'mount':
        container['volumeMounts'].append({'name': 'scratch-data', 'mountPath': '/usr/lib'})
    elif change == 'volume':
        spec['volumes'][1] = {'name': 'scratch-data', 'hostPath': {'path': '/tmp'}}
    elif change == 'lifecycle':
        container['lifecycle'] = {'postStart': {'exec': {'command': ['sh', '-c', 'false']}}}
    elif change == 'security':
        container['securityContext']['readOnlyRootFilesystem'] = False
    elif change == 'owner':
        pod['metadata']['ownerReferences'][0]['uid'] = '33333333-3333-4333-8333-333333333333'
    elif change == 'node':
        spec['nodeName'] = 'trt-eai-oldlab-2'
    elif change == 'args':
        container['args'] = ['--foreign']
    elif change == 'duplicate-env':
        container['env'].append(deepcopy(container['env'][0]))
    elif change == 'device':
        container['volumeDevices'] = [{'name': 'pgdata', 'devicePath': '/dev/foreign'}]
    with pytest.raises((ValueError, RuntimeError), match='CNPG'):
        admit_cnpg_primary_pod(pod, cluster_uid='11111111-1111-4111-8111-111111111111')


def _runtime():
    from loom_cli.rollout.operator.protected_cnpg_runtime_admission import CNPGPrimaryRuntime
    from tests.loom_cli.rollout.operator.test_cnpg_manager_replacement import _manager

    return CNPGPrimaryRuntime(_manager(), 'b' * 64, 39, 23456, 32, 101,
                              '11111111-1111-4111-8111-111111111111')


def test_runtime_binding_is_immutable_and_recovery_view_is_readonly(tmp_path, monkeypatch):
    from dataclasses import replace

    from tests.loom_cli.rollout.operator.test_application_admission_recovery import _component
    from tests.loom_cli.rollout.operator.test_final_gate_plan import _plan
    from tests.loom_cli.rollout.operator.test_protected_apply_journal import _journal

    plan, journal = _plan(tmp_path), _journal(tmp_path)
    runtime = _runtime()
    def apply(_):
        journal.record_application_cnpg_runtime(plan, runtime=runtime)
        assert journal.read_application_cnpg_runtime(plan) == runtime
        journal.record_application_cnpg_runtime(plan, runtime=runtime)
        with pytest.raises(RuntimeError, match='cannot be replaced'):
            journal.record_application_cnpg_runtime(plan, runtime=replace(runtime, postgres_started_ticks=34567))
        raise RuntimeError('runtime saved before admission')
    component = _component(apply)
    with pytest.raises(RuntimeError, match='runtime saved'):
        journal.execute(plan, [component])
    def forbid(*args, **kwargs):
        pytest.fail('classification must remain read-only')
    monkeypatch.setattr(journal, '_sync_application_recovery', forbid)
    monkeypatch.setattr(journal, '_publish_or_match', forbid)
    view = journal.read_application_recovery_view(plan, component, ordinal=0)
    assert view.admission is None and view.cnpg_runtime == runtime


@pytest.mark.parametrize('change', ['postgres_started_ticks', 'postgres_inode', 'postgres_pid', 'pod_spec_sha256', 'cluster_uid'])
def test_manager_reconciliation_preserves_original_postmaster_and_inputs(tmp_path, monkeypatch, change):
    from dataclasses import replace

    import loom_cli.rollout.operator.protected_cnpg_runtime_admission as admission
    from tests.loom_cli.rollout.operator.test_application_admission_recovery import _component
    from tests.loom_cli.rollout.operator.test_cnpg_manager_replacement import _admit
    from tests.loom_cli.rollout.operator.test_final_gate_plan import _plan
    from tests.loom_cli.rollout.operator.test_protected_apply_journal import _journal

    plan, journal = _plan(tmp_path), _journal(tmp_path)
    original = _runtime()
    changed = replace(original, manager=replace(original.manager, executable_inode=999))
    value = {'pod_spec_sha256': 'c' * 64, 'cluster_uid': '33333333-3333-4333-8333-333333333333'}.get(change, 98765)
    observed = [replace(changed, **{change: value})]
    monkeypatch.setattr(admission, 'observe_cnpg_primary_runtime', lambda *args, **kwargs: observed[0])
    def apply(_):
        journal.record_application_cnpg_runtime(plan, runtime=original)
        _admit(journal)
        journal.prepare_application_manager_replacement(identity=original.manager)
        journal.begin_application_manager_replacement()
        with pytest.raises(RuntimeError, match='CNPG original'):
            admission.reconcile_cnpg_primary_runtime(plan, journal=journal, runner=object())
        assert journal.read_application_manager_replacement()[2] is None
        observed[0] = changed
        assert admission.reconcile_cnpg_primary_runtime(plan, journal=journal, runner=object()) == changed
        assert journal.read_application_manager_replacement()[2].identity == changed.manager
        assert journal.read_application_cnpg_runtime(plan) == original
        raise RuntimeError('original identity preserved')
    with pytest.raises(RuntimeError, match='identity preserved'):
        journal.execute(plan, [_component(apply)])
