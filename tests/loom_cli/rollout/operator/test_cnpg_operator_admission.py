"""Admit actual operator processes only with the fixed Pod and filesystem profile."""

import copy
import json
from uuid import uuid4

import pytest

from loom_cli.rollout.operator.protected_cnpg_manager_replacement import CNPG_MANAGER_SHA256


def _pod():
    return {
        'apiVersion': 'v1', 'kind': 'Pod',
        'metadata': {'name': 'cnpg-controller-manager-abcde-12345', 'namespace': 'cnpg-system',
                     'uid': str(uuid4()), 'resourceVersion': '100'},
        'spec': {
            'nodeName': 'trt-eai-oldlab-4', 'serviceAccountName': 'cnpg-manager',
            'containers': [{
                'name': 'manager', 'image': 'ghcr.io/cloudnative-pg/cloudnative-pg:1.25.1',
                'command': ['/manager'],
                'args': ['controller', '--leader-elect', '--max-concurrent-reconciles=10',
                         '--config-map-name=cnpg-controller-manager-config',
                         '--secret-name=cnpg-controller-manager-config', '--webhook-port=9443'],
                'env': [
                    {'name': 'OPERATOR_IMAGE_NAME', 'value': 'ghcr.io/cloudnative-pg/cloudnative-pg:1.25.1'},
                    {'name': 'OPERATOR_NAMESPACE', 'valueFrom': {'fieldRef': {'apiVersion': 'v1', 'fieldPath': 'metadata.namespace'}}},
                    {'name': 'MONITORING_QUERIES_CONFIGMAP', 'value': 'cnpg-default-monitoring'},
                ],
                'securityContext': {'allowPrivilegeEscalation': False, 'capabilities': {'drop': ['ALL']},
                    'readOnlyRootFilesystem': True, 'runAsGroup': 10001, 'runAsUser': 10001,
                    'seccompProfile': {'type': 'RuntimeDefault'}},
                'volumeMounts': [{'name': 'scratch-data', 'mountPath': '/controller'},
                    {'name': 'webhook-certificates', 'mountPath': '/run/secrets/cnpg.io/webhook'},
                    {'name': 'kube-api-access-abcde', 'mountPath': '/var/run/secrets/kubernetes.io/serviceaccount', 'readOnly': True}],
            }],
            'securityContext': {'runAsNonRoot': True, 'seccompProfile': {'type': 'RuntimeDefault'}},
            'volumes': [{'name': 'scratch-data', 'emptyDir': {}},
                {'name': 'webhook-certificates', 'secret': {'secretName': 'cnpg-webhook-cert', 'defaultMode': 420, 'optional': True}},
                {'name': 'kube-api-access-abcde', 'projected': {'defaultMode': 420, 'sources': [
                    {'serviceAccountToken': {'expirationSeconds': 3607, 'path': 'token'}},
                    {'configMap': {'name': 'kube-root-ca.crt', 'items': [{'key': 'ca.crt', 'path': 'ca.crt'}]}},
                    {'downwardAPI': {'items': [{'path': 'namespace', 'fieldRef': {'apiVersion': 'v1', 'fieldPath': 'metadata.namespace'}}]}},
                ]}},
            ],
        },
        'status': {'phase': 'Running', 'containerStatuses': [{
            'name': 'manager', 'ready': True, 'restartCount': 0,
            'containerID': 'containerd://' + 'a' * 64,
            'imageID': 'ghcr.io/cloudnative-pg/cloudnative-pg@sha256:b5210df46c05bed3c5dbb67d316dece0ed67f4d148acac169416079dc10e4a91',
            'state': {'running': {'startedAt': '2026-09-10T00:00:00Z'}},
        }]},
    }


class Runner:
    def __init__(self):
        self.environment = {'KUBECONFIG': '/fixture'}
        self.pod = _pod()
        self.override = b''
        self.change = None
        self.inspections = []
    def capture_stdout(self, argv, **kwargs):
        if '--raw=/api/v1/namespaces/cnpg-system/pods' in argv:
            return json.dumps({'apiVersion': 'v1', 'kind': 'PodList', 'metadata': {'resourceVersion': '200'}, 'items': [self.pod]}).encode()
        assert 'cnpg-controller-manager-config' in argv
        return self.override
    def inspect_staging_cnpg_operator(self, identity):
        self.inspections.append(identity)
        result = {'schema_version': 1, 'pod_uid': identity.pod_uid, 'container_id': identity.container_id,
            'node_name': identity.node_name, 'pid': 1000, 'started_ticks': 90000,
            'mount_namespace': 'mnt:[4026001000]', 'pid_namespace': 'pid:[4026002000]',
            'executable_device': 2049, 'executable_inode': 2000,
            'executable_sha256': CNPG_MANAGER_SHA256, 'stored_sha256': CNPG_MANAGER_SHA256,
            'root_readonly': True, 'namespace_pid': 1}
        if self.change:
            self.change(result)
        return result


@pytest.mark.parametrize('drift', [None, 'digest', 'stored', 'pod', 'container', 'node', 'root', 'namespace',
                                   'argv', 'env', 'envFrom', 'mount', 'volume', 'sidecar', 'hostPID', 'override', 'replaced'])
def test_operator_admission_binds_process_bytes_and_refuses_changed_inputs(drift):
    from loom_cli.rollout.operator.protected_cnpg_operator_admission import observe_cnpg_operator

    runner = Runner()
    pod = runner.pod
    if drift in {'digest', 'stored', 'pod', 'container', 'node', 'root', 'namespace'}:
        key, value = {'digest': ('executable_sha256', '0' * 64), 'stored': ('stored_sha256', '0' * 64),
            'pod': ('pod_uid', str(uuid4())), 'container': ('container_id', 'b' * 64),
            'node': ('node_name', 'trt-eai-oldlab-2'), 'root': ('root_readonly', False),
            'namespace': ('namespace_pid', 2)}[drift]
        runner.change = lambda result: result.update({key: value})
    elif drift == 'argv':
        pod['spec']['containers'][0]['args'].append('--log-level=debug')
    elif drift == 'env':
        pod['spec']['containers'][0]['env'].append({'name': 'INHERITED_CONFIG', 'value': 'unknown'})
    elif drift == 'envFrom':
        pod['spec']['containers'][0]['envFrom'] = [{'secretRef': {'name': 'override'}}]
    elif drift == 'mount':
        pod['spec']['containers'][0]['volumeMounts'][0]['mountPath'] = '/operator'
    elif drift == 'volume':
        pod['spec']['volumes'][0] = {'name': 'scratch-data', 'hostPath': {'path': '/tmp'}}
    elif drift == 'sidecar':
        pod['spec']['ephemeralContainers'] = [{'name': 'debugger'}]
    elif drift == 'hostPID':
        pod['spec']['hostPID'] = True
    elif drift == 'override':
        runner.override = b'{}'
    elif drift == 'replaced':
        runner.change = lambda _: pod['metadata'].update({'uid': str(uuid4())})
    if drift:
        with pytest.raises(ValueError, match='CNPG operator'):
            observe_cnpg_operator(runner)
    else:
        first = observe_cnpg_operator(runner)
        assert first.executable_sha256 == CNPG_MANAGER_SHA256
        assert first.pod_uid == pod['metadata']['uid']
        assert len(first.digest) == 64
        assert observe_cnpg_operator(runner) == first
        assert len(runner.inspections) == 2


def test_operator_inventory_ignores_only_verified_terminal_container_records():
    from loom_cli.rollout.operator.protected_cnpg_operator_admission import select_cnpg_operator

    pod = _pod()
    terminal = copy.deepcopy(pod)
    terminal['metadata']['uid'] = str(uuid4())
    terminal['status']['phase'] = 'Failed'
    with pytest.raises(ValueError, match='CNPG operator'):
        select_cnpg_operator([pod, terminal])
    terminal['status']['containerStatuses'][0]['state'] = {'terminated': {'exitCode': 137}}
    assert select_cnpg_operator([pod, terminal]).pod_uid == pod['metadata']['uid']


@pytest.mark.parametrize('change', ['node', 'container', 'pod', 'uid', 'extra'])
def test_host_observer_refuses_unbounded_input_before_any_runtime_command(monkeypatch, change):
    from loom_cli.rollout.operator import protected_cnpg_operator_host as host

    request = {'node_name': 'trt-eai-oldlab-4', 'pod_name': _pod()['metadata']['name'],
               'pod_uid': str(uuid4()), 'container_id': 'a' * 64}
    key, value = {'node': ('node_name', 'trt-eai-oldlab-2'), 'container': ('container_id', 'x;command'),
        'pod': ('pod_name', '../foreign'), 'uid': ('pod_uid', 'missing'),
        'extra': ('command', 'untrusted')}[change]
    request[key] = value
    monkeypatch.setattr(host.subprocess, 'run', lambda *args, **kwargs: pytest.fail('refused request reached runtime'))
    with pytest.raises(RuntimeError, match='CNPG operator'):
        host.inspect_cnpg_operator_host(request)


@pytest.mark.parametrize('change', [None, 'bytes', 'size', 'during-read'])
def test_host_binary_observation_reads_actual_bytes_and_refuses_inflight_change(tmp_path, monkeypatch, change):
    import hashlib

    from loom_cli.rollout.operator import protected_cnpg_operator_host as host

    expected = b'admitted executable bytes'
    executable = tmp_path / 'executable'
    executable.write_bytes(expected)
    monkeypatch.setattr(host, '_BINARY_SHA256', hashlib.sha256(expected).hexdigest())
    monkeypatch.setattr(host, '_BINARY_SIZE', len(expected))
    if change == 'bytes':
        executable.write_bytes(b'x' * len(expected))
    elif change == 'size':
        executable.write_bytes(expected + b'x')
    elif change == 'during-read':
        digest = host.hashlib.file_digest
        def changed(stream, algorithm):
            result = digest(stream, algorithm)
            executable.write_bytes(b'x' * len(expected))
            return result
        monkeypatch.setattr(host.hashlib, 'file_digest', changed)
    if change:
        with pytest.raises(RuntimeError, match='CNPG operator'):
            host._binary(executable)
    else:
        device, inode, digest = host._binary(executable)
        assert (device, inode) == (executable.stat().st_dev, executable.stat().st_ino)
        assert digest == hashlib.sha256(expected).hexdigest()


def test_host_process_admission_refuses_a_second_process_in_operator_namespace(tmp_path, monkeypatch):
    from loom_cli.rollout.operator import protected_cnpg_operator_host as host

    monkeypatch.setattr(host, '_PROC', tmp_path)
    for pid, namespace in ((1000, 'pid:[100]'), (2000, 'pid:[200]')):
        path = tmp_path / str(pid) / 'ns'
        path.mkdir(parents=True)
        (path / 'pid').symlink_to(namespace)
    host._only_operator_in_namespace(1000, 'pid:[100]')
    path = tmp_path / '2000/ns/pid'
    path.unlink()
    path.symlink_to('pid:[100]')
    with pytest.raises(RuntimeError, match='CNPG operator'):
        host._only_operator_in_namespace(1000, 'pid:[100]')
