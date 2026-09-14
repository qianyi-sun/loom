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
    environment = {'KUBECONFIG': '/fixture'}
    def __init__(self):
        self.pod = _pod()
        self.override = b''
        self.change = None
        self.inspections = []
    def capture_stdout(self, argv, **kwargs):
        if 'pods' in argv:
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
