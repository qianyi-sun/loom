"""Bind the actual PostgreSQL storage path and reject a second Kubernetes consumer."""

import copy
import json
from dataclasses import asdict, replace
from uuid import uuid4

import pytest

from tests.loom_cli.rollout.operator.test_cnpg_runtime_admission import (
    _runtime,
    pod,  # noqa: F401
)


def _fixture(primary):
    from loom_cli.rollout.operator.protected_cnpg_runtime_admission import admit_cnpg_primary_pod

    runtime = _runtime()
    identity, digest = admit_cnpg_primary_pod(primary, cluster_uid=runtime.cluster_uid)
    runtime = replace(runtime, manager=replace(runtime.manager, **asdict(identity)), pod_spec_sha256=digest)
    claim_uid = str(uuid4())
    claim = {'apiVersion': 'v1', 'kind': 'PersistentVolumeClaim',
        'metadata': {'name': identity.pod_name, 'namespace': 'loom-staging', 'uid': claim_uid,
            'ownerReferences': [{'apiVersion': 'postgresql.cnpg.io/v1', 'kind': 'Cluster', 'name': 'loom-postgres',
                'uid': runtime.cluster_uid, 'controller': True}]},
        'spec': {'accessModes': ['ReadWriteOnce'], 'storageClassName': 'longhorn', 'volumeMode': 'Filesystem',
            'volumeName': 'pvc-' + claim_uid, 'resources': {'requests': {'storage': '50Gi'}}},
        'status': {'phase': 'Bound'}}
    pv = {'apiVersion': 'v1', 'kind': 'PersistentVolume',
        'metadata': {'name': claim['spec']['volumeName'], 'uid': str(uuid4())},
        'spec': {'accessModes': ['ReadWriteOnce'], 'volumeMode': 'Filesystem', 'storageClassName': 'longhorn',
            'capacity': {'storage': '50Gi'}, 'persistentVolumeReclaimPolicy': 'Delete',
            'claimRef': {'apiVersion': 'v1', 'kind': 'PersistentVolumeClaim', 'name': identity.pod_name,
                'namespace': 'loom-staging', 'uid': claim_uid},
            'csi': {'driver': 'driver.longhorn.io', 'fsType': 'ext4', 'volumeHandle': claim['spec']['volumeName'],
                'volumeAttributes': {'dataEngine': 'v1', 'numberOfReplicas': '3'}}},
        'status': {'phase': 'Bound'}}
    attachment = {'apiVersion': 'storage.k8s.io/v1', 'kind': 'VolumeAttachment',
        'metadata': {'name': 'csi-' + 'a' * 64, 'uid': str(uuid4())},
        'spec': {'attacher': 'driver.longhorn.io', 'nodeName': identity.node_name,
            'source': {'persistentVolumeName': pv['metadata']['name']}}, 'status': {'attached': True}}
    class Runner:
        def __init__(self):
            self.environment = {}
            self.claim = claim
            self.pvs = [pv]
            self.attachments = [attachment]
            self.pods = [primary]
            self.change = None
            self.observations = 0
        def capture_stdout(self, argv, **kwargs):
            assert 'get' in argv
            path = next(item.removeprefix('--raw=') for item in argv if item.startswith('--raw='))
            if '/persistentvolumeclaims/' in path:
                self.observations += 1
                if self.change and self.observations == 2:
                    self.change()
                return json.dumps(self.claim).encode()
            values, kind, version = {
                '/api/v1/persistentvolumes': (self.pvs, 'PersistentVolume', 'v1'),
                '/apis/storage.k8s.io/v1/volumeattachments': (self.attachments, 'VolumeAttachment', 'storage.k8s.io/v1'),
                '/api/v1/pods': (self.pods, 'Pod', 'v1'),
            }[path]
            return json.dumps({'apiVersion': version, 'kind': kind + 'List', 'metadata': {'resourceVersion': '100'},
                               'items': values}).encode()
    return runtime, Runner()


@pytest.mark.parametrize('drift', [None, 'claim-owner', 'claim-uid', 'driver', 'mode', 'node', 'detached',
    'second-attachment', 'pv-alias', 'second-pod', 'inline-consumer', 'pod-replaced', 'during-observation'])
def test_volume_admission_rejects_aliases_second_consumers_and_changed_bindings(pod, drift):  # noqa: F811
    from loom_cli.rollout.operator.protected_cnpg_volume_admission import observe_cnpg_volume

    runtime, runner = _fixture(pod)
    if drift == 'claim-owner':
        runner.claim['metadata']['ownerReferences'][0]['uid'] = str(uuid4())
    elif drift == 'claim-uid':
        runner.claim['metadata']['uid'] = str(uuid4())
    elif drift == 'driver':
        runner.pvs[0]['spec']['csi']['driver'] = 'foreign.storage'
    elif drift == 'mode':
        runner.pvs[0]['spec']['volumeMode'] = 'Block'
    elif drift == 'node':
        runner.attachments[0]['spec']['nodeName'] = 'trt-eai-oldlab-3'
    elif drift == 'detached':
        runner.attachments[0]['status']['attached'] = False
    elif drift == 'second-attachment':
        runner.attachments.append(copy.deepcopy(runner.attachments[0]))
    elif drift == 'pv-alias':
        alias = copy.deepcopy(runner.pvs[0])
        alias['metadata'].update({'name': 'alias', 'uid': str(uuid4())})
        runner.pvs.append(alias)
    elif drift in {'second-pod', 'inline-consumer'}:
        consumer = copy.deepcopy(pod)
        consumer['metadata'].update({'name': 'foreign-consumer', 'uid': str(uuid4())})
        if drift == 'inline-consumer':
            consumer['spec']['volumes'] = [{'name': 'aliased', 'csi': {'driver': 'driver.longhorn.io',
                'volumeAttributes': {'volumeHandle': runner.pvs[0]['spec']['csi']['volumeHandle']}}}]
        runner.pods.append(consumer)
    elif drift == 'pod-replaced':
        runner.pods[0]['metadata']['uid'] = str(uuid4())
    elif drift == 'during-observation':
        runner.change = lambda: runner.attachments[0]['metadata'].update({'uid': str(uuid4())})
    if drift:
        with pytest.raises(ValueError, match='CNPG volume'):
            observe_cnpg_volume(runner, runtime=runtime)
    else:
        observed = observe_cnpg_volume(runner, runtime=runtime)
        assert observed.claim_uid == runner.claim['metadata']['uid']
        assert observed.volume_uid == runner.pvs[0]['metadata']['uid']
        assert observed.attachment_uid == runner.attachments[0]['metadata']['uid']
        assert len(observed.digest) == 64
        assert observe_cnpg_volume(runner, runtime=runtime) == observed
