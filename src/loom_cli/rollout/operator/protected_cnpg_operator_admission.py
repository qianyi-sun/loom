"""Admit the fixed CNPG operator using Kubernetes and actual host observations.

The process transport must inspect the original container's host procfs, not
return image metadata as executable evidence. Administrator/host/storage writer
exclusion remains an enclosing maintenance-window prerequisite. This read-only
observer neither creates an inspection workload nor grants a new host capability.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass
from typing import Protocol

from .protected_cnpg_manager_replacement import CNPG_MANAGER_SHA256
from .protected_cnpg_writer_configuration import _json, _mapping

_IMAGE = 'ghcr.io/cloudnative-pg/cloudnative-pg@sha256:b5210df46c05bed3c5dbb67d316dece0ed67f4d148acac169416079dc10e4a91'
_COMMAND = ['/manager', 'controller', '--leader-elect', '--max-concurrent-reconciles=10',
            '--config-map-name=cnpg-controller-manager-config', '--secret-name=cnpg-controller-manager-config',
            '--webhook-port=9443']
_ENV = [
    {'name': 'OPERATOR_IMAGE_NAME', 'value': 'ghcr.io/cloudnative-pg/cloudnative-pg:1.25.1'},
    {'name': 'OPERATOR_NAMESPACE', 'valueFrom': {'fieldRef': {'apiVersion': 'v1', 'fieldPath': 'metadata.namespace'}}},
    {'name': 'MONITORING_QUERIES_CONFIGMAP', 'value': 'cnpg-default-monitoring'},
]
_SECURITY = {'allowPrivilegeEscalation': False, 'capabilities': {'drop': ['ALL']},
             'readOnlyRootFilesystem': True, 'runAsGroup': 10001, 'runAsUser': 10001,
             'seccompProfile': {'type': 'RuntimeDefault'}}
_POD_FIELDS = frozenset({'containers', 'volumes', 'nodeName', 'serviceAccount', 'serviceAccountName',
    'securityContext', 'dnsPolicy', 'enableServiceLinks', 'preemptionPolicy', 'priority',
    'restartPolicy', 'schedulerName', 'terminationGracePeriodSeconds', 'tolerations',
    'nodeSelector', 'affinity', 'imagePullSecrets', 'automountServiceAccountToken'})
_CONTAINER_FIELDS = frozenset({'name', 'image', 'imagePullPolicy', 'command', 'args', 'env',
    'resources', 'securityContext', 'volumeMounts', 'ports', 'livenessProbe', 'readinessProbe',
    'startupProbe', 'terminationMessagePath', 'terminationMessagePolicy'})
_UID = re.compile(r'[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}\Z')
_SHA = re.compile(r'[0-9a-f]{64}\Z')


def _digest(value: object) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(',', ':')).encode()).hexdigest()


def _refuse() -> ValueError:
    return ValueError('CNPG operator process or input profile changed')


def _objects(value: object) -> list[dict[str, object]]:
    if not isinstance(value, list):
        raise _refuse()
    return [_mapping(item) for item in value]


@dataclass(frozen=True, slots=True)
class CNPGOperatorIdentity:
    pod_name: str
    pod_uid: str
    container_id: str
    node_name: str
    restart_count: int
    pod_spec_sha256: str

    def __post_init__(self) -> None:
        if (re.fullmatch(r'cnpg-controller-manager-[a-z0-9]{5,16}-[a-z0-9]{5}', self.pod_name) is None
                or _UID.fullmatch(self.pod_uid) is None or _SHA.fullmatch(self.container_id) is None
                or self.node_name not in {'trt-eai-oldlab-3', 'trt-eai-oldlab-4', 'trt-eai-oldlab-5'}
                or type(self.restart_count) is not int or not 0 <= self.restart_count < 2**31
                or _SHA.fullmatch(self.pod_spec_sha256) is None):
            raise _refuse()


@dataclass(frozen=True, slots=True)
class CNPGOperatorRuntime:
    identity: CNPGOperatorIdentity
    pid: int
    started_ticks: int
    mount_namespace: str
    pid_namespace: str
    executable_device: int
    executable_inode: int

    @property
    def pod_uid(self) -> str:
        return self.identity.pod_uid

    @property
    def executable_sha256(self) -> str:
        return CNPG_MANAGER_SHA256

    @property
    def digest(self) -> str:
        return _digest({'schema_version': 1, **asdict(self), 'image': _IMAGE,
                        'executable_sha256': CNPG_MANAGER_SHA256})

    @classmethod
    def from_host_observation(cls, identity: CNPGOperatorIdentity, value: Mapping[str, object]) -> CNPGOperatorRuntime:
        numbers = ('pid', 'started_ticks', 'executable_device', 'executable_inode')
        if (set(value) != {'schema_version', 'pod_uid', 'container_id', 'node_name', *numbers,
                'mount_namespace', 'pid_namespace', 'executable_sha256', 'stored_sha256', 'root_readonly', 'namespace_pid'}
                or type(value['schema_version']) is not int or value['schema_version'] != 1
                or value['pod_uid'] != identity.pod_uid or value['container_id'] != identity.container_id
                or value['node_name'] != identity.node_name
                or any(type(value[k]) is not int or not 0 < int(str(value[k])) < 2**64 for k in numbers)
                or not 1 < int(str(value['pid'])) < 2**31
                or value['executable_sha256'] != CNPG_MANAGER_SHA256 or value['stored_sha256'] != CNPG_MANAGER_SHA256
                or value['root_readonly'] is not True or type(value['namespace_pid']) is not int or value['namespace_pid'] != 1):
            raise _refuse()
        for field, prefix in (('mount_namespace', 'mnt'), ('pid_namespace', 'pid')):
            if not isinstance(value[field], str) or re.fullmatch(prefix + r':\[[1-9][0-9]{0,19}\]', str(value[field])) is None:
                raise _refuse()
        return cls(identity, int(str(value['pid'])), int(str(value['started_ticks'])),
                   str(value['mount_namespace']), str(value['pid_namespace']),
                   int(str(value['executable_device'])), int(str(value['executable_inode'])))


class CNPGOperatorRunner(Protocol):
    @property
    def environment(self) -> Mapping[str, str]: ...

    def capture_stdout(self, argv: Sequence[str], *, env: Mapping[str, str], timeout_seconds: float) -> bytes: ...

    def inspect_staging_cnpg_operator(self, identity: CNPGOperatorIdentity) -> Mapping[str, object]: ...


def _volumes(spec: Mapping[str, object], container: Mapping[str, object]) -> None:
    volumes = _objects(spec.get('volumes'))
    mounts = _objects(container.get('volumeMounts'))
    tokens = [v for v in volumes if isinstance(v.get('name'), str)
              and re.fullmatch('kube-api-access-[a-z0-9]{5}', str(v['name']))]
    if len(tokens) != 1 or len(volumes) != 3 or len(mounts) != 3:
        raise _refuse()
    token = tokens[0]
    projection = _mapping(token.get('projected'))
    sources = _objects(projection.get('sources'))
    expected_sources: list[dict[str, object]] = [
        {'serviceAccountToken': {'expirationSeconds': 3607, 'path': 'token'}},
        {'configMap': {'name': 'kube-root-ca.crt', 'items': [{'key': 'ca.crt', 'path': 'ca.crt'}]}},
        {'downwardAPI': {'items': [{'path': 'namespace', 'fieldRef': {'apiVersion': 'v1', 'fieldPath': 'metadata.namespace'}}]}},
    ]
    expected_volumes = [
        {'name': 'scratch-data', 'emptyDir': {}},
        {'name': 'webhook-certificates', 'secret': {'secretName': 'cnpg-webhook-cert', 'defaultMode': 420, 'optional': True}},
        {'name': token['name'], 'projected': {'defaultMode': 420, 'sources': expected_sources}},
    ]
    expected_mounts = [{'name': 'scratch-data', 'mountPath': '/controller'},
        {'name': 'webhook-certificates', 'mountPath': '/run/secrets/cnpg.io/webhook'},
        {'name': token['name'], 'mountPath': '/var/run/secrets/kubernetes.io/serviceaccount', 'readOnly': True}]
    if sources != expected_sources or volumes != expected_volumes or mounts != expected_mounts:
        raise _refuse()


def select_cnpg_operator(pods: Sequence[Mapping[str, object]]) -> CNPGOperatorIdentity:
    live = []
    for pod in pods:
        status = _mapping(pod.get('status', {}))
        if status.get('phase') in {'Succeeded', 'Failed'}:
            states = [entry for key in ('containerStatuses', 'initContainerStatuses', 'ephemeralContainerStatuses')
                      for entry in _objects(status.get(key, []))]
            if states and all(set(_mapping(s.get('state'))) == {'terminated'} for s in states):
                continue
        live.append(pod)
    if len(live) != 1:
        raise _refuse()
    pod = live[0]
    metadata, spec, status = (_mapping(pod.get(k)) for k in ('metadata', 'spec', 'status'))
    if (pod.get('apiVersion', 'v1') != 'v1' or pod.get('kind', 'Pod') != 'Pod'
            or metadata.get('namespace') != 'cnpg-system' or metadata.get('deletionTimestamp') is not None
            or set(spec) - _POD_FIELDS or spec.get('serviceAccountName') != 'cnpg-manager'
            or spec.get('serviceAccount', 'cnpg-manager') != 'cnpg-manager'
            or spec.get('securityContext') != {'runAsNonRoot': True, 'seccompProfile': {'type': 'RuntimeDefault'}}
            or status.get('phase') != 'Running'):
        raise _refuse()
    containers, states = _objects(spec.get('containers')), _objects(status.get('containerStatuses'))
    if len(containers) != 1 or len(states) != 1:
        raise _refuse()
    container, state = containers[0], states[0]
    if (set(container) - _CONTAINER_FIELDS or container.get('name') != 'manager' or state.get('name') != 'manager'
            or container.get('command') != _COMMAND[:1] or container.get('args') != _COMMAND[1:]
            or container.get('env') != _ENV or container.get('securityContext') != _SECURITY
            or container.get('image') not in {_IMAGE, 'ghcr.io/cloudnative-pg/cloudnative-pg:1.25.1'}
            or not str(state.get('containerID')).startswith('containerd://')
            or state.get('imageID') != _IMAGE or state.get('ready') is not True
            or set(_mapping(state.get('state'))) != {'running'}):
        raise _refuse()
    for key in ('livenessProbe', 'readinessProbe', 'startupProbe'):
        if key not in container:
            continue
        probe = _mapping(container[key])
        if (set(probe) - {'httpGet', 'failureThreshold', 'periodSeconds', 'successThreshold', 'timeoutSeconds'}
                or probe.get('httpGet') != {'path': '/readyz', 'port': 9443, 'scheme': 'HTTPS'}):
            raise _refuse()
    _volumes(spec, container)
    return CNPGOperatorIdentity(str(metadata.get('name')), str(metadata.get('uid')),
        str(state.get('containerID')).removeprefix('containerd://'), str(spec.get('nodeName')),
        state.get('restartCount'), _digest(spec))  # type: ignore[arg-type]


def _observe_inputs(runner: CNPGOperatorRunner) -> CNPGOperatorIdentity:
    payload = _json(runner.capture_stdout(
        ('kubectl', 'get', '--raw=/api/v1/namespaces/cnpg-system/pods', '--request-timeout=30s'),
        env=runner.environment, timeout_seconds=30))
    metadata = _mapping(payload.get('metadata'))
    if (payload.get('apiVersion') != 'v1' or payload.get('kind') != 'PodList'
            or not isinstance(metadata.get('resourceVersion'), str) or not metadata['resourceVersion']
            or metadata.get('continue', '') or metadata.get('remainingItemCount', 0) != 0):
        raise _refuse()
    for kind in ('configmap', 'secret'):
        response = runner.capture_stdout(
            ('kubectl', '--namespace=cnpg-system', 'get', kind, 'cnpg-controller-manager-config',
             '--ignore-not-found=true', '--output=json', '--request-timeout=30s'),
            env=runner.environment, timeout_seconds=30)
        if response.strip():
            raise _refuse()
    return select_cnpg_operator(_objects(payload.get('items')))


def observe_cnpg_operator(runner: CNPGOperatorRunner) -> CNPGOperatorRuntime:
    """Bracket actual byte/process observation with unchanged, admitted API inputs."""
    identity = _observe_inputs(runner)
    runtime = CNPGOperatorRuntime.from_host_observation(identity, runner.inspect_staging_cnpg_operator(identity))
    if _observe_inputs(runner) != identity:
        raise _refuse()
    return runtime
