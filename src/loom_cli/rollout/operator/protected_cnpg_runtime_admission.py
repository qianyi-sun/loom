"""Observe the fixed staging primary's executable and mount inputs, without mutation.

Administrator and host writer exclusion is an enclosing prerequisite. This
observation cannot establish it, retire SQL, or authorize a manager replacement.
The caller must bind the returned identities to its original operation and keep
checking them across recovery. No discovered successor is adopted here.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping
from dataclasses import asdict, dataclass

from .protected_application_credential_recovery import CredentialRecoveryRunner
from .protected_cnpg_manager_replacement import (
    CNPG_MANAGER_IMAGE,
    CNPG_MANAGER_SHA256,
    CNPGManagerIdentity,
)
from .protected_cnpg_writer_configuration import _json, _mapping

CNPG_POSTGRES_IMAGE = 'ghcr.io/cloudnative-pg/postgresql@sha256:3c0ba08ea353c9705a755c113e4ae395be76553e0ed68076e5410cb09b9d17d9'
# Independently extracted, without starting the pinned amd64 image, from
# /usr/lib/postgresql/17/bin/postgres (9,963,336 bytes). Never learn this from live.
CNPG_POSTGRES_SHA256 = '592d01a1517af056c5209a3cdcd37ddd9ba44c39f4cbd372cd72acceab2f3a36'
_NAMESPACE = 'loom-staging'
_CLUSTER = 'loom-postgres'
_UID = re.compile(r'[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}\Z')
_SECURITY = {'allowPrivilegeEscalation': False, 'capabilities': {'drop': ['ALL']},
             'privileged': False, 'readOnlyRootFilesystem': True, 'runAsNonRoot': True,
             'seccompProfile': {'type': 'RuntimeDefault'}}
_POD_SECURITY = {'fsGroup': 26, 'runAsGroup': 26, 'runAsUser': 26,
                 'runAsNonRoot': True, 'seccompProfile': {'type': 'RuntimeDefault'}}
_COMMAND = ['/controller/manager', 'instance', 'run', '--status-port-tls', '--log-level=info']
_BOOTSTRAP = ['/manager', 'bootstrap', '/controller/manager', '--log-level=info']
_MOUNTS: list[dict[str, object]] = [
    {'name': 'pgdata', 'mountPath': '/var/lib/postgresql/data'},
    {'name': 'scratch-data', 'mountPath': '/run'},
    {'name': 'scratch-data', 'mountPath': '/controller'},
    {'name': 'shm', 'mountPath': '/dev/shm'},
]
_CONTAINER_FIELDS = frozenset({'name', 'image', 'imagePullPolicy', 'command', 'env', 'resources',
    'securityContext', 'terminationMessagePath', 'terminationMessagePolicy', 'volumeMounts',
    'ports', 'livenessProbe', 'readinessProbe', 'startupProbe'})
_POD_FIELDS = frozenset({'affinity', 'containers', 'initContainers', 'volumes', 'nodeName', 'hostname',
    'securityContext', 'dnsPolicy', 'enableServiceLinks', 'preemptionPolicy', 'priority', 'priorityClassName',
    'restartPolicy', 'schedulerName', 'serviceAccount', 'serviceAccountName', 'terminationGracePeriodSeconds',
    'tolerations', 'nodeSelector', 'imagePullSecrets', 'automountServiceAccountToken'})


@dataclass(frozen=True, slots=True)
class CNPGPrimaryPodIdentity:
    pod_name: str
    pod_uid: str
    container_id: str
    node_name: str
    restart_count: int

    def __post_init__(self) -> None:
        if (not isinstance(self.pod_name, str) or re.fullmatch(r'loom-postgres-[1-9][0-9]{0,5}', self.pod_name) is None
                or not isinstance(self.pod_uid, str) or _UID.fullmatch(self.pod_uid) is None
                or not isinstance(self.container_id, str) or re.fullmatch(r'containerd://[0-9a-f]{64}', self.container_id) is None
                or self.node_name not in {'trt-eai-oldlab-3', 'trt-eai-oldlab-4', 'trt-eai-oldlab-5'}
                or type(self.restart_count) is not int or not 0 <= self.restart_count < 2**31):
            raise ValueError('CNPG primary container identity is unsupported')


@dataclass(frozen=True, slots=True)
class CNPGPrimaryRuntime:
    manager: CNPGManagerIdentity
    pod_spec_sha256: str
    postgres_pid: int
    postgres_started_ticks: int
    postgres_device: int
    postgres_inode: int


def _one(value: object, name: str) -> dict[str, object]:
    if not isinstance(value, list) or len(value) != 1 or _mapping(value[0]).get('name') != name:
        raise ValueError('CNPG container set is unsupported')
    return _mapping(value[0])


def _environment(pod: str) -> list[dict[str, object]]:
    return [{'name': k, 'value': v} for k, v in {
        'PGDATA': '/var/lib/postgresql/data/pgdata', 'POD_NAME': pod, 'NAMESPACE': _NAMESPACE,
        'CLUSTER_NAME': _CLUSTER, 'PSQL_HISTORY': '/controller/tmp/.psql_history',
        'PGPORT': '5432', 'PGHOST': '/controller/run', 'TMPDIR': '/controller/tmp',
    }.items()]


def _canonical(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()


def _same_items(observed: object, expected: list[dict[str, object]]) -> bool:
    return isinstance(observed, list) and sorted(map(_canonical, observed)) == sorted(map(_canonical, expected))


def _volumes(spec: Mapping[str, object], pod: str) -> list[dict[str, object]]:
    mounts = [dict(item) for item in _MOUNTS]
    volumes = spec.get('volumes')
    if not isinstance(volumes, list):
        raise ValueError('CNPG volumes are absent')
    expected: list[dict[str, object]] = [
        {'name': 'pgdata', 'persistentVolumeClaim': {'claimName': pod}},
        {'name': 'scratch-data', 'emptyDir': {}}, {'name': 'shm', 'emptyDir': {'medium': 'Memory'}},
    ]
    projected = [v for v in volumes if 'projected' in _mapping(v)]
    if projected:
        volume = _one_projected(projected)
        name = volume['name']
        expected.append(volume)
        mounts.append({'name': name, 'mountPath': '/var/run/secrets/kubernetes.io/serviceaccount', 'readOnly': True})
    if not _same_items(volumes, expected):
        raise ValueError('CNPG volume sources are unsupported')
    return mounts


def _one_projected(values: list[object]) -> dict[str, object]:
    if len(values) != 1:
        raise ValueError('CNPG projected volumes are unsupported')
    volume = _mapping(values[0])
    name = volume.get('name')
    if set(volume) != {'name', 'projected'} or not isinstance(name, str) or re.fullmatch(r'kube-api-access-[a-z0-9]{5}', name) is None:
        raise ValueError('CNPG projected volume is unsupported')
    projection = _mapping(volume['projected'])
    sources = projection.get('sources')
    if set(projection) != {'defaultMode', 'sources'} or projection['defaultMode'] != 420 or not isinstance(sources, list) or len(sources) != 3:
        raise ValueError('CNPG projected sources are unsupported')
    token = _mapping(_mapping(sources[0]).get('serviceAccountToken'))
    if (set(_mapping(sources[0])) != {'serviceAccountToken'} or set(token) != {'expirationSeconds', 'path'}
            or token['path'] != 'token' or type(token['expirationSeconds']) is not int
            or not 600 <= token['expirationSeconds'] <= 86400
            or sources[1] != {'configMap': {'name': 'kube-root-ca.crt', 'items': [{'key': 'ca.crt', 'path': 'ca.crt'}]}}
            or sources[2] != {'downwardAPI': {'items': [{'fieldRef': {'apiVersion': 'v1', 'fieldPath': 'metadata.namespace'}, 'path': 'namespace'}]}}):
        raise ValueError('CNPG projected credentials are unsupported')
    return volume


def admit_cnpg_primary_pod(value: Mapping[str, object], *, cluster_uid: str) -> tuple[CNPGPrimaryPodIdentity, str]:
    """Validate startup/volume/image inputs before any exec can reach a target."""
    metadata, spec, status = (_mapping(value.get(key)) for key in ('metadata', 'spec', 'status'))
    if (value.get('apiVersion') != 'v1' or value.get('kind') != 'Pod'
            or metadata.get('namespace') != _NAMESPACE or metadata.get('deletionTimestamp') is not None
            or _UID.fullmatch(cluster_uid) is None or status.get('phase') != 'Running'
            or metadata.get('ownerReferences') != [{'apiVersion': 'postgresql.cnpg.io/v1', 'kind': 'Cluster',
                'name': _CLUSTER, 'uid': cluster_uid, 'controller': True, 'blockOwnerDeletion': True}]
            or set(spec) - _POD_FIELDS or spec.get('securityContext') != _POD_SECURITY
            or spec.get('serviceAccountName') != _CLUSTER or spec.get('serviceAccount', _CLUSTER) != _CLUSTER):
        raise ValueError('CNPG primary Pod profile is unsupported')
    running = _one(status.get('containerStatuses'), 'postgres')
    bootstrap_status = _one(status.get('initContainerStatuses'), 'bootstrap-controller')
    if (running.get('imageID') != CNPG_POSTGRES_IMAGE or set(_mapping(running.get('state'))) != {'running'}
            or bootstrap_status.get('imageID') != CNPG_MANAGER_IMAGE
            or set(_mapping(bootstrap_status.get('state'))) != {'terminated'}
            or _mapping(_mapping(bootstrap_status['state'])['terminated']).get('exitCode') != 0
            or status.get('ephemeralContainerStatuses', []) != []):
        raise ValueError('CNPG running image profile is unsupported')
    restarts = running.get('restartCount')
    if type(restarts) is not int:
        raise ValueError('CNPG restart count is invalid')
    identity = CNPGPrimaryPodIdentity(str(metadata.get('name')), str(metadata.get('uid')),
        str(running.get('containerID')), str(spec.get('nodeName')), restarts)
    mounts = _volumes(spec, identity.pod_name)
    for field, name, command, images, env in (
        ('containers', 'postgres', _COMMAND, {CNPG_POSTGRES_IMAGE, 'ghcr.io/cloudnative-pg/postgresql:17.4',
            'ghcr.io/cloudnative-pg/postgresql:17.4@' + CNPG_POSTGRES_IMAGE.split('@')[1]}, _environment(identity.pod_name)),
        ('initContainers', 'bootstrap-controller', _BOOTSTRAP,
            {CNPG_MANAGER_IMAGE, 'ghcr.io/cloudnative-pg/cloudnative-pg:1.25.1'}, []),
    ):
        container = _one(spec.get(field), name)
        if (set(container) - _CONTAINER_FIELDS or container.get('image') not in images
                or container.get('command') != command or container.get('securityContext') != _SECURITY
                or not _same_items(container.get('env', []), env)
                or not _same_items(container.get('volumeMounts'), mounts)):
            raise ValueError('CNPG executable inputs are unsupported')
        for key in ('livenessProbe', 'readinessProbe', 'startupProbe'):
            if key in container:
                probe = _mapping(container[key])
                if (set(probe) - {'failureThreshold', 'successThreshold', 'periodSeconds', 'timeoutSeconds', 'httpGet'}
                        or probe.get('httpGet') != {'path': '/readyz' if key == 'readinessProbe' else '/healthz',
                                                  'port': 8000, 'scheme': 'HTTPS'}):
                    raise ValueError('CNPG process probe is unsupported')
    return identity, hashlib.sha256(_canonical(spec)).hexdigest()


# Fixed files/commands, not a shell program supplied by a plan or API object.
_PROCESS = r'''read -r pgpid < /var/lib/postgresql/data/pgdata/postmaster.pid
case "$pgpid" in ''|*[!0-9]*) exit 1;; esac
cat /proc/1/cmdline; printf '\n'
cat /proc/1/stat
stat -Lc '%d %i' /proc/1/exe /controller/manager
sha256sum /proc/1/exe /controller/manager
cat /proc/"$pgpid"/cmdline; printf '\n'
cat /proc/"$pgpid"/stat
stat -Lc '%d %i' /proc/"$pgpid"/exe /usr/lib/postgresql/17/bin/postgres
sha256sum /proc/"$pgpid"/exe /usr/lib/postgresql/17/bin/postgres
'''


def _process(payload: bytes, pod: CNPGPrimaryPodIdentity, digest: str) -> CNPGPrimaryRuntime:
    lines = payload.splitlines()
    if len(lines) != 12 or lines[0] != b'\0'.join(s.encode() for s in _COMMAND) + b'\0' or lines[6] != b'postgres\0-D\0/var/lib/postgresql/data/pgdata\0':
        raise ValueError('CNPG process command is unsupported')
    def identity(line: bytes, command: bytes) -> tuple[int, int]:
        match = re.fullmatch(rb'([1-9][0-9]*) \(' + command + rb'\) (.+)', line)
        if match is None:
            raise ValueError('CNPG process identity is invalid')
        fields = match[2].split()
        if len(fields) < 20 or (command == b'postgres' and fields[1] != b'1'):
            raise ValueError('CNPG postmaster parent changed')
        return int(match[1]), int(fields[19])
    try:
        manager_pid, started = identity(lines[1], b'manager')
        postgres_pid, postgres_started = identity(lines[7], b'postgres')
        device, inode = (int(part) for part in lines[2].split())
        pg_device, pg_inode = (int(part) for part in lines[8].split())
        if (manager_pid != 1 or lines[2] != lines[3] or lines[8] != lines[9]
                or lines[4] != (CNPG_MANAGER_SHA256 + '  /proc/1/exe').encode()
                or lines[5] != (CNPG_MANAGER_SHA256 + '  /controller/manager').encode()
                or lines[10] != (CNPG_POSTGRES_SHA256 + f'  /proc/{postgres_pid}/exe').encode()
                or lines[11] != (CNPG_POSTGRES_SHA256 + '  /usr/lib/postgresql/17/bin/postgres').encode()
                or any(not 0 < n < 2**64 for n in (started, postgres_started, pg_device, pg_inode))):
            raise ValueError('CNPG executable profile is unsupported')
    except (ValueError, IndexError):
        raise ValueError('CNPG executable profile is unsupported') from None
    manager = CNPGManagerIdentity(**asdict(pod), process_started_ticks=started,
                                  executable_device=device, executable_inode=inode)
    return CNPGPrimaryRuntime(manager, digest, postgres_pid, postgres_started, pg_device, pg_inode)


def observe_cnpg_primary_runtime(runner: CredentialRecoveryRunner, *, cluster_uid: str,
                                 pod_name: str) -> CNPGPrimaryRuntime:
    """Bracket actual executable observation with exact primary/Pod readback.

    Target names are fixed staging instances, and node admission precedes exec.
    This does not read or execute commands on replicas (including OLDLAB2).
    """
    if re.fullmatch(r'loom-postgres-[1-9][0-9]{0,5}', pod_name) is None:
        raise ValueError('CNPG primary name is invalid')
    def read() -> tuple[CNPGPrimaryPodIdentity, str]:
        def get(resource: str) -> dict[str, object]:
            return _json(runner.capture_stdout(
                ('kubectl', '--namespace', _NAMESPACE, 'get', resource, '--output=json', '--request-timeout=30s'),
                env=runner.environment, timeout_seconds=30))
        cluster = get('cluster.postgresql.cnpg.io/' + _CLUSTER)
        status = _mapping(cluster.get('status'))
        if (_mapping(cluster.get('metadata')).get('uid') != cluster_uid
                or status.get('currentPrimary') != pod_name or status.get('targetPrimary') != pod_name):
            raise RuntimeError('CNPG primary topology changed')
        return admit_cnpg_primary_pod(get('pod/' + pod_name), cluster_uid=cluster_uid)
    before = read()
    result = _process(runner.capture_stdout(
        ('kubectl', '--namespace', _NAMESPACE, 'exec', 'pod/' + pod_name, '--container=postgres', '--', 'sh', '-ceu', _PROCESS),
        env=runner.environment, timeout_seconds=30), *before)
    if read() != before:
        raise RuntimeError('CNPG primary inputs changed during process observation')
    return result
