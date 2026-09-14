"""Observe the fixed primary's actual PVC/PV/CSI path and Kubernetes consumers.

Run under the enclosing administrator/host/storage-writer exclusion. Matching
Pod references alone never establish that exclusion: privileged host consumers
are separately inventoried and bound so they cannot silently change across the
handoff. The trusted storage platform still owns ordinary block replication/I/O.
This observer grants no authority to pause or adopt a foreign storage workload.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass
from typing import Protocol

from .protected_cnpg_runtime_admission import CNPGPrimaryRuntime, admit_cnpg_primary_pod
from .protected_cnpg_writer_configuration import _json, _mapping

_UID = re.compile(r'[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}\Z')
_DRIVER = 'driver.longhorn.io'
_LISTS = {'PersistentVolume': ('v1', '/api/v1/persistentvolumes'),
          'VolumeAttachment': ('storage.k8s.io/v1', '/apis/storage.k8s.io/v1/volumeattachments'),
          'Pod': ('v1', '/api/v1/pods')}


class CNPGVolumeRunner(Protocol):
    @property
    def environment(self) -> Mapping[str, str]: ...

    def capture_stdout(self, argv: Sequence[str], *, env: Mapping[str, str], timeout_seconds: float) -> bytes: ...


def _refuse() -> ValueError:
    return ValueError('CNPG volume primary storage identity or consumer profile changed')


def _digest(value: object) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(',', ':')).encode()).hexdigest()


def _objects(value: object) -> list[dict[str, object]]:
    if not isinstance(value, list):
        raise _refuse()
    return [_mapping(item) for item in value]


def _metadata(value: Mapping[str, object]) -> dict[str, object]:
    metadata = _mapping(value.get('metadata'))
    if (not isinstance(metadata.get('name'), str) or not metadata['name']
            or not isinstance(metadata.get('uid'), str) or _UID.fullmatch(str(metadata['uid'])) is None
            or metadata.get('deletionTimestamp') is not None):
        raise _refuse()
    return metadata


def _read(runner: CNPGVolumeRunner, path: str) -> dict[str, object]:
    return _json(runner.capture_stdout(('kubectl', 'get', '--raw=' + path, '--request-timeout=30s'),
                                       env=runner.environment, timeout_seconds=30))


def _list(runner: CNPGVolumeRunner, kind: str) -> list[dict[str, object]]:
    version, path = _LISTS[kind]
    value = _read(runner, path)
    metadata, items = _mapping(value.get('metadata')), _objects(value.get('items'))
    if (value.get('apiVersion') != version or value.get('kind') != kind + 'List' or len(items) > 2048
            or not isinstance(metadata.get('resourceVersion'), str) or not metadata['resourceVersion']
            or metadata.get('continue', '') or metadata.get('remainingItemCount', 0) != 0):
        raise _refuse()
    for item in items:
        if item.get('kind', kind) != kind or item.get('apiVersion', version) != version:
            raise _refuse()
        item.update(kind=kind, apiVersion=version)
    return items


def _terminal(pod: Mapping[str, object]) -> bool:
    status = _mapping(pod.get('status', {}))
    states = [item for key in ('containerStatuses', 'initContainerStatuses', 'ephemeralContainerStatuses')
              for item in _objects(status.get(key, []))]
    return (status.get('phase') in {'Succeeded', 'Failed'} and bool(states)
            and all(set(_mapping(item.get('state'))) == {'terminated'} for item in states))


def _host_consumer(pod: Mapping[str, object]) -> bool:
    spec = _mapping(pod.get('spec'))
    if any(spec.get(key) is True for key in ('hostPID', 'hostIPC', 'hostNetwork')):
        return True
    if any('hostPath' in volume for volume in _objects(spec.get('volumes', []))):
        return True
    for kind in ('containers', 'initContainers', 'ephemeralContainers'):
        for container in _objects(spec.get(kind, [])):
            security = _mapping(container.get('securityContext', {}))
            if (security.get('privileged') is True
                    or _mapping(security.get('capabilities', {})).get('add', [])):
                return True
    return False


@dataclass(frozen=True, slots=True)
class CNPGVolumeObservation:
    claim_uid: str
    volume_uid: str
    attachment_uid: str
    topology_sha256: str
    privileged_host_consumers_sha256: str

    @property
    def digest(self) -> str:
        return _digest({'schema_version': 1, **asdict(self)})


def _observe(runner: CNPGVolumeRunner, runtime: CNPGPrimaryRuntime) -> CNPGVolumeObservation:
    claim = _read(runner, '/api/v1/namespaces/loom-staging/persistentvolumeclaims/' + runtime.manager.pod_name)
    claim_meta, claim_spec = _metadata(claim), _mapping(claim.get('spec'))
    owners = _objects(claim_meta.get('ownerReferences', []))
    if (claim.get('apiVersion') != 'v1' or claim.get('kind') != 'PersistentVolumeClaim'
            or claim_meta.get('namespace') != 'loom-staging' or claim_meta['name'] != runtime.manager.pod_name
            or len(owners) != 1 or any(owners[0].get(key) != expected for key, expected in {
                'apiVersion': 'postgresql.cnpg.io/v1', 'kind': 'Cluster', 'name': 'loom-postgres',
                'uid': runtime.cluster_uid, 'controller': True}.items())
            or set(claim_spec) != {'accessModes', 'storageClassName', 'volumeMode', 'volumeName', 'resources'}
            or claim_spec['accessModes'] != ['ReadWriteOnce'] or claim_spec['storageClassName'] != 'longhorn'
            or claim_spec['volumeMode'] != 'Filesystem' or claim_spec['volumeName'] != 'pvc-' + str(claim_meta['uid'])
            or _mapping(claim.get('status')).get('phase') != 'Bound'):
        raise _refuse()
    volumes = _list(runner, 'PersistentVolume')
    selected = [pv for pv in volumes if _mapping(pv.get('metadata')).get('name') == claim_spec['volumeName']]
    if len(selected) != 1:
        raise _refuse()
    volume = selected[0]
    volume_meta, volume_spec = _metadata(volume), _mapping(volume.get('spec'))
    csi = _mapping(volume_spec.get('csi'))
    attributes = _mapping(csi.get('volumeAttributes'))
    ref = _mapping(volume_spec.get('claimRef'))
    if (set(volume_spec) != {'accessModes', 'volumeMode', 'storageClassName', 'capacity',
                            'persistentVolumeReclaimPolicy', 'claimRef', 'csi'}
            or volume_spec['accessModes'] != ['ReadWriteOnce'] or volume_spec['volumeMode'] != 'Filesystem'
            or volume_spec['storageClassName'] != 'longhorn' or volume_spec['persistentVolumeReclaimPolicy'] != 'Delete'
            or any(ref.get(key) != expected for key, expected in {'apiVersion': 'v1', 'kind': 'PersistentVolumeClaim',
                'name': claim_meta['name'], 'namespace': 'loom-staging', 'uid': claim_meta['uid']}.items())
            or set(csi) != {'driver', 'fsType', 'volumeHandle', 'volumeAttributes'} or csi['driver'] != _DRIVER
            or csi['fsType'] != 'ext4' or csi['volumeHandle'] != volume_meta['name']
            or set(attributes) - {'dataEngine', 'dataLocality', 'disableRevisionCounter', 'fromBackup', 'fsType',
                'numberOfReplicas', 'staleReplicaTimeout', 'storage.kubernetes.io/csiProvisionerIdentity',
                'unmapMarkSnapChainRemoved'}
            or attributes.get('dataEngine') != 'v1' or attributes.get('numberOfReplicas') != '3'
            or attributes.get('fromBackup', '') != '' or attributes.get('dataLocality', 'disabled') != 'disabled'
            or _mapping(volume.get('status')).get('phase') != 'Bound'):
        raise _refuse()
    if _mapping(claim_spec['resources']).get('requests') != volume_spec['capacity']:
        raise _refuse()
    aliases = [pv for pv in volumes if _mapping(_mapping(pv.get('spec')).get('csi', {})).get('driver') == _DRIVER
               and _mapping(_mapping(pv.get('spec')).get('csi', {})).get('volumeHandle') == csi['volumeHandle']]
    if aliases != [volume]:
        raise _refuse()
    attachments = []
    for item in _list(runner, 'VolumeAttachment'):
        spec = _mapping(item.get('spec'))
        source = _mapping(spec.get('source'))
        if source.get('persistentVolumeName') == volume_meta['name']:
            attachments.append(item)
        elif spec.get('attacher') == _DRIVER and 'inlineVolumeSpec' in source:
            raise _refuse()  # Inline Longhorn attachment is outside this profile.
    if len(attachments) != 1:
        raise _refuse()
    attachment = attachments[0]
    attachment_meta = _metadata(attachment)
    if (attachment.get('spec') != {'attacher': _DRIVER, 'nodeName': runtime.manager.node_name,
                                  'source': {'persistentVolumeName': volume_meta['name']}}
            or attachment.get('status') != {'attached': True}):
        raise _refuse()
    consumers = []
    host_consumers = []
    for pod in _list(runner, 'Pod'):
        if _terminal(pod):
            continue
        spec, metadata = _mapping(pod.get('spec')), _mapping(pod.get('metadata'))
        for mount in _objects(spec.get('volumes', [])):
            if (metadata.get('namespace') == 'loom-staging'
                    and _mapping(mount.get('persistentVolumeClaim', {})).get('claimName') == claim_meta['name']):
                consumers.append(pod)
            if _mapping(mount.get('csi', {})).get('driver') == _DRIVER:
                raise _refuse()
        if _host_consumer(pod):
            # This records the actual privileged workload set. Admission of those
            # holders belongs to the enclosing coordinated maintenance authority.
            host_consumers.append({'uid': metadata.get('uid'), 'namespace': metadata.get('namespace'),
                'name': metadata.get('name'), 'spec': spec,
                'containers': {key: _mapping(pod.get('status', {})).get(key, []) for key in
                    ('containerStatuses', 'initContainerStatuses', 'ephemeralContainerStatuses')}})
    if len(consumers) != 1:
        raise _refuse()
    identity, pod_digest = admit_cnpg_primary_pod(consumers[0], cluster_uid=runtime.cluster_uid)
    if (any(getattr(identity, key) != getattr(runtime.manager, key) for key in asdict(identity))
            or pod_digest != runtime.pod_spec_sha256):
        raise _refuse()
    host_consumers.sort(key=lambda value: str(value['uid']))
    return CNPGVolumeObservation(str(claim_meta['uid']), str(volume_meta['uid']), str(attachment_meta['uid']),
        _digest({'claim': claim_spec, 'volume': volume_spec, 'attachment': attachment['spec'],
                 'primary': asdict(identity), 'pod_spec_sha256': pod_digest}), _digest(host_consumers))


def observe_cnpg_volume(runner: CNPGVolumeRunner, *, runtime: CNPGPrimaryRuntime) -> CNPGVolumeObservation:
    """Require two complete live inventories of the same original storage path."""
    before = _observe(runner, runtime)
    if _observe(runner, runtime) != before:
        raise _refuse()
    return before
