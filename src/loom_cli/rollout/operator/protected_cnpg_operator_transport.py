"""Fixed dedicated SSH observation channel; no caller-selected remote commands.

The trusted installer must provision the dedicated key, root-owned host inventory
and host keys, and a forced-command observer account on the admitted K3s nodes.
The remote helper must be the exact installed candidate's standalone observer.
This module never provisions access or falls back to a personal/root SSH identity.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import stat
import subprocess
from pathlib import Path
from uuid import uuid4

from loom_cli.rollout.credential_authority import read_trusted_file

from .protected_cnpg_operator_admission import CNPGOperatorIdentity, CNPGOperatorRuntime
from .protected_cnpg_writer_configuration import _json, _mapping

_CONFIG = Path('/etc/loom/staging-cnpg-observer-ssh-config')
_KNOWN_HOSTS = Path('/etc/loom/staging-cnpg-observer-known-hosts')
_IDENTITY = Path('/var/lib/loom-staging-rollout/cnpg-observer-ed25519')
_SOURCE = Path(__file__).with_name('protected_cnpg_operator_host.py')
_NODES = frozenset({'trt-eai-oldlab-3', 'trt-eai-oldlab-4', 'trt-eai-oldlab-5'})


def _refuse() -> ValueError:
    return ValueError('CNPG operator fixed observation transport changed or is unavailable')


def _validate_config(payload: bytes, node: str) -> None:
    """Admit literal endpoint/port mappings only, without includes or executable options."""
    hosts: dict[str, dict[str, str]] = {}
    current: str | None = None
    for raw in payload.decode('ascii').splitlines():
        line = raw.split('#', 1)[0].strip()
        if not line:
            continue
        fields = line.split()
        if len(fields) != 2:
            raise _refuse()
        option, value = fields[0].lower(), fields[1]
        if option == 'host':
            if value not in _NODES or value in hosts:
                raise _refuse()
            current = value
            hosts[current] = {}
        else:
            if current is None or option not in {'hostname', 'port'} or option in hosts[current]:
                raise _refuse()
            if ((option == 'hostname' and re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9.:-]{0,252}', value) is None)
                    or (option == 'port' and (not value.isdecimal() or not 1 <= int(value) <= 65535))):
                raise _refuse()
            hosts[current][option] = value
    if node not in hosts or any('hostname' not in values for values in hosts.values()):
        raise _refuse()


def _inputs(node: str) -> tuple[str, tuple[tuple[str, str, str], ...]]:
    snapshots = []
    source_sha = ''
    for path in (_CONFIG, _KNOWN_HOSTS, _IDENTITY, _SOURCE):
        private = path == _IDENTITY
        observed = read_trusted_file(path, service_uid=os.geteuid(), private=private,
            max_bytes=64 * 1024, require_nonempty=True)
        if ((path in {_CONFIG, _KNOWN_HOSTS} and observed.metadata.st_uid != 0)
                or (private and (observed.metadata.st_uid != os.geteuid()
                                 or stat.S_IMODE(observed.metadata.st_mode) != 0o600))):
            raise _refuse()
        if path == _CONFIG:
            _validate_config(observed.payload, node)
        digest = hashlib.sha256(observed.payload).hexdigest()
        snapshots.append((digest, observed.metadata_fingerprint, observed.acl_fingerprint))
        if path == _SOURCE:
            source_sha = digest
    return source_sha, tuple(snapshots)


def inspect_staging_cnpg_operator(identity: CNPGOperatorIdentity) -> dict[str, object]:
    if type(identity) is not CNPGOperatorIdentity or identity.node_name not in _NODES:
        raise _refuse()
    # Revalidate even if an object was modified by an enclosing caller.
    identity.__post_init__()
    before = _inputs(identity.node_name)
    request = {'schema_version': 1, 'nonce': uuid4().hex, 'observer_sha256': before[0],
        'identity': {key: getattr(identity, key) for key in ('node_name', 'pod_name', 'pod_uid', 'container_id')}}
    options = ['BatchMode=yes', 'IdentitiesOnly=yes', 'IdentityAgent=none', 'CertificateFile=none',
        'StrictHostKeyChecking=yes', 'UserKnownHostsFile=' + str(_KNOWN_HOSTS), 'GlobalKnownHostsFile=/dev/null',
        'HostKeyAlias=' + identity.node_name, 'UpdateHostKeys=no', 'ForwardAgent=no', 'ForwardX11=no',
        'ClearAllForwardings=yes', 'PermitLocalCommand=no', 'RemoteCommand=none',
        'ControlMaster=no', 'ControlPath=none', 'ConnectionAttempts=1', 'ConnectTimeout=10',
        'ServerAliveInterval=5', 'ServerAliveCountMax=2', 'PasswordAuthentication=no', 'KbdInteractiveAuthentication=no']
    argv = ['/usr/bin/ssh', '-T', '-F', str(_CONFIG), '-i', str(_IDENTITY), '-l', 'loom-cnpg-observer']
    for option in options:
        argv.extend(('-o', option))
    argv.append(identity.node_name)
    try:
        response = subprocess.run(argv, input=json.dumps(request, sort_keys=True, separators=(',', ':')).encode(),
            capture_output=True, check=False, timeout=45,
            env={'PATH': '/usr/bin:/bin', 'LANG': 'C', 'LC_ALL': 'C'})
    except (OSError, subprocess.SubprocessError) as exc:
        raise _refuse() from exc
    if response.returncode != 0 or not 0 < len(response.stdout) <= 64 * 1024:
        raise _refuse()
    reply = _json(response.stdout)
    if (set(reply) != {*request, 'observation'} or type(reply['schema_version']) is not int
            or any(reply[key] != value for key, value in request.items()) or _inputs(identity.node_name) != before):
        raise _refuse()
    observation = _mapping(reply['observation'])
    CNPGOperatorRuntime.from_host_observation(identity, observation)
    return observation
