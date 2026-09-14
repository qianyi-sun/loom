"""Fixed read-only host observer for the staging CNPG operator.

Run by a trusted host transport, in the actual K3s node's host namespaces. Input
selects only a validated original CNPG container; it never supplies a command,
filesystem path, SQL, image or replacement executable. Output contains identities
and hashes only. This module does not install or authorize that transport.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import socket
import stat
import subprocess
from pathlib import Path

_BINARY_SHA256 = '0a8f22a9c14805f67b92f6994d6487da7570929108443d1a70a66b8d47a51b2f'
_BINARY_SIZE = 61_046_968
_COMMAND = ['/manager', 'controller', '--leader-elect', '--max-concurrent-reconciles=10',
            '--config-map-name=cnpg-controller-manager-config', '--secret-name=cnpg-controller-manager-config',
            '--webhook-port=9443']
_PROC = Path('/proc')


def _file_identity(value: os.stat_result) -> tuple[int, ...]:
    # Reads may update access time; it is not evidence of a changed executable.
    return (value.st_dev, value.st_ino, value.st_mode, value.st_uid, value.st_gid,
            value.st_nlink, value.st_size, value.st_mtime_ns, value.st_ctime_ns)


def _refuse() -> RuntimeError:
    return RuntimeError('CNPG operator host process observation refused')


def _request(value: object) -> dict[str, str]:
    if (not isinstance(value, dict) or set(value) != {'node_name', 'pod_name', 'pod_uid', 'container_id'}
            or not all(isinstance(item, str) for item in value.values())
            or value['node_name'] not in {'trt-eai-oldlab-3', 'trt-eai-oldlab-4', 'trt-eai-oldlab-5'}
            or re.fullmatch(r'cnpg-controller-manager-[a-z0-9]{5,16}-[a-z0-9]{5}', value['pod_name']) is None
            or re.fullmatch(r'[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}', value['pod_uid']) is None
            or re.fullmatch(r'[0-9a-f]{64}', value['container_id']) is None):
        raise _refuse()
    return value


def _runtime_pid(request: dict[str, str]) -> int:
    result = subprocess.run(
        ['/usr/local/bin/k3s', 'crictl', '--runtime-endpoint=unix:///run/k3s/containerd/containerd.sock',
         'inspect', request['container_id']], capture_output=True, check=True, timeout=15,
        env={'PATH': '/usr/local/bin:/usr/bin:/bin', 'LANG': 'C', 'LC_ALL': 'C'},
    )
    if not 0 < len(result.stdout) <= 1024 * 1024:
        raise _refuse()
    value = json.loads(result.stdout)
    status = value['status']
    labels = status['labels']
    if (status['id'] != request['container_id'] or status['state'] != 'CONTAINER_RUNNING'
            or labels.get('io.kubernetes.pod.namespace') != 'cnpg-system'
            or labels.get('io.kubernetes.pod.name') != request['pod_name']
            or labels.get('io.kubernetes.pod.uid') != request['pod_uid']
            or labels.get('io.kubernetes.container.name') != 'manager'):
        raise _refuse()
    pid = value['info']['pid']
    if type(pid) is not int or not 1 < pid < 2**31:
        raise _refuse()
    return pid


def _identity(process: Path, pid: int) -> tuple[int, str, str]:
    raw = (process / 'stat').read_text()
    tail = raw.rsplit(')', 1)[1].split()
    if int(raw.split(' ', 1)[0]) != pid or tail[0] in {'Z', 'X'}:
        raise _refuse()
    return int(tail[19]), os.readlink(process / 'ns/mnt'), os.readlink(process / 'ns/pid')


def _binary(path: Path) -> tuple[int, int, str]:
    with path.open('rb') as stream:
        before = os.fstat(stream.fileno())
        if not stat.S_ISREG(before.st_mode) or before.st_size != _BINARY_SIZE:
            raise _refuse()
        digest = hashlib.file_digest(stream, 'sha256').hexdigest()
        after = os.fstat(stream.fileno())
        if (_file_identity(before) != _file_identity(after) or digest != _BINARY_SHA256):
            raise _refuse()
    return before.st_dev, before.st_ino, digest


def _only_operator_in_namespace(pid: int, namespace: str) -> None:
    observed = []
    for candidate in _PROC.iterdir():
        if not candidate.name.isdecimal():
            continue
        try:
            if os.readlink(candidate / 'ns/pid') == namespace:
                observed.append(int(candidate.name))
        except FileNotFoundError:
            continue  # Unrelated host process exited during the scan.
    if observed != [pid]:
        raise _refuse()


def inspect_cnpg_operator_host(value: object) -> dict[str, object]:
    request = _request(value)
    if os.geteuid() != 0 or socket.gethostname().lower() != request['node_name']:
        raise _refuse()
    pid = _runtime_pid(request)
    process = _PROC / str(pid)
    before = _identity(process, pid)
    if (process / 'cmdline').read_bytes().split(b'\0') != [*(s.encode() for s in _COMMAND), b'']:
        raise _refuse()
    cgroup = (process / 'cgroup').read_text()
    if request['container_id'] not in cgroup or request['pod_uid'].replace('-', '_') not in cgroup:
        raise _refuse()
    status = (process / 'status').read_text().splitlines()
    namespace_pid = next(line for line in status if line.startswith('NSpid:')).split()[-1]
    mounts = [line.split() for line in (process / 'mountinfo').read_text().splitlines() if line.split()[4] == '/']
    if namespace_pid != '1' or len(mounts) != 1 or 'ro' not in mounts[0][5].split(','):
        raise _refuse()
    _only_operator_in_namespace(pid, before[2])
    executable, stored = _binary(process / 'exe'), _binary(process / 'root/manager')
    if executable != stored or os.readlink(process / 'exe') != '/operator/manager_amd64':
        raise _refuse()
    _only_operator_in_namespace(pid, before[2])
    if _runtime_pid(request) != pid or _identity(process, pid) != before:
        raise _refuse()
    return {'schema_version': 1, 'pod_uid': request['pod_uid'], 'container_id': request['container_id'],
        'node_name': request['node_name'], 'pid': pid, 'started_ticks': before[0],
        'mount_namespace': before[1], 'pid_namespace': before[2], 'executable_device': executable[0],
        'executable_inode': executable[1], 'executable_sha256': executable[2], 'stored_sha256': stored[2],
        'root_readonly': True, 'namespace_pid': 1}


def handle_request(payload: bytes) -> dict[str, object]:
    """Serve one fixed typed request for this exact independently installed source."""
    if not 0 < len(payload) <= 8192:
        raise _refuse()
    def unique(pairs: list[tuple[str, object]]) -> dict[str, object]:
        result: dict[str, object] = {}
        for key, item in pairs:
            if key in result:
                raise _refuse()
            result[key] = item
        return result
    request = json.loads(payload, object_pairs_hook=unique)
    source = Path(__file__).read_bytes()
    if (not isinstance(request, dict) or set(request) != {'schema_version', 'nonce', 'observer_sha256', 'identity'}
            or type(request['schema_version']) is not int or request['schema_version'] != 1
            or not isinstance(request['nonce'], str) or re.fullmatch(r'[0-9a-f]{32}', request['nonce']) is None
            or request['observer_sha256'] != hashlib.sha256(source).hexdigest()):
        raise _refuse()
    identity = _request(request['identity'])
    observation = inspect_cnpg_operator_host(identity)
    if Path(__file__).read_bytes() != source:
        raise _refuse()
    return {**request, 'observation': observation}


def main() -> int:
    import sys

    try:
        if len(sys.argv) != 1 or os.geteuid() != 0:
            raise _refuse()
        result = handle_request(sys.stdin.buffer.read(8193))
        sys.stdout.write(json.dumps(result, sort_keys=True, separators=(',', ':')) + '\n')
        return 0
    except Exception:
        # Do not echo runtime commands, CRI contents, host paths or untrusted input.
        sys.stderr.write('CNPG operator host process observation refused\n')
        return 1


if __name__ == '__main__':
    raise SystemExit(main())
