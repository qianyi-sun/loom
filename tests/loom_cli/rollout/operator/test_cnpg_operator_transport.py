"""The installed observation channel cannot select arbitrary commands or reuse replies."""

import json
import os
import stat
from types import SimpleNamespace

import pytest

from tests.loom_cli.rollout.operator.test_cnpg_operator_admission import Runner, _pod


@pytest.mark.parametrize('drift', [None, 'nonce', 'source', 'identity', 'key-mode', 'config-owner', 'config-changed', 'unknown-node'])
def test_fixed_host_transport_binds_program_request_and_trust_files(monkeypatch, drift):
    from loom_cli.rollout.operator import protected_cnpg_operator_transport as module
    from loom_cli.rollout.operator.protected_cnpg_operator_admission import select_cnpg_operator

    identity = select_cnpg_operator([_pod()])
    inputs = []
    changes = [False]
    def trusted(path, **kwargs):
        private = path == module._IDENTITY
        source = path == module._SOURCE
        metadata = SimpleNamespace(st_uid=os.geteuid() if private or source else 0,
            st_mode=stat.S_IFREG | (0o600 if private else 0o644))
        if drift == 'key-mode' and private:
            metadata.st_mode = stat.S_IFREG | 0o644
        if drift == 'config-owner' and path == module._CONFIG:
            metadata.st_uid = 9999
        payload = b'fixed admitted input'
        if drift == 'config-changed' and changes[0] and path == module._CONFIG:
            payload = b'changed config'
        return SimpleNamespace(payload=payload, metadata=metadata,
            metadata_fingerprint='1' * 64, acl_fingerprint='2' * 64)
    monkeypatch.setattr(module, 'read_trusted_file', trusted)
    def run(argv, **kwargs):
        inputs.append((argv, kwargs))
        assert argv[0] == '/usr/bin/ssh' and argv[-1] == identity.node_name
        assert '-T' in argv and 'StrictHostKeyChecking=yes' in argv
        assert 'IdentitiesOnly=yes' in argv and 'ForwardAgent=no' in argv
        assert argv[argv.index('-F') + 1] == str(module._CONFIG)
        assert argv[argv.index('-i') + 1] == str(module._IDENTITY)
        assert 'UserKnownHostsFile=' + str(module._KNOWN_HOSTS) in argv
        assert kwargs['env'] == {'PATH': '/usr/bin:/bin', 'LANG': 'C', 'LC_ALL': 'C'}
        request = json.loads(kwargs['input'])
        assert request['identity']['container_id'] == identity.container_id
        reply = {**request, 'observation': Runner().inspect_staging_cnpg_operator(identity)}
        if drift == 'nonce':
            reply['nonce'] = '0' * 32
        elif drift == 'source':
            reply['observer_sha256'] = '0' * 64
        elif drift == 'identity':
            reply['identity']['container_id'] = 'b' * 64
        changes[0] = True
        return SimpleNamespace(returncode=0, stdout=json.dumps(reply).encode())
    monkeypatch.setattr(module.subprocess, 'run', run)
    if drift == 'unknown-node':
        object.__setattr__(identity, 'node_name', 'trt-eai-oldlab-2')
    if drift:
        with pytest.raises(ValueError, match='CNPG operator'):
            module.inspect_staging_cnpg_operator(identity)
        if drift in {'key-mode', 'config-owner', 'unknown-node'}:
            assert not inputs
    else:
        first = module.inspect_staging_cnpg_operator(identity)
        assert first['pod_uid'] == identity.pod_uid
        module.inspect_staging_cnpg_operator(identity)
        assert json.loads(inputs[0][1]['input'])['nonce'] != json.loads(inputs[1][1]['input'])['nonce']
