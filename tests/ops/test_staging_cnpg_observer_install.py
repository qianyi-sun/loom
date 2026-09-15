"""Root endpoint installation preserves unrelated files and recovers lost replies."""

import base64
import hashlib
import os
import subprocess
from types import SimpleNamespace

import pytest


def _context(tmp_path, monkeypatch):
    from scripts.ops import staging_cnpg_observer_install as module

    for key, name in (('STATE', 'state'), ('ACCOUNT_HOME', 'home'), ('SUDOERS', 'sudoers'), ('WRAPPER', 'observer')):
        monkeypatch.setattr(module, key, tmp_path / name)
    monkeypatch.setattr(module, 'ROOT_UID', os.getuid())
    monkeypatch.setattr(module.socket, 'gethostname', lambda: 'TRT-EAI-OLDLAB-4')
    users = {}
    def user(name):
        if name not in users:
            raise KeyError(name)
        return users[name]
    monkeypatch.setattr(module.pwd, 'getpwnam', user)
    monkeypatch.setattr(module.pwd, 'getpwall', lambda: list(users.values()))
    monkeypatch.setattr(module.grp, 'getgrall', lambda: [])
    calls = []
    lost_ack = [False]
    def run(argv, **kwargs):
        calls.append(argv)
        if argv[0] == '/usr/sbin/useradd':
            uid = int(argv[argv.index('--uid') + 1])
            users[module.ACCOUNT] = SimpleNamespace(pw_uid=uid, pw_gid=uid,
                pw_dir=str(module.ACCOUNT_HOME), pw_shell='/bin/sh')
            if lost_ack[0]:
                lost_ack[0] = False
                raise subprocess.TimeoutExpired(argv[0], 30)
        else:
            assert argv[:2] == ['/usr/sbin/visudo', '-cf']
        return SimpleNamespace(returncode=0)
    monkeypatch.setattr(module.subprocess, 'run', run)
    source = tmp_path / 'source.py'
    source.write_bytes(b'"""Trusted fixture observer."""\n')
    source.chmod(0o600)
    blob = b'\0\0\0\x0bssh-ed25519\0\0\0\x20' + b'a' * 32
    key = tmp_path / 'key.pub'
    key.write_text('ssh-ed25519 ' + base64.b64encode(blob).decode() + ' fixture\n')
    key.chmod(0o600)
    arguments = {'source': source, 'expected_sha256': hashlib.sha256(source.read_bytes()).hexdigest(), 'public_key': key}
    return module, arguments, users, calls, lost_ack


@pytest.mark.parametrize('interruption', [None, 'account', 'wrapper'])
def test_install_endpoint_is_idempotent_and_recovers_its_original_interrupted_write(tmp_path, monkeypatch, interruption):
    module, arguments, users, calls, lost_ack = _context(tmp_path, monkeypatch)
    if interruption == 'account':
        lost_ack[0] = True
    elif interruption == 'wrapper':
        replace = module.os.replace
        interrupted = [False]
        def ambiguous(source, destination, *args, **kwargs):
            result = replace(source, destination, *args, **kwargs)
            if destination == module.WRAPPER and not interrupted[0]:
                interrupted[0] = True
                raise OSError('lost wrapper publication acknowledgement')
            return result
        monkeypatch.setattr(module.os, 'replace', ambiguous)
    if interruption:
        with pytest.raises((OSError, subprocess.TimeoutExpired)):
            module.install_node(**arguments)
    result = module.install_node(**arguments)
    assert result['observer_sha256'] == arguments['expected_sha256']
    assert users[module.ACCOUNT].pw_dir == str(module.ACCOUNT_HOME)
    assert sum(call[0] == '/usr/sbin/useradd' for call in calls) == 1
    wrapper = module.WRAPPER.read_bytes()
    authorized = (module.ACCOUNT_HOME / '.ssh/authorized_keys').read_text()
    assert authorized.startswith('restrict,command="/usr/bin/sudo -n ' + str(module.WRAPPER) + '" ssh-ed25519 ')
    assert b'/usr/bin/python3 -I -B ' in wrapper
    assert arguments['expected_sha256'].encode() in wrapper
    assert module.install_node(**arguments) == result
    assert module.WRAPPER.read_bytes() == wrapper
    assert not (module.STATE / 'pending.json').exists()


@pytest.mark.parametrize('foreign', ['wrapper', 'account', 'source', 'key'])
def test_install_refuses_foreign_authority_before_account_mutation(tmp_path, monkeypatch, foreign):
    module, arguments, users, calls, _ = _context(tmp_path, monkeypatch)
    if foreign == 'wrapper':
        module.WRAPPER.write_bytes(b'foreign root helper')
    elif foreign == 'account':
        users[module.ACCOUNT] = SimpleNamespace(pw_uid=500, pw_gid=500, pw_dir='/foreign', pw_shell='/bin/sh')
    elif foreign == 'source':
        arguments['expected_sha256'] = '0' * 64
    else:
        arguments['public_key'].write_text('ssh-ed25519 invalid\ncommand=foreign')
    with pytest.raises((ValueError, RuntimeError), match='CNPG observer'):
        module.install_node(**arguments)
    assert not any(call[0] == '/usr/sbin/useradd' for call in calls)
    if foreign == 'wrapper':
        assert module.WRAPPER.read_bytes() == b'foreign root helper'


def test_endpoint_upgrade_keeps_candidate_rollback_material_and_refuses_local_drift(tmp_path, monkeypatch):
    module, arguments, _users, calls, _ = _context(tmp_path, monkeypatch)
    first = module.install_node(**arguments)
    old_wrapper = module.WRAPPER.read_bytes()
    arguments['source'].write_bytes(b'"""Next trusted fixture observer."""\n')
    arguments['expected_sha256'] = hashlib.sha256(arguments['source'].read_bytes()).hexdigest()
    second = module.install_node(**arguments)
    assert first['observer_sha256'] != second['observer_sha256']
    assert (module.STATE / 'candidates' / first['observer_sha256'] / 'observer.py').is_file()
    assert old_wrapper != module.WRAPPER.read_bytes()
    module.WRAPPER.write_bytes(b'foreign replacement')
    with pytest.raises((ValueError, RuntimeError), match='CNPG observer'):
        module.install_node(**arguments)
    assert module.WRAPPER.read_bytes() == b'foreign replacement'
    assert sum(call[0] == '/usr/sbin/useradd' for call in calls) == 1


@pytest.mark.parametrize('drift', ['mode', 'home', 'source-copy'])
def test_successful_replay_rechecks_permissions_and_immutable_observer_bytes(tmp_path, monkeypatch, drift):
    module, arguments, _users, calls, _ = _context(tmp_path, monkeypatch)
    receipt = module.install_node(**arguments)
    if drift == 'mode':
        module.WRAPPER.chmod(0o600)
    elif drift == 'home':
        module.ACCOUNT_HOME.chmod(0o700)
    else:
        source = module.STATE / 'candidates' / receipt['observer_sha256'] / 'observer.py'
        source.chmod(0o600)
        source.write_bytes(b'foreign executable')
    before = len(calls)
    with pytest.raises(ValueError, match='CNPG observer'):
        module.install_node(**arguments)
    assert len(calls) == before


def test_endpoint_public_key_remains_readable_under_private_installer_umask(tmp_path, monkeypatch):
    module, arguments, *_ = _context(tmp_path, monkeypatch)
    old_umask = os.umask(0o077)
    try:
        module.install_node(**arguments)
    finally:
        os.umask(old_umask)
    assert module.ACCOUNT_HOME.stat().st_mode & 0o777 == 0o755
    assert (module.ACCOUNT_HOME / '.ssh').stat().st_mode & 0o777 == 0o755
