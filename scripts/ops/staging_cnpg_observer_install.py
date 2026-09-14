#!/usr/bin/env python3
"""Install only the dedicated read-only CNPG observer on an admitted K3s node.

Invoke as root from the trusted, protected release installation, with the exact
release observer source/digest and the rollout controller's public observation
key. This does not authorize an unmerged release or install any workload/SQL
mutation capability. Existing unrelated accounts and files are never adopted.
"""

from __future__ import annotations

import argparse
import base64
import fcntl
import grp
import hashlib
import json
import os
import pwd
import re
import shlex
import socket
import stat
import subprocess
from pathlib import Path
from uuid import uuid4

ROOT_UID = 0
ACCOUNT = 'loom-cnpg-observer'
STATE = Path('/var/lib/loom-cnpg-observer-install')
ACCOUNT_HOME = Path('/var/lib/loom-cnpg-observer')
SUDOERS = Path('/etc/sudoers.d/loom-cnpg-observer')
WRAPPER = Path('/usr/local/libexec/loom-staging-cnpg-observer')
_NODES = frozenset({'trt-eai-oldlab-3', 'trt-eai-oldlab-4', 'trt-eai-oldlab-5'})


def _file_identity(value: os.stat_result) -> tuple[int, ...]:
    # Reads may update access time; it is not evidence of a changed executable.
    return (value.st_dev, value.st_ino, value.st_mode, value.st_uid, value.st_gid,
            value.st_nlink, value.st_size, value.st_mtime_ns, value.st_ctime_ns)


def _refuse() -> ValueError:
    return ValueError('CNPG observer installation authority changed or is unsupported')


def _hash(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _json(value: object) -> bytes:
    return (json.dumps(value, sort_keys=True, separators=(',', ':')) + '\n').encode()


def _read(path: Path, *, limit: int = 65536) -> bytes:
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC)
    try:
        before = os.fstat(fd)
        if (not stat.S_ISREG(before.st_mode) or before.st_uid != ROOT_UID
                or before.st_nlink != 1 or stat.S_IMODE(before.st_mode) & 0o022
                or not 0 < before.st_size <= limit):
            raise _refuse()
        with os.fdopen(fd, 'rb', closefd=False) as stream:
            payload = stream.read(limit + 1)
        after = os.fstat(fd)
        if (len(payload) != before.st_size or _file_identity(before) != _file_identity(after)):
            raise _refuse()
        return payload
    finally:
        os.close(fd)


def _record(path: Path) -> dict[str, object] | None:
    try:
        payload = _read(path)
    except FileNotFoundError:
        return None
    value = json.loads(payload)
    if not isinstance(value, dict) or payload != _json(value):
        raise _refuse()
    return value


def _directory(path: Path, mode: int = 0o700) -> None:
    try:
        path.mkdir(mode=mode)
        path.chmod(mode)
    except FileExistsError:
        pass
    metadata = path.lstat()
    if (not stat.S_ISDIR(metadata.st_mode) or metadata.st_uid != ROOT_UID
            or stat.S_IMODE(metadata.st_mode) & 0o022):
        raise _refuse()


def _sync_directory(path: Path) -> None:
    fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _write(path: Path, payload: bytes, mode: int) -> None:
    try:
        if _read(path) == payload and stat.S_IMODE(path.lstat().st_mode) == mode:
            return
    except FileNotFoundError:
        pass
    temporary = path.with_name('.' + path.name + '.' + uuid4().hex)
    fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, mode)
    try:
        with os.fdopen(fd, 'wb') as stream:
            os.fchmod(stream.fileno(), mode)
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        _sync_directory(path.parent)
    finally:
        temporary.unlink(missing_ok=True)


def _public_key(payload: bytes) -> str:
    try:
        fields = payload.decode('ascii').strip().split()
        if len(fields) not in {2, 3} or fields[0] != 'ssh-ed25519':
            raise _refuse()
        blob = base64.b64decode(fields[1], validate=True)
        if len(blob) != 51 or blob[:19] != b'\0\0\0\x0bssh-ed25519\0\0\0\x20':
            raise _refuse()
        return 'ssh-ed25519 ' + base64.b64encode(blob).decode('ascii')
    except (UnicodeError, ValueError) as exc:
        raise _refuse() from exc


def _account() -> pwd.struct_passwd | None:
    try:
        return pwd.getpwnam(ACCOUNT)
    except KeyError:
        return None


def _require_account(uid: int) -> bool:
    observed = _account()
    if observed is None:
        return False
    if (observed.pw_uid != uid or observed.pw_gid != uid
            or observed.pw_dir != str(ACCOUNT_HOME) or observed.pw_shell != '/bin/sh'):
        raise _refuse()
    return True


def _files(source_sha: str, key: str) -> dict[Path, tuple[bytes, int]]:
    source = STATE / 'candidates' / source_sha / 'observer.py'
    wrapper = ('#!/bin/sh\nset -eu\n[ "$#" -eq 0 ]\nexec /usr/bin/python3 -I -B '
               + shlex.quote(str(source)) + '\n').encode()
    if any(c.isspace() or c in '"\\' for c in str(WRAPPER)):
        raise _refuse()
    authorized = ('restrict,command="/usr/bin/sudo -n ' + str(WRAPPER) + '" '
                  + key + ' loom-staging-cnpg-observer\n').encode()
    sudoers = (ACCOUNT + ' ALL=(root) NOPASSWD:NOSETENV: ' + str(WRAPPER) + ' ""\n').encode()
    return {WRAPPER: (wrapper, 0o755), SUDOERS: (sudoers, 0o440),
            ACCOUNT_HOME / '.ssh/authorized_keys': (authorized, 0o644)}


def _validate_outputs(files: dict[Path, tuple[bytes, int]], *, current: dict[str, object] | None,
                      pending: dict[str, object] | None) -> None:
    old = {} if current is None else current.get('file_sha256')
    if not isinstance(old, dict) or (current is not None and set(old) != {str(path) for path in files}):
        raise _refuse()
    for path, (payload, mode) in files.items():
        try:
            actual = _hash(_read(path))
        except FileNotFoundError:
            if str(path) in old:
                raise _refuse() from None
            continue
        allowed = {old[str(path)]} if str(path) in old else set()
        if pending is not None:
            allowed.add(_hash(payload))
        if actual not in allowed or stat.S_IMODE(path.lstat().st_mode) != mode:
            raise _refuse()


def _require_installed_directories() -> None:
    for path in (ACCOUNT_HOME, ACCOUNT_HOME / '.ssh'):
        metadata = path.lstat()
        if (not stat.S_ISDIR(metadata.st_mode) or metadata.st_uid != ROOT_UID
                or stat.S_IMODE(metadata.st_mode) != 0o755):
            raise _refuse()


def install_node(*, source: Path, expected_sha256: str, public_key: Path) -> dict[str, object]:
    if (os.geteuid() != ROOT_UID or socket.gethostname().lower() not in _NODES
            or re.fullmatch('[0-9a-f]{64}', expected_sha256) is None):
        raise _refuse()
    payload = _read(source)
    if _hash(payload) != expected_sha256:
        raise _refuse()
    key = _public_key(_read(public_key, limit=8192))
    files = _files(expected_sha256, key)
    _directory(STATE)
    lock = os.open(STATE / 'install.lock', os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW | os.O_CLOEXEC, 0o600)
    try:
        metadata = os.fstat(lock)
        if (not stat.S_ISREG(metadata.st_mode) or metadata.st_uid != ROOT_UID
                or metadata.st_nlink != 1 or stat.S_IMODE(metadata.st_mode) != 0o600):
            raise _refuse()
        fcntl.flock(lock, fcntl.LOCK_EX)
        current = _record(STATE / 'current.json')
        pending = _record(STATE / 'pending.json')
        account = _record(STATE / 'account.json')
        if account is None:
            if _account() is not None or ACCOUNT_HOME.exists():
                raise _refuse()
            used = {user.pw_uid for user in pwd.getpwall()} | {group.gr_gid for group in grp.getgrall()}
            uid = next((n for n in range(999, 949, -1) if n not in used), None)
            if uid is None:
                raise _refuse()
            account = {'schema_version': 1, 'account': ACCOUNT, 'uid': uid, 'home': str(ACCOUNT_HOME)}
        if (set(account) != {'schema_version', 'account', 'uid', 'home'} or account['schema_version'] != 1
                or account['account'] != ACCOUNT or type(account['uid']) is not int or not 950 <= account['uid'] <= 999
                or account['home'] != str(ACCOUNT_HOME)):
            raise _refuse()
        uid = account['uid']
        _require_account(uid)
        target: dict[str, object] = {'schema_version': 1, 'observer_sha256': expected_sha256,
            'public_key_sha256': _hash(key.encode()), 'account_uid': uid,
            'node': socket.gethostname().lower(), 'file_sha256': {str(path): _hash(data) for path, (data, _) in files.items()}}
        if pending is not None and pending.get('target') != target:
            raise _refuse()
        _validate_outputs(files, current=current, pending=pending)
        if current == target and pending is None:
            if not _require_account(uid):
                raise _refuse()
            _require_installed_directories()
            if _read(STATE / 'candidates' / expected_sha256 / 'observer.py') != payload:
                raise _refuse()
            return target
        _directory(STATE / 'candidates')
        candidate = STATE / 'candidates' / expected_sha256
        _directory(candidate)
        copied_source = candidate / 'observer.py'
        if copied_source.exists() and _read(copied_source) != payload:
            raise _refuse()
        _write(copied_source, payload, 0o444)
        _write(candidate / 'sudoers', files[SUDOERS][0], 0o440)
        subprocess.run(['/usr/sbin/visudo', '-cf', str(candidate / 'sudoers')], check=True, capture_output=True, timeout=30)
        if pending is None:
            pending = {'schema_version': 1, 'target': target, 'previous': current}
            _write(STATE / 'pending.json', _json(pending), 0o600)
        _write(STATE / 'account.json', _json(account), 0o600)
        if not _require_account(uid):
            subprocess.run(['/usr/sbin/useradd', '--system', '--uid', str(uid), '--user-group',
                '--home-dir', str(ACCOUNT_HOME), '--no-create-home', '--shell', '/bin/sh', '--password', '*', ACCOUNT],
                check=True, capture_output=True, timeout=30)
            if not _require_account(uid):
                raise _refuse()
        _directory(ACCOUNT_HOME, 0o755)
        _directory(ACCOUNT_HOME / '.ssh', 0o755)
        _directory(WRAPPER.parent, 0o755)
        _directory(SUDOERS.parent, 0o755)
        for path, (data, mode) in files.items():
            _write(path, data, mode)
        _validate_outputs(files, current=target, pending=None)
        _require_installed_directories()
        # Candidate material and transition evidence persist for review/rollback.
        transition = STATE / 'candidates' / expected_sha256 / ('transition-' + _hash(_json(pending)) + '.json')
        _write(transition, _json(pending), 0o600)
        _write(STATE / 'current.json', _json(target), 0o600)
        (STATE / 'pending.json').unlink()
        _sync_directory(STATE)
        return target
    finally:
        os.close(lock)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--observer-source', required=True, type=Path)
    parser.add_argument('--observer-sha256', required=True)
    parser.add_argument('--public-key-file', required=True, type=Path)
    args = parser.parse_args()
    result = install_node(source=args.observer_source, expected_sha256=args.observer_sha256,
                          public_key=args.public_key_file)
    print(_json(result).decode(), end='')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
