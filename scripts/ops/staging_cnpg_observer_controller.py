#!/usr/bin/env python3
"""Prepare dedicated CNPG observer trust on OLDLAB1; never contact remote hosts.

Run from an exact root-owned merged release. The explicit inventory contains
independently verified host keys, not keys discovered by this tool. Successful
preparation is not endpoint installation or a passing runtime observation.
"""

from __future__ import annotations

import argparse
import base64
import fcntl
import hashlib
import importlib.util
import ipaddress
import json
import os
import pwd
import re
import socket
import stat
import subprocess
import sys
from pathlib import Path
from typing import Any
from uuid import uuid4

ROOT_UID = 0
ROOT_GID = 0
STATE = Path("/var/lib/loom-cnpg-observer-controller")
CONFIG = Path("/etc/loom/staging-cnpg-observer-ssh-config")
KNOWN_HOSTS = Path("/etc/loom/staging-cnpg-observer-known-hosts")
PUBLIC_KEY = Path("/etc/loom/staging-cnpg-observer.pub")
IDENTITY = Path("/var/lib/loom-staging-rollout/cnpg-observer-ed25519")
_NODES = tuple(f"trt-eai-oldlab-{n}" for n in (3, 4, 5))
_PRIVATE = tuple(ipaddress.ip_network(network) for network in
                 ("10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16", "fc00::/7"))


def _refuse() -> ValueError:
    return ValueError("CNPG observer controller trust is unsafe, changed, or unadmitted")


def _json(value: object) -> bytes:
    return (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode()


def _digest(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _identity(value: os.stat_result) -> tuple[int, ...]:
    return (value.st_dev, value.st_ino, value.st_mode, value.st_uid, value.st_gid,
            value.st_nlink, value.st_size, value.st_mtime_ns, value.st_ctime_ns)


def _read(path: Path, *, uid: int, gid: int, mode: int | None = None) -> bytes:
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC | os.O_NONBLOCK)
    try:
        before = os.fstat(fd)
        if (not stat.S_ISREG(before.st_mode) or before.st_uid != uid or before.st_gid != gid
                or before.st_nlink != 1 or not 0 < before.st_size <= 65536
                or stat.S_IMODE(before.st_mode) & 0o022
                or (mode is not None and stat.S_IMODE(before.st_mode) != mode)):
            raise _refuse()
        with os.fdopen(fd, "rb", closefd=False) as stream:
            payload = stream.read(65537)
        if len(payload) != before.st_size or _identity(before) != _identity(os.fstat(fd)):
            raise _refuse()
        return payload
    finally:
        os.close(fd)


def _root_read(path: Path, mode: int | None = None) -> bytes:
    return _read(path, uid=ROOT_UID, gid=ROOT_GID, mode=mode)


def _record(path: Path) -> dict[str, Any] | None:
    try:
        raw = _root_read(path, 0o600)
    except FileNotFoundError:
        return None
    value = json.loads(raw)
    if not isinstance(value, dict) or _json(value) != raw:
        raise _refuse()
    return value


def _directory(path: Path, *, uid: int, gid: int, mode: int) -> None:
    value = path.lstat()
    if (not stat.S_ISDIR(value.st_mode) or value.st_uid != uid or value.st_gid != gid
            or stat.S_IMODE(value.st_mode) != mode):
        raise _refuse()


def _sync(path: Path) -> None:
    fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _write(path: Path, payload: bytes, *, uid: int, gid: int, mode: int,
           replace_existing: bool = True) -> None:
    try:
        if _read(path, uid=uid, gid=gid, mode=mode) == payload:
            return
        if not replace_existing:
            raise _refuse()
    except FileNotFoundError:
        pass
    temporary = path.with_name("." + path.name + "." + uuid4().hex)
    fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    try:
        with os.fdopen(fd, "wb") as stream:
            os.fchown(stream.fileno(), uid, gid)
            os.fchmod(stream.fileno(), mode)
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        _sync(path.parent)
    finally:
        temporary.unlink(missing_ok=True)


def _root_write(path: Path, value: object) -> None:
    _write(path, _json(value), uid=ROOT_UID, gid=ROOT_GID, mode=0o600)


def _public_key(value: object) -> str:
    if not isinstance(value, str):
        raise _refuse()
    fields = value.split()
    if len(fields) != 2 or fields[0] != "ssh-ed25519":
        raise _refuse()
    try:
        blob = base64.b64decode(fields[1], validate=True)
    except ValueError as exc:
        raise _refuse() from exc
    if len(blob) != 51 or blob[:19] != b"\0\0\0\x0bssh-ed25519\0\0\0\x20":
        raise _refuse()
    canonical = "ssh-ed25519 " + base64.b64encode(blob).decode()
    if canonical != value:
        raise _refuse()
    return canonical


def _inventory(path: Path, expected: str) -> tuple[bytes, bytes]:
    payload = _root_read(path, 0o600)
    if re.fullmatch("[0-9a-f]{64}", expected) is None or _digest(payload) != expected:
        raise _refuse()
    value = json.loads(payload)
    if (not isinstance(value, dict) or set(value) != {"schema_version", "nodes"}
            or type(value["schema_version"]) is not int or value["schema_version"] != 1
            or _json(value) != payload or not isinstance(value["nodes"], list)
            or len(value["nodes"]) != 3):
        raise _refuse()
    config, known = [], []
    endpoints = set()
    for name, node in zip(_NODES, value["nodes"], strict=True):
        if (not isinstance(node, dict) or set(node) != {"node", "address", "port", "host_key"}
                or node["node"] != name or not isinstance(node["address"], str)
                or type(node["port"]) is not int or not 1 <= node["port"] <= 65535):
            raise _refuse()
        address = ipaddress.ip_address(node["address"])
        if (getattr(address, "scope_id", None) is not None
                or str(address) != node["address"] or not any(address in net for net in _PRIVATE)):
            raise _refuse()
        endpoint = (str(address), node["port"])
        if endpoint in endpoints:
            raise _refuse()
        endpoints.add(endpoint)
        key = _public_key(node["host_key"])
        config.append(f"Host {name}\n  HostName {address}\n  Port {node['port']}\n")
        known.append(f"{name} {key}\n")
    return "".join(config).encode(), "".join(known).encode()


def _host_installer() -> Any:
    # Same-directory trusted source; never import a user-controlled module path.
    dependency = importlib.util.spec_from_file_location(
        "_loom_cnpg_sealed_source", Path(__file__).with_name("staging_rollout_sealed_source.py"))
    if dependency is None or dependency.loader is None:
        raise _refuse()
    sealed = importlib.util.module_from_spec(dependency)
    sys.modules[dependency.name] = sealed
    dependency.loader.exec_module(sealed)
    # The existing installer supports package and direct-script imports. Bind
    # both names to its exact sibling without broadening isolated sys.path.
    sys.modules["scripts.ops.staging_rollout_sealed_source"] = sealed
    sys.modules["staging_rollout_sealed_source"] = sealed
    spec = importlib.util.spec_from_file_location(
        "_loom_cnpg_host_installer", Path(__file__).with_name("staging_rollout_host.py"))
    if spec is None or spec.loader is None:
        raise _refuse()
    host = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = host
    spec.loader.exec_module(host)
    return host


def _require_authority(inventory_file: Path) -> tuple[int, int, str]:
    if os.geteuid() != ROOT_UID or socket.gethostname() != "TRT-EAI-OLDLAB-1":
        raise _refuse()
    host = _host_installer()
    system = host.HostSystem(host.SubprocessRunner())
    source = system.validate_invocation_checkout()
    system.validate_install_record_authority(allow_absent=False)
    record = host.LocalFilesystem().load_install_record()
    if record is None or not isinstance(record.get("source_sha"), str):
        raise _refuse()
    system.validate_invocation_dev_head(source, record["source_sha"])
    if not inventory_file.is_absolute():
        raise _refuse()
    for path in (inventory_file, STATE, CONFIG, KNOWN_HOSTS, PUBLIC_KEY, IDENTITY.parent):
        host._validate_root_authority_parent_chain(path)
    host._safe_root_executable(Path("/usr/bin/ssh-keygen"), label="observer key generator")
    service = pwd.getpwnam("loom-rollout")
    return service.pw_uid, service.pw_gid, source


def _key() -> tuple[bytes, bytes]:
    seed = STATE / "identity"
    env = {"PATH": "/usr/bin:/bin", "LANG": "C", "LC_ALL": "C"}
    if not seed.exists() and not seed.is_symlink():
        if (STATE / "identity.pub").exists() or (STATE / "identity.pub").is_symlink():
            raise _refuse()
        subprocess.run(["/usr/bin/ssh-keygen", "-q", "-t", "ed25519", "-N", "",
                        "-C", "loom-staging-cnpg-observer", "-f", str(seed)],
                       env=env, capture_output=True, check=True, timeout=30)
    private = _root_read(seed, 0o600)
    result = subprocess.run(["/usr/bin/ssh-keygen", "-y", "-f", str(seed)],
                            env=env, capture_output=True, check=True, timeout=30)
    public = (_public_key(" ".join(result.stdout.decode("ascii").split()[:2])) + "\n").encode()
    if _root_read(seed, 0o600) != private:
        raise _refuse()
    # ssh-keygen completion alone does not make the original key crash-durable.
    # Persist it before any service-readable copy or completion record is exposed.
    fd = os.open(seed, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)
    _sync(STATE)
    return private, public


def prepare_controller(*, inventory_file: Path, inventory_sha256: str) -> dict[str, Any]:
    uid, gid, source = _require_authority(inventory_file)
    config, known = _inventory(inventory_file, inventory_sha256)
    for path in (CONFIG.parent, KNOWN_HOSTS.parent, PUBLIC_KEY.parent):
        _directory(path, uid=ROOT_UID, gid=ROOT_GID, mode=0o755)
    _directory(IDENTITY.parent, uid=uid, gid=gid, mode=0o700)
    outputs = (IDENTITY, CONFIG, KNOWN_HOSTS, PUBLIC_KEY)
    if not STATE.exists() and not STATE.is_symlink():
        if any(path.exists() or path.is_symlink() for path in outputs):
            raise _refuse()
        STATE.mkdir(mode=0o700)
        _sync(STATE.parent)
    _directory(STATE, uid=ROOT_UID, gid=ROOT_GID, mode=0o700)
    lock = os.open(STATE / "lock", os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600)
    try:
        meta = os.fstat(lock)
        if (not stat.S_ISREG(meta.st_mode) or meta.st_uid != ROOT_UID or meta.st_gid != ROOT_GID
                or meta.st_nlink != 1 or stat.S_IMODE(meta.st_mode) != 0o600):
            raise _refuse()
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        current, pending = _record(STATE / "current.json"), _record(STATE / "pending.json")
        if pending is not None and (
                set(pending) != {"schema_version", "inventory_sha256", "source_sha"}
                or type(pending["schema_version"]) is not int or pending["schema_version"] != 1
                or not isinstance(pending["source_sha"], str)
                or re.fullmatch("[0-9a-f]{40}", pending["source_sha"]) is None):
            raise _refuse()
        if current is None and pending is None:
            if any(path.exists() or path.is_symlink() for path in (*outputs, STATE / "identity")):
                raise _refuse()
            pending = {"schema_version": 1, "inventory_sha256": inventory_sha256, "source_sha": source}
            _root_write(STATE / "pending.json", pending)
        binding = current if current is not None else pending
        if (binding is None or binding.get("inventory_sha256") != inventory_sha256
                or (pending is not None and pending.get("inventory_sha256") != inventory_sha256)):
            raise _refuse()
        if (not (STATE / "identity").exists()
                and (current is not None or any(path.exists() or path.is_symlink() for path in outputs))):
            raise _refuse()
        private, public = _key()
        desired = {IDENTITY: (private, uid, gid, 0o600), CONFIG: (config, ROOT_UID, ROOT_GID, 0o444),
                   KNOWN_HOSTS: (known, ROOT_UID, ROOT_GID, 0o444),
                   PUBLIC_KEY: (public, ROOT_UID, ROOT_GID, 0o444)}
        result = {"schema_version": 1, "status": "prepared-not-observed",
                  "inventory_sha256": inventory_sha256, "public_key_sha256": _digest(public),
                  "file_sha256": {str(path): _digest(data[0]) for path, data in desired.items()}}
        if current is not None and current != result:
            raise _refuse()
        for path, (data, owner, group, mode) in desired.items():
            try:
                actual = _read(path, uid=owner, gid=group, mode=mode)
            except FileNotFoundError:
                if current is not None:
                    raise _refuse() from None
            else:
                if actual != data:
                    raise _refuse()
        if _inventory(inventory_file, inventory_sha256) != (config, known):
            raise _refuse()
        for path, (data, owner, group, mode) in desired.items():
            _write(path, data, uid=owner, gid=group, mode=mode, replace_existing=False)
        for path, (data, owner, group, mode) in desired.items():
            if _read(path, uid=owner, gid=group, mode=mode) != data:
                raise _refuse()
        _root_write(STATE / "current.json", result)
        (STATE / "pending.json").unlink(missing_ok=True)
        _sync(STATE)
        return result
    finally:
        os.close(lock)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--inventory-file", type=Path, required=True)
    parser.add_argument("--inventory-sha256", required=True)
    args = parser.parse_args()
    os.umask(0o077)
    try:
        result = prepare_controller(inventory_file=args.inventory_file, inventory_sha256=args.inventory_sha256)
    except (ValueError, OSError, RuntimeError, subprocess.SubprocessError):
        print(json.dumps({"error": str(_refuse())}), file=sys.stderr)
        return 1
    print(_json(result).decode(), end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
