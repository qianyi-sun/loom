"""Dedicated controller trust preparation, without remote access or key rotation."""

from __future__ import annotations

import base64
import hashlib
import json
import os
import stat
import subprocess
import sys
from types import SimpleNamespace

import pytest


def _json(value):
    return (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode()


def _context(tmp_path, monkeypatch):
    from scripts.ops import staging_cnpg_observer_controller as module

    config = tmp_path / "etc"
    service = tmp_path / "service"
    config.mkdir(mode=0o755)
    service.mkdir(mode=0o700)
    for name, path in {
        "STATE": tmp_path / "state",
        "CONFIG": config / "config",
        "KNOWN_HOSTS": config / "known_hosts",
        "PUBLIC_KEY": config / "key.pub",
        "IDENTITY": service / "key",
    }.items():
        monkeypatch.setattr(module, name, path)
    monkeypatch.setattr(module, "ROOT_UID", os.getuid())
    monkeypatch.setattr(module, "ROOT_GID", os.getgid())
    monkeypatch.setattr(module, "_require_authority", lambda path: (os.getuid(), os.getgid(), "a" * 40))
    nodes = []
    for number in (3, 4, 5):
        blob = b"\0\0\0\x0bssh-ed25519\0\0\0\x20" + bytes([number]) * 32
        nodes.append({"node": f"trt-eai-oldlab-{number}", "address": f"192.168.50.{number}",
                      "port": 22, "host_key": "ssh-ed25519 " + base64.b64encode(blob).decode()})
    inventory = tmp_path / "inventory.json"
    inventory.write_bytes(_json({"schema_version": 1, "nodes": nodes}))
    inventory.chmod(0o600)
    arguments = {"inventory_file": inventory,
                 "inventory_sha256": hashlib.sha256(inventory.read_bytes()).hexdigest()}
    return module, arguments


def test_prepare_real_key_is_stable_and_only_publishes_fixed_observer_trust(tmp_path, monkeypatch):
    module, args = _context(tmp_path, monkeypatch)
    first = module.prepare_controller(**args)
    key = module.IDENTITY.read_bytes()
    identities = {path: path.stat().st_ino for path in
                  (module.IDENTITY, module.CONFIG, module.KNOWN_HOSTS, module.PUBLIC_KEY)}
    assert module.prepare_controller(**args) == first
    assert identities == {path: path.stat().st_ino for path in identities}
    assert module.IDENTITY.read_bytes() == key
    derived = subprocess.run(["/usr/bin/ssh-keygen", "-y", "-f", str(module.IDENTITY)],
                             capture_output=True, check=True).stdout.split()[:2]
    assert module.PUBLIC_KEY.read_bytes().split()[:2] == derived
    assert stat.S_IMODE(module.IDENTITY.stat().st_mode) == 0o600
    for path in (module.CONFIG, module.KNOWN_HOSTS, module.PUBLIC_KEY):
        assert stat.S_IMODE(path.stat().st_mode) == 0o444
    assert module.CONFIG.read_text() == "".join(
        f"Host trt-eai-oldlab-{n}\n  HostName 192.168.50.{n}\n  Port 22\n" for n in (3, 4, 5))
    assert [line.split()[0] for line in module.KNOWN_HOSTS.read_text().splitlines()] == [
        f"trt-eai-oldlab-{n}" for n in (3, 4, 5)]
    assert first["status"] == "prepared-not-observed"
    assert "PRIVATE KEY" not in json.dumps(first)


@pytest.mark.parametrize("change", ["missing", "oldlab2", "duplicate", "public_ip", "bool_port",
                                   "bad_key", "extra_field", "wrong_digest", "scoped_ipv6",
                                   "newline_ipv6"])
def test_invalid_inventory_refuses_before_any_key_or_state_creation(tmp_path, monkeypatch, change):
    module, args = _context(tmp_path, monkeypatch)
    document = json.loads(args["inventory_file"].read_bytes())
    if change == "missing":
        document["nodes"].pop()
    elif change == "oldlab2":
        document["nodes"][0]["node"] = "trt-eai-oldlab-2"
    elif change == "duplicate":
        document["nodes"][1] = document["nodes"][0]
    elif change == "public_ip":
        document["nodes"][0]["address"] = "8.8.8.8"
    elif change == "scoped_ipv6":
        document["nodes"][0]["address"] = "fc00::1%eth0"
    elif change == "newline_ipv6":
        document["nodes"][0]["address"] = "fc00::1%eth0\n  Unexpected setting"
    elif change == "bool_port":
        document["nodes"][0]["port"] = True
    elif change == "bad_key":
        document["nodes"][0]["host_key"] = "ssh-ed25519 invalid"
    elif change == "extra_field":
        document["nodes"][0]["ProxyCommand"] = "unexpected"
    args["inventory_file"].write_bytes(_json(document))
    args["inventory_sha256"] = ("0" * 64 if change == "wrong_digest" else
                                hashlib.sha256(args["inventory_file"].read_bytes()).hexdigest())
    with pytest.raises(ValueError):
        module.prepare_controller(**args)
    assert not module.STATE.exists()
    assert not module.IDENTITY.exists()


@pytest.mark.parametrize("target", ["CONFIG", "KNOWN_HOSTS", "PUBLIC_KEY", "IDENTITY"])
def test_untracked_existing_destination_is_not_adopted(tmp_path, monkeypatch, target):
    module, args = _context(tmp_path, monkeypatch)
    path = getattr(module, target)
    path.write_bytes(b"foreign material\n")
    path.chmod(0o600)
    with pytest.raises(ValueError):
        module.prepare_controller(**args)
    assert path.read_bytes() == b"foreign material\n"
    assert not module.STATE.exists()


@pytest.mark.parametrize("change", ["bytes", "mode", "symlink", "missing"])
def test_installed_key_drift_is_never_repaired_by_rotation(tmp_path, monkeypatch, change):
    module, args = _context(tmp_path, monkeypatch)
    module.prepare_controller(**args)
    original = module.IDENTITY.read_bytes()
    if change == "bytes":
        module.IDENTITY.write_bytes(b"changed\n")
    elif change == "mode":
        module.IDENTITY.chmod(0o644)
    else:
        module.IDENTITY.unlink()
        if change == "symlink":
            target = tmp_path / "foreign"
            target.write_bytes(original)
            module.IDENTITY.symlink_to(target)
    with pytest.raises((ValueError, OSError)):
        module.prepare_controller(**args)


@pytest.mark.parametrize("target", ["IDENTITY", "CONFIG", "KNOWN_HOSTS", "PUBLIC_KEY"])
def test_lost_publish_ack_recovers_original_key_and_exact_inventory(tmp_path, monkeypatch, target):
    module, args = _context(tmp_path, monkeypatch)
    replace = module.os.replace
    failed = False

    def interrupted(source, destination):
        nonlocal failed
        replace(source, destination)
        if destination == getattr(module, target) and not failed:
            failed = True
            raise OSError("lost write acknowledgement")

    monkeypatch.setattr(module.os, "replace", interrupted)
    with pytest.raises(OSError, match="lost write"):
        module.prepare_controller(**args)
    key = (module.STATE / "identity").read_bytes()
    assert module.prepare_controller(**args)["status"] == "prepared-not-observed"
    assert module.IDENTITY.read_bytes() == key
    assert not (module.STATE / "pending.json").exists()


def test_output_change_between_validation_and_write_is_not_overwritten(tmp_path, monkeypatch):
    module, args = _context(tmp_path, monkeypatch)
    module.prepare_controller(**args)
    write = module._write

    def race(path, payload, **kwargs):
        if path == module.IDENTITY:
            path.write_bytes(b"concurrent change\n")
        write(path, payload, **kwargs)

    monkeypatch.setattr(module, "_write", race)
    with pytest.raises(ValueError):
        module.prepare_controller(**args)
    assert module.IDENTITY.read_bytes() == b"concurrent change\n"


def test_orphan_public_key_symlink_is_rejected_before_keygen(tmp_path, monkeypatch):
    module, args = _context(tmp_path, monkeypatch)
    foreign = tmp_path / "foreign"
    foreign.write_bytes(b"preserve\n")
    write = module._root_write

    def orphan(path, value):
        write(path, value)
        if path == module.STATE / "pending.json":
            (module.STATE / "identity.pub").symlink_to(foreign)

    monkeypatch.setattr(module, "_root_write", orphan)
    with pytest.raises(ValueError):
        module.prepare_controller(**args)
    assert foreign.read_bytes() == b"preserve\n"
    assert not (module.STATE / "identity").exists()


def test_pending_record_with_extra_authority_fields_is_rejected(tmp_path, monkeypatch):
    module, args = _context(tmp_path, monkeypatch)
    module.STATE.mkdir(mode=0o700)
    (module.STATE / "pending.json").write_bytes(_json({"schema_version": 1,
        "inventory_sha256": args["inventory_sha256"], "source_sha": "a" * 40,
        "unexpected": "authority"}))
    (module.STATE / "pending.json").chmod(0o600)
    with pytest.raises(ValueError):
        module.prepare_controller(**args)
    assert not module.IDENTITY.exists()


@pytest.mark.timeout(2)
def test_fifo_destination_refuses_without_waiting_for_a_writer(tmp_path, monkeypatch):
    module, args = _context(tmp_path, monkeypatch)
    module.prepare_controller(**args)
    module.IDENTITY.unlink()
    os.mkfifo(module.IDENTITY, 0o600)
    with pytest.raises(ValueError):
        module.prepare_controller(**args)


def test_original_key_and_directory_are_synced_before_publication(tmp_path, monkeypatch):
    module, args = _context(tmp_path, monkeypatch)
    synced = set()
    sync, write = module.os.fsync, module._write

    def record(fd):
        metadata = os.fstat(fd)
        synced.add((metadata.st_dev, metadata.st_ino))
        sync(fd)

    def verify(path, payload, **kwargs):
        if path == module.IDENTITY:
            for original in (module.STATE / "identity", module.STATE):
                metadata = original.stat()
                assert (metadata.st_dev, metadata.st_ino) in synced
        write(path, payload, **kwargs)

    monkeypatch.setattr(module.os, "fsync", record)
    monkeypatch.setattr(module, "_write", verify)
    module.prepare_controller(**args)


def test_installer_siblings_load_under_isolated_python_without_pythonpath(tmp_path):
    from scripts.ops import staging_cnpg_observer_controller as module

    result = subprocess.run([sys.executable, "-I", "-B", "-c",
        "import importlib.util; from pathlib import Path; "
        f"s=importlib.util.spec_from_file_location('observer', {str(module.__file__)!r}); "
        "m=importlib.util.module_from_spec(s); s.loader.exec_module(m); "
        "h=m._host_installer(); print(h.REPO_ROOT)"],
        cwd=tmp_path, capture_output=True, text=True, check=False)
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == str(module.Path(module.__file__).resolve().parents[2])


@pytest.mark.parametrize("refusal", [None, "source", "record", "history", "parent", "executable"])
def test_real_authority_wiring_runs_before_any_output(tmp_path, monkeypatch, refusal):
    from scripts.ops import staging_cnpg_observer_controller as module

    authority = module._require_authority
    module, args = _context(tmp_path, monkeypatch)
    events = []

    def check(name, result=None):
        def operation(*positional, **kwargs):
            events.append((name, positional, kwargs))
            if refusal == name:
                raise ValueError("authority refusal")
            return result
        return operation

    system = SimpleNamespace(
        validate_invocation_checkout=check("source", "a" * 40),
        validate_install_record_authority=check("record"),
        validate_invocation_dev_head=check("history"))
    host = SimpleNamespace(HostSystem=lambda runner: system, SubprocessRunner=lambda: None,
        LocalFilesystem=lambda: SimpleNamespace(load_install_record=lambda: {"source_sha": "b" * 40}),
        _validate_root_authority_parent_chain=check("parent"),
        _safe_root_executable=check("executable"))
    monkeypatch.setattr(module, "_host_installer", lambda: host)
    monkeypatch.setattr(module, "_require_authority", authority)
    monkeypatch.setattr(module.socket, "gethostname", lambda: "TRT-EAI-OLDLAB-1")
    monkeypatch.setattr(module.pwd, "getpwnam", lambda name: SimpleNamespace(pw_uid=os.getuid(), pw_gid=os.getgid()))
    if refusal:
        with pytest.raises(ValueError, match="authority refusal"):
            module.prepare_controller(**args)
        assert not module.STATE.exists()
    else:
        module.prepare_controller(**args)
        assert [item[0] for item in events[:3]] == ["source", "record", "history"]
        assert events[2][1] == ("a" * 40, "b" * 40)
        assert args["inventory_file"] in [event[1][0] for event in events if event[0] == "parent"]


def test_installed_identity_deleted_after_readback_is_not_recreated(tmp_path, monkeypatch):
    module, args = _context(tmp_path, monkeypatch)
    module.prepare_controller(**args)
    write = module._write

    def disappear(path, payload, **kwargs):
        if path == module.IDENTITY:
            path.unlink()
        write(path, payload, **kwargs)

    monkeypatch.setattr(module, "_write", disappear)
    with pytest.raises((ValueError, FileNotFoundError)):
        module.prepare_controller(**args)
    assert not module.IDENTITY.exists()


def test_new_foreign_destination_at_atomic_publication_is_not_overwritten(tmp_path, monkeypatch):
    module, args = _context(tmp_path, monkeypatch)
    replace, link = module.os.replace, module.os.link

    def race(operation):
        def publish(source, destination, *positional, **kwargs):
            if destination == module.IDENTITY:
                destination.write_bytes(b"foreign publication\n")
                destination.chmod(0o600)
            return operation(source, destination, *positional, **kwargs)
        return publish

    monkeypatch.setattr(module.os, "replace", race(replace))
    monkeypatch.setattr(module.os, "link", race(link))
    with pytest.raises((ValueError, FileExistsError)):
        module.prepare_controller(**args)
    assert module.IDENTITY.read_bytes() == b"foreign publication\n"
