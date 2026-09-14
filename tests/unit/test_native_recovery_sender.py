"""Fixed recovery transport never accepts feature commands or unbounded replies."""

import asyncio
from hashlib import sha256
from importlib import import_module
from uuid import uuid4

import pytest


def target(module, **changes):
    values = dict(installation_id=uuid4(), pool_id="pool-a", node_id="node-a", address="192.0.2.10",
        port=22, profile_sha256="a" * 64, host_sha256="b" * 64,
        identity="/var/lib/loom-native-recovery/id_ed25519", identity_sha256="c" * 64,
        known_hosts="/var/lib/loom-native-recovery/known_hosts", known_hosts_sha256="d" * 64)
    return module.NativeRecoveryTargetV1(**(values | changes))


def test_recovery_ssh_has_no_remote_command_or_ambient_authority():
    module = import_module("loom_capacity_build_guard.native_recovery_sender")
    configured = target(module)
    argv = module._ssh_argv(configured)
    assert argv[0] == "/usr/bin/ssh"
    assert argv[-1] == "loom-native-recovery@192.0.2.10"
    assert argv[argv.index("-F") + 1] == "/dev/null" and "-T" in argv
    options = [argv[i + 1] for i, value in enumerate(argv[:-1]) if value == "-o"]
    for required in ("BatchMode=yes", "StrictHostKeyChecking=yes", "IdentitiesOnly=yes",
        "IdentityAgent=none", "ForwardAgent=no", "ForwardX11=no", "ClearAllForwardings=yes",
        "PermitLocalCommand=no", "ControlMaster=no", "ControlPath=none", "ProxyCommand=none",
        "PasswordAuthentication=no", "KbdInteractiveAuthentication=no", "UpdateHostKeys=no",
        "GlobalKnownHostsFile=/dev/null", "UserKnownHostsFile=" + configured.known_hosts):
        assert required in options


@pytest.mark.parametrize("changes", [dict(address="-oProxyCommand=evil"), dict(address="example.org"),
    dict(identity="relative"), dict(known_hosts="/root/../tmp/keys"), dict(port=0),
    dict(identity="/a\nLocalCommand evil"), dict(identity="/"), dict(identity="/tmp/a b")])
def test_recovery_target_rejects_ambiguous_ssh_options(changes):
    module = import_module("loom_capacity_build_guard.native_recovery_sender")
    with pytest.raises(ValueError):
        target(module, **changes)


@pytest.mark.parametrize("fault", ["mode", "symlink", "hardlink", "digest", "oversized"])
def test_private_transport_material_rejects_changed_identity(tmp_path, fault):
    module = import_module("loom_capacity_build_guard.native_recovery_sender")
    material = tmp_path / "key"
    material.write_bytes(b"private-fixture-key")
    material.chmod(0o600)
    expected = sha256(material.read_bytes()).hexdigest()
    if fault == "mode":
        material.chmod(0o644)
    elif fault == "symlink":
        replacement = tmp_path / "link"
        replacement.symlink_to(material)
        material = replacement
    elif fault == "hardlink":
        (tmp_path / "alias").hardlink_to(material)
    elif fault == "digest":
        expected = "f" * 64
    else:
        material.write_bytes(b"x" * (65536 + 1))
    with pytest.raises((ValueError, OSError)):
        module._read_transport_material(material, expected_sha256=expected)


async def test_transport_bounds_real_subprocess_output_and_reaps(monkeypatch):
    import sys

    module = import_module("loom_capacity_build_guard.native_recovery_sender")
    processes = []

    async def spawn(_target):
        process = await asyncio.create_subprocess_exec(sys.executable, "-c",
            "import sys,time; sys.stdin.buffer.read(); sys.stdout.buffer.write(b'x'*100000); sys.stdout.flush(); time.sleep(60)",
            stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL,
            limit=4097)
        processes.append(process)
        return process

    monkeypatch.setattr(module, "_spawn", spawn)
    with pytest.raises(ValueError, match="bound"):
        await module._exchange(target(module), b"{}", timeout_seconds=2)
    assert len(processes) == 1 and processes[0].returncode is not None


@pytest.mark.parametrize("interruption", ["timeout", "cancel"])
async def test_transport_interruption_reaps_without_replaying(monkeypatch, interruption):
    import sys

    module = import_module("loom_capacity_build_guard.native_recovery_sender")
    processes = []
    started = asyncio.Event()

    async def spawn(_target):
        process = await asyncio.create_subprocess_exec(sys.executable, "-c", "import time; time.sleep(60)",
            stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL)
        processes.append(process)
        started.set()
        return process

    monkeypatch.setattr(module, "_spawn", spawn)
    task = asyncio.create_task(module._exchange(target(module), b"{}", timeout_seconds=0.1 if interruption == "timeout" else 5))
    await started.wait()
    if interruption == "cancel":
        task.cancel()
    with pytest.raises(TimeoutError if interruption == "timeout" else asyncio.CancelledError):
        await task
    assert len(processes) == 1 and processes[0].returncode is not None
