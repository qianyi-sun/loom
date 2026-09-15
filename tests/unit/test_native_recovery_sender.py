"""Fixed recovery transport never accepts feature commands or unbounded replies."""

import asyncio
import os
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


@pytest.fixture
def transport_module(monkeypatch):
    module = import_module("loom_capacity_build_guard.native_recovery_sender")
    # Pipe/lifetime tests replace SSH, not the separate material validation tests.
    monkeypatch.setattr(module, "_snapshot_target", lambda target, stack: target, raising=False)
    return module


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
        "PasswordAuthentication=no", "KbdInteractiveAuthentication=no", "UpdateHostKeys=no", "CertificateFile=none",
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
def test_private_transport_material_rejects_changed_identity(tmp_path, monkeypatch, fault):
    module = import_module("loom_capacity_build_guard.native_recovery_sender")
    # Pytest's /tmp ancestry is intentionally not a protected installation.
    # Keep real file mode/link/digest checks; parent protection is tested below.
    def fixture_parent(path, stack):
        fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        stack.callback(os.close, fd)
        return fd
    monkeypatch.setattr(module, "_protected_parents", fixture_parent)
    material = tmp_path / "key"
    material.write_bytes(b"private-fixture-key")
    material.chmod(0o600)
    expected = sha256(material.read_bytes()).hexdigest()
    assert module._read_transport_material(material, expected_sha256=expected) == b"private-fixture-key"
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


def test_transport_rejects_unprotected_ancestor(tmp_path):
    module = import_module("loom_capacity_build_guard.native_recovery_sender")
    material = tmp_path / "key"
    material.write_bytes(b"private-fixture-key")
    material.chmod(0o600)
    with pytest.raises(ValueError, match="unprotected parent"):
        module._read_transport_material(material, expected_sha256=sha256(material.read_bytes()).hexdigest())


async def test_transport_bounds_real_subprocess_output_and_reaps(monkeypatch, transport_module):
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
async def test_transport_interruption_reaps_without_replaying(monkeypatch, interruption, transport_module):
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


async def test_cancel_during_ssh_creation_reaps_late_process(monkeypatch, transport_module):
    import sys

    module = import_module("loom_capacity_build_guard.native_recovery_sender")
    real_spawn = asyncio.create_subprocess_exec
    processes = []
    created, release = asyncio.Event(), asyncio.Event()

    async def delayed(*args, **kwargs):
        process = await real_spawn(sys.executable, "-c", "import time; time.sleep(60)",
            stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL)
        processes.append(process)
        created.set()
        await release.wait()
        return process

    monkeypatch.setattr(module, "_read_transport_material", lambda *args, **kwargs: None)
    monkeypatch.setattr(module.asyncio, "create_subprocess_exec", delayed)
    task = asyncio.create_task(module._exchange(target(module), b"{}"))
    await created.wait()
    task.cancel()
    await asyncio.sleep(0)
    task.cancel()
    release.set()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert len(processes) == 1 and processes[0].returncode is not None


async def test_transport_uses_sealed_verified_bytes_during_material_rotation(tmp_path, monkeypatch):
    from contextlib import ExitStack

    module = import_module("loom_capacity_build_guard.native_recovery_sender")
    identity, known = tmp_path / "key", tmp_path / "known_hosts"
    identity.write_bytes(b"original-private-fixture")
    known.write_bytes(b"original-host-key-fixture")
    identity.chmod(0o600)
    known.chmod(0o600)
    configured = target(module, identity=str(identity), known_hosts=str(known),
        identity_sha256=sha256(identity.read_bytes()).hexdigest(), known_hosts_sha256=sha256(known.read_bytes()).hexdigest())

    def fixture_parent(path, stack):
        fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        stack.callback(os.close, fd)
        return fd
    monkeypatch.setattr(module, "_protected_parents", fixture_parent)
    with ExitStack() as stack:
        snapshot = module._snapshot_target(configured, stack)
        identity.write_bytes(b"rotated-private-fixture")
        known.write_bytes(b"rotated-host-key-fixture")
        for path, expected in ((snapshot.identity, b"original-private-fixture"),
            (snapshot.known_hosts, b"original-host-key-fixture")):
            from pathlib import Path

            assert Path(path).read_bytes() == expected
            with pytest.raises(PermissionError):
                Path(path).write_bytes(b"replacement")
        # OpenSSH closes inherited descriptors at startup. Parent-FD paths stay
        # readable by the same-UID child after close_fds, unlike /proc/self/fd.
        import sys

        process = await asyncio.create_subprocess_exec(sys.executable, "-c",
            "import pathlib,sys; assert pathlib.Path(sys.argv[1]).read_bytes()==b'original-private-fixture'",
            snapshot.identity, close_fds=True)
        assert await process.wait() == 0


def test_openssh_reads_original_private_key_from_sealed_parent_snapshot(tmp_path, monkeypatch):
    import subprocess
    from contextlib import ExitStack

    module = import_module("loom_capacity_build_guard.native_recovery_sender")
    identity, known = tmp_path / "key", tmp_path / "known_hosts"
    subprocess.run(["/usr/bin/ssh-keygen", "-q", "-t", "ed25519", "-N", "", "-f", str(identity)],
        check=True, timeout=5, capture_output=True)
    expected = subprocess.run(["/usr/bin/ssh-keygen", "-y", "-f", str(identity)], check=True,
        timeout=5, capture_output=True).stdout
    known.write_bytes(b"fixture-host-key")
    known.chmod(0o600)

    def fixture_parent(path, stack):
        fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        stack.callback(os.close, fd)
        return fd
    monkeypatch.setattr(module, "_protected_parents", fixture_parent)
    configured = target(module, identity=str(identity), known_hosts=str(known),
        identity_sha256=sha256(identity.read_bytes()).hexdigest(), known_hosts_sha256=sha256(known.read_bytes()).hexdigest())
    with ExitStack() as stack:
        snapshot = module._snapshot_target(configured, stack)
        identity.write_bytes(b"replaced-after-validation")
        observed = subprocess.run(["/usr/bin/ssh-keygen", "-y", "-f", snapshot.identity], check=True,
            timeout=5, capture_output=True).stdout
        assert observed == expected
