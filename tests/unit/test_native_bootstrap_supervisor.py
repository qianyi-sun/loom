"""Fixed local policy and explicit service ownership, without live installation."""

import asyncio
import hashlib
import json
import os
import subprocess
import sys
import tempfile
from importlib import import_module
from pathlib import Path
from types import SimpleNamespace

import pytest


@pytest.fixture
def configuration(request):
    tree = tempfile.TemporaryDirectory(prefix="loom-native-supervisor-test-", dir=Path.home())
    request.addfinalizer(tree.cleanup)
    base = Path(tree.name)
    admission = base / "admission"
    admission.mkdir(mode=0o700)
    from loom_capacity_executor.native_bootstrap_receiver import NativeBootstrapReceiverConfigV1
    from loom_capacity_executor.runtime import canonical_admission_directory_digest

    receiver = NativeBootstrapReceiverConfigV1(directory=str(base), target_node="oldlab-5", pool_id="oldlab",
        trusted_release_sha256="a" * 64, admission_directory=str(admission),
        admission_directory_sha256=canonical_admission_directory_digest(admission))

    def write(name, data):
        raw = json.dumps(data, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode()
        path = base / name
        path.write_bytes(raw)
        path.chmod(0o600)
        return {"path": str(path), "sha256": hashlib.sha256(raw).hexdigest(), "owner_uid": os.geteuid()}

    receiver_pin = write("receiver.json", receiver.model_dump(mode="json", by_alias=True))
    tls = {name: {"path": str(base / f"{name}.pem"), "sha256": "c" * 64}
        for name in ("ca", "certificate", "private_key")}
    data = {"schema": "loom.native-bootstrap-supervisor-config/v1", "listen_address": "127.0.0.1",
        "listen_port": 9443, "target_node": "oldlab-5", "pool_id": "oldlab", "trusted_release_sha256": "a" * 64,
        "identity": tls, "receiver": {"interpreter": {"path": sys.executable, "sha256": "b" * 64,
        "owner_uid": os.geteuid(), "mode": 0o555}, "configuration": receiver_pin,
        "timeout_seconds": 15.0, "cleanup_wait_seconds": 1.0, "maximum_processes": 2},
        "limits": {"maximum_connections": 8, "maximum_operations": 2, "handshake_seconds": 3.0, "total_seconds": 20.0},
        "peers": [{"certificate_sha256": "d" * 64, "pool_id": "oldlab", "executor_id": "executor",
        "executor_incarnation": "00000000-0000-0000-0000-000000000001", "operations": ["deliver", "status"],
        "expires_at": "2099-01-01T00:00:00Z"}]}
    return SimpleNamespace(data=data, write=write, receiver=receiver)


def test_closed_canonical_config_loads_and_binds_receiver_scope(configuration):
    module = import_module("loom_capacity_executor.native_bootstrap_supervisor")
    from loom_capacity_executor.slurm_contracts import SlurmFileIdentityV2

    pin = configuration.write("supervisor.json", configuration.data)
    loaded = module.load_native_bootstrap_supervisor_config(SlurmFileIdentityV2(**pin))
    assert loaded.target_node == configuration.receiver.target_node
    assert loaded.receiver.configuration.path == configuration.data["receiver"]["configuration"]["path"]


@pytest.mark.parametrize("change", ("wildcard", "hostname", "privileged-port", "foreign-node", "foreign-pool",
    "extra-command", "duplicate-peer", "peer-pool", "operations-order", "capacity", "digest", "noncanonical"))
def test_config_rejects_ambient_authority_and_conflicting_pins(configuration, change):
    module = import_module("loom_capacity_executor.native_bootstrap_supervisor")
    from loom_capacity_executor.slurm_contracts import SlurmFileIdentityV2

    data = configuration.data
    if change in {"wildcard", "hostname"}:
        data["listen_address"] = "0.0.0.0" if change == "wildcard" else "localhost"
    elif change == "privileged-port":
        data["listen_port"] = 443
    elif change == "foreign-node":
        data["target_node"] = "foreign"
    elif change == "foreign-pool":
        data["pool_id"] = "foreign"
    elif change == "extra-command":
        data["command"] = "untrusted"
    elif change == "duplicate-peer":
        data["peers"] *= 2
    elif change == "peer-pool":
        data["peers"][0]["pool_id"] = "foreign"
    elif change == "operations-order":
        data["peers"][0]["operations"] = ["status", "deliver"]
    elif change == "capacity":
        data["receiver"]["maximum_processes"] = 3
    pin = configuration.write("supervisor.json", data)
    if change == "digest":
        pin["sha256"] = "f" * 64
    elif change == "noncanonical":
        path = Path(pin["path"])
        raw = path.read_bytes() + b"\n"
        path.write_bytes(raw)
        pin["sha256"] = hashlib.sha256(raw).hexdigest()
    with pytest.raises(ValueError):
        module.load_native_bootstrap_supervisor_config(SlurmFileIdentityV2(**pin))


@pytest.fixture
def service(configuration, monkeypatch):
    module = import_module("loom_capacity_executor.native_bootstrap_supervisor")
    config = module.NativeBootstrapSupervisorConfigV1.model_validate_json(json.dumps(configuration.data))
    state = SimpleNamespace(events=[], started=asyncio.Event(), failure=asyncio.Event(),
        cleanup_started=asyncio.Event(), cleanup_release=asyncio.Event(), hold_cleanup=False, mode="stop")

    class Adapter:
        def __init__(self, policy):
            assert policy == config.receiver
            state.events.append("adapter-created")

        async def wait(self):
            await state.failure.wait()
            if state.mode == "adapter-failure":
                raise RuntimeError("private-cleanup-detail")
            await asyncio.Event().wait()

        async def aclose(self):
            state.events.append("adapter-close")
            state.cleanup_started.set()
            if state.hold_cleanup:
                await state.cleanup_release.wait()
            state.events.append("adapter-reaped")

    class Server:
        def __init__(self, adapter, **kwargs):
            assert isinstance(adapter, Adapter)
            assert kwargs["target_node"] == config.target_node
            assert set(kwargs["peers"]) == {"d" * 64}
            if state.mode == "constructor-failure":
                raise RuntimeError("private-tls-detail")
            state.events.append("server-created")

        async def start(self, **kwargs):
            assert kwargs == {"host": "127.0.0.1", "port": 9443}
            if state.mode == "start-failure":
                raise RuntimeError("private-listener-detail")
            state.events.append("server-started")
            state.started.set()

        async def wait(self):
            await state.failure.wait()
            if state.mode == "listener-failure":
                raise RuntimeError("private-accept-detail")
            await asyncio.Event().wait()

        async def aclose(self):
            state.events.append("server-close")
            if state.mode == "close-failure":
                raise RuntimeError("private-close-detail")

    monkeypatch.setattr(module, "_assert_private_process", lambda: None)
    monkeypatch.setattr(module, "NativeBootstrapProcessAdapter", Adapter)
    monkeypatch.setattr(module, "NativeBootstrapTLSServer", Server)
    return SimpleNamespace(module=module, config=config, state=state)


@pytest.mark.parametrize("mode", ("stop", "constructor-failure", "start-failure", "listener-failure", "adapter-failure", "close-failure"))
async def test_service_joins_every_constructed_owner_on_stop_or_failure(service, mode):
    state = service.state
    state.mode = mode
    stop = asyncio.Event()
    task = asyncio.create_task(service.module.run_native_bootstrap_supervisor(service.config, stop))
    try:
        if mode not in {"constructor-failure", "start-failure"}:
            await asyncio.wait_for(state.started.wait(), 1)
            if mode in {"listener-failure", "adapter-failure"}:
                state.failure.set()
            else:
                stop.set()
        if mode == "stop":
            await asyncio.wait_for(task, 1)
        else:
            with pytest.raises(ValueError, match="supervisor unavailable or refused"):
                await asyncio.wait_for(task, 1)
        assert state.events[-2:] == ["adapter-close", "adapter-reaped"]
        if mode != "constructor-failure":
            assert state.events.index("server-close") < state.events.index("adapter-close")
    finally:
        stop.set()
        await asyncio.gather(task, return_exceptions=True)


async def test_repeated_cancellation_cannot_abandon_service_cleanup(service):
    state = service.state
    state.hold_cleanup = True
    task = asyncio.create_task(service.module.run_native_bootstrap_supervisor(service.config, asyncio.Event()))
    try:
        await asyncio.wait_for(state.started.wait(), 1)
        task.cancel()
        await asyncio.wait_for(state.cleanup_started.wait(), 1)
        task.cancel()
        done, _pending = await asyncio.wait({task}, timeout=0.05)
        assert not done
        state.cleanup_release.set()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(task, 1)
        assert state.events[-1] == "adapter-reaped"
    finally:
        state.cleanup_release.set()
        await asyncio.gather(task, return_exceptions=True)


def test_main_hardens_before_argument_errors_and_sanitizes(configuration):
    probe = """
import ctypes, resource
from loom_capacity_executor.native_bootstrap_supervisor import main
result = main(['--untrusted-command', 'private-detail'])
assert resource.getrlimit(resource.RLIMIT_CORE) == (0, 0)
assert ctypes.CDLL(None).prctl(3, 0, 0, 0, 0) == 0
raise SystemExit(result)
"""
    result = subprocess.run([sys.executable, "-I", "-B", "-c", probe], capture_output=True, timeout=10)
    assert result.returncode == 2 and result.stdout == b""
    assert result.stderr == b"native bootstrap supervisor unavailable or refused\n"
