"""Exercise the dedicated receiver's actual process-wide secret boundary."""

import asyncio
import json
import sys
import tempfile
from pathlib import Path

import pytest

from tests.unit.test_native_bootstrap_delivery import delivery as delivery
from tests.unit.test_native_bootstrap_delivery import objects


@pytest.fixture
def private_delivery(delivery, request):
    # Unlike the storage tests, this entrypoint must refuse writable ancestors
    # such as /tmp and /dev/shm. Use an actual protected user-owned namespace.
    tree = tempfile.TemporaryDirectory(prefix="loom-native-receiver-test-", dir=Path.home())
    request.addfinalizer(tree.cleanup)
    delivery.node = Path(tree.name)
    return delivery


def _probe(delivery, *, injected=""):
    config = {"directory": str(delivery.node), "observation": delivery.admission.current.model_dump_json(),
        "route": delivery.admission.route_sha256, "now": delivery.now.isoformat()}
    return f"""
import asyncio, ctypes, json, os, resource, stat, sys
from datetime import datetime
from pathlib import Path
from loom_capacity_agent.admission import CurrentExecutableBootstrapV2
from loom_capacity_executor.native_bootstrap_delivery import NativeBootstrapReceiver
from loom_capacity_executor import native_bootstrap_receiver as receiver_process
config = json.loads({json.dumps(config)!r})
{injected}
def factory():
    assert resource.getrlimit(resource.RLIMIT_CORE) == (0, 0)
    assert ctypes.CDLL(None).prctl(3, 0, 0, 0, 0) == 0
    observation = CurrentExecutableBootstrapV2.model_validate_json(config['observation'])
    class Admission:
        def bootstrap_handoff_route_sha256(self, binding):
            return config['route']
        async def observe_current_bootstrap(self, physical):
            assert physical == observation.physical_binding
            assert stat.S_ISCHR(os.fstat(0).st_mode)
            assert os.read(0, 1) == b''
            return observation
    binding = observation.physical_binding.binding
    return NativeBootstrapReceiver(directory=Path(config['directory']),
        target_node=binding.node_ids[0], pool_id=binding.pool_id,
        trusted_release_sha256=binding.execution.trusted_fleet_release_sha256,
        admission=Admission(), now=lambda: datetime.fromisoformat(config['now']))
result = receiver_process.run_native_bootstrap_receiver_process(factory)
assert stat.S_ISCHR(os.fstat(0).st_mode)
assert os.read(0, 1) == b''
raise SystemExit(result)
"""


async def _run(delivery, payload, *, injected=""):
    process = await asyncio.create_subprocess_exec(sys.executable, "-B", "-c", _probe(delivery, injected=injected),
        stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
    try:
        stdout, stderr = await asyncio.wait_for(process.communicate(payload), 10)
        return process.returncode, stdout, stderr
    finally:
        if process.returncode is None:
            process.kill()
        await process.wait()


async def test_receiver_process_hardens_before_factory_and_detaches_before_admission(private_delivery):
    module, payload, _receiver = objects(private_delivery)
    code, stdout, stderr = await _run(private_delivery, payload)
    assert code == 0, stderr.decode()
    assert stderr == b""
    receipt = module.read_native_delivery_receipt(module.native_delivery_directory(private_delivery.node,
        private_delivery.lease.reference), private_delivery.lease.reference)
    assert json.loads(stdout) == receipt.model_dump(mode="json", by_alias=True)
    secret = json.loads(payload)["record"]["capability"].encode()
    assert secret not in stdout + stderr


@pytest.mark.parametrize("mutation", ("empty", "truncated", "trailing", "oversize", "malformed"))
async def test_receiver_process_rejects_bad_input_without_secret_diagnostics(private_delivery, mutation):
    _module, payload, _receiver = objects(private_delivery)
    if mutation == "empty":
        payload = b""
    elif mutation == "truncated":
        payload = payload[:-1]
    elif mutation == "trailing":
        payload += b"\n"
    elif mutation == "oversize":
        payload += b"a" * (256 * 1024)
    else:
        payload = b'{"capability":"private-input-never-log"}'
    code, stdout, stderr = await _run(private_delivery, payload)
    assert code == 2
    assert stdout == b""
    assert stderr == b"native bootstrap receiver refused\n"
    assert list(private_delivery.node.iterdir()) == []


async def test_receiver_process_refuses_writable_ancestor_before_read(delivery):
    _module, payload, _receiver = objects(delivery)
    injected = """
def forbidden_read(*args, **kwargs):
    raise AssertionError('must reject destination before reading capability')
receiver_process._read_delivery_stdin = forbidden_read
"""
    code, stdout, stderr = await _run(delivery, payload, injected=injected)
    assert code == 2
    assert stdout == b""
    assert stderr == b"native bootstrap receiver refused\n"
    assert list(delivery.node.iterdir()) == []


async def test_receiver_process_requires_eof_within_deadline(private_delivery):
    _module, payload, _receiver = objects(private_delivery)
    injected = """
read_input = receiver_process._read_delivery_stdin
receiver_process._read_delivery_stdin = lambda: read_input(timeout_seconds=0.05)
"""
    process = await asyncio.create_subprocess_exec(sys.executable, "-B", "-c", _probe(private_delivery, injected=injected),
        stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
    try:
        process.stdin.write(payload)
        await process.stdin.drain()
        await asyncio.wait_for(process.wait(), 10)
        assert process.returncode == 2
        assert await process.stdout.read() == b""
        assert await process.stderr.read() == b"native bootstrap receiver refused\n"
        assert list(private_delivery.node.iterdir()) == []
    finally:
        if process.returncode is None:
            process.kill()
        await process.wait()


async def test_receiver_hardening_failure_never_constructs_admission_or_reads(private_delivery):
    _module, payload, _receiver = objects(private_delivery)
    injected = """
def fail_hardening():
    raise OSError('private-detail-never-log')
receiver_process._disable_bootstrap_dumps = fail_hardening
"""
    code, stdout, stderr = await _run(private_delivery, payload, injected=injected)
    assert code == 2
    assert stdout == b""
    assert stderr == b"native bootstrap receiver refused\n"
    assert list(private_delivery.node.iterdir()) == []
