"""Controller and worker have separate private filesystems, not a shared mount."""

import asyncio
import hashlib
import json
import sys
import tempfile
from datetime import timedelta
from importlib import import_module
from pathlib import Path
from types import SimpleNamespace
from uuid import UUID, uuid5

import pytest

from loom_capacity_agent.admission import CurrentExecutableBootstrapV2
from loom_capacity_executor.bootstrap_handoff import (
    BootstrapHandoffStore,
    bind_bootstrap_handoff_ownership,
    claim_bootstrap_handoff_launch,
    consume_bootstrap_handoff,
    resolve_bootstrap_handoff_physical_binding,
)
from loom_capacity_manager.executable_contracts import canonical_executable_digest
from tests.unit.test_capacity_executor_bootstrap_handoff import _NOW, _Admission, _physical
from tests.unit.test_capacity_executor_launch_renderer import launch_context_fixture


@pytest.fixture
def delivery(tmp_path, request):
    controller = tmp_path / "controller"
    controller.mkdir(mode=0o700)
    # A different mount, not merely another directory in the controller tree.
    # All staging/atomic rename stays within the node's own filesystem.
    node_tree = tempfile.TemporaryDirectory(prefix="loom-native-bootstrap-test-", dir="/dev/shm")
    request.addfinalizer(node_tree.cleanup)
    node = Path(node_tree.name)
    assert controller.stat().st_dev != node.stat().st_dev
    binding = launch_context_fixture().binding
    physical = _physical(binding).model_copy(update={"operation_id": uuid5(
        UUID("cb359b0c-a844-4bc5-9592-a4c35e344f3d"), f"physical-bind:{binding.intent_id}")})
    store = BootstrapHandoffStore(controller)
    lease = store.prepare(binding, bootstrap_registration_epoch=1,
        expires_at=_NOW + timedelta(minutes=5),
        trusted_launcher_release_sha256=binding.execution.trusted_fleet_release_sha256,
        protected_admission_route_sha256=_Admission.route_sha256)
    bind_bootstrap_handoff_ownership(controller, lease.reference, binding,
        bootstrap_registration_epoch=1, ownership_evidence_sha256=physical.ownership_evidence_sha256,
        trusted_launcher_release_sha256=binding.execution.trusted_fleet_release_sha256, now=lambda: _NOW)
    current = CurrentExecutableBootstrapV2(physical_binding=physical, agent_incarnation=UUID(int=81),
        bootstrap_sha256=lease.bootstrap_sha256, observed_at=_NOW,
        bootstrap_expires_at=_NOW + timedelta(minutes=5), request_digest=canonical_executable_digest(physical))

    class Admission(_Admission):
        def __init__(self):
            super().__init__()
            self.reads = 0
            self.current = current

        async def observe_current_bootstrap(self, requested):
            assert requested == physical
            self.reads += 1
            await asyncio.sleep(0)
            return self.current

    return SimpleNamespace(controller=controller, node=node, store=store, physical=physical,
        lease=lease, admission=Admission(), now=_NOW)


def objects(delivery):
    module = import_module("loom_capacity_executor.native_bootstrap_delivery")
    payload = module.export_native_bootstrap(delivery.store, delivery.physical, now=lambda: delivery.now)
    receiver = module.NativeBootstrapReceiver(directory=delivery.node,
        target_node=delivery.physical.binding.node_ids[0], pool_id=delivery.physical.binding.pool_id,
        trusted_release_sha256=delivery.physical.binding.execution.trusted_fleet_release_sha256,
        admission=delivery.admission, now=lambda: delivery.now)
    return module, payload, receiver


async def test_two_filesystem_delivery_retains_credentials_only_on_worker(delivery):
    module, payload, receiver = objects(delivery)
    receipt = await receiver.receive(payload)
    directory = module.native_delivery_directory(delivery.node, delivery.lease.reference)
    assert directory != delivery.controller
    import base64

    physical = resolve_bootstrap_handoff_physical_binding(directory, delivery.lease.reference,
        operation_id=delivery.physical.binding.intent_id, slurm_job_id=delivery.physical.slurm_job_id,
        ownership_token=base64.urlsafe_b64encode(bytes.fromhex(delivery.physical.ownership_evidence_sha256)).rstrip(b"=").decode(),
        trusted_launcher_release_sha256=delivery.physical.binding.execution.trusted_fleet_release_sha256,
        now=lambda: delivery.now)
    assert physical == delivery.physical
    credential = await consume_bootstrap_handoff(directory, delivery.lease.reference, physical, delivery.admission, now=lambda: delivery.now)
    assert claim_bootstrap_handoff_launch(directory, delivery.lease.reference, physical, delivery.admission, now=lambda: delivery.now) == credential
    assert all(credential not in path.read_text() for path in delivery.controller.iterdir() if path.is_file())
    assert credential not in receipt.model_dump_json()
    assert json.loads(payload)["record"]["capability"] not in repr(receipt)
    assert receipt.executable is False
    assert receipt.source_payload_sha256 == hashlib.sha256(payload).hexdigest()
    # Lost delivery response after launch is a historical receipt replay, not
    # permission to copy the capability again or restart the worker.
    delivery.admission.current = None
    assert await receiver.receive(payload) == receipt
    assert delivery.admission.reads == 1
    assert not (directory / delivery.lease.reference).exists()
    assert (directory / delivery.lease.reference).with_suffix(".launched").is_file()


async def test_concurrent_delivery_publishes_one_complete_directory(delivery):
    module, payload, receiver = objects(delivery)
    left, right = await asyncio.gather(receiver.receive(payload), receiver.receive(payload))
    assert left == right
    directory = module.native_delivery_directory(delivery.node, delivery.lease.reference)
    assert (directory / delivery.lease.reference).is_file()
    assert (directory / delivery.lease.reference).with_suffix(".ownership").is_file()
    assert sorted(path.name for path in delivery.node.iterdir()) == [directory.name]


@pytest.mark.parametrize("changed", ("node", "pool", "release", "route", "capability", "expiry", "ownership", "physical", "extra", "duplicate", "oversize"))
async def test_wrong_scope_or_malformed_delivery_never_creates_worker_files(delivery, changed):
    _module, payload, receiver = objects(delivery)
    value = json.loads(payload)
    if changed in {"node", "pool", "release"}:
        setattr(receiver, {"node": "target_node", "pool": "pool_id", "release": "trusted_release_sha256"}[changed], "foreign")
    elif changed == "route":
        value["record"]["protected_admission_route_sha256"] = "f" * 64
    elif changed == "capability":
        value["record"]["capability"] = "f" * 43
    elif changed == "expiry":
        value["record"]["expires_at"] = "2027-08-13T16:00:00Z"
        value["ownership"]["expires_at"] = value["record"]["expires_at"]
    elif changed == "ownership":
        value["ownership"]["ownership_evidence_sha256"] = "f" * 64
    elif changed == "physical":
        value["physical"]["bootstrap_registration_epoch"] = 2
    elif changed == "extra":
        value["worker_credential"] = "must-not-be-delivered"
    payload = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("ascii")
    if changed == "duplicate":
        payload = payload.replace(b'"physical":', b'"physical":null,"physical":', 1)
    elif changed == "oversize":
        payload += b" " * (256 * 1024)
    with pytest.raises(ValueError):
        await receiver.receive(payload)
    assert list(delivery.node.iterdir()) == []


@pytest.mark.parametrize("state", ("expired", "future", "stale", "changed", "cancelled"))
async def test_delivery_requires_fresh_current_unused_bootstrap(delivery, state):
    _module, payload, receiver = objects(delivery)
    if state == "expired":
        delivery.now += timedelta(minutes=6)
    elif state in {"future", "stale"}:
        shift = timedelta(seconds=60 if state == "future" else -60)
        delivery.admission.current = delivery.admission.current.model_copy(update={"observed_at": _NOW + shift})
    elif state == "changed":
        delivery.admission.current = delivery.admission.current.model_copy(update={"bootstrap_sha256": "f" * 64})
    else:
        async def cancelled(request):
            raise asyncio.CancelledError()
        delivery.admission.observe_current_bootstrap = cancelled
    with pytest.raises(asyncio.CancelledError if state == "cancelled" else ValueError):
        await receiver.receive(payload)
    assert list(delivery.node.iterdir()) == []


async def test_conflicting_duplicate_cannot_overwrite_received_capability(delivery):
    _module, payload, receiver = objects(delivery)
    receipt = await receiver.receive(payload)
    value = json.loads(payload)
    value["record"]["capability"] = "x" * 43
    value["record"]["capability_sha256"] = hashlib.sha256(value["record"]["capability"].encode()).hexdigest()
    changed = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("ascii")
    with pytest.raises(ValueError):
        await receiver.receive(changed)
    assert await receiver.receive(payload) == receipt


async def test_retry_after_uncertain_rename_reestablishes_directory_durability(delivery, monkeypatch):
    module, payload, receiver = objects(delivery)
    fsync = module._fsync_directory
    attempts = []

    def interrupted(path):
        if path == delivery.node:
            attempts.append(path)
            if len(attempts) == 1:
                raise OSError("injected parent fsync failure")
        return fsync(path)

    monkeypatch.setattr(module, "_fsync_directory", interrupted)
    with pytest.raises(ValueError):
        await receiver.receive(payload)
    receipt = await receiver.receive(payload)
    assert receipt.source_payload_sha256 == hashlib.sha256(payload).hexdigest()
    assert len(attempts) >= 2, "an uncertain publish must be fsynced before replay acknowledgment"


async def test_delivery_never_replaces_an_empty_destination_created_during_publish(delivery, monkeypatch):
    module, payload, receiver = objects(delivery)
    publish = module._publish_private_new
    final = module.native_delivery_directory(delivery.node, delivery.lease.reference)
    identity = []

    def raced(path, data):
        result = publish(path, data)
        if path.name == "delivery-receipt.json":
            final.mkdir(mode=0o700)
            identity.append(final.stat().st_ino)
        return result

    monkeypatch.setattr(module, "_publish_private_new", raced)
    with pytest.raises(ValueError):
        await receiver.receive(payload)
    assert final.stat().st_ino == identity[0]
    assert list(final.iterdir()) == []


async def test_worker_waits_for_complete_delivery_on_its_own_filesystem(delivery):
    module, payload, receiver = objects(delivery)
    waiting = asyncio.create_task(module.wait_native_bootstrap_delivery(delivery.node,
        delivery.lease.reference, now=lambda: delivery.now, timeout_seconds=1))
    try:
        receipt = await receiver.receive(payload)
        directory = await waiting
    finally:
        if not waiting.done():
            waiting.cancel()
            await asyncio.gather(waiting, return_exceptions=True)
    assert module.read_native_delivery_receipt(directory, delivery.lease.reference) == receipt


async def test_worker_delivery_wait_is_bounded_and_never_uses_controller_files(delivery):
    module, _payload, _receiver = objects(delivery)
    with pytest.raises(ValueError, match="delivery"):
        await module.wait_native_bootstrap_delivery(delivery.node, delivery.lease.reference,
            now=lambda: delivery.now, timeout_seconds=0.01)
    assert (delivery.controller / delivery.lease.reference).is_file()
    assert list(delivery.node.iterdir()) == []


async def test_worker_never_accepts_partial_or_expired_delivery(delivery):
    module, payload, receiver = objects(delivery)
    receipt = await receiver.receive(payload)
    directory = module.native_delivery_directory(delivery.node, delivery.lease.reference)
    delivery.now = receipt.expires_at
    with pytest.raises(ValueError):
        await module.wait_native_bootstrap_delivery(delivery.node, delivery.lease.reference,
            now=lambda: delivery.now, timeout_seconds=0.01)
    (directory / "delivery-receipt.json").unlink()
    with pytest.raises(ValueError):
        await module.wait_native_bootstrap_delivery(delivery.node, delivery.lease.reference,
            now=lambda: _NOW, timeout_seconds=0.01)


async def test_two_receiver_processes_converge_after_independent_authority_reads(delivery):
    _module, payload, _receiver = objects(delivery)
    # Independent authority is a test adapter here; actual cross-process file
    # publication and conflict resolution use the production implementation.
    probe = """
import asyncio, json, sys
from datetime import datetime
from pathlib import Path
from loom_capacity_agent.admission import CurrentExecutableBootstrapV2
from loom_capacity_executor.native_bootstrap_delivery import NativeBootstrapReceiver

data = json.loads(sys.stdin.readline())
observation = CurrentExecutableBootstrapV2.model_validate_json(data['observation'])
class Admission:
    def bootstrap_handoff_route_sha256(self, binding):
        return data['route']
    async def observe_current_bootstrap(self, physical):
        assert physical == observation.physical_binding
        print('ready', flush=True)
        assert await asyncio.to_thread(sys.stdin.readline) == 'publish\\n'
        return observation

async def main():
    binding = observation.physical_binding.binding
    receiver = NativeBootstrapReceiver(directory=Path(data['directory']),
        target_node=binding.node_ids[0], pool_id=binding.pool_id,
        trusted_release_sha256=binding.execution.trusted_fleet_release_sha256,
        admission=Admission(), now=lambda: datetime.fromisoformat(data['now']))
    receipt = await receiver.receive(data['payload'].encode('ascii'))
    print(receipt.model_dump_json(by_alias=True), flush=True)

asyncio.run(main())
"""
    data = json.dumps({"observation": delivery.admission.current.model_dump_json(),
        "directory": str(delivery.node), "payload": payload.decode("ascii"),
        "route": delivery.admission.route_sha256, "now": delivery.now.isoformat()}).encode() + b"\n"
    processes = []
    try:
        for _ in range(2):
            process = await asyncio.create_subprocess_exec(sys.executable, "-B", "-c", probe,
                stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
            processes.append(process)
            process.stdin.write(data)
            await process.stdin.drain()
        for process in processes:
            assert await asyncio.wait_for(process.stdout.readline(), 10) == b"ready\n"
        for process in processes:
            process.stdin.write(b"publish\n")
            await process.stdin.drain()
        results = await asyncio.wait_for(asyncio.gather(*(process.communicate() for process in processes)), 10)
        receipts = []
        for process, (stdout, stderr) in zip(processes, results, strict=True):
            assert process.returncode == 0, stderr.decode()
            receipts.append(json.loads(stdout))
        assert receipts[0] == receipts[1]
        assert receipts[0]["source_payload_sha256"] == hashlib.sha256(payload).hexdigest()
    finally:
        for process in processes:
            if process.returncode is None:
                process.kill()
            await process.wait()


async def test_receiver_deadline_cancels_unfinished_authority_read(delivery, monkeypatch):
    module, payload, receiver = objects(delivery)
    timeout = asyncio.timeout
    cancelled = []

    async def stalled(request):
        try:
            await asyncio.Future()
        finally:
            cancelled.append(True)

    monkeypatch.setattr(module.asyncio, "timeout", lambda value: timeout(0.01))
    delivery.admission.observe_current_bootstrap = stalled
    with pytest.raises(ValueError, match="delivery refused"):
        await receiver.receive(payload)
    assert cancelled == [True]
    assert list(delivery.node.iterdir()) == []


async def test_receiver_rejects_destination_replacement_during_authority_read(delivery):
    _module, payload, receiver = objects(delivery)
    observe = delivery.admission.observe_current_bootstrap
    with tempfile.TemporaryDirectory(prefix="loom-native-replacement-test-", dir="/dev/shm") as holder:
        async def replaced(request):
            result = await observe(request)
            delivery.node.rename(Path(holder) / "original")
            delivery.node.mkdir(mode=0o700)
            return result

        delivery.admission.observe_current_bootstrap = replaced
        with pytest.raises(ValueError):
            await receiver.receive(payload)
        assert list(delivery.node.iterdir()) == []
        assert list((Path(holder) / "original").iterdir()) == []
