"""Real TLS sockets prove bootstrap confidentiality and fixed peer authority."""

import asyncio
import hashlib
import json
import ssl
import subprocess
import sys
import time
from contextlib import asynccontextmanager
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from importlib import import_module
from types import SimpleNamespace

import pytest
from cryptography import x509
from cryptography.hazmat.primitives import serialization

from loom_capacity_executor.pinned_admission_transport import PinnedAdmissionFileV1
from loom_capacity_executor.bootstrap_handoff import claim_bootstrap_handoff_launch, consume_bootstrap_handoff
from tests.unit.test_native_bootstrap_delivery import delivery as delivery
from tests.unit.test_native_bootstrap_delivery import objects
from tests.unit.test_task_image_publication_transport import _identity, _new_ca


@asynccontextmanager
async def service(delivery, monkeypatch, *, limits=None, peer_change=None):
    module = import_module("loom_capacity_executor.native_bootstrap_transport")
    # Only the process hardening prerequisite is substituted in this shared
    # pytest process. Dedicated subprocess coverage verifies that prerequisite.
    monkeypatch.setattr(module, "_assert_private_process", lambda: None)
    ca_key, ca = _new_ca()
    ca_path = delivery.controller / "tls-ca.pem"
    ca_path.write_bytes(ca.public_bytes(serialization.Encoding.PEM))
    server_cert, server_key = _identity(delivery.controller, "localhost", ca_key, ca, server=True)
    client_cert, client_key = _identity(delivery.controller, "controller", ca_key, ca, server=False)
    stranger_cert, stranger_key = _identity(delivery.controller, "stranger", ca_key, ca, server=False)

    def pin(path):
        path.chmod(0o600)
        return PinnedAdmissionFileV1(path=str(path), sha256=hashlib.sha256(path.read_bytes()).hexdigest())

    def certificate_digest(path):
        return hashlib.sha256(x509.load_pem_x509_certificate(path.read_bytes()).public_bytes(serialization.Encoding.DER)).hexdigest()

    server_identity = module.NativeBootstrapTLSIdentity(ca=pin(ca_path), certificate=pin(server_cert), private_key=pin(server_key))
    client_identity = module.NativeBootstrapTLSIdentity(ca=pin(ca_path), certificate=pin(client_cert), private_key=pin(client_key))
    stranger_identity = module.NativeBootstrapTLSIdentity(ca=pin(ca_path), certificate=pin(stranger_cert), private_key=pin(stranger_key))
    binding = delivery.physical.binding
    peer = module.NativeBootstrapPeer(pool_id=binding.pool_id, executor_id=binding.executor_id,
        executor_incarnation=binding.executor_incarnation, operations=frozenset({"deliver", "status"}),
        expires_at=datetime.now(UTC) + timedelta(hours=1))
    if peer_change:
        peer = replace(peer, **peer_change)
    storage, payload, receiver = objects(delivery)
    calls = []

    class Operations:
        async def receive(self, raw):
            calls.append("deliver")
            return await receiver.receive(raw)

        async def observe_receipt(self, raw):
            calls.append("status")
            return await receiver.observe_receipt(raw)

    server = module.NativeBootstrapTLSServer(Operations(), identity=server_identity,
        target_node=binding.node_ids[0], pool_id=binding.pool_id,
        trusted_release_sha256=binding.execution.trusted_fleet_release_sha256,
        peers={certificate_digest(client_cert): peer}, **({"limits": limits} if limits else {}))
    await server.start(host="127.0.0.1", port=0)
    route = module.NativeBootstrapRoute(address="127.0.0.1", port=server.port, hostname="localhost",
        target_node=binding.node_ids[0], pool_id=binding.pool_id,
        trusted_release_sha256=binding.execution.trusted_fleet_release_sha256,
        server_certificate_sha256=certificate_digest(server_cert), expires_at=datetime.now(UTC) + timedelta(hours=1))
    try:
        yield SimpleNamespace(module=module, server=server, identity=client_identity, route=route,
            storage=storage, payload=payload, calls=calls, receiver=receiver,
            tls_paths=(ca_path, client_cert, client_key), server_identity=server_identity, stranger_identity=stranger_identity)
    finally:
        await server.aclose()


async def test_real_mtls_delivery_and_capability_free_status(delivery, monkeypatch):
    async with service(delivery, monkeypatch) as setup:
        expected = setup.storage.expected_native_delivery_receipt(setup.payload)
        query = setup.storage.encode_native_delivery_query(delivery.physical, expected)
        client = setup.module.NativeBootstrapTLSClient(route=setup.route, identity=setup.identity)
        try:
            assert await client.observe_receipt(query) is None
            assert await client.deliver(setup.payload) == expected
            assert await client.observe_receipt(query) == expected
            assert setup.calls == ["status", "deliver", "status"]
        finally:
            await client.aclose()


@pytest.mark.parametrize("field,value", (("target_node", "foreign"), ("pool_id", "foreign"),
    ("trusted_release_sha256", "f" * 64), ("hostname", "foreign.invalid"), ("server_certificate_sha256", "f" * 64)))
async def test_wrong_destination_never_reaches_receiver(delivery, monkeypatch, field, value):
    async with service(delivery, monkeypatch) as setup:
        client = setup.module.NativeBootstrapTLSClient(route=replace(setup.route, **{field: value}), identity=setup.identity)
        try:
            with pytest.raises(ValueError):
                await client.deliver(setup.payload)
            assert not setup.calls
            assert list(delivery.node.iterdir()) == []
        finally:
            await client.aclose()


@pytest.mark.parametrize("change", ({"executor_id": "foreign"}, {"pool_id": "foreign"},
    {"operations": frozenset({"status"})}, {"expires_at": datetime(2020, 1, 1, tzinfo=UTC)}))
async def test_peer_scope_checked_before_any_receiver_operation(delivery, monkeypatch, change):
    async with service(delivery, monkeypatch, peer_change=change) as setup:
        client = setup.module.NativeBootstrapTLSClient(route=setup.route, identity=setup.identity)
        try:
            with pytest.raises(ValueError):
                await client.deliver(setup.payload)
            assert not setup.calls
            assert list(delivery.node.iterdir()) == []
        finally:
            await client.aclose()


async def test_wrong_server_pin_receives_zero_application_bytes(delivery, monkeypatch):
    async with service(delivery, monkeypatch) as setup:
        observed = asyncio.Future()
        context = setup.module._tls_context(setup.server_identity, server=True)

        async def read_only(reader, writer):
            try:
                observed.set_result(await reader.read(1))
            except (OSError, ssl.SSLError) as error:
                observed.set_exception(error)
            finally:
                writer.transport.abort()

        server = await asyncio.start_server(read_only, "127.0.0.1", 0, ssl=context)
        route = replace(setup.route, port=server.sockets[0].getsockname()[1], server_certificate_sha256="f" * 64)
        client = setup.module.NativeBootstrapTLSClient(route=route, identity=setup.identity)
        try:
            with pytest.raises(ValueError):
                await client.deliver(setup.payload)
            assert await asyncio.wait_for(observed, 3) == b""
        finally:
            await client.aclose()
            server.close()
            await server.wait_closed()


async def test_prehandshake_slots_are_bounded_and_shutdown_joins_them(delivery, monkeypatch):
    module = import_module("loom_capacity_executor.native_bootstrap_transport")
    limits = module.NativeBootstrapTransportLimits(maximum_connections=1, handshake_seconds=0.2)
    async with service(delivery, monkeypatch, limits=limits) as setup:
        reader, writer = await asyncio.open_connection("127.0.0.1", setup.server.port)
        try:
            for _ in range(20):
                if setup.server.active_connections:
                    break
                await asyncio.sleep(0.01)
            assert setup.server.active_connections == 1
            await asyncio.wait_for(setup.server.aclose(), 3)
            assert setup.server.active_connections == 0
            assert await asyncio.wait_for(reader.read(), 3) == b""
            assert not setup.calls
        finally:
            writer.transport.abort()


async def test_raw_bad_frames_never_reach_receiver(delivery, monkeypatch):
    async with service(delivery, monkeypatch) as setup:
        context = setup.module._tls_context(setup.identity, server=False)
        for wire in (b"WRNGD\0\0\0\2{}", b"LNB1X\0\0\0\2{}", b"LNB1D\xff\xff\xff\xff", b"LNB1D\0\0\0\2{}"):
            reader, writer = await asyncio.open_connection("127.0.0.1", setup.server.port,
                ssl=context, server_hostname="localhost")
            try:
                writer.write(wire)
                await writer.drain()
                response = await asyncio.wait_for(reader.read(4096), 3)
                assert response in (b"", b"LNR1F\0\0\0\0")
            finally:
                writer.transport.abort()
        assert not setup.calls
        assert list(delivery.node.iterdir()) == []


async def test_process_hardening_precedes_real_tls_key_loading(delivery, monkeypatch):
    async with service(delivery, monkeypatch) as setup:
        config = {name: getattr(setup.identity, name).model_dump(mode="json") for name in ("ca", "certificate", "private_key")}
        probe = """
import ctypes, json, sys
from loom_capacity_executor import native_bootstrap_transport as module
from loom_capacity_executor.native_worker_bootstrap import _disable_bootstrap_dumps
from loom_capacity_executor.pinned_admission_transport import PinnedAdmissionFileV1
assert ctypes.CDLL(None).prctl(4, 1, 0, 0, 0) == 0
try:
    module._assert_private_process()
except ValueError:
    pass
else:
    raise AssertionError('unhardened process accepted')
_disable_bootstrap_dumps()
config = json.loads(sys.stdin.read())
identity = module.NativeBootstrapTLSIdentity(**{key: PinnedAdmissionFileV1.model_validate(value) for key, value in config.items()})
context = module._tls_context(identity, server=False)
assert context.check_hostname and context.verify_mode == 2
print('protected TLS identity loaded')
"""
        result = await asyncio.to_thread(subprocess.run, [sys.executable, "-B", "-c", probe],
            input=json.dumps(config).encode(), capture_output=True, check=False, timeout=10)
        assert result.returncode == 0, result.stderr.decode()
        assert result.stdout == b"protected TLS identity loaded\n"
        assert result.stderr == b""


async def test_same_ca_unpinned_client_certificate_invokes_no_operations(delivery, monkeypatch):
    async with service(delivery, monkeypatch) as setup:
        # Correct CA and client EKU, but not an independently approved peer pin.
        client = setup.module.NativeBootstrapTLSClient(route=setup.route, identity=setup.stranger_identity)
        try:
            with pytest.raises(ValueError):
                await client.deliver(setup.payload)
            assert not setup.calls
        finally:
            await client.aclose()


async def test_missing_client_certificate_invokes_no_operations(delivery, monkeypatch):
    async with service(delivery, monkeypatch) as setup:
        ca, _cert, _key = setup.tls_paths
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        context.load_verify_locations(cafile=str(ca))
        context.set_alpn_protocols(["loom-native-bootstrap/1"])
        writer = None
        try:
            reader, writer = await asyncio.open_connection("127.0.0.1", setup.server.port,
                ssl=context, server_hostname="localhost")
            writer.write(b"LNB1D" + len(setup.payload).to_bytes(4, "big") + setup.payload)
            await writer.drain()
            assert await asyncio.wait_for(reader.read(1), 3) == b""
        except (OSError, ssl.SSLError):
            pass
        finally:
            if writer is not None:
                writer.transport.abort()
        assert not setup.calls


async def test_disconnect_after_publication_recovers_without_recreating_capability(delivery, monkeypatch):
    async with service(delivery, monkeypatch) as setup:
        completed, release = asyncio.Event(), asyncio.Event()
        receive = setup.server._operations.receive

        async def lost_reply(raw):
            result = await receive(raw)
            completed.set()
            await release.wait()
            return result

        monkeypatch.setattr(setup.server._operations, "receive", lost_reply)
        client = setup.module.NativeBootstrapTLSClient(route=setup.route, identity=setup.identity)
        task = asyncio.create_task(client.deliver(setup.payload))
        try:
            await asyncio.wait_for(completed.wait(), 3)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
            release.set()
            expected = setup.storage.expected_native_delivery_receipt(setup.payload)
            directory = setup.storage.native_delivery_directory(delivery.node, expected.reference)
            await consume_bootstrap_handoff(directory, expected.reference, delivery.physical, delivery.admission, now=lambda: delivery.now)
            claim_bootstrap_handoff_launch(directory, expected.reference, delivery.physical, delivery.admission, now=lambda: delivery.now)
            before = {path.name: (path.stat().st_ino, path.read_bytes()) for path in directory.iterdir()}
            delivery.now += timedelta(days=1)
            query = setup.storage.encode_native_delivery_query(delivery.physical, expected)
            assert await client.observe_receipt(query) == expected
            assert before == {path.name: (path.stat().st_ino, path.read_bytes()) for path in directory.iterdir()}
            assert setup.calls == ["deliver", "status"]
        finally:
            release.set()
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            await client.aclose()


async def test_server_shutdown_cancels_operation_and_client_queue(delivery, monkeypatch):
    async with service(delivery, monkeypatch) as setup:
        entered, cancelled = asyncio.Event(), asyncio.Event()

        async def blocked(raw):
            try:
                entered.set()
                await asyncio.Future()
            finally:
                cancelled.set()

        monkeypatch.setattr(setup.server._operations, "receive", blocked)
        limits = setup.module.NativeBootstrapTransportLimits(maximum_operations=1)
        client = setup.module.NativeBootstrapTLSClient(route=setup.route, identity=setup.identity, limits=limits)
        first = asyncio.create_task(client.deliver(setup.payload))
        second = None
        try:
            await asyncio.wait_for(entered.wait(), 3)
            second = asyncio.create_task(client.deliver(setup.payload))
            await asyncio.wait_for(client.aclose(), 3)
            assert first.cancelled() and second.cancelled()
            await asyncio.wait_for(setup.server.aclose(), 3)
            assert cancelled.is_set() and setup.server.active_connections == 0
            assert list(delivery.node.iterdir()) == []
        finally:
            for task in (first, second):
                if task is not None:
                    task.cancel()
                    await asyncio.gather(task, return_exceptions=True)
            await client.aclose()


async def test_total_deadline_includes_operation_and_client_wait(delivery, monkeypatch):
    module = import_module("loom_capacity_executor.native_bootstrap_transport")
    limits = module.NativeBootstrapTransportLimits(total_seconds=0.15)
    async with service(delivery, monkeypatch, limits=limits) as setup:
        cancelled = asyncio.Event()

        async def blocked(raw):
            try:
                await asyncio.Future()
            finally:
                cancelled.set()

        monkeypatch.setattr(setup.server._operations, "receive", blocked)
        client = setup.module.NativeBootstrapTLSClient(route=setup.route, identity=setup.identity, limits=limits)
        try:
            with pytest.raises(ValueError):
                await asyncio.wait_for(client.deliver(setup.payload), 3)
            await asyncio.wait_for(cancelled.wait(), 3)
            assert list(delivery.node.iterdir()) == []
        finally:
            await client.aclose()


async def test_synchronous_operation_cannot_send_success_after_server_deadline(delivery, monkeypatch):
    module = import_module("loom_capacity_executor.native_bootstrap_transport")
    limits = module.NativeBootstrapTransportLimits(total_seconds=0.1)
    async with service(delivery, monkeypatch, limits=limits) as setup:
        expected = setup.storage.expected_native_delivery_receipt(setup.payload)

        async def delayed(raw):
            # An async adapter may contain non-yielding filesystem work. It
            # must not report on-time success merely by outrunning cancellation.
            time.sleep(0.2)
            return expected

        monkeypatch.setattr(setup.server._operations, "receive", delayed)
        client = setup.module.NativeBootstrapTLSClient(route=setup.route, identity=setup.identity)
        try:
            with pytest.raises(ValueError):
                await asyncio.wait_for(client.deliver(setup.payload), 3)
        finally:
            await client.aclose()


async def test_listener_failure_is_observable_and_closes_listener(delivery, monkeypatch):
    loop = asyncio.get_running_loop()

    async def failed_accept(listener):
        raise OSError("disposable-private-accept-detail")

    monkeypatch.setattr(loop, "sock_accept", failed_accept)
    async with service(delivery, monkeypatch) as setup:
        with pytest.raises(ValueError, match="unavailable or refused") as caught:
            await asyncio.wait_for(setup.server.wait(), 3)
        assert "private-accept-detail" not in str(caught.value)
        assert setup.server._listener.fileno() == -1
        assert setup.server.active_connections == 0
