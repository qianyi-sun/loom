"""Real TLS sockets prove bootstrap confidentiality and fixed peer authority."""

import asyncio
import hashlib
import json
import ssl
from contextlib import asynccontextmanager
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from importlib import import_module
from types import SimpleNamespace

import pytest
from cryptography import x509
from cryptography.hazmat.primitives import serialization

from loom_capacity_executor.pinned_admission_transport import PinnedAdmissionFileV1
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

    def pin(path):
        path.chmod(0o600)
        return PinnedAdmissionFileV1(path=str(path), sha256=hashlib.sha256(path.read_bytes()).hexdigest())

    def certificate_digest(path):
        return hashlib.sha256(x509.load_pem_x509_certificate(path.read_bytes()).public_bytes(serialization.Encoding.DER)).hexdigest()

    server_identity = module.NativeBootstrapTLSIdentity(ca=pin(ca_path), certificate=pin(server_cert), private_key=pin(server_key))
    client_identity = module.NativeBootstrapTLSIdentity(ca=pin(ca_path), certificate=pin(client_cert), private_key=pin(client_key))
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
            tls_paths=(ca_path, client_cert, client_key), server_identity=server_identity)
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
