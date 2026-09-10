"""Real socket peer identities, bounded framing and lifecycle of the signer."""

import asyncio
import hashlib
import importlib
import ssl
from contextlib import asynccontextmanager

import pytest
import rfc8785
from cryptography import x509
from cryptography.hazmat.primitives import serialization

from tests.unit.test_task_image_keyset_signing_request import payload
from tests.unit.test_task_image_publication_signing import setup_signing
from tests.unit.test_task_image_publication_transport import _identity, _new_ca


def module():
    name = "loom_task_image_signer.server"
    assert importlib.util.find_spec(name) is not None, "dedicated authenticated signer listener is missing"
    return importlib.import_module(name)


class Policy:
    def __init__(self):
        self.calls = []
        self.entered, self.release = asyncio.Event(), asyncio.Event()
        self.release.set()
        self.error = None

    async def _call(self, operation, wire):
        self.calls.append((operation, wire))
        self.entered.set()
        await self.release.wait()
        if self.error:
            raise self.error
        return b'{}'

    async def sign_keyset(self, wire):
        return await self._call("keyset", wire)

    async def sign_publication(self, wire):
        return await self._call("publication", wire)


@asynccontextmanager
async def service(tmp_path, *, limits=None):
    m = module()
    ca_key, ca = _new_ca()
    ca_path = tmp_path / "ca.pem"
    ca_path.write_bytes(ca.public_bytes(serialization.Encoding.PEM))
    server_cert, server_key = _identity(tmp_path, "localhost", ca_key, ca, server=True)
    identities, grants = {}, {}
    for name in ("keyset", "publication", "stranger"):
        cert, key = _identity(tmp_path, name, ca_key, ca, server=False)
        identities[name] = dict(ca_file=ca_path, client_cert_file=cert, client_key_file=key)
        if name != "stranger":
            der = x509.load_pem_x509_certificate(cert.read_bytes()).public_bytes(serialization.Encoding.DER)
            grants[hashlib.sha256(der).hexdigest()] = frozenset({name})
    policy = Policy()
    server = m.SignerServer(
        policy, ca_file=ca_path, certificate_file=server_cert, private_key_file=server_key,
        peer_operations=grants, **({"limits": limits} if limits else {}),
    )
    await server.start(host="127.0.0.1", port=0)
    for options in identities.values():
        options["origin"] = f"https://localhost:{server.port}"
    try:
        yield server, policy, identities
    finally:
        await server.aclose()


def context(options):
    tls = ssl.create_default_context(cafile=options["ca_file"])
    tls.minimum_version = ssl.TLSVersion.TLSv1_3
    tls.load_cert_chain(options["client_cert_file"], options["client_key_file"])
    return tls


async def connect(server, options):
    return await asyncio.open_connection("127.0.0.1", server.port, ssl=context(options), server_hostname="localhost")


async def test_real_peer_certificate_pins_separate_the_two_fixed_operations(tmp_path):
    module()
    t = importlib.import_module("loom_task_image_authority.publication_transport")
    unsigned = setup_signing()[6]
    c = importlib.import_module("loom_task_image_authority.publication_contracts")
    async with service(tmp_path) as (_, policy, identities):
        async with t.HTTPSKeysetSigner(**identities["keyset"]) as client:
            assert await client.sign_keyset(rfc8785.dumps(payload()), maximum_reply_bytes=131072) == b'{}'
        async with t.HTTPSPublicationSigner(**identities["publication"]) as client:
            assert await client.sign_publication(c.canonical_publication_bytes(unsigned), maximum_reply_bytes=131072) == b'{}'
        for name in ("publication", "stranger"):
            async with t.HTTPSKeysetSigner(**identities[name]) as client:
                with pytest.raises(ValueError):
                    await client.sign_keyset(rfc8785.dumps(payload()), maximum_reply_bytes=131072)
        assert [kind for kind, _ in policy.calls] == ["keyset", "publication"]


@pytest.mark.parametrize("extra", [
    b"Content-Length: 2\r\nContent-Length: 2\r\n",
    b"Content-Length: 2,2\r\n", b"Content-Length: 2\r\nTransfer-Encoding: chunked\r\n",
    b"Transfer-Encoding: chunked\r\n", b"Content-Length: 2\r\n Folded: value\r\n",
    b"Content-Length: 2\r\nContent-Encoding: gzip\r\n", b"Content-Length: 2\r\nExpect: 100-continue\r\n",
    b"Content-Length: 2\r\nUpgrade: websocket\r\n", b"Content-Length: 2\r\nX-Forwarded-Client-Cert: trusted\r\n",
])
async def test_raw_framing_and_forwarded_identity_refused_before_policy(tmp_path, extra):
    async with service(tmp_path) as (server, policy, identities):
        reader, writer = await connect(server, identities["keyset"])
        try:
            writer.write(b"POST /v1/keysets/sign HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\n" + extra + b"\r\n{}")
            await writer.drain()
            response = await asyncio.wait_for(reader.read(), 2)
            assert response.startswith(b"HTTP/1.1 400 ")
            assert not policy.calls
        finally:
            writer.transport.abort()


async def test_pipelined_second_operation_never_reaches_policy(tmp_path):
    async with service(tmp_path) as (server, policy, identities):
        reader, writer = await connect(server, identities["keyset"])
        body = rfc8785.dumps(payload())
        request = b"POST /v1/keysets/sign HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: " + str(len(body)).encode() + b"\r\n\r\n" + body
        try:
            writer.write(request * 2)
            await writer.drain()
            await asyncio.wait_for(reader.read(), 2)
            assert len(policy.calls) <= 1
        finally:
            writer.transport.abort()


@pytest.mark.parametrize("error,status", [(ValueError("private-details"), 400), (ConnectionError("secret-endpoint"), 503), (TimeoutError("secret-endpoint"), 503)])
async def test_errors_are_sanitized_and_transient_status_is_explicit(tmp_path, error, status):
    async with service(tmp_path) as (server, policy, identities):
        policy.error = error
        reader, writer = await connect(server, identities["keyset"])
        body = rfc8785.dumps(payload())
        try:
            writer.write(b"POST /v1/keysets/sign HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: " + str(len(body)).encode() + b"\r\n\r\n" + body)
            await writer.drain()
            response = await asyncio.wait_for(reader.read(), 2)
            assert response.startswith(f"HTTP/1.1 {status} ".encode())
            assert b"private-details" not in response and b"secret-endpoint" not in response
        finally:
            writer.transport.abort()


async def test_shutdown_cancels_and_joins_accepted_handshake_and_policy_work(tmp_path):
    t = importlib.import_module("loom_task_image_authority.publication_transport")
    async with service(tmp_path) as (server, policy, identities):
        policy.release.clear()
        async with t.HTTPSKeysetSigner(**identities["keyset"]) as client:
            task = asyncio.create_task(client.sign_keyset(rfc8785.dumps(payload()), maximum_reply_bytes=131072))
            reader, writer = await asyncio.open_connection("127.0.0.1", server.port)
            try:
                await asyncio.wait_for(policy.entered.wait(), 2)
                await asyncio.wait_for(server.aclose(), 2)
                with pytest.raises((ValueError, ConnectionError)):
                    await task
                assert await asyncio.wait_for(reader.read(), 2) == b""
                assert not server.active_connections
            finally:
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
                writer.transport.abort()


async def test_prehandshake_admission_is_bounded_and_recovers(tmp_path):
    m = module()
    async with service(tmp_path, limits=m.SignerServerLimits(maximum_connections=1, handshake_seconds=0.2)) as (server, policy, identities):
        _, writer = await asyncio.open_connection("127.0.0.1", server.port)
        try:
            t = importlib.import_module("loom_task_image_authority.publication_transport")
            async with t.HTTPSKeysetSigner(**identities["keyset"]) as client:
                task = asyncio.create_task(client.sign_keyset(rfc8785.dumps(payload()), maximum_reply_bytes=131072))
                await asyncio.sleep(0.05)
                assert server.active_connections <= 1
                assert not policy.calls
                assert await asyncio.wait_for(task, 2) == b'{}'
        finally:
            writer.transport.abort()
