"""Actual mutual-TLS sockets exercise the dedicated signer client boundary."""

from __future__ import annotations

import asyncio
import importlib
import ssl
from contextlib import asynccontextmanager

import pytest
import rfc8785
from cryptography.hazmat.primitives import serialization

from tests.unit.test_task_image_builder_guard_authority import (
    _certificate,
    _key_bytes,
    _new_ca,
)
from tests.unit.test_task_image_publication_signing import NOW, setup_signing


def transport():
    name = "loom_task_image_authority.publication_transport"
    assert importlib.util.find_spec(name) is not None, "authenticated signer transport is missing"
    return importlib.import_module(name)


def _identity(tmp_path, name, ca_key, ca, *, server):
    key, cert = _certificate(name, ca_key, ca, server=server)
    cert_path, key_path = tmp_path / f"{name}.pem", tmp_path / f"{name}.key"
    cert_path.write_bytes(cert.public_bytes(serialization.Encoding.PEM))
    key_path.write_bytes(_key_bytes(key))
    return cert_path, key_path


@asynccontextmanager
async def signer_server(tmp_path, *, response=None, hold=False):
    ca_key, ca = _new_ca()
    ca_path = tmp_path / "ca.pem"
    ca_path.write_bytes(ca.public_bytes(serialization.Encoding.PEM))
    server_cert, server_key = _identity(tmp_path, "localhost", ca_key, ca, server=True)
    client_cert, client_key = _identity(tmp_path, "authority", ca_key, ca, server=False)
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.minimum_version = ssl.TLSVersion.TLSv1_3
    context.verify_mode = ssl.CERT_REQUIRED
    context.load_verify_locations(cafile=str(ca_path))
    context.load_cert_chain(server_cert, server_key)
    entered, closed = asyncio.Event(), asyncio.Event()
    requests, tasks = [], set()
    reply = rfc8785.dumps(setup_signing()[-1])
    if response is None:
        response = b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: " + str(len(reply)).encode() + b"\r\n\r\n" + reply

    async def handle(reader, writer):
        task = asyncio.current_task()
        tasks.add(task)
        try:
            head = await reader.readuntil(b"\r\n\r\n")
            length = next(int(line.split(b":", 1)[1]) for line in head.split(b"\r\n") if line.lower().startswith(b"content-length:"))
            body = await reader.readexactly(length)
            requests.append((head, body, writer.get_extra_info("peercert")))
            entered.set()
            if not hold:
                writer.write(response)
                await writer.drain()
            await reader.read()
            closed.set()
        except (ConnectionError, asyncio.IncompleteReadError):
            closed.set()
        finally:
            writer.transport.abort()
            tasks.discard(task)

    server = await asyncio.start_server(handle, "127.0.0.1", 0, ssl=context)
    options = dict(origin=f"https://localhost:{server.sockets[0].getsockname()[1]}", ca_file=ca_path, client_cert_file=client_cert, client_key_file=client_key)
    try:
        yield options, requests, entered, closed, reply
    finally:
        server.close()
        await server.wait_closed()
        for task in tuple(tasks):
            task.cancel()
        await asyncio.gather(*tuple(tasks), return_exceptions=True)


async def test_real_mtls_client_sends_only_canonical_input_to_fixed_operation(tmp_path, monkeypatch):
    t = transport()
    c, s, _private, key, state, distribution, unsigned, reply = setup_signing()
    payload = rfc8785.dumps(reply)
    response = b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: " + str(len(payload)).encode() + b"\r\n\r\n" + payload
    monkeypatch.setenv("HTTPS_PROXY", "http://127.0.0.1:1")
    async with signer_server(tmp_path, response=response) as (options, requests, _, closed, _):
        async with t.HTTPSPublicationSigner(**options) as signer:
            result = await s.request_publication_signature(signer, unsigned, key=key, state=state, distribution=distribution, clock=lambda: NOW)
        assert result.statement.signing_key_id == key.key_id
        assert len(requests) == 1
        head, body, peer = requests[0]
        assert head.startswith(b"POST /v1/publications/sign HTTP/1.1\r\n")
        assert b"Accept-Encoding: identity\r\n" in head
        assert body == c.canonical_publication_bytes(unsigned)
        assert peer["subject"][0][0] == ("commonName", "authority")
        await asyncio.wait_for(closed.wait(), 1)


@pytest.mark.parametrize("payload", [b"{}", b"not json", b"x" * (128 * 1024), b'{"schema":"arbitrary-signing"}'], ids=["empty", "not-json", "oversize", "wrong-schema"])
async def test_invalid_input_rejected_before_network(tmp_path, payload):
    t = transport()
    async with signer_server(tmp_path) as (options, requests, *_):
        async with t.HTTPSPublicationSigner(**options) as signer:
            with pytest.raises(ValueError):
                await signer.sign_publication(payload, maximum_reply_bytes=128 * 1024)
        assert not requests


@pytest.mark.parametrize("kind", ["server_ca", "hostname", "client_identity"])
async def test_mtls_rejects_untrusted_identity_before_http(tmp_path, kind):
    t = transport()
    c, _, _, _, _, _, unsigned, _ = setup_signing()
    async with signer_server(tmp_path) as (options, requests, *_):
        foreign_key, foreign_ca = _new_ca()
        if kind == "server_ca":
            path = tmp_path / "foreign-ca.pem"
            path.write_bytes(foreign_ca.public_bytes(serialization.Encoding.PEM))
            options["ca_file"] = path
        elif kind == "hostname":
            options["origin"] = options["origin"].replace("localhost", "127.0.0.1")
        else:
            options["client_cert_file"], options["client_key_file"] = _identity(tmp_path, "foreign", foreign_key, foreign_ca, server=False)
        async with t.HTTPSPublicationSigner(**options) as signer:
            with pytest.raises((ValueError, ConnectionError)):
                await signer.sign_publication(c.canonical_publication_bytes(unsigned), maximum_reply_bytes=128 * 1024)
        assert not requests


@pytest.mark.parametrize("response", [
    b"HTTP/1.1 302 Found\r\nLocation: https://other.invalid/\r\nContent-Length: 0\r\n\r\n",
    b"HTTP/1.1 401 Unauthorized\r\nContent-Length: 0\r\n\r\n",
    b"HTTP/1.1 100 Continue\r\n\r\n",
    b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 200000\r\n\r\n",
    b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Encoding: gzip\r\nContent-Length: 2\r\n\r\n{}",
    b"HTTP/1.1 200 OK\r\nContent-Type: text/html\r\nContent-Length: 2\r\n\r\n{}",
    b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Type: application/json\r\nContent-Length: 2\r\n\r\n{}",
    b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nTransfer-Encoding: chunked\r\n\r\n2\r\n{}\r\n0\r\nX-Trailer: forbidden\r\n\r\n",
    b"HTTP/1.1 200 OK\r\nX-Large: " + b"x" * 33000,
], ids=["redirect", "unauthorized", "informational", "oversize", "compressed", "html", "duplicate", "trailer", "large-header"])
async def test_protocol_rejection_is_bounded_sanitized_and_closes_io(tmp_path, response):
    t = transport()
    c, _, _, _, _, _, unsigned, _ = setup_signing()
    async with signer_server(tmp_path, response=response) as (options, requests, _, closed, _):
        async with t.HTTPSPublicationSigner(**options) as signer:
            with pytest.raises(ValueError, match="publication signer") as exc:
                await signer.sign_publication(c.canonical_publication_bytes(unsigned), maximum_reply_bytes=128 * 1024)
            assert "other.invalid" not in str(exc.value)
            assert exc.value.__cause__ is None
        assert len(requests) == 1
        await asyncio.wait_for(closed.wait(), 1)


@pytest.mark.parametrize("finish", ["timeout", "cancel", "close"])
async def test_timeout_cancellation_and_close_join_io(tmp_path, finish):
    t = transport()
    c, _, _, _, _, _, unsigned, _ = setup_signing()
    async with signer_server(tmp_path, hold=True) as (options, requests, entered, closed, _):
        async with t.HTTPSPublicationSigner(**options, limits=t.PublicationSignerLimits(total_timeout_seconds=0.3)) as signer:
            pending = asyncio.create_task(signer.sign_publication(c.canonical_publication_bytes(unsigned), maximum_reply_bytes=128 * 1024))
            await asyncio.wait_for(entered.wait(), 1)
            if finish == "cancel":
                pending.cancel()
            elif finish == "close":
                await signer.aclose()
            with pytest.raises(TimeoutError if finish == "timeout" else asyncio.CancelledError):
                await pending
            await asyncio.wait_for(closed.wait(), 1)
            assert len(requests) == 1
        with pytest.raises(ValueError):
            await signer.sign_publication(c.canonical_publication_bytes(unsigned), maximum_reply_bytes=128 * 1024)
