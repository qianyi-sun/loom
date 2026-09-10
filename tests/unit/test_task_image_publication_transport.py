"""Actual mutual-TLS sockets exercise the dedicated signer client boundary."""

from __future__ import annotations

import asyncio
import importlib
import ssl
from contextlib import asynccontextmanager
from types import SimpleNamespace

import pytest
import rfc8785
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization

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
    if server:
        # The reused guard fixture trusts both localhost and its IP. This
        # narrower signer fixture deliberately trusts only the DNS identity.
        builder = x509.CertificateBuilder().subject_name(cert.subject).issuer_name(cert.issuer).public_key(key.public_key()).serial_number(cert.serial_number).not_valid_before(cert.not_valid_before_utc).not_valid_after(cert.not_valid_after_utc)
        for extension in cert.extensions:
            value = x509.SubjectAlternativeName([x509.DNSName(name)]) if isinstance(extension.value, x509.SubjectAlternativeName) else extension.value
            builder = builder.add_extension(value, critical=extension.critical)
        cert = builder.sign(ca_key, hashes.SHA256())
    cert_path, key_path = tmp_path / f"{name}.pem", tmp_path / f"{name}.key"
    cert_path.write_bytes(cert.public_bytes(serialization.Encoding.PEM))
    key_path.write_bytes(_key_bytes(key))
    return cert_path, key_path


@asynccontextmanager
async def signer_server(tmp_path, *, response=None, hold=False, early_close=False):
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
            if early_close:
                writer.close()
                await writer.wait_closed()
                return
            await reader.read()
            closed.set()
        except (ConnectionError, asyncio.IncompleteReadError):
            closed.set()
        finally:
            closed.set()
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


@pytest.mark.parametrize("framing", [
    b"Content-Length: 2\r\nContent-Length: 2",
    b"Content-Length: 2, 2",
    b"Content-Length: 2\r\nContent-Length: 3",
    b"Transfer-Encoding: chunked\r\nContent-Length: 2",
], ids=["identical", "list", "conflicting", "chunked-and-length"])
async def test_raw_response_framing_cannot_be_normalized_into_acceptance(tmp_path, framing):
    t = transport()
    c, _, _, _, _, _, unsigned, _ = setup_signing()
    body = b"2\r\n{}\r\n0\r\n\r\n" if b"chunked" in framing else b"{}"
    response = b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n" + framing + b"\r\n\r\n" + body
    async with signer_server(tmp_path, response=response) as (options, requests, _, closed, _):
        async with t.HTTPSPublicationSigner(**options) as signer:
            with pytest.raises(ValueError):
                await signer.sign_publication(c.canonical_publication_bytes(unsigned), maximum_reply_bytes=128 * 1024)
        assert len(requests) == 1
        await asyncio.wait_for(closed.wait(), 1)


async def test_truncated_response_rejected_without_body_in_error(tmp_path):
    t = transport()
    c, _, _, _, _, _, unsigned, _ = setup_signing()
    response = b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 100\r\n\r\nprivate-body"
    async with signer_server(tmp_path, response=response, early_close=True) as (options, requests, _, closed, _):
        async with t.HTTPSPublicationSigner(**options) as signer:
            with pytest.raises(ValueError) as exc:
                await signer.sign_publication(c.canonical_publication_bytes(unsigned), maximum_reply_bytes=128 * 1024)
            assert "private-body" not in str(exc.value)
        assert len(requests) == 1
        await asyncio.wait_for(closed.wait(), 1)


async def test_verifier_deadline_cancels_queued_request_before_connection(tmp_path):
    t = transport()
    c, s, _, key, state, distribution, unsigned, _ = setup_signing()
    async with signer_server(tmp_path, hold=True) as (options, requests, entered, closed, _):
        async with t.HTTPSPublicationSigner(**options, limits=t.PublicationSignerLimits(maximum_concurrent_requests=1)) as signer:
            first = asyncio.create_task(signer.sign_publication(c.canonical_publication_bytes(unsigned), maximum_reply_bytes=128 * 1024))
            await asyncio.wait_for(entered.wait(), 1)
            with pytest.raises(TimeoutError):
                await s.request_publication_signature(signer, unsigned, key=key, state=state, distribution=distribution, clock=lambda: NOW, timeout_seconds=0.1)
            assert len(requests) == 1
            assert not first.done()
            first.cancel()
            with pytest.raises(asyncio.CancelledError):
                await first
            await asyncio.wait_for(closed.wait(), 1)


async def test_cancelled_close_retains_shutdown_owner_and_can_be_joined(tmp_path, monkeypatch):
    t = transport()
    c, _, _, _, _, _, unsigned, _ = setup_signing()
    entered, cancelled, release, aborted = (asyncio.Event() for _ in range(4))

    async def handoff(*args, **kwargs):
        entered.set()
        try:
            await asyncio.Future()
        except asyncio.CancelledError:
            cancelled.set()
            await release.wait()
            return None, SimpleNamespace(transport=SimpleNamespace(abort=aborted.set))

    async with signer_server(tmp_path) as (options, requests, *_):
        monkeypatch.setattr(asyncio, "open_connection", handoff)
        signer = t.HTTPSPublicationSigner(**options)
        pending = asyncio.create_task(signer.sign_publication(c.canonical_publication_bytes(unsigned), maximum_reply_bytes=128 * 1024))
        await asyncio.wait_for(entered.wait(), 1)
        closer = asyncio.create_task(signer.aclose())
        try:
            await asyncio.wait_for(cancelled.wait(), 1)
            closer.cancel()
            with pytest.raises(asyncio.CancelledError):
                await closer
            assert not pending.done()
        finally:
            release.set()
            await signer.aclose()
            await asyncio.gather(pending, return_exceptions=True)
        assert pending.cancelled()
        await asyncio.wait_for(aborted.wait(), 1)
        assert not requests


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


async def test_close_joins_queued_and_active_requests_without_second_connection(tmp_path):
    t = transport()
    c, _, _, _, _, _, unsigned, _ = setup_signing()
    async with signer_server(tmp_path, hold=True) as (options, requests, entered, closed, _):
        signer = t.HTTPSPublicationSigner(**options, limits=t.PublicationSignerLimits(maximum_concurrent_requests=1))
        first = asyncio.create_task(signer.sign_publication(c.canonical_publication_bytes(unsigned), maximum_reply_bytes=128 * 1024))
        await asyncio.wait_for(entered.wait(), 1)
        second = asyncio.create_task(signer.sign_publication(c.canonical_publication_bytes(unsigned), maximum_reply_bytes=128 * 1024))
        await asyncio.sleep(0)
        await signer.aclose()
        results = await asyncio.gather(first, second, return_exceptions=True)
        assert all(isinstance(result, asyncio.CancelledError) for result in results)
        await asyncio.wait_for(closed.wait(), 1)
        assert len(requests) == 1


async def test_cancelled_connect_handoff_retains_writer_disposal_owner(tmp_path, monkeypatch):
    t = transport()
    c, _, _, _, _, _, unsigned, _ = setup_signing()
    entered, aborted = asyncio.Event(), asyncio.Event()

    async def handoff(*args, **kwargs):
        entered.set()
        try:
            await asyncio.Future()
        except asyncio.CancelledError:
            return None, SimpleNamespace(transport=SimpleNamespace(abort=aborted.set))

    async with signer_server(tmp_path) as (options, requests, *_):
        monkeypatch.setattr(asyncio, "open_connection", handoff)
        async with t.HTTPSPublicationSigner(**options) as signer:
            pending = asyncio.create_task(signer.sign_publication(c.canonical_publication_bytes(unsigned), maximum_reply_bytes=128 * 1024))
            await asyncio.wait_for(entered.wait(), 1)
            pending.cancel()
            with pytest.raises(asyncio.CancelledError):
                await pending
            await asyncio.wait_for(aborted.wait(), 1)
        assert not requests


@pytest.mark.parametrize("limits", [
    {"total_timeout_seconds": 10.1}, {"total_timeout_seconds": float("inf")},
    {"idle_timeout_seconds": float("nan")}, {"connect_timeout_seconds": 0.0},
    {"maximum_header_bytes": 65537}, {"maximum_wire_bytes": 1048577},
    {"maximum_concurrent_requests": 33}, {"maximum_concurrent_requests": True},
])
def test_transport_limits_are_finite_typed_and_bounded(limits):
    with pytest.raises(ValueError):
        transport().PublicationSignerLimits(**limits)


@pytest.mark.parametrize("status", [429, 502, 503, 504])
async def test_transient_service_failure_is_retryable_but_never_retried_here(tmp_path, status):
    t = transport()
    c, _, _, _, _, _, unsigned, _ = setup_signing()
    response = f"HTTP/1.1 {status} Unavailable\r\nContent-Length: 0\r\n\r\n".encode()
    async with signer_server(tmp_path, response=response) as (options, requests, _, closed, _):
        async with t.HTTPSPublicationSigner(**options) as signer:
            with pytest.raises(ConnectionError):
                await signer.sign_publication(c.canonical_publication_bytes(unsigned), maximum_reply_bytes=128 * 1024)
        assert len(requests) == 1
        await asyncio.wait_for(closed.wait(), 1)


@pytest.mark.parametrize("limit", [False, 0, 128 * 1024 + 1, 1.0])
async def test_reply_bound_rejected_before_network(tmp_path, limit):
    t = transport()
    c, _, _, _, _, _, unsigned, _ = setup_signing()
    async with signer_server(tmp_path) as (options, requests, *_):
        async with t.HTTPSPublicationSigner(**options) as signer:
            with pytest.raises(ValueError):
                await signer.sign_publication(c.canonical_publication_bytes(unsigned), maximum_reply_bytes=limit)
        assert not requests


@pytest.mark.parametrize("mode", ["valid-chunks", "body-limit", "wire-limit"])
async def test_chunked_body_and_wire_budgets_are_enforced_during_read(tmp_path, mode):
    t = transport()
    c, _, _, _, _, _, unsigned, reply = setup_signing()
    payload = rfc8785.dumps(reply)
    body = f"{len(payload):x}\r\n".encode() + payload + b"\r\n0\r\n\r\n"
    if mode == "wire-limit":
        body = b"1;waste=" + b"x" * 4096 + b"\r\nx\r\n0\r\n\r\n"
    response = b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nTransfer-Encoding: chunked\r\n\r\n" + body
    async with signer_server(tmp_path, response=response) as (options, requests, _, closed, _):
        async with t.HTTPSPublicationSigner(**options, limits=t.PublicationSignerLimits(maximum_wire_bytes=4096 if mode == "wire-limit" else 1024 * 1024)) as signer:
            pending = signer.sign_publication(c.canonical_publication_bytes(unsigned), maximum_reply_bytes=len(payload) - 1 if mode == "body-limit" else 128 * 1024)
            if mode == "valid-chunks":
                assert await pending == payload
            else:
                with pytest.raises(ValueError):
                    await pending
        assert len(requests) == 1
        await asyncio.wait_for(closed.wait(), 1)
