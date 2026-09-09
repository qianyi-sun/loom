"""Real TLS wire and lifecycle contracts for authority-only S3 listing reads."""

import asyncio
import importlib
import ipaddress
import ssl
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest
import pytest_asyncio
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID


def _module():
    return importlib.import_module("loom_task_image_authority.bundle_s3_transport")


@pytest_asyncio.fixture
async def tls_listing(tmp_path):
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "disposable-listing")])
    now = datetime.now(UTC)
    cert = (
        x509.CertificateBuilder().subject_name(name).issuer_name(name).public_key(key.public_key())
        .serial_number(x509.random_serial_number()).not_valid_before(now - timedelta(days=1))
        .not_valid_after(now + timedelta(days=1))
        .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True)
        .add_extension(x509.SubjectAlternativeName([x509.IPAddress(ipaddress.ip_address("127.0.0.1"))]), critical=False)
        .sign(key, hashes.SHA256())
    )
    cert_file, key_file = tmp_path / "cert.pem", tmp_path / "key.pem"
    cert_file.write_bytes(cert.public_bytes(serialization.Encoding.PEM))
    key_file.write_bytes(key.private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption()))
    key_file.chmod(0o600)
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain(cert_file, key_file)
    state = SimpleNamespace(
        response=b"HTTP/1.1 200 OK\r\nContent-Length: 6\r\n\r\n<xml/>",
        received=asyncio.Event(), closed=asyncio.Event(), requests=[], tasks=set(),
        ca_file=cert_file,
    )

    async def handle(reader, writer):
        task = asyncio.current_task()
        state.tasks.add(task)
        try:
            request = await reader.readuntil(b"\r\n\r\n")
            state.requests.append(request)
            state.received.set()
            if state.response is not None:
                writer.write(state.response)
                await writer.drain()
            await reader.read()
        except (ConnectionError, asyncio.IncompleteReadError):
            pass
        finally:
            state.closed.set()
            writer.close()
            try:
                await writer.wait_closed()
            except ConnectionError:
                pass
            state.tasks.discard(task)

    server = await asyncio.start_server(handle, "127.0.0.1", 0, ssl=context)
    state.origin = f"https://127.0.0.1:{server.sockets[0].getsockname()[1]}"
    state.url = state.origin + "/loom-bundles?list-type=2&prefix=revision%2F&X-Amz-Signature=private-fixture"
    try:
        yield state
    finally:
        server.close()
        await server.wait_closed()
        tasks = tuple(state.tasks)
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


def _reader(fixture, **limits):
    module = _module()
    return module.HTTPSBundleListingReader(
        origin=fixture.origin, bucket="loom-bundles", ca_file=fixture.ca_file,
        limits=module.S3ListingReadLimits(**limits),
    )


def _deadline(seconds=5):
    return asyncio.get_running_loop().time() + seconds


async def test_reads_exact_bytes_over_verified_tls_and_closes_connection(tls_listing, caplog):
    async with _reader(tls_listing) as reader:
        assert await reader.fetch(tls_listing.url, deadline=_deadline()) == b"<xml/>"
    await asyncio.wait_for(tls_listing.closed.wait(), 2)
    assert b"GET /loom-bundles?list-type=2&prefix=revision%2F&X-Amz-Signature=private-fixture HTTP/1.1" in tls_listing.requests[0]
    assert b"Accept-Encoding: identity" in tls_listing.requests[0]
    assert "private-fixture" not in caplog.text


@pytest.mark.parametrize("response", [
    b"HTTP/1.1 302 Found\r\nLocation: https://private.example/\r\nContent-Length: 0\r\n\r\n",
    b"HTTP/1.1 103 Early Hints\r\n\r\nHTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n",
    b"HTTP/1.1 200 OK\r\nContent-Encoding: gzip\r\nContent-Length: 0\r\n\r\n",
    b"HTTP/1.1 200 OK\r\nContent-Length: 101\r\n\r\n",
    b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n65\r\n" + b"x" * 101 + b"\r\n0\r\n\r\n",
    b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n1\r\nx\r\n0\r\nPrivate: data\r\n\r\n",
    b"HTTP/1.1 200 OK\r\nPrivate: " + b"x" * 1024 + b"\r\nContent-Length: 0\r\n\r\n",
    b"HTTP/1.1 200 OK\r\nContent-Length: 1\r\nContent-Length: 2\r\n\r\nx",
], ids=lambda _: "unsafe-wire")
async def test_rejects_unsafe_response_without_echo_or_redirect(tls_listing, response):
    tls_listing.response = response
    async with _reader(tls_listing, maximum_body_bytes=100, maximum_header_bytes=512) as reader:
        with pytest.raises(RuntimeError) as error:
            await reader.fetch(tls_listing.url, deadline=_deadline())
    assert "private" not in str(error.value).lower()
    await asyncio.wait_for(tls_listing.closed.wait(), 2)
    assert len(tls_listing.requests) == 1


@pytest.mark.parametrize("end", ["cancel", "close", "idle", "total"])
async def test_cancellation_shutdown_and_deadlines_close_owned_socket(tls_listing, end):
    tls_listing.response = None
    async with _reader(tls_listing, idle_timeout_seconds=0.1 if end == "idle" else 5.0) as reader:
        request = asyncio.create_task(reader.fetch(tls_listing.url, deadline=_deadline(0.2 if end == "total" else 5)))
        await asyncio.wait_for(tls_listing.received.wait(), 2)
        if end == "cancel":
            request.cancel()
        elif end == "close":
            await reader.aclose()
        with pytest.raises(asyncio.CancelledError if end in {"cancel", "close"} else RuntimeError):
            await request
        await asyncio.wait_for(tls_listing.closed.wait(), 2)


async def test_queued_read_uses_same_total_deadline_without_opening_connection(tls_listing):
    tls_listing.response = None
    async with _reader(tls_listing, maximum_concurrent_reads=1) as reader:
        first = asyncio.create_task(reader.fetch(tls_listing.url, deadline=_deadline()))
        await asyncio.wait_for(tls_listing.received.wait(), 2)
        with pytest.raises(RuntimeError):
            await reader.fetch(tls_listing.url, deadline=_deadline(0.05))
        assert len(tls_listing.requests) == 1
        first.cancel()
        with pytest.raises(asyncio.CancelledError):
            await first


@pytest.mark.parametrize("suffix", ["/other-bucket?private=1", "/loom-bundles?private=1#fragment", "/loom-bundles", "/loom-bundles?private=1\n"])
async def test_rejects_unbound_url_before_connect(tls_listing, suffix):
    async with _reader(tls_listing) as reader:
        with pytest.raises(RuntimeError):
            await reader.fetch(tls_listing.origin + suffix, deadline=_deadline())
    assert not tls_listing.requests


async def test_expired_deadline_or_closed_reader_never_opens_connection(tls_listing):
    reader = _reader(tls_listing)
    with pytest.raises(RuntimeError):
        await reader.fetch(tls_listing.url, deadline=_deadline(-1))
    await reader.aclose()
    with pytest.raises(RuntimeError):
        await reader.fetch(tls_listing.url, deadline=_deadline())
    assert not tls_listing.requests
