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


@pytest.fixture(params=["listing", "manifest"])
def fetch_request(request, tls_listing):
    """Exercise shared wire/lifecycle guarantees through both public entrypoints."""
    async def fetch(reader, *, deadline):
        if request.param == "listing":
            return await reader.fetch(tls_listing.url, deadline=deadline)
        digest = "a" * 64
        url = tls_listing.origin + f"/loom-bundles/loom-bundle-manifests/v1/sha256/{digest}.json?X-Amz-Signature=private-fixture"
        return await reader.fetch_manifest(url, expected_sha256=digest, deadline=deadline)
    return fetch


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
    b"HTTP/1.1 200 " + b"X" * (513 - len(b"HTTP/1.1 200 \r\nContent-Length: 1\r\n\r\n")) + b"\r\nContent-Length: 1\r\n\r\nx",
    b"HTTP/1.1 200 OK\r\nX:" + b" " * (513 - len(b"HTTP/1.1 200 OK\r\nX:a\r\nContent-Length: 1\r\n\r\n")) + b"a\r\nContent-Length: 1\r\n\r\nx",
], ids=lambda _: "unsafe-wire")
async def test_rejects_unsafe_response_without_echo_or_redirect(tls_listing, response, fetch_request):
    tls_listing.response = response
    async with _reader(tls_listing, maximum_body_bytes=100, maximum_header_bytes=512) as reader:
        with pytest.raises(RuntimeError) as error:
            await fetch_request(reader, deadline=_deadline())
    assert "private" not in str(error.value).lower()
    await asyncio.wait_for(tls_listing.closed.wait(), 2)
    assert len(tls_listing.requests) == 1


@pytest.mark.parametrize("end", ["cancel", "close", "idle", "total"])
async def test_cancellation_shutdown_and_deadlines_close_owned_socket(tls_listing, end, fetch_request):
    tls_listing.response = None
    async with _reader(tls_listing, idle_timeout_seconds=0.1 if end == "idle" else 5.0) as reader:
        request = asyncio.create_task(fetch_request(reader, deadline=_deadline(0.2 if end == "total" else 5)))
        await asyncio.wait_for(tls_listing.received.wait(), 2)
        if end == "cancel":
            request.cancel()
        elif end == "close":
            await reader.aclose()
        with pytest.raises(asyncio.CancelledError if end in {"cancel", "close"} else RuntimeError):
            await request
        await asyncio.wait_for(tls_listing.closed.wait(), 2)


async def test_queued_read_uses_same_total_deadline_without_opening_connection(tls_listing, fetch_request):
    tls_listing.response = None
    async with _reader(tls_listing, maximum_concurrent_reads=1) as reader:
        first = asyncio.create_task(reader.fetch(tls_listing.url, deadline=_deadline()))
        await asyncio.wait_for(tls_listing.received.wait(), 2)
        with pytest.raises(RuntimeError):
            await fetch_request(reader, deadline=_deadline(0.05))
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


async def test_expired_deadline_or_closed_reader_never_opens_connection(tls_listing, fetch_request):
    reader = _reader(tls_listing)
    with pytest.raises(RuntimeError):
        await fetch_request(reader, deadline=_deadline(-1))
    await reader.aclose()
    with pytest.raises(RuntimeError):
        await fetch_request(reader, deadline=_deadline())
    assert not tls_listing.requests


async def test_raw_chunk_framing_counts_toward_total_wire_limit(tls_listing):
    tls_listing.response = b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n" + b"1\r\nx\r\n" * 50 + b"0\r\n\r\n"
    async with _reader(tls_listing, maximum_wire_bytes=100) as reader:
        with pytest.raises(RuntimeError):
            await reader.fetch(tls_listing.url, deadline=_deadline())


async def test_valid_chunked_response_and_identity_encoding(tls_listing):
    tls_listing.response = b"HTTP/1.1 200 OK\r\nContent-Encoding: identity\r\nTransfer-Encoding: chunked\r\n\r\n3\r\n<xm\r\n3\r\nl/>\r\n0\r\n\r\n"
    async with _reader(tls_listing) as reader:
        assert await reader.fetch(tls_listing.url, deadline=_deadline()) == b"<xml/>"


async def test_different_origin_is_rejected_before_connection(tls_listing):
    async with _reader(tls_listing) as reader:
        with pytest.raises(RuntimeError):
            await reader.fetch("https://foreign.example/loom-bundles?private=1", deadline=_deadline())
    assert not tls_listing.requests


async def test_tls_certificate_must_match_trusted_ca(tls_listing, monkeypatch, fetch_request):
    # Loading the system roots instead of the disposable CA cannot validate the
    # self-signed fixture. Keep hostname checking and CERT_REQUIRED unchanged.
    def system_roots(context, *args, **kwargs):
        context.load_default_certs()

    monkeypatch.setattr(ssl.SSLContext, "load_verify_locations", system_roots)
    async with _reader(tls_listing) as reader:
        with pytest.raises(RuntimeError):
            await fetch_request(reader, deadline=_deadline())
    assert not tls_listing.requests


async def test_shutdown_closes_active_and_queued_reads(tls_listing, fetch_request):
    tls_listing.response = None
    reader = _reader(tls_listing, maximum_concurrent_reads=1)
    first = asyncio.create_task(reader.fetch(tls_listing.url, deadline=_deadline()))
    await asyncio.wait_for(tls_listing.received.wait(), 2)
    second = asyncio.create_task(fetch_request(reader, deadline=_deadline()))
    await asyncio.sleep(0)  # Admit the queued coroutine, not a wall-time assumption.
    await asyncio.gather(reader.aclose(), reader.aclose())
    results = await asyncio.gather(first, second, return_exceptions=True)
    assert all(isinstance(result, asyncio.CancelledError) for result in results)
    await asyncio.wait_for(tls_listing.closed.wait(), 2)
    assert len(tls_listing.requests) == 1


async def test_manifest_fetch_preserves_exact_signed_target(tls_listing):
    digest = "a" * 64
    target = f"/loom-bundles/loom-bundle-manifests/v1/sha256/{digest}.json?X-Amz-Credential=access%2Fscope&X-Amz-Signature=private-fixture"
    async with _reader(tls_listing) as reader:
        assert await reader.fetch_manifest(tls_listing.origin + target, expected_sha256=digest, deadline=_deadline()) == b"<xml/>"
        # Listing authority must not become arbitrary object-read authority.
        with pytest.raises(RuntimeError):
            await reader.fetch(tls_listing.origin + target, deadline=_deadline())
    assert tls_listing.requests[0].startswith(f"GET {target} HTTP/1.1\r\n".encode())
    assert b"Accept: application/json\r\n" in tls_listing.requests[0]


@pytest.mark.parametrize("change", ["digest", "encoded", "parent", "bucket", "origin", "fragment", "unsigned", "invalid_digest"])
async def test_manifest_fetch_rejects_other_targets_before_network(tls_listing, change):
    digest = "a" * 64
    url = tls_listing.origin + f"/loom-bundles/loom-bundle-manifests/v1/sha256/{digest}.json?sig=private"
    if change == "digest":
        url = url.replace(digest, "b" * 64)
    elif change == "encoded":
        url = url.replace("sha256/", "sha256%2F")
    elif change == "parent":
        url = url.replace("sha256/", "sha256/../sha256/")
    elif change == "bucket":
        url = url.replace("/loom-bundles/", "/foreign/")
    elif change == "origin":
        url = url.replace(tls_listing.origin, "https://foreign.example")
    elif change == "fragment":
        url += "#fragment"
    elif change == "unsigned":
        url = url.split("?")[0]
    else:
        digest = "A" * 64
    async with _reader(tls_listing) as reader:
        with pytest.raises(RuntimeError):
            await reader.fetch_manifest(url, expected_sha256=digest, deadline=_deadline())
    assert not tls_listing.requests


async def test_cancelled_connect_handoff_aborts_orphaned_writer(tls_listing, monkeypatch):
    entered, cancelled, aborted = asyncio.Event(), asyncio.Event(), asyncio.Event()

    async def handoff(*args, **kwargs):
        entered.set()
        try:
            await asyncio.Future()
        except asyncio.CancelledError:
            cancelled.set()
            return None, SimpleNamespace(transport=SimpleNamespace(abort=aborted.set))

    monkeypatch.setattr(asyncio, "open_connection", handoff)
    async with _reader(tls_listing) as reader:
        request = asyncio.create_task(reader.fetch(tls_listing.url, deadline=_deadline()))
        await entered.wait()
        request.cancel()
        with pytest.raises(asyncio.CancelledError):
            await request
    assert cancelled.is_set()
    await asyncio.wait_for(aborted.wait(), 2)


@pytest.mark.parametrize("limits", [
    {"connect_timeout_seconds": 0.0}, {"idle_timeout_seconds": float("nan")},
    {"total_timeout_seconds": float("inf")}, {"total_timeout_seconds": 121.0},
    {"maximum_body_bytes": 4194305}, {"maximum_header_bytes": 65537},
    {"maximum_wire_bytes": 8388609}, {"maximum_concurrent_reads": True},
    {"maximum_concurrent_reads": 33},
])
def test_limits_are_finite_typed_and_bounded(limits):
    with pytest.raises(ValueError):
        _module().S3ListingReadLimits(**limits)
