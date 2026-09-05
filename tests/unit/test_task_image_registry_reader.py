from __future__ import annotations

import asyncio
import hashlib
import ipaddress
import json
import ssl
from collections.abc import AsyncIterator, Iterator
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import UUID

import jwt
import pytest
import pytest_asyncio
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID

from loom_task_image_authority.config import (
    TaskImageAuthorityConfigurationError,
    TaskImageAuthoritySettings,
)
from loom_task_image_authority.oci_verification import (
    OCIDescriptor,
    verify_oci_graph,
)
from loom_task_image_authority.registry_reader import (
    HTTPSRegistryReader,
    RegistryReaderLimits,
    RegistryReadError,
    load_https_registry_reader,
)
from loom_task_image_authority.registry_token import DistributionRegistryTokenIssuer

ATTEMPT_ID = UUID("11111111-1111-4111-8111-111111111111")
REPOSITORY = f"loom-task-image-attempts/arm64/{ATTEMPT_ID}/task"
OCI_MANIFEST = "application/vnd.oci.image.manifest.v1+json"
OCI_CONFIG = "application/vnd.oci.image.config.v1+json"
OCI_LAYER = "application/vnd.oci.image.layer.v1.tar+gzip"


def _json_bytes(value: object) -> bytes:
    return json.dumps(value, separators=(",", ":"), sort_keys=True).encode("utf-8")


def _descriptor(media_type: str, payload: bytes) -> OCIDescriptor:
    return OCIDescriptor(
        media_type=media_type,
        digest="sha256:" + hashlib.sha256(payload).hexdigest(),
        size=len(payload),
    )


@dataclass
class _Response:
    status: int = 200
    headers: list[tuple[str, str]] = field(default_factory=list)
    chunks: tuple[bytes, ...] = ()
    initial_delay: float = 0.0
    chunk_delay: float = 0.0


@dataclass(frozen=True)
class _Request:
    method: str
    target: str
    headers: dict[str, str]


class _TLSRegistry:
    def __init__(self, ca_file: Path, certificate_file: Path, key_file: Path) -> None:
        self.ca_file = ca_file
        self.certificate_file = certificate_file
        self.key_file = key_file
        self.routes: dict[str, _Response] = {}
        self.requests: list[_Request] = []
        self.server: asyncio.AbstractServer | None = None
        self.active_requests = 0
        self.maximum_active_requests = 0

    @property
    def origin(self) -> str:
        assert self.server is not None
        port = self.server.sockets[0].getsockname()[1]
        return f"https://127.0.0.1:{port}"

    async def start(self) -> None:
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        context.minimum_version = ssl.TLSVersion.TLSv1_2
        context.load_cert_chain(self.certificate_file, self.key_file)
        self.server = await asyncio.start_server(
            self._handle,
            "127.0.0.1",
            0,
            ssl=context,
        )

    async def aclose(self) -> None:
        if self.server is not None:
            self.server.close()
            await self.server.wait_closed()

    async def _handle(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        self.active_requests += 1
        self.maximum_active_requests = max(
            self.maximum_active_requests,
            self.active_requests,
        )
        try:
            raw = await asyncio.wait_for(reader.readuntil(b"\r\n\r\n"), timeout=2.0)
            lines = raw.decode("ascii").split("\r\n")
            method, target, _version = lines[0].split(" ", 2)
            headers = {
                name.strip().lower(): value.strip()
                for line in lines[1:]
                if line
                for name, value in (line.split(":", 1),)
            }
            self.requests.append(_Request(method, target, headers))
            response = self.routes.get(target, _Response(status=404))
            if response.initial_delay:
                await asyncio.sleep(response.initial_delay)
            reason = {200: "OK", 302: "Found", 401: "Unauthorized", 404: "Not Found"}[
                response.status
            ]
            response_headers = list(response.headers)
            content_length = any(name.lower() == "content-length" for name, _ in response_headers)
            if not content_length:
                response_headers.append(("Transfer-Encoding", "chunked"))
            response_headers.append(("Connection", "close"))
            head = f"HTTP/1.1 {response.status} {reason}\r\n" + "".join(
                f"{name}: {value}\r\n" for name, value in response_headers
            )
            writer.write(head.encode("ascii") + b"\r\n")
            await writer.drain()
            for chunk in response.chunks:
                if response.chunk_delay:
                    await asyncio.sleep(response.chunk_delay)
                if content_length:
                    writer.write(chunk)
                else:
                    writer.write(f"{len(chunk):x}\r\n".encode("ascii") + chunk + b"\r\n")
                await writer.drain()
            if not content_length:
                writer.write(b"0\r\n\r\n")
            await writer.drain()
        except (ConnectionError, asyncio.IncompleteReadError, TimeoutError):
            pass
        finally:
            self.active_requests -= 1
            writer.close()
            try:
                await writer.wait_closed()
            except ConnectionError:
                pass


def _write_tls_material(tmp_path: Path) -> tuple[Path, Path, Path]:
    tmp_path.mkdir(parents=True, exist_ok=True)
    now = datetime.now(UTC)
    ca_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    ca_name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "Loom test registry CA")])
    ca = (
        x509.CertificateBuilder()
        .subject_name(ca_name)
        .issuer_name(ca_name)
        .public_key(ca_key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - timedelta(minutes=1))
        .not_valid_after(now + timedelta(hours=1))
        .add_extension(x509.BasicConstraints(ca=True, path_length=0), critical=True)
        .sign(ca_key, hashes.SHA256())
    )
    server_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    server_name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "127.0.0.1")])
    server = (
        x509.CertificateBuilder()
        .subject_name(server_name)
        .issuer_name(ca.subject)
        .public_key(server_key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - timedelta(minutes=1))
        .not_valid_after(now + timedelta(hours=1))
        .add_extension(
            x509.SubjectAlternativeName([x509.IPAddress(ipaddress.ip_address("127.0.0.1"))]),
            critical=False,
        )
        .add_extension(x509.BasicConstraints(ca=False, path_length=None), critical=True)
        .sign(ca_key, hashes.SHA256())
    )
    ca_file = tmp_path / "ca.pem"
    certificate_file = tmp_path / "server.pem"
    key_file = tmp_path / "server-key.pem"
    ca_file.write_bytes(ca.public_bytes(serialization.Encoding.PEM))
    certificate_file.write_bytes(server.public_bytes(serialization.Encoding.PEM))
    key_file.write_bytes(
        server_key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        )
    )
    return ca_file, certificate_file, key_file


@pytest_asyncio.fixture
async def tls_registry(tmp_path: Path) -> AsyncIterator[_TLSRegistry]:
    registry = _TLSRegistry(*_write_tls_material(tmp_path))
    await registry.start()
    try:
        yield registry
    finally:
        await registry.aclose()


@pytest.fixture(scope="module")
def token_key() -> Iterator[rsa.RSAPrivateKey]:
    yield rsa.generate_private_key(public_exponent=65537, key_size=3072)


def _issuer(
    registry: _TLSRegistry,
    token_key: rsa.RSAPrivateKey,
) -> DistributionRegistryTokenIssuer:
    return DistributionRegistryTokenIssuer(
        private_key=token_key,
        registry_origin=registry.origin,
        service="registry.test",
        issuer="loom-task-image-authority",
    )


def _route(registry: _TLSRegistry, kind: str, descriptor: OCIDescriptor, payload: bytes) -> None:
    registry.routes[f"/v2/{REPOSITORY}/{kind}s/{descriptor.digest}"] = _Response(
        headers=[
            (
                "Content-Type",
                descriptor.media_type if kind == "manifest" else "application/octet-stream",
            ),
            ("Docker-Content-Digest", descriptor.digest),
        ],
        chunks=(payload[: max(1, len(payload) // 2)], payload[max(1, len(payload) // 2) :]),
    )


@pytest.mark.asyncio
async def test_real_tls_streams_repository_bound_graph_with_fresh_pull_tokens(
    tls_registry: _TLSRegistry,
    token_key: rsa.RSAPrivateKey,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("HTTPS_PROXY", "http://127.0.0.1:1")
    layer = b"compressed-layer-bytes"
    layer_descriptor = _descriptor(OCI_LAYER, layer)
    config = _json_bytes(
        {
            "architecture": "arm64",
            "os": "linux",
            "rootfs": {"type": "layers", "diff_ids": ["sha256:" + "a" * 64]},
        }
    )
    config_descriptor = _descriptor(OCI_CONFIG, config)
    manifest = _json_bytes(
        {
            "schemaVersion": 2,
            "mediaType": OCI_MANIFEST,
            "config": {
                "mediaType": config_descriptor.media_type,
                "digest": config_descriptor.digest,
                "size": config_descriptor.size,
            },
            "layers": [
                {
                    "mediaType": layer_descriptor.media_type,
                    "digest": layer_descriptor.digest,
                    "size": layer_descriptor.size,
                }
            ],
        }
    )
    manifest_descriptor = _descriptor(OCI_MANIFEST, manifest)
    _route(tls_registry, "manifest", manifest_descriptor, manifest)
    _route(tls_registry, "blob", config_descriptor, config)
    _route(tls_registry, "blob", layer_descriptor, layer)

    async with HTTPSRegistryReader(
        repository=REPOSITORY,
        token_issuer=_issuer(tls_registry, token_key),
        ca_file=tls_registry.ca_file,
    ) as reader:
        verified = await verify_oci_graph(reader, manifest_descriptor, "linux/arm64")

    assert verified.manifest == manifest_descriptor
    assert verified.config == config_descriptor
    assert verified.layers == (layer_descriptor,)
    assert [request.method for request in tls_registry.requests] == ["GET", "GET", "GET"]
    assert [request.target for request in tls_registry.requests] == [
        f"/v2/{REPOSITORY}/manifests/{manifest_descriptor.digest}",
        f"/v2/{REPOSITORY}/blobs/{config_descriptor.digest}",
        f"/v2/{REPOSITORY}/blobs/{layer_descriptor.digest}",
    ]
    claims = [
        jwt.decode(
            request.headers["authorization"].removeprefix("Bearer "),
            token_key.public_key(),
            algorithms=["RS256"],
            audience="registry.test",
            issuer="loom-task-image-authority",
        )
        for request in tls_registry.requests
    ]
    assert len({claim["jti"] for claim in claims}) == 3
    assert all(claim["sub"].startswith("loom-task-image-verifier:") for claim in claims)
    assert all(
        claim["access"]
        == [{"type": "repository", "name": REPOSITORY, "actions": ["pull"]}]
        for claim in claims
    )
    assert all(request.headers["accept-encoding"] == "identity" for request in tls_registry.requests)


async def _read_all(
    reader: HTTPSRegistryReader,
    kind: str,
    descriptor: OCIDescriptor,
) -> bytes:
    return b"".join(
        [chunk async for chunk in reader.read(kind, descriptor)]  # type: ignore[arg-type]
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("status", "headers", "retryable"),
    [
        (302, [("Location", "https://elsewhere.example/v2/stolen")], False),
        (
            401,
            [
                (
                    "WWW-Authenticate",
                    'Bearer realm="https://elsewhere.example/token",scope="registry:*:*"',
                )
            ],
            True,
        ),
    ],
)
async def test_redirects_and_cross_origin_challenges_are_not_followed(
    tls_registry: _TLSRegistry,
    token_key: rsa.RSAPrivateKey,
    status: int,
    headers: list[tuple[str, str]],
    retryable: bool,
) -> None:
    payload = b"manifest"
    descriptor = _descriptor(OCI_MANIFEST, payload)
    target = f"/v2/{REPOSITORY}/manifests/{descriptor.digest}"
    tls_registry.routes[target] = _Response(status=status, headers=headers)

    async with HTTPSRegistryReader(
        repository=REPOSITORY,
        token_issuer=_issuer(tls_registry, token_key),
        ca_file=tls_registry.ca_file,
    ) as reader:
        with pytest.raises(RegistryReadError) as raised:
            await _read_all(reader, "manifest", descriptor)

    assert raised.value.retryable is retryable
    assert [request.target for request in tls_registry.requests] == [target]


@pytest.mark.asyncio
async def test_tls_rejects_wrong_ca_and_wrong_server_name(
    tls_registry: _TLSRegistry,
    token_key: rsa.RSAPrivateKey,
    tmp_path: Path,
) -> None:
    payload = b"blob"
    descriptor = _descriptor(OCI_LAYER, payload)
    _route(tls_registry, "blob", descriptor, payload)
    wrong_ca, _certificate, _key = _write_tls_material(tmp_path / "wrong")

    wrong_ca_reader = HTTPSRegistryReader(
        repository=REPOSITORY,
        token_issuer=_issuer(tls_registry, token_key),
        ca_file=wrong_ca,
    )
    async with wrong_ca_reader:
        with pytest.raises(RegistryReadError, match="transport failure"):
            await _read_all(wrong_ca_reader, "blob", descriptor)

    wrong_name_issuer = DistributionRegistryTokenIssuer(
        private_key=token_key,
        registry_origin=tls_registry.origin.replace("127.0.0.1", "localhost"),
        service="registry.test",
        issuer="loom-task-image-authority",
    )
    wrong_name_reader = HTTPSRegistryReader(
        repository=REPOSITORY,
        token_issuer=wrong_name_issuer,
        ca_file=tls_registry.ca_file,
    )
    async with wrong_name_reader:
        with pytest.raises(RegistryReadError, match="transport failure"):
            await _read_all(wrong_name_reader, "blob", descriptor)


@pytest.mark.asyncio
async def test_headers_chunks_and_manifest_size_are_bounded_before_trust(
    tls_registry: _TLSRegistry,
    token_key: rsa.RSAPrivateKey,
) -> None:
    payload = b"0123456789abcdef"
    descriptor = _descriptor(OCI_MANIFEST, payload)
    target = f"/v2/{REPOSITORY}/manifests/{descriptor.digest}"
    tls_registry.routes[target] = _Response(
        headers=[("Content-Type", OCI_MANIFEST), ("X-Oversized", "x" * 512)],
        chunks=(payload,),
    )
    header_reader = HTTPSRegistryReader(
        repository=REPOSITORY,
        token_issuer=_issuer(tls_registry, token_key),
        ca_file=tls_registry.ca_file,
        limits=RegistryReaderLimits(maximum_response_header_bytes=256),
    )
    async with header_reader:
        with pytest.raises(RegistryReadError, match="header limit"):
            await _read_all(header_reader, "manifest", descriptor)

    tls_registry.routes[target] = _Response(
        headers=[("Content-Type", OCI_MANIFEST)],
        chunks=(payload,),
    )
    chunk_reader = HTTPSRegistryReader(
        repository=REPOSITORY,
        token_issuer=_issuer(tls_registry, token_key),
        ca_file=tls_registry.ca_file,
        limits=RegistryReaderLimits(maximum_chunk_bytes=8),
    )
    async with chunk_reader:
        with pytest.raises(RegistryReadError, match="chunk limit"):
            await _read_all(chunk_reader, "manifest", descriptor)

    large_descriptor = OCIDescriptor(OCI_MANIFEST, "sha256:" + "a" * 64, 17)
    bounded_reader = HTTPSRegistryReader(
        repository=REPOSITORY,
        token_issuer=_issuer(tls_registry, token_key),
        ca_file=tls_registry.ca_file,
        limits=RegistryReaderLimits(maximum_manifest_bytes=16),
    )
    request_count = len(tls_registry.requests)
    async with bounded_reader:
        with pytest.raises(RegistryReadError, match="manifest size"):
            await _read_all(bounded_reader, "manifest", large_descriptor)
    assert len(tls_registry.requests) == request_count


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("headers", "message"),
    [
        ([("Content-Encoding", "gzip")], "encoding"),
        ([("Docker-Content-Digest", "sha256:" + "f" * 64)], "digest header"),
        ([("Content-Type", "application/octet-stream")], "media type"),
        ([("Content-Length", "999")], "size mismatch"),
    ],
)
async def test_untrusted_response_metadata_is_only_a_consistency_check(
    tls_registry: _TLSRegistry,
    token_key: rsa.RSAPrivateKey,
    headers: list[tuple[str, str]],
    message: str,
) -> None:
    payload = b"manifest"
    descriptor = _descriptor(OCI_MANIFEST, payload)
    target = f"/v2/{REPOSITORY}/manifests/{descriptor.digest}"
    tls_registry.routes[target] = _Response(headers=headers, chunks=(payload,))

    async with HTTPSRegistryReader(
        repository=REPOSITORY,
        token_issuer=_issuer(tls_registry, token_key),
        ca_file=tls_registry.ca_file,
    ) as reader:
        with pytest.raises(RegistryReadError, match=message):
            await _read_all(reader, "manifest", descriptor)


@pytest.mark.asyncio
async def test_idle_and_total_deadlines_close_the_stream(
    tls_registry: _TLSRegistry,
    token_key: rsa.RSAPrivateKey,
) -> None:
    payload = b"slow-response"
    descriptor = _descriptor(OCI_LAYER, payload)
    target = f"/v2/{REPOSITORY}/blobs/{descriptor.digest}"
    tls_registry.routes[target] = _Response(
        headers=[("Content-Type", "application/octet-stream")],
        chunks=(payload,),
        initial_delay=0.1,
    )
    idle_reader = HTTPSRegistryReader(
        repository=REPOSITORY,
        token_issuer=_issuer(tls_registry, token_key),
        ca_file=tls_registry.ca_file,
        limits=RegistryReaderLimits(idle_timeout_seconds=0.03, total_timeout_seconds=1.0),
    )
    async with idle_reader:
        with pytest.raises(RegistryReadError, match="transport timeout") as raised:
            await _read_all(idle_reader, "blob", descriptor)
    assert raised.value.retryable is True

    tls_registry.routes[target] = _Response(
        headers=[("Content-Type", "application/octet-stream")],
        chunks=(payload[:4], payload[4:8], payload[8:]),
        chunk_delay=0.035,
    )
    total_reader = HTTPSRegistryReader(
        repository=REPOSITORY,
        token_issuer=_issuer(tls_registry, token_key),
        ca_file=tls_registry.ca_file,
        limits=RegistryReaderLimits(idle_timeout_seconds=0.1, total_timeout_seconds=0.07),
    )
    async with total_reader:
        with pytest.raises(RegistryReadError, match="total timeout") as raised:
            await _read_all(total_reader, "blob", descriptor)
    assert raised.value.retryable is True


@pytest.mark.asyncio
async def test_concurrency_and_early_stream_close_release_transport_capacity(
    tls_registry: _TLSRegistry,
    token_key: rsa.RSAPrivateKey,
) -> None:
    payload = b"two-chunks"
    descriptor = _descriptor(OCI_LAYER, payload)
    target = f"/v2/{REPOSITORY}/blobs/{descriptor.digest}"
    tls_registry.routes[target] = _Response(
        headers=[("Content-Type", "application/octet-stream")],
        chunks=(payload[:3], payload[3:]),
        initial_delay=0.04,
    )
    reader = HTTPSRegistryReader(
        repository=REPOSITORY,
        token_issuer=_issuer(tls_registry, token_key),
        ca_file=tls_registry.ca_file,
        limits=RegistryReaderLimits(maximum_concurrent_reads=2),
    )
    async with reader:
        assert await asyncio.gather(*[_read_all(reader, "blob", descriptor) for _ in range(4)]) == [
            payload,
            payload,
            payload,
            payload,
        ]
        stream = reader.read("blob", descriptor)
        assert await anext(stream) == payload[:3]
        await stream.aclose()
        assert await _read_all(reader, "blob", descriptor) == payload

    assert tls_registry.maximum_active_requests == 2
    with pytest.raises(RuntimeError, match="closed"):
        await _read_all(reader, "blob", descriptor)


@pytest.mark.asyncio
async def test_queued_read_has_total_deadline_and_mints_only_after_admission(
    tls_registry: _TLSRegistry,
    token_key: rsa.RSAPrivateKey,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    payload = b"queued-read"
    descriptor = _descriptor(OCI_LAYER, payload)
    _route(tls_registry, "blob", descriptor, payload)
    issuer = _issuer(tls_registry, token_key)
    issued_count = 0
    issue_pull = DistributionRegistryTokenIssuer.issue_pull

    def counted_issue_pull(
        token_issuer: DistributionRegistryTokenIssuer,
        **kwargs: object,
    ) -> object:
        nonlocal issued_count
        issued_count += 1
        return issue_pull(token_issuer, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(DistributionRegistryTokenIssuer, "issue_pull", counted_issue_pull)
    reader = HTTPSRegistryReader(
        repository=REPOSITORY,
        token_issuer=issuer,
        ca_file=tls_registry.ca_file,
        limits=RegistryReaderLimits(total_timeout_seconds=0.05, maximum_concurrent_reads=1),
    )
    async with reader:
        held_stream = reader.read("blob", descriptor)
        assert await anext(held_stream) == payload[: len(payload) // 2]
        with pytest.raises(RegistryReadError, match="total timeout"):
            await _read_all(reader, "blob", descriptor)
        assert issued_count == 1
        await held_stream.aclose()
        assert await _read_all(reader, "blob", descriptor) == payload
        assert issued_count == 2


def _settings(tmp_path: Path, token_key: rsa.RSAPrivateKey, registry: _TLSRegistry) -> TaskImageAuthoritySettings:
    key_file = tmp_path / "registry-signing.pem"
    key_file.write_bytes(
        token_key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        )
    )
    key_file.chmod(0o600)
    return TaskImageAuthoritySettings(
        principals_file=tmp_path / "principals.json",
        db_url_file=tmp_path / "database-url",
        secret_store_keyring_file=tmp_path / "keyring.json",
        tls_cert_file=tmp_path / "authority.pem",
        tls_key_file=tmp_path / "authority-key.pem",
        tls_client_ca_file=tmp_path / "client-ca.pem",
        registry_origin=registry.origin,
        registry_service="registry.test",
        registry_issuer="loom-task-image-authority",
        registry_signing_key_file=key_file,
        registry_reader_ca_file=registry.ca_file,
        registry_maximum_manifest_bytes=1024,
        registry_maximum_response_bytes=2048,
        registry_read_concurrency_limit=2,
    )


@pytest.mark.asyncio
async def test_settings_loader_is_fail_closed_and_applies_reader_bounds(
    tls_registry: _TLSRegistry,
    token_key: rsa.RSAPrivateKey,
    tmp_path: Path,
) -> None:
    unavailable = TaskImageAuthoritySettings(
        principals_file=tmp_path / "principals.json",
        db_url_file=tmp_path / "database-url",
        secret_store_keyring_file=tmp_path / "keyring.json",
        tls_cert_file=tmp_path / "authority.pem",
        tls_key_file=tmp_path / "authority-key.pem",
        tls_client_ca_file=tmp_path / "client-ca.pem",
    )
    with pytest.raises(TaskImageAuthorityConfigurationError, match="unavailable"):
        load_https_registry_reader(unavailable, REPOSITORY)

    reader = load_https_registry_reader(_settings(tmp_path, token_key, tls_registry), REPOSITORY)
    large_manifest = OCIDescriptor(OCI_MANIFEST, "sha256:" + "b" * 64, 1025)
    async with reader:
        with pytest.raises(RegistryReadError, match="manifest size"):
            await _read_all(reader, "manifest", large_manifest)
