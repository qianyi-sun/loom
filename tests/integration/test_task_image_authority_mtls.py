"""Real-loopback mutual-TLS enforcement for the task-image authority."""

from __future__ import annotations

import asyncio
import base64
import hashlib
import ipaddress
import json
import socket
import ssl
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from threading import Thread

import httpx
import pytest
import uvicorn
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID

from loom_task_image_authority.api import create_app
from loom_task_image_authority.config import (
    TaskImageAuthoritySettings,
    build_uvicorn_kwargs,
)


def _write(path: Path, payload: bytes) -> Path:
    path.write_bytes(payload)
    path.chmod(0o600)
    return path


def _new_ca(name: str) -> tuple[rsa.RSAPrivateKey, x509.Certificate]:
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    subject = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, name)])
    now = datetime.now(UTC)
    certificate = (
        x509.CertificateBuilder()
        .subject_name(subject)
        .issuer_name(subject)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - timedelta(minutes=1))
        .not_valid_after(now + timedelta(hours=1))
        .add_extension(x509.BasicConstraints(ca=True, path_length=0), critical=True)
        .sign(key, hashes.SHA256())
    )
    return key, certificate


def _signed_certificate(
    common_name: str,
    ca_key: rsa.RSAPrivateKey,
    ca_certificate: x509.Certificate,
    *,
    server: bool,
) -> tuple[rsa.RSAPrivateKey, x509.Certificate]:
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    now = datetime.now(UTC)
    builder = (
        x509.CertificateBuilder()
        .subject_name(x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, common_name)]))
        .issuer_name(ca_certificate.subject)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - timedelta(minutes=1))
        .not_valid_after(now + timedelta(hours=1))
        .add_extension(x509.BasicConstraints(ca=False, path_length=None), critical=True)
        .add_extension(
            x509.ExtendedKeyUsage(
                [ExtendedKeyUsageOID.SERVER_AUTH if server else ExtendedKeyUsageOID.CLIENT_AUTH]
            ),
            critical=True,
        )
    )
    if server:
        builder = builder.add_extension(
            x509.SubjectAlternativeName(
                [
                    x509.DNSName("localhost"),
                    x509.DNSName(
                        "loom-task-image-authority.loom-staging.svc.cluster.local"
                    ),
                    x509.IPAddress(ipaddress.ip_address("127.0.0.1")),
                ]
            ),
            critical=False,
        )
    return key, builder.sign(ca_key, hashes.SHA256())


def _private_key_bytes(key: rsa.RSAPrivateKey) -> bytes:
    return key.private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption(),
    )


def _free_port() -> int:
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return int(listener.getsockname()[1])


@pytest.mark.timeout(20)
async def test_real_server_requires_trusted_client_tls_and_verified_server_identity(
    tmp_path: Path,
    postgres_url: str,
) -> None:
    trusted_ca_key, trusted_ca = _new_ca("task-image-guard-ca")
    untrusted_ca_key, untrusted_ca = _new_ca("untrusted-task-image-ca")
    server_key, server_certificate = _signed_certificate(
        "localhost",
        trusted_ca_key,
        trusted_ca,
        server=True,
    )
    trusted_key, trusted_certificate = _signed_certificate(
        "trusted-node-guard",
        trusted_ca_key,
        trusted_ca,
        server=False,
    )
    untrusted_key, untrusted_certificate = _signed_certificate(
        "untrusted-client",
        untrusted_ca_key,
        untrusted_ca,
        server=False,
    )

    ca_path = _write(
        tmp_path / "client-ca.pem",
        trusted_ca.public_bytes(serialization.Encoding.PEM),
    )
    server_cert_path = _write(
        tmp_path / "server.pem",
        server_certificate.public_bytes(serialization.Encoding.PEM),
    )
    server_key_path = _write(tmp_path / "server-key.pem", _private_key_bytes(server_key))
    trusted_cert_path = _write(
        tmp_path / "trusted-client.pem",
        trusted_certificate.public_bytes(serialization.Encoding.PEM),
    )
    trusted_key_path = _write(
        tmp_path / "trusted-client-key.pem",
        _private_key_bytes(trusted_key),
    )
    untrusted_cert_path = _write(
        tmp_path / "untrusted-client.pem",
        untrusted_certificate.public_bytes(serialization.Encoding.PEM),
    )
    untrusted_key_path = _write(
        tmp_path / "untrusted-client-key.pem",
        _private_key_bytes(untrusted_key),
    )
    bearer = "mtls-test-node-bearer"
    principals_path = _write(
        tmp_path / "principals.json",
        json.dumps(
            {
                "schema_version": 1,
                "principals": [
                    {
                        "principal_id": "gb10-trt-gb10-1",
                        "token_sha256": hashlib.sha256(bearer.encode()).hexdigest(),
                        "slurm_cluster_id": "gb10",
                        "node_name": "trt-gb10-1",
                        "scopes": ["task-image:attest", "task-image:project"],
                    }
                ],
            }
        ).encode(),
    )
    keyring_path = _write(
        tmp_path / "keyring.json",
        json.dumps(
            {
                "schema_version": 1,
                "primary": {
                    "version": 1,
                    "key_base64": base64.b64encode(b"k" * 32).decode("ascii"),
                },
                "fallbacks": [],
            }
        ).encode(),
    )
    port = _free_port()
    settings = TaskImageAuthoritySettings(
        principals_file=principals_path,
        db_url_file=_write(tmp_path / "database-url", postgres_url.encode()),
        secret_store_keyring_file=keyring_path,
        tls_cert_file=server_cert_path,
        tls_key_file=server_key_path,
        tls_client_ca_file=ca_path,
        port=port,
    )
    server = uvicorn.Server(
        uvicorn.Config(
            create_app(settings),
            log_level="warning",
            **build_uvicorn_kwargs(settings),
        )
    )
    thread = Thread(target=lambda: asyncio.run(server.serve()), daemon=True)
    thread.start()
    deadline = time.monotonic() + 5
    while not server.started and thread.is_alive() and time.monotonic() < deadline:
        await asyncio.sleep(0.02)
    assert server.started

    url = f"https://127.0.0.1:{port}/healthz"
    no_client_context = ssl.create_default_context(cafile=str(ca_path))
    untrusted_context = ssl.create_default_context(cafile=str(ca_path))
    untrusted_context.load_cert_chain(untrusted_cert_path, untrusted_key_path)
    trusted_context = ssl.create_default_context(cafile=str(ca_path))
    trusted_context.minimum_version = ssl.TLSVersion.TLSv1_2
    trusted_context.load_cert_chain(trusted_cert_path, trusted_key_path)
    wrong_hostname_context = ssl.create_default_context(cafile=str(ca_path))
    wrong_hostname_context.load_cert_chain(trusted_cert_path, trusted_key_path)
    try:
        with pytest.raises(httpx.TransportError):
            with httpx.Client(verify=no_client_context, timeout=2, trust_env=False) as client:
                client.get(url)
        with pytest.raises(httpx.TransportError):
            with httpx.Client(verify=untrusted_context, timeout=2, trust_env=False) as client:
                client.get(url)
        with httpx.Client(verify=trusted_context, timeout=2, trust_env=False) as client:
            response = client.get(url)
        assert response.status_code == 200
        assert response.json() == {"status": "ok"}

        with socket.create_connection(("127.0.0.1", port), timeout=2) as raw_socket:
            with trusted_context.wrap_socket(raw_socket, server_hostname="localhost") as tls_socket:
                assert tls_socket.version() in {"TLSv1.2", "TLSv1.3"}
        with socket.create_connection(("127.0.0.1", port), timeout=2) as raw_socket:
            with pytest.raises(ssl.CertificateError):
                wrong_hostname_context.wrap_socket(
                    raw_socket,
                    server_hostname="wrong.example.test",
                )
    finally:
        server.should_exit = True
        thread.join(timeout=5)
        assert not thread.is_alive()
