"""Real-loopback mutual-TLS enforcement for the capacity manager."""

from __future__ import annotations

import asyncio
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
from sqlalchemy import delete, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from loom_capacity_manager.api import create_app
from loom_capacity_manager.config import CapacityManagerSettings, build_uvicorn_kwargs
from loom_capacity_manager.health_probe import (
    CapacityHealthProbeError,
    probe_capacity_manager,
)
from loom_capacity_manager.models import Base, CapacityAuthorityState
from tests.capacity_fixtures import AUTHORITY_ID


def _write(path: Path, payload: bytes, *, mode: int = 0o600) -> Path:
    path.write_bytes(payload)
    path.chmod(mode)
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
    include_service_dns: bool = True,
    include_loopback_ip: bool = True,
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
        .add_extension(
            x509.ExtendedKeyUsage(
                [ExtendedKeyUsageOID.SERVER_AUTH if server else ExtendedKeyUsageOID.CLIENT_AUTH]
            ),
            critical=True,
        )
    )
    if server:
        identities: list[x509.GeneralName] = [x509.DNSName("localhost")]
        if include_service_dns:
            identities.append(
                x509.DNSName("loom-capacity-manager.loom-dev.svc.cluster.local")
            )
        if include_loopback_ip:
            identities.append(x509.IPAddress(ipaddress.ip_address("127.0.0.1")))
        builder = builder.add_extension(
            x509.SubjectAlternativeName(identities),
            critical=False,
        )
    return key, builder.sign(ca_key, hashes.SHA256())


def _private_key_bytes(key: rsa.RSAPrivateKey) -> bytes:
    return key.private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption(),
    )


async def _reset_capacity_database(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    async with session_factory() as session, session.begin():
        for table in reversed(Base.metadata.sorted_tables):
            if table.name != CapacityAuthorityState.__tablename__:
                await session.execute(delete(table))
        await session.execute(
            update(CapacityAuthorityState)
            .where(CapacityAuthorityState.singleton_id == 1)
            .values(
                authority_incarnation=AUTHORITY_ID,
                writer_epoch=0,
                recovery_state="shadow",
                increase_freeze=True,
                increase_freeze_reason="initial_shadow_freeze",
                executable_new_capacity_ceiling=0,
            )
        )


def _free_port() -> int:
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return int(listener.getsockname()[1])


@pytest.mark.timeout(20)
async def test_real_server_rejects_missing_and_untrusted_client_certificates(
    tmp_path: Path,
    capacity_postgres_url: str,
    capacity_session_factory: async_sessionmaker[AsyncSession],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    await _reset_capacity_database(capacity_session_factory)
    ca_key, ca_certificate = _new_ca("trusted-capacity-ca")
    unrelated_ca_key, unrelated_ca_certificate = _new_ca("unrelated-ca")
    server_key, server_certificate = _signed_certificate(
        "localhost",
        ca_key,
        ca_certificate,
        server=True,
    )
    client_key, client_certificate = _signed_certificate(
        "trusted-client",
        ca_key,
        ca_certificate,
        server=False,
    )
    unrelated_key, unrelated_certificate = _signed_certificate(
        "unrelated-client",
        unrelated_ca_key,
        unrelated_ca_certificate,
        server=False,
    )
    _, incomplete_server_certificate = _signed_certificate(
        "localhost",
        ca_key,
        ca_certificate,
        server=True,
        include_service_dns=False,
    )
    ca_path = _write(tmp_path / "ca.pem", ca_certificate.public_bytes(serialization.Encoding.PEM))
    server_cert_path = _write(
        tmp_path / "server.pem",
        server_certificate.public_bytes(serialization.Encoding.PEM),
    )
    server_key_path = _write(tmp_path / "server-key.pem", _private_key_bytes(server_key))
    client_cert_path = _write(
        tmp_path / "client.pem",
        client_certificate.public_bytes(serialization.Encoding.PEM),
    )
    client_key_path = _write(tmp_path / "client-key.pem", _private_key_bytes(client_key))
    unrelated_cert_path = _write(
        tmp_path / "unrelated.pem",
        unrelated_certificate.public_bytes(serialization.Encoding.PEM),
    )
    unrelated_key_path = _write(
        tmp_path / "unrelated-key.pem",
        _private_key_bytes(unrelated_key),
    )
    incomplete_server_cert_path = _write(
        tmp_path / "incomplete-server.pem",
        incomplete_server_certificate.public_bytes(serialization.Encoding.PEM),
    )
    linked_server_cert_path = tmp_path / "linked-server.pem"
    linked_server_cert_path.symlink_to(server_cert_path)
    db_url_path = _write(tmp_path / "database-url", capacity_postgres_url.encode())
    token = "mtls-operator-secret"
    principals_path = _write(
        tmp_path / "principals.json",
        json.dumps(
            {
                "schema_version": 1,
                "principals": [
                    {
                        "principal_id": "fleet-operator",
                        "token_sha256": hashlib.sha256(token.encode()).hexdigest(),
                        "scopes": ["capacity:reconcile"],
                        "subject_id": None,
                        "subject_incarnation": None,
                        "demand_reporter_incarnation": None,
                        "pool_id": None,
                        "pool_reporter_incarnation": None,
                    }
                ],
            }
        ).encode(),
    )
    port = _free_port()
    settings = CapacityManagerSettings(
        principals_file=principals_path,
        db_url_file=db_url_path,
        expected_authority_incarnation=AUTHORITY_ID,
        tls_cert_file=server_cert_path,
        tls_key_file=server_key_path,
        tls_client_ca_file=ca_path,
        host="127.0.0.1",
        port=port,
    )
    config = uvicorn.Config(
        create_app(settings),
        log_level="warning",
        **build_uvicorn_kwargs(settings),
    )
    server = uvicorn.Server(config)
    thread = Thread(target=lambda: asyncio.run(server.serve()), daemon=True)
    thread.start()
    deadline = time.monotonic() + 5
    while not server.started and thread.is_alive() and time.monotonic() < deadline:
        await asyncio.sleep(0.02)
    assert server.started
    url = f"https://127.0.0.1:{port}/healthz"
    no_client_context = ssl.create_default_context(cafile=str(ca_path))
    unrelated_context = ssl.create_default_context(cafile=str(ca_path))
    unrelated_context.load_cert_chain(unrelated_cert_path, unrelated_key_path)
    trusted_context = ssl.create_default_context(cafile=str(ca_path))
    trusted_context.load_cert_chain(client_cert_path, client_key_path)
    try:
        with pytest.raises(httpx.TransportError):
            with httpx.Client(verify=no_client_context, timeout=2) as client:
                client.get(url)
        with pytest.raises(httpx.TransportError):
            with httpx.Client(
                verify=unrelated_context,
                timeout=2,
            ) as client:
                client.get(url)
        with httpx.Client(
            verify=trusted_context,
            timeout=2,
        ) as client:
            response = client.get(url)
        assert response.status_code == 200
        assert response.json()["executable_new_capacity_ceiling"] == 0
        monkeypatch.setenv("HTTPS_PROXY", "http://127.0.0.1:1")
        monkeypatch.setenv("https_proxy", "http://127.0.0.1:1")
        monkeypatch.setenv("NO_PROXY", "")
        monkeypatch.setenv("no_proxy", "")
        assert probe_capacity_manager(
            url=url,
            ca_file=ca_path,
            certificate_file=client_cert_path,
            private_key_file=client_key_path,
            server_certificate_file=server_cert_path,
            timeout_seconds=2,
        ) == {
            "status": "ready",
            "executable_new_capacity_ceiling": 0,
        }
        with pytest.raises(CapacityHealthProbeError, match="transport"):
            probe_capacity_manager(
                url=url,
                ca_file=ca_path,
                certificate_file=unrelated_cert_path,
                private_key_file=unrelated_key_path,
                server_certificate_file=server_cert_path,
                timeout_seconds=2,
            )
        with pytest.raises(CapacityHealthProbeError, match="identities"):
            probe_capacity_manager(
                url=url,
                ca_file=ca_path,
                certificate_file=client_cert_path,
                private_key_file=client_key_path,
                server_certificate_file=incomplete_server_cert_path,
                timeout_seconds=2,
            )
        with pytest.raises(CapacityHealthProbeError, match="identities"):
            probe_capacity_manager(
                url=url,
                ca_file=ca_path,
                certificate_file=client_cert_path,
                private_key_file=client_key_path,
                server_certificate_file=linked_server_cert_path,
                timeout_seconds=2,
            )
    finally:
        server.should_exit = True
        thread.join(timeout=5)
        assert not thread.is_alive()
        await _reset_capacity_database(capacity_session_factory)
