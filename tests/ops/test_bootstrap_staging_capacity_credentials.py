from __future__ import annotations

import os
import stat
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ed25519, rsa
from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID
from scripts.ops.bootstrap_staging_capacity_credentials import (
    BootstrapError,
    BootstrapRequest,
    bootstrap,
)


def _certificate(
    *,
    common_name: str,
    issuer_key: rsa.RSAPrivateKey,
    issuer_name: x509.Name,
    subject_key: rsa.RSAPrivateKey,
    is_ca: bool = False,
) -> x509.Certificate:
    now = datetime.now(UTC)
    builder = (
        x509.CertificateBuilder()
        .subject_name(x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, common_name)]))
        .issuer_name(issuer_name)
        .public_key(subject_key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - timedelta(minutes=1))
        .not_valid_after(now + timedelta(days=30))
        .add_extension(x509.BasicConstraints(ca=is_ca, path_length=None), critical=True)
    )
    if not is_ca:
        builder = builder.add_extension(
            x509.ExtendedKeyUsage([ExtendedKeyUsageOID.CLIENT_AUTH]), critical=False
        )
    return builder.sign(issuer_key, hashes.SHA256())


def _write_private(path: Path, payload: bytes) -> None:
    path.write_bytes(payload)
    path.chmod(0o600)


def _source(tmp_path: Path) -> tuple[Path, Path]:
    client_root = tmp_path / "client-material"
    pki_root = tmp_path / "pki-root"
    client_root.mkdir(mode=0o700)
    pki_root.mkdir(mode=0o700)
    client_ca_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    client_ca_name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "manager-client-ca")])
    client_ca = _certificate(
        common_name="manager-client-ca",
        issuer_key=client_ca_key,
        issuer_name=client_ca_name,
        subject_key=client_ca_key,
        is_ca=True,
    )
    server_ca_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    server_ca_name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "manager-server-ca")])
    server_ca = _certificate(
        common_name="manager-server-ca",
        issuer_key=server_ca_key,
        issuer_name=server_ca_name,
        subject_key=server_ca_key,
        is_ca=True,
    )
    _write_private(
        pki_root / "client-ca-private-key.pem",
        client_ca_key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        ),
    )
    _write_private(
        pki_root / "client-ca.pem",
        client_ca.public_bytes(serialization.Encoding.PEM),
    )
    _write_private(
        pki_root / "server-ca.pem",
        server_ca.public_bytes(serialization.Encoding.PEM),
    )
    for name in (
        "capacity-read",
        "capacity-config-fleet",
        "capacity-config-subject",
        "capacity-config-activate",
    ):
        directory = client_root / name
        directory.mkdir(mode=0o700)
        key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        cert = _certificate(
            common_name=name,
            issuer_key=client_ca_key,
            issuer_name=client_ca.subject,
            subject_key=key,
        )
        _write_private(
            directory / "private-key.pem",
            key.private_bytes(
                serialization.Encoding.PEM,
                serialization.PrivateFormat.PKCS8,
                serialization.NoEncryption(),
            ),
        )
        _write_private(directory / "certificate.pem", cert.public_bytes(serialization.Encoding.PEM))
        _write_private(
            directory / "manager-ca.pem",
            server_ca.public_bytes(serialization.Encoding.PEM),
        )
        _write_private(directory / "bearer-token", f"token-{name}-{'x' * 48}".encode())
    return client_root, pki_root


def test_bootstrap_is_atomic_distinct_and_never_copies_ca_private_key(tmp_path: Path) -> None:
    source_root, pki_root = _source(tmp_path)
    state_root = tmp_path / "state"
    state_root.mkdir(mode=0o700)
    request = BootstrapRequest(
        source_client_root=source_root,
        source_client_ca_certificate=pki_root / "client-ca.pem",
        source_client_ca_private_key=pki_root / "client-ca-private-key.pem",
        source_manager_ca_certificate=pki_root / "server-ca.pem",
        state_root=state_root,
        source_uid=os.geteuid(),
        source_gid=os.getegid(),
        target_uid=os.geteuid(),
        target_gid=os.getegid(),
    )

    destination = bootstrap(request, require_root=False)

    assert destination == state_root / "protected-capacity" / "credentials"
    assert {path.name for path in destination.iterdir()} == {
        "client-ca.pem",
        "configuration-read",
        "configuration-fleet",
        "configuration-subject",
        "configuration-activate",
        "manager-read",
        "manager-prepare",
        "manager-activate",
        "manager-drain",
        "manager-retire",
        "manager-abort",
        "pool-executor-gb10",
        "pool-executor-oldlab",
        "pool-ownership-gb10",
        "pool-ownership-oldlab",
        "staging-reporter",
    }
    assert not list(destination.rglob("*ca*private*"))
    assert (destination / "client-ca.pem").read_bytes() == (pki_root / "client-ca.pem").read_bytes()
    assert (destination / "staging-reporter" / "manager-ca.pem").read_bytes() == (
        pki_root / "server-ca.pem"
    ).read_bytes()
    assert (destination / "staging-reporter" / "manager-ca.pem").read_bytes() != (
        destination / "client-ca.pem"
    ).read_bytes()
    reporter_key = serialization.load_pem_private_key(
        (destination / "staging-reporter" / "private-key.pem").read_bytes(), password=None
    )
    reporter_public = reporter_key.public_key().public_bytes(
        serialization.Encoding.DER, serialization.PublicFormat.SubjectPublicKeyInfo
    )
    copied_public = {
        serialization.load_pem_private_key(path.read_bytes(), password=None)
        .public_key()
        .public_bytes(serialization.Encoding.DER, serialization.PublicFormat.SubjectPublicKeyInfo)
        for path in destination.glob("configuration-*/private-key.pem")
    }
    assert reporter_public not in copied_public
    execution_principals = (
        "manager-read",
        "manager-prepare",
        "manager-activate",
        "manager-drain",
        "manager-retire",
        "manager-abort",
        "pool-executor-gb10",
        "pool-executor-oldlab",
    )
    execution_public_keys: set[bytes] = set()
    execution_tokens: set[bytes] = set()
    client_ca = x509.load_pem_x509_certificate((destination / "client-ca.pem").read_bytes())
    for principal in execution_principals:
        directory = destination / principal
        assert {path.name for path in directory.iterdir()} == {
            "bearer-token",
            "certificate.pem",
            "manager-ca.pem",
            "private-key.pem",
        }
        certificate = x509.load_pem_x509_certificate((directory / "certificate.pem").read_bytes())
        certificate.verify_directly_issued_by(client_ca)
        assert certificate.extensions.get_extension_for_class(x509.ExtendedKeyUsage).value == (
            x509.ExtendedKeyUsage([ExtendedKeyUsageOID.CLIENT_AUTH])
        )
        san = certificate.extensions.get_extension_for_class(x509.SubjectAlternativeName).value
        assert san.get_values_for_type(x509.UniformResourceIdentifier) == [
            f"spiffe://loom.openai.dev/staging/capacity/{principal}"
        ]
        private_key = serialization.load_pem_private_key(
            (directory / "private-key.pem").read_bytes(), password=None
        )
        public_key = private_key.public_key().public_bytes(
            serialization.Encoding.DER,
            serialization.PublicFormat.SubjectPublicKeyInfo,
        )
        assert public_key == certificate.public_key().public_bytes(
            serialization.Encoding.DER,
            serialization.PublicFormat.SubjectPublicKeyInfo,
        )
        execution_public_keys.add(public_key)
        execution_tokens.add((directory / "bearer-token").read_bytes())
    assert len(execution_public_keys) == len(execution_principals)
    assert len(execution_tokens) == len(execution_principals)
    ownership_public_keys = set()
    for pool in ("gb10", "oldlab"):
        ownership_directory = destination / f"pool-ownership-{pool}"
        assert {path.name for path in ownership_directory.iterdir()} == {"ownership-private-key"}
        raw = (ownership_directory / "ownership-private-key").read_bytes()
        assert len(raw) == 32
        ownership_public_keys.add(
            ed25519.Ed25519PrivateKey.from_private_bytes(raw)
            .public_key()
            .public_bytes(
                serialization.Encoding.Raw,
                serialization.PublicFormat.Raw,
            )
        )
    assert len(ownership_public_keys) == 2
    for path in destination.rglob("*"):
        mode = stat.S_IMODE(path.stat(follow_symlinks=False).st_mode)
        assert mode == (0o700 if path.is_dir() else 0o600)
    with pytest.raises(BootstrapError, match="already exists"):
        bootstrap(request, require_root=False)


def test_bootstrap_rejects_unsafe_source_before_creating_destination(tmp_path: Path) -> None:
    source_root, pki_root = _source(tmp_path)
    (source_root / "capacity-read" / "bearer-token").chmod(0o640)
    state_root = tmp_path / "state"
    state_root.mkdir(mode=0o700)
    request = BootstrapRequest(
        source_client_root=source_root,
        source_client_ca_certificate=pki_root / "client-ca.pem",
        source_client_ca_private_key=pki_root / "client-ca-private-key.pem",
        source_manager_ca_certificate=pki_root / "server-ca.pem",
        state_root=state_root,
        source_uid=os.geteuid(),
        source_gid=os.getegid(),
        target_uid=os.geteuid(),
        target_gid=os.getegid(),
    )

    with pytest.raises(BootstrapError, match="source credential file is unsafe"):
        bootstrap(request, require_root=False)

    assert not (state_root / "protected-capacity").exists()


def test_bootstrap_rejects_distinct_ca_certificates_with_one_key(tmp_path: Path) -> None:
    source_root, pki_root = _source(tmp_path)
    client_ca_key = serialization.load_pem_private_key(
        (pki_root / "client-ca-private-key.pem").read_bytes(), password=None
    )
    manager_ca_name = x509.Name(
        [x509.NameAttribute(NameOID.COMMON_NAME, "manager-server-ca-with-client-key")]
    )
    manager_ca = _certificate(
        common_name="manager-server-ca-with-client-key",
        issuer_key=client_ca_key,
        issuer_name=manager_ca_name,
        subject_key=client_ca_key,
        is_ca=True,
    )
    manager_ca_payload = manager_ca.public_bytes(serialization.Encoding.PEM)
    _write_private(pki_root / "server-ca.pem", manager_ca_payload)
    for path in source_root.glob("*/manager-ca.pem"):
        _write_private(path, manager_ca_payload)
    state_root = tmp_path / "state"
    state_root.mkdir(mode=0o700)
    request = BootstrapRequest(
        source_client_root=source_root,
        source_client_ca_certificate=pki_root / "client-ca.pem",
        source_client_ca_private_key=pki_root / "client-ca-private-key.pem",
        source_manager_ca_certificate=pki_root / "server-ca.pem",
        state_root=state_root,
        source_uid=os.geteuid(),
        source_gid=os.getegid(),
        target_uid=os.geteuid(),
        target_gid=os.getegid(),
    )

    with pytest.raises(BootstrapError, match="manager CA certificate is invalid"):
        bootstrap(request, require_root=False)

    assert not (state_root / "protected-capacity").exists()


@pytest.mark.parametrize(
    "token_payload",
    (
        b"too-short",
        b"x" * 31 + b"\n",
        b"x" * 4097,
    ),
    ids=("short", "control-byte", "oversized"),
)
def test_bootstrap_rejects_invalid_configuration_bearer_before_publish(
    tmp_path: Path,
    token_payload: bytes,
) -> None:
    source_root, pki_root = _source(tmp_path)
    _write_private(source_root / "capacity-read" / "bearer-token", token_payload)
    state_root = tmp_path / "state"
    state_root.mkdir(mode=0o700)
    request = BootstrapRequest(
        source_client_root=source_root,
        source_client_ca_certificate=pki_root / "client-ca.pem",
        source_client_ca_private_key=pki_root / "client-ca-private-key.pem",
        source_manager_ca_certificate=pki_root / "server-ca.pem",
        state_root=state_root,
        source_uid=os.geteuid(),
        source_gid=os.getegid(),
        target_uid=os.geteuid(),
        target_gid=os.getegid(),
    )

    with pytest.raises(BootstrapError, match="source bearer token is invalid"):
        bootstrap(request, require_root=False)

    assert not (state_root / "protected-capacity").exists()
