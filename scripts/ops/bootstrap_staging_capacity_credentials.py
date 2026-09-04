#!/usr/bin/env python3
"""Atomically install the one-time protected staging capacity credentials."""

from __future__ import annotations

import argparse
import ctypes
import errno
import os
import secrets
import shutil
import stat
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, TypeAlias, cast
from uuid import uuid4

from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import dsa, ec, ed448, ed25519, rsa
from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID

_CLIENTS = {
    "configuration-read": "capacity-read",
    "configuration-fleet": "capacity-config-fleet",
    "configuration-subject": "capacity-config-subject",
    "configuration-activate": "capacity-config-activate",
}
_GENERATED_CLIENTS = (
    "manager-read",
    "manager-prepare",
    "manager-activate",
    "manager-drain",
    "manager-retire",
    "manager-abort",
    "pool-executor-gb10",
    "pool-executor-oldlab",
)
_OWNERSHIP_POOLS = ("gb10", "oldlab")
_CLIENT_FILES = ("bearer-token", "certificate.pem", "manager-ca.pem", "private-key.pem")
_MAX_FILE_BYTES = 1024 * 1024
_RENAME_NOREPLACE = 1
_AT_FDCWD = -100
_CertificateSigningKey: TypeAlias = (
    dsa.DSAPrivateKey
    | ec.EllipticCurvePrivateKey
    | ed25519.Ed25519PrivateKey
    | ed448.Ed448PrivateKey
    | rsa.RSAPrivateKey
)


class BootstrapError(RuntimeError):
    """The protected credential seed could not be installed safely."""


@dataclass(frozen=True, slots=True)
class BootstrapRequest:
    source_client_root: Path
    source_client_ca_certificate: Path
    source_client_ca_private_key: Path
    source_manager_ca_certificate: Path
    state_root: Path
    source_uid: int
    source_gid: int
    target_uid: int
    target_gid: int

    def __post_init__(self) -> None:
        paths = (
            self.source_client_root,
            self.source_client_ca_certificate,
            self.source_client_ca_private_key,
            self.source_manager_ca_certificate,
            self.state_root,
        )
        if (
            any(not path.is_absolute() or ".." in path.parts for path in paths)
            or len(set(paths)) != len(paths)
            or len(
                {
                    self.source_client_ca_certificate.parent,
                    self.source_client_ca_private_key.parent,
                    self.source_manager_ca_certificate.parent,
                }
            )
            != 1
            or min(self.source_uid, self.source_gid, self.target_uid, self.target_gid) < 0
        ):
            raise ValueError("staging capacity credential bootstrap request is invalid")


def _metadata_identity(value: os.stat_result) -> tuple[int, ...]:
    return (
        value.st_dev,
        value.st_ino,
        value.st_mode,
        value.st_uid,
        value.st_gid,
        value.st_nlink,
        value.st_size,
        value.st_mtime_ns,
        value.st_ctime_ns,
    )


def _validate_directory(path: Path, *, uid: int, gid: int, label: str) -> None:
    try:
        metadata = path.stat(follow_symlinks=False)
    except OSError as exc:
        raise BootstrapError(f"{label} is unavailable") from exc
    if (
        not stat.S_ISDIR(metadata.st_mode)
        or stat.S_IMODE(metadata.st_mode) != 0o700
        or metadata.st_uid != uid
        or metadata.st_gid != gid
    ):
        raise BootstrapError(f"{label} is unsafe")


def _read_private(path: Path, *, uid: int, gid: int) -> bytes:
    try:
        descriptor = os.open(
            path,
            os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0),
        )
    except OSError as exc:
        raise BootstrapError("source credential file is unsafe") from exc
    try:
        before = os.fstat(descriptor)
        if (
            not stat.S_ISREG(before.st_mode)
            or stat.S_IMODE(before.st_mode) != 0o600
            or before.st_uid != uid
            or before.st_gid != gid
            or before.st_nlink != 1
            or not 0 < before.st_size <= _MAX_FILE_BYTES
        ):
            raise BootstrapError("source credential file is unsafe")
        chunks: list[bytes] = []
        total = 0
        while total <= _MAX_FILE_BYTES:
            chunk = os.read(descriptor, min(65536, _MAX_FILE_BYTES + 1 - total))
            if not chunk:
                break
            chunks.append(chunk)
            total += len(chunk)
        after = os.fstat(descriptor)
    finally:
        os.close(descriptor)
    payload = b"".join(chunks)
    if (
        not payload
        or len(payload) > _MAX_FILE_BYTES
        or _metadata_identity(before) != _metadata_identity(after)
    ):
        raise BootstrapError("source credential file changed while reading")
    return payload


def _validate_bearer_token(payload: bytes) -> None:
    if not 32 <= len(payload) <= 4096 or any(not 0x21 <= byte <= 0x7E for byte in payload):
        raise BootstrapError("source bearer token is invalid")


def _public_key_bytes(key: Any) -> bytes:
    try:
        return cast(
            bytes,
            key.public_key().public_bytes(
                serialization.Encoding.DER,
                serialization.PublicFormat.SubjectPublicKeyInfo,
            ),
        )
    except AttributeError as exc:
        raise BootstrapError("credential private key is invalid") from exc


def _validate_pair(
    *, certificate_payload: bytes, private_key_payload: bytes, ca: x509.Certificate
) -> bytes:
    try:
        certificate = x509.load_pem_x509_certificate(certificate_payload)
        private_key = serialization.load_pem_private_key(private_key_payload, password=None)
        certificate.verify_directly_issued_by(ca)
        certificate_public: bytes = certificate.public_key().public_bytes(
            serialization.Encoding.DER,
            serialization.PublicFormat.SubjectPublicKeyInfo,
        )
        private_public = _public_key_bytes(private_key)
    except (TypeError, ValueError) as exc:
        raise BootstrapError("credential certificate authority is invalid") from exc
    now = datetime.now(UTC)
    if (
        certificate_public != private_public
        or not certificate.not_valid_before_utc <= now <= certificate.not_valid_after_utc
    ):
        raise BootstrapError("credential certificate and private key are invalid")
    return certificate_public


def _signing_algorithm(key: _CertificateSigningKey) -> hashes.SHA256 | None:
    if isinstance(key, (ed25519.Ed25519PrivateKey, ed448.Ed448PrivateKey)):
        return None
    return hashes.SHA256()


def _generated_client_material(
    ca_payload: bytes,
    ca_key_payload: bytes,
    *,
    common_name: str,
    uri_san: str | None = None,
) -> tuple[bytes, bytes]:
    try:
        ca = x509.load_pem_x509_certificate(ca_payload)
        ca_key = serialization.load_pem_private_key(ca_key_payload, password=None)
        constraints = ca.extensions.get_extension_for_class(x509.BasicConstraints).value
    except (TypeError, ValueError, x509.ExtensionNotFound) as exc:
        raise BootstrapError("manager client CA material is invalid") from exc
    if not constraints.ca or _public_key_bytes(ca_key) != ca.public_key().public_bytes(
        serialization.Encoding.DER,
        serialization.PublicFormat.SubjectPublicKeyInfo,
    ):
        raise BootstrapError("manager client CA material is invalid")
    now = datetime.now(UTC)
    not_before = max(now - timedelta(minutes=5), ca.not_valid_before_utc)
    not_after = min(now + timedelta(days=365), ca.not_valid_after_utc)
    if not_before >= not_after or not ca.not_valid_before_utc <= now <= ca.not_valid_after_utc:
        raise BootstrapError("manager client CA is not current")
    reporter_key = rsa.generate_private_key(public_exponent=65537, key_size=3072)
    signing_key = cast(_CertificateSigningKey, ca_key)
    builder = (
        x509.CertificateBuilder()
        .subject_name(x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, common_name)]))
        .issuer_name(ca.subject)
        .public_key(reporter_key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(not_before)
        .not_valid_after(not_after)
        .add_extension(x509.BasicConstraints(ca=False, path_length=None), critical=True)
        .add_extension(
            x509.KeyUsage(
                digital_signature=True,
                content_commitment=False,
                key_encipherment=True,
                data_encipherment=False,
                key_agreement=False,
                key_cert_sign=False,
                crl_sign=False,
                encipher_only=cast(bool, None),
                decipher_only=cast(bool, None),
            ),
            critical=True,
        )
        .add_extension(x509.ExtendedKeyUsage([ExtendedKeyUsageOID.CLIENT_AUTH]), critical=False)
    )
    if uri_san is not None:
        builder = builder.add_extension(
            x509.SubjectAlternativeName([x509.UniformResourceIdentifier(uri_san)]),
            critical=False,
        )
    certificate = builder.sign(signing_key, _signing_algorithm(signing_key))
    private_key = reporter_key.private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption(),
    )
    certificate_payload = certificate.public_bytes(serialization.Encoding.PEM)
    _validate_pair(
        certificate_payload=certificate_payload,
        private_key_payload=private_key,
        ca=ca,
    )
    return certificate_payload, private_key


def _reporter_material(ca_payload: bytes, ca_key_payload: bytes) -> tuple[bytes, bytes]:
    return _generated_client_material(
        ca_payload,
        ca_key_payload,
        common_name="loom-staging-capacity-reporter",
    )


def _execution_client_material(
    ca_payload: bytes,
    ca_key_payload: bytes,
    *,
    principal: str,
) -> tuple[bytes, bytes]:
    certificate_payload, private_key = _generated_client_material(
        ca_payload,
        ca_key_payload,
        common_name=f"loom-staging-capacity-{principal}",
        uri_san=f"spiffe://loom.openai.dev/staging/capacity/{principal}",
    )
    try:
        certificate = x509.load_pem_x509_certificate(certificate_payload)
        eku = certificate.extensions.get_extension_for_class(x509.ExtendedKeyUsage).value
        san = certificate.extensions.get_extension_for_class(x509.SubjectAlternativeName).value
    except (ValueError, x509.ExtensionNotFound) as exc:
        raise BootstrapError("generated execution client certificate is invalid") from exc
    if eku != x509.ExtendedKeyUsage([ExtendedKeyUsageOID.CLIENT_AUTH]) or san.get_values_for_type(
        x509.UniformResourceIdentifier
    ) != [f"spiffe://loom.openai.dev/staging/capacity/{principal}"]:
        raise BootstrapError("generated execution client certificate is invalid")
    return certificate_payload, private_key


def _ownership_private_key() -> bytes:
    return ed25519.Ed25519PrivateKey.generate().private_bytes(
        serialization.Encoding.Raw,
        serialization.PrivateFormat.Raw,
        serialization.NoEncryption(),
    )


def _make_directory(path: Path, *, uid: int, gid: int) -> None:
    os.mkdir(path, 0o700)
    os.chmod(path, 0o700, follow_symlinks=False)
    os.chown(path, uid, gid, follow_symlinks=False)


def _write_target(path: Path, payload: bytes, *, uid: int, gid: int) -> None:
    descriptor = os.open(
        path,
        os.O_WRONLY
        | os.O_CREAT
        | os.O_EXCL
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_NOFOLLOW", 0),
        0o600,
    )
    try:
        os.fchmod(descriptor, 0o600)
        os.fchown(descriptor, uid, gid)
        offset = 0
        while offset < len(payload):
            offset += os.write(descriptor, payload[offset:])
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _rename_noreplace(source: Path, destination: Path) -> None:
    libc = ctypes.CDLL(None, use_errno=True)
    renameat2 = getattr(libc, "renameat2", None)
    if renameat2 is None:
        raise BootstrapError("atomic no-replace rename is unavailable")
    renameat2.argtypes = [
        ctypes.c_int,
        ctypes.c_char_p,
        ctypes.c_int,
        ctypes.c_char_p,
        ctypes.c_uint,
    ]
    renameat2.restype = ctypes.c_int
    result = renameat2(
        _AT_FDCWD,
        os.fsencode(source),
        _AT_FDCWD,
        os.fsencode(destination),
        _RENAME_NOREPLACE,
    )
    if result == 0:
        return
    error = ctypes.get_errno()
    if error == errno.EEXIST:
        raise BootstrapError("protected staging credentials already exists")
    raise BootstrapError(f"atomic protected credential publication failed: errno {error}")


def bootstrap(request: BootstrapRequest, *, require_root: bool = True) -> Path:
    """Validate all inputs, then publish one complete credential directory."""

    if require_root and os.geteuid() != 0:
        raise BootstrapError("staging capacity credential bootstrap must run as root")
    _validate_directory(
        request.source_client_root,
        uid=request.source_uid,
        gid=request.source_gid,
        label="source client root",
    )
    _validate_directory(
        request.source_client_ca_certificate.parent,
        uid=request.source_uid,
        gid=request.source_gid,
        label="source CA root",
    )
    _validate_directory(
        request.state_root,
        uid=request.target_uid,
        gid=request.target_gid,
        label="target state root",
    )
    source_client_ca = _read_private(
        request.source_client_ca_certificate,
        uid=request.source_uid,
        gid=request.source_gid,
    )
    source_client_ca_key = _read_private(
        request.source_client_ca_private_key,
        uid=request.source_uid,
        gid=request.source_gid,
    )
    source_manager_ca = _read_private(
        request.source_manager_ca_certificate,
        uid=request.source_uid,
        gid=request.source_gid,
    )
    try:
        client_ca = x509.load_pem_x509_certificate(source_client_ca)
        manager_ca = x509.load_pem_x509_certificate(source_manager_ca)
        manager_constraints = manager_ca.extensions.get_extension_for_class(
            x509.BasicConstraints
        ).value
        manager_ca.verify_directly_issued_by(manager_ca)
    except ValueError as exc:
        raise BootstrapError("manager CA certificate is invalid") from exc
    except (TypeError, x509.ExtensionNotFound) as exc:
        raise BootstrapError("manager CA certificate is invalid") from exc
    now = datetime.now(UTC)
    client_ca_public_key = client_ca.public_key().public_bytes(
        serialization.Encoding.DER,
        serialization.PublicFormat.SubjectPublicKeyInfo,
    )
    manager_ca_public_key = manager_ca.public_key().public_bytes(
        serialization.Encoding.DER,
        serialization.PublicFormat.SubjectPublicKeyInfo,
    )
    if (
        not manager_constraints.ca
        or not manager_ca.not_valid_before_utc <= now <= manager_ca.not_valid_after_utc
        or manager_ca.public_bytes(serialization.Encoding.DER)
        == client_ca.public_bytes(serialization.Encoding.DER)
        or manager_ca_public_key == client_ca_public_key
    ):
        raise BootstrapError("manager CA certificate is invalid")
    source_payloads: dict[str, dict[str, bytes]] = {}
    public_keys: set[bytes] = set()
    for target_name, source_name in _CLIENTS.items():
        source_directory = request.source_client_root / source_name
        _validate_directory(
            source_directory,
            uid=request.source_uid,
            gid=request.source_gid,
            label="source client directory",
        )
        payloads = {
            name: _read_private(
                source_directory / name,
                uid=request.source_uid,
                gid=request.source_gid,
            )
            for name in _CLIENT_FILES
        }
        _validate_bearer_token(payloads["bearer-token"])
        if payloads["manager-ca.pem"] != source_manager_ca:
            raise BootstrapError("source client manager CA is inconsistent")
        public_key = _validate_pair(
            certificate_payload=payloads["certificate.pem"],
            private_key_payload=payloads["private-key.pem"],
            ca=client_ca,
        )
        if public_key in public_keys:
            raise BootstrapError("source client private key is reused")
        public_keys.add(public_key)
        source_payloads[target_name] = payloads
    reporter_cert, reporter_key = _reporter_material(
        source_client_ca,
        source_client_ca_key,
    )
    reporter_public = _validate_pair(
        certificate_payload=reporter_cert,
        private_key_payload=reporter_key,
        ca=client_ca,
    )
    if reporter_public in public_keys:
        raise BootstrapError("staging reporter private key is reused")
    public_keys.add(reporter_public)
    bearer_tokens = {payloads["bearer-token"] for payloads in source_payloads.values()}
    generated_payloads: dict[str, dict[str, bytes]] = {}
    for principal in _GENERATED_CLIENTS:
        certificate_payload, private_key_payload = _execution_client_material(
            source_client_ca,
            source_client_ca_key,
            principal=principal,
        )
        public_key = _validate_pair(
            certificate_payload=certificate_payload,
            private_key_payload=private_key_payload,
            ca=client_ca,
        )
        if public_key in public_keys:
            raise BootstrapError("generated execution client private key is reused")
        public_keys.add(public_key)
        token = secrets.token_urlsafe(48).encode("ascii")
        while token in bearer_tokens:
            token = secrets.token_urlsafe(48).encode("ascii")
        _validate_bearer_token(token)
        bearer_tokens.add(token)
        generated_payloads[principal] = {
            "bearer-token": token,
            "certificate.pem": certificate_payload,
            "manager-ca.pem": source_manager_ca,
            "private-key.pem": private_key_payload,
        }
    ownership_keys = {pool: _ownership_private_key() for pool in _OWNERSHIP_POOLS}

    protected_root = request.state_root / "protected-capacity"
    destination = protected_root / "credentials"
    if destination.exists() or destination.is_symlink():
        raise BootstrapError("protected staging credentials already exists")
    if protected_root.exists():
        _validate_directory(
            protected_root,
            uid=request.target_uid,
            gid=request.target_gid,
            label="protected capacity root",
        )
    else:
        _make_directory(protected_root, uid=request.target_uid, gid=request.target_gid)
    temporary = protected_root / f".credentials.{uuid4().hex}.tmp"
    try:
        _make_directory(temporary, uid=request.target_uid, gid=request.target_gid)
        _write_target(
            temporary / "client-ca.pem",
            source_client_ca,
            uid=request.target_uid,
            gid=request.target_gid,
        )
        for target_name, payloads in source_payloads.items():
            directory = temporary / target_name
            _make_directory(directory, uid=request.target_uid, gid=request.target_gid)
            for name, payload in payloads.items():
                _write_target(
                    directory / name,
                    payload,
                    uid=request.target_uid,
                    gid=request.target_gid,
                )
            directory_fd = os.open(directory, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)
        for target_name, payloads in generated_payloads.items():
            directory = temporary / target_name
            _make_directory(directory, uid=request.target_uid, gid=request.target_gid)
            for name, payload in payloads.items():
                _write_target(
                    directory / name,
                    payload,
                    uid=request.target_uid,
                    gid=request.target_gid,
                )
            directory_fd = os.open(directory, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)
        for pool, private_key in ownership_keys.items():
            directory = temporary / f"pool-ownership-{pool}"
            _make_directory(directory, uid=request.target_uid, gid=request.target_gid)
            _write_target(
                directory / "ownership-private-key",
                private_key,
                uid=request.target_uid,
                gid=request.target_gid,
            )
            directory_fd = os.open(directory, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)
        reporter_directory = temporary / "staging-reporter"
        _make_directory(reporter_directory, uid=request.target_uid, gid=request.target_gid)
        for name, payload in {
            "certificate.pem": reporter_cert,
            "manager-ca.pem": source_manager_ca,
            "private-key.pem": reporter_key,
        }.items():
            _write_target(
                reporter_directory / name,
                payload,
                uid=request.target_uid,
                gid=request.target_gid,
            )
        for directory in (reporter_directory, temporary):
            directory_fd = os.open(directory, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)
        _rename_noreplace(temporary, destination)
        protected_fd = os.open(protected_root, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
        try:
            os.fsync(protected_fd)
        finally:
            os.close(protected_fd)
    except Exception:
        if temporary.exists() and not temporary.is_symlink():
            shutil.rmtree(temporary)
        raise
    return destination


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-client-root", required=True, type=Path)
    parser.add_argument("--source-client-ca-certificate", required=True, type=Path)
    parser.add_argument("--source-client-ca-private-key", required=True, type=Path)
    parser.add_argument("--source-manager-ca-certificate", required=True, type=Path)
    parser.add_argument("--state-root", required=True, type=Path)
    parser.add_argument("--source-uid", required=True, type=int)
    parser.add_argument("--source-gid", required=True, type=int)
    parser.add_argument("--target-uid", required=True, type=int)
    parser.add_argument("--target-gid", required=True, type=int)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    destination = bootstrap(
        BootstrapRequest(
            source_client_root=args.source_client_root,
            source_client_ca_certificate=args.source_client_ca_certificate,
            source_client_ca_private_key=args.source_client_ca_private_key,
            source_manager_ca_certificate=args.source_manager_ca_certificate,
            state_root=args.state_root,
            source_uid=args.source_uid,
            source_gid=args.source_gid,
            target_uid=args.target_uid,
            target_gid=args.target_gid,
        )
    )
    print(f"protected staging capacity credentials installed at {destination}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
