#!/usr/bin/env python3
"""Private certificate qualification for protected Nebius operations.

Generation publication is not a Kubernetes write or a public routing cutover.
"""
from __future__ import annotations

import fcntl
import hashlib
import json
import os
import re
import ssl
import stat
import tempfile
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec, rsa
from cryptography.x509.oid import ExtendedKeyUsageOID
from cryptography.x509.verification import PolicyBuilder, Store, VerificationError

_HOST = re.compile(r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?(?:\.[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?)+")
_GENERATION = re.compile(r"[0-9a-f]{64}")
_CERTIFICATE_PEM = rb"\s*(?:-----BEGIN CERTIFICATE-----\s+[A-Za-z0-9+/=\s]+-----END CERTIFICATE-----\s*)+"


class CertificateError(RuntimeError):
    """Fixed, payload-free certificate failure; never forward external errors."""


def certificate_names(child_domain: str, management_host: str) -> list[str]:
    if (any(len(name) > 250 or not _HOST.fullmatch(name) for name in (child_domain, management_host))
            or management_host == child_domain or management_host.endswith("." + child_domain)):
        raise CertificateError("invalid or overlapping certificate subjects")
    return ["*." + child_domain, management_host]


def validate_certificate(chain: bytes, key: bytes, *, child_domain: str, management_host: str,
                         now: datetime | None = None,
                         roots: Sequence[x509.Certificate] | None = None) -> dict[str, Any]:
    """Verify exact SANs, public trust, validity and possession before delivery.

    Tests can inject a local CA. Operational callers use the system public trust
    store, never a certificate-supplied root or a caller-selected CA file.
    """
    names = certificate_names(child_domain, management_host)
    if not 0 < len(chain) <= 65_536 or not 0 < len(key) <= 16_384:
        raise CertificateError("certificate material exceeds bounds")
    now = now or datetime.now(UTC)
    if now.tzinfo is None:
        raise CertificateError("certificate validation requires an aware clock")
    try:
        if not re.fullmatch(_CERTIFICATE_PEM, chain):
            raise CertificateError("invalid certificate encoding")
        certificates = x509.load_pem_x509_certificates(chain)
        if not 1 <= len(certificates) <= 5:
            raise CertificateError("invalid certificate chain length")
        leaf = certificates[0]
        private = serialization.load_pem_private_key(key, password=None)
        if not ((isinstance(private, rsa.RSAPrivateKey) and private.key_size >= 2048)
                or (isinstance(private, ec.EllipticCurvePrivateKey)
                    and isinstance(private.curve, (ec.SECP256R1, ec.SECP384R1)))):
            raise CertificateError("unsupported certificate key")
        if (leaf.public_key().public_bytes(serialization.Encoding.DER, serialization.PublicFormat.SubjectPublicKeyInfo)
                != private.public_key().public_bytes(serialization.Encoding.DER, serialization.PublicFormat.SubjectPublicKeyInfo)):
            raise CertificateError("certificate private key mismatch")
        sans = list(leaf.extensions.get_extension_for_class(x509.SubjectAlternativeName).value)
        if len(sans) != 2 or set(sans) != {x509.DNSName(name) for name in names}:
            raise CertificateError("certificate subjects differ from protected scope")
        if leaf.extensions.get_extension_for_class(x509.BasicConstraints).value.ca:
            raise CertificateError("certificate leaf must not be a CA")
        if ExtendedKeyUsageOID.SERVER_AUTH not in leaf.extensions.get_extension_for_class(x509.ExtendedKeyUsage).value:
            raise CertificateError("certificate does not permit server authentication")
        if leaf.not_valid_before_utc > now or leaf.not_valid_after_utc < now + timedelta(days=7):
            raise CertificateError("certificate is not valid for the delivery window")
        trusted = list(roots) if roots is not None else [
            x509.load_der_x509_certificate(der) for der in ssl.create_default_context().get_ca_certs(binary_form=True)
        ]
        (PolicyBuilder().store(Store(trusted)).time(now).build_server_verifier(x509.DNSName(management_host))
         .verify(leaf, certificates[1:]))
        return {"sans": names, "expires_at": leaf.not_valid_after_utc.isoformat(),
                "fingerprint_sha256": leaf.fingerprint(hashes.SHA256()).hex()}
    except CertificateError:
        raise
    except (ValueError, TypeError, x509.ExtensionNotFound, x509.DuplicateExtension, VerificationError):
        raise CertificateError("certificate validation failed") from None


def _sync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _private_directory(path: Path) -> None:
    # Trusted operator owns the parent. Reject symlinks in every path component,
    # not only the final component, before creating any private material.
    if path.absolute() != path.resolve():
        raise CertificateError("certificate state path must not contain symlinks")
    path.mkdir(mode=0o700, exist_ok=True)
    info = path.lstat()
    if not stat.S_ISDIR(info.st_mode) or info.st_uid != os.getuid() or info.st_mode & 0o077:
        raise CertificateError("private certificate directory required")
    _sync_directory(path.parent)


def _private_read(path: Path, *, limit: int = 65_536) -> bytes:
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(descriptor, "rb") as stream:
        info = os.fstat(stream.fileno())
        if (not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid()
                or info.st_mode & 0o077 or info.st_nlink != 1):
            raise CertificateError("private regular certificate file required")
        value = stream.read(limit + 1)
    if len(value) > limit:
        raise CertificateError("private certificate file exceeds bound")
    return value


@contextmanager
def _locked_state(root: Path) -> Iterator[None]:
    try:
        _private_directory(root)
        descriptor = os.open(root / "operation.lock", os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW | os.O_NONBLOCK, 0o600)
        with os.fdopen(descriptor, "rb+") as stream:
            info = os.fstat(stream.fileno())
            if (not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid()
                    or info.st_mode & 0o077 or info.st_nlink != 1):
                raise CertificateError("private certificate lock required")
            fcntl.flock(stream, fcntl.LOCK_EX | fcntl.LOCK_NB)
            yield
    except OSError:
        raise CertificateError("certificate storage unavailable or already in use") from None


def _selected(root: Path) -> dict[str, Any] | None:
    path = root / "selected.json"
    try:
        path.lstat()
    except FileNotFoundError:
        return None
    try:
        value = json.loads(_private_read(path))
        if (not isinstance(value, dict) or value.get("schema") != "loom.nebius-certificate.v1"
                or not isinstance(value.get("generation"), str) or not _GENERATION.fullmatch(value["generation"])
                or not isinstance(value.get("sans"), list)):
            raise CertificateError("invalid certificate selection")
        return value
    except (ValueError, RecursionError):
        raise CertificateError("invalid certificate selection") from None


def _write_private(path: Path, value: bytes) -> None:
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    with os.fdopen(descriptor, "wb") as stream:
        stream.write(value)
        stream.flush()
        os.fsync(stream.fileno())


def _publish(root: Path, chain: bytes, key: bytes, report: dict[str, Any]) -> dict[str, Any]:
    previous = _selected(root)
    if previous is not None and previous["sans"] != report["sans"]:
        raise CertificateError("selected certificate belongs to different subjects")
    generations = root / "generations"
    _private_directory(generations)
    generation_id = hashlib.sha256(chain).hexdigest()
    generation = generations / generation_id
    if generation.exists() or generation.is_symlink():
        _private_directory(generation)
        if (_private_read(generation / "fullchain.pem") != chain
                or _private_read(generation / "privkey.pem", limit=16_384) != key):
            raise CertificateError("existing certificate generation differs")
    else:
        temporary = Path(tempfile.mkdtemp(prefix=".pending-", dir=generations))
        _write_private(temporary / "fullchain.pem", chain)
        _write_private(temporary / "privkey.pem", key)
        _sync_directory(temporary)
        os.rename(temporary, generation)
        _sync_directory(generations)
    if previous is not None and previous["generation"] == generation_id:
        return previous
    selected = {"schema": "loom.nebius-certificate.v1", "generation": generation_id,
                "previous_generation": previous["generation"] if previous else None, **report}
    # Keep partially written generations for private recovery, but never select
    # them. Only the atomic manifest replacement changes the deliverable result.
    with tempfile.NamedTemporaryFile(dir=root, prefix=".selection-", delete=False) as stream:
        pending = Path(stream.name)
        stream.write(json.dumps(selected, sort_keys=True).encode())
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(pending, root / "selected.json")
    _sync_directory(root)
    return selected


def publish_certificate(root: Path, chain: bytes, key: bytes, *, child_domain: str,
                        management_host: str, now: datetime | None = None,
                        roots: Sequence[x509.Certificate] | None = None) -> dict[str, Any]:
    report = validate_certificate(chain, key, child_domain=child_domain,
                                  management_host=management_host, now=now, roots=roots)
    with _locked_state(root):
        return _publish(root, chain, key, report)
