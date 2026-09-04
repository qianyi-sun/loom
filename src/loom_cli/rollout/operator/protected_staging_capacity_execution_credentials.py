"""Strict local authority for protected staging execution credentials."""

from __future__ import annotations

import base64
import hashlib
import json
import os
import stat
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from types import MappingProxyType
from typing import Any, cast

from cryptography import x509
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import ed25519
from cryptography.x509.oid import ExtendedKeyUsageOID
from pydantic import ValidationError

from loom_capacity_manager.auth import _RegistryDocument
from loom_capacity_manager.ownership import (
    OwnershipKeyring,
    OwnershipKeyringError,
    public_key_fingerprint,
)
from loom_cli.capacity_control_plane import CapacityPoolExecutorBinding

_CLIENT_PRINCIPALS = (
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
_MANAGER_PRINCIPALS = _CLIENT_PRINCIPALS[:6]
_CLIENT_FILES = frozenset({"bearer-token", "certificate.pem", "manager-ca.pem", "private-key.pem"})
_MAX_CREDENTIAL_BYTES = 1024 * 1024


@dataclass(frozen=True, slots=True)
class ExecutionClientCredential:
    """One validated client identity; secret payloads are omitted from repr."""

    principal_id: str
    spiffe_uri: str
    bearer_token: bytes = field(repr=False)
    certificate: bytes = field(repr=False)
    manager_ca: bytes = field(repr=False)
    private_key: bytes = field(repr=False)


@dataclass(frozen=True, slots=True)
class ExecutionCredentialBundle:
    """Validated credentials plus their secret-free exact metadata bindings."""

    client_ca_certificate: bytes = field(repr=False)
    clients: Mapping[str, ExecutionClientCredential] = field(repr=False)
    ownership_private_keys: Mapping[str, bytes] = field(repr=False)
    metadata_sha256: Mapping[str, str]


def load_execution_credential_bundle(
    root: Path,
    *,
    expected_uid: int,
    expected_gid: int,
) -> ExecutionCredentialBundle:
    """Load the exact execution subset from one owner-only bootstrap root."""

    if (
        not root.is_absolute()
        or ".." in root.parts
        or type(expected_uid) is not int
        or type(expected_gid) is not int
        or min(expected_uid, expected_gid) < 0
    ):
        raise ValueError("execution credential authority is invalid")
    _validate_directory(root, uid=expected_uid, gid=expected_gid)
    client_ca_payload = _read_private(
        root / "client-ca.pem",
        uid=expected_uid,
        gid=expected_gid,
    )
    client_ca = _load_current_ca(client_ca_payload, label="client")
    client_ca_der = client_ca.public_bytes(serialization.Encoding.DER)
    client_ca_public = _public_key_bytes(client_ca.public_key())
    now = datetime.now(UTC)
    clients: dict[str, ExecutionClientCredential] = {}
    metadata: dict[str, str] = {}
    public_keys: set[bytes] = set()
    tokens: set[bytes] = set()
    manager_ca_payload: bytes | None = None
    for principal in _CLIENT_PRINCIPALS:
        directory = root / principal
        _validate_directory(directory, uid=expected_uid, gid=expected_gid)
        if {path.name for path in directory.iterdir()} != _CLIENT_FILES:
            raise ValueError("execution credential client file set is invalid")
        payloads = {
            name: _read_private(directory / name, uid=expected_uid, gid=expected_gid)
            for name in _CLIENT_FILES
        }
        token = payloads["bearer-token"]
        if not 32 <= len(token) <= 4096 or any(not 0x21 <= byte <= 0x7E for byte in token):
            raise ValueError("execution bearer credential is invalid")
        if token in tokens:
            raise ValueError("execution bearer credential is reused")
        tokens.add(token)
        try:
            certificate = x509.load_pem_x509_certificate(payloads["certificate.pem"])
            certificate.verify_directly_issued_by(client_ca)
            constraints = certificate.extensions.get_extension_for_class(
                x509.BasicConstraints
            ).value
            eku = certificate.extensions.get_extension_for_class(x509.ExtendedKeyUsage).value
            san = certificate.extensions.get_extension_for_class(x509.SubjectAlternativeName).value
            private_key = serialization.load_pem_private_key(
                payloads["private-key.pem"],
                password=None,
            )
        except (TypeError, ValueError, x509.ExtensionNotFound) as exc:
            raise ValueError("execution client identity is invalid") from exc
        spiffe_uri = f"spiffe://loom.openai.dev/staging/capacity/{principal}"
        certificate_public = _public_key_bytes(certificate.public_key())
        if (
            constraints.ca
            or eku != x509.ExtendedKeyUsage([ExtendedKeyUsageOID.CLIENT_AUTH])
            or len(san) != 1
            or san.get_values_for_type(x509.UniformResourceIdentifier) != [spiffe_uri]
            or not certificate.not_valid_before_utc <= now <= certificate.not_valid_after_utc
            or certificate_public != _private_public_key_bytes(private_key)
            or certificate_public in public_keys
        ):
            raise ValueError("execution client identity is invalid")
        public_keys.add(certificate_public)
        observed_manager_ca = _load_current_ca(payloads["manager-ca.pem"], label="manager")
        observed_manager_ca_der = observed_manager_ca.public_bytes(serialization.Encoding.DER)
        if (
            observed_manager_ca_der == client_ca_der
            or _public_key_bytes(observed_manager_ca.public_key()) == client_ca_public
        ):
            raise ValueError("execution manager and client CAs overlap")
        if manager_ca_payload is None:
            manager_ca_payload = payloads["manager-ca.pem"]
        elif payloads["manager-ca.pem"] != manager_ca_payload:
            raise ValueError("execution clients use different manager CAs")
        clients[principal] = ExecutionClientCredential(
            principal_id=principal,
            spiffe_uri=spiffe_uri,
            bearer_token=token,
            certificate=payloads["certificate.pem"],
            manager_ca=payloads["manager-ca.pem"],
            private_key=payloads["private-key.pem"],
        )
        metadata[principal] = _hash_json(
            {
                "files": {
                    name: hashlib.sha256(payload).hexdigest()
                    for name, payload in sorted(payloads.items())
                },
                "principal_id": principal,
                "public_key_sha256": hashlib.sha256(certificate_public).hexdigest(),
                "schema_version": 1,
                "spiffe_uri": spiffe_uri,
            }
        )
    ownership_private_keys: dict[str, bytes] = {}
    ownership_public_keys: set[bytes] = set()
    for pool in _OWNERSHIP_POOLS:
        directory = root / f"pool-ownership-{pool}"
        _validate_directory(directory, uid=expected_uid, gid=expected_gid)
        if {path.name for path in directory.iterdir()} != {"ownership-private-key"}:
            raise ValueError("execution ownership credential file set is invalid")
        private_payload = _read_private(
            directory / "ownership-private-key",
            uid=expected_uid,
            gid=expected_gid,
        )
        try:
            private_key = ed25519.Ed25519PrivateKey.from_private_bytes(private_payload)
            public_payload = private_key.public_key().public_bytes(
                serialization.Encoding.Raw,
                serialization.PublicFormat.Raw,
            )
        except ValueError as exc:
            raise ValueError("execution ownership credential is invalid") from exc
        if public_payload in ownership_public_keys:
            raise ValueError("execution ownership credential is reused")
        ownership_public_keys.add(public_payload)
        ownership_private_keys[pool] = private_payload
        metadata[f"pool-ownership-{pool}"] = _hash_json(
            {
                "pool_id": pool,
                "private_key_sha256": hashlib.sha256(private_payload).hexdigest(),
                "public_key_sha256": hashlib.sha256(public_payload).hexdigest(),
                "schema_version": 1,
            }
        )
    return ExecutionCredentialBundle(
        client_ca_certificate=client_ca_payload,
        clients=MappingProxyType(clients),
        ownership_private_keys=MappingProxyType(ownership_private_keys),
        metadata_sha256=MappingProxyType(dict(sorted(metadata.items()))),
    )


def build_execution_principal_registry(
    payload: bytes,
    *,
    bundle: ExecutionCredentialBundle,
    pools: Sequence[CapacityPoolExecutorBinding],
) -> bytes:
    """Add only the exact execution principals to an existing valid registry."""

    bindings = _pool_bindings(pools)
    registry = _strict_json(payload, label="execution principal registry")
    principals = registry.get("principals")
    if not isinstance(principals, list):
        raise ValueError("execution principal registry is invalid")
    try:
        _RegistryDocument.model_validate_json(
            json.dumps(registry, sort_keys=True, separators=(",", ":"))
        )
    except (ValidationError, ValueError) as exc:
        raise ValueError("execution principal registry is invalid") from exc
    desired: list[dict[str, object]] = []
    manager_scopes = {
        "manager-read": "capacity:read",
        "manager-prepare": "capacity:execution:prepare",
        "manager-activate": "capacity:execution:activate",
        "manager-drain": "capacity:execution:drain",
        "manager-retire": "capacity:execution:retire",
        "manager-abort": "capacity:execution:abort",
    }
    for principal_name, scope in manager_scopes.items():
        desired.append(
            _principal_document(
                principal_id=principal_name,
                token=bundle.clients[principal_name].bearer_token,
                scope=scope,
            )
        )
    for pool_id in _OWNERSHIP_POOLS:
        binding = bindings[pool_id]
        desired.append(
            _principal_document(
                principal_id=f"pool-executor-{pool_id}",
                token=bundle.clients[f"pool-executor-{pool_id}"].bearer_token,
                scope="capacity:execute:pool",
                pool=binding,
            )
        )
    existing_ids: dict[str, object] = {}
    existing_tokens: dict[str, str] = {}
    for existing_principal in principals:
        if not isinstance(existing_principal, dict):
            raise ValueError("execution principal registry is invalid")
        principal_id = existing_principal.get("principal_id")
        token_sha256 = existing_principal.get("token_sha256")
        if not isinstance(principal_id, str) or not isinstance(token_sha256, str):
            raise ValueError("execution principal registry is invalid")
        existing_ids[principal_id] = existing_principal
        existing_tokens[token_sha256] = principal_id
    for desired_principal in desired:
        principal_id = str(desired_principal["principal_id"])
        token_sha256 = str(desired_principal["token_sha256"])
        existing = existing_ids.get(principal_id)
        if existing is not None:
            if existing != desired_principal:
                raise ValueError("execution principal conflicts with existing authority")
            continue
        token_owner = existing_tokens.get(token_sha256)
        if token_owner is not None:
            raise ValueError("execution principal token overlaps existing authority")
        principals.append(desired_principal)
        existing_ids[principal_id] = desired_principal
        existing_tokens[token_sha256] = principal_id
    canonical = (json.dumps(registry, sort_keys=True, separators=(",", ":")) + "\n").encode("ascii")
    try:
        _RegistryDocument.model_validate_json(canonical)
    except (ValidationError, ValueError, json.JSONDecodeError) as exc:
        raise ValueError("execution principal registry is invalid") from exc
    return canonical


def build_execution_ownership_keyring(
    payload: bytes,
    *,
    bundle: ExecutionCredentialBundle,
    pools: Sequence[CapacityPoolExecutorBinding],
) -> bytes:
    """Add the two exact pool public keys without exposing private material."""

    bindings = _pool_bindings(pools)
    document = _strict_json(payload, label="execution ownership keyring")
    entries = document.get("keys")
    if (
        set(document) != {"schema_version", "keys"}
        or document.get("schema_version") != 1
        or not isinstance(entries, list)
    ):
        raise ValueError("execution ownership keyring is invalid")
    try:
        OwnershipKeyring.from_json(payload.decode("ascii"))
    except (UnicodeDecodeError, OwnershipKeyringError) as exc:
        raise ValueError("execution ownership keyring is invalid") from exc
    desired: dict[str, dict[str, object]] = {}
    for pool_id in _OWNERSHIP_POOLS:
        binding = bindings[pool_id]
        private_key = ed25519.Ed25519PrivateKey.from_private_bytes(
            bundle.ownership_private_keys[pool_id]
        )
        public_key = private_key.public_key()
        if public_key_fingerprint(public_key) != binding.signing_key_sha256:
            raise ValueError("execution ownership key differs from the pool binding")
        desired[binding.signing_key_id] = {
            "public_key_base64": base64.b64encode(
                public_key.public_bytes(
                    serialization.Encoding.Raw,
                    serialization.PublicFormat.Raw,
                )
            ).decode("ascii"),
            "signing_key_id": binding.signing_key_id,
        }
    existing: dict[str, dict[str, object]] = {}
    for entry in entries:
        if not isinstance(entry, dict) or not isinstance(entry.get("signing_key_id"), str):
            raise ValueError("execution ownership keyring is invalid")
        existing[str(entry["signing_key_id"])] = entry
    for key_id, entry in desired.items():
        current = existing.get(key_id)
        if current is not None and current != entry:
            raise ValueError("execution ownership key conflicts with existing authority")
        existing[key_id] = entry
    document["keys"] = [existing[key_id] for key_id in sorted(existing)]
    canonical = (json.dumps(document, sort_keys=True, separators=(",", ":")) + "\n").encode("ascii")
    try:
        OwnershipKeyring.from_json(canonical.decode("ascii"))
    except OwnershipKeyringError as exc:
        raise ValueError("execution ownership keyring is invalid") from exc
    return canonical


def build_execution_backup_secret_documents(
    bundle: ExecutionCredentialBundle,
) -> Mapping[str, bytes]:
    """Render the three immutable recovery Secrets from one validated bundle."""

    if not isinstance(bundle, ExecutionCredentialBundle):
        raise ValueError("execution credential bundle is invalid")
    operator_data = {
        f"{principal}.{filename}": base64.b64encode(
            _client_payload(bundle.clients[principal], filename)
        ).decode("ascii")
        for principal in _MANAGER_PRINCIPALS
        for filename in sorted(_CLIENT_FILES)
    }
    documents = {
        "loom-capacity-execution-operator": _secret_document(
            "loom-capacity-execution-operator",
            operator_data,
        )
    }
    for pool in _OWNERSHIP_POOLS:
        credential = bundle.clients[f"pool-executor-{pool}"]
        documents[f"loom-capacity-executor-{pool}"] = _secret_document(
            f"loom-capacity-executor-{pool}",
            {
                "bearer-token": base64.b64encode(credential.bearer_token).decode("ascii"),
                "client-certificate.pem": base64.b64encode(credential.certificate).decode("ascii"),
                "client-private-key.pem": base64.b64encode(credential.private_key).decode("ascii"),
                "manager-ca.pem": base64.b64encode(credential.manager_ca).decode("ascii"),
                "ownership-private-key": base64.b64encode(
                    bundle.ownership_private_keys[pool]
                ).decode("ascii"),
            },
        )
    return MappingProxyType(documents)


def _client_payload(credential: ExecutionClientCredential, filename: str) -> bytes:
    values = {
        "bearer-token": credential.bearer_token,
        "certificate.pem": credential.certificate,
        "manager-ca.pem": credential.manager_ca,
        "private-key.pem": credential.private_key,
    }
    try:
        return values[filename]
    except KeyError as exc:  # pragma: no cover - fixed internal inventory
        raise ValueError("execution client Secret field is invalid") from exc


def _secret_document(name: str, data: Mapping[str, str]) -> bytes:
    return (
        json.dumps(
            {
                "apiVersion": "v1",
                "data": dict(sorted(data.items())),
                "immutable": True,
                "kind": "Secret",
                "metadata": {
                    "labels": {
                        "app.kubernetes.io/managed-by": "loom-staging-rollout",
                        "app.kubernetes.io/name": name,
                        "loom.carin.dev/protected-component": (
                            "staging-capacity-execution-credentials"
                        ),
                    },
                    "name": name,
                    "namespace": "loom-dev",
                },
                "type": "Opaque",
            },
            sort_keys=True,
            separators=(",", ":"),
        )
        + "\n"
    ).encode("ascii")


def _pool_bindings(
    pools: Sequence[CapacityPoolExecutorBinding],
) -> dict[str, CapacityPoolExecutorBinding]:
    bindings: dict[str, CapacityPoolExecutorBinding] = {
        pool.pool_id: pool for pool in pools if isinstance(pool, CapacityPoolExecutorBinding)
    }
    if len(pools) != 2 or set(bindings) != set(_OWNERSHIP_POOLS):
        raise ValueError("execution pool credential bindings are invalid")
    return bindings


def _principal_document(
    *,
    principal_id: str,
    token: bytes,
    scope: str,
    pool: CapacityPoolExecutorBinding | None = None,
) -> dict[str, object]:
    return {
        "demand_reporter_incarnation": None,
        "executor_id": None if pool is None else pool.executor_id,
        "executor_incarnation": None if pool is None else pool.executor_incarnation,
        "executor_pool_generation": None if pool is None else pool.pool_generation,
        "pool_id": None if pool is None else pool.pool_id,
        "pool_reporter_incarnation": None,
        "principal_id": principal_id,
        "scopes": [scope],
        "subject_id": None,
        "subject_incarnation": None,
        "token_sha256": hashlib.sha256(token).hexdigest(),
    }


def _validate_directory(path: Path, *, uid: int, gid: int) -> None:
    metadata = path.stat(follow_symlinks=False)
    if (
        not stat.S_ISDIR(metadata.st_mode)
        or stat.S_IMODE(metadata.st_mode) != 0o700
        or metadata.st_uid != uid
        or metadata.st_gid != gid
    ):
        raise ValueError("execution credential directory is unsafe")


def _read_private(path: Path, *, uid: int, gid: int) -> bytes:
    descriptor = os.open(
        path,
        os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0),
    )
    try:
        before = os.fstat(descriptor)
        if (
            not stat.S_ISREG(before.st_mode)
            or stat.S_IMODE(before.st_mode) != 0o600
            or before.st_uid != uid
            or before.st_gid != gid
            or before.st_nlink != 1
            or not 0 < before.st_size <= _MAX_CREDENTIAL_BYTES
        ):
            raise ValueError("execution credential file is unsafe")
        chunks: list[bytes] = []
        remaining = before.st_size
        while remaining:
            chunk = os.read(descriptor, min(65536, remaining))
            if not chunk:
                raise ValueError("execution credential file changed while reading")
            chunks.append(chunk)
            remaining -= len(chunk)
        after = os.fstat(descriptor)
        if _metadata_identity(before) != _metadata_identity(after):
            raise ValueError("execution credential file changed while reading")
        return b"".join(chunks)
    finally:
        os.close(descriptor)


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


def _load_current_ca(payload: bytes, *, label: str) -> x509.Certificate:
    try:
        certificate = x509.load_pem_x509_certificate(payload)
        constraints = certificate.extensions.get_extension_for_class(x509.BasicConstraints).value
        certificate.verify_directly_issued_by(certificate)
    except (TypeError, ValueError, x509.ExtensionNotFound) as exc:
        raise ValueError(f"execution {label} CA is invalid") from exc
    now = datetime.now(UTC)
    if (
        not constraints.ca
        or not certificate.not_valid_before_utc <= now <= certificate.not_valid_after_utc
    ):
        raise ValueError(f"execution {label} CA is invalid")
    return certificate


def _public_key_bytes(key: Any) -> bytes:
    try:
        return cast(
            bytes,
            key.public_bytes(
                serialization.Encoding.DER,
                serialization.PublicFormat.SubjectPublicKeyInfo,
            ),
        )
    except (AttributeError, TypeError, ValueError) as exc:
        raise ValueError("execution credential public key is invalid") from exc


def _private_public_key_bytes(key: Any) -> bytes:
    try:
        return _public_key_bytes(key.public_key())
    except AttributeError as exc:
        raise ValueError("execution credential private key is invalid") from exc


def _hash_json(value: object) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":")).encode("ascii")
    ).hexdigest()


def _strict_json(payload: bytes, *, label: str) -> dict[str, object]:
    if not payload or len(payload) > _MAX_CREDENTIAL_BYTES:
        raise ValueError(f"{label} is invalid")

    def reject_duplicates(pairs: list[tuple[str, object]]) -> dict[str, object]:
        result: dict[str, object] = {}
        for key, value in pairs:
            if key in result:
                raise ValueError(f"{label} contains duplicate fields")
            result[key] = value
        return result

    try:
        value = json.loads(payload, object_pairs_hook=reject_duplicates)
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
        raise ValueError(f"{label} is invalid") from exc
    if not isinstance(value, dict):
        raise ValueError(f"{label} is invalid")
    return value


__all__ = [
    "ExecutionClientCredential",
    "ExecutionCredentialBundle",
    "build_execution_backup_secret_documents",
    "build_execution_ownership_keyring",
    "build_execution_principal_registry",
    "load_execution_credential_bundle",
]
