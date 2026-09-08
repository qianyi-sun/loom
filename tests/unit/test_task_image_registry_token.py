from __future__ import annotations

import base64
import hashlib
import json
import os
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import UUID

import jwt
import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import ec, rsa

from loom_task_image_authority import config
from loom_task_image_authority.config import (
    TaskImageAuthorityConfigurationError,
    TaskImageAuthoritySettings,
)
from loom_task_image_authority.registry_token import (
    DistributionRegistryTokenIssuer,
    load_distribution_registry_token_issuer,
    publication_repository,
)

ATTEMPT_ID = UUID("11111111-1111-4111-8111-111111111111")
CREDENTIAL_ID = UUID("22222222-2222-4222-8222-222222222222")
NOW = datetime(2026, 9, 4, 12, 0, tzinfo=UTC)


def _private_key(*, bits: int = 3072, exponent: int = 65537) -> rsa.RSAPrivateKey:
    return rsa.generate_private_key(public_exponent=exponent, key_size=bits)


def _pem(
    private_key: rsa.RSAPrivateKey,
    *,
    encryption: serialization.KeySerializationEncryption | None = None,
) -> bytes:
    return private_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=encryption or serialization.NoEncryption(),
    )


def _owner_only(path: Path, payload: bytes) -> Path:
    path.write_bytes(payload)
    path.chmod(0o600)
    return path


def _settings(
    tmp_path: Path,
    signing_key_file: Path,
    **overrides: object,
) -> TaskImageAuthoritySettings:
    values: dict[str, object] = {
        "principals_file": tmp_path / "principals.json",
        "db_url_file": tmp_path / "database-url",
        "secret_store_keyring_file": tmp_path / "keyring.json",
        "tls_cert_file": tmp_path / "server.pem",
        "tls_key_file": tmp_path / "server-key.pem",
        "tls_client_ca_file": tmp_path / "client-ca.pem",
        "registry_origin": "https://registry.example:5443",
        "registry_service": "registry.example",
        "registry_issuer": "loom-task-image-authority",
        "registry_signing_key_file": signing_key_file,
    }
    values.update(overrides)
    return TaskImageAuthoritySettings(**values)


def _base64url_uint(value: int) -> str:
    raw = value.to_bytes((value.bit_length() + 7) // 8, "big")
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode("ascii")


def _expected_thumbprint(private_key: rsa.RSAPrivateKey) -> str:
    public = private_key.public_key().public_numbers()
    canonical_jwk = json.dumps(
        {"e": _base64url_uint(public.e), "kty": "RSA", "n": _base64url_uint(public.n)},
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return base64.urlsafe_b64encode(hashlib.sha256(canonical_jwk).digest()).rstrip(
        b"="
    ).decode("ascii")


def test_publication_repository_derives_exact_task_and_sidecar_paths() -> None:
    assert publication_repository(
        purpose="production",
        shadow_campaign_id=None,
        cpu_arch="x86_64",
        attempt_id=ATTEMPT_ID,
        component="task",
    ) == f"loom-task-image-attempts/x86_64/{ATTEMPT_ID}/task"

    sidecar_digest = hashlib.sha256(b"sidecar:Redis_cache.1").hexdigest()
    assert publication_repository(
        purpose="production",
        shadow_campaign_id=None,
        cpu_arch="arm64",
        attempt_id=ATTEMPT_ID,
        component="sidecar:Redis_cache.1",
    ) == (
        f"loom-task-image-attempts/arm64/{ATTEMPT_ID}/"
        f"sidecar-sha256-{sidecar_digest}"
    )


@pytest.mark.parametrize(
    "changes",
    [
        {"purpose": "shadow"},
        {"purpose": "Production"},
        {"shadow_campaign_id": UUID("33333333-3333-4333-8333-333333333333")},
        {"cpu_arch": "amd64"},
        {"cpu_arch": "ARM64"},
        {"attempt_id": UUID(int=0)},
        {"attempt_id": str(ATTEMPT_ID)},
        {"component": ""},
        {"component": "sidecar:"},
        {"component": "sidecar:bad/name"},
        {"component": "sidecar:" + "x" * 129},
        {"component": "TASK"},
    ],
)
def test_publication_repository_rejects_unavailable_or_noncanonical_inputs(
    changes: dict[str, object],
) -> None:
    values: dict[str, object] = {
        "purpose": "production",
        "shadow_campaign_id": None,
        "cpu_arch": "arm64",
        "attempt_id": ATTEMPT_ID,
        "component": "task",
    }
    values.update(changes)
    with pytest.raises((TypeError, ValueError)):
        publication_repository(**values)  # type: ignore[arg-type]


def test_loaded_issuer_signs_one_exact_standard_distribution_scope(
    tmp_path: Path,
) -> None:
    private_key = _private_key()
    key_path = _owner_only(tmp_path / "registry-signing.pem", _pem(private_key))
    issuer = load_distribution_registry_token_issuer(_settings(tmp_path, key_path))
    repository = publication_repository(
        purpose="production",
        shadow_campaign_id=None,
        cpu_arch="arm64",
        attempt_id=ATTEMPT_ID,
        component="task",
    )

    issued = issuer.issue(
        credential_id=CREDENTIAL_ID,
        repository=repository,
        issued_at=NOW,
        expires_at=NOW + timedelta(seconds=45),
    )

    expected_key_id = _expected_thumbprint(private_key)
    assert issued.key_id == expected_key_id
    assert issued.registry_origin == "https://registry.example:5443"
    assert issued.service == "registry.example"
    assert issued.issuer == "loom-task-image-authority"
    assert jwt.get_unverified_header(issued.token) == {
        "alg": "RS256",
        "kid": expected_key_id,
        "typ": "JWT",
    }
    claims = jwt.decode(
        issued.token,
        private_key.public_key(),
        algorithms=["RS256"],
        audience="registry.example",
        issuer="loom-task-image-authority",
        options={"verify_exp": False, "verify_nbf": False, "verify_iat": False},
    )
    assert claims == {
        "iss": "loom-task-image-authority",
        "sub": f"loom-task-image-builder:{CREDENTIAL_ID}",
        "aud": "registry.example",
        "exp": int((NOW + timedelta(seconds=45)).timestamp()),
        "nbf": int(NOW.timestamp()),
        "iat": int(NOW.timestamp()),
        "jti": str(CREDENTIAL_ID),
        "access": [
            {
                "type": "repository",
                "name": repository,
                "actions": ["pull", "push"],
            }
        ],
    }
    rendered = repr(issued)
    assert issued.token not in rendered
    assert "eyJ" not in rendered
    assert "<redacted>" in rendered


def test_key_id_is_stable_for_the_same_public_key(tmp_path: Path) -> None:
    private_key = _private_key()
    first_path = _owner_only(tmp_path / "first.pem", _pem(private_key))
    second_path = _owner_only(tmp_path / "second.pem", _pem(private_key))

    first = load_distribution_registry_token_issuer(_settings(tmp_path, first_path))
    second = load_distribution_registry_token_issuer(_settings(tmp_path, second_path))

    assert first.key_id == second.key_id == _expected_thumbprint(private_key)


def test_loader_is_unavailable_when_optional_registry_settings_are_absent(
    tmp_path: Path,
) -> None:
    settings = TaskImageAuthoritySettings(
        principals_file=tmp_path / "principals.json",
        db_url_file=tmp_path / "database-url",
        secret_store_keyring_file=tmp_path / "keyring.json",
        tls_cert_file=tmp_path / "server.pem",
        tls_key_file=tmp_path / "server-key.pem",
        tls_client_ca_file=tmp_path / "client-ca.pem",
    )

    with pytest.raises(TaskImageAuthorityConfigurationError, match="unavailable"):
        load_distribution_registry_token_issuer(settings)


@pytest.mark.parametrize(
    "changes",
    [
        {"credential_id": UUID(int=0)},
        {"credential_id": str(CREDENTIAL_ID)},
        {"repository": "library/alpine"},
        {"repository": "loom-task-image-attempts/arm64/not-a-uuid/task"},
        {"issued_at": datetime(2026, 9, 4, 12, 0)},
        {"expires_at": datetime(2026, 9, 4, 12, 1)},
        {"expires_at": NOW},
        {"expires_at": NOW + timedelta(seconds=46)},
        {"issued_at": NOW + timedelta(microseconds=1)},
        {"expires_at": NOW + timedelta(seconds=45, microseconds=1)},
    ],
)
def test_issuer_rejects_invalid_identity_repository_or_times(
    tmp_path: Path,
    changes: dict[str, object],
) -> None:
    private_key = _private_key()
    key_path = _owner_only(tmp_path / "registry-signing.pem", _pem(private_key))
    issuer = load_distribution_registry_token_issuer(_settings(tmp_path, key_path))
    values: dict[str, object] = {
        "credential_id": CREDENTIAL_ID,
        "repository": f"loom-task-image-attempts/arm64/{ATTEMPT_ID}/task",
        "issued_at": NOW,
        "expires_at": NOW + timedelta(seconds=45),
    }
    values.update(changes)

    with pytest.raises((TypeError, ValueError)):
        issuer.issue(**values)  # type: ignore[arg-type]


def test_issuer_has_no_caller_selected_algorithm_or_claims(tmp_path: Path) -> None:
    private_key = _private_key()
    key_path = _owner_only(tmp_path / "registry-signing.pem", _pem(private_key))
    issuer = load_distribution_registry_token_issuer(_settings(tmp_path, key_path))
    values = {
        "credential_id": CREDENTIAL_ID,
        "repository": f"loom-task-image-attempts/arm64/{ATTEMPT_ID}/task",
        "issued_at": NOW,
        "expires_at": NOW + timedelta(seconds=45),
    }

    with pytest.raises(TypeError):
        issuer.issue(**values, algorithm="HS256")  # type: ignore[call-arg]
    with pytest.raises(TypeError):
        issuer.issue(**values, access=[{"type": "registry", "actions": ["*"]}])  # type: ignore[call-arg]


@pytest.mark.parametrize(
    ("payload_factory", "message"),
    [
        (lambda: _pem(_private_key(bits=2048)), "3072"),
        (lambda: _pem(_private_key(exponent=3)), "65537"),
        (
            lambda: _pem(
                _private_key(),
                encryption=serialization.BestAvailableEncryption(b"password"),
            ),
            "unencrypted",
        ),
        (
            lambda: ec.generate_private_key(ec.SECP256R1()).private_bytes(
                serialization.Encoding.PEM,
                serialization.PrivateFormat.PKCS8,
                serialization.NoEncryption(),
            ),
            "RSA",
        ),
        (
            lambda: _private_key().public_key().public_bytes(
                serialization.Encoding.PEM,
                serialization.PublicFormat.SubjectPublicKeyInfo,
            ),
            "private key",
        ),
        (lambda: b"not a PEM", "private key"),
    ],
)
def test_loader_rejects_wrong_or_unsafe_key_material(
    tmp_path: Path,
    payload_factory: Callable[[], bytes],
    message: str,
) -> None:
    payload = payload_factory()
    key_path = _owner_only(tmp_path / "registry-signing.pem", payload)

    with pytest.raises(TaskImageAuthorityConfigurationError, match=message):
        load_distribution_registry_token_issuer(_settings(tmp_path, key_path))


def test_loader_rejects_unsafe_key_file_metadata(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    private_key = _private_key()
    key_path = _owner_only(tmp_path / "registry-signing.pem", _pem(private_key))
    key_path.chmod(0o640)
    with pytest.raises(TaskImageAuthorityConfigurationError, match="0600"):
        load_distribution_registry_token_issuer(_settings(tmp_path, key_path))

    key_path.chmod(0o600)
    link_path = tmp_path / "registry-signing-link.pem"
    link_path.symlink_to(key_path)
    with pytest.raises(TaskImageAuthorityConfigurationError, match="nonsymlink"):
        load_distribution_registry_token_issuer(_settings(tmp_path, link_path))

    current_uid = os.getuid()
    monkeypatch.setattr(config.os, "getuid", lambda: current_uid + 1)
    with pytest.raises(TaskImageAuthorityConfigurationError, match="current-uid"):
        load_distribution_registry_token_issuer(_settings(tmp_path, key_path))


def test_constructor_rejects_invalid_origin_service_and_issuer() -> None:
    private_key = _private_key()
    valid = {
        "private_key": private_key,
        "registry_origin": "https://registry.example",
        "service": "registry.example",
        "issuer": "loom-task-image-authority",
    }
    invalid_changes = [
        {"registry_origin": "http://registry.example"},
        {"registry_origin": "https://user@registry.example"},
        {"registry_origin": "https://registry.example/path"},
        {"service": ""},
        {"service": "UPPER"},
        {"issuer": ""},
        {"issuer": "issuer with spaces"},
    ]
    for changes in invalid_changes:
        with pytest.raises((TypeError, ValueError)):
            DistributionRegistryTokenIssuer(**(valid | changes))  # type: ignore[arg-type]
