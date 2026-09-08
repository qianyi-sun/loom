"""Exact-scope OCI Distribution registry token issuance."""

from __future__ import annotations

import base64
import hashlib
import re
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID

import jwt
import rfc8785
from cryptography.exceptions import UnsupportedAlgorithm
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa

from loom_task_image_authority.config import (
    TaskImageAuthorityConfigurationError,
    TaskImageAuthoritySettings,
    _validate_https_origin,
    _validate_registry_identity,
    read_owner_only_bytes,
)

_COMPONENT_RE = re.compile(r"(?:task|sidecar:[A-Za-z0-9][A-Za-z0-9_.-]{0,127})")
_REPOSITORY_RE = re.compile(
    r"loom-task-image-attempts/(?:x86_64|arm64)/"
    r"(?P<attempt>[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-"
    r"[0-9a-f]{4}-[0-9a-f]{12})/"
    r"(?:task|sidecar-sha256-[0-9a-f]{64})"
)
_MAX_TOKEN_LIFETIME = timedelta(seconds=45)
_MAX_SIGNING_KEY_BYTES = 64 * 1024


def publication_repository(
    *,
    purpose: str,
    shadow_campaign_id: UUID | None,
    cpu_arch: str,
    attempt_id: UUID,
    component: str,
) -> str:
    """Derive the only production repository authorized for a component."""

    if purpose != "production" or shadow_campaign_id is not None:
        raise ValueError("registry publication is available only for production attempts")
    if cpu_arch not in {"x86_64", "arm64"}:
        raise ValueError("registry publication architecture is invalid")
    if type(attempt_id) is not UUID or attempt_id.int == 0:
        raise TypeError("registry publication attempt ID must be a nonzero UUID")
    if type(component) is not str or _COMPONENT_RE.fullmatch(component) is None:
        raise ValueError("registry publication component is invalid")

    if component == "task":
        component_segment = "task"
    else:
        component_sha256 = hashlib.sha256(component.encode("ascii")).hexdigest()
        component_segment = f"sidecar-sha256-{component_sha256}"
    return (
        f"loom-task-image-attempts/{cpu_arch}/{attempt_id}/"
        f"{component_segment}"
    )


def _base64url_uint(value: int) -> str:
    raw = value.to_bytes((value.bit_length() + 7) // 8, "big")
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode("ascii")


def _public_jwk_thumbprint(private_key: rsa.RSAPrivateKey) -> str:
    numbers = private_key.public_key().public_numbers()
    canonical_jwk = rfc8785.dumps(
        {
            "e": _base64url_uint(numbers.e),
            "kty": "RSA",
            "n": _base64url_uint(numbers.n),
        }
    )
    return base64.urlsafe_b64encode(hashlib.sha256(canonical_jwk).digest()).rstrip(
        b"="
    ).decode("ascii")


def _validated_private_key(private_key: object) -> rsa.RSAPrivateKey:
    if not isinstance(private_key, rsa.RSAPrivateKey):
        raise TaskImageAuthorityConfigurationError("registry signing key must be RSA")
    if private_key.key_size < 3072:
        raise TaskImageAuthorityConfigurationError(
            "registry signing RSA key must be at least 3072 bits"
        )
    if private_key.public_key().public_numbers().e != 65537:
        raise TaskImageAuthorityConfigurationError(
            "registry signing RSA key exponent must be 65537"
        )
    return private_key


def _utc_second(value: datetime, *, label: str) -> datetime:
    if (
        type(value) is not datetime
        or value.utcoffset() is None
        or value.microsecond != 0
    ):
        raise ValueError(f"{label} must be a timezone-aware whole second")
    return value.astimezone(UTC)


def _validate_repository(repository: str) -> str:
    if type(repository) is not str:
        raise TypeError("registry repository must be a string")
    match = _REPOSITORY_RE.fullmatch(repository)
    if match is None:
        raise ValueError("registry repository is invalid")
    attempt_text = match.group("attempt")
    try:
        attempt_id = UUID(attempt_text)
    except ValueError:
        raise ValueError("registry repository attempt ID is invalid") from None
    if attempt_id.int == 0 or str(attempt_id) != attempt_text:
        raise ValueError("registry repository attempt ID is invalid")
    return repository


@dataclass(frozen=True, slots=True, repr=False)
class IssuedRegistryToken:
    """One sensitive bearer token plus its nonsecret verifier metadata."""

    token: str = field(repr=False)
    key_id: str
    registry_origin: str
    service: str
    issuer: str

    def __repr__(self) -> str:
        return (
            "IssuedRegistryToken(token=<redacted>, "
            f"key_id={self.key_id!r}, registry_origin={self.registry_origin!r}, "
            f"service={self.service!r}, issuer={self.issuer!r})"
        )


class DistributionRegistryTokenIssuer:
    """Fixed RS256 signer for one configured Distribution token service."""

    __slots__ = (
        "_issuer",
        "_key_id",
        "_private_key",
        "_registry_origin",
        "_service",
    )

    def __init__(
        self,
        *,
        private_key: rsa.RSAPrivateKey,
        registry_origin: str,
        service: str,
        issuer: str,
    ) -> None:
        validated_key = _validated_private_key(private_key)
        self._private_key = validated_key
        self._registry_origin = _validate_https_origin(
            registry_origin,
            label="registry origin",
        )
        self._service = _validate_registry_identity(service, label="registry service")
        self._issuer = _validate_registry_identity(issuer, label="registry issuer")
        self._key_id = _public_jwk_thumbprint(validated_key)

    @property
    def key_id(self) -> str:
        return self._key_id

    @property
    def registry_origin(self) -> str:
        return self._registry_origin

    @property
    def service(self) -> str:
        return self._service

    @property
    def issuer(self) -> str:
        return self._issuer

    def issue(
        self,
        *,
        credential_id: UUID,
        repository: str,
        issued_at: datetime,
        expires_at: datetime,
    ) -> IssuedRegistryToken:
        """Sign one short-lived pull/push grant for an exact repository."""

        if type(credential_id) is not UUID or credential_id.int == 0:
            raise TypeError("registry credential ID must be a nonzero UUID")
        repository = _validate_repository(repository)
        issued_at = _utc_second(issued_at, label="registry token issue time")
        expires_at = _utc_second(expires_at, label="registry token expiry")
        if not issued_at < expires_at or expires_at - issued_at > _MAX_TOKEN_LIFETIME:
            raise ValueError("registry token lifetime is invalid")

        issued_epoch = int(issued_at.timestamp())
        claims: dict[str, Any] = {
            "iss": self._issuer,
            "sub": f"loom-task-image-builder:{credential_id}",
            "aud": self._service,
            "exp": int(expires_at.timestamp()),
            "nbf": issued_epoch,
            "iat": issued_epoch,
            "jti": str(credential_id),
            "access": [
                {
                    "type": "repository",
                    "name": repository,
                    "actions": ["pull", "push"],
                }
            ],
        }
        token = jwt.encode(
            claims,
            self._private_key,
            algorithm="RS256",
            headers={"kid": self._key_id, "typ": "JWT"},
        )
        if type(token) is not str:
            raise RuntimeError("registry token signer returned an invalid result")
        return IssuedRegistryToken(
            token=token,
            key_id=self._key_id,
            registry_origin=self._registry_origin,
            service=self._service,
            issuer=self._issuer,
        )


def load_distribution_registry_token_issuer(
    settings: TaskImageAuthoritySettings,
) -> DistributionRegistryTokenIssuer:
    """Load the optional fixed registry signer from fail-closed settings."""

    if type(settings) is not TaskImageAuthoritySettings:
        raise TypeError("task-image authority settings are required")
    if (
        settings.registry_origin is None
        or settings.registry_service is None
        or settings.registry_issuer is None
        or settings.registry_signing_key_file is None
    ):
        raise TaskImageAuthorityConfigurationError(
            "registry credential configuration is unavailable"
        )

    payload = read_owner_only_bytes(
        settings.registry_signing_key_file,
        max_bytes=_MAX_SIGNING_KEY_BYTES,
    )
    try:
        private_key = serialization.load_pem_private_key(payload, password=None)
    except TypeError:
        raise TaskImageAuthorityConfigurationError(
            "registry signing key must be unencrypted"
        ) from None
    except (ValueError, UnsupportedAlgorithm):
        raise TaskImageAuthorityConfigurationError(
            "registry signing private key is invalid"
        ) from None

    return DistributionRegistryTokenIssuer(
        private_key=_validated_private_key(private_key),
        registry_origin=settings.registry_origin,
        service=settings.registry_service,
        issuer=settings.registry_issuer,
    )


__all__ = [
    "DistributionRegistryTokenIssuer",
    "IssuedRegistryToken",
    "load_distribution_registry_token_issuer",
    "publication_repository",
]
