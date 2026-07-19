"""Bounded TokenRequest authority for least-privilege preflight kubeconfigs."""

from __future__ import annotations

import base64
import binascii
import hashlib
import json
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime

import yaml  # type: ignore[import-untyped]

_DNS_LABEL_RE = re.compile(r"^[a-z0-9](?:[-a-z0-9]{0,61}[a-z0-9])?$")
_MAX_KUBECONFIG_BYTES = 1 << 20
_MIN_REMAINING_SECONDS = 2 * 60 * 60
_MAX_LIFETIME_SECONDS = 24 * 60 * 60


@dataclass(frozen=True, slots=True)
class TokenRequestEvidence:
    """Non-secret identity and freshness evidence for one Kubernetes token."""

    subject: str
    audiences: tuple[str, ...]
    issued_at: int
    expires_at: int
    metadata_digest: str

    @property
    def remaining_seconds(self) -> int:
        return self.expires_at - self.issued_at


@dataclass(frozen=True, slots=True)
class RenderedKubeconfig:
    """Private kubeconfig plus non-secret TokenRequest evidence."""

    payload: bytes
    evidence: TokenRequestEvidence

    def __repr__(self) -> str:
        return f"RenderedKubeconfig(evidence={self.evidence!r}, payload=<redacted>)"


def validate_token_request(
    token: str,
    *,
    namespace: str,
    service_account: str,
    now: datetime,
    minimum_remaining_seconds: int = _MIN_REMAINING_SECONDS,
) -> TokenRequestEvidence:
    """Validate bounded JWT metadata without treating it as signature authority."""
    if (
        not token
        or any(character.isspace() for character in token)
        or not _valid_name(namespace)
        or not _valid_name(service_account)
        or now.tzinfo is None
        or not 60 <= minimum_remaining_seconds <= _MAX_LIFETIME_SECONDS
    ):
        raise ValueError("preflight TokenRequest input is invalid")
    parts = token.split(".")
    if len(parts) != 3 or any(not part for part in parts):
        raise ValueError("preflight TokenRequest is not a compact JWT")
    claims = _jwt_object(parts[1])
    expected_subject = f"system:serviceaccount:{namespace}:{service_account}"
    subject = claims.get("sub")
    issued_at = claims.get("iat")
    expires_at = claims.get("exp")
    raw_audience = claims.get("aud")
    audiences = (
        (raw_audience,)
        if isinstance(raw_audience, str)
        else tuple(raw_audience)
        if isinstance(raw_audience, list)
        else ()
    )
    now_seconds = int(now.astimezone(UTC).timestamp())
    if (
        subject != expected_subject
        or type(issued_at) is not int
        or type(expires_at) is not int
        or issued_at > now_seconds + 60
        or expires_at - issued_at > _MAX_LIFETIME_SECONDS
        or expires_at - now_seconds < minimum_remaining_seconds
        or not audiences
        or len(audiences) > 4
        or any(
            not isinstance(audience, str)
            or not audience
            or len(audience) > 512
            or any(character.isspace() for character in audience)
            for audience in audiences
        )
        or len(set(audiences)) != len(audiences)
    ):
        raise ValueError("preflight TokenRequest authority or freshness is invalid")
    metadata = {
        "audiences": sorted(audiences),
        "expires_at": expires_at,
        "issued_at": issued_at,
        "subject": subject,
    }
    digest = hashlib.sha256(
        json.dumps(metadata, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    return TokenRequestEvidence(
        subject=subject,
        audiences=tuple(sorted(audiences)),
        issued_at=issued_at,
        expires_at=expires_at,
        metadata_digest=digest,
    )


def render_token_request_kubeconfig(
    source_payload: bytes,
    token: str,
    *,
    namespace: str,
    service_account: str,
    now: datetime,
    minimum_remaining_seconds: int = _MIN_REMAINING_SECONDS,
) -> RenderedKubeconfig:
    """Render one minimal kubeconfig whose only user is the bounded token."""
    if not source_payload or len(source_payload) > _MAX_KUBECONFIG_BYTES:
        raise ValueError("source kubeconfig is unavailable")
    try:
        source = yaml.safe_load(source_payload)
    except (UnicodeDecodeError, yaml.YAMLError) as exc:
        raise ValueError("source kubeconfig is invalid") from exc
    if not isinstance(source, dict):
        raise ValueError("source kubeconfig is invalid")
    cluster = _single_named_entry(source.get("clusters"), "cluster")
    context = _single_named_entry(source.get("contexts"), "context")
    cluster_body = cluster["cluster"]
    context_body = context["context"]
    if not isinstance(cluster_body, dict) or not isinstance(context_body, dict):
        raise ValueError("source kubeconfig is invalid")
    if context_body.get("cluster") != cluster["name"]:
        raise ValueError("source kubeconfig context is invalid")
    server = cluster_body.get("server")
    certificate = cluster_body.get("certificate-authority-data")
    if (
        not isinstance(server, str)
        or not server.startswith("https://")
        or not isinstance(certificate, str)
        or not certificate
    ):
        raise ValueError("source kubeconfig cluster authority is invalid")
    try:
        decoded_certificate = base64.b64decode(certificate, validate=True)
    except (ValueError, binascii.Error) as exc:
        raise ValueError("source kubeconfig CA authority is invalid") from exc
    if not decoded_certificate.startswith(b"-----BEGIN CERTIFICATE-----"):
        raise ValueError("source kubeconfig CA authority is invalid")
    evidence = validate_token_request(
        token,
        namespace=namespace,
        service_account=service_account,
        now=now,
        minimum_remaining_seconds=minimum_remaining_seconds,
    )
    identity = f"{namespace}-{service_account}"
    payload = {
        "apiVersion": "v1",
        "clusters": [{"cluster": dict(cluster_body), "name": cluster["name"]}],
        "contexts": [
            {
                "context": {
                    "cluster": cluster["name"],
                    "namespace": namespace,
                    "user": identity,
                },
                "name": identity,
            }
        ],
        "current-context": identity,
        "kind": "Config",
        "preferences": {},
        "users": [{"name": identity, "user": {"token": token}}],
    }
    rendered = yaml.safe_dump(payload, sort_keys=True).encode()
    if len(rendered) > _MAX_KUBECONFIG_BYTES:
        raise ValueError("rendered preflight kubeconfig is too large")
    return RenderedKubeconfig(payload=rendered, evidence=evidence)


def validate_token_request_kubeconfig(
    payload: bytes,
    *,
    namespace: str,
    service_account: str,
    now: datetime,
    minimum_remaining_seconds: int = _MIN_REMAINING_SECONDS,
) -> TokenRequestEvidence:
    """Revalidate the exact minimal installed kubeconfig without inherited auth."""
    if not payload or len(payload) > _MAX_KUBECONFIG_BYTES:
        raise ValueError("installed preflight kubeconfig is unavailable")
    try:
        document = yaml.safe_load(payload)
    except (UnicodeDecodeError, yaml.YAMLError) as exc:
        raise ValueError("installed preflight kubeconfig is invalid") from exc
    expected_identity = f"{namespace}-{service_account}"
    if (
        not isinstance(document, dict)
        or set(document)
        != {
            "apiVersion",
            "clusters",
            "contexts",
            "current-context",
            "kind",
            "preferences",
            "users",
        }
        or document.get("apiVersion") != "v1"
        or document.get("kind") != "Config"
        or document.get("preferences") != {}
        or document.get("current-context") != expected_identity
    ):
        raise ValueError("installed preflight kubeconfig is invalid")
    cluster = _single_named_entry(document.get("clusters"), "cluster")
    context = _single_named_entry(document.get("contexts"), "context")
    user = _single_named_entry(document.get("users"), "user")
    cluster_body = cluster["cluster"]
    context_body = context["context"]
    user_body = user["user"]
    if (
        not isinstance(cluster_body, dict)
        or set(cluster_body) not in (
            {"certificate-authority-data", "server"},
            {"certificate-authority-data", "server", "tls-server-name"},
        )
        or not isinstance(context_body, dict)
        or context_body
        != {
            "cluster": cluster["name"],
            "namespace": namespace,
            "user": expected_identity,
        }
        or user.get("name") != expected_identity
        or not isinstance(user_body, dict)
        or set(user_body) != {"token"}
    ):
        raise ValueError("installed preflight kubeconfig authority is invalid")
    server = cluster_body.get("server")
    certificate = cluster_body.get("certificate-authority-data")
    token = user_body.get("token")
    if (
        not isinstance(server, str)
        or not server.startswith("https://")
        or not isinstance(certificate, str)
        or not isinstance(token, str)
    ):
        raise ValueError("installed preflight kubeconfig authority is invalid")
    try:
        decoded_certificate = base64.b64decode(certificate, validate=True)
    except (ValueError, binascii.Error) as exc:
        raise ValueError("installed preflight kubeconfig CA authority is invalid") from exc
    if not decoded_certificate.startswith(b"-----BEGIN CERTIFICATE-----"):
        raise ValueError("installed preflight kubeconfig CA authority is invalid")
    return validate_token_request(
        token,
        namespace=namespace,
        service_account=service_account,
        now=now,
        minimum_remaining_seconds=minimum_remaining_seconds,
    )


def _valid_name(value: str) -> bool:
    return bool(value and _DNS_LABEL_RE.fullmatch(value))


def _jwt_object(segment: str) -> Mapping[str, object]:
    try:
        padded = segment + "=" * (-len(segment) % 4)
        raw = base64.urlsafe_b64decode(padded.encode("ascii"))
        value = json.loads(raw)
    except (UnicodeEncodeError, UnicodeDecodeError, ValueError, json.JSONDecodeError) as exc:
        raise ValueError("preflight TokenRequest claims are invalid") from exc
    if not isinstance(value, dict):
        raise ValueError("preflight TokenRequest claims are invalid")
    return value


def _single_named_entry(value: object, payload_key: str) -> dict[str, object]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)) or len(value) != 1:
        raise ValueError("source kubeconfig must be minified")
    entry = value[0]
    if (
        not isinstance(entry, dict)
        or not isinstance(entry.get("name"), str)
        or not isinstance(entry.get(payload_key), dict)
    ):
        raise ValueError("source kubeconfig is invalid")
    return entry


__all__ = [
    "RenderedKubeconfig",
    "TokenRequestEvidence",
    "render_token_request_kubeconfig",
    "validate_token_request",
    "validate_token_request_kubeconfig",
]
