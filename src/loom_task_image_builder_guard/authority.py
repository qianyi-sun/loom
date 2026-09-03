"""Fixed mutually authenticated client for the task-image authority."""

from __future__ import annotations

import hashlib
import http.client
import json
import os
import re
import ssl
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import cast
from urllib.parse import urlsplit
from uuid import UUID

from loom_task_image_builder_guard.errors import GuardError
from loom_task_image_builder_guard.models import AuthorityConfig
from loom_task_image_builder_guard.protocol import create_sealed_memfd
from loom_task_image_builder_guard.safeio import read_stable_file

_MAX_CREDENTIAL_BYTES = 64 * 1024
_MAX_BEARER_BYTES = 4096
_MAX_REQUEST_BYTES = 64 * 1024
_MAX_FIELDS = 32
_DIGEST = re.compile(r"^[0-9a-f]{64}$")
_IDENTIFIER = re.compile(r"^[a-z0-9][a-z0-9_.-]{0,127}$")
_TOKEN = {
    "bootstrap": re.compile(r"^loom_tibp_[A-Za-z0-9_-]{64,128}$"),
    "session": re.compile(r"^loom_tibs_[A-Za-z0-9_-]{64,128}$"),
}


def _pairs(pairs: list[tuple[str, object]]) -> dict[str, object]:
    if len(pairs) > _MAX_FIELDS:
        raise ValueError("too many fields")
    value: dict[str, object] = {}
    for key, item in pairs:
        if key in value:
            raise ValueError("duplicate key")
        value[key] = item
    return value


def _canonical(value: object) -> bytes:
    """Encode the integer/string/null-only authority subset canonically."""

    def check(item: object) -> None:
        if item is None or isinstance(item, str) or type(item) is int:
            return
        if isinstance(item, list):
            for child in item:
                check(child)
            return
        if isinstance(item, dict) and all(isinstance(key, str) for key in item):
            for child in item.values():
                check(child)
            return
        raise GuardError("authority_request_invalid")

    check(value)
    try:
        payload = json.dumps(
            value,
            ensure_ascii=True,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("ascii")
    except (TypeError, ValueError, UnicodeEncodeError):
        raise GuardError("authority_request_invalid") from None
    if not payload or len(payload) > _MAX_REQUEST_BYTES:
        raise GuardError("authority_request_invalid")
    return payload


def _document(payload: bytes) -> dict[str, object]:
    try:
        value = json.loads(payload, object_pairs_hook=_pairs)
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError):
        raise GuardError("authority_response_invalid") from None
    if not isinstance(value, dict):
        raise GuardError("authority_response_invalid")
    return cast(dict[str, object], value)


def _exact(value: object, keys: frozenset[str], *, code: str) -> dict[str, object]:
    if not isinstance(value, dict) or set(value) != keys:
        raise GuardError(code)
    return cast(dict[str, object], value)


def _uuid(value: object, *, code: str) -> UUID:
    try:
        if not isinstance(value, str):
            raise ValueError
        parsed = UUID(value)
    except (ValueError, AttributeError):
        raise GuardError(code) from None
    if parsed.int == 0 or str(parsed) != value:
        raise GuardError(code)
    return parsed


def _digest(value: object, *, code: str) -> str:
    if not isinstance(value, str) or _DIGEST.fullmatch(value) is None or value == "0" * 64:
        raise GuardError(code)
    return value


def _integer(value: object, *, code: str) -> int:
    if type(value) is not int or not 1 <= value <= (1 << 63) - 1:
        raise GuardError(code)
    return value


def _time(value: object, *, code: str) -> datetime:
    if not isinstance(value, str) or not value.endswith("Z") or len(value) > 40:
        raise GuardError(code)
    try:
        parsed = datetime.fromisoformat(f"{value[:-1]}+00:00")
    except ValueError:
        raise GuardError(code) from None
    if parsed.tzinfo is None or parsed.utcoffset() != timedelta(0):
        raise GuardError(code)
    return parsed.astimezone(UTC)


def _active_interval(
    issued: object,
    expires: object,
    *,
    now: datetime,
    maximum: timedelta,
    code: str,
) -> tuple[datetime, datetime]:
    issued_at = _time(issued, code=code)
    expires_at = _time(expires, code=code)
    if (
        expires_at <= issued_at
        or expires_at - issued_at > maximum
        or now < issued_at
        or now >= expires_at
    ):
        raise GuardError(code)
    return issued_at, expires_at


def _token(value: object, *, kind: str, code: str) -> str:
    if not isinstance(value, str) or _TOKEN[kind].fullmatch(value) is None:
        raise GuardError(code)
    return value


@dataclass(frozen=True, slots=True)
class ProjectionChallenge:
    request_id: UUID
    grant_id: UUID
    request_sha256: str
    challenge_nonce: UUID
    containment_policy_sha256: str
    resource_profile_sha256: str
    issued_at: datetime
    expires_at: datetime


@dataclass(frozen=True, slots=True)
class ProjectionReceipt:
    grant_id: UUID
    proof_id: UUID
    proof_sha256: str
    bootstrap_token: str = field(repr=False)
    issued_at: datetime
    expires_at: datetime
    wire_payload: bytes = field(repr=False)

    @property
    def public_binding_sha256(self) -> str:
        value = _document(self.wire_payload)
        value.pop("bootstrap_token")
        value["bootstrap_token_sha256"] = hashlib.sha256(
            self.bootstrap_token.encode("ascii")
        ).hexdigest()
        return hashlib.sha256(_canonical(value)).hexdigest()


@dataclass(frozen=True, slots=True)
class BuildSession:
    grant_id: UUID
    session_id: UUID
    purpose: str
    shadow_campaign_id: UUID | None
    pool_id: str
    cpu_arch: str
    session_token: str = field(repr=False)
    attestation_generation: int
    attestation_sha256: str
    issued_at: datetime
    expires_at: datetime
    wire_payload: bytes = field(repr=False)

    @property
    def public_binding_sha256(self) -> str:
        value = _document(self.wire_payload)
        value.pop("session_token")
        value["session_token_sha256"] = hashlib.sha256(
            self.session_token.encode("ascii")
        ).hexdigest()
        return hashlib.sha256(_canonical(value)).hexdigest()


@dataclass(frozen=True, slots=True)
class AcceptedAttestation:
    attestation_id: UUID
    grant_id: UUID
    generation: int
    issued_at: datetime
    expires_at: datetime
    sha256: str


class AuthorityClient:
    """TLS-1.3-only node client whose routes cannot be request-selected."""

    def __init__(
        self,
        config: AuthorityConfig,
        *,
        trusted_uid: int = 0,
        trusted_gid: int = 0,
        now_factory: Callable[[], datetime] | None = None,
    ) -> None:
        self._config = config
        self._now = now_factory or (lambda: datetime.now(UTC))
        parsed = urlsplit(config.base_url)
        try:
            port = parsed.port or 443
        except ValueError:
            raise GuardError("authority_origin_invalid") from None
        if (
            parsed.scheme != "https"
            or parsed.hostname is None
            or parsed.username is not None
            or parsed.password is not None
            or parsed.path not in {"", "/"}
            or parsed.query
            or parsed.fragment
            or not 1 <= port <= 65535
        ):
            raise GuardError("authority_origin_invalid")
        self._hostname = parsed.hostname
        self._port = port
        try:
            ca = read_stable_file(
                config.ca_path,
                uid=trusted_uid,
                gid=trusted_gid,
                mode=0o444,
                maximum=_MAX_CREDENTIAL_BYTES,
            )
            certificate = read_stable_file(
                config.cert_path,
                uid=trusted_uid,
                gid=trusted_gid,
                mode=0o444,
                maximum=_MAX_CREDENTIAL_BYTES,
            )
            private_key = read_stable_file(
                config.key_path,
                uid=trusted_uid,
                gid=trusted_gid,
                mode=0o600,
                maximum=_MAX_CREDENTIAL_BYTES,
            )
            bearer_bytes = read_stable_file(
                config.bearer_path,
                uid=trusted_uid,
                gid=trusted_gid,
                mode=0o600,
                maximum=_MAX_BEARER_BYTES,
            )
            bearer = bearer_bytes.decode("ascii")
            if not bearer or len(bearer) > _MAX_BEARER_BYTES or any(
                character.isspace() or ord(character) < 0x21 or ord(character) > 0x7E
                for character in bearer
            ):
                raise ValueError("invalid bearer")
            context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
            context.minimum_version = ssl.TLSVersion.TLSv1_3
            context.maximum_version = ssl.TLSVersion.TLSv1_3
            context.check_hostname = True
            context.verify_mode = ssl.CERT_REQUIRED
            context.load_verify_locations(cadata=ca.decode("ascii"))
            certificate_fd = create_sealed_memfd(
                "guard-client-certificate", certificate, maximum=_MAX_CREDENTIAL_BYTES
            )
            key_fd = create_sealed_memfd(
                "guard-client-private-key", private_key, maximum=_MAX_CREDENTIAL_BYTES
            )
            try:
                context.load_cert_chain(
                    f"/proc/self/fd/{certificate_fd}",
                    f"/proc/self/fd/{key_fd}",
                )
            finally:
                os.close(key_fd)
                os.close(certificate_fd)
        except Exception:
            raise GuardError("authority_credentials_invalid") from None
        self._bearer = bearer
        self._context = context

    @staticmethod
    def request_sha256(value: object) -> str:
        return hashlib.sha256(_canonical(value)).hexdigest()

    def _request(self, route: str, body: object, *, expected_status: int) -> bytes:
        payload = _canonical(body)
        connection = http.client.HTTPSConnection(
            self._hostname,
            self._port,
            timeout=self._config.timeout_seconds,
            context=self._context,
        )
        try:
            connection.request(
                "PUT",
                route,
                body=payload,
                headers={
                    "Accept": "application/json",
                    "Authorization": f"Bearer {self._bearer}",
                    "Connection": "close",
                    "Content-Type": "application/json",
                },
            )
            response = connection.getresponse()
            if response.status != expected_status:
                raise GuardError("authority_http_failed")
            transfer = response.headers.get_all("Transfer-Encoding", failobj=[])
            lengths = response.headers.get_all("Content-Length", failobj=[])
            if transfer or len(lengths) > 1:
                raise GuardError("authority_response_invalid")
            if expected_status == 204:
                if lengths and (not lengths[0].isdigit() or int(lengths[0]) != 0):
                    raise GuardError("authority_response_invalid")
                if response.read(1):
                    raise GuardError("authority_response_invalid")
                return b""
            content_types = response.headers.get_all("Content-Type", failobj=[])
            if content_types != ["application/json"] or len(lengths) != 1:
                raise GuardError("authority_response_invalid")
            if not lengths[0].isdigit():
                raise GuardError("authority_response_invalid")
            declared = int(lengths[0])
            if declared > self._config.max_response_bytes:
                raise GuardError("authority_response_too_large")
            result = response.read(self._config.max_response_bytes + 1)
            if len(result) > self._config.max_response_bytes:
                raise GuardError("authority_response_too_large")
            if len(result) != declared or not result:
                raise GuardError("authority_response_invalid")
            return result
        except GuardError:
            raise
        except (OSError, ssl.SSLError, http.client.HTTPException):
            raise GuardError("authority_transport_failed") from None
        finally:
            connection.close()

    def _now_utc(self) -> datetime:
        value = self._now()
        if value.tzinfo is None or value.utcoffset() is None:
            raise GuardError("authority_clock_invalid")
        return value.astimezone(UTC)

    @staticmethod
    def _grant(value: UUID) -> UUID:
        if not isinstance(value, UUID) or value.int == 0:
            raise GuardError("authority_grant_invalid")
        return value

    def challenge(
        self,
        grant_id: UUID,
        request: dict[str, object],
        *,
        containment_policy_sha256: str,
        resource_profile_sha256: str,
    ) -> ProjectionChallenge:
        grant = self._grant(grant_id)
        request_id = _uuid(request.get("request_id"), code="authority_challenge_invalid")
        if _uuid(request.get("grant_id"), code="authority_challenge_invalid") != grant:
            raise GuardError("authority_challenge_invalid")
        request_sha = self.request_sha256(request)
        raw = self._request(f"/v1/projections/{grant}/challenge", request, expected_status=200)
        value = _exact(
            _document(raw),
            frozenset(
                {
                    "schema_version",
                    "request_id",
                    "grant_id",
                    "request_sha256",
                    "challenge_nonce",
                    "containment_policy_sha256",
                    "resource_profile_sha256",
                    "issued_at",
                    "expires_at",
                }
            ),
            code="authority_challenge_invalid",
        )
        issued, expires = _active_interval(
            value["issued_at"],
            value["expires_at"],
            now=self._now_utc(),
            maximum=timedelta(seconds=60),
            code="authority_challenge_invalid",
        )
        challenge = ProjectionChallenge(
            request_id=_uuid(value["request_id"], code="authority_challenge_invalid"),
            grant_id=_uuid(value["grant_id"], code="authority_challenge_invalid"),
            request_sha256=_digest(
                value["request_sha256"], code="authority_challenge_invalid"
            ),
            challenge_nonce=_uuid(
                value["challenge_nonce"], code="authority_challenge_invalid"
            ),
            containment_policy_sha256=_digest(
                value["containment_policy_sha256"], code="authority_challenge_invalid"
            ),
            resource_profile_sha256=_digest(
                value["resource_profile_sha256"], code="authority_challenge_invalid"
            ),
            issued_at=issued,
            expires_at=expires,
        )
        if (
            value["schema_version"] != 1
            or challenge.request_id != request_id
            or challenge.grant_id != grant
            or challenge.request_sha256 != request_sha
            or challenge.containment_policy_sha256 != containment_policy_sha256
            or challenge.resource_profile_sha256 != resource_profile_sha256
        ):
            raise GuardError("authority_challenge_invalid")
        return challenge

    def attach(self, grant_id: UUID, proof: dict[str, object]) -> ProjectionReceipt:
        grant = self._grant(grant_id)
        proof_id = _uuid(proof.get("proof_id"), code="authority_receipt_invalid")
        if _uuid(proof.get("grant_id"), code="authority_receipt_invalid") != grant:
            raise GuardError("authority_receipt_invalid")
        proof_sha = self.request_sha256(proof)
        raw = self._request(f"/v1/projections/{grant}/attachment", proof, expected_status=200)
        value = _exact(
            _document(raw),
            frozenset(
                {
                    "schema_version",
                    "grant_id",
                    "proof_id",
                    "proof_sha256",
                    "bootstrap_token",
                    "issued_at",
                    "expires_at",
                }
            ),
            code="authority_receipt_invalid",
        )
        issued, expires = _active_interval(
            value["issued_at"],
            value["expires_at"],
            now=self._now_utc(),
            maximum=timedelta(seconds=60),
            code="authority_receipt_invalid",
        )
        receipt = ProjectionReceipt(
            grant_id=_uuid(value["grant_id"], code="authority_receipt_invalid"),
            proof_id=_uuid(value["proof_id"], code="authority_receipt_invalid"),
            proof_sha256=_digest(value["proof_sha256"], code="authority_receipt_invalid"),
            bootstrap_token=_token(
                value["bootstrap_token"], kind="bootstrap", code="authority_receipt_invalid"
            ),
            issued_at=issued,
            expires_at=expires,
            wire_payload=raw,
        )
        if (
            value["schema_version"] != 1
            or receipt.grant_id != grant
            or receipt.proof_id != proof_id
            or receipt.proof_sha256 != proof_sha
        ):
            raise GuardError("authority_receipt_invalid")
        return receipt

    def exchange(self, grant_id: UUID, request: dict[str, object]) -> BuildSession:
        grant = self._grant(grant_id)
        if _uuid(request.get("grant_id"), code="authority_session_invalid") != grant:
            raise GuardError("authority_session_invalid")
        raw = self._request(f"/v1/projections/{grant}/exchange", request, expected_status=200)
        value = _exact(
            _document(raw),
            frozenset(
                {
                    "schema_version",
                    "grant_id",
                    "session_id",
                    "purpose",
                    "shadow_campaign_id",
                    "pool_id",
                    "cpu_arch",
                    "session_token",
                    "attestation_generation",
                    "attestation_sha256",
                    "issued_at",
                    "expires_at",
                }
            ),
            code="authority_session_invalid",
        )
        issued, expires = _active_interval(
            value["issued_at"],
            value["expires_at"],
            now=self._now_utc(),
            maximum=timedelta(minutes=15),
            code="authority_session_invalid",
        )
        purpose = value["purpose"]
        shadow = value["shadow_campaign_id"]
        if purpose not in {"production", "shadow"}:
            raise GuardError("authority_session_invalid")
        shadow_id = None if shadow is None else _uuid(shadow, code="authority_session_invalid")
        pool = value["pool_id"]
        architecture = value["cpu_arch"]
        if (
            not isinstance(pool, str)
            or _IDENTIFIER.fullmatch(pool) is None
            or architecture not in {"x86_64", "arm64"}
            or (purpose == "production") != (shadow_id is None)
        ):
            raise GuardError("authority_session_invalid")
        session = BuildSession(
            grant_id=_uuid(value["grant_id"], code="authority_session_invalid"),
            session_id=_uuid(value["session_id"], code="authority_session_invalid"),
            purpose=purpose,
            shadow_campaign_id=shadow_id,
            pool_id=pool,
            cpu_arch=architecture,
            session_token=_token(
                value["session_token"], kind="session", code="authority_session_invalid"
            ),
            attestation_generation=_integer(
                value["attestation_generation"], code="authority_session_invalid"
            ),
            attestation_sha256=_digest(
                value["attestation_sha256"], code="authority_session_invalid"
            ),
            issued_at=issued,
            expires_at=expires,
            wire_payload=raw,
        )
        if value["schema_version"] != 1 or session.grant_id != grant:
            raise GuardError("authority_session_invalid")
        return session

    def attest(
        self,
        grant_id: UUID,
        generation: int,
        attestation: dict[str, object],
    ) -> AcceptedAttestation:
        grant = self._grant(grant_id)
        if (
            type(generation) is not int
            or generation <= 0
            or _uuid(attestation.get("grant_id"), code="authority_attestation_invalid")
            != grant
            or attestation.get("generation") != generation
        ):
            raise GuardError("authority_attestation_invalid")
        raw = self._request(
            f"/v1/projections/{grant}/attestations/{generation}",
            attestation,
            expected_status=200,
        )
        value = _document(raw)
        if value != attestation:
            raise GuardError("authority_attestation_invalid")
        issued, expires = _active_interval(
            value.get("issued_at"),
            value.get("expires_at"),
            now=self._now_utc(),
            maximum=timedelta(seconds=60),
            code="authority_attestation_invalid",
        )
        return AcceptedAttestation(
            attestation_id=_uuid(
                value.get("attestation_id"), code="authority_attestation_invalid"
            ),
            grant_id=grant,
            generation=generation,
            issued_at=issued,
            expires_at=expires,
            sha256=self.request_sha256(value),
        )

    def revoke(self, grant_id: UUID, request: dict[str, object]) -> None:
        grant = self._grant(grant_id)
        if _uuid(request.get("grant_id"), code="authority_revocation_invalid") != grant:
            raise GuardError("authority_revocation_invalid")
        self._request(
            f"/v1/projections/{grant}/revocation",
            request,
            expected_status=204,
        )


__all__ = [
    "AcceptedAttestation",
    "AuthorityClient",
    "BuildSession",
    "ProjectionChallenge",
    "ProjectionReceipt",
]
