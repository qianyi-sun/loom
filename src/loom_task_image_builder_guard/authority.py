"""Fixed mutually authenticated client for the task-image authority."""

from __future__ import annotations

import errno
import hashlib
import http.client
import ipaddress
import json
import os
import re
import socket
import ssl
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Literal, cast
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
_MAX_CONTENT_LENGTH_DIGITS = 20
_DIGEST = re.compile(r"^[0-9a-f]{64}$")
_IDENTIFIER = re.compile(r"^[a-z0-9][a-z0-9_.-]{0,127}$")
_BUILDER_ID = re.compile(r"^rootless:[0-9a-f]{32}$")
_COMPONENT = re.compile(r"^(?:task|sidecar:[A-Za-z0-9][A-Za-z0-9_.-]{0,127})$")
_MANIFEST_DIGEST = re.compile(r"^sha256:([0-9a-f]{64})$")
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
    except (UnicodeDecodeError, json.JSONDecodeError, RecursionError, ValueError):
        raise GuardError("authority_response_invalid") from None
    if not isinstance(value, dict):
        raise GuardError("authority_response_invalid")
    return cast(dict[str, object], value)


def _content_length(value: str, *, maximum: int | None) -> int:
    if (
        not value
        or len(value) > _MAX_CONTENT_LENGTH_DIGITS
        or any(character < "0" or character > "9" for character in value)
    ):
        raise GuardError("authority_response_invalid")
    declared = int(value)
    if maximum is not None and declared > maximum:
        raise GuardError("authority_response_too_large")
    return declared


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


def _credential_generation(value: object, *, code: str) -> int:
    generation = _integer(value, code=code)
    if generation > 512:
        raise GuardError(code)
    return generation


def _component(value: object, *, code: str) -> str:
    if not isinstance(value, str) or _COMPONENT.fullmatch(value) is None:
        raise GuardError(code)
    return value


def _manifest_digest(value: object, *, code: str) -> str:
    if not isinstance(value, str):
        raise GuardError(code)
    matched = _MANIFEST_DIGEST.fullmatch(value)
    if matched is None or matched.group(1) == "0" * 64:
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
    generation: int = 1

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


@dataclass(slots=True)
class SealedAuthorityPayload:
    """Own an opaque authority response without exposing it through repr."""

    descriptor: int = field(repr=False)
    sha256: str
    _closed: bool = field(default=False, init=False, repr=False)

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        os.close(self.descriptor)


@dataclass(frozen=True, slots=True)
class LeaseAcknowledgement:
    operation: str
    operation_id: UUID
    materialization_id: UUID
    attempt_id: UUID
    lease_epoch: int
    state: str
    deterministic_failure_count: int
    lease_expires_at: datetime | None


@dataclass(frozen=True, slots=True)
class PublicationCandidateAcknowledgement:
    """Strict nonsecret binding returned for one inert publication candidate."""

    candidate_id: UUID
    operation_id: UUID
    credential_id: UUID
    credential_generation: int
    grant_id: UUID
    session_id: UUID
    session_generation: int
    materialization_id: UUID
    attempt_id: UUID
    attempt_number: int
    lease_epoch: int
    builder_id: str
    component: str
    manifest_digest: str
    manifest_size: int
    oci_file_sha256: str
    oci_file_size: int
    platform: Literal["linux/amd64", "linux/arm64"]
    recorded_at: datetime
    response_sha256: str


class _PinnedHTTPSConnection(http.client.HTTPSConnection):
    """Connect to one configured IP while retaining the URL host for TLS."""

    def __init__(
        self,
        hostname: str,
        port: int,
        *,
        connect_ip: str,
        context: ssl.SSLContext,
        deadline: float,
        monotonic: Callable[[], float],
        expired: threading.Event,
    ) -> None:
        super().__init__(hostname, port, timeout=None, context=context)
        self._connect_ip = connect_ip
        self._deadline = deadline
        self._monotonic = monotonic
        self._expired = expired
        self._tls_context = context

    def _remaining(self) -> float:
        remaining = self._deadline - self._monotonic()
        if self._expired.is_set() or remaining <= 0:
            raise TimeoutError("authority deadline exceeded")
        return remaining

    def connect(self) -> None:
        raw: socket.socket | None = None
        try:
            address = ipaddress.ip_address(self._connect_ip)
            family = socket.AF_INET if address.version == 4 else socket.AF_INET6
            raw = socket.socket(
                family,
                socket.SOCK_STREAM | getattr(socket, "SOCK_CLOEXEC", 0),
            )
            raw.settimeout(self._remaining())
            destination: tuple[object, ...] = (
                (self._connect_ip, self.port)
                if family == socket.AF_INET
                else (self._connect_ip, self.port, 0, 0)
            )
            raw.connect(destination)
            self.sock = raw
            try:
                raw.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            except OSError as exc:
                if exc.errno != errno.ENOPROTOOPT:
                    raise
            raw.settimeout(self._remaining())
            wrapped = self._tls_context.wrap_socket(raw, server_hostname=self.host)
            self.sock = wrapped
            raw = None
            wrapped.settimeout(self._remaining())
        except Exception:
            self.abort()
            if raw is not None:
                raw.close()
            raise

    def assert_before_deadline(self) -> None:
        remaining = self._remaining()
        if self.sock is not None:
            self.sock.settimeout(remaining)

    def abort(self) -> None:
        active = self.sock
        self.sock = None
        if active is None:
            return
        try:
            active.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        active.close()


class AuthorityClient:
    """TLS-1.3-only node client whose routes cannot be request-selected."""

    def __init__(
        self,
        config: AuthorityConfig,
        *,
        trusted_uid: int = 0,
        trusted_gid: int = 0,
        now_factory: Callable[[], datetime] | None = None,
        monotonic: Callable[[], float] = time.monotonic,
        progress: Callable[[], None] = lambda: None,
    ) -> None:
        self._config = config
        self._now = now_factory or (lambda: datetime.now(UTC))
        self._monotonic = monotonic
        self._progress = progress
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
            try:
                key_fd = create_sealed_memfd(
                    "guard-client-private-key",
                    private_key,
                    maximum=_MAX_CREDENTIAL_BYTES,
                )
                try:
                    context.load_cert_chain(
                        f"/proc/self/fd/{certificate_fd}",
                        f"/proc/self/fd/{key_fd}",
                    )
                finally:
                    os.close(key_fd)
            finally:
                os.close(certificate_fd)
        except Exception:
            raise GuardError("authority_credentials_invalid") from None
        self._bearer = bearer
        self._context = context

    @staticmethod
    def request_sha256(value: object) -> str:
        return hashlib.sha256(_canonical(value)).hexdigest()

    def _request(
        self,
        route: str,
        body: object,
        *,
        expected_status: int | tuple[int, ...],
        method: Literal["POST", "PUT"] = "PUT",
        maximum_bytes: int | None = None,
    ) -> bytes:
        payload = _canonical(body)
        response_maximum = (
            self._config.max_response_bytes
            if maximum_bytes is None
            else maximum_bytes
        )
        if (
            type(response_maximum) is not int
            or response_maximum <= 0
            or response_maximum > 8 * 1024 * 1024
        ):
            raise GuardError("authority_response_too_large")
        statuses: tuple[int, ...] = (
            (expected_status,)
            if type(expected_status) is int
            else cast(tuple[int, ...], expected_status)
        )
        if not statuses or any(type(item) is not int for item in statuses):
            raise GuardError("authority_response_invalid")
        deadline = self._monotonic() + self._config.timeout_seconds
        expired = threading.Event()
        connection = _PinnedHTTPSConnection(
            self._hostname,
            self._port,
            connect_ip=self._config.connect_ip,
            context=self._context,
            deadline=deadline,
            monotonic=self._monotonic,
            expired=expired,
        )
        def expire() -> None:
            expired.set()
            connection.abort()

        timer = threading.Timer(self._config.timeout_seconds, expire)
        timer.daemon = True
        timer.start()
        try:
            self._progress()
            connection.request(
                method,
                route,
                body=payload,
                headers={
                    "Accept": "application/json",
                    "Authorization": f"Bearer {self._bearer}",
                    "Connection": "close",
                    "Content-Type": "application/json",
                },
            )
            self._progress()
            connection.assert_before_deadline()
            response = connection.getresponse()
            self._progress()
            connection.assert_before_deadline()
            if response.status not in statuses:
                raise GuardError("authority_http_failed")
            transfer = response.headers.get_all("Transfer-Encoding", failobj=[])
            lengths = response.headers.get_all("Content-Length", failobj=[])
            if transfer or len(lengths) > 1:
                raise GuardError("authority_response_invalid")
            if response.status == 204:
                if lengths and _content_length(lengths[0], maximum=None) != 0:
                    raise GuardError("authority_response_invalid")
                if response.read(1):
                    raise GuardError("authority_response_invalid")
                self._progress()
                connection.assert_before_deadline()
                return b""
            content_types = response.headers.get_all("Content-Type", failobj=[])
            if content_types != ["application/json"] or len(lengths) != 1:
                raise GuardError("authority_response_invalid")
            declared = _content_length(
                lengths[0], maximum=response_maximum
            )
            result = response.read(response_maximum + 1)
            self._progress()
            connection.assert_before_deadline()
            if len(result) > response_maximum:
                raise GuardError("authority_response_too_large")
            if len(result) != declared or not result:
                raise GuardError("authority_response_invalid")
            return result
        except GuardError:
            raise
        except (OSError, ssl.SSLError, http.client.HTTPException):
            if expired.is_set() or self._monotonic() >= deadline:
                raise GuardError("authority_deadline_exceeded") from None
            raise GuardError("authority_transport_failed") from None
        finally:
            timer.cancel()
            connection.close()
            timer.join()

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

    def _session(
        self,
        raw: bytes,
        *,
        grant: UUID,
        expected_generation: int,
        expected_attestation_sha256: str | None = None,
    ) -> BuildSession:
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
                    "generation",
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
            generation=_integer(value["generation"], code="authority_session_invalid"),
        )
        if (
            value["schema_version"] != 2
            or session.grant_id != grant
            or session.generation != expected_generation
            or session.attestation_generation != expected_generation
            or (
                expected_attestation_sha256 is not None
                and session.attestation_sha256 != expected_attestation_sha256
            )
        ):
            raise GuardError("authority_session_invalid")
        return session

    def parse_session(self, payload: bytes) -> BuildSession:
        """Validate one current sealed-session wire document."""

        try:
            value = _document(payload)
            grant = _uuid(value.get("grant_id"), code="authority_session_invalid")
            generation = _integer(value.get("generation"), code="authority_session_invalid")
            return self._session(
                payload,
                grant=grant,
                expected_generation=generation,
            )
        except GuardError:
            raise
        except Exception:
            raise GuardError("authority_session_invalid") from None

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
        return self._session(raw, grant=grant, expected_generation=1)

    def renew(
        self,
        grant_id: UUID,
        generation: int,
        request: dict[str, object],
    ) -> BuildSession:
        grant = self._grant(grant_id)
        attestation = request.get("attestation")
        if (
            type(generation) is not int
            or generation <= 0
            or _uuid(request.get("grant_id"), code="authority_session_invalid") != grant
            or request.get("session_generation") != generation
            or not isinstance(attestation, dict)
            or attestation.get("generation") != generation + 1
        ):
            raise GuardError("authority_session_invalid")
        attestation_sha256 = self.request_sha256(attestation)
        raw = self._request(
            f"/v1/projections/{grant}/sessions/{generation}/renew",
            request,
            expected_status=200,
        )
        return self._session(
            raw,
            grant=grant,
            expected_generation=generation + 1,
            expected_attestation_sha256=attestation_sha256,
        )

    @staticmethod
    def _request_binding(
        grant: UUID,
        materialization_id: UUID,
        request: dict[str, object],
    ) -> tuple[UUID, UUID, int]:
        if (
            _uuid(request.get("grant_id"), code="authority_lease_invalid") != grant
            or _uuid(
                request.get("materialization_id"), code="authority_lease_invalid"
            )
            != materialization_id
        ):
            raise GuardError("authority_lease_invalid")
        return (
            _uuid(request.get("operation_id"), code="authority_lease_invalid"),
            _uuid(request.get("attempt_id"), code="authority_lease_invalid"),
            _integer(request.get("lease_epoch"), code="authority_lease_invalid"),
        )

    def claim(
        self,
        grant_id: UUID,
        request: dict[str, object],
    ) -> SealedAuthorityPayload | None:
        grant = self._grant(grant_id)
        _uuid(request.get("claim_id"), code="authority_lease_invalid")
        if _uuid(request.get("grant_id"), code="authority_lease_invalid") != grant:
            raise GuardError("authority_lease_invalid")
        raw = self._request(
            f"/v1/projections/{grant}/materializations/claim",
            request,
            expected_status=(200, 204),
            method="POST",
        )
        if not raw:
            return None
        return self._seal_response("guard-claim", raw)

    def _lease_operation(
        self,
        operation: Literal["start", "heartbeat", "release", "fail"],
        grant_id: UUID,
        materialization_id: UUID,
        request: dict[str, object],
    ) -> LeaseAcknowledgement:
        grant = self._grant(grant_id)
        operation_id, attempt_id, lease_epoch = self._request_binding(
            grant, materialization_id, request
        )
        raw = self._request(
            f"/v1/projections/{grant}/materializations/{materialization_id}/{operation}",
            request,
            expected_status=200,
        )
        value = _exact(
            _document(raw),
            frozenset(
                {
                    "schema_version",
                    "operation",
                    "operation_id",
                    "materialization_id",
                    "attempt_id",
                    "lease_epoch",
                    "state",
                    "deterministic_failure_count",
                    "lease_expires_at",
                }
            ),
            code="authority_lease_invalid",
        )
        expected_operation: str = operation
        if operation == "fail":
            expected_operation = (
                "containment_release"
                if request.get("failure_kind") == "containment"
                else "deterministic_fail"
            )
        expires = (
            None
            if value["lease_expires_at"] is None
            else _time(value["lease_expires_at"], code="authority_lease_invalid")
        )
        state = value["state"]
        count = value["deterministic_failure_count"]
        if (
            value["schema_version"] != "loom.task-image-materialization-operation.v1"
            or value["operation"] != expected_operation
            or _uuid(value["operation_id"], code="authority_lease_invalid") != operation_id
            or _uuid(value["materialization_id"], code="authority_lease_invalid")
            != materialization_id
            or _uuid(value["attempt_id"], code="authority_lease_invalid") != attempt_id
            or value["lease_epoch"] != lease_epoch
            or state not in {"claimed", "running", "queued", "failed"}
            or type(count) is not int
            or count < 0
            or ((state in {"claimed", "running"}) != (expires is not None))
        ):
            raise GuardError("authority_lease_invalid")
        return LeaseAcknowledgement(
            expected_operation,
            operation_id,
            materialization_id,
            attempt_id,
            lease_epoch,
            state,
            count,
            expires,
        )

    def start(
        self, grant_id: UUID, materialization_id: UUID, request: dict[str, object]
    ) -> LeaseAcknowledgement:
        return self._lease_operation("start", grant_id, materialization_id, request)

    def heartbeat(
        self, grant_id: UUID, materialization_id: UUID, request: dict[str, object]
    ) -> LeaseAcknowledgement:
        return self._lease_operation("heartbeat", grant_id, materialization_id, request)

    def release(
        self, grant_id: UUID, materialization_id: UUID, request: dict[str, object]
    ) -> LeaseAcknowledgement:
        return self._lease_operation("release", grant_id, materialization_id, request)

    def fail(
        self, grant_id: UUID, materialization_id: UUID, request: dict[str, object]
    ) -> LeaseAcknowledgement:
        return self._lease_operation("fail", grant_id, materialization_id, request)

    @staticmethod
    def _seal_response(name: str, raw: bytes) -> SealedAuthorityPayload:
        descriptor = create_sealed_memfd(name, raw, maximum=8 * 1024 * 1024)
        return SealedAuthorityPayload(descriptor, hashlib.sha256(raw).hexdigest())

    def bundle(
        self,
        grant_id: UUID,
        materialization_id: UUID,
        request: dict[str, object],
    ) -> SealedAuthorityPayload:
        grant = self._grant(grant_id)
        self._request_binding(grant, materialization_id, request)
        raw = self._request(
            f"/v1/projections/{grant}/materializations/{materialization_id}/bundle",
            request,
            expected_status=200,
            maximum_bytes=8 * 1024 * 1024,
        )
        return self._seal_response("guard-bundle", raw)

    def registry_credential(
        self,
        grant_id: UUID,
        materialization_id: UUID,
        request: dict[str, object],
    ) -> SealedAuthorityPayload:
        """Request one fixed-scope credential and seal its body without parsing it."""

        grant = self._grant(grant_id)
        if not isinstance(materialization_id, UUID) or materialization_id.int == 0:
            raise GuardError("authority_registry_credential_invalid")
        code = "authority_registry_credential_invalid"
        value = _exact(
            request,
            frozenset(
                {
                    "schema_version",
                    "request_id",
                    "grant_id",
                    "session_id",
                    "session_generation",
                    "session_token",
                    "materialization_id",
                    "attempt_id",
                    "lease_epoch",
                    "component",
                    "predecessor_credential_id",
                    "predecessor_generation",
                }
            ),
            code=code,
        )
        predecessor_id = value["predecessor_credential_id"]
        predecessor_generation = value["predecessor_generation"]
        if (
            value["schema_version"] != 1
            or _uuid(value["request_id"], code=code).int == 0
            or _uuid(value["grant_id"], code=code) != grant
            or _uuid(value["session_id"], code=code).int == 0
            or _integer(value["session_generation"], code=code) <= 0
            or not _token(value["session_token"], kind="session", code=code)
            or _uuid(value["materialization_id"], code=code)
            != materialization_id
            or _uuid(value["attempt_id"], code=code).int == 0
            or _integer(value["lease_epoch"], code=code) <= 0
            or not _component(value["component"], code=code)
            or (predecessor_id is None) != (predecessor_generation is None)
        ):
            raise GuardError(code)
        if predecessor_id is not None:
            _uuid(predecessor_id, code=code)
            _credential_generation(predecessor_generation, code=code)
        raw = self._request(
            f"/v1/projections/{grant}/materializations/{materialization_id}/"
            "registry-credential",
            request,
            expected_status=200,
            maximum_bytes=_MAX_REQUEST_BYTES,
        )
        descriptor = create_sealed_memfd(
            "guard-registry-credential",
            raw,
            maximum=_MAX_REQUEST_BYTES,
        )
        return SealedAuthorityPayload(descriptor, hashlib.sha256(raw).hexdigest())

    def publication_candidate(
        self,
        grant_id: UUID,
        materialization_id: UUID,
        request: dict[str, object],
    ) -> PublicationCandidateAcknowledgement:
        """Record one inert candidate and return only its strict nonsecret binding."""

        grant = self._grant(grant_id)
        if not isinstance(materialization_id, UUID) or materialization_id.int == 0:
            raise GuardError("authority_candidate_invalid")
        code = "authority_candidate_invalid"
        request_value = _exact(
            request,
            frozenset(
                {
                    "schema_version",
                    "operation_id",
                    "grant_id",
                    "session_id",
                    "session_generation",
                    "session_token",
                    "materialization_id",
                    "attempt_id",
                    "lease_epoch",
                    "credential_id",
                    "credential_generation",
                    "component",
                    "manifest_digest",
                    "manifest_size",
                    "oci_file_sha256",
                    "oci_file_size",
                    "platform",
                }
            ),
            code=code,
        )
        request_operation_id = _uuid(request_value["operation_id"], code=code)
        request_session_id = _uuid(request_value["session_id"], code=code)
        request_session_generation = _integer(
            request_value["session_generation"],
            code=code,
        )
        request_attempt_id = _uuid(request_value["attempt_id"], code=code)
        request_lease_epoch = _integer(request_value["lease_epoch"], code=code)
        request_credential_id = _uuid(request_value["credential_id"], code=code)
        request_credential_generation = _credential_generation(
            request_value["credential_generation"],
            code=code,
        )
        request_component = _component(request_value["component"], code=code)
        request_manifest_digest = _manifest_digest(
            request_value["manifest_digest"],
            code=code,
        )
        request_manifest_size = _integer(request_value["manifest_size"], code=code)
        request_oci_file_sha256 = _digest(request_value["oci_file_sha256"], code=code)
        request_oci_file_size = _integer(request_value["oci_file_size"], code=code)
        request_platform = request_value["platform"]
        if (
            request_value["schema_version"] != 1
            or _uuid(request_value["grant_id"], code=code) != grant
            or not _token(request_value["session_token"], kind="session", code=code)
            or _uuid(request_value["materialization_id"], code=code)
            != materialization_id
            or request_platform not in {"linux/amd64", "linux/arm64"}
        ):
            raise GuardError(code)

        raw = self._request(
            f"/v1/projections/{grant}/materializations/{materialization_id}/"
            "publication-candidate",
            request,
            expected_status=200,
            maximum_bytes=_MAX_REQUEST_BYTES,
        )
        response = _exact(
            _document(raw),
            frozenset(
                {
                    "schema_version",
                    "candidate_id",
                    "operation_id",
                    "credential_id",
                    "credential_generation",
                    "grant_id",
                    "session_id",
                    "session_generation",
                    "materialization_id",
                    "attempt_id",
                    "attempt_number",
                    "lease_epoch",
                    "builder_id",
                    "component",
                    "repository",
                    "manifest_digest",
                    "manifest_size",
                    "oci_file_sha256",
                    "oci_file_size",
                    "platform",
                    "recorded_at",
                }
            ),
            code=code,
        )
        candidate_id = _uuid(response["candidate_id"], code=code)
        attempt_number = _integer(response["attempt_number"], code=code)
        builder_id = response["builder_id"]
        repository = response["repository"]
        recorded_at = _time(response["recorded_at"], code=code)
        if (
            response["schema_version"]
            != "loom.task-image-publication-candidate.v1"
            or _uuid(response["operation_id"], code=code) != request_operation_id
            or _uuid(response["credential_id"], code=code) != request_credential_id
            or _credential_generation(response["credential_generation"], code=code)
            != request_credential_generation
            or _uuid(response["grant_id"], code=code) != grant
            or _uuid(response["session_id"], code=code) != request_session_id
            or _integer(response["session_generation"], code=code)
            != request_session_generation
            or _uuid(response["materialization_id"], code=code)
            != materialization_id
            or _uuid(response["attempt_id"], code=code) != request_attempt_id
            or _integer(response["lease_epoch"], code=code) != request_lease_epoch
            or not isinstance(builder_id, str)
            or _BUILDER_ID.fullmatch(builder_id) is None
            or _component(response["component"], code=code) != request_component
            or not isinstance(repository, str)
            or not 1 <= len(repository) <= 255
            or _manifest_digest(response["manifest_digest"], code=code)
            != request_manifest_digest
            or _integer(response["manifest_size"], code=code)
            != request_manifest_size
            or _digest(response["oci_file_sha256"], code=code)
            != request_oci_file_sha256
            or _integer(response["oci_file_size"], code=code)
            != request_oci_file_size
            or response["platform"] != request_platform
        ):
            raise GuardError(code)
        return PublicationCandidateAcknowledgement(
            candidate_id=candidate_id,
            operation_id=request_operation_id,
            credential_id=request_credential_id,
            credential_generation=request_credential_generation,
            grant_id=grant,
            session_id=request_session_id,
            session_generation=request_session_generation,
            materialization_id=materialization_id,
            attempt_id=request_attempt_id,
            attempt_number=attempt_number,
            lease_epoch=request_lease_epoch,
            builder_id=builder_id,
            component=request_component,
            manifest_digest=request_manifest_digest,
            manifest_size=request_manifest_size,
            oci_file_sha256=request_oci_file_sha256,
            oci_file_size=request_oci_file_size,
            platform=cast(Literal["linux/amd64", "linux/arm64"], request_platform),
            recorded_at=recorded_at,
            response_sha256=hashlib.sha256(raw).hexdigest(),
        )

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
    "LeaseAcknowledgement",
    "ProjectionChallenge",
    "ProjectionReceipt",
    "PublicationCandidateAcknowledgement",
    "SealedAuthorityPayload",
]
