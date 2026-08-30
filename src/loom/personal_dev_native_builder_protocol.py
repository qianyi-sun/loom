"""Canonical protocol values for the isolated personal-dev native builder."""

from __future__ import annotations

import hashlib
import hmac
import json
import os
import re
import stat
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import MappingProxyType
from typing import Literal
from urllib.parse import urlsplit
from uuid import UUID

from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import (
    Ed25519PrivateKey,
    Ed25519PublicKey,
)

NATIVE_BUILDER_PLATFORM = "linux/arm64"
NATIVE_BUILDER_PROVIDER = "gb10-gvisor-docker-v1"
NATIVE_BUILDER_PROTOCOL_VERSION = 1
NATIVE_BUILDER_MAX_CONCURRENCY = 2
NATIVE_BUILDER_RUNTIME_NAME = "runsc-personal-dev-native"

_DIGEST_RE = re.compile(r"[0-9a-f]{64}")
_IMMUTABLE_IMAGE_RE = re.compile(
    r"[A-Za-z0-9][A-Za-z0-9._:/-]{0,511}@sha256:[0-9a-f]{64}"
)
_KEY_ID_RE = re.compile(r"[a-z][a-z0-9._-]{0,63}")
_HOST_NAME_RE = re.compile(r"[a-z0-9](?:[a-z0-9.-]{0,61}[a-z0-9])?")
_REASON_RE = re.compile(r"[a-z][a-z0-9_]{0,127}")
_CONTAINER_ID_RE = re.compile(r"[0-9a-f]{64}")
_SIGNATURE_RE = re.compile(r"[0-9a-f]{128}")
_POLL_DOMAIN = b"loom-personal-dev-native-builder:poll:v1\n"
_HEARTBEAT_DOMAIN = b"loom-personal-dev-native-builder:heartbeat:v1\n"
_COMPLETION_DOMAIN = b"loom-personal-dev-native-builder:completion:v1\n"


def _canonical_bytes(payload: Mapping[str, object]) -> bytes:
    return json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("ascii")


def _utc_timestamp(value: datetime, *, label: str) -> str:
    if not isinstance(value, datetime) or value.tzinfo is None:
        raise ValueError(f"native builder {label} timestamp must include a timezone")
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _validate_uuid(value: UUID, *, label: str) -> None:
    if not isinstance(value, UUID) or value.int == 0:
        raise ValueError(f"native builder {label} must be a nonzero UUID")


def _validate_digest(value: str, *, label: str) -> None:
    if not isinstance(value, str) or _DIGEST_RE.fullmatch(value) is None or value == "0" * 64:
        raise ValueError(f"native builder {label} digest is invalid")


def _validate_image(value: str, *, label: str) -> None:
    if not isinstance(value, str) or _IMMUTABLE_IMAGE_RE.fullmatch(value) is None:
        raise ValueError(f"native builder {label} image is not immutable")


def _validate_key_id(value: str) -> None:
    if not isinstance(value, str) or _KEY_ID_RE.fullmatch(value) is None:
        raise ValueError("native builder agent key id is invalid")


def _validate_host(value: str) -> None:
    if not isinstance(value, str) or _HOST_NAME_RE.fullmatch(value) is None:
        raise ValueError("native builder host name is invalid")


def _validate_identity(
    *,
    provider: str,
    platform: str,
    host_architecture: str,
) -> None:
    if provider != NATIVE_BUILDER_PROVIDER:
        raise ValueError("native builder provider is invalid")
    if platform != NATIVE_BUILDER_PLATFORM:
        raise ValueError("native builder platform is invalid")
    if host_architecture != "aarch64":
        raise ValueError("native builder host architecture is invalid")


def _validate_https_url(value: str) -> None:
    if not isinstance(value, str) or not 1 <= len(value) <= 8192:
        raise ValueError("native builder capability URL is invalid")
    parsed = urlsplit(value)
    if (
        parsed.scheme != "https"
        or not parsed.netloc
        or parsed.hostname is None
        or parsed.username is not None
        or parsed.password is not None
        or parsed.fragment
        or any(character in value for character in "\r\n\0")
    ):
        raise ValueError("native builder capability URL is invalid")


def _unique_json_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    value: dict[str, object] = {}
    for key, item in pairs:
        if key in value:
            raise ValueError("native builder contract contains duplicate fields")
        value[key] = item
    return value


def _validate_contract(value: str, digest: str) -> None:
    if not isinstance(value, str) or not 2 <= len(value.encode("utf-8")) <= 64 * 1024:
        raise ValueError("native builder contract is invalid")
    try:
        parsed = json.loads(value, object_pairs_hook=_unique_json_object)
    except (TypeError, ValueError, json.JSONDecodeError):
        raise ValueError("native builder contract is invalid") from None
    if not isinstance(parsed, dict) or _canonical_bytes(parsed).decode("ascii") != value:
        raise ValueError("native builder contract must be canonical ASCII JSON")
    _validate_digest(digest, label="contract")
    if hashlib.sha256(value.encode("ascii")).hexdigest() != digest:
        raise ValueError("native builder contract digest does not match")


def _status_payload(status: NativeBuilderAgentStatus) -> dict[str, object]:
    return {
        "active_grant_ids": [str(grant_id) for grant_id in status.active_grant_ids],
        "agent_image": status.agent_image,
        "agent_instance_id": str(status.agent_instance_id),
        "agent_key_id": status.agent_key_id,
        "available": status.available,
        "builder_image": status.builder_image,
        "host_architecture": status.host_architecture,
        "host_boot_id": str(status.host_boot_id),
        "host_name": status.host_name,
        "managed_grant_ids": [str(grant_id) for grant_id in status.managed_grant_ids],
        "max_concurrency": status.max_concurrency,
        "platform": status.platform,
        "protocol_version": status.protocol_version,
        "provider": status.provider,
        "readiness_evidence_sha256": status.readiness_evidence_sha256,
        "runtime_profile_sha256": status.runtime_profile_sha256,
        "unavailable_reason": status.unavailable_reason,
    }


@dataclass(frozen=True, slots=True)
class NativeBuilderAgentStatus:
    """Secret-free exact identity and managed-resource inventory from one agent."""

    agent_instance_id: UUID
    agent_key_id: str
    provider: str
    platform: str
    protocol_version: int
    host_name: str
    host_architecture: str
    host_boot_id: UUID
    agent_image: str
    builder_image: str
    runtime_profile_sha256: str
    max_concurrency: int
    managed_grant_ids: tuple[UUID, ...]
    active_grant_ids: tuple[UUID, ...]
    available: bool
    unavailable_reason: str | None
    readiness_evidence_sha256: str

    def __post_init__(self) -> None:
        _validate_uuid(self.agent_instance_id, label="agent instance")
        _validate_uuid(self.host_boot_id, label="host boot")
        _validate_key_id(self.agent_key_id)
        _validate_identity(
            provider=self.provider,
            platform=self.platform,
            host_architecture=self.host_architecture,
        )
        if type(self.protocol_version) is not int or (
            self.protocol_version != NATIVE_BUILDER_PROTOCOL_VERSION
        ):
            raise ValueError("native builder protocol version is invalid")
        _validate_host(self.host_name)
        _validate_image(self.agent_image, label="agent")
        _validate_image(self.builder_image, label="builder")
        _validate_digest(self.runtime_profile_sha256, label="runtime profile")
        _validate_digest(self.readiness_evidence_sha256, label="readiness evidence")
        if type(self.max_concurrency) is not int or (
            self.max_concurrency != NATIVE_BUILDER_MAX_CONCURRENCY
        ):
            raise ValueError("native builder concurrency is invalid")
        managed = self.managed_grant_ids
        active = self.active_grant_ids
        if (
            not isinstance(managed, tuple)
            or not isinstance(active, tuple)
            or len(managed) > 64
            or len(active) > NATIVE_BUILDER_MAX_CONCURRENCY
            or any(not isinstance(item, UUID) or item.int == 0 for item in (*managed, *active))
            or managed != tuple(sorted(set(managed), key=str))
            or active != tuple(sorted(set(active), key=str))
            or not set(active).issubset(managed)
        ):
            raise ValueError("native builder grant inventory is invalid")
        if type(self.available) is not bool or (
            self.available != (self.unavailable_reason is None)
        ):
            raise ValueError("native builder availability is inconsistent")
        if self.unavailable_reason is not None and (
            not isinstance(self.unavailable_reason, str)
            or _REASON_RE.fullmatch(self.unavailable_reason) is None
        ):
            raise ValueError("native builder unavailable reason is invalid")

    def canonical_bytes(self) -> bytes:
        return _canonical_bytes(_status_payload(self))


@dataclass(frozen=True, slots=True)
class NativeBuilderPollRequest:
    """Fresh signed agent inventory and grant request."""

    status: NativeBuilderAgentStatus
    requested_at: datetime
    request_nonce: UUID

    def __post_init__(self) -> None:
        if not isinstance(self.status, NativeBuilderAgentStatus):
            raise ValueError("native builder poll status is invalid")
        _utc_timestamp(self.requested_at, label="poll request")
        _validate_uuid(self.request_nonce, label="request nonce")

    @property
    def agent_key_id(self) -> str:
        return self.status.agent_key_id

    def canonical_bytes(self) -> bytes:
        return _canonical_bytes(
            {
                "request_nonce": str(self.request_nonce),
                "requested_at": _utc_timestamp(self.requested_at, label="poll request"),
                "schema_version": NATIVE_BUILDER_PROTOCOL_VERSION,
                "status": _status_payload(self.status),
            }
        )


@dataclass(frozen=True, slots=True)
class NativeBuilderGrantPayload:
    """One secret response bound to an exact native build grant."""

    grant_id: UUID
    candidate_id: UUID
    candidate_sha: str
    attempt_id: UUID
    attempt_lease_epoch: int
    platform: str
    provider: str
    agent_instance_id: UUID
    agent_key_id: str
    builder_image: str
    runtime_profile_sha256: str
    contract_json: str
    contract_sha256: str
    source_get_url: str
    artifact_upload_url: str
    artifact_upload_fields: Mapping[str, str]
    artifact_max_bytes: int
    capability_expires_at: datetime
    active_deadline_seconds: int

    def __post_init__(self) -> None:
        for label, value in (
            ("grant", self.grant_id),
            ("candidate", self.candidate_id),
            ("attempt", self.attempt_id),
            ("agent instance", self.agent_instance_id),
        ):
            _validate_uuid(value, label=label)
        _validate_digest(self.candidate_sha, label="candidate")
        if type(self.attempt_lease_epoch) is not int or self.attempt_lease_epoch <= 0:
            raise ValueError("native builder attempt lease is invalid")
        _validate_identity(
            provider=self.provider,
            platform=self.platform,
            host_architecture="aarch64",
        )
        _validate_key_id(self.agent_key_id)
        _validate_image(self.builder_image, label="builder")
        _validate_digest(self.runtime_profile_sha256, label="runtime profile")
        _validate_contract(self.contract_json, self.contract_sha256)
        _validate_https_url(self.source_get_url)
        _validate_https_url(self.artifact_upload_url)
        fields = dict(self.artifact_upload_fields)
        if (
            not 1 <= len(fields) <= 32
            or any(
                not isinstance(key, str)
                or not isinstance(value, str)
                or not key
                or len(key) > 256
                or len(value) > 8192
                or any(character in key + value for character in "\r\n\0")
                for key, value in fields.items()
            )
        ):
            raise ValueError("native builder artifact upload fields are invalid")
        object.__setattr__(self, "artifact_upload_fields", MappingProxyType(fields))
        if (
            type(self.artifact_max_bytes) is not int
            or not 1 <= self.artifact_max_bytes <= 16 * 1024 * 1024 * 1024
        ):
            raise ValueError("native builder artifact byte limit is invalid")
        _utc_timestamp(self.capability_expires_at, label="capability expiry")
        if (
            type(self.active_deadline_seconds) is not int
            or not 300 <= self.active_deadline_seconds <= 7200
        ):
            raise ValueError("native builder active deadline is invalid")


@dataclass(frozen=True, slots=True)
class NativeBuilderHeartbeatRequest:
    """Fresh signed liveness assertion for one exact running grant."""

    agent_instance_id: UUID
    agent_key_id: str
    grant_id: UUID
    attempt_id: UUID
    attempt_lease_epoch: int
    requested_at: datetime
    request_nonce: UUID

    def __post_init__(self) -> None:
        for label, value in (
            ("agent instance", self.agent_instance_id),
            ("grant", self.grant_id),
            ("attempt", self.attempt_id),
            ("request nonce", self.request_nonce),
        ):
            _validate_uuid(value, label=label)
        _validate_key_id(self.agent_key_id)
        if type(self.attempt_lease_epoch) is not int or self.attempt_lease_epoch <= 0:
            raise ValueError("native builder attempt lease is invalid")
        _utc_timestamp(self.requested_at, label="heartbeat request")

    def canonical_bytes(self) -> bytes:
        return _canonical_bytes(
            {
                "agent_instance_id": str(self.agent_instance_id),
                "agent_key_id": self.agent_key_id,
                "attempt_id": str(self.attempt_id),
                "attempt_lease_epoch": self.attempt_lease_epoch,
                "grant_id": str(self.grant_id),
                "request_nonce": str(self.request_nonce),
                "requested_at": _utc_timestamp(self.requested_at, label="heartbeat request"),
                "schema_version": NATIVE_BUILDER_PROTOCOL_VERSION,
            }
        )


@dataclass(frozen=True, slots=True)
class NativeBuilderRuntimeEvidence:
    """Canonical secret-free Docker/runtime evidence for one successful grant."""

    agent_instance_id: UUID
    grant_id: UUID
    attempt_id: UUID
    attempt_lease_epoch: int
    provider: str
    platform: str
    host_name: str
    host_architecture: str
    host_boot_id: UUID
    agent_image: str
    builder_image: str
    runtime_profile_sha256: str
    contract_sha256: str
    runtime_name: str
    client_container_id: str
    buildkit_container_id: str
    network_id: str
    client_inspect_sha256: str
    buildkit_inspect_sha256: str
    network_inspect_sha256: str
    client_exit_code: int
    client_oom_killed: bool
    client_restart_count: int
    buildkit_restart_count: int
    buildkit_running: bool
    observed_at: datetime

    def __post_init__(self) -> None:
        for identifier_label, identifier in (
            ("agent instance", self.agent_instance_id),
            ("grant", self.grant_id),
            ("attempt", self.attempt_id),
            ("host boot", self.host_boot_id),
        ):
            _validate_uuid(identifier, label=identifier_label)
        if type(self.attempt_lease_epoch) is not int or self.attempt_lease_epoch <= 0:
            raise ValueError("native builder attempt lease is invalid")
        _validate_identity(
            provider=self.provider,
            platform=self.platform,
            host_architecture=self.host_architecture,
        )
        _validate_host(self.host_name)
        _validate_image(self.agent_image, label="agent")
        _validate_image(self.builder_image, label="builder")
        for digest_label, digest_value in (
            ("runtime profile", self.runtime_profile_sha256),
            ("contract", self.contract_sha256),
            ("client inspect", self.client_inspect_sha256),
            ("buildkit inspect", self.buildkit_inspect_sha256),
            ("network inspect", self.network_inspect_sha256),
        ):
            _validate_digest(digest_value, label=digest_label)
        if self.runtime_name != NATIVE_BUILDER_RUNTIME_NAME:
            raise ValueError("native builder runtime is invalid")
        for resource_label, resource_id in (
            ("client container", self.client_container_id),
            ("buildkit container", self.buildkit_container_id),
            ("network", self.network_id),
        ):
            if _CONTAINER_ID_RE.fullmatch(resource_id) is None:
                raise ValueError(f"native builder {resource_label} id is invalid")
        if self.client_container_id == self.buildkit_container_id:
            raise ValueError("native builder container identities are not distinct")
        if type(self.client_exit_code) is not int or not 0 <= self.client_exit_code <= 255:
            raise ValueError("native builder client exit code is invalid")
        if type(self.client_oom_killed) is not bool or type(self.buildkit_running) is not bool:
            raise ValueError("native builder runtime boolean evidence is invalid")
        if (
            type(self.client_restart_count) is not int
            or self.client_restart_count != 0
            or type(self.buildkit_restart_count) is not int
            or self.buildkit_restart_count != 0
        ):
            raise ValueError("native builder restart evidence is invalid")
        _utc_timestamp(self.observed_at, label="runtime observation")

    def _payload(self) -> dict[str, object]:
        return {
            "agent_image": self.agent_image,
            "agent_instance_id": str(self.agent_instance_id),
            "attempt_id": str(self.attempt_id),
            "attempt_lease_epoch": self.attempt_lease_epoch,
            "builder_image": self.builder_image,
            "buildkit_container_id": self.buildkit_container_id,
            "buildkit_inspect_sha256": self.buildkit_inspect_sha256,
            "buildkit_restart_count": self.buildkit_restart_count,
            "buildkit_running": self.buildkit_running,
            "client_container_id": self.client_container_id,
            "client_exit_code": self.client_exit_code,
            "client_inspect_sha256": self.client_inspect_sha256,
            "client_oom_killed": self.client_oom_killed,
            "client_restart_count": self.client_restart_count,
            "contract_sha256": self.contract_sha256,
            "grant_id": str(self.grant_id),
            "host_architecture": self.host_architecture,
            "host_boot_id": str(self.host_boot_id),
            "host_name": self.host_name,
            "network_id": self.network_id,
            "network_inspect_sha256": self.network_inspect_sha256,
            "observed_at": _utc_timestamp(self.observed_at, label="runtime observation"),
            "platform": self.platform,
            "provider": self.provider,
            "runtime_name": self.runtime_name,
            "runtime_profile_sha256": self.runtime_profile_sha256,
        }

    def canonical_bytes(self) -> bytes:
        return _canonical_bytes(self._payload())


@dataclass(frozen=True, slots=True)
class NativeBuilderCompletion:
    """Fresh signed terminal report for one exact grant."""

    agent_instance_id: UUID
    agent_key_id: str
    grant_id: UUID
    attempt_id: UUID
    attempt_lease_epoch: int
    outcome: Literal["succeeded", "failed"]
    failure_reason: str | None
    evidence: NativeBuilderRuntimeEvidence | None
    requested_at: datetime
    request_nonce: UUID

    def __post_init__(self) -> None:
        for label, value in (
            ("agent instance", self.agent_instance_id),
            ("grant", self.grant_id),
            ("attempt", self.attempt_id),
            ("request nonce", self.request_nonce),
        ):
            _validate_uuid(value, label=label)
        _validate_key_id(self.agent_key_id)
        if type(self.attempt_lease_epoch) is not int or self.attempt_lease_epoch <= 0:
            raise ValueError("native builder attempt lease is invalid")
        _utc_timestamp(self.requested_at, label="completion request")
        if self.outcome == "succeeded":
            if self.failure_reason is not None or not isinstance(
                self.evidence, NativeBuilderRuntimeEvidence
            ):
                raise ValueError("native builder completion shape is invalid")
            evidence = self.evidence
            if (
                evidence.agent_instance_id != self.agent_instance_id
                or evidence.grant_id != self.grant_id
                or evidence.attempt_id != self.attempt_id
                or evidence.attempt_lease_epoch != self.attempt_lease_epoch
                or evidence.client_exit_code != 0
                or evidence.client_oom_killed
                or not evidence.buildkit_running
            ):
                raise ValueError("native builder completion evidence is invalid")
        elif self.outcome == "failed":
            if (
                self.evidence is not None
                or not isinstance(self.failure_reason, str)
                or _REASON_RE.fullmatch(self.failure_reason) is None
            ):
                raise ValueError("native builder completion failure reason is invalid")
        else:
            raise ValueError("native builder completion outcome is invalid")

    def canonical_bytes(self) -> bytes:
        return _canonical_bytes(
            {
                "agent_instance_id": str(self.agent_instance_id),
                "agent_key_id": self.agent_key_id,
                "attempt_id": str(self.attempt_id),
                "attempt_lease_epoch": self.attempt_lease_epoch,
                "evidence": self.evidence._payload() if self.evidence is not None else None,
                "failure_reason": self.failure_reason,
                "grant_id": str(self.grant_id),
                "outcome": self.outcome,
                "request_nonce": str(self.request_nonce),
                "requested_at": _utc_timestamp(self.requested_at, label="completion request"),
                "schema_version": NATIVE_BUILDER_PROTOCOL_VERSION,
            }
        )


class PersonalDevNativeBuilderSigner:
    """Agent-only Ed25519 signing authority for native-builder requests."""

    def __init__(self, *, keys: Mapping[str, bytes]) -> None:
        normalized = dict(keys)
        if not normalized or any(
            not isinstance(key_id, str)
            or _KEY_ID_RE.fullmatch(key_id) is None
            or not isinstance(key, bytes)
            or len(key) != 32
            for key_id, key in normalized.items()
        ):
            raise ValueError("native builder signer keys are invalid")
        self._keys = MappingProxyType(
            {
                key_id: Ed25519PrivateKey.from_private_bytes(key)
                for key_id, key in normalized.items()
            }
        )

    def _sign(self, *, key_id: str, domain: bytes, payload: bytes) -> str:
        try:
            key = self._keys[key_id]
        except KeyError:
            raise ValueError("native builder signing key is unknown") from None
        return key.sign(domain + payload).hex()

    def sign_poll(self, request: NativeBuilderPollRequest) -> str:
        return self._sign(
            key_id=request.agent_key_id,
            domain=_POLL_DOMAIN,
            payload=request.canonical_bytes(),
        )

    def sign_heartbeat(self, request: NativeBuilderHeartbeatRequest) -> str:
        return self._sign(
            key_id=request.agent_key_id,
            domain=_HEARTBEAT_DOMAIN,
            payload=request.canonical_bytes(),
        )

    def sign_completion(self, request: NativeBuilderCompletion) -> str:
        return self._sign(
            key_id=request.agent_key_id,
            domain=_COMPLETION_DOMAIN,
            payload=request.canonical_bytes(),
        )

    def public_key_bytes(self, key_id: str) -> bytes:
        try:
            key = self._keys[key_id]
        except KeyError:
            raise ValueError("native builder signing key is unknown") from None
        return key.public_key().public_bytes(
            encoding=serialization.Encoding.Raw,
            format=serialization.PublicFormat.Raw,
        )


class PersonalDevNativeBuilderVerifier:
    """Management-only verification authority with bounded freshness."""

    def __init__(
        self,
        *,
        keys: Mapping[str, bytes],
        max_age_seconds: int = 60,
        future_skew_seconds: int = 15,
    ) -> None:
        normalized = dict(keys)
        if not normalized or any(
            not isinstance(key_id, str)
            or _KEY_ID_RE.fullmatch(key_id) is None
            or not isinstance(key, bytes)
            or len(key) != 32
            for key_id, key in normalized.items()
        ):
            raise ValueError("native builder verifier keys are invalid")
        if type(max_age_seconds) is not int or not 1 <= max_age_seconds <= 300:
            raise ValueError("native builder verification max age is invalid")
        if type(future_skew_seconds) is not int or not 0 <= future_skew_seconds <= 60:
            raise ValueError("native builder verification future skew is invalid")
        self._keys = MappingProxyType(
            {key_id: Ed25519PublicKey.from_public_bytes(key) for key_id, key in normalized.items()}
        )
        self._max_age = timedelta(seconds=max_age_seconds)
        self._future_skew = timedelta(seconds=future_skew_seconds)

    def _verify(
        self,
        *,
        key_id: str,
        requested_at: datetime,
        domain: bytes,
        payload: bytes,
        signature: str,
        now: datetime,
    ) -> str:
        if not isinstance(now, datetime) or now.tzinfo is None:
            raise ValueError("native builder verification time must include a timezone")
        normalized_request = requested_at.astimezone(UTC)
        normalized_now = now.astimezone(UTC)
        if (
            normalized_request < normalized_now - self._max_age
            or normalized_request > normalized_now + self._future_skew
        ):
            raise ValueError("native builder request freshness window expired")
        if not isinstance(signature, str) or _SIGNATURE_RE.fullmatch(signature) is None:
            raise ValueError("native builder request signature is invalid")
        try:
            key = self._keys[key_id]
        except KeyError:
            raise ValueError("native builder verification key is unknown") from None
        try:
            key.verify(bytes.fromhex(signature), domain + payload)
        except InvalidSignature:
            raise ValueError("native builder request signature is invalid") from None
        return hashlib.sha256(payload).hexdigest()

    def verify_poll(
        self,
        request: NativeBuilderPollRequest,
        *,
        signature: str,
        now: datetime,
    ) -> str:
        return self._verify(
            key_id=request.agent_key_id,
            requested_at=request.requested_at,
            domain=_POLL_DOMAIN,
            payload=request.canonical_bytes(),
            signature=signature,
            now=now,
        )

    def verify_heartbeat(
        self,
        request: NativeBuilderHeartbeatRequest,
        *,
        signature: str,
        now: datetime,
    ) -> str:
        return self._verify(
            key_id=request.agent_key_id,
            requested_at=request.requested_at,
            domain=_HEARTBEAT_DOMAIN,
            payload=request.canonical_bytes(),
            signature=signature,
            now=now,
        )

    def verify_completion(
        self,
        request: NativeBuilderCompletion,
        *,
        signature: str,
        now: datetime,
    ) -> str:
        return self._verify(
            key_id=request.agent_key_id,
            requested_at=request.requested_at,
            domain=_COMPLETION_DOMAIN,
            payload=request.canonical_bytes(),
            signature=signature,
            now=now,
        )


def _load_native_builder_key(
    key_file: Path,
    *,
    private: bool,
) -> bytes:
    label = "private owner-only" if private else "public read-only"
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(key_file, flags)
    except OSError:
        raise RuntimeError(f"native builder {label} key must be an available regular file") from None
    try:
        before = os.fstat(descriptor)
        mode = stat.S_IMODE(before.st_mode)
        accepted_modes = {0o400} if private else {0o400, 0o440}
        if (
            not stat.S_ISREG(before.st_mode)
            or before.st_uid != os.geteuid()
            or before.st_nlink != 1
            or mode not in accepted_modes
            or before.st_size != 32
        ):
            raise RuntimeError(f"native builder {label} key must be a regular {label} file")
        key = os.read(descriptor, 33)
        after = os.fstat(descriptor)
    except OSError:
        raise RuntimeError(f"native builder {label} key is unreadable") from None
    finally:
        os.close(descriptor)
    stable_fields = (
        "st_dev",
        "st_ino",
        "st_mode",
        "st_nlink",
        "st_uid",
        "st_gid",
        "st_size",
        "st_mtime_ns",
        "st_ctime_ns",
    )
    if len(key) != 32 or any(
        getattr(before, field) != getattr(after, field) for field in stable_fields
    ):
        raise RuntimeError(f"native builder {label} key changed while reading")
    return key


def load_personal_dev_native_builder_signer(
    key_file: Path,
    *,
    key_id: str,
) -> PersonalDevNativeBuilderSigner:
    key = _load_native_builder_key(key_file, private=True)
    return PersonalDevNativeBuilderSigner(keys={key_id: key})


def load_personal_dev_native_builder_verifier(
    key_file: Path,
    *,
    key_id: str,
    expected_sha256: str | None = None,
    max_age_seconds: int = 60,
    future_skew_seconds: int = 15,
) -> PersonalDevNativeBuilderVerifier:
    key = _load_native_builder_key(key_file, private=False)
    if expected_sha256 is not None and (
        not isinstance(expected_sha256, str)
        or _DIGEST_RE.fullmatch(expected_sha256) is None
        or not hmac.compare_digest(hashlib.sha256(key).hexdigest(), expected_sha256)
    ):
        raise RuntimeError("native builder public key digest does not match")
    return PersonalDevNativeBuilderVerifier(
        keys={key_id: key},
        max_age_seconds=max_age_seconds,
        future_skew_seconds=future_skew_seconds,
    )


__all__ = [
    "NATIVE_BUILDER_MAX_CONCURRENCY",
    "NATIVE_BUILDER_PLATFORM",
    "NATIVE_BUILDER_PROTOCOL_VERSION",
    "NATIVE_BUILDER_PROVIDER",
    "NATIVE_BUILDER_RUNTIME_NAME",
    "NativeBuilderAgentStatus",
    "NativeBuilderCompletion",
    "NativeBuilderGrantPayload",
    "NativeBuilderHeartbeatRequest",
    "NativeBuilderPollRequest",
    "NativeBuilderRuntimeEvidence",
    "PersonalDevNativeBuilderSigner",
    "PersonalDevNativeBuilderVerifier",
    "load_personal_dev_native_builder_signer",
    "load_personal_dev_native_builder_verifier",
]
