"""Signed two-phase activation acknowledgements for personal environments."""

from __future__ import annotations

import hashlib
import json
import os
import re
import stat
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import MappingProxyType
from uuid import UUID

from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import (
    Ed25519PrivateKey,
    Ed25519PublicKey,
)

from loom.dev_instance import InvalidDevInstanceNameError, validate_name
from loom.personal_dev_candidate import PERSONAL_DEV_COMPONENTS

_DIGEST_RE = re.compile(r"[0-9a-f]{64}")
_SIGNATURE_RE = re.compile(r"[0-9a-f]{128}")
_KEY_ID_RE = re.compile(r"[a-z][a-z0-9._-]{0,63}")
_IMMUTABLE_IMAGE_RE = re.compile(
    r"[A-Za-z0-9][A-Za-z0-9._:/-]{0,511}@sha256:[0-9a-f]{64}",
)


def _canonical_bytes(payload: Mapping[str, object]) -> bytes:
    return json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode("ascii")


@dataclass(frozen=True, slots=True)
class PersonalDevActivationIntentRequest:
    """Fresh signed read request; possession of the agent key gates intent access."""

    agent_key_id: str
    request_nonce: UUID
    requested_at: datetime
    operation_id: UUID | None = None
    exclude_operation_ids: tuple[UUID, ...] = ()

    def __post_init__(self) -> None:
        if _KEY_ID_RE.fullmatch(self.agent_key_id) is None:
            raise ValueError("activation agent key id is invalid")
        if self.requested_at.tzinfo is None:
            raise ValueError("activation intent request timestamp must include a timezone")
        if (
            len(self.exclude_operation_ids) > 16
            or len(set(self.exclude_operation_ids)) != len(self.exclude_operation_ids)
            or (self.operation_id is not None and self.exclude_operation_ids)
        ):
            raise ValueError("activation intent request selection is invalid")

    def canonical_bytes(self) -> bytes:
        requested_at = self.requested_at.astimezone(UTC).isoformat().replace("+00:00", "Z")
        return _canonical_bytes(
            {
                "agent_key_id": self.agent_key_id,
                "exclude_operation_ids": sorted(
                    str(operation_id) for operation_id in self.exclude_operation_ids
                ),
                "operation_id": str(self.operation_id) if self.operation_id is not None else None,
                "request_nonce": str(self.request_nonce),
                "requested_at": requested_at,
                "schema_version": 1,
            }
        )


@dataclass(frozen=True, slots=True)
class PersonalDevActivationIntent:
    """Secret-free exact central intent consumed by the independent agent."""

    environment_name: str
    subject_id: UUID
    subject_incarnation: UUID
    operation_id: UUID
    operation_epoch: int
    attempt_id: UUID
    attempt_sequence: int
    candidate_id: UUID
    candidate_sha: str
    candidate_publication_sha256: str
    deployment_generation: int
    readiness_evidence_sha256: str
    min_slots: int
    max_slots: int
    images: Mapping[str, str]
    intent_created_at: datetime

    def __post_init__(self) -> None:
        try:
            validate_name(self.environment_name)
        except InvalidDevInstanceNameError as exc:
            raise ValueError("activation environment name is invalid") from exc
        if (
            type(self.operation_epoch) is not int
            or self.operation_epoch <= 0
            or type(self.attempt_sequence) is not int
            or self.attempt_sequence < 0
            or type(self.deployment_generation) is not int
            or self.deployment_generation <= 0
        ):
            raise ValueError("activation intent counters are invalid")
        if any(
            _DIGEST_RE.fullmatch(value) is None
            for value in (
                self.candidate_sha,
                self.candidate_publication_sha256,
                self.readiness_evidence_sha256,
            )
        ):
            raise ValueError("activation intent digest binding is invalid")
        if (
            type(self.min_slots) is not int
            or type(self.max_slots) is not int
            or not 0 <= self.min_slots <= self.max_slots <= 8
        ):
            raise ValueError("activation intent slot policy is invalid")
        normalized_images = dict(self.images)
        if set(normalized_images) != set(PERSONAL_DEV_COMPONENTS) or any(
            not isinstance(reference, str) or _IMMUTABLE_IMAGE_RE.fullmatch(reference) is None
            for reference in normalized_images.values()
        ):
            raise ValueError("activation intent image publication is invalid")
        object.__setattr__(self, "images", MappingProxyType(normalized_images))
        if self.intent_created_at.tzinfo is None:
            raise ValueError("activation intent timestamp must include a timezone")

    def canonical_bytes(self) -> bytes:
        created_at = self.intent_created_at.astimezone(UTC).isoformat().replace("+00:00", "Z")
        return _canonical_bytes(
            {
                "attempt_id": str(self.attempt_id),
                "attempt_sequence": self.attempt_sequence,
                "candidate_id": str(self.candidate_id),
                "candidate_publication_sha256": self.candidate_publication_sha256,
                "candidate_sha": self.candidate_sha,
                "deployment_generation": self.deployment_generation,
                "environment_name": self.environment_name,
                "images": dict(self.images),
                "intent_created_at": created_at,
                "max_slots": self.max_slots,
                "min_slots": self.min_slots,
                "operation_epoch": self.operation_epoch,
                "operation_id": str(self.operation_id),
                "readiness_evidence_sha256": self.readiness_evidence_sha256,
                "schema_version": 1,
                "subject_id": str(self.subject_id),
                "subject_incarnation": str(self.subject_incarnation),
            }
        )

    @property
    def intent_sha256(self) -> str:
        return hashlib.sha256(self.canonical_bytes()).hexdigest()


@dataclass(frozen=True, slots=True)
class PersonalDevActivationAcknowledgement:
    """Exact local transaction evidence bound to one central activation intent."""

    environment_name: str
    subject_id: UUID
    subject_incarnation: UUID
    operation_id: UUID
    operation_epoch: int
    attempt_id: UUID
    candidate_id: UUID
    candidate_sha: str
    deployment_generation: int
    readiness_evidence_sha256: str
    local_activation_sha256: str
    agent_key_id: str
    observed_at: datetime

    def __post_init__(self) -> None:
        try:
            validate_name(self.environment_name)
        except InvalidDevInstanceNameError as exc:
            raise ValueError("activation environment name is invalid") from exc
        if type(self.operation_epoch) is not int or self.operation_epoch <= 0:
            raise ValueError("activation operation epoch must be positive")
        if type(self.deployment_generation) is not int or self.deployment_generation <= 0:
            raise ValueError("activation deployment generation must be positive")
        if any(
            _DIGEST_RE.fullmatch(digest) is None
            for digest in (
                self.candidate_sha,
                self.readiness_evidence_sha256,
                self.local_activation_sha256,
            )
        ):
            raise ValueError("activation digest binding is invalid")
        if _KEY_ID_RE.fullmatch(self.agent_key_id) is None:
            raise ValueError("activation agent key id is invalid")
        if self.observed_at.tzinfo is None:
            raise ValueError("activation observation timestamp must include a timezone")

    def canonical_bytes(self) -> bytes:
        observed_at = self.observed_at.astimezone(UTC).isoformat().replace("+00:00", "Z")
        payload = {
            "agent_key_id": self.agent_key_id,
            "attempt_id": str(self.attempt_id),
            "candidate_id": str(self.candidate_id),
            "candidate_sha": self.candidate_sha,
            "deployment_generation": self.deployment_generation,
            "environment_name": self.environment_name,
            "local_activation_sha256": self.local_activation_sha256,
            "observed_at": observed_at,
            "operation_epoch": self.operation_epoch,
            "operation_id": str(self.operation_id),
            "readiness_evidence_sha256": self.readiness_evidence_sha256,
            "schema_version": 1,
            "subject_id": str(self.subject_id),
            "subject_incarnation": str(self.subject_incarnation),
        }
        return _canonical_bytes(payload)


@dataclass(frozen=True, slots=True)
class VerifiedPersonalDevActivationAcknowledgement:
    acknowledgement: PersonalDevActivationAcknowledgement
    payload_sha256: str
    signature_sha256: str


class PersonalDevActivationVerifier:
    """Authenticate agent evidence using public keys unavailable for signing."""

    def __init__(
        self,
        *,
        keys: Mapping[str, bytes],
        max_age_seconds: int = 300,
        future_skew_seconds: int = 30,
    ) -> None:
        normalized = dict(keys)
        if not normalized or any(
            _KEY_ID_RE.fullmatch(key_id) is None or not isinstance(key, bytes) or len(key) != 32
            for key_id, key in normalized.items()
        ):
            raise ValueError("activation verifier keys are invalid")
        if type(max_age_seconds) is not int or max_age_seconds <= 0:
            raise ValueError("activation acknowledgement max age must be positive")
        if type(future_skew_seconds) is not int or future_skew_seconds < 0:
            raise ValueError("activation acknowledgement future skew must be nonnegative")
        self._keys = MappingProxyType(
            {
                key_id: Ed25519PublicKey.from_public_bytes(key)
                for key_id, key in normalized.items()
            }
        )
        self._max_age = timedelta(seconds=max_age_seconds)
        self._future_skew = timedelta(seconds=future_skew_seconds)

    def verify(
        self,
        acknowledgement: PersonalDevActivationAcknowledgement,
        *,
        signature: str,
        now: datetime,
    ) -> VerifiedPersonalDevActivationAcknowledgement:
        if now.tzinfo is None:
            raise ValueError("activation verification time must include a timezone")
        observed_at = acknowledgement.observed_at.astimezone(UTC)
        normalized_now = now.astimezone(UTC)
        if (
            observed_at < normalized_now - self._max_age
            or observed_at > normalized_now + self._future_skew
        ):
            raise ValueError("activation acknowledgement freshness window expired")
        if _SIGNATURE_RE.fullmatch(signature) is None:
            raise ValueError("activation acknowledgement signature is invalid")
        try:
            key = self._keys[acknowledgement.agent_key_id]
        except KeyError:
            raise ValueError("activation acknowledgement key is unknown") from None
        signature_bytes = bytes.fromhex(signature)
        try:
            key.verify(signature_bytes, acknowledgement.canonical_bytes())
        except InvalidSignature:
            raise ValueError("activation acknowledgement signature is invalid") from None
        canonical = acknowledgement.canonical_bytes()
        return VerifiedPersonalDevActivationAcknowledgement(
            acknowledgement=acknowledgement,
            payload_sha256=hashlib.sha256(canonical).hexdigest(),
            signature_sha256=hashlib.sha256(signature_bytes).hexdigest(),
        )

    def verify_intent_request(
        self,
        request: PersonalDevActivationIntentRequest,
        *,
        signature: str,
        now: datetime,
    ) -> None:
        """Authenticate a fresh, read-only activation-intent poll."""
        self._verify_fresh_signature(
            key_id=request.agent_key_id,
            observed_at=request.requested_at,
            payload=request.canonical_bytes(),
            signature=signature,
            now=now,
        )

    def _verify_fresh_signature(
        self,
        *,
        key_id: str,
        observed_at: datetime,
        payload: bytes,
        signature: str,
        now: datetime,
    ) -> None:
        if now.tzinfo is None:
            raise ValueError("activation verification time must include a timezone")
        normalized_observed_at = observed_at.astimezone(UTC)
        normalized_now = now.astimezone(UTC)
        if (
            normalized_observed_at < normalized_now - self._max_age
            or normalized_observed_at > normalized_now + self._future_skew
        ):
            raise ValueError("activation acknowledgement freshness window expired")
        if _SIGNATURE_RE.fullmatch(signature) is None:
            raise ValueError("activation acknowledgement signature is invalid")
        try:
            key = self._keys[key_id]
        except KeyError:
            raise ValueError("activation acknowledgement key is unknown") from None
        try:
            key.verify(bytes.fromhex(signature), payload)
        except InvalidSignature:
            raise ValueError("activation acknowledgement signature is invalid") from None


class PersonalDevActivationSigner:
    """Agent-only signing authority backed by rotatable Ed25519 private keys."""

    def __init__(self, *, keys: Mapping[str, bytes]) -> None:
        normalized = dict(keys)
        if not normalized or any(
            _KEY_ID_RE.fullmatch(key_id) is None or not isinstance(key, bytes) or len(key) != 32
            for key_id, key in normalized.items()
        ):
            raise ValueError("activation signer keys are invalid")
        self._keys = MappingProxyType(
            {
                key_id: Ed25519PrivateKey.from_private_bytes(key)
                for key_id, key in normalized.items()
            }
        )

    def sign(self, acknowledgement: PersonalDevActivationAcknowledgement) -> str:
        try:
            key = self._keys[acknowledgement.agent_key_id]
        except KeyError:
            raise ValueError("activation acknowledgement key is unknown") from None
        return key.sign(acknowledgement.canonical_bytes()).hex()

    def sign_intent_request(self, request: PersonalDevActivationIntentRequest) -> str:
        try:
            key = self._keys[request.agent_key_id]
        except KeyError:
            raise ValueError("activation acknowledgement key is unknown") from None
        return key.sign(request.canonical_bytes()).hex()

    def public_key_bytes(self, key_id: str) -> bytes:
        try:
            key = self._keys[key_id]
        except KeyError:
            raise ValueError("activation acknowledgement key is unknown") from None
        return key.public_key().public_bytes(
            encoding=serialization.Encoding.Raw,
            format=serialization.PublicFormat.Raw,
        )


def _load_activation_key(
    key_file: Path,
    *,
    owner_only: bool,
    label: str,
) -> bytes:
    """Read one exact raw Ed25519 key without following or racing links."""
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(key_file, flags)
    except OSError:
        raise RuntimeError(
            f"personal-dev activation {label} must be an available regular file"
        ) from None
    try:
        first = os.fstat(descriptor)
        mode = stat.S_IMODE(first.st_mode)
        unsafe_mode = mode != 0o600 if owner_only else bool(mode & 0o133)
        if (
            not stat.S_ISREG(first.st_mode)
            or first.st_uid != os.geteuid()
            or first.st_nlink != 1
            or unsafe_mode
            or not mode & stat.S_IRUSR
            or first.st_size != 32
        ):
            authority = "owner-only " if owner_only else "read-only "
            raise RuntimeError(
                f"personal-dev activation {label} must be a regular {authority}file"
            )
        key = os.read(descriptor, 33)
        second = os.fstat(descriptor)
    except OSError:
        raise RuntimeError(f"personal-dev activation {label} is unreadable") from None
    finally:
        os.close(descriptor)
    if (
        len(key) != 32
        or (first.st_dev, first.st_ino, first.st_size, first.st_mtime_ns)
        != (second.st_dev, second.st_ino, second.st_size, second.st_mtime_ns)
    ):
        raise RuntimeError(f"personal-dev activation {label} changed while reading")
    return key


def load_personal_dev_activation_verifier(
    key_file: Path,
    *,
    key_id: str,
    max_age_seconds: int,
) -> PersonalDevActivationVerifier:
    """Load one raw public verification key; this grants no signing authority."""
    key = _load_activation_key(
        key_file,
        owner_only=False,
        label="public key",
    )
    return PersonalDevActivationVerifier(
        keys={key_id: key},
        max_age_seconds=max_age_seconds,
    )


def load_personal_dev_activation_signer(
    key_file: Path,
    *,
    key_id: str,
) -> PersonalDevActivationSigner:
    """Load one raw private signing key exclusively in the independent agent."""
    key = _load_activation_key(
        key_file,
        owner_only=True,
        label="private key",
    )
    return PersonalDevActivationSigner(keys={key_id: key})


__all__ = [
    "PersonalDevActivationAcknowledgement",
    "PersonalDevActivationIntent",
    "PersonalDevActivationIntentRequest",
    "PersonalDevActivationSigner",
    "PersonalDevActivationVerifier",
    "VerifiedPersonalDevActivationAcknowledgement",
    "load_personal_dev_activation_signer",
    "load_personal_dev_activation_verifier",
]
