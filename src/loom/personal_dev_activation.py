"""Signed two-phase activation acknowledgements for personal environments."""

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
from uuid import UUID

from loom.dev_instance import InvalidDevInstanceNameError, validate_name

_DIGEST_RE = re.compile(r"[0-9a-f]{64}")
_KEY_ID_RE = re.compile(r"[a-z][a-z0-9._-]{0,63}")


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
        return json.dumps(
            payload,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
        ).encode("ascii")


@dataclass(frozen=True, slots=True)
class VerifiedPersonalDevActivationAcknowledgement:
    acknowledgement: PersonalDevActivationAcknowledgement
    payload_sha256: str
    signature_sha256: str


class PersonalDevActivationVerifier:
    """Authenticate bounded agent evidence with rotatable operator keys."""

    def __init__(
        self,
        *,
        keys: Mapping[str, bytes],
        max_age_seconds: int = 300,
        future_skew_seconds: int = 30,
    ) -> None:
        normalized = dict(keys)
        if not normalized or any(
            _KEY_ID_RE.fullmatch(key_id) is None or not isinstance(key, bytes) or len(key) < 32
            for key_id, key in normalized.items()
        ):
            raise ValueError("activation verifier keys are invalid")
        if type(max_age_seconds) is not int or max_age_seconds <= 0:
            raise ValueError("activation acknowledgement max age must be positive")
        if type(future_skew_seconds) is not int or future_skew_seconds < 0:
            raise ValueError("activation acknowledgement future skew must be nonnegative")
        self._keys = MappingProxyType(normalized)
        self._max_age = timedelta(seconds=max_age_seconds)
        self._future_skew = timedelta(seconds=future_skew_seconds)

    def sign(self, acknowledgement: PersonalDevActivationAcknowledgement) -> str:
        try:
            key = self._keys[acknowledgement.agent_key_id]
        except KeyError:
            raise ValueError("activation acknowledgement key is unknown") from None
        return hmac.new(key, acknowledgement.canonical_bytes(), hashlib.sha256).hexdigest()

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
        if _DIGEST_RE.fullmatch(signature) is None:
            raise ValueError("activation acknowledgement signature is invalid")
        expected = self.sign(acknowledgement)
        if not hmac.compare_digest(expected, signature):
            raise ValueError("activation acknowledgement signature is invalid")
        canonical = acknowledgement.canonical_bytes()
        return VerifiedPersonalDevActivationAcknowledgement(
            acknowledgement=acknowledgement,
            payload_sha256=hashlib.sha256(canonical).hexdigest(),
            signature_sha256=hashlib.sha256(signature.encode("ascii")).hexdigest(),
        )


def load_personal_dev_activation_verifier(
    key_file: Path,
    *,
    key_id: str,
    max_age_seconds: int,
) -> PersonalDevActivationVerifier:
    """Load one bounded owner-only key without exposing its contents."""
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(key_file, flags)
    except OSError:
        raise RuntimeError(
            "personal-dev activation key must be an available regular owner-only file"
        ) from None
    try:
        first = os.fstat(descriptor)
        mode = stat.S_IMODE(first.st_mode)
        if (
            not stat.S_ISREG(first.st_mode)
            or first.st_uid != os.geteuid()
            or first.st_nlink != 1
            or mode & ~0o600
            or not mode & stat.S_IRUSR
        ):
            raise RuntimeError("personal-dev activation key must be a regular owner-only file")
        if first.st_size > 4096:
            raise RuntimeError("personal-dev activation key file is oversized")
        key = os.read(descriptor, 4097)
        second = os.fstat(descriptor)
    except OSError:
        raise RuntimeError("personal-dev activation key file is unreadable") from None
    finally:
        os.close(descriptor)
    if (
        len(key) > 4096
        or first.st_size != len(key)
        or (first.st_dev, first.st_ino, first.st_size, first.st_mtime_ns)
        != (second.st_dev, second.st_ino, second.st_size, second.st_mtime_ns)
    ):
        raise RuntimeError("personal-dev activation key file changed while reading")
    key = key.rstrip(b"\r\n")
    if len(key) < 32:
        raise RuntimeError("personal-dev activation key material is too short")
    return PersonalDevActivationVerifier(
        keys={key_id: key},
        max_age_seconds=max_age_seconds,
    )


__all__ = [
    "PersonalDevActivationAcknowledgement",
    "PersonalDevActivationVerifier",
    "VerifiedPersonalDevActivationAcknowledgement",
    "load_personal_dev_activation_verifier",
]
