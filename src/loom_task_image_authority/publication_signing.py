"""Retained publication signature verification and public key lifecycle records.

Historical signatures prove their recorded provenance, not present execution
readiness. Execution verifies the current keyset and grant separately.
"""

from __future__ import annotations

import base64
import hashlib
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Literal

from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey
from pydantic import TypeAdapter

from loom_task_image_authority.contracts import Identifier
from loom_task_image_authority.publication_contracts import (
    MAX_SAFE_INTEGER,
    PUBLICATION_DOMAIN,
    PublicationEnvelope,
    PublicationStatement,
    decode_publication_envelope,
    decode_publication_statement,
)

MAX_SIGNER_CLOCK_SKEW = timedelta(seconds=5)
MAX_DISTRIBUTION_SNAPSHOT_LIFETIME = timedelta(minutes=15)


def _time(value: datetime) -> datetime:
    if type(value) is not datetime or value.utcoffset() != timedelta(0) or value.microsecond:
        raise ValueError("publication time must be a whole UTC second")
    return value


def _counter(value: int, minimum: int = 0) -> None:
    if type(value) is not int or not minimum <= value <= MAX_SAFE_INTEGER:
        raise ValueError("invalid publication counter")


@dataclass(frozen=True)
class PublicationKeyRecord:
    key_id: str
    public_key: bytes
    activated_at: datetime
    status: Literal["active", "verify_only", "revoked"] = "active"
    retired_at: datetime | None = None
    revoked_at: datetime | None = None

    def __post_init__(self) -> None:
        TypeAdapter(Identifier).validate_python(self.key_id, strict=True)
        if type(self.public_key) is not bytes or len(self.public_key) != 32:
            raise ValueError("invalid publication public key")
        _time(self.activated_at)
        for value in (self.retired_at, self.revoked_at):
            if value is not None and _time(value) < self.activated_at:
                raise ValueError("invalid publication key interval")
        if (
            self.status not in {"active", "verify_only", "revoked"}
            or (
                self.status == "active"
                and (self.retired_at is not None or self.revoked_at is not None)
            )
            or (
                self.status == "verify_only"
                and (self.retired_at is None or self.revoked_at is not None)
            )
            or (self.status == "revoked" and self.revoked_at is None)
            or (
                self.retired_at is not None
                and self.revoked_at is not None
                and self.revoked_at < self.retired_at
            )
        ):
            raise ValueError("invalid publication key lifecycle")


@dataclass(frozen=True)
class PublicationState:
    revocation_epoch: int = 0
    keyset_version: int = 0

    def __post_init__(self) -> None:
        _counter(self.revocation_epoch)
        _counter(self.keyset_version)


@dataclass(frozen=True)
class DistributedKeysetSnapshot:
    """Trusted adapter result, NOT untrusted request data or proof by itself.

    A future authenticated signed-keyset adapter must prove distribution and
    membership, pinning these fields to durable state. No default adapter exists.
    Snapshot times cannot outlive the authenticated evidence or be refreshed by
    re-stamping old evidence; refresh requires current valid authority.
    """

    keyset_version: int
    revocation_epoch: int
    key_ids: tuple[str, ...]
    issued_at: datetime
    expires_at: datetime

    def __post_init__(self) -> None:
        _counter(self.keyset_version, 1)
        _counter(self.revocation_epoch)
        if (
            type(self.key_ids) is not tuple
            or len(self.key_ids) > 128
            or len(set(self.key_ids)) != len(self.key_ids)
            or _time(self.expires_at) <= _time(self.issued_at)
            or self.expires_at - self.issued_at > MAX_DISTRIBUTION_SNAPSHOT_LIFETIME
        ):
            raise ValueError("invalid distributed publication keyset")
        for key_id in self.key_ids:
            TypeAdapter(Identifier).validate_python(key_id, strict=True)


@dataclass(frozen=True)
class VerifiedPublication:
    envelope: PublicationEnvelope
    statement: PublicationStatement


def verify_historical_publication(
    reply: bytes, *, key: PublicationKeyRecord
) -> VerifiedPublication:
    """Cryptographic history, including retained revoked keys; grants NO readiness."""
    envelope = decode_publication_envelope(reply)
    canonical = envelope.canonical_statement.encode("utf-8")
    statement = decode_publication_statement(canonical)
    issued = datetime.strptime(statement.issued_at, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=UTC)
    if (
        envelope.key_id != key.key_id
        or statement.signing_key_id != key.key_id
        or envelope.statement_sha256 != hashlib.sha256(canonical).hexdigest()
        or issued < key.activated_at
        or (key.retired_at is not None and issued >= key.retired_at)
        or (key.revoked_at is not None and issued >= key.revoked_at)
    ):
        raise ValueError("publication key, digest or interval mismatch")
    signature = base64.urlsafe_b64decode(envelope.signature + "==")
    if base64.urlsafe_b64encode(signature).rstrip(b"=").decode("ascii") != envelope.signature:
        raise ValueError("noncanonical publication signature")
    try:
        Ed25519PublicKey.from_public_bytes(key.public_key).verify(
            signature, PUBLICATION_DOMAIN + canonical
        )
    except InvalidSignature:
        raise ValueError("invalid publication signature") from None
    return VerifiedPublication(envelope, statement)
