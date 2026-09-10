"""Bounded signed keysets and publication verification, not execution authority.

The execution-grant trust root comes from a pinned trusted worker release, never
from the keyset or registry. Expected state and snapshot identity must come from
the authenticated execution grant (or current durable publication state).
Verification here neither proves distribution nor consumes one-use online start
authority. No production signer or distribution adapter is installed here.
"""

from __future__ import annotations

import base64
import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Annotated, Literal, TypeVar

import rfc8785
from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey
from pydantic import Field, TypeAdapter, model_validator

from loom_task_image_authority.contracts import Digest, Identifier
from loom_task_image_authority.publication_contracts import (
    PublicationTimestamp,
    PublicationUnsignedInput,
    SafeNonnegativeInteger,
    SafePositiveInteger,
    _ClosedPublicationModel,
    _reject_constant,
    _unique_object,
    canonical_publication_bytes,
    decode_publication_envelope,
    decode_unsigned_input,
)
from loom_task_image_authority.publication_signing import (
    MAX_DISTRIBUTION_SNAPSHOT_LIFETIME,
    MAX_SIGNER_CLOCK_SKEW,
    PublicationKeyRecord,
    PublicationState,
    VerifiedPublication,
    _time,
    verify_historical_publication,
)

PUBLICATION_KEYSET_DOMAIN = b"loom-task-image-publication-keyset-v1\x00"
MAX_KEYSET_BYTES = 64 * 1024
MAX_KEYSET_ENVELOPE_BYTES = 128 * 1024


def _instant(value: str) -> datetime:
    return datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=UTC)


def _base64url(value: str, expected_bytes: int) -> bytes:
    raw = base64.urlsafe_b64decode(value + "=" * (-len(value) % 4))
    if (
        len(raw) != expected_bytes
        or base64.urlsafe_b64encode(raw).rstrip(b"=").decode("ascii") != value
    ):
        raise ValueError("invalid canonical keyset base64url")
    return raw


@dataclass(frozen=True)
class ExecutionGrantTrustRoot:
    """Trusted release pin; these public bytes never come from a response."""

    key_id: str
    environment: str
    public_key: bytes
    activated_at: datetime
    expires_at: datetime

    def __post_init__(self) -> None:
        for value in (self.key_id, self.environment):
            TypeAdapter(Identifier).validate_python(value, strict=True)
        if (
            type(self.public_key) is not bytes or len(self.public_key) != 32
            or _time(self.expires_at) <= _time(self.activated_at)
        ):
            raise ValueError("invalid execution-grant trust root")


class PublicationVerificationKey(_ClosedPublicationModel):
    key_id: Identifier
    public_key: Annotated[str, Field(pattern=r"^[A-Za-z0-9_-]{43}$")]
    status: Literal["active", "verify_only", "revoked"]
    activated_at: PublicationTimestamp
    retired_at: PublicationTimestamp | None = None
    revoked_at: PublicationTimestamp | None = None

    def record(self) -> PublicationKeyRecord:
        return PublicationKeyRecord(
            key_id=self.key_id, public_key=_base64url(self.public_key, 32), status=self.status,
            activated_at=_instant(self.activated_at),
            retired_at=_instant(self.retired_at) if self.retired_at is not None else None,
            revoked_at=_instant(self.revoked_at) if self.revoked_at is not None else None,
        )

    @model_validator(mode="after")
    def _lifecycle(self) -> PublicationVerificationKey:
        self.record()
        return self


class PublicationVerificationKeysetV1(_ClosedPublicationModel):
    schema_name: Literal["loom.task-image-publication-keyset/v1"] = Field(alias="schema")
    environment: Identifier
    keyset_version: SafePositiveInteger
    revocation_epoch: SafeNonnegativeInteger
    issued_at: PublicationTimestamp
    expires_at: PublicationTimestamp
    keys: Annotated[tuple[PublicationVerificationKey, ...], Field(min_length=1, max_length=128)]

    @model_validator(mode="after")
    def _bindings(self) -> PublicationVerificationKeysetV1:
        names = tuple(key.key_id for key in self.keys)
        if (
            names != tuple(sorted(set(names)))
            or len({key.public_key for key in self.keys}) != len(self.keys)
            or not _instant(self.issued_at) < _instant(self.expires_at)
            or _instant(self.expires_at) - _instant(self.issued_at) > MAX_DISTRIBUTION_SNAPSHOT_LIFETIME
        ):
            raise ValueError("invalid publication keyset bindings or lifetime")
        return self


class PublicationKeysetEnvelope(_ClosedPublicationModel):
    canonical_keyset: Annotated[str, Field(min_length=1, max_length=MAX_KEYSET_BYTES)]
    keyset_sha256: Digest
    key_id: Identifier
    algorithm: Literal["Ed25519"]
    signature: Annotated[str, Field(pattern=r"^[A-Za-z0-9_-]{86}$")]


_KeysetModel = TypeVar("_KeysetModel", PublicationVerificationKeysetV1, PublicationKeysetEnvelope)


def canonical_keyset_bytes(model: PublicationVerificationKeysetV1 | PublicationKeysetEnvelope) -> bytes:
    if type(model) not in {PublicationVerificationKeysetV1, PublicationKeysetEnvelope}:
        raise ValueError("invalid keyset wire model")
    checked = type(model).model_validate(model.model_dump(mode="json", by_alias=True, exclude_none=True))
    encoded = rfc8785.dumps(checked.model_dump(mode="json", by_alias=True, exclude_none=True))
    maximum = MAX_KEYSET_ENVELOPE_BYTES if isinstance(model, PublicationKeysetEnvelope) else MAX_KEYSET_BYTES
    if len(encoded) > maximum:
        raise ValueError("publication keyset exceeds byte ceiling")
    return encoded


def _decode_keyset(wire: bytes, model: type[_KeysetModel], maximum: int) -> _KeysetModel:
    if type(wire) is not bytes or not 0 < len(wire) <= maximum:
        raise ValueError("publication keyset wire exceeds ceiling")
    try:
        parsed = model.model_validate(json.loads(
            wire.decode("utf-8"), object_pairs_hook=_unique_object, parse_constant=_reject_constant,
        ))
        if canonical_keyset_bytes(parsed) != wire:
            raise ValueError("noncanonical keyset")
        return parsed
    except (ValueError, UnicodeError, RecursionError, OverflowError):
        raise ValueError("invalid publication keyset wire") from None


def decode_publication_keyset(wire: bytes) -> PublicationVerificationKeysetV1:
    return _decode_keyset(wire, PublicationVerificationKeysetV1, MAX_KEYSET_BYTES)


def decode_keyset_envelope(wire: bytes) -> PublicationKeysetEnvelope:
    return _decode_keyset(wire, PublicationKeysetEnvelope, MAX_KEYSET_ENVELOPE_BYTES)


@dataclass(frozen=True)
class VerifiedPublicationKeyset:
    envelope: PublicationKeysetEnvelope
    keyset: PublicationVerificationKeysetV1
    snapshot_sha256: str


def verify_publication_keyset(
    wire: bytes, *, trust_root: ExecutionGrantTrustRoot,
    expected_state: PublicationState, now: datetime,
) -> VerifiedPublicationKeyset:
    """Verify exact expected counters, never turn signed bytes into distribution."""
    if type(trust_root) is not ExecutionGrantTrustRoot or type(expected_state) is not PublicationState:
        raise ValueError("publication keyset requires trusted root and state")
    trust_root.__post_init__()
    expected_state.__post_init__()
    _time(now)
    envelope = decode_keyset_envelope(wire)
    canonical = envelope.canonical_keyset.encode("utf-8")
    keyset = decode_publication_keyset(canonical)
    issued, expires = _instant(keyset.issued_at), _instant(keyset.expires_at)
    if (
        envelope.key_id != trust_root.key_id
        or envelope.keyset_sha256 != hashlib.sha256(canonical).hexdigest()
        or keyset.environment != trust_root.environment
        or keyset.keyset_version != expected_state.keyset_version
        or keyset.revocation_epoch != expected_state.revocation_epoch
        or not trust_root.activated_at <= issued <= now < expires <= trust_root.expires_at
        or any(key.record().public_key == trust_root.public_key for key in keyset.keys)
    ):
        raise ValueError("publication keyset authority binding mismatch")
    try:
        Ed25519PublicKey.from_public_bytes(trust_root.public_key).verify(
            _base64url(envelope.signature, 64), PUBLICATION_KEYSET_DOMAIN + canonical,
        )
    except (InvalidSignature, ValueError):
        raise ValueError("invalid publication keyset signature") from None
    return VerifiedPublicationKeyset(envelope, keyset, hashlib.sha256(wire).hexdigest())


def verify_keyset_publication(
    publication_wire: bytes, *, keyset_wire: bytes, trust_root: ExecutionGrantTrustRoot,
    expected_state: PublicationState, expected_snapshot_sha256: str,
    expected_unsigned: PublicationUnsignedInput, now: datetime,
) -> VerifiedPublication:
    """Verify one exact grant component; not complete-set or one-use start authority.

    Rotation may advance keyset/epoch after publication. Retained nonrevoked keys
    can verify those older statements, but no publication may name future state.
    Every call authenticates the raw keyset, not a caller-constructed result object.
    """
    verified = verify_publication_keyset(keyset_wire, trust_root=trust_root, expected_state=expected_state, now=now)
    TypeAdapter(Digest).validate_python(expected_snapshot_sha256, strict=True)
    unsigned = decode_unsigned_input(canonical_publication_bytes(expected_unsigned))
    if (
        expected_snapshot_sha256 != verified.snapshot_sha256
        or unsigned.environment != verified.keyset.environment
    ):
        raise ValueError("publication keyset grant binding mismatch")
    envelope = decode_publication_envelope(publication_wire)
    selected = next((key for key in verified.keyset.keys if key.key_id == envelope.key_id), None)
    if selected is None or selected.status == "revoked":
        raise ValueError("publication verification key is unavailable")
    result = verify_historical_publication(publication_wire, key=selected.record())
    if (
        result.statement.distributed_keyset_version > verified.keyset.keyset_version
        or result.statement.revocation_epoch > verified.keyset.revocation_epoch
        or _instant(result.statement.issued_at) > now + MAX_SIGNER_CLOCK_SKEW
        or canonical_publication_bytes(result.statement.unsigned_input()) != canonical_publication_bytes(unsigned)
    ):
        raise ValueError("publication keyset statement binding mismatch")
    return result
