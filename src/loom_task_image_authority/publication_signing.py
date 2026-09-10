"""Dedicated publication signer boundary; no production private keys or DB I/O.

The dedicated mutual-TLS client lives in publication_transport; production
transport and signed-keyset distribution remain deliberately uncomposed.
Evidence returned here is NOT readiness: the final transaction must lock the
durable publication singleton first, then keys, grant, projection, current
session, materialization, attempt, candidate/job and (later) trial-start rows.
Never hold those locks across this module's signer call.
"""

from __future__ import annotations

import asyncio
import base64
import hashlib
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Literal, Protocol

from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey
from pydantic import TypeAdapter

from loom_task_image_authority.contracts import Identifier
from loom_task_image_authority.publication_contracts import (
    MAX_SAFE_INTEGER,
    MAX_SIGNER_REPLY_BYTES,
    PUBLICATION_DOMAIN,
    PublicationEnvelope,
    PublicationStatement,
    PublicationUnsignedInput,
    canonical_publication_bytes,
    decode_publication_envelope,
    decode_publication_statement,
    decode_unsigned_input,
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


def _eligible(
    key: PublicationKeyRecord,
    state: PublicationState,
    distribution: DistributedKeysetSnapshot | None,
    now: datetime,
) -> None:
    _time(now)
    if (
        key.status != "active"
        or now < key.activated_at
        or distribution is None
        or not distribution.issued_at <= now < distribution.expires_at
        or key.key_id not in distribution.key_ids
        or distribution.keyset_version != state.keyset_version
        or distribution.revocation_epoch != state.revocation_epoch
    ):
        raise ValueError("publication signing eligibility is closed")


def prepare_publication_statement(
    unsigned: PublicationUnsignedInput,
    *,
    key: PublicationKeyRecord,
    state: PublicationState,
    signer_now: datetime,
    distribution: DistributedKeysetSnapshot | None = None,
) -> PublicationStatement:
    """Dedicated service policy: stamp its clock after validating unsigned input.

    This prepares only this schema/domain; actual signing belongs to a dedicated
    host service or KMS/HSM, never an in-process production private key provider.
    """
    validated = decode_unsigned_input(canonical_publication_bytes(unsigned))
    _eligible(key, state, distribution, signer_now)
    return PublicationStatement.model_validate(
        {
            **validated.model_dump(mode="json", by_alias=True, exclude_none=True),
            "issued_at": signer_now.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "signing_key_id": key.key_id,
            "distributed_keyset_version": state.keyset_version,
            "revocation_epoch": state.revocation_epoch,
        }
    )


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


def verify_publication_reply(
    reply: bytes,
    *,
    unsigned: PublicationUnsignedInput,
    key: PublicationKeyRecord,
    state: PublicationState,
    requested_at: datetime,
    received_at: datetime,
    distribution: DistributedKeysetSnapshot | None = None,
) -> VerifiedPublication:
    _time(requested_at)
    _eligible(key, state, distribution, received_at)
    if received_at < requested_at:
        raise ValueError("publication request clock regressed")
    result = verify_historical_publication(reply, key=key)
    statement = result.statement
    issued = datetime.strptime(statement.issued_at, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=UTC)
    _eligible(key, state, distribution, issued)
    if (
        not requested_at - MAX_SIGNER_CLOCK_SKEW <= issued <= received_at + MAX_SIGNER_CLOCK_SKEW
        or statement.distributed_keyset_version != state.keyset_version
        or statement.revocation_epoch != state.revocation_epoch
        or canonical_publication_bytes(statement.unsigned_input())
        != canonical_publication_bytes(unsigned)
    ):
        raise ValueError("publication signer reply binding mismatch")
    return result


class PublicationSigner(Protocol):
    """Authenticated dedicated service transport; never arbitrary-byte signing.

    Implementations must enforce maximum_reply_bytes while reading, not after
    buffering an unbounded response, and close I/O on timeout or cancellation.
    Production composition must supply and verify this transport explicitly.
    """

    async def sign_publication(
        self, canonical_unsigned_input: bytes, *, maximum_reply_bytes: int
    ) -> bytes: ...


async def request_publication_signature(
    signer: PublicationSigner,
    unsigned: PublicationUnsignedInput,
    *,
    key: PublicationKeyRecord,
    state: PublicationState,
    distribution: DistributedKeysetSnapshot | None = None,
    clock: Callable[[], datetime] = lambda: datetime.now(UTC).replace(microsecond=0),
    timeout_seconds: float = 5,
) -> VerifiedPublication:
    if isinstance(timeout_seconds, bool) or not 0 < timeout_seconds <= 10:
        raise ValueError("invalid publication signer deadline")
    canonical = canonical_publication_bytes(
        decode_unsigned_input(canonical_publication_bytes(unsigned))
    )
    requested_at = clock()
    _eligible(key, state, distribution, requested_at)
    async with asyncio.timeout(timeout_seconds):
        reply = await signer.sign_publication(canonical, maximum_reply_bytes=MAX_SIGNER_REPLY_BYTES)
    return verify_publication_reply(
        reply,
        unsigned=unsigned,
        key=key,
        state=state,
        distribution=distribution,
        requested_at=requested_at,
        received_at=clock(),
    )
