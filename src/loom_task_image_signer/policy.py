"""Two fixed signing policies over independently read, committed public authority.

The authenticated publication verifier remains responsible for OCI build facts.
This service checks configured provenance selection and current signing authority;
it does not claim to fetch registry bytes or grant complete-set readiness.
"""

from __future__ import annotations

import asyncio
import base64
import hashlib
import math
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Protocol

from pydantic import TypeAdapter
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from loom_task_image_authority.contracts import Identifier
from loom_task_image_authority.keyset_signing_request import (
    KeysetSigningRequest,
    canonical_keyset_signing_request,
    decode_keyset_signing_request,
)
from loom_task_image_authority.publication_contracts import (
    PUBLICATION_DOMAIN,
    PublicationEnvelope,
    PublicationUnsignedInput,
    canonical_publication_bytes,
    decode_unsigned_input,
)
from loom_task_image_authority.publication_keyset import (
    PUBLICATION_KEYSET_DOMAIN,
    ExecutionGrantTrustRoot,
    PublicationKeysetEnvelope,
    PublicationVerificationKeysetV1,
    canonical_keyset_bytes,
    verify_publication_keyset,
)
from loom_task_image_authority.publication_keyset_store import (
    KeysetPreparation,
    StoredPublicationKeyset,
    prepare_keyset,
    read_keyset,
)
from loom_task_image_authority.publication_signing import (
    DistributedKeysetSnapshot,
    PublicationKeyRecord,
    prepare_publication_statement,
    verify_publication_reply,
)


class SigningProvider(Protocol):
    """Host-owned or KMS-backed fixed handle; never selected by an HTTP request."""

    @property
    def public_key(self) -> bytes: ...

    async def sign(self, preimage: bytes) -> bytes: ...


def _clock() -> datetime:
    return datetime.now(UTC).replace(microsecond=0)


def _stamp(value: datetime) -> str:
    if type(value) is not datetime or value.utcoffset() != timedelta(0) or value.microsecond:
        raise ValueError("signer clock must be whole UTC seconds")
    return value.strftime("%Y-%m-%dT%H:%M:%SZ")


def _b64(value: bytes) -> str:
    if type(value) is not bytes or len(value) != 64:
        raise ValueError("invalid provider signature")
    return base64.urlsafe_b64encode(value).rstrip(b"=").decode("ascii")


@dataclass(frozen=True)
class PublicationSelection:
    """Stable operator selection, not per-allocation attestation or job identity.

    The authenticated verifier supplies dynamic build/containment facts. Those
    facts remain signed and checked by the final publication authority.
    """

    environment: str
    registry_origin: str
    pool_id: str
    platform: str
    slurm_cluster_id: str
    build_policy_sha256: str
    builder_release_sha256: str
    supervisor_executable_sha256: str
    purpose: str
    shadow_campaign_id: str | None

    @classmethod
    def from_unsigned(cls, unsigned: PublicationUnsignedInput) -> PublicationSelection:
        validated = decode_unsigned_input(canonical_publication_bytes(unsigned))
        return cls(**{name: getattr(validated, name) for name in cls.__dataclass_fields__})


def _request(preparation: KeysetPreparation) -> bytes:
    return canonical_keyset_signing_request(KeysetSigningRequest.model_validate(dict(
        schema="loom.task-image-keyset-signing-request/v1", environment=preparation.environment,
        previous_keyset_version=preparation.previous_state.keyset_version,
        proposed_keyset_version=preparation.proposed_state.keyset_version,
        revocation_epoch=preparation.previous_state.revocation_epoch,
        keys=preparation.keys,
    )))


class SignerPolicy:
    """Owned deadline covers DB checkout, two short transactions and provider I/O.

    A caller uses an authenticated operation-scoped transport, never passes key
    records or distribution snapshots. No database mutation is performed here;
    row locks still require narrow column UPDATE privileges. Final publication
    readiness and keyset persistence remain separately fenced caller operations.
    """

    def __init__(
        self, engine: AsyncEngine, *, trust_root: ExecutionGrantTrustRoot,
        publication_key_id: str, publication_provider: SigningProvider,
        execution_provider: SigningProvider, selections: tuple[PublicationSelection, ...],
        clock: Callable[[], datetime] = _clock, timeout_seconds: float = 5.0,
        keyset_lifetime_seconds: int = 300,
    ) -> None:
        trust_root.__post_init__()
        TypeAdapter(Identifier).validate_python(publication_key_id, strict=True)
        if (
            type(timeout_seconds) is not float or not math.isfinite(timeout_seconds)
            or not 0 < timeout_seconds <= 10
            or type(keyset_lifetime_seconds) is not int or not 1 <= keyset_lifetime_seconds <= 900
            or type(selections) is not tuple or not 1 <= len(selections) <= 128
            or any(type(item) is not PublicationSelection or item.environment != trust_root.environment for item in selections)
            or len(set(selections)) != len(selections)
            or type(publication_provider.public_key) is not bytes or len(publication_provider.public_key) != 32
            or execution_provider.public_key != trust_root.public_key
            or publication_provider.public_key == execution_provider.public_key
            or publication_key_id == trust_root.key_id
        ):
            raise ValueError("invalid fixed signer configuration")
        self._engine, self._root, self._key_id = engine, trust_root, publication_key_id
        self._publication, self._execution = publication_provider, execution_provider
        self._selections, self._clock, self._timeout = selections, clock, timeout_seconds
        self._lifetime = timedelta(seconds=keyset_lifetime_seconds)

    async def _read(self, *, publication: bool) -> tuple[KeysetPreparation, StoredPublicationKeyset | None]:
        async with self._engine.connect() as connection:
            await connection.execution_options(isolation_level="READ COMMITTED")
            async with AsyncSession(connection, expire_on_commit=False) as session, session.begin():
                # Explicit pg_temp last prevents inherited search_path or
                # connection-local temporary tables shadowing public authority.
                await session.execute(text("SET LOCAL search_path=pg_catalog,public,pg_temp"))
                await session.execute(text(
                    "SELECT pg_catalog.set_config('statement_timeout', :bound, true), "
                    "pg_catalog.set_config('idle_in_transaction_session_timeout', :bound, true)"
                ), {"bound": f"{math.ceil(self._timeout * 1000)}ms"})
                prepared = await prepare_keyset(session, trust_root=self._root)
                stored = await read_keyset(
                    session, trust_root=self._root, expected_state=prepared.previous_state, clock=self._clock,
                ) if publication else None
        if stored is not None:
            verify_publication_keyset(stored.wire, trust_root=self._root, expected_state=stored.state, now=self._clock())
        return prepared, stored

    async def sign_keyset(self, canonical_request: bytes) -> bytes:
        decode_keyset_signing_request(canonical_request)
        async with asyncio.timeout(self._timeout):
            before, _ = await self._read(publication=False)
            if _request(before) != canonical_request:
                raise ValueError("keyset request differs from current authority")
            now = self._clock()
            issued = _stamp(now)
            expiry = min(now + self._lifetime, self._root.expires_at)
            if not self._root.activated_at <= now < expiry:
                raise ValueError("execution root is not currently eligible")
            keyset = PublicationVerificationKeysetV1.model_validate(dict(
                schema="loom.task-image-publication-keyset/v1", environment=self._root.environment,
                keyset_version=before.proposed_state.keyset_version,
                revocation_epoch=before.proposed_state.revocation_epoch,
                issued_at=issued, expires_at=_stamp(expiry), keys=before.keys,
            ))
            canonical = canonical_keyset_bytes(keyset)
            signature = await self._execution.sign(PUBLICATION_KEYSET_DOMAIN + canonical)
            envelope = PublicationKeysetEnvelope(
                canonical_keyset=canonical.decode(), keyset_sha256=hashlib.sha256(canonical).hexdigest(),
                key_id=self._root.key_id, algorithm="Ed25519", signature=_b64(signature),
            )
            wire = canonical_keyset_bytes(envelope)
            after, _ = await self._read(publication=False)
            if after != before:
                raise ValueError("keyset authority changed during signing")
            verify_publication_keyset(wire, trust_root=self._root, expected_state=before.proposed_state, now=self._clock())
            return wire

    def _key(self, prepared: KeysetPreparation) -> PublicationKeyRecord:
        key = next((member.record() for member in prepared.keys if member.key_id == self._key_id), None)
        if key is None or key.public_key != self._publication.public_key:
            raise ValueError("configured publication key is not registered")
        return key

    async def sign_publication(self, canonical_unsigned_input: bytes) -> bytes:
        unsigned = decode_unsigned_input(canonical_unsigned_input)
        if PublicationSelection.from_unsigned(unsigned) not in self._selections:
            raise ValueError("publication provenance selection is not configured")
        async with asyncio.timeout(self._timeout):
            before, stored = await self._read(publication=True)
            assert stored is not None
            key = self._key(before)
            distribution = DistributedKeysetSnapshot(
                keyset_version=stored.state.keyset_version, revocation_epoch=stored.state.revocation_epoch,
                key_ids=tuple(member.key_id for member in stored.keys),
                issued_at=stored.issued_at, expires_at=stored.expires_at,
            )
            now = self._clock()
            statement = prepare_publication_statement(
                unsigned, key=key, state=stored.state, distribution=distribution, signer_now=now,
            )
            canonical = canonical_publication_bytes(statement)
            signature = await self._publication.sign(PUBLICATION_DOMAIN + canonical)
            wire = canonical_publication_bytes(PublicationEnvelope(
                canonical_statement=canonical.decode(), statement_sha256=hashlib.sha256(canonical).hexdigest(),
                key_id=key.key_id, algorithm="Ed25519", signature=_b64(signature),
            ))
            after, retained = await self._read(publication=True)
            if after != before or retained != stored:
                raise ValueError("publication authority changed during signing")
            verify_publication_reply(
                wire, unsigned=unsigned, key=key, state=stored.state, distribution=distribution,
                requested_at=now, received_at=self._clock(),
            )
            return wire
