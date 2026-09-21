"""Fixed signing policies over independently read, committed public authority.

Retained publication signatures preserve historical OCI build facts.
This service checks their configured provenance and current execution authority;
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
from uuid import UUID

from sqlalchemy import exists, select, text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from loom.db.schema import TaskImageExecutionGrant, TaskImageExecutionStart
from loom_task_image_authority.execution_grant import (
    EXECUTION_GRANT_DOMAIN,
    MAX_EXECUTION_GRANT_BYTES,
    ExecutionGrantEnvelope,
    LegacyExecutionClaim,
    TaskImageExecutionGrantV2,
    _decode,
    canonical_execution_grant_bytes,
    verify_execution_grant,
    verify_execution_grant_input,
)
from loom_task_image_authority.execution_signing_request import (
    ExecutionSigningRequest,
    decode_execution_signing_request,
)
from loom_task_image_authority.keyset_signing_request import (
    KeysetSigningRequest,
    canonical_keyset_signing_request,
    decode_keyset_signing_request,
)
from loom_task_image_authority.publication_contracts import (
    PublicationUnsignedInput,
    canonical_publication_bytes,
    decode_publication_envelope,
    decode_publication_statement,
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

    Retained publications contain the historical build/containment facts.
    Execution signing checks their provenance against this configured allowlist.
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
        execution_provider: SigningProvider, selections: tuple[PublicationSelection, ...],
        clock: Callable[[], datetime] = _clock, timeout_seconds: float = 5.0,
        keyset_lifetime_seconds: int = 300,
    ) -> None:
        trust_root.__post_init__()
        if (
            type(timeout_seconds) is not float or not math.isfinite(timeout_seconds)
            or not 0 < timeout_seconds <= 10
            or type(keyset_lifetime_seconds) is not int or not 1 <= keyset_lifetime_seconds <= 900
            or type(selections) is not tuple or not 1 <= len(selections) <= 128
            or any(type(item) is not PublicationSelection or item.environment != trust_root.environment for item in selections)
            or len(set(selections)) != len(selections)
            or execution_provider.public_key != trust_root.public_key
        ):
            raise ValueError("invalid fixed signer configuration")
        self._engine, self._root = engine, trust_root
        self._execution = execution_provider
        self._selections, self._clock, self._timeout = selections, clock, timeout_seconds
        self._lifetime = timedelta(seconds=keyset_lifetime_seconds)

    async def _read(self) -> KeysetPreparation:
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
        return prepared

    async def sign_keyset(self, canonical_request: bytes) -> bytes:
        decode_keyset_signing_request(canonical_request)
        async with asyncio.timeout(self._timeout):
            before = await self._read()
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
            after = await self._read()
            if after != before:
                raise ValueError("keyset authority changed during signing")
            verify_publication_keyset(wire, trust_root=self._root, expected_state=before.proposed_state, now=self._clock())
            return wire

    async def _read_execution(
        self, request: ExecutionSigningRequest,
    ) -> tuple[TaskImageExecutionGrantV2, StoredPublicationKeyset, bytes | None]:
        async with self._engine.connect() as connection:
            await connection.execution_options(isolation_level="READ COMMITTED")
            async with AsyncSession(connection, expire_on_commit=False) as session, session.begin():
                await session.execute(text("SET LOCAL search_path=pg_catalog,public,pg_temp"))
                await session.execute(text(
                    "SELECT pg_catalog.set_config('statement_timeout', :bound, true), "
                    "pg_catalog.set_config('idle_in_transaction_session_timeout', :bound, true)"
                ), {"bound": f"{math.ceil(self._timeout * 1000)}ms"})
                prepared = await prepare_keyset(session, trust_root=self._root)
                stored = await read_keyset(session, trust_root=self._root,
                    expected_state=prepared.previous_state, clock=self._clock)
                # Grant and start mutation triggers take the already-held state
                # lock first. SELECT suffices: no worker/Trial access or new
                # UPDATE privileges are needed by the private-key process.
                row = await session.get(TaskImageExecutionGrant, (UUID(request.grant_id), request.revision))
                if row is None or row.revoked_at is not None or row.grant_sha256 != request.grant_sha256:
                    raise ValueError("execution signing preparation is absent, revoked or substituted")
                grant = _decode(row.canonical_grant, TaskImageExecutionGrantV2, MAX_EXECUTION_GRANT_BYTES)
                latest = await session.scalar(select(TaskImageExecutionGrant.revision)
                    .where(TaskImageExecutionGrant.claim_id == row.claim_id)
                    .order_by(TaskImageExecutionGrant.revision.desc()).limit(1))
                consumed = await session.scalar(select(exists().where(TaskImageExecutionStart.claim_id == row.claim_id)))
                if (
                    not isinstance(grant.claim, LegacyExecutionClaim) or consumed
                    or latest != request.revision or grant.revision != request.revision
                    or grant.grant_id != request.grant_id
                    or grant.claim.claim_id != str(row.claim_id)
                    or grant.claim.trial_id != str(row.trial_id) or grant.claim.worker_id != str(row.worker_id)
                    or hashlib.sha256(row.canonical_grant).hexdigest() != request.grant_sha256
                    or grant.keyset_version != row.keyset_version
                    or grant.keyset_version != stored.state.keyset_version
                    or grant.revocation_epoch != stored.state.revocation_epoch
                    or grant.keyset_sha256 != stored.snapshot_sha256
                ):
                    raise ValueError("execution signing preparation is stale or inconsistent")
                retained_wire = row.canonical_envelope
        verify_execution_grant_input(
            canonical_grant=canonical_execution_grant_bytes(grant), plan_wire=request.frozen_plan.encode(),
            publication_wires=tuple(item.encode() for item in request.publications), keyset_wire=stored.wire,
            trust_root=self._root, expected_claim=grant.claim, expected_purpose=grant.purpose,
            expected_shadow_campaign_id=grant.shadow_campaign_id, now=self._clock(),
        )
        for publication in request.publications:
            unsigned = decode_publication_statement(decode_publication_envelope(publication.encode()).canonical_statement.encode()).unsigned_input()
            if PublicationSelection.from_unsigned(unsigned) not in self._selections:
                raise ValueError("execution publication provenance selection is not configured")
        return grant, stored, retained_wire

    async def sign_execution(self, canonical_request: bytes) -> bytes:
        request = decode_execution_signing_request(canonical_request)
        async with asyncio.timeout(self._timeout):
            before, stored, retained = await self._read_execution(request)
            canonical = canonical_execution_grant_bytes(before)
            if retained is None:
                signature = await self._execution.sign(EXECUTION_GRANT_DOMAIN + canonical)
                wire = canonical_execution_grant_bytes(ExecutionGrantEnvelope(
                    canonical_grant=canonical.decode(), grant_sha256=request.grant_sha256,
                    key_id=self._root.key_id, algorithm="Ed25519", signature=_b64(signature),
                ))
            else:
                wire = retained
            after, current, saved = await self._read_execution(request)
            if before != after or stored != current or (saved is not None and saved != wire):
                raise ValueError("execution authority changed during signing")
            verify_execution_grant(
                wire=wire, plan_wire=request.frozen_plan.encode(),
                publication_wires=tuple(item.encode() for item in request.publications), keyset_wire=stored.wire,
                trust_root=self._root, expected_claim=before.claim, expected_purpose=before.purpose,
                expected_shadow_campaign_id=before.shadow_campaign_id, now=self._clock(),
            )
            return wire
