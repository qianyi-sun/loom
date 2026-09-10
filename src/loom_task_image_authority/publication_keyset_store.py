"""Short state-first keyset transactions; no signing I/O or runtime activation.

Prepare and COMMIT, obtain the external signature without database locks, then
finalize in a new transaction. Callers must roll back on failure. Retained bytes
are distribution input, not proof of worker capability or one-use start authority.
"""

from __future__ import annotations

import asyncio
import base64
import hashlib
import math
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime

from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from loom.db.schema import (
    TaskImagePublicationKey,
    TaskImagePublicationKeyset,
    TaskImagePublicationKeysetMember,
    TaskImagePublicationState,
)
from loom_task_image_authority.publication_contracts import MAX_SAFE_INTEGER
from loom_task_image_authority.publication_keyset import (
    ExecutionGrantTrustRoot,
    PublicationVerificationKey,
    VerifiedPublicationKeyset,
    verify_publication_keyset,
)
from loom_task_image_authority.publication_signing import (
    DistributedKeysetSnapshot,
    PublicationKeyRecord,
    PublicationState,
)

Clock = Callable[[], datetime]


def _clock() -> datetime:
    return datetime.now(UTC).replace(microsecond=0)


def _instant(value: str) -> datetime:
    return datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=UTC)


def _stamp(value: datetime) -> str:
    if value.utcoffset() is None or value.microsecond:
        raise ValueError("keyset key times must be whole UTC seconds")
    return value.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


async def _transaction(session: AsyncSession) -> None:
    # Never autoflush pending authority or unrelated caller objects. Separate
    # text-only probes reject per-statement AUTOCOMMIT as well as fixed snapshots.
    if session.new or session.dirty or session.deleted:
        raise ValueError("keyset transaction contains pending writes")
    await session.execute(text("SELECT pg_catalog.pg_current_xact_id()"))
    valid = await session.scalar(text(
        "SELECT pg_catalog.current_setting('transaction_isolation') = 'read committed' "
        "AND pg_catalog.pg_current_xact_id_if_assigned() IS NOT NULL"
    ))
    if valid is not True:
        raise ValueError("keyset authority requires an explicit READ COMMITTED transaction")


def _state(row: TaskImagePublicationState) -> PublicationState:
    return PublicationState(keyset_version=row.keyset_version, revocation_epoch=row.revocation_epoch)


def _key(row: TaskImagePublicationKey) -> PublicationVerificationKey:
    values = dict(
        key_id=row.key_id, public_key=base64.urlsafe_b64encode(row.public_key).rstrip(b"=").decode("ascii"),
        activated_at=_stamp(row.activated_at), status=row.status,
    )
    if row.retired_at is not None:
        values["retired_at"] = _stamp(row.retired_at)
    if row.revoked_at is not None:
        values["revoked_at"] = _stamp(row.revoked_at)
    return PublicationVerificationKey.model_validate(values)


async def _locked(
    session: AsyncSession,
) -> tuple[TaskImagePublicationState, tuple[PublicationVerificationKey, ...]]:
    await _transaction(session)
    state = await session.scalar(select(TaskImagePublicationState)
        .where(TaskImagePublicationState.singleton_id == 1)
        .execution_options(populate_existing=True).with_for_update())
    if state is None:
        raise ValueError("publication state is absent")
    _state(state)
    # Migration 0135's statement-level key mutation trigger takes state first.
    # READ COMMITTED plus that held lock fences new-key phantoms and lifecycles.
    rows = (await session.scalars(select(TaskImagePublicationKey)
        .order_by(TaskImagePublicationKey.key_id).limit(129)
        .execution_options(populate_existing=True).with_for_update())).all()
    if not 1 <= len(rows) <= 128:
        raise ValueError("full publication keyset requires 1 to 128 retained keys")
    return state, tuple(_key(row) for row in rows)


@dataclass(frozen=True)
class KeysetPreparation:
    environment: str
    root_sha256: str
    execution_key_id: str
    previous_state: PublicationState
    keys: tuple[PublicationVerificationKey, ...]

    @property
    def proposed_state(self) -> PublicationState:
        return PublicationState(
            keyset_version=self.previous_state.keyset_version + 1,
            revocation_epoch=self.previous_state.revocation_epoch,
        )


@dataclass(frozen=True)
class StoredPublicationKeyset:
    wire: bytes
    state: PublicationState
    snapshot_sha256: str
    keys: tuple[PublicationVerificationKey, ...]
    issued_at: datetime
    expires_at: datetime


def _result(wire: bytes, verified: VerifiedPublicationKeyset) -> StoredPublicationKeyset:
    keyset = verified.keyset
    return StoredPublicationKeyset(
        wire=wire,
        state=PublicationState(keyset_version=keyset.keyset_version, revocation_epoch=keyset.revocation_epoch),
        snapshot_sha256=verified.snapshot_sha256, keys=keyset.keys,
        issued_at=_instant(keyset.issued_at), expires_at=_instant(keyset.expires_at),
    )


async def prepare_keyset(session: AsyncSession, *, trust_root: ExecutionGrantTrustRoot) -> KeysetPreparation:
    """Freeze the entire retained public key set; commit before external signing."""
    trust_root.__post_init__()
    state, keys = await _locked(session)
    if state.keyset_version == MAX_SAFE_INTEGER:
        raise ValueError("publication keyset version is exhausted")
    if any(key.record().public_key == trust_root.public_key for key in keys):
        raise ValueError("execution root must be distinct from publication keys")
    return KeysetPreparation(
        environment=trust_root.environment, root_sha256=hashlib.sha256(trust_root.public_key).hexdigest(),
        execution_key_id=trust_root.key_id, previous_state=_state(state), keys=keys,
    )


async def _retained(
    session: AsyncSession, *, state: PublicationState, keys: tuple[PublicationVerificationKey, ...],
    trust_root: ExecutionGrantTrustRoot, clock: Clock,
) -> StoredPublicationKeyset:
    row = await session.scalar(select(TaskImagePublicationKeyset)
        .where(TaskImagePublicationKeyset.keyset_version == state.keyset_version)
        .execution_options(populate_existing=True))
    if row is None:
        raise ValueError("authenticated publication keyset is absent")
    members = tuple((await session.scalars(select(TaskImagePublicationKeysetMember.key_id)
        .where(TaskImagePublicationKeysetMember.keyset_version == state.keyset_version)
        .order_by(TaskImagePublicationKeysetMember.key_id).limit(129))).all())
    verified = verify_publication_keyset(row.canonical_envelope, trust_root=trust_root, expected_state=state, now=clock())
    result = _result(row.canonical_envelope, verified)
    if (
        result.keys != keys or members != tuple(key.key_id for key in keys)
        or row.revocation_epoch != state.revocation_epoch
        or row.environment != trust_root.environment or row.execution_key_id != trust_root.key_id
        or row.root_sha256 != hashlib.sha256(trust_root.public_key).hexdigest()
        or row.keyset_sha256 != verified.envelope.keyset_sha256
        or row.snapshot_sha256 != result.snapshot_sha256
        or row.issued_at != result.issued_at or row.expires_at != result.expires_at
    ):
        raise ValueError("retained keyset metadata or full membership changed")
    # No await between final freshness verification and returning the artifact.
    verify_publication_keyset(result.wire, trust_root=trust_root, expected_state=state, now=clock())
    return result


async def finalize_keyset(
    session: AsyncSession, *, preparation: KeysetPreparation, wire: bytes,
    trust_root: ExecutionGrantTrustRoot, clock: Clock = _clock,
) -> StoredPublicationKeyset:
    """Atomically retain exact signed bytes and advance state, or exact current replay."""
    proposed = preparation.proposed_state
    verified = verify_publication_keyset(wire, trust_root=trust_root, expected_state=proposed, now=clock())
    if (
        preparation.environment != trust_root.environment
        or preparation.execution_key_id != trust_root.key_id
        or preparation.root_sha256 != hashlib.sha256(trust_root.public_key).hexdigest()
        or preparation.keys != verified.keyset.keys
    ):
        raise ValueError("signed keyset differs from prepared authority")
    row, keys = await _locked(session)
    if keys != preparation.keys:
        raise ValueError("publication keyset changed during signing")
    if _state(row) == proposed:
        retained = await _retained(session, state=proposed, keys=keys, trust_root=trust_root, clock=clock)
        if retained.wire != wire:
            raise ValueError("publication keyset replay conflicts with retained bytes")
        return retained
    if _state(row) != preparation.previous_state:
        raise ValueError("publication keyset state changed during signing")
    # Time can expire while acquiring either state or keys. Recheck before write,
    # and after flush as database I/O may also wait. Failure requires rollback.
    verify_publication_keyset(wire, trust_root=trust_root, expected_state=proposed, now=clock())
    result = _result(wire, verified)
    session.add(TaskImagePublicationKeyset(
        keyset_version=proposed.keyset_version, revocation_epoch=proposed.revocation_epoch,
        environment=trust_root.environment, execution_key_id=trust_root.key_id,
        root_sha256=preparation.root_sha256, keyset_sha256=verified.envelope.keyset_sha256,
        snapshot_sha256=result.snapshot_sha256, canonical_envelope=wire,
        issued_at=result.issued_at, expires_at=result.expires_at,
    ))
    await session.flush()
    session.add_all(TaskImagePublicationKeysetMember(keyset_version=proposed.keyset_version, key_id=key.key_id) for key in keys)
    row.keyset_version = proposed.keyset_version
    await session.flush()
    verify_publication_keyset(wire, trust_root=trust_root, expected_state=proposed, now=clock())
    return result


async def read_keyset(
    session: AsyncSession, *, trust_root: ExecutionGrantTrustRoot,
    expected_state: PublicationState, clock: Clock = _clock,
) -> StoredPublicationKeyset:
    """Admission-time full snapshot validation, not retroactive unrelated revocation."""
    row, keys = await _locked(session)
    if _state(row) != expected_state:
        raise ValueError("publication keyset requested state is stale")
    return await _retained(session, state=expected_state, keys=keys, trust_root=trust_root, clock=clock)


class DatabasePublicationDistribution:
    """Inert adapter for a trusted DB connection and explicitly pinned public root.

    Compose only with authenticated exact-envelope delivery to capable workers
    and the online one-use start gate. A stored signature is not fleet readiness.
    The passed engine's authentication/lifetime remain operator/caller owned.
    """

    def __init__(
        self, engine: AsyncEngine, *, trust_root: ExecutionGrantTrustRoot,
        clock: Clock = _clock, timeout_seconds: float = 5,
    ) -> None:
        if type(timeout_seconds) not in (int, float) or not math.isfinite(timeout_seconds) or not 0 < timeout_seconds <= 30:
            raise ValueError("keyset distribution deadline must be within 30 seconds")
        trust_root.__post_init__()
        self._engine = engine
        self._root, self._clock, self._timeout = trust_root, clock, timeout_seconds

    async def _read(self, state: PublicationState) -> StoredPublicationKeyset:
        async with asyncio.timeout(self._timeout):
            async with self._engine.connect() as connection:
                # Apply after checkout: inherited engine hooks can override
                # derived-engine isolation options, including AUTOCOMMIT.
                await connection.execution_options(isolation_level="READ COMMITTED")
                async with AsyncSession(connection, expire_on_commit=False) as session, session.begin():
                    result = await read_keyset(session, trust_root=self._root, expected_state=state, clock=self._clock)
            # Transaction commit and pool cleanup also consume the deadline.
            verify_publication_keyset(result.wire, trust_root=self._root, expected_state=state, now=self._clock())
            return result

    async def envelope(self, *, state: PublicationState) -> bytes:
        """Return the exact retained envelope for future authenticated claim delivery."""
        return (await self._read(state)).wire

    async def snapshot(self, *, state: PublicationState, key: PublicationKeyRecord) -> DistributedKeysetSnapshot:
        result = await self._read(state)
        if key not in tuple(member.record() for member in result.keys):
            raise ValueError("requested publication key differs from distributed record")
        return DistributedKeysetSnapshot(
            keyset_version=result.state.keyset_version, revocation_epoch=result.state.revocation_epoch,
            key_ids=tuple(member.key_id for member in result.keys),
            issued_at=result.issued_at, expires_at=result.expires_at,
        )
