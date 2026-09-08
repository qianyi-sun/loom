"""Fenced signed completion and immutable historical confirmation, never HTTP auth."""

from __future__ import annotations

import hashlib
from datetime import UTC, datetime, timedelta
from typing import Literal, cast
from uuid import UUID, uuid4

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from loom.db.schema import (
    TaskImagePublicationCandidate,
    TaskImagePublicationEnvelope,
    TaskImagePublicationJob,
    TaskImagePublicationKey,
    TaskImagePublicationState,
    TaskImageRegistryCredentialGeneration,
)
from loom.task_image_materialization import validate_task_image_registry_images
from loom_task_image_authority.publication_contracts import (
    PublicationEnvelope,
    PublicationUnsignedInput,
    canonical_publication_bytes,
)
from loom_task_image_authority.publication_jobs import (
    PublicationJob,
    PublicationJobAuthorizationError,
    PublicationJobConflictError,
    PublicationSnapshot,
)
from loom_task_image_authority.publication_receipts import (
    PublicationCandidateIdentity,
    PublicationEnvelopeIdentity,
    PublicationReceipt,
    candidate_set_sha256,
    canonical_receipt_bytes,
    decode_publication_receipt,
    publication_set_sha256,
)
from loom_task_image_authority.publication_signing import (
    DistributedKeysetSnapshot,
    PublicationKeyRecord,
    PublicationState,
    VerifiedPublication,
    verify_historical_publication,
)
from loom_task_image_authority.publication_store import (
    Clock,
    LockedPublicationInput,
    _live_at,
    _now,
    _owner,
    _result,
    _uuid,
    lock_publication_input,
)
from loom_task_image_authority.registry_credentials import parse_stored_publication_candidate_v2


def _clean(session: AsyncSession) -> None:
    if session.new or session.dirty or session.deleted:
        raise PublicationJobConflictError("publication transaction contains unflushed writes")


def _key(row: TaskImagePublicationKey) -> PublicationKeyRecord:
    return PublicationKeyRecord(
        row.key_id,
        row.public_key,
        row.activated_at,
        cast(Literal["active", "verify_only", "revoked"], row.status),
        row.retired_at,
        row.revoked_at,
    )


async def _locked_keys(
    session: AsyncSession, key_ids: tuple[str, ...]
) -> tuple[PublicationState, dict[str, PublicationKeyRecord]]:
    _clean(session)
    state = await session.scalar(
        select(TaskImagePublicationState)
        .where(TaskImagePublicationState.singleton_id == 1)
        .execution_options(populate_existing=True)
        .with_for_update()
    )
    if state is None:
        raise PublicationJobAuthorizationError("publication state unavailable")
    keys = {}
    for key_id in sorted(set(key_ids)):
        row = await session.scalar(
            select(TaskImagePublicationKey)
            .where(TaskImagePublicationKey.key_id == key_id)
            .execution_options(populate_existing=True)
            .with_for_update()
        )
        if row is None:
            raise PublicationJobAuthorizationError("publication key unavailable")
        keys[key_id] = _key(row)
    return PublicationState(state.revocation_epoch, state.keyset_version), keys


async def read_publication_signing_state(
    session: AsyncSession, *, key_id: str
) -> tuple[PublicationState, PublicationKeyRecord]:
    """Short state-first transaction; caller commits before distribution/signer I/O."""
    state, keys = await _locked_keys(session, (key_id,))
    return state, keys[key_id]


def _unsigned_binding(
    snapshot: PublicationSnapshot, result: VerifiedPublication, index: int
) -> None:
    component = snapshot.components[index]
    values = snapshot.model_dump(mode="json", by_alias=True, exclude={"components", "builder_id"})
    values.update(
        schema="loom.task-image-publication/v1",
        component=component.candidate.component,
        repository=component.candidate.repository,
        root=component.root.model_dump(),
        manifest=component.root.model_dump(),
        config=result.statement.config.model_dump(),
        layers=[layer.model_dump() for layer in result.statement.layers],
        observed_base_digests=component.candidate.base_resolution.observed_base_digests,
    )
    expected = PublicationUnsignedInput.model_validate(values)
    if canonical_publication_bytes(expected) != canonical_publication_bytes(
        result.statement.unsigned_input()
    ):
        raise PublicationJobConflictError("publication statement changed frozen input")


def _verify(
    snapshot: PublicationSnapshot,
    publications: tuple[VerifiedPublication, ...],
    keys: dict[str, PublicationKeyRecord],
) -> tuple[VerifiedPublication, ...]:
    if type(publications) is not tuple or len(publications) != len(snapshot.components):
        raise PublicationJobConflictError("publication envelope set is incomplete")
    checked = []
    try:
        for index, supplied in enumerate(publications):
            result = verify_historical_publication(
                canonical_publication_bytes(supplied.envelope), key=keys[supplied.envelope.key_id]
            )
            if canonical_publication_bytes(result.statement) != canonical_publication_bytes(
                supplied.statement
            ):
                raise ValueError("publication statement and envelope disagree")
            _unsigned_binding(snapshot, result, index)
            checked.append(result)
    except (ValueError, KeyError, TypeError) as exc:
        raise PublicationJobConflictError("publication signature or binding invalid") from exc
    return tuple(checked)


def _receipt(
    job: PublicationJob, publications: tuple[VerifiedPublication, ...], completed_at: datetime
) -> PublicationReceipt:
    candidates = tuple(
        PublicationCandidateIdentity(
            candidate_id=str(item.candidate.candidate_id), component=item.candidate.component
        )
        for item in job.snapshot.components
    )
    envelopes = tuple(
        PublicationEnvelopeIdentity(
            candidate_id=identity.candidate_id,
            component=identity.component,
            envelope_sha256=hashlib.sha256(
                canonical_publication_bytes(result.envelope)
            ).hexdigest(),
        )
        for identity, result in zip(candidates, publications, strict=True)
    )
    return PublicationReceipt.model_validate(
        dict(
            schema="loom.task-image-publication-receipt/v1",
            operation_id=job.operation_id,
            materialization_id=job.snapshot.materialization_id,
            attempt_id=job.snapshot.attempt_id,
            lease_epoch=job.snapshot.lease_epoch,
            worker_generation=job.worker_generation,
            snapshot_sha256=job.snapshot_sha256,
            candidate_set_sha256=candidate_set_sha256(candidates),
            publication_set_sha256=publication_set_sha256(envelopes),
            component_count=len(candidates),
            completed_at=completed_at.strftime("%Y-%m-%dT%H:%M:%SZ"),
        )
    )


def _eligible(
    state: PublicationState,
    keys: dict[str, PublicationKeyRecord],
    distribution: DistributedKeysetSnapshot,
    publications: tuple[VerifiedPublication, ...],
    now: datetime,
) -> None:
    # Reconstruct trusted adapter output to reject unchecked mutated instances.
    distribution = DistributedKeysetSnapshot(
        distribution.keyset_version,
        distribution.revocation_epoch,
        distribution.key_ids,
        distribution.issued_at,
        distribution.expires_at,
    )
    if (
        not distribution.issued_at <= now < distribution.expires_at
        or distribution.keyset_version != state.keyset_version
        or distribution.revocation_epoch != state.revocation_epoch
    ):
        raise PublicationJobAuthorizationError("publication distribution is stale")
    for result in publications:
        key = keys[result.envelope.key_id]
        issued = datetime.strptime(result.statement.issued_at, "%Y-%m-%dT%H:%M:%SZ").replace(
            tzinfo=UTC
        )
        if (
            key.status != "active"
            or key.key_id not in distribution.key_ids
            or now < key.activated_at
            or result.statement.distributed_keyset_version != state.keyset_version
            or result.statement.revocation_epoch != state.revocation_epoch
            or not distribution.issued_at <= issued < distribution.expires_at
            or issued > now + timedelta(seconds=5)
        ):
            raise PublicationJobAuthorizationError("publication signing authority changed")


async def complete_publication_job(
    session: AsyncSession,
    *,
    job: PublicationJob,
    owner_id: UUID,
    generation: int,
    publications: tuple[VerifiedPublication, ...],
    distribution: DistributedKeysetSnapshot,
    clock: Clock,
) -> PublicationReceipt:
    """Caller-owned atomic transaction. On any exception caller MUST roll back.

    State -> sorted keys -> full live input locks -> job -> envelope inserts.
    All graph/signing work must already have finished outside this transaction.
    VerifiedPublication is re-parsed and cryptographically checked, not trusted.
    """
    if type(publications) is not tuple or not 1 <= len(publications) <= 128:
        raise PublicationJobConflictError("publication envelope set is invalid")
    state, keys = await _locked_keys(session, tuple(item.envelope.key_id for item in publications))
    # Historical completion can win before this worker. No live-lease requirement
    # is imposed on that already committed evidence; replay takes no new locks.
    existing = await session.scalar(
        select(TaskImagePublicationJob.state).where(
            TaskImagePublicationJob.operation_id == UUID(job.operation_id)
        )
    )
    if existing == "completed":
        receipt = await replay_completed_publication(session, operation_id=job.operation_id)
        if receipt.snapshot_sha256 != job.snapshot_sha256:
            raise PublicationJobConflictError("publication completion operation changed")
        return receipt
    locked = await lock_publication_input(
        session,
        grant_id=UUID(job.snapshot.grant_id),
        operation_id=UUID(job.operation_id),
        materialization_id=UUID(job.snapshot.materialization_id),
        attempt_id=UUID(job.snapshot.attempt_id),
        lease_epoch=job.snapshot.lease_epoch,
        registry_origin=job.snapshot.registry_origin,
        clock=clock,
    )
    if locked.existing is None:
        raise PublicationJobConflictError("publication job missing")
    stored = locked.existing
    current = _result(stored)
    if current.snapshot_sha256 != job.snapshot_sha256 or current.snapshot != job.snapshot:
        raise PublicationJobConflictError("publication job input changed")
    checked = _verify(current.snapshot, publications, keys)
    now = _now(clock)
    _final_liveness(locked, owner_id, generation, state, keys, distribution, checked, now)
    receipt = _receipt(current, checked, now)
    for component, publication in zip(current.snapshot.components, checked, strict=True):
        statement, envelope = publication.statement, publication.envelope
        session.add(
            TaskImagePublicationEnvelope(
                envelope_id=uuid4(),
                candidate_id=component.candidate.candidate_id,
                materialization_attempt_id=UUID(current.snapshot.attempt_id),
                component=component.candidate.component,
                key_id=envelope.key_id,
                canonical_statement=envelope.canonical_statement.encode(),
                statement_sha256=envelope.statement_sha256,
                algorithm=envelope.algorithm,
                signature=envelope.signature,
                issued_at=datetime.strptime(statement.issued_at, "%Y-%m-%dT%H:%M:%SZ").replace(
                    tzinfo=UTC
                ),
                recorded_at=now,
                distributed_keyset_version=statement.distributed_keyset_version,
                revocation_epoch=statement.revocation_epoch,
            )
        )
    # Flush envelopes BEFORE clearing the lease so the post-wait authority check
    # still uses its unchanged locked live rows. Later ready/job flush is checked too.
    await session.flush()
    _final_liveness(locked, owner_id, generation, state, keys, distribution, checked, _now(clock))
    row = locked.materialization
    materialization_lease_expires_at = row.lease_expires_at
    assert materialization_lease_expires_at is not None
    images = {
        component.candidate.component: f"{current.snapshot.registry_origin.removeprefix('https://')}/{component.candidate.repository}@{publication.statement.manifest.digest}"
        for component, publication in zip(current.snapshot.components, checked, strict=True)
    }
    row.registry_images = validate_task_image_registry_images(
        images, expected_components=set(images), require_complete=True
    )
    row.state, row.claimed_by, row.lease_expires_at = "ready", None, None
    row.ready_at = row.finished_at = row.updated_at = now
    stored.state, stored.worker_id, stored.worker_expires_at = "completed", None, None
    stored.completed_at = now
    stored.canonical_receipt = canonical_receipt_bytes(receipt)
    stored.receipt_sha256 = hashlib.sha256(stored.canonical_receipt).hexdigest()
    await session.flush()
    # Lease expiry values were deliberately captured before clearing ORM fields.
    final = _now(clock)
    if final >= min(
        current.deadline,
        current.lease.expires_at if current.lease else current.deadline,
        locked.authorization.grant_expires_at,
        locked.authorization.session_expires_at,
        locked.authorization.attestation_expires_at,
        materialization_lease_expires_at,
    ):
        raise PublicationJobAuthorizationError("publication authority expired during commit")
    _eligible(state, keys, distribution, checked, final)
    return receipt


def _final_liveness(
    locked: LockedPublicationInput,
    owner: UUID,
    generation: int,
    state: PublicationState,
    keys: dict[str, PublicationKeyRecord],
    distribution: DistributedKeysetSnapshot,
    publications: tuple[VerifiedPublication, ...],
    now: datetime,
) -> None:
    assert locked.existing is not None
    _live_at(locked.authorization, locked.materialization, now)
    _owner(locked.existing, owner, generation, now)
    _eligible(state, keys, distribution, publications, now)


async def replay_completed_publication(
    session: AsyncSession, *, operation_id: UUID | str
) -> PublicationReceipt:
    """Historical evidence only: no FOR UPDATE, state/key locks, signing or writes.

    Safe after caller authentication's grant locks. Immutable job/envelope/public
    key identity reads do not grant fresh readiness or execution authorization.
    Current materialization state/lease and current epoch are intentionally irrelevant.
    """
    _clean(session)
    identity = _uuid(UUID(operation_id) if isinstance(operation_id, str) else operation_id)
    row = await session.scalar(
        select(TaskImagePublicationJob)
        .where(TaskImagePublicationJob.operation_id == identity)
        .execution_options(populate_existing=True)
    )
    if (
        row is None
        or row.state != "completed"
        or row.completed_at is None
        or row.canonical_receipt is None
    ):
        raise PublicationJobConflictError("publication completion unavailable")
    job = _result(row)
    credentials = {
        item.credential_id: item.generation
        for item in await session.scalars(
            select(TaskImageRegistryCredentialGeneration)
            .where(
                TaskImageRegistryCredentialGeneration.materialization_attempt_id
                == row.materialization_attempt_id
            )
            .execution_options(populate_existing=True)
        )
    }
    candidates = list(
        await session.scalars(
            select(TaskImagePublicationCandidate)
            .where(
                TaskImagePublicationCandidate.materialization_attempt_id
                == row.materialization_attempt_id
            )
            .execution_options(populate_existing=True)
        )
    )
    candidates.sort(key=lambda item: (item.component != "task", item.component))
    if len(candidates) != len(job.snapshot.components):
        raise PublicationJobConflictError("historical candidate set changed")
    for candidate, component in zip(candidates, job.snapshot.components, strict=True):
        if (
            candidate.credential_id not in credentials
            or parse_stored_publication_candidate_v2(
                candidate, credential_generation=credentials[candidate.credential_id]
            )
            != component.candidate
        ):
            raise PublicationJobConflictError("historical candidate changed")
    envelopes = list(
        await session.scalars(
            select(TaskImagePublicationEnvelope)
            .where(
                TaskImagePublicationEnvelope.materialization_attempt_id
                == row.materialization_attempt_id
            )
            .execution_options(populate_existing=True)
        )
    )
    envelopes.sort(key=lambda item: (item.component != "task", item.component))
    if len(envelopes) != len(candidates):
        raise PublicationJobConflictError("historical envelope set incomplete")
    keys = {}
    publications = []
    for envelope, candidate in zip(envelopes, candidates, strict=True):
        key_row = await session.scalar(
            select(TaskImagePublicationKey)
            .where(TaskImagePublicationKey.key_id == envelope.key_id)
            .execution_options(populate_existing=True)
        )
        if key_row is None:
            raise PublicationJobConflictError("historical publication key missing")
        key = keys[envelope.key_id] = _key(key_row)
        wire = PublicationEnvelope(
            canonical_statement=envelope.canonical_statement.decode(),
            statement_sha256=envelope.statement_sha256,
            key_id=envelope.key_id,
            algorithm=cast(Literal["Ed25519"], envelope.algorithm),
            signature=envelope.signature,
        )
        verified = verify_historical_publication(canonical_publication_bytes(wire), key=key)
        if (
            envelope.candidate_id != candidate.candidate_id
            or envelope.component != candidate.component
            or envelope.recorded_at != row.completed_at
            or envelope.issued_at
            != datetime.strptime(verified.statement.issued_at, "%Y-%m-%dT%H:%M:%SZ").replace(
                tzinfo=UTC
            )
            or envelope.distributed_keyset_version != verified.statement.distributed_keyset_version
            or envelope.revocation_epoch != verified.statement.revocation_epoch
        ):
            raise PublicationJobConflictError("historical publication row binding changed")
        publications.append(verified)
    checked = _verify(job.snapshot, tuple(publications), keys)
    receipt = decode_publication_receipt(row.canonical_receipt)
    if receipt != _receipt(job, checked, row.completed_at):
        raise PublicationJobConflictError("historical receipt changed")
    return receipt
