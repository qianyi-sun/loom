"""Read and verify immutable historical publication evidence; no build or signing writes."""

from __future__ import annotations

import hashlib
from datetime import UTC, datetime
from typing import Literal, cast
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from loom.db.schema import (
    TaskImagePublicationCandidate,
    TaskImagePublicationEnvelope,
    TaskImagePublicationJob,
    TaskImagePublicationKey,
    TaskImageRegistryCredentialGeneration,
)
from loom_task_image_authority.publication_contracts import (
    PublicationEnvelope,
    PublicationUnsignedInput,
    canonical_publication_bytes,
)
from loom_task_image_authority.publication_jobs import (
    PublicationJob,
    PublicationJobConflictError,
    PublicationSnapshot,
)
from loom_task_image_authority.publication_receipts import (
    PublicationCandidateIdentity,
    PublicationEnvelopeIdentity,
    PublicationReceipt,
    candidate_set_sha256,
    decode_publication_receipt,
    publication_set_sha256,
)
from loom_task_image_authority.publication_signing import (
    PublicationKeyRecord,
    VerifiedPublication,
    verify_historical_publication,
)
from loom_task_image_authority.publication_store import (
    _result,
    _uuid,
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
