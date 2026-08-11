"""SQLAlchemy authority for immutable personal-dev candidates and build leases."""

from __future__ import annotations

import hashlib
from datetime import datetime, timedelta
from typing import cast
from uuid import UUID

from sqlalchemy import func, or_, select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from loom.db.schema import PersonalDevCandidate, PersonalDevCandidateBuildAttempt
from loom.personal_dev_candidate import (
    BuildAttemptState,
    CandidateRegistration,
    CandidateStatus,
    PersonalDevCandidateBuildAttemptRecord,
    PersonalDevCandidateRecord,
    validate_personal_dev_candidate_publication,
)


class PersonalDevBuildLeaseFencedError(RuntimeError):
    """The attempt lease no longer belongs to the caller."""


class PersonalDevBuildOperationConflictError(RuntimeError):
    """One lifecycle operation was already bound to a different candidate."""


def _operation_lock_keys(
    subject_id: UUID,
    subject_incarnation: UUID,
    operation_epoch: int,
) -> tuple[int, int]:
    material = (
        subject_id.bytes
        + subject_incarnation.bytes
        + operation_epoch.to_bytes(8, byteorder="big", signed=False)
    )
    digest = hashlib.sha256(material).digest()
    return (
        int.from_bytes(digest[:4], byteorder="big", signed=True),
        int.from_bytes(digest[4:8], byteorder="big", signed=True),
    )


def _candidate_record(row: PersonalDevCandidate) -> PersonalDevCandidateRecord:
    return PersonalDevCandidateRecord(
        id=row.id,
        owner_user_id=row.owner_user_id,
        owner_team_id=row.owner_team_id,
        candidate_sha=row.candidate_sha,
        source_sha256=row.source_sha256,
        archive_sha256=row.archive_sha256,
        build_contract_sha256=row.build_contract_sha256,
        source_commit=row.source_commit,
        dirty=row.dirty,
        manifest_json=row.manifest_json,
        object_bucket=row.object_bucket,
        object_key=row.object_key,
        archive_size_bytes=row.archive_size_bytes,
        status=cast(CandidateStatus, row.status),
        image_manifest_digest=row.image_manifest_digest,
        publication_json=row.publication_json,
        publication_sha256=row.publication_sha256,
        failure_reason=row.failure_reason,
        created_at=row.created_at,
        updated_at=row.updated_at,
        ready_at=row.ready_at,
    )


def _attempt_record(
    row: PersonalDevCandidateBuildAttempt,
) -> PersonalDevCandidateBuildAttemptRecord:
    return PersonalDevCandidateBuildAttemptRecord(
        id=row.id,
        candidate_id=row.candidate_id,
        subject_id=row.subject_id,
        subject_incarnation=row.subject_incarnation,
        operation_id=row.operation_id,
        operation_epoch=row.operation_epoch,
        attempt_sequence=row.attempt_sequence,
        state=cast(BuildAttemptState, row.state),
        lease_epoch=row.lease_epoch,
        claimed_by=row.claimed_by,
        lease_expires_at=row.lease_expires_at,
        failure_reason=row.failure_reason,
        created_at=row.created_at,
        updated_at=row.updated_at,
        started_at=row.started_at,
        finished_at=row.finished_at,
    )


class SqlAlchemyPersonalDevCandidateStore:
    """Request-scoped candidate registry with idempotent source identity."""

    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def register(
        self,
        requested: PersonalDevCandidateRecord,
    ) -> CandidateRegistration:
        inserted_id = (
            await self.session.execute(
                pg_insert(PersonalDevCandidate)
                .values(
                    id=requested.id,
                    owner_user_id=requested.owner_user_id,
                    owner_team_id=requested.owner_team_id,
                    candidate_sha=requested.candidate_sha,
                    source_sha256=requested.source_sha256,
                    archive_sha256=requested.archive_sha256,
                    build_contract_sha256=requested.build_contract_sha256,
                    source_commit=requested.source_commit,
                    dirty=requested.dirty,
                    manifest_json=dict(requested.manifest_json),
                    object_bucket=requested.object_bucket,
                    object_key=requested.object_key,
                    archive_size_bytes=requested.archive_size_bytes,
                    status="uploaded",
                    created_at=requested.created_at,
                    updated_at=requested.updated_at,
                )
                .on_conflict_do_nothing(
                    constraint="personal_dev_candidates_owner_source_uidx",
                )
                .returning(PersonalDevCandidate.id),
            )
        ).scalar_one_or_none()
        created = inserted_id is not None
        if created:
            candidate_row = await self.session.get(PersonalDevCandidate, requested.id)
            if candidate_row is None:  # pragma: no cover - insert returned the same key
                raise RuntimeError("personal-dev candidate disappeared during registration")
            candidate_record = _candidate_record(candidate_row)
            await self.session.commit()
            return CandidateRegistration(
                candidate=candidate_record,
                build_attempt=None,
                created=True,
            )

        candidate_row = (
            await self.session.execute(
                select(PersonalDevCandidate).where(
                    PersonalDevCandidate.owner_user_id == requested.owner_user_id,
                    PersonalDevCandidate.owner_team_id == requested.owner_team_id,
                    PersonalDevCandidate.source_sha256 == requested.source_sha256,
                    PersonalDevCandidate.archive_sha256 == requested.archive_sha256,
                    PersonalDevCandidate.build_contract_sha256 == requested.build_contract_sha256,
                ),
            )
        ).scalar_one()
        candidate_record = _candidate_record(candidate_row)
        await self.session.commit()
        return CandidateRegistration(
            candidate=candidate_record,
            build_attempt=None,
            created=False,
        )

    async def get(self, candidate_id: UUID) -> CandidateRegistration | None:
        candidate = await self.session.get(PersonalDevCandidate, candidate_id)
        if candidate is None:
            return None
        attempt = (
            await self.session.execute(
                select(PersonalDevCandidateBuildAttempt)
                .where(PersonalDevCandidateBuildAttempt.candidate_id == candidate_id)
                .order_by(PersonalDevCandidateBuildAttempt.attempt_sequence.desc())
                .limit(1),
            )
        ).scalar_one_or_none()
        return CandidateRegistration(
            candidate=_candidate_record(candidate),
            build_attempt=_attempt_record(attempt) if attempt is not None else None,
            created=False,
        )

    async def list_visible(
        self,
        *,
        owner_user_id: UUID | None,
        limit: int = 100,
    ) -> list[CandidateRegistration]:
        statement = select(PersonalDevCandidate)
        if owner_user_id is not None:
            statement = statement.where(PersonalDevCandidate.owner_user_id == owner_user_id)
        candidates = (
            (
                await self.session.execute(
                    statement.order_by(
                        PersonalDevCandidate.created_at.desc(),
                        PersonalDevCandidate.id.desc(),
                    ).limit(limit),
                )
            )
            .scalars()
            .all()
        )
        if not candidates:
            return []
        attempts = (
            (
                await self.session.execute(
                    select(PersonalDevCandidateBuildAttempt)
                    .where(
                        PersonalDevCandidateBuildAttempt.candidate_id.in_(
                            [candidate.id for candidate in candidates],
                        ),
                    )
                    .order_by(
                        PersonalDevCandidateBuildAttempt.candidate_id,
                        PersonalDevCandidateBuildAttempt.attempt_sequence.desc(),
                    ),
                )
            )
            .scalars()
            .all()
        )
        latest: dict[UUID, PersonalDevCandidateBuildAttempt] = {}
        for attempt in attempts:
            latest.setdefault(attempt.candidate_id, attempt)
        return [
            CandidateRegistration(
                candidate=_candidate_record(candidate),
                build_attempt=(
                    _attempt_record(latest[candidate.id]) if candidate.id in latest else None
                ),
                created=False,
            )
            for candidate in candidates
        ]

    async def enqueue_build(
        self,
        *,
        candidate_id: UUID,
        subject_id: UUID,
        subject_incarnation: UUID,
        operation_id: UUID,
        operation_epoch: int,
        now: datetime,
    ) -> CandidateRegistration:
        """Bind build start to one already-authorized lifecycle operation.

        Locking the candidate serializes retries and prevents two lifecycle
        requests from creating parallel builds for the same immutable source.
        """

        if type(operation_epoch) is not int or operation_epoch <= 0:
            raise ValueError("operation_epoch must be a positive integer")
        lock_a, lock_b = _operation_lock_keys(
            subject_id,
            subject_incarnation,
            operation_epoch,
        )
        await self.session.execute(select(func.pg_advisory_xact_lock(lock_a, lock_b)))
        candidate = (
            await self.session.execute(
                select(PersonalDevCandidate)
                .where(PersonalDevCandidate.id == candidate_id)
                .with_for_update(),
            )
        ).scalar_one_or_none()
        if candidate is None:
            await self.session.rollback()
            raise KeyError("personal-dev candidate not found")
        operation_attempt = (
            await self.session.execute(
                select(PersonalDevCandidateBuildAttempt)
                .where(
                    PersonalDevCandidateBuildAttempt.subject_id == subject_id,
                    PersonalDevCandidateBuildAttempt.subject_incarnation == subject_incarnation,
                    PersonalDevCandidateBuildAttempt.operation_epoch == operation_epoch,
                )
                .order_by(PersonalDevCandidateBuildAttempt.attempt_sequence.desc())
                .limit(1),
            )
        ).scalar_one_or_none()
        if operation_attempt is not None:
            if (
                operation_attempt.candidate_id != candidate_id
                or operation_attempt.operation_id != operation_id
            ):
                await self.session.rollback()
                raise PersonalDevBuildOperationConflictError(
                    "lifecycle operation is already bound to another candidate",
                )
            if operation_attempt.state != "failed":
                candidate_record = _candidate_record(candidate)
                attempt_record = _attempt_record(operation_attempt)
                await self.session.commit()
                return CandidateRegistration(
                    candidate=candidate_record,
                    build_attempt=attempt_record,
                    created=False,
                )

        latest = (
            await self.session.execute(
                select(PersonalDevCandidateBuildAttempt)
                .where(PersonalDevCandidateBuildAttempt.candidate_id == candidate_id)
                .order_by(PersonalDevCandidateBuildAttempt.attempt_sequence.desc())
                .limit(1),
            )
        ).scalar_one_or_none()
        if candidate.status == "ready":
            candidate_record = _candidate_record(candidate)
            ready_attempt_record = _attempt_record(latest) if latest is not None else None
            await self.session.commit()
            return CandidateRegistration(
                candidate=candidate_record,
                build_attempt=ready_attempt_record,
                created=False,
            )
        if latest is not None and latest.state in {"queued", "claimed", "running"}:
            candidate_record = _candidate_record(candidate)
            attempt_record = _attempt_record(latest)
            await self.session.commit()
            return CandidateRegistration(
                candidate=candidate_record,
                build_attempt=attempt_record,
                created=False,
            )

        attempt = PersonalDevCandidateBuildAttempt(
            candidate_id=candidate_id,
            subject_id=subject_id,
            subject_incarnation=subject_incarnation,
            operation_id=operation_id,
            operation_epoch=operation_epoch,
            attempt_sequence=0 if latest is None else latest.attempt_sequence + 1,
            state="queued",
            lease_epoch=0,
            created_at=now,
            updated_at=now,
        )
        self.session.add(attempt)
        candidate.status = "queued"
        candidate.failure_reason = None
        candidate.updated_at = now
        await self.session.flush()
        candidate_record = _candidate_record(candidate)
        attempt_record = _attempt_record(attempt)
        await self.session.commit()
        return CandidateRegistration(
            candidate=candidate_record,
            build_attempt=attempt_record,
            created=True,
        )

    async def claim_next_build(
        self,
        *,
        builder_id: str,
        now: datetime,
        lease_seconds: int,
    ) -> CandidateRegistration | None:
        """Claim one queued/stale build with row locking and a monotonic epoch."""

        if not builder_id or len(builder_id) > 128 or builder_id.strip() != builder_id:
            raise ValueError("builder_id must be a non-empty bounded identifier")
        if type(lease_seconds) is not int or lease_seconds <= 0:
            raise ValueError("lease_seconds must be a positive integer")
        attempt = (
            await self.session.execute(
                select(PersonalDevCandidateBuildAttempt)
                .where(
                    or_(
                        PersonalDevCandidateBuildAttempt.state == "queued",
                        (
                            PersonalDevCandidateBuildAttempt.state.in_(("claimed", "running"))
                            & (PersonalDevCandidateBuildAttempt.lease_expires_at <= now)
                        ),
                    ),
                )
                .order_by(
                    PersonalDevCandidateBuildAttempt.created_at,
                    PersonalDevCandidateBuildAttempt.id,
                )
                .with_for_update(skip_locked=True)
                .limit(1),
            )
        ).scalar_one_or_none()
        if attempt is None:
            await self.session.rollback()
            return None
        attempt.state = "claimed"
        attempt.lease_epoch += 1
        attempt.claimed_by = builder_id
        attempt.lease_expires_at = now + timedelta(seconds=lease_seconds)
        attempt.started_at = None
        attempt.finished_at = None
        attempt.failure_reason = None
        attempt.updated_at = now
        candidate = await self.session.get(PersonalDevCandidate, attempt.candidate_id)
        if candidate is None:  # pragma: no cover - FK prevents this
            raise RuntimeError("personal-dev build candidate disappeared")
        candidate.status = "building"
        candidate.failure_reason = None
        candidate.updated_at = now
        candidate_record = _candidate_record(candidate)
        attempt_record = _attempt_record(attempt)
        await self.session.commit()
        return CandidateRegistration(
            candidate=candidate_record,
            build_attempt=attempt_record,
            created=False,
        )

    async def start_build(
        self,
        *,
        attempt_id: UUID,
        builder_id: str,
        lease_epoch: int,
        now: datetime,
    ) -> PersonalDevCandidateBuildAttemptRecord:
        result = await self.session.execute(
            update(PersonalDevCandidateBuildAttempt)
            .where(
                PersonalDevCandidateBuildAttempt.id == attempt_id,
                PersonalDevCandidateBuildAttempt.claimed_by == builder_id,
                PersonalDevCandidateBuildAttempt.lease_epoch == lease_epoch,
                PersonalDevCandidateBuildAttempt.state == "claimed",
                PersonalDevCandidateBuildAttempt.lease_expires_at > now,
            )
            .values(state="running", started_at=now, updated_at=now)
            .returning(PersonalDevCandidateBuildAttempt),
        )
        row = result.scalar_one_or_none()
        if row is None:
            await self.session.rollback()
            raise PersonalDevBuildLeaseFencedError("personal-dev build lease was superseded")
        record = _attempt_record(row)
        await self.session.commit()
        return record

    async def heartbeat_build(
        self,
        *,
        attempt_id: UUID,
        builder_id: str,
        lease_epoch: int,
        now: datetime,
        lease_seconds: int,
    ) -> PersonalDevCandidateBuildAttemptRecord:
        if type(lease_seconds) is not int or lease_seconds <= 0:
            raise ValueError("lease_seconds must be a positive integer")
        result = await self.session.execute(
            update(PersonalDevCandidateBuildAttempt)
            .where(
                PersonalDevCandidateBuildAttempt.id == attempt_id,
                PersonalDevCandidateBuildAttempt.claimed_by == builder_id,
                PersonalDevCandidateBuildAttempt.lease_epoch == lease_epoch,
                PersonalDevCandidateBuildAttempt.state.in_(("claimed", "running")),
                PersonalDevCandidateBuildAttempt.lease_expires_at > now,
            )
            .values(
                lease_expires_at=now + timedelta(seconds=lease_seconds),
                updated_at=now,
            )
            .returning(PersonalDevCandidateBuildAttempt),
        )
        row = result.scalar_one_or_none()
        if row is None:
            await self.session.rollback()
            raise PersonalDevBuildLeaseFencedError("personal-dev build lease was superseded")
        record = _attempt_record(row)
        await self.session.commit()
        return record

    async def finish_build(
        self,
        *,
        attempt_id: UUID,
        builder_id: str,
        lease_epoch: int,
        now: datetime,
        publication: dict[str, object] | None = None,
        failure_reason: str | None = None,
    ) -> CandidateRegistration:
        success = publication is not None
        if success == (failure_reason is not None):
            raise ValueError("provide exactly one build outcome")
        if failure_reason is not None and (
            not failure_reason
            or len(failure_reason) > 256
            or failure_reason.strip() != failure_reason
        ):
            raise ValueError("failure_reason must be a non-empty bounded value")
        attempt = (
            await self.session.execute(
                select(PersonalDevCandidateBuildAttempt)
                .where(
                    PersonalDevCandidateBuildAttempt.id == attempt_id,
                    PersonalDevCandidateBuildAttempt.claimed_by == builder_id,
                    PersonalDevCandidateBuildAttempt.lease_epoch == lease_epoch,
                    PersonalDevCandidateBuildAttempt.state == "running",
                    PersonalDevCandidateBuildAttempt.lease_expires_at > now,
                )
                .with_for_update(),
            )
        ).scalar_one_or_none()
        if attempt is None:
            await self.session.rollback()
            raise PersonalDevBuildLeaseFencedError("personal-dev build lease was superseded")
        candidate = (
            await self.session.execute(
                select(PersonalDevCandidate)
                .where(PersonalDevCandidate.id == attempt.candidate_id)
                .with_for_update(),
            )
        ).scalar_one()
        publication_json: dict[str, object] | None = None
        publication_sha256: str | None = None
        image_manifest_digest: str | None = None
        if publication is not None:
            try:
                (
                    publication_json,
                    publication_sha256,
                    image_manifest_digest,
                ) = validate_personal_dev_candidate_publication(
                    _candidate_record(candidate),
                    publication,
                )
            except ValueError:
                await self.session.rollback()
                raise
        attempt.state = "succeeded" if success else "failed"
        attempt.lease_expires_at = None
        attempt.finished_at = now
        attempt.failure_reason = failure_reason
        attempt.updated_at = now
        candidate.status = "ready" if success else "failed"
        candidate.image_manifest_digest = image_manifest_digest
        candidate.publication_json = publication_json
        candidate.publication_sha256 = publication_sha256
        candidate.failure_reason = failure_reason
        candidate.ready_at = now if success else None
        candidate.updated_at = now
        candidate_record = _candidate_record(candidate)
        attempt_record = _attempt_record(attempt)
        await self.session.commit()
        return CandidateRegistration(
            candidate=candidate_record,
            build_attempt=attempt_record,
            created=False,
        )


__all__ = [
    "PersonalDevBuildLeaseFencedError",
    "PersonalDevBuildOperationConflictError",
    "SqlAlchemyPersonalDevCandidateStore",
]
