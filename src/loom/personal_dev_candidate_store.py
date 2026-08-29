"""SQLAlchemy authority for immutable personal-dev candidates and build leases."""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timedelta
from typing import cast
from uuid import UUID

from sqlalchemy import and_, func, or_, select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import aliased
from sqlalchemy.sql.elements import ColumnElement

from loom.db.schema import (
    DevInstance,
    DevLifecycleOperation,
    PersonalDevCandidate,
    PersonalDevCandidateArtifactCollection,
    PersonalDevCandidateBuildAttempt,
)
from loom.personal_dev_candidate import (
    BuildAttemptState,
    CandidateArtifactState,
    CandidateRegistration,
    CandidateStatus,
    PersonalDevArtifactCollectionInProgressError,
    PersonalDevCandidateBuildAttemptRecord,
    PersonalDevCandidateLimits,
    PersonalDevCandidateQuotaError,
    PersonalDevCandidateRecord,
    validate_personal_dev_candidate_publication,
)
from loom.personal_dev_candidate_gc import (
    PersonalDevArtifactGcAuthorityUnavailableError,
    PersonalDevArtifactGcClaim,
    PersonalDevArtifactGcManifest,
    build_personal_dev_artifact_gc_manifest,
    validate_personal_dev_registry_prefix,
)


class PersonalDevBuildLeaseFencedError(RuntimeError):
    """The attempt lease no longer belongs to the caller."""


class PersonalDevBuildOperationConflictError(RuntimeError):
    """One lifecycle operation was already bound to a different candidate."""


class PersonalDevArtifactGcLeaseFencedError(RuntimeError):
    """The artifact deletion lease no longer belongs to the caller."""


_BUILD_CLAIM_LOCK_DIGEST = hashlib.sha256(b"personal-dev-build-claim-v1").digest()
_BUILD_CLAIM_LOCK_KEYS = (
    int.from_bytes(_BUILD_CLAIM_LOCK_DIGEST[:4], byteorder="big", signed=True),
    int.from_bytes(_BUILD_CLAIM_LOCK_DIGEST[4:8], byteorder="big", signed=True),
)


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


def _owner_lock_keys(owner_user_id: UUID) -> tuple[int, int]:
    digest = hashlib.sha256(b"personal-dev-candidate-owner-v1\0" + owner_user_id.bytes).digest()
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
        source_generation_id=row.source_generation_id,
        archive_size_bytes=row.archive_size_bytes,
        status=cast(CandidateStatus, row.status),
        image_manifest_digest=row.image_manifest_digest,
        publication_json=row.publication_json,
        publication_sha256=row.publication_sha256,
        failure_reason=row.failure_reason,
        created_at=row.created_at,
        updated_at=row.updated_at,
        ready_at=row.ready_at,
        registry_prefix=row.registry_prefix,
        artifact_state=cast(CandidateArtifactState, row.artifact_state),
        artifact_gc_lease_epoch=row.artifact_gc_lease_epoch,
        artifact_gc_unreferenced_at=row.artifact_gc_unreferenced_at,
        artifact_gc_claimed_by=row.artifact_gc_claimed_by,
        artifact_gc_blocked_reason=row.artifact_gc_blocked_reason,
        artifact_gc_lease_expires_at=row.artifact_gc_lease_expires_at,
        artifact_gc_manifest_json=row.artifact_gc_manifest_json,
        artifact_gc_manifest_sha256=row.artifact_gc_manifest_sha256,
        artifact_collected_at=row.artifact_collected_at,
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


def _same_registration(
    existing: PersonalDevCandidateRecord,
    requested: PersonalDevCandidateRecord,
) -> bool:
    return (
        existing.owner_user_id == requested.owner_user_id
        and existing.owner_team_id == requested.owner_team_id
        and existing.candidate_sha == requested.candidate_sha
        and existing.source_sha256 == requested.source_sha256
        and existing.archive_sha256 == requested.archive_sha256
        and existing.build_contract_sha256 == requested.build_contract_sha256
        and existing.source_commit == requested.source_commit
        and existing.dirty is requested.dirty
        and _canonical_json(existing.manifest_json) == _canonical_json(requested.manifest_json)
        and existing.object_bucket == requested.object_bucket
        and existing.archive_size_bytes == requested.archive_size_bytes
    )


def _canonical_json(value: object) -> str:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    )


class SqlAlchemyPersonalDevCandidateStore:
    """Request-scoped candidate registry with idempotent source identity."""

    def __init__(
        self,
        session: AsyncSession,
        *,
        limits: PersonalDevCandidateLimits | None = None,
    ) -> None:
        self.session = session
        self._limits = limits or PersonalDevCandidateLimits()

    async def register(
        self,
        requested: PersonalDevCandidateRecord,
    ) -> CandidateRegistration:
        expected_object_key = (
            f"personal-dev/sources/{requested.owner_team_id}/{requested.owner_user_id}/"
            f"{requested.candidate_sha}/{requested.source_generation_id}/"
            f"{requested.archive_sha256}.tar"
        )
        if (
            requested.object_key != expected_object_key
            or requested.source_generation_id != requested.id
            or not requested.object_bucket
            or requested.object_bucket.strip() != requested.object_bucket
            or "/" in requested.object_bucket
        ):
            raise ValueError("personal-dev candidate source object binding is invalid")
        lock_a, lock_b = _owner_lock_keys(requested.owner_user_id)
        await self.session.execute(select(func.pg_advisory_xact_lock(lock_a, lock_b)))
        existing = (
            await self.session.execute(
                select(PersonalDevCandidate)
                .where(
                    PersonalDevCandidate.owner_user_id == requested.owner_user_id,
                    PersonalDevCandidate.owner_team_id == requested.owner_team_id,
                    PersonalDevCandidate.source_sha256 == requested.source_sha256,
                    PersonalDevCandidate.archive_sha256 == requested.archive_sha256,
                    PersonalDevCandidate.build_contract_sha256 == requested.build_contract_sha256,
                )
                .with_for_update(),
            )
        ).scalar_one_or_none()
        if existing is not None:
            candidate_record = _candidate_record(existing)
            if not _same_registration(candidate_record, requested):
                await self.session.rollback()
                raise ValueError("personal-dev candidate replay changed an immutable binding")
            if existing.artifact_state == "collecting":
                await self.session.rollback()
                raise PersonalDevArtifactCollectionInProgressError(
                    "personal-dev candidate artifacts are being collected"
                )
            if existing.artifact_state == "collected":
                retained_count, retained_bytes = (
                    await self.session.execute(
                        select(
                            func.count(PersonalDevCandidate.id),
                            func.coalesce(func.sum(PersonalDevCandidate.archive_size_bytes), 0),
                        ).where(
                            PersonalDevCandidate.owner_user_id == requested.owner_user_id,
                            PersonalDevCandidate.artifact_state != "collected",
                        ),
                    )
                ).one()
                if int(retained_count) + 1 > self._limits.per_owner_retained_candidates:
                    await self.session.rollback()
                    raise PersonalDevCandidateQuotaError(
                        "personal-dev retained candidate count limit is exhausted"
                    )
                if (
                    int(retained_bytes) + requested.archive_size_bytes
                    > self._limits.per_owner_retained_archive_bytes
                ):
                    await self.session.rollback()
                    raise PersonalDevCandidateQuotaError(
                        "personal-dev retained candidate byte limit is exhausted"
                    )
                existing.status = "uploaded"
                existing.image_manifest_digest = None
                existing.publication_json = None
                existing.publication_sha256 = None
                existing.failure_reason = None
                existing.ready_at = None
                existing.registry_prefix = None
                existing.object_key = requested.object_key
                existing.source_generation_id = requested.source_generation_id
                existing.artifact_state = "retained"
                existing.artifact_gc_unreferenced_at = None
                existing.artifact_gc_claimed_by = None
                existing.artifact_gc_blocked_reason = None
                existing.artifact_gc_lease_expires_at = None
                existing.artifact_gc_manifest_json = None
                existing.artifact_gc_manifest_sha256 = None
                existing.artifact_collected_at = None
                existing.updated_at = requested.updated_at
                candidate_record = _candidate_record(existing)
            else:
                # An exact, freshly verified re-upload proves the canonical
                # source object exists again and restarts its unreferenced grace.
                existing.artifact_gc_unreferenced_at = None
                existing.artifact_gc_blocked_reason = None
                existing.updated_at = requested.updated_at
                candidate_record = _candidate_record(existing)
            await self.session.commit()
            return CandidateRegistration(
                candidate=candidate_record,
                build_attempt=None,
                created=False,
            )
        retained_count, retained_bytes = (
            await self.session.execute(
                select(
                    func.count(PersonalDevCandidate.id),
                    func.coalesce(func.sum(PersonalDevCandidate.archive_size_bytes), 0),
                ).where(
                    PersonalDevCandidate.owner_user_id == requested.owner_user_id,
                    PersonalDevCandidate.artifact_state != "collected",
                ),
            )
        ).one()
        if int(retained_count) + 1 > self._limits.per_owner_retained_candidates:
            await self.session.rollback()
            raise PersonalDevCandidateQuotaError(
                "personal-dev retained candidate count limit is exhausted"
            )
        if (
            int(retained_bytes) + requested.archive_size_bytes
            > self._limits.per_owner_retained_archive_bytes
        ):
            await self.session.rollback()
            raise PersonalDevCandidateQuotaError(
                "personal-dev retained candidate byte limit is exhausted"
            )
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
                    source_generation_id=requested.source_generation_id,
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

    async def reconcile_registration(
        self,
        requested: PersonalDevCandidateRecord,
    ) -> CandidateRegistration | None:
        """Read a potentially committed exact upload after a register error."""
        await self.session.rollback()
        self.session.expire_all()
        candidate = (
            await self.session.execute(
                select(PersonalDevCandidate)
                .where(
                    PersonalDevCandidate.object_bucket == requested.object_bucket,
                    PersonalDevCandidate.object_key == requested.object_key,
                    PersonalDevCandidate.source_generation_id == requested.source_generation_id,
                )
                .execution_options(populate_existing=True)
            )
        ).scalar_one_or_none()
        if candidate is None:
            return None
        attempt = (
            await self.session.execute(
                select(PersonalDevCandidateBuildAttempt)
                .where(PersonalDevCandidateBuildAttempt.candidate_id == candidate.id)
                .order_by(PersonalDevCandidateBuildAttempt.attempt_sequence.desc())
                .limit(1)
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

    @staticmethod
    def _artifact_unreferenced() -> ColumnElement[bool]:
        active_environment = (
            select(DevInstance.name)
            .where(
                DevInstance.candidate_id == PersonalDevCandidate.id,
                DevInstance.status != "deleted",
            )
            .exists()
        )
        active_operation = (
            select(DevLifecycleOperation.id)
            .where(
                DevLifecycleOperation.candidate_id == PersonalDevCandidate.id,
                DevLifecycleOperation.state.in_(
                    ("requested", "running", "activating", "cancelling")
                ),
            )
            .exists()
        )
        active_build = (
            select(PersonalDevCandidateBuildAttempt.id)
            .where(
                PersonalDevCandidateBuildAttempt.candidate_id == PersonalDevCandidate.id,
                PersonalDevCandidateBuildAttempt.state.in_(("queued", "claimed", "running")),
            )
            .exists()
        )
        return and_(~active_environment, ~active_operation, ~active_build)

    async def mark_next_artifact_gc(self, *, now: datetime) -> bool:
        """Start the grace clock for one exact, currently unreferenced candidate."""

        if now.tzinfo is None:
            raise ValueError("personal-dev artifact GC time must include a timezone")
        candidate = (
            await self.session.execute(
                select(PersonalDevCandidate)
                .where(
                    PersonalDevCandidate.artifact_state == "retained",
                    PersonalDevCandidate.artifact_gc_unreferenced_at.is_(None),
                    PersonalDevCandidate.artifact_gc_blocked_reason.is_(None),
                    PersonalDevCandidate.status.in_(("uploaded", "ready", "failed")),
                    self._artifact_unreferenced(),
                )
                .order_by(PersonalDevCandidate.created_at, PersonalDevCandidate.id)
                .with_for_update(skip_locked=True)
                .limit(1),
            )
        ).scalar_one_or_none()
        if candidate is None:
            await self.session.rollback()
            return False
        candidate.artifact_gc_unreferenced_at = now
        candidate.updated_at = now
        await self.session.commit()
        return True

    async def claim_next_artifact_gc(
        self,
        *,
        collector_id: str,
        now: datetime,
        retention_seconds: int,
        lease_seconds: int,
    ) -> PersonalDevArtifactGcClaim | None:
        """Lease one grace-expired mark or an expired prior collection."""

        if not collector_id or collector_id.strip() != collector_id or len(collector_id) > 128:
            raise ValueError("personal-dev artifact collector identifier is invalid")
        if now.tzinfo is None:
            raise ValueError("personal-dev artifact GC time must include a timezone")
        if type(retention_seconds) is not int or retention_seconds < 0:
            raise ValueError("personal-dev artifact retention must be non-negative")
        if type(lease_seconds) is not int or lease_seconds <= 0:
            raise ValueError("personal-dev artifact GC lease must be positive")
        cutoff = now - timedelta(seconds=retention_seconds)
        retained_eligible = and_(
            PersonalDevCandidate.artifact_state == "retained",
            PersonalDevCandidate.artifact_gc_blocked_reason.is_(None),
            PersonalDevCandidate.artifact_gc_unreferenced_at <= cutoff,
            PersonalDevCandidate.status.in_(("uploaded", "ready", "failed")),
            self._artifact_unreferenced(),
        )
        expired_claim = and_(
            PersonalDevCandidate.artifact_state == "collecting",
            PersonalDevCandidate.artifact_gc_lease_expires_at <= now,
        )
        candidate = (
            await self.session.execute(
                select(PersonalDevCandidate)
                .where(or_(retained_eligible, expired_claim))
                .order_by(
                    PersonalDevCandidate.artifact_gc_unreferenced_at,
                    PersonalDevCandidate.id,
                )
                .with_for_update(skip_locked=True)
                .limit(1),
            )
        ).scalar_one_or_none()
        if candidate is None:
            await self.session.rollback()
            return None
        if candidate.artifact_state == "collecting":
            if (
                candidate.artifact_gc_manifest_json is None
                or candidate.artifact_gc_manifest_sha256 is None
            ):
                await self.session.rollback()
                raise RuntimeError("personal-dev artifact GC manifest disappeared")
            try:
                manifest = PersonalDevArtifactGcManifest.from_json(
                    candidate.artifact_gc_manifest_json,
                    candidate.artifact_gc_manifest_sha256,
                )
            except ValueError:
                # A persisted claim should already be valid. Quarantine any
                # privileged or out-of-band corruption without allowing one
                # row to starve all later collection work.
                candidate.artifact_state = "retained"
                candidate.artifact_gc_claimed_by = None
                candidate.artifact_gc_lease_expires_at = None
                candidate.artifact_gc_manifest_json = None
                candidate.artifact_gc_manifest_sha256 = None
                candidate.artifact_gc_blocked_reason = "manifest_authority_invalid"
                candidate.updated_at = now
                await self.session.commit()
                return None
        else:
            attempt_rows = (
                (
                    await self.session.execute(
                        select(PersonalDevCandidateBuildAttempt)
                        .where(PersonalDevCandidateBuildAttempt.candidate_id == candidate.id)
                        .order_by(
                            PersonalDevCandidateBuildAttempt.attempt_sequence,
                            PersonalDevCandidateBuildAttempt.id,
                        ),
                    )
                )
                .scalars()
                .all()
            )
            try:
                manifest = build_personal_dev_artifact_gc_manifest(
                    _candidate_record(candidate),
                    [_attempt_record(attempt) for attempt in attempt_rows],
                )
            except PersonalDevArtifactGcAuthorityUnavailableError:
                candidate.artifact_gc_blocked_reason = "registry_authority_unavailable"
                candidate.updated_at = now
                await self.session.commit()
                return None
            except ValueError:
                candidate.artifact_gc_blocked_reason = "manifest_authority_invalid"
                candidate.updated_at = now
                await self.session.commit()
                return None
            candidate.artifact_state = "collecting"
            candidate.artifact_gc_blocked_reason = None
            candidate.artifact_gc_manifest_json = manifest.payload()
            candidate.artifact_gc_manifest_sha256 = manifest.manifest_sha256
        candidate.artifact_gc_lease_epoch += 1
        candidate.artifact_gc_claimed_by = collector_id
        candidate.artifact_gc_lease_expires_at = now + timedelta(seconds=lease_seconds)
        candidate.updated_at = now
        claim = PersonalDevArtifactGcClaim(
            candidate_id=candidate.id,
            collector_id=collector_id,
            lease_epoch=candidate.artifact_gc_lease_epoch,
            lease_expires_at=candidate.artifact_gc_lease_expires_at,
            manifest=manifest,
        )
        await self.session.commit()
        return claim

    async def finish_artifact_gc(
        self,
        *,
        candidate_id: UUID,
        collector_id: str,
        lease_epoch: int,
        manifest_sha256: str,
        now: datetime,
    ) -> None:
        """Commit collection only for the exact current deletion authority."""

        if now.tzinfo is None:
            raise ValueError("personal-dev artifact GC time must include a timezone")
        candidate = (
            await self.session.execute(
                select(PersonalDevCandidate)
                .where(
                    PersonalDevCandidate.id == candidate_id,
                    PersonalDevCandidate.artifact_state == "collecting",
                    PersonalDevCandidate.artifact_gc_claimed_by == collector_id,
                    PersonalDevCandidate.artifact_gc_lease_epoch == lease_epoch,
                    PersonalDevCandidate.artifact_gc_manifest_sha256 == manifest_sha256,
                    PersonalDevCandidate.artifact_gc_lease_expires_at > now,
                )
                .with_for_update(),
            )
        ).scalar_one_or_none()
        if candidate is None:
            await self.session.rollback()
            raise PersonalDevArtifactGcLeaseFencedError(
                "personal-dev artifact GC lease was superseded"
            )
        if (
            candidate.artifact_gc_manifest_json is None
            or candidate.artifact_gc_unreferenced_at is None
        ):
            await self.session.rollback()
            raise RuntimeError("personal-dev artifact GC evidence disappeared")
        prior_sequence = int(
            (
                await self.session.execute(
                    select(
                        func.coalesce(
                            func.max(PersonalDevCandidateArtifactCollection.collection_sequence),
                            0,
                        )
                    ).where(PersonalDevCandidateArtifactCollection.candidate_id == candidate_id),
                )
            ).scalar_one()
        )
        self.session.add(
            PersonalDevCandidateArtifactCollection(
                candidate_id=candidate_id,
                collection_sequence=prior_sequence + 1,
                collector_id=collector_id,
                gc_lease_epoch=lease_epoch,
                manifest_json=candidate.artifact_gc_manifest_json,
                manifest_sha256=manifest_sha256,
                unreferenced_at=candidate.artifact_gc_unreferenced_at,
                collected_at=now,
            )
        )
        candidate.artifact_state = "collected"
        candidate.artifact_gc_claimed_by = None
        candidate.artifact_gc_lease_expires_at = None
        candidate.artifact_collected_at = now
        candidate.updated_at = now
        await self.session.commit()

    async def heartbeat_artifact_gc(
        self,
        *,
        candidate_id: UUID,
        collector_id: str,
        lease_epoch: int,
        manifest_sha256: str,
        now: datetime,
        lease_seconds: int,
    ) -> None:
        if now.tzinfo is None:
            raise ValueError("personal-dev artifact GC time must include a timezone")
        if type(lease_seconds) is not int or lease_seconds <= 0:
            raise ValueError("personal-dev artifact GC lease must be positive")
        result = await self.session.execute(
            update(PersonalDevCandidate)
            .where(
                PersonalDevCandidate.id == candidate_id,
                PersonalDevCandidate.artifact_state == "collecting",
                PersonalDevCandidate.artifact_gc_claimed_by == collector_id,
                PersonalDevCandidate.artifact_gc_lease_epoch == lease_epoch,
                PersonalDevCandidate.artifact_gc_manifest_sha256 == manifest_sha256,
                PersonalDevCandidate.artifact_gc_lease_expires_at > now,
            )
            .values(
                artifact_gc_lease_expires_at=now + timedelta(seconds=lease_seconds),
                updated_at=now,
            )
            .returning(PersonalDevCandidate.id),
        )
        if result.scalar_one_or_none() is None:
            await self.session.rollback()
            raise PersonalDevArtifactGcLeaseFencedError(
                "personal-dev artifact GC lease was superseded"
            )
        await self.session.commit()

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
        if candidate.artifact_state != "retained":
            await self.session.rollback()
            raise KeyError("personal-dev candidate artifacts are unavailable")
        candidate.artifact_gc_unreferenced_at = None
        candidate.updated_at = now
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
        registry_prefix: str | None = None,
    ) -> CandidateRegistration | None:
        """Claim one queued/stale build with row locking and a monotonic epoch."""

        if not builder_id or len(builder_id) > 128 or builder_id.strip() != builder_id:
            raise ValueError("builder_id must be a non-empty bounded identifier")
        if type(lease_seconds) is not int or lease_seconds <= 0:
            raise ValueError("lease_seconds must be a positive integer")
        if registry_prefix is not None:
            validate_personal_dev_registry_prefix(registry_prefix)
        await self.session.execute(select(func.pg_advisory_xact_lock(*_BUILD_CLAIM_LOCK_KEYS)))
        live_global = int(
            (
                await self.session.execute(
                    select(func.count(PersonalDevCandidateBuildAttempt.id)).where(
                        PersonalDevCandidateBuildAttempt.state.in_(("claimed", "running")),
                        PersonalDevCandidateBuildAttempt.lease_expires_at > now,
                    ),
                )
            ).scalar_one()
        )
        if live_global >= self._limits.global_active_builds:
            await self.session.rollback()
            return None
        active_attempt = aliased(PersonalDevCandidateBuildAttempt)
        active_candidate = aliased(PersonalDevCandidate)
        owner_active = (
            select(func.count(active_attempt.id))
            .select_from(active_attempt)
            .join(active_candidate, active_candidate.id == active_attempt.candidate_id)
            .where(
                active_candidate.owner_user_id == PersonalDevCandidate.owner_user_id,
                active_attempt.state.in_(("claimed", "running")),
                active_attempt.lease_expires_at > now,
            )
            .correlate(PersonalDevCandidate)
            .scalar_subquery()
        )
        attempt = (
            await self.session.execute(
                select(PersonalDevCandidateBuildAttempt)
                .join(
                    PersonalDevCandidate,
                    PersonalDevCandidate.id == PersonalDevCandidateBuildAttempt.candidate_id,
                )
                .where(
                    or_(
                        PersonalDevCandidateBuildAttempt.state == "queued",
                        (
                            PersonalDevCandidateBuildAttempt.state.in_(("claimed", "running"))
                            & (PersonalDevCandidateBuildAttempt.lease_expires_at <= now)
                        ),
                    ),
                    PersonalDevCandidate.artifact_state == "retained",
                    owner_active < self._limits.per_owner_active_builds,
                )
                .order_by(
                    owner_active,
                    PersonalDevCandidateBuildAttempt.created_at,
                    PersonalDevCandidateBuildAttempt.id,
                )
                .with_for_update(of=PersonalDevCandidateBuildAttempt, skip_locked=True)
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
        if candidate.artifact_state != "retained":
            await self.session.rollback()
            return None
        if registry_prefix is not None:
            if candidate.registry_prefix is None:
                candidate.registry_prefix = registry_prefix
            elif candidate.registry_prefix != registry_prefix:
                await self.session.rollback()
                raise RuntimeError("personal-dev candidate registry prefix changed")
        candidate.status = "building"
        candidate.artifact_gc_unreferenced_at = None
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
    "PersonalDevArtifactGcLeaseFencedError",
    "PersonalDevBuildLeaseFencedError",
    "PersonalDevBuildOperationConflictError",
    "PersonalDevCandidateQuotaError",
    "SqlAlchemyPersonalDevCandidateStore",
]
