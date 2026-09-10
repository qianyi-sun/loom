"""Durable cold platform demand under management's existing build lease.

The caller authenticates the management actor and build-service installation.
This store re-reads source/lease ownership; it neither certifies that installation
nor grants membership, capacity, capabilities or native execution. Transactions
belong to the caller. Logical cancellation never constitutes physical release.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict
from datetime import datetime
from typing import cast
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from loom.db.schema import (
    PersonalDevBuildPlatformRequest,
    PersonalDevCandidate,
    PersonalDevCandidateBuildAttempt,
)
from loom.personal_dev_build_demand import (
    PersonalDevBuildDemandRequest,
    personal_build_work_identity,
    project_personal_build_demand,
)
from loom.personal_dev_build_runtime_installation import PersonalBuildRuntimeInstallation
from loom.personal_dev_candidate import CandidateRegistration, PersonalDevPlatform
from loom.personal_dev_candidate_store import _attempt_record, _candidate_record
from loom_capacity_manager.build_value_contracts import PersonalBuildMemberV1, PersonalBuildMemberV2
from loom_capacity_manager.contracts import MAX_DEMAND_BUCKETS_PER_REPORT, DemandBucketV1


def _digest(value: object) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"),
        ensure_ascii=True, allow_nan=False).encode("ascii")).hexdigest()


def _member(member: PersonalBuildMemberV1, *, allow_disabled: bool = False) -> PersonalBuildMemberV1:
    if type(member) not in (PersonalBuildMemberV1, PersonalBuildMemberV2):
        raise ValueError("platform request requires an authenticated build member")
    member = type(member).model_validate_json(member.model_dump_json())
    if member.configuration.lifecycle_state != "active" and not allow_disabled:
        raise ValueError("platform request build service is disabled")
    return member


def _installation(member: PersonalBuildMemberV1, runtime: PersonalBuildRuntimeInstallation) -> str:
    if runtime.candidate != member.acknowledgement.candidate or runtime.profiles != member.configuration.profiles:
        raise ValueError("platform request runtime differs from build service")
    return _digest({"candidate": runtime.candidate.model_dump(mode="json"),
        "execution_manifest_sha256": runtime.execution_manifest_sha256,
        "trusted_fleet_release_sha256": runtime.trusted_fleet_release_sha256,
        "template_sha256": runtime.template_sha256, "release_evidence_sha256": runtime.release_evidence_sha256,
        "profiles": [item.model_dump(mode="json") for item in runtime.profiles],
        "pools": [asdict(item) for item in runtime.pools],
        "protected_admission_sha256": member.acknowledgement.protected_admission_sha256})


def _platforms(platforms: tuple[PersonalDevPlatform, ...]) -> tuple[PersonalDevPlatform, ...]:
    if (not isinstance(platforms, tuple) or not 1 <= len(platforms) <= 2
        or len(set(platforms)) != len(platforms)
        or not set(platforms) <= {"linux/amd64", "linux/arm64"}):
        raise ValueError("platform requests require distinct native platforms")
    return tuple(sorted(platforms))


def _source(registration: CandidateRegistration) -> str:
    candidate, attempt = registration.candidate, registration.build_attempt
    if attempt is None:
        raise ValueError("platform request has no whole-attempt lease")
    return _digest({"owner_user_id": str(candidate.owner_user_id), "owner_team_id": str(candidate.owner_team_id),
        "candidate_id": str(candidate.id), "source_generation_id": str(candidate.source_generation_id),
        "candidate_sha256": candidate.candidate_sha, "source_sha256": candidate.source_sha256,
        "archive_sha256": candidate.archive_sha256, "build_contract_sha256": candidate.build_contract_sha256,
        "object_bucket": candidate.object_bucket, "object_key": candidate.object_key,
        "archive_size_bytes": candidate.archive_size_bytes,
        "attempt_id": str(attempt.id), "lease_epoch": attempt.lease_epoch, "claimed_by": attempt.claimed_by,
        "subject_id": str(attempt.subject_id), "subject_incarnation": str(attempt.subject_incarnation),
        "operation_id": str(attempt.operation_id), "operation_epoch": attempt.operation_epoch})


def _bucket(registration: CandidateRegistration, owner: UUID, platform: PersonalDevPlatform, now: datetime) -> DemandBucketV1:
    return project_personal_build_demand(owner_user_id=owner,
        requests=(PersonalDevBuildDemandRequest(registration, platform),), now=now)[0]


def _values(registration: CandidateRegistration, member: PersonalBuildMemberV1,
    installation: str, platform: PersonalDevPlatform, now: datetime, *, cancellation: bool = False,
) -> dict[str, object]:
    if registration.candidate.owner_user_id != member.owner_id:
        raise ValueError("platform request owner changed")
    if not cancellation:
        _bucket(registration, member.owner_id, platform, now)
    bucket_id, attempt_id = personal_build_work_identity(registration, platform)
    attempt = registration.build_attempt
    assert attempt is not None
    subject = member.configuration
    return dict(id=attempt_id, owner_user_id=member.owner_id,
        candidate_id=registration.candidate.id, attempt_id=attempt.id, attempt_lease_epoch=attempt.lease_epoch,
        platform=platform, subject_id=subject.subject_id, subject_incarnation=subject.subject_incarnation,
        deployment_generation=subject.deployment_generation, bucket_id=bucket_id,
        source_binding_sha256=_source(registration), runtime_installation_sha256=installation)


async def stage_platform_requests(
    session: AsyncSession, registration: CandidateRegistration, *, member: PersonalBuildMemberV1,
    runtime: PersonalBuildRuntimeInstallation, platforms: tuple[PersonalDevPlatform, ...], now: datetime,
) -> tuple[PersonalDevBuildPlatformRequest, ...]:
    """Stage exact native work atomically without requiring an idle worker."""
    member, platforms = _member(member), _platforms(platforms)
    installation = _installation(member, runtime)
    expected = sorted((_values(registration, member, installation, platform, now) for platform in platforms),
        key=lambda values: str(values["id"]))
    attempt = registration.build_attempt
    assert attempt is not None
    async with session.begin_nested():
        parent = (await session.scalars(select(PersonalDevCandidateBuildAttempt).where(
            PersonalDevCandidateBuildAttempt.id == attempt.id).with_for_update()
            .execution_options(populate_existing=True))).one_or_none()
        candidate = (await session.scalars(select(PersonalDevCandidate).where(
            PersonalDevCandidate.id == registration.candidate.id).with_for_update()
            .execution_options(populate_existing=True))).one_or_none()
        if parent is None or candidate is None or parent.claimed_by != attempt.claimed_by:
            raise ValueError("platform request parent lease changed")
        current = CandidateRegistration(candidate=_candidate_record(candidate), build_attempt=_attempt_record(parent), created=False)
        if expected != sorted((_values(current, member, installation, platform, now) for platform in platforms),
            key=lambda values: str(values["id"])):
            raise ValueError("platform request source or lease changed")
        result = []
        for values in expected:
            row = (await session.scalars(select(PersonalDevBuildPlatformRequest).where(
                PersonalDevBuildPlatformRequest.id == values["id"]).with_for_update()
                .execution_options(populate_existing=True))).one_or_none()
            if row is not None:
                if any(getattr(row, field) != value for field, value in values.items()):
                    raise ValueError("platform request installation or identity changed")
                if row.cancelled_at is not None:
                    raise ValueError("platform request was cancelled")
            else:
                row = PersonalDevBuildPlatformRequest(**values, created_at=now)
                session.add(row)
            result.append(row)
        await session.flush()
    return tuple(result)


async def pending_platform_demand(
    session: AsyncSession, *, member: PersonalBuildMemberV1,
    runtime: PersonalBuildRuntimeInstallation, now: datetime,
) -> tuple[DemandBucketV1, ...]:
    """Read current uncancelled requests; this is not a full demand snapshot.

Future assignment consumption must exclude assigned rows transactionally and
report commitments separately. The membership builder intake remains closed.
"""
    member = _member(member)
    installation = _installation(member, runtime)
    project_personal_build_demand(owner_user_id=member.owner_id, requests=(), now=now)
    request, parent, candidate = PersonalDevBuildPlatformRequest, PersonalDevCandidateBuildAttempt, PersonalDevCandidate
    rows = (await session.execute(select(request, parent, candidate)
        .join(parent, parent.id == request.attempt_id).join(candidate, candidate.id == request.candidate_id)
        .where(request.owner_user_id == member.owner_id, request.subject_id == member.configuration.subject_id,
            request.subject_incarnation == member.configuration.subject_incarnation,
            request.deployment_generation == member.configuration.deployment_generation, request.cancelled_at.is_(None),
            parent.state == "running", parent.lease_epoch == request.attempt_lease_epoch,
            parent.lease_expires_at > now, candidate.status == "building", candidate.artifact_state == "retained")
        .order_by(request.id).limit(MAX_DEMAND_BUCKETS_PER_REPORT + 1)
        .execution_options(populate_existing=True))).all()
    if len(rows) > MAX_DEMAND_BUCKETS_PER_REPORT:
        raise ValueError("platform demand exceeds its complete observation bound")
    buckets = []
    for row, attempt, source in rows:
        registration = CandidateRegistration(candidate=_candidate_record(source), build_attempt=_attempt_record(attempt), created=False)
        platform = cast(PersonalDevPlatform, row.platform)
        expected = _values(registration, member, installation, platform, now)
        if any(getattr(row, field) != value for field, value in expected.items()):
            raise ValueError("retained platform request source or installation changed")
        buckets.append(_bucket(registration, member.owner_id, platform, now))
    return tuple(sorted(buckets, key=lambda item: item.bucket_id))


async def cancel_platform_requests(
    session: AsyncSession, registration: CandidateRegistration, *, member: PersonalBuildMemberV1,
    runtime: PersonalBuildRuntimeInstallation, platforms: tuple[PersonalDevPlatform, ...], now: datetime,
) -> int:
    """Durably close each platform even when cancellation wins the staging race."""
    member, platforms = _member(member, allow_disabled=True), _platforms(platforms)
    installation = _installation(member, runtime)
    project_personal_build_demand(owner_user_id=member.owner_id, requests=(), now=now)
    attempt = registration.build_attempt
    if attempt is None or registration.candidate.owner_user_id != member.owner_id:
        raise ValueError("platform cancellation owner or attempt changed")
    expected = sorted((_values(registration, member, installation, platform, now, cancellation=True)
        for platform in platforms), key=lambda values: str(values["id"]))
    changed = 0
    async with session.begin_nested():
        # Match staging's parent -> candidate -> sorted request lock order.
        parent = (await session.scalars(select(PersonalDevCandidateBuildAttempt).where(
            PersonalDevCandidateBuildAttempt.id == attempt.id).with_for_update()
            .execution_options(populate_existing=True))).one_or_none()
        candidate = (await session.scalars(select(PersonalDevCandidate).where(
            PersonalDevCandidate.id == registration.candidate.id).with_for_update()
            .execution_options(populate_existing=True))).one_or_none()
        if parent is None or candidate is None or any(getattr(parent, field) != getattr(attempt, field)
            for field in ("id", "candidate_id", "subject_id", "subject_incarnation", "operation_id", "operation_epoch")):
            raise ValueError("platform cancellation parent identity changed")
        if attempt.lease_epoch > parent.lease_epoch or (
            attempt.lease_epoch == parent.lease_epoch and attempt.claimed_by != parent.claimed_by
        ):
            raise ValueError("platform cancellation lease identity changed")
        source = CandidateRegistration(candidate=_candidate_record(candidate), build_attempt=attempt, created=False)
        if _source(source) != _source(registration):
            raise ValueError("platform cancellation source identity changed")
        for values in expected:
            row = (await session.scalars(select(PersonalDevBuildPlatformRequest).where(
                PersonalDevBuildPlatformRequest.id == values["id"]).with_for_update()
                .execution_options(populate_existing=True))).one_or_none()
            if row is None:
                session.add(PersonalDevBuildPlatformRequest(**values, created_at=now, cancelled_at=now))
                changed += 1
            else:
                if any(getattr(row, field) != value for field, value in values.items()):
                    raise ValueError("platform cancellation installation or identity changed")
                if row.cancelled_at is None:
                    if now < row.created_at:
                        raise ValueError("platform cancellation predates request")
                    row.cancelled_at = now
                    changed += 1
        await session.flush()
    return changed
