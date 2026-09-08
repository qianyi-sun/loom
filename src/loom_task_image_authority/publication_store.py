"""Short caller-owned transactions for frozen publication work; never readiness."""

from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID

import rfc8785
from pydantic import ConfigDict, TypeAdapter
from sqlalchemy import or_, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from loom.db.schema import (
    TaskImageBuildGrant,
    TaskImageMaterialization,
    TaskImageMaterializationAttempt,
    TaskImagePublicationCandidate,
    TaskImagePublicationJob,
    TaskImageRegistryCredentialGeneration,
)
from loom.task_image_build_plan import TaskImageBuildPlanV1
from loom.task_image_materialization import task_image_materialization_key
from loom_task_image_authority.contracts import (
    TaskImageBuildGrantAuthorityV2,
    TaskImageRegistryCredentialV1,
    canonical_authority_sha256,
)
from loom_task_image_authority.publication_contracts import MAX_SAFE_INTEGER
from loom_task_image_authority.publication_jobs import (
    PublicationFailureCode,
    PublicationJob,
    PublicationJobAuthorizationError,
    PublicationJobConflictError,
    PublicationJobOwnershipError,
    PublicationSnapshot,
    PublicationSnapshotComponent,
    PublicationWorkerLease,
    canonical_snapshot_bytes,
    decode_publication_snapshot,
)
from loom_task_image_authority.registry_credentials import parse_stored_publication_candidate_v2
from loom_task_image_authority.registry_token import publication_repository
from loom_task_image_authority.store import (
    TaskImageBuildSessionAuthorization,
    validate_current_task_image_build_session,
)

Clock = Callable[[], datetime]
_ROWS = (
    TaskImageMaterialization,
    TaskImageMaterializationAttempt,
    TaskImageRegistryCredentialGeneration,
    TaskImagePublicationCandidate,
    TaskImagePublicationJob,
)
_CREDENTIAL_FIELDS: dict[str, TypeAdapter[Any]] = {
    name: TypeAdapter(field.rebuild_annotation(), config=ConfigDict(strict=True))
    for name, field in TaskImageRegistryCredentialV1.model_fields.items()
    if name != "bearer_token"
}


def _uuid(value: UUID) -> UUID:
    if type(value) is not UUID or value.int == 0:
        raise ValueError("publication identity must be a nonzero UUID")
    return value


def _counter(value: int) -> int:
    if type(value) is not int or not 0 < value <= MAX_SAFE_INTEGER:
        raise ValueError("publication counter must be a positive safe integer")
    return value


def _duration(value: float, maximum: int) -> timedelta:
    if type(value) not in (int, float) or not math.isfinite(value) or not 0 < value <= maximum:
        raise ValueError("publication duration outside bounds")
    result = timedelta(seconds=value)
    if result <= timedelta(0):
        raise ValueError("publication duration below clock resolution")
    return result


def _now(clock: Clock) -> datetime:
    value = clock()
    if type(value) is not datetime or value.utcoffset() is None:
        raise ValueError("publication clock must be timezone-aware")
    return value.astimezone(UTC)


async def _fresh(session: AsyncSession, *, jobs_only: bool = False) -> None:
    # Never flush other authority from a job-only transaction: this would acquire
    # earlier locks after a worker already acquired its job lock on a prior call.
    models = (TaskImagePublicationJob,) if jobs_only else _ROWS
    for row in (*session.new, *session.dirty, *session.deleted):
        if isinstance(row, models):
            raise PublicationJobConflictError("publication authority has unflushed changes")
    if jobs_only and (session.new or session.dirty or session.deleted):
        raise PublicationJobConflictError("worker transaction contains unrelated writes")


def _result(row: TaskImagePublicationJob) -> PublicationJob:
    try:
        snapshot = decode_publication_snapshot(row.canonical_snapshot)
        if (
            canonical_snapshot_bytes(snapshot) != row.canonical_snapshot
            or hashlib.sha256(row.canonical_snapshot).hexdigest() != row.snapshot_sha256
        ):
            raise ValueError("snapshot digest mismatch")
        for field, source in (
            ("attempt_id", "materialization_attempt_id"),
            ("materialization_id", "materialization_id"),
            ("attempt_number", "attempt_number"),
            ("lease_epoch", "lease_epoch"),
            ("builder_id", "builder_id"),
            ("grant_id", "grant_id"),
        ):
            if str(getattr(snapshot, field)) != str(getattr(row, source)):
                raise ValueError("snapshot row binding mismatch")
        lease = None
        if row.state == "running":
            if row.worker_id is None or row.worker_expires_at is None:
                raise ValueError("missing worker fence")
            lease = PublicationWorkerLease(
                owner_id=str(row.worker_id),
                generation=row.worker_generation,
                expires_at=row.worker_expires_at,
            )
        elif row.worker_id is not None or row.worker_expires_at is not None:
            raise ValueError("unexpected worker fence")
        if (
            (row.state == "failed") != (row.failure_code is not None)
            or not row.created_at < row.deadline <= row.created_at + timedelta(seconds=7200)
            or row.available_at < row.created_at
            or (lease is not None and lease.expires_at > row.deadline)
        ):
            raise ValueError("invalid job state")
        values: dict[str, Any] = dict(
            operation_id=str(row.operation_id),
            state=row.state,
            snapshot_sha256=row.snapshot_sha256,
            snapshot=snapshot,
            created_at=row.created_at,
            deadline=row.deadline,
            available_at=row.available_at,
            worker_generation=row.worker_generation,
        )
        if lease is not None:
            values["lease"] = lease
        if row.failure_code is not None:
            values["failure_code"] = row.failure_code
        return PublicationJob.model_validate(values)
    except (ValueError, TypeError, OverflowError) as exc:
        raise PublicationJobConflictError("stored publication job changed") from exc


async def _locked_job(session: AsyncSession, operation_id: UUID) -> TaskImagePublicationJob:
    _uuid(operation_id)
    await _fresh(session, jobs_only=True)
    with session.no_autoflush:
        row = await session.scalar(
            select(TaskImagePublicationJob)
            .where(TaskImagePublicationJob.operation_id == operation_id)
            .execution_options(populate_existing=True)
            .with_for_update()
        )
    if row is None:
        raise PublicationJobConflictError("publication job unavailable")
    _result(row)
    return row


async def read_publication_job(session: AsyncSession, *, operation_id: UUID) -> PublicationJob:
    """Trusted durable input read, not network authorization or ownership proof."""
    return _result(await _locked_job(session, operation_id))


def _credential(
    row: TaskImageRegistryCredentialGeneration,
    *,
    candidate: TaskImagePublicationCandidate,
    registry_origin: str,
    plan: TaskImageBuildPlanV1,
) -> None:
    payload = row.response_public_json
    if (
        set(payload)
        != (set(TaskImageRegistryCredentialV1.model_fields) - {"bearer_token"})
        | {"bearer_token_sha256"}
        or hashlib.sha256(rfc8785.dumps(payload)).hexdigest() != row.response_sha256
    ):
        raise PublicationJobConflictError("credential canonical provenance changed")
    # Reuse the exact reviewed public field types without fabricating a bearer
    # token or touching its secret store. Cross-field bindings are checked below.
    try:
        for name, adapter in _CREDENTIAL_FIELDS.items():
            parsed = adapter.validate_json(json.dumps(payload[name]))
            if adapter.dump_python(parsed, mode="json") != payload[name]:
                raise ValueError("noncanonical credential public field")
    except ValueError as exc:
        raise PublicationJobConflictError("credential public schema changed") from exc
    mapping = {"attempt_id": "materialization_attempt_id", "bearer_token_sha256": "token_hash"}
    for name in (
        "credential_id",
        "request_id",
        "grant_id",
        "session_id",
        "session_generation",
        "attestation_generation",
        "attestation_sha256",
        "materialization_id",
        "attempt_id",
        "attempt_number",
        "lease_epoch",
        "builder_id",
        "component",
        "generation",
        "predecessor_credential_id",
        "lease_heartbeat_operation_id",
        "registry_origin",
        "registry_service",
        "registry_issuer",
        "repository",
        "registry_key_id",
        "bearer_token_sha256",
        "issued_at",
        "expires_at",
    ):
        actual = getattr(row, mapping.get(name, name))
        expected = payload[name]
        if isinstance(actual, datetime):
            expected = datetime.fromisoformat(expected)
        elif isinstance(actual, UUID):
            actual = str(actual)
        elif isinstance(actual, bytes):
            actual = actual.hex()
        if actual != expected:
            raise PublicationJobConflictError("credential row provenance changed")
    if (
        payload["purpose"] != "production"
        or payload["shadow_campaign_id"] is not None
        or payload["cpu_arch"] != plan.cpu_arch
        or payload["platform"] != plan.platform
        or payload["actions"] != ["pull", "push"]
        or payload["predecessor_generation"] != (row.generation - 1 if row.generation > 1 else None)
        or row.registry_origin != registry_origin
        or not row.issued_at < row.expires_at <= row.issued_at + timedelta(seconds=45)
        or row.issued_at.microsecond != 0
        or row.expires_at.microsecond != 0
        or (
            row.generation == 1
            and (
                row.predecessor_credential_id is not None
                or row.lease_heartbeat_operation_id is not None
            )
        )
        or (
            row.generation > 1
            and (
                row.predecessor_credential_id is None
                or row.predecessor_credential_id == row.credential_id
                or row.lease_heartbeat_operation_id is None
            )
        )
    ):
        raise PublicationJobConflictError("credential destination changed")
    for field in (
        "materialization_attempt_id",
        "materialization_id",
        "attempt_number",
        "lease_epoch",
        "builder_id",
        "grant_id",
        "component",
        "repository",
    ):
        if getattr(row, field) != getattr(candidate, field):
            raise PublicationJobConflictError("credential candidate binding changed")


async def submit_publication_job(
    session: AsyncSession,
    *,
    authorization: TaskImageBuildSessionAuthorization,
    operation_id: UUID,
    materialization_id: UUID,
    attempt_id: UUID,
    lease_epoch: int,
    registry_origin: str,
    clock: Clock,
    lifetime_seconds: float = 3600,
) -> PublicationJob:
    """Bind authenticated current session to database-derived immutable input.

    The caller owns commit/rollback. No network authentication occurs here.
    Grant -> projection/current session/attestation -> materialization -> attempt
    -> credentials -> candidates -> job. No earlier lock is acquired afterward.
    """
    for value in (operation_id, materialization_id, attempt_id):
        _uuid(value)
    _counter(lease_epoch)
    lifetime = _duration(lifetime_seconds, 7200)
    await _fresh(session)
    live = await validate_current_task_image_build_session(
        session, grant_id=authorization.grant_id, clock=lambda: _now(clock)
    )
    if (
        (authorization.grant_id, authorization.session_id, authorization.session_generation)
        != (live.grant_id, live.session_id, live.session_generation)
        or live.authority_version != 2
        or live.purpose != "production"
    ):
        raise PublicationJobAuthorizationError(
            "publication requires authenticated current V2 session"
        )
    grant = await session.get(TaskImageBuildGrant, live.grant_id)
    assert grant is not None  # locked and validated by current-session validator
    authority = TaskImageBuildGrantAuthorityV2.model_validate_json(json.dumps(grant.authority_spec))
    if (
        canonical_authority_sha256(authority) != grant.authority_sha256
        or authority.slurm_cluster_id != grant.slurm_cluster_id
        or grant.slurm_job_id is None
    ):
        raise PublicationJobAuthorizationError("publication grant binding changed")
    row = await session.scalar(
        select(TaskImageMaterialization)
        .where(TaskImageMaterialization.id == materialization_id)
        .execution_options(populate_existing=True)
        .with_for_update()
    )
    attempt = await session.scalar(
        select(TaskImageMaterializationAttempt)
        .where(TaskImageMaterializationAttempt.id == attempt_id)
        .execution_options(populate_existing=True)
        .with_for_update()
    )
    if (
        row is None
        or attempt is None
        or row.state not in ("claimed", "running")
        or row.lease_epoch != lease_epoch
        or row.lease_expires_at is None
        or attempt.materialization_id != row.id
        or attempt.lease_epoch != lease_epoch
        or attempt.builder_id != row.claimed_by
        or attempt.claim_deterministic_failure_count != row.attempt_count
        or attempt.grant_id != live.grant_id
        or attempt.claim_plan_json is None
        or attempt.session_id is None
        or attempt.session_generation is None
    ):
        raise PublicationJobAuthorizationError("publication attempt lease unavailable")
    plan = TaskImageBuildPlanV1.model_validate_json(json.dumps(attempt.claim_plan_json))
    payload = plan.model_dump(mode="json", exclude_none=False)
    if (
        payload != attempt.claim_plan_json
        or hashlib.sha256(rfc8785.dumps(payload)).hexdigest() != attempt.claim_plan_sha256
        or plan.materialization_id != row.id
        or plan.grant_id != live.grant_id
        or plan.session_id != attempt.session_id
        or plan.session_generation != attempt.session_generation
        or plan.builder_id != attempt.builder_id
        or plan.task_id != row.task_id
        or plan.task_checksum != row.task_checksum
        or plan.cpu_arch != row.cpu_arch
        or plan.cpu_arch != live.cpu_arch
        or row.materialization_key
        != task_image_materialization_key(
            task_id=row.task_id, task_checksum=row.task_checksum, cpu_arch=row.cpu_arch
        )
    ):
        raise PublicationJobConflictError("frozen publication claim plan changed")
    # Credential-before-candidate matches candidate recording. Lock all retained
    # credential generations deterministically before loading the complete set.
    credentials = list(
        await session.scalars(
            select(TaskImageRegistryCredentialGeneration)
            .where(TaskImageRegistryCredentialGeneration.materialization_attempt_id == attempt_id)
            .order_by(TaskImageRegistryCredentialGeneration.credential_id)
            .execution_options(populate_existing=True)
            .with_for_update()
        )
    )
    candidates = list(
        await session.scalars(
            select(TaskImagePublicationCandidate)
            .where(TaskImagePublicationCandidate.materialization_attempt_id == attempt_id)
            .order_by(TaskImagePublicationCandidate.component)
            .execution_options(populate_existing=True)
            .with_for_update()
        )
    )
    candidates.sort(key=lambda item: (item.component != "task", item.component))
    if tuple(item.component for item in candidates) != tuple(item.name for item in plan.components):
        raise PublicationJobConflictError("publication candidate set is incomplete")
    by_id = {item.credential_id: item for item in credentials}
    components = []
    for candidate in candidates:
        credential = by_id.get(candidate.credential_id)
        if credential is None:
            raise PublicationJobConflictError("publication credential missing")
        _credential(credential, candidate=candidate, registry_origin=registry_origin, plan=plan)
        acknowledgement = parse_stored_publication_candidate_v2(
            candidate, credential_generation=credential.generation
        )
        if candidate.repository != publication_repository(
            purpose="production",
            shadow_campaign_id=None,
            cpu_arch=plan.cpu_arch,
            attempt_id=attempt.id,
            component=candidate.component,
        ):
            raise PublicationJobConflictError("publication destination changed")
        components.append(
            PublicationSnapshotComponent(
                candidate=acknowledgement, candidate_sha256=candidate.response_sha256
            )
        )
    existing = await session.scalar(
        select(TaskImagePublicationJob)
        .where(
            or_(
                TaskImagePublicationJob.operation_id == operation_id,
                TaskImagePublicationJob.materialization_attempt_id == attempt_id,
            )
        )
        .order_by(TaskImagePublicationJob.operation_id)
        .execution_options(populate_existing=True)
        .with_for_update()
    )
    previous = _result(existing) if existing is not None else None
    snapshot = PublicationSnapshot.model_validate(
        dict(
            schema="loom.task-image-publication-job-input/v1",
            materialization_id=str(row.id),
            materialization_key=row.materialization_key,
            task_id=row.task_id,
            task_checksum=row.task_checksum,
            platform=plan.platform,
            purpose="production",
            attempt_id=str(attempt.id),
            attempt_number=attempt.attempt_number,
            lease_epoch=attempt.lease_epoch,
            builder_id=attempt.builder_id,
            grant_id=str(live.grant_id),
            original_claim_session_id=str(attempt.session_id),
            original_claim_session_generation=attempt.session_generation,
            frozen_plan_sha256=attempt.claim_plan_sha256,
            environment=live.environment,
            pool_id=live.pool_id,
            slurm_cluster_id=grant.slurm_cluster_id,
            slurm_job_id=grant.slurm_job_id,
            build_policy_sha256=authority.build_policy_sha256,
            builder_release_sha256=live.builder_release_sha256,
            supervisor_executable_sha256=live.supervisor_executable_sha256,
            containment_attestation_sha256=(
                previous.snapshot.containment_attestation_sha256
                if previous
                else live.attestation_sha256
            ),
            registry_origin=registry_origin,
            components=tuple(components),
        )
    )
    encoded = canonical_snapshot_bytes(snapshot)
    digest = hashlib.sha256(encoded).hexdigest()
    now = _now(clock)
    _live_at(live, row, now)
    if previous is not None:
        if previous.operation_id != str(operation_id) or previous.snapshot != snapshot:
            raise PublicationJobConflictError("publication operation binding conflict")
        return previous  # terminal failures remain terminal; deadline never resets
    # Different attempts can race on the globally unique operation ID. DO NOTHING
    # waits on its unique-index owner without aborting the caller's transaction.
    inserted = await session.scalar(
        insert(TaskImagePublicationJob)
        .values(
            operation_id=operation_id,
            materialization_attempt_id=attempt_id,
            materialization_id=row.id,
            attempt_number=attempt.attempt_number,
            lease_epoch=lease_epoch,
            builder_id=attempt.builder_id,
            grant_id=live.grant_id,
            canonical_snapshot=encoded,
            snapshot_sha256=digest,
            created_at=now,
            deadline=min(now + lifetime, live.grant_expires_at),
            available_at=now,
            state="queued",
            worker_generation=0,
        )
        .on_conflict_do_nothing()
        .returning(TaskImagePublicationJob.operation_id)
    )
    _live_at(live, row, _now(clock))  # includes a possible unique-index wait
    if inserted is None:
        raise PublicationJobConflictError("publication operation binding conflict")
    stored = await session.get(TaskImagePublicationJob, inserted)
    assert stored is not None
    return _result(stored)


def _live_at(
    live: TaskImageBuildSessionAuthorization, row: TaskImageMaterialization, now: datetime
) -> None:
    if row.lease_expires_at is None or now >= min(
        live.grant_expires_at,
        live.session_expires_at,
        live.attestation_expires_at,
        row.lease_expires_at,
    ):
        raise PublicationJobAuthorizationError("publication live authority expired")


async def claim_publication_job(
    session: AsyncSession,
    *,
    operation_id: UUID,
    owner_id: UUID,
    clock: Clock,
    lease_seconds: float = 60,
) -> PublicationJob:
    _uuid(owner_id)
    duration = _duration(lease_seconds, 300)
    row = await _locked_job(session, operation_id)
    now = _now(clock)
    if (
        now >= row.deadline
        or now < row.available_at
        or row.worker_generation >= MAX_SAFE_INTEGER
        or row.state not in ("queued", "running")
        or (
            row.state == "running"
            and row.worker_expires_at is not None
            and now < row.worker_expires_at
        )
        or row.worker_id == owner_id
    ):
        raise PublicationJobOwnershipError("publication job cannot be claimed")
    row.state, row.worker_id = "running", owner_id
    row.worker_generation += 1
    row.worker_expires_at = min(now + duration, row.deadline)
    await session.flush([row])
    return _result(row)


def _owner(row: TaskImagePublicationJob, owner_id: UUID, generation: int, now: datetime) -> None:
    _uuid(owner_id)
    _counter(generation)
    if (
        row.state != "running"
        or row.worker_id != owner_id
        or row.worker_generation != generation
        or row.worker_expires_at is None
        or now >= min(row.worker_expires_at, row.deadline)
    ):
        raise PublicationJobOwnershipError("publication worker fence lost")


async def renew_publication_job(
    session: AsyncSession,
    *,
    operation_id: UUID,
    owner_id: UUID,
    generation: int,
    clock: Clock,
    lease_seconds: float = 60,
) -> PublicationJob:
    duration = _duration(lease_seconds, 300)
    row = await _locked_job(session, operation_id)
    now = _now(clock)
    _owner(row, owner_id, generation, now)
    row.worker_expires_at = min(now + duration, row.deadline)
    await session.flush([row])
    return _result(row)


async def release_publication_job(
    session: AsyncSession,
    *,
    operation_id: UUID,
    owner_id: UUID,
    generation: int,
    clock: Clock,
    retry_delay_seconds: float = 5,
) -> PublicationJob:
    delay = _duration(retry_delay_seconds, 300)
    row = await _locked_job(session, operation_id)
    now = _now(clock)
    _owner(row, owner_id, generation, now)
    row.state, row.worker_id, row.worker_expires_at = "queued", None, None
    row.available_at = now + delay
    await session.flush([row])
    return _result(row)


async def fail_publication_job(
    session: AsyncSession,
    *,
    operation_id: UUID,
    owner_id: UUID,
    generation: int,
    clock: Clock,
    failure_code: PublicationFailureCode,
) -> PublicationJob:
    if failure_code not in ("integrity", "authority_lost", "verification_failed", "deadline"):
        raise ValueError("unknown safe publication failure code")
    row = await _locked_job(session, operation_id)
    _owner(row, owner_id, generation, _now(clock))
    row.state, row.worker_id, row.worker_expires_at = "failed", None, None
    row.failure_code = failure_code
    await session.flush([row])
    return _result(row)
