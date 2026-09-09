"""Owned, bounded retirement observations; deliberately not wired to a collector.

This is database retirement, not registry deletion or execution-start authority.
It trusts the existing atomic publication writer and stored completion metadata;
historical cryptographic verification remains in publication_completion. Before
activation, execution-start must serialize on the same parent fence and refuse
retired attempts, including terminal-trial verifier reservations. Registry writer
quiescence, permanent ingress denial and authenticated maintenance are separate.
"""

from __future__ import annotations

import asyncio
import hashlib
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Literal
from uuid import UUID

from sqlalchemy import or_, select, text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession
from sqlalchemy.orm import load_only

from loom.db.schema import (
    ServiceExecutionLease,
    Task,
    TaskImageAttemptRetention,
    TaskImageMaterialization,
    TaskImageMaterializationAttempt,
    TaskImagePublicationCandidate,
    TaskImagePublicationEnvelope,
    TaskImagePublicationJob,
    TaskImageRegistryCredentialGeneration,
    Trial,
    TrialTaskImageMaterialization,
)
from loom.task_image_build_plan import TaskImageBuildPlanV1
from loom_task_image_authority.publication_jobs import PublicationJobConflictError
from loom_task_image_authority.publication_store import _credential, _result
from loom_task_image_authority.registry_credentials import parse_stored_publication_candidate_v2
from loom_task_image_authority.retirement_snapshot import (
    _PUBLIC_CREDENTIAL_FIELDS,
    PreparedAttemptRetirementInventory,
    RetirementInventoryChangedError,
    RetirementInventoryUnavailableError,
    prepare_attempt_retirement_inventory,
    revalidate_retirement_inventory,
)

_JOB_FIELDS = tuple(TaskImagePublicationJob.__table__.columns.keys())
_CANDIDATE_FIELDS = tuple(TaskImagePublicationCandidate.__table__.columns.keys())
# No signature re-verification here: completion metadata is trusted, not a new
# execution grant. Check complete exact envelope identities and recording time.
_ENVELOPE_FIELDS = ("candidate_id", "component", "recorded_at")
_TRANSACTION_SECONDS = 5


@dataclass(frozen=True)
class RetirementPolicy:
    ready_grace: timedelta = timedelta(hours=168)
    abandoned_grace: timedelta = timedelta(hours=24)

    def __post_init__(self) -> None:
        for value in (self.ready_grace, self.abandoned_grace):
            if type(value) is not timedelta or not timedelta(0) < value <= timedelta(days=365):
                raise ValueError("retirement grace outside bounds")


_DEFAULT_POLICY = RetirementPolicy()


@dataclass(frozen=True)
class RetirementObservation:
    attempt_id: UUID
    status: Literal["pinned", "observing", "retired"]
    observed_at: datetime
    unreferenced_since: datetime | None
    retired_at: datetime | None
    pins: tuple[str, ...] = ()


@dataclass(frozen=True)
class _PreparedPublication:
    job_values: tuple[object, ...] | None
    candidate_values: tuple[tuple[object, ...], ...]
    envelope_values: tuple[tuple[object, ...], ...]
    operation_id: UUID | None
    completed_at: datetime | None
    images: tuple[tuple[str, str], ...]


def _values(row: object, fields: tuple[str, ...]) -> tuple[object, ...]:
    return tuple(getattr(row, name) for name in fields)


def _validate_publication(
    prepared: PreparedAttemptRetirementInventory,
    row: TaskImagePublicationJob | None,
    candidates: list[TaskImagePublicationCandidate],
    credentials: list[TaskImageRegistryCredentialGeneration],
    envelopes: tuple[tuple[object, ...], ...],
) -> _PreparedPublication:
    try:
        if len(candidates) > 128 or len(envelopes) > 128:
            raise ValueError("publication set exceeds bounds")
        if row is None:
            if envelopes:
                raise ValueError("envelopes without completed job")
            return _PreparedPublication(
                None,
                tuple(_values(item, _CANDIDATE_FIELDS) for item in candidates),
                envelopes,
                None,
                None,
                (),
            )
        job = _result(row)
        snapshot = job.snapshot
        plan = TaskImageBuildPlanV1.model_validate_json(prepared.canonical_plan)
        materialization = prepared.materialization_values
        attempt = prepared.attempt_values
        if (
            (
                snapshot.materialization_id,
                snapshot.materialization_key,
                snapshot.task_id,
                snapshot.task_checksum,
                snapshot.platform,
            )
            != (
                str(materialization[0]),
                materialization[1],
                materialization[2],
                materialization[3],
                "linux/arm64" if materialization[4] == "arm64" else "linux/amd64",
            )
            or (
                snapshot.attempt_id,
                snapshot.attempt_number,
                snapshot.lease_epoch,
                snapshot.builder_id,
                snapshot.grant_id,
                snapshot.original_claim_session_id,
                snapshot.original_claim_session_generation,
                snapshot.frozen_plan_sha256,
            )
            != (
                str(attempt[0]),
                attempt[2],
                attempt[3],
                attempt[4],
                str(attempt[5]),
                str(attempt[6]),
                attempt[7],
                attempt[9],
            )
            or snapshot.registry_origin != prepared.inventory.registry_origin
            or {item.candidate.component for item in snapshot.components}
            != {item.name for item in plan.components}
        ):
            raise ValueError("publication retirement identity changed")
        by_credential = {item.credential_id: item for item in credentials}
        by_repository = {
            item.component: item.repository for item in prepared.inventory.repositories
        }
        by_candidate = {item.component: item for item in candidates}
        if set(by_candidate) != {item.candidate.component for item in snapshot.components}:
            raise ValueError("publication candidate set changed")
        for component in snapshot.components:
            candidate = by_candidate[component.candidate.component]
            credential = by_credential[candidate.credential_id]
            _credential(
                credential, candidate=candidate, registry_origin=snapshot.registry_origin, plan=plan
            )
            if (
                parse_stored_publication_candidate_v2(
                    candidate, credential_generation=credential.generation
                )
                != component.candidate
                or candidate.repository != by_repository[candidate.component]
            ):
                raise ValueError("publication candidate binding changed")
        expected_envelopes = (
            tuple((item.candidate_id, item.component, row.completed_at) for item in candidates)
            if job.state == "completed"
            else ()
        )
        if envelopes != expected_envelopes:
            raise ValueError("publication completion envelope set changed")
        images = (
            tuple(
                (
                    item.candidate.component,
                    f"{snapshot.registry_origin.removeprefix('https://')}/{item.candidate.repository}@{item.root.digest}",
                )
                for item in snapshot.components
            )
            if job.state == "completed"
            else ()
        )
        return _PreparedPublication(
            _values(row, _JOB_FIELDS),
            tuple(_values(item, _CANDIDATE_FIELDS) for item in candidates),
            envelopes,
            row.operation_id,
            row.completed_at,
            images,
        )
    except (ValueError, TypeError, KeyError, PublicationJobConflictError) as exc:
        raise RetirementInventoryUnavailableError(
            "retirement publication metadata unavailable"
        ) from exc


async def _prepare_publication(
    engine: AsyncEngine,
    prepared: PreparedAttemptRetirementInventory,
) -> _PreparedPublication:
    attempt_id = prepared.inventory.attempt_id
    async with engine.connect() as connection:
        await connection.execution_options(isolation_level="READ COMMITTED")
        async with (
            AsyncSession(connection, expire_on_commit=False, autoflush=False) as session,
            session.begin(),
        ):
            await session.execute(text("SET TRANSACTION READ ONLY"))
            await session.execute(text("SET LOCAL statement_timeout = '5s'"))
            await session.execute(text("SET LOCAL idle_in_transaction_session_timeout = '5s'"))
            row = await session.scalar(
                select(TaskImagePublicationJob).where(
                    TaskImagePublicationJob.materialization_attempt_id == attempt_id,
                )
            )
            candidates = list(
                await session.scalars(
                    select(TaskImagePublicationCandidate)
                    .where(TaskImagePublicationCandidate.materialization_attempt_id == attempt_id)
                    .order_by(TaskImagePublicationCandidate.component)
                    .limit(129)
                )
            )
            credentials = list(
                await session.scalars(
                    select(TaskImageRegistryCredentialGeneration)
                    .options(
                        load_only(
                            *(
                                getattr(TaskImageRegistryCredentialGeneration, name)
                                for name in _PUBLIC_CREDENTIAL_FIELDS
                            ),
                            raiseload=True,
                        )
                    )
                    .where(
                        TaskImageRegistryCredentialGeneration.credential_id.in_(
                            [item.credential_id for item in candidates],
                        )
                    )
                    .limit(129)
                )
            )
            envelopes = tuple(
                tuple(item)
                for item in await session.execute(
                    select(
                        *(getattr(TaskImagePublicationEnvelope, name) for name in _ENVELOPE_FIELDS)
                    )
                    .where(TaskImagePublicationEnvelope.materialization_attempt_id == attempt_id)
                    .order_by(TaskImagePublicationEnvelope.component)
                    .limit(129)
                )
            )
    return await asyncio.to_thread(
        _validate_publication, prepared, row, candidates, credentials, envelopes
    )


async def _recheck_publication(
    session: AsyncSession,
    attempt_id: UUID,
    prepared: _PreparedPublication,
) -> TaskImagePublicationJob | None:
    job = await session.scalar(
        select(TaskImagePublicationJob)
        .where(TaskImagePublicationJob.materialization_attempt_id == attempt_id)
        .with_for_update(nowait=True)
    )
    if (None if job is None else _values(job, _JOB_FIELDS)) != prepared.job_values:
        raise RetirementInventoryChangedError("retirement publication job changed")
    candidates = tuple(
        tuple(item)
        for item in await session.execute(
            select(*(getattr(TaskImagePublicationCandidate, name) for name in _CANDIDATE_FIELDS))
            .where(TaskImagePublicationCandidate.materialization_attempt_id == attempt_id)
            .order_by(TaskImagePublicationCandidate.component)
            .limit(129)
        )
    )
    envelopes = tuple(
        tuple(item)
        for item in await session.execute(
            select(*(getattr(TaskImagePublicationEnvelope, name) for name in _ENVELOPE_FIELDS))
            .where(TaskImagePublicationEnvelope.materialization_attempt_id == attempt_id)
            .order_by(TaskImagePublicationEnvelope.component)
            .limit(129)
        )
    )
    if candidates != prepared.candidate_values or envelopes != prepared.envelope_values:
        raise RetirementInventoryChangedError("retirement publication evidence changed")
    return job


def _now(clock: Callable[[], datetime], previous: datetime | None = None) -> datetime:
    value = clock()
    if type(value) is not datetime or value.utcoffset() is None:
        raise ValueError("retirement clock must be timezone-aware")
    value = value.astimezone(UTC)
    if previous is not None and value < previous:
        raise ValueError("retirement clock regressed")
    return value


async def _pins(
    session: AsyncSession,
    row: TaskImageMaterialization,
    attempt: TaskImageMaterializationAttempt,
    job: TaskImagePublicationJob | None,
    *,
    current_ready: bool,
    now: datetime,
) -> tuple[str, ...]:
    pins = []
    if (
        row.state in ("claimed", "running")
        and row.lease_epoch == attempt.lease_epoch
        and row.claimed_by == attempt.builder_id
        and row.lease_expires_at is not None
        and row.lease_expires_at > now
    ):
        pins.append("build_lease")
    if job is not None and job.state in ("queued", "running") and job.deadline > now:
        pins.append("publication_job")
    if current_ready and await session.scalar(
        select(
            select(Task.id)
            .where(
                Task.id == row.task_id,
                Task.checksum.in_((row.task_checksum, "sha256:" + row.task_checksum)),
            )
            .exists()
        )
    ):
        pins.append("current_task")
    if job is not None and job.state == "completed":
        if await session.scalar(
            select(
                select(Trial.id)
                .join(
                    TrialTaskImageMaterialization,
                    TrialTaskImageMaterialization.trial_id == Trial.id,
                )
                .where(
                    TrialTaskImageMaterialization.materialization_id == row.id,
                    Trial.state.not_in(("succeeded", "failed", "cancelled")),
                )
                .exists()
            )
        ):
            pins.append("nonterminal_trial")
        if await session.scalar(
            select(
                select(ServiceExecutionLease.id)
                .join(
                    TrialTaskImageMaterialization,
                    TrialTaskImageMaterialization.trial_id == ServiceExecutionLease.trial_id,
                )
                .where(
                    TrialTaskImageMaterialization.materialization_id == row.id,
                    or_(
                        ServiceExecutionLease.deleted_at.is_(None),
                        ServiceExecutionLease.cleanup_state != "complete",
                    ),
                )
                .exists()
            )
        ):
            pins.append("execution_lease")
    return tuple(pins)


async def observe_or_retire_attempt(
    engine: AsyncEngine,
    *,
    attempt_id: UUID,
    registry_origin: str,
    clock: Callable[[], datetime],
    policy: RetirementPolicy = _DEFAULT_POLICY,
) -> RetirementObservation:
    """Prepare owned evidence, observe pins and atomically freeze exact retirement.

    Precommit errors/cancellation roll back the WHOLE transaction. A transport
    failure during COMMIT can leave its outcome unknown: retry from preparation
    and recover the immutable marker, never infer rollback from a lost response.
    The future collector must count skips/backlog. Grace measures observations,
    not continuous absence. No registry/filesystem I/O or caller session is used.
    """
    prepared = await prepare_attempt_retirement_inventory(
        engine,
        attempt_id=attempt_id,
        registry_origin=registry_origin,
    )
    publication = await _prepare_publication(engine, prepared)
    # asyncio's deadline uses the event loop's monotonic clock, including commit.
    async with asyncio.timeout(_TRANSACTION_SECONDS):
        async with engine.connect() as connection:
            await connection.execution_options(isolation_level="READ COMMITTED")
            async with (
                AsyncSession(connection, expire_on_commit=False, autoflush=False) as session,
                session.begin(),
            ):
                await session.execute(text("SET LOCAL statement_timeout = '1s'"))
                await session.execute(text("SET LOCAL idle_in_transaction_session_timeout = '1s'"))
                await session.execute(text("LOCK TABLE public.tasks IN SHARE MODE NOWAIT"))
                row = await session.scalar(
                    select(TaskImageMaterialization)
                    .options(
                        load_only(
                            TaskImageMaterialization.id,
                            TaskImageMaterialization.task_id,
                            TaskImageMaterialization.task_checksum,
                            TaskImageMaterialization.state,
                            TaskImageMaterialization.lease_epoch,
                            TaskImageMaterialization.claimed_by,
                            TaskImageMaterialization.lease_expires_at,
                            TaskImageMaterialization.registry_images,
                            TaskImageMaterialization.ready_at,
                            TaskImageMaterialization.ready_publication_operation_id,
                            raiseload=True,
                        )
                    )
                    .where(TaskImageMaterialization.id == prepared.inventory.materialization_id)
                    .with_for_update(nowait=True)
                )
                attempt = await session.scalar(
                    select(TaskImageMaterializationAttempt)
                    .options(
                        load_only(
                            TaskImageMaterializationAttempt.id,
                            TaskImageMaterializationAttempt.lease_epoch,
                            TaskImageMaterializationAttempt.builder_id,
                            raiseload=True,
                        )
                    )
                    .where(TaskImageMaterializationAttempt.id == attempt_id)
                    .with_for_update(nowait=True)
                )
                if row is None or attempt is None:
                    raise RetirementInventoryChangedError("retirement parent unavailable")
                await revalidate_retirement_inventory(session, prepared=prepared)
                job = await _recheck_publication(session, attempt_id, publication)
                marker = await session.scalar(
                    select(TaskImageAttemptRetention)
                    .where(
                        TaskImageAttemptRetention.attempt_id == attempt_id,
                    )
                    .with_for_update(nowait=True)
                )
                if marker is not None and marker.retired_at is not None:
                    if marker.canonical_inventory != prepared.inventory.canonical_bytes:
                        raise RetirementInventoryChangedError("retired inventory changed")
                    return RetirementObservation(
                        attempt_id,
                        "retired",
                        marker.observed_at,
                        marker.unreferenced_since,
                        marker.retired_at,
                    )
                now = _now(clock, None if marker is None else marker.observed_at)
                current_ready = (
                    publication.operation_id is not None
                    and row.ready_publication_operation_id == publication.operation_id
                )
                if current_ready and (
                    publication.completed_at is None
                    or row.state != "ready"
                    or row.ready_at != publication.completed_at
                    or row.registry_images != dict(publication.images)
                ):
                    raise RetirementInventoryUnavailableError(
                        "current ready publication binding changed"
                    )
                pins = await _pins(session, row, attempt, job, current_ready=current_ready, now=now)
                now = _now(clock, now)
                if marker is None:
                    marker = TaskImageAttemptRetention(attempt_id=attempt_id, observed_at=now)
                    session.add(marker)
                marker.observed_at = now
                status: Literal["pinned", "observing", "retired"] = (
                    "pinned" if pins else "observing"
                )
                if pins:
                    marker.unreferenced_since = None
                else:
                    if marker.unreferenced_since is None:
                        marker.unreferenced_since = now
                    grace = (
                        policy.ready_grace
                        if publication.completed_at is not None
                        else policy.abandoned_grace
                    )
                    if now - marker.unreferenced_since >= grace:
                        marker.retired_at = now
                        marker.canonical_inventory = prepared.inventory.canonical_bytes
                        marker.inventory_sha256 = hashlib.sha256(
                            marker.canonical_inventory
                        ).hexdigest()
                        status = "retired"
                        if current_ready:
                            row.registry_images = {}
                            row.ready_at = row.ready_publication_operation_id = None
                            row.state, row.updated_at = "retired", now
                await session.flush()
                _now(clock, now)
                result = RetirementObservation(
                    attempt_id,
                    status,
                    marker.observed_at,
                    marker.unreferenced_since,
                    marker.retired_at,
                    pins,
                )
    return result
