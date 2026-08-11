"""Transactional personal-development environment lifecycle authority."""

from __future__ import annotations

import hashlib
import re
from datetime import UTC, datetime
from typing import cast
from uuid import UUID, uuid4

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from loom.db.schema import (
    DevInstance,
    DevLifecycleOperation,
    DevLifecycleOperationAttempt,
    PersonalDevCandidate,
)
from loom.personal_dev_candidate_store import SqlAlchemyPersonalDevCandidateStore
from loom.personal_dev_environment import (
    PersonalDevApplyReservation,
    PersonalDevEnvironmentApplyRequest,
    PersonalDevEnvironmentRecord,
    PersonalDevEnvironmentStatus,
    PersonalDevLifecycleLimits,
    PersonalDevLifecycleOperationRecord,
    PersonalDevOperationKind,
    PersonalDevOperationState,
)

_DIGEST_RE = re.compile(r"[0-9a-f]{64}")
_ACTIVE_OPERATION_STATES = ("requested", "running", "activating", "cancelling")
_PUBLIC_FAILURE_REASONS = frozenset(
    {
        "activation_timeout",
        "candidate_build_failed",
        "provisioning_failed",
        "readiness_failed",
        "update_failed",
    },
)


class PersonalDevEnvironmentNotFoundError(KeyError):
    """The candidate/environment is absent or intentionally hidden from this owner."""


class PersonalDevEnvironmentConflictError(RuntimeError):
    """The request conflicts with an existing owner binding or active operation."""


class PersonalDevEnvironmentEpochFencedError(RuntimeError):
    """The caller's expected environment operation epoch is stale."""


class PersonalDevEnvironmentOperationFencedError(RuntimeError):
    """A trusted lifecycle callback no longer names the current operation."""


def _lock_keys(name: str) -> tuple[int, int]:
    digest = hashlib.sha256(b"loom-personal-dev-environment-v1\0" + name.encode("ascii")).digest()
    return (
        int.from_bytes(digest[:4], byteorder="big", signed=True),
        int.from_bytes(digest[4:8], byteorder="big", signed=True),
    )


def _environment_record(row: DevInstance) -> PersonalDevEnvironmentRecord:
    return PersonalDevEnvironmentRecord(
        name=row.name,
        subject_id=row.subject_id,
        subject_incarnation=row.subject_incarnation,
        owner_user_id=row.owner_user_id,
        owner_team_id=row.owner_team_id,
        min_slots=row.min_slots,
        max_slots=row.max_slots,
        status=cast(PersonalDevEnvironmentStatus, row.status),
        deployment_generation=row.deployment_generation,
        candidate_id=row.candidate_id,
        candidate_sha=row.candidate_sha,
        operation_epoch=row.operation_epoch,
        operation_id=row.operation_id,
        operation_step=row.operation_step,
        failure_reason=row.failure_reason,
        created_at=row.created_at,
        updated_at=row.updated_at,
        ready_at=row.ready_at,
        deleted_at=row.deleted_at,
    )


def _operation_record(row: DevLifecycleOperation) -> PersonalDevLifecycleOperationRecord:
    return PersonalDevLifecycleOperationRecord(
        id=row.id,
        idempotency_key=row.idempotency_key,
        environment_name=row.environment_name,
        subject_id=row.subject_id,
        subject_incarnation=row.subject_incarnation,
        owner_user_id=row.owner_user_id,
        owner_team_id=row.owner_team_id,
        operation_epoch=row.operation_epoch,
        expected_operation_epoch=row.expected_operation_epoch,
        kind=cast(PersonalDevOperationKind, row.kind),
        state=cast(PersonalDevOperationState, row.state),
        attempt_id=row.attempt_id,
        attempt_sequence=row.attempt_sequence,
        request_sha256=row.request_sha256,
        candidate_id=row.candidate_id,
        candidate_sha=row.candidate_sha,
        min_slots=row.min_slots,
        max_slots=row.max_slots,
        deployment_generation=row.deployment_generation,
        checkpoint=row.checkpoint,
        failure_reason=row.failure_reason,
        readiness_evidence_sha256=row.readiness_evidence_sha256,
        activation_acknowledgement_sha256=(
            row.activation_acknowledgement_sha256
        ),
        created_at=row.created_at,
        updated_at=row.updated_at,
        started_at=row.started_at,
        finished_at=row.finished_at,
    )


class SqlAlchemyPersonalDevEnvironmentAuthority:
    """Serialize apply requests and bind build admission in one transaction."""

    def __init__(
        self,
        session: AsyncSession,
        *,
        limits: PersonalDevLifecycleLimits | None = None,
    ) -> None:
        self.session = session
        self._candidates = SqlAlchemyPersonalDevCandidateStore(session)
        self._limits = limits or PersonalDevLifecycleLimits()

    async def apply(
        self,
        requested: PersonalDevEnvironmentApplyRequest,
        *,
        now: datetime | None = None,
    ) -> PersonalDevApplyReservation:
        now = now or datetime.now(UTC)
        try:
            reservation = await self._claim_apply(requested, now=now)
            if reservation.requires_build_binding:
                await self._candidates.enqueue_build(
                    candidate_id=reservation.operation.candidate_id,
                    subject_id=reservation.operation.subject_id,
                    subject_incarnation=reservation.operation.subject_incarnation,
                    operation_id=reservation.operation.id,
                    operation_epoch=reservation.operation.operation_epoch,
                    now=now,
                )
            else:
                await self.session.commit()
            return reservation
        except Exception:
            if self.session.in_transaction():
                await self.session.rollback()
            raise

    async def _claim_apply(
        self,
        requested: PersonalDevEnvironmentApplyRequest,
        *,
        now: datetime,
    ) -> PersonalDevApplyReservation:
        global_lock_a, global_lock_b = _lock_keys("\0global-admission")
        await self.session.execute(
            select(func.pg_advisory_xact_lock(global_lock_a, global_lock_b)),
        )
        lock_a, lock_b = _lock_keys(requested.name)
        await self.session.execute(select(func.pg_advisory_xact_lock(lock_a, lock_b)))

        prior = (
            await self.session.execute(
                select(DevLifecycleOperation).where(
                    DevLifecycleOperation.owner_user_id == requested.owner_user_id,
                    DevLifecycleOperation.idempotency_key == requested.idempotency_key,
                ),
            )
        ).scalar_one_or_none()
        if prior is not None:
            if (
                prior.environment_name != requested.name
                or prior.owner_team_id != requested.owner_team_id
                or prior.request_sha256 != requested.request_sha256
            ):
                raise PersonalDevEnvironmentConflictError(
                    "idempotency key is already bound to a different request",
                )
            environment = await self._locked_environment(requested.name)
            if (
                environment is None
                or environment.owner_user_id != requested.owner_user_id
                or environment.subject_id != prior.subject_id
                or environment.subject_incarnation != prior.subject_incarnation
            ):
                raise PersonalDevEnvironmentOperationFencedError(
                    "personal-dev lifecycle operation subject was superseded",
                )
            if prior.state in (*_ACTIVE_OPERATION_STATES, "failed") and (
                environment.operation_id != prior.id
                or environment.operation_epoch != prior.operation_epoch
            ):
                raise PersonalDevEnvironmentOperationFencedError(
                    "personal-dev lifecycle operation was superseded",
                )
            if prior.state == "failed":
                return await self._retry_failed_operation(prior, environment, now=now)
            prior_record = _operation_record(prior)
            return PersonalDevApplyReservation(
                environment=_environment_record(environment),
                operation=prior_record,
                acquired=False,
                requires_build_binding=(
                    prior_record.kind in {"create", "update"}
                    and prior_record.state in {"requested", "running"}
                ),
            )

        candidate = (
            await self.session.execute(
                select(PersonalDevCandidate)
                .where(PersonalDevCandidate.id == requested.candidate_id)
                .with_for_update(),
            )
        ).scalar_one_or_none()
        if (
            candidate is None
            or candidate.owner_user_id != requested.owner_user_id
            or candidate.owner_team_id != requested.owner_team_id
            or candidate.candidate_sha != requested.candidate_sha
        ):
            raise PersonalDevEnvironmentNotFoundError("personal-dev candidate not found")

        environment = await self._locked_environment(requested.name)
        operation_id = uuid4()
        if environment is None:
            if requested.expected_operation_epoch != 0:
                raise PersonalDevEnvironmentEpochFencedError(
                    "personal-dev environment does not exist at the expected epoch",
                )
            await self._assert_limits(requested, replacing_name=None)
            subject_id = uuid4()
            subject_incarnation = uuid4()
            operation_epoch = 1
            generation = 1
            kind: PersonalDevOperationKind = "create"
            state: PersonalDevOperationState = "running"
            environment = DevInstance(
                name=requested.name,
                subject_id=subject_id,
                subject_incarnation=subject_incarnation,
                owner_user_id=requested.owner_user_id,
                owner_team_id=requested.owner_team_id,
                min_slots=requested.min_slots,
                max_slots=requested.max_slots,
                status="provisioning",
                deployment_generation=generation,
                candidate_id=requested.candidate_id,
                candidate_sha=requested.candidate_sha,
                operation_epoch=operation_epoch,
                operation_id=operation_id,
                operation_step="candidate_build",
                keep_data=False,
                failure_reason=None,
                created_at=now,
                updated_at=now,
            )
            self.session.add(environment)
            await self.session.flush()
        else:
            if (
                environment.owner_user_id != requested.owner_user_id
                or environment.owner_team_id != requested.owner_team_id
            ):
                raise PersonalDevEnvironmentNotFoundError(
                    "personal-dev environment not found",
                )
            exact_prior = (
                await self.session.execute(
                    select(DevLifecycleOperation).where(
                        DevLifecycleOperation.subject_id == environment.subject_id,
                        DevLifecycleOperation.subject_incarnation
                        == environment.subject_incarnation,
                        DevLifecycleOperation.expected_operation_epoch
                        == requested.expected_operation_epoch,
                        DevLifecycleOperation.request_sha256 == requested.request_sha256,
                    ),
                )
            ).scalar_one_or_none()
            if exact_prior is not None and (
                (
                    environment.operation_id == exact_prior.id
                    and environment.operation_epoch == exact_prior.operation_epoch
                )
                or (
                    exact_prior.kind == "noop"
                    and environment.operation_epoch == requested.expected_operation_epoch
                    and environment.status == "ready"
                    and environment.candidate_id == requested.candidate_id
                    and environment.candidate_sha == requested.candidate_sha
                    and environment.min_slots == requested.min_slots
                    and environment.max_slots == requested.max_slots
                )
            ):
                if exact_prior.state == "failed":
                    return await self._retry_failed_operation(
                        exact_prior,
                        environment,
                        now=now,
                    )
                exact_record = _operation_record(exact_prior)
                return PersonalDevApplyReservation(
                    environment=_environment_record(environment),
                    operation=exact_record,
                    acquired=False,
                    requires_build_binding=(
                        exact_record.kind in {"create", "update"}
                        and exact_record.state in {"requested", "running"}
                    ),
                )
            if environment.operation_epoch != requested.expected_operation_epoch:
                raise PersonalDevEnvironmentEpochFencedError(
                    "personal-dev environment operation epoch changed",
                )
            active = (
                await self.session.execute(
                    select(DevLifecycleOperation.id).where(
                        DevLifecycleOperation.environment_name == requested.name,
                        DevLifecycleOperation.state.in_(_ACTIVE_OPERATION_STATES),
                    ),
                )
            ).scalar_one_or_none()
            if active is not None:
                raise PersonalDevEnvironmentConflictError(
                    "another personal-dev lifecycle operation is active",
                )
            subject_id = environment.subject_id
            subject_incarnation = environment.subject_incarnation
            if environment.status == "deleted":
                await self._assert_limits(requested, replacing_name=None)
                subject_incarnation = uuid4()
                operation_epoch = environment.operation_epoch + 1
                generation = 1
                kind = "create"
                state = "running"
                environment.subject_incarnation = subject_incarnation
                environment.min_slots = requested.min_slots
                environment.max_slots = requested.max_slots
                environment.status = "provisioning"
                environment.deployment_generation = generation
                environment.candidate_id = requested.candidate_id
                environment.candidate_sha = requested.candidate_sha
                environment.operation_epoch = operation_epoch
                environment.operation_id = operation_id
                environment.operation_step = "candidate_build"
                environment.keep_data = False
                environment.failure_reason = None
                environment.ready_at = None
                environment.deleted_at = None
                environment.updated_at = now
            elif environment.status == "ready":
                same_candidate = (
                    environment.candidate_id == requested.candidate_id
                    and environment.candidate_sha == requested.candidate_sha
                )
                same_policy = (
                    environment.min_slots == requested.min_slots
                    and environment.max_slots == requested.max_slots
                )
                if same_candidate and same_policy:
                    operation_epoch = environment.operation_epoch
                    generation = environment.deployment_generation
                    kind = "noop"
                    state = "succeeded"
                elif same_candidate:
                    await self._assert_limits(requested, replacing_name=environment.name)
                    operation_epoch = environment.operation_epoch + 1
                    generation = environment.deployment_generation
                    kind = "capacity"
                    state = "succeeded"
                    environment.min_slots = requested.min_slots
                    environment.max_slots = requested.max_slots
                    environment.operation_epoch = operation_epoch
                    environment.operation_id = operation_id
                    environment.operation_step = "complete"
                    environment.failure_reason = None
                    environment.updated_at = now
                else:
                    await self._assert_limits(requested, replacing_name=environment.name)
                    operation_epoch = environment.operation_epoch + 1
                    generation = environment.deployment_generation + 1
                    kind = "update"
                    state = "running"
                    environment.status = "updating"
                    environment.operation_epoch = operation_epoch
                    environment.operation_id = operation_id
                    environment.operation_step = "candidate_build"
                    environment.failure_reason = None
                    environment.updated_at = now
            elif environment.status == "failed" and environment.ready_at is None:
                await self._assert_limits(requested, replacing_name=environment.name)
                operation_epoch = environment.operation_epoch + 1
                generation = environment.deployment_generation + 1
                kind = "create"
                state = "running"
                environment.min_slots = requested.min_slots
                environment.max_slots = requested.max_slots
                environment.status = "provisioning"
                environment.deployment_generation = generation
                environment.candidate_id = requested.candidate_id
                environment.candidate_sha = requested.candidate_sha
                environment.operation_epoch = operation_epoch
                environment.operation_id = operation_id
                environment.operation_step = "candidate_build"
                environment.failure_reason = None
                environment.updated_at = now
            else:
                raise PersonalDevEnvironmentConflictError(
                    f"personal-dev environment cannot apply while {environment.status}",
                )

        finished_at = now if state == "succeeded" else None
        attempt_id = uuid4()
        operation = DevLifecycleOperation(
            id=operation_id,
            idempotency_key=requested.idempotency_key,
            environment_name=requested.name,
            subject_id=subject_id,
            subject_incarnation=subject_incarnation,
            owner_user_id=requested.owner_user_id,
            owner_team_id=requested.owner_team_id,
            operation_epoch=operation_epoch,
            expected_operation_epoch=requested.expected_operation_epoch,
            kind=kind,
            state=state,
            attempt_id=attempt_id,
            attempt_sequence=0,
            request_sha256=requested.request_sha256,
            candidate_id=requested.candidate_id,
            candidate_sha=requested.candidate_sha,
            min_slots=requested.min_slots,
            max_slots=requested.max_slots,
            deployment_generation=generation,
            checkpoint="complete" if state == "succeeded" else "candidate_build",
            created_at=now,
            updated_at=now,
            started_at=now,
            finished_at=finished_at,
        )
        self.session.add(operation)
        await self.session.flush()
        self.session.add(
            DevLifecycleOperationAttempt(
                id=attempt_id,
                operation_id=operation_id,
                subject_id=subject_id,
                subject_incarnation=subject_incarnation,
                operation_epoch=operation_epoch,
                attempt_sequence=0,
                state="succeeded" if state == "succeeded" else "running",
                checkpoint="complete" if state == "succeeded" else "candidate_build",
                created_at=now,
                updated_at=now,
                started_at=now,
                finished_at=finished_at,
            ),
        )
        await self.session.flush()
        return PersonalDevApplyReservation(
            environment=_environment_record(environment),
            operation=_operation_record(operation),
            acquired=True,
            requires_build_binding=kind in {"create", "update"},
        )

    async def begin_activation(
        self,
        *,
        operation_id: UUID,
        operation_epoch: int,
        readiness_evidence_sha256: str,
        now: datetime | None = None,
    ) -> PersonalDevApplyReservation:
        if _DIGEST_RE.fullmatch(readiness_evidence_sha256) is None:
            raise ValueError("readiness evidence must be a lowercase SHA-256 digest")
        now = now or datetime.now(UTC)
        operation, environment = await self._locked_current_operation(
            operation_id,
            operation_epoch,
        )
        if operation.state in {"activating", "succeeded"}:
            if operation.readiness_evidence_sha256 != readiness_evidence_sha256:
                await self.session.rollback()
                raise PersonalDevEnvironmentConflictError(
                    "readiness evidence changed for the same operation",
                )
            result = self._reservation(environment, operation, acquired=False)
            await self.session.commit()
            return result
        if operation.kind not in {"create", "update"} or operation.state != "running":
            await self.session.rollback()
            raise PersonalDevEnvironmentOperationFencedError(
                "personal-dev operation cannot begin activation",
            )
        attempt = await self._locked_current_attempt(operation)
        if attempt.state != "running":
            await self.session.rollback()
            raise PersonalDevEnvironmentOperationFencedError(
                "personal-dev lifecycle attempt was superseded",
            )
        candidate = await self.session.get(PersonalDevCandidate, operation.candidate_id)
        if (
            candidate is None
            or candidate.status != "ready"
            or candidate.publication_json is None
            or candidate.publication_sha256 is None
            or candidate.image_manifest_digest is None
        ):
            await self.session.rollback()
            raise PersonalDevEnvironmentConflictError(
                "personal-dev candidate is not ready for activation",
            )
        operation.state = "activating"
        operation.checkpoint = "activation_intent"
        operation.readiness_evidence_sha256 = readiness_evidence_sha256
        operation.updated_at = now
        attempt.state = "activating"
        attempt.checkpoint = "activation_intent"
        attempt.updated_at = now
        environment.status = "activating"
        environment.operation_step = "activation_intent"
        environment.updated_at = now
        await self.session.flush()
        result = self._reservation(environment, operation, acquired=True)
        await self.session.commit()
        return result

    async def acknowledge_activation(
        self,
        *,
        operation_id: UUID,
        operation_epoch: int,
        acknowledgement_sha256: str,
        now: datetime | None = None,
    ) -> PersonalDevApplyReservation:
        if _DIGEST_RE.fullmatch(acknowledgement_sha256) is None:
            raise ValueError("activation acknowledgement must be a lowercase SHA-256 digest")
        now = now or datetime.now(UTC)
        operation, environment = await self._locked_current_operation(
            operation_id,
            operation_epoch,
        )
        if operation.state == "succeeded":
            if operation.activation_acknowledgement_sha256 != acknowledgement_sha256:
                await self.session.rollback()
                raise PersonalDevEnvironmentConflictError(
                    "activation acknowledgement changed for the same operation",
                )
            result = self._reservation(environment, operation, acquired=False)
            await self.session.commit()
            return result
        if operation.state != "activating" or operation.checkpoint not in {
            "activation_intent",
            "activation_acknowledged",
        }:
            await self.session.rollback()
            raise PersonalDevEnvironmentOperationFencedError(
                "personal-dev activation intent is unavailable",
            )
        existing = operation.activation_acknowledgement_sha256
        if existing is not None and existing != acknowledgement_sha256:
            await self.session.rollback()
            raise PersonalDevEnvironmentConflictError(
                "activation acknowledgement changed for the same operation",
            )
        operation.activation_acknowledgement_sha256 = acknowledgement_sha256
        operation.checkpoint = "activation_acknowledged"
        operation.updated_at = now
        attempt = await self._locked_current_attempt(operation)
        if attempt.state != "activating":
            await self.session.rollback()
            raise PersonalDevEnvironmentOperationFencedError(
                "personal-dev lifecycle attempt was superseded",
            )
        attempt.checkpoint = "activation_acknowledged"
        attempt.updated_at = now
        environment.operation_step = "activation_acknowledged"
        environment.updated_at = now
        await self.session.flush()
        result = self._reservation(environment, operation, acquired=existing is None)
        await self.session.commit()
        return result

    async def complete_activation(
        self,
        *,
        operation_id: UUID,
        operation_epoch: int,
        now: datetime | None = None,
    ) -> PersonalDevApplyReservation:
        now = now or datetime.now(UTC)
        operation, environment = await self._locked_current_operation(
            operation_id,
            operation_epoch,
        )
        if operation.state == "succeeded":
            if (
                operation.checkpoint != "complete"
                or environment.status != "ready"
                or environment.candidate_id != operation.candidate_id
                or environment.candidate_sha != operation.candidate_sha
                or environment.deployment_generation != operation.deployment_generation
            ):
                await self.session.rollback()
                raise PersonalDevEnvironmentOperationFencedError(
                    "personal-dev activation completion was superseded",
                )
            result = self._reservation(environment, operation, acquired=False)
            await self.session.commit()
            return result
        if (
            operation.state != "activating"
            or operation.checkpoint != "activation_acknowledged"
            or operation.readiness_evidence_sha256 is None
            or operation.activation_acknowledgement_sha256 is None
        ):
            await self.session.rollback()
            raise PersonalDevEnvironmentOperationFencedError(
                "personal-dev activation is not acknowledged",
            )
        candidate = await self.session.get(PersonalDevCandidate, operation.candidate_id)
        if (
            candidate is None
            or candidate.status != "ready"
            or candidate.candidate_sha != operation.candidate_sha
            or candidate.publication_sha256 is None
        ):
            await self.session.rollback()
            raise PersonalDevEnvironmentOperationFencedError(
                "personal-dev candidate readiness was superseded",
            )
        attempt = await self._locked_current_attempt(operation)
        if attempt.state != "activating" or attempt.checkpoint != "activation_acknowledged":
            await self.session.rollback()
            raise PersonalDevEnvironmentOperationFencedError(
                "personal-dev lifecycle attempt was superseded",
            )
        environment.candidate_id = operation.candidate_id
        environment.candidate_sha = operation.candidate_sha
        environment.min_slots = operation.min_slots
        environment.max_slots = operation.max_slots
        environment.deployment_generation = operation.deployment_generation
        environment.status = "ready"
        environment.operation_step = "complete"
        environment.failure_reason = None
        environment.ready_at = now
        environment.updated_at = now
        operation.state = "succeeded"
        operation.checkpoint = "complete"
        operation.finished_at = now
        operation.updated_at = now
        attempt.state = "succeeded"
        attempt.checkpoint = "complete"
        attempt.finished_at = now
        attempt.updated_at = now
        await self.session.flush()
        result = self._reservation(environment, operation, acquired=True)
        await self.session.commit()
        return result

    async def fail_pre_activation(
        self,
        *,
        operation_id: UUID,
        operation_epoch: int,
        failure_reason: str,
        now: datetime | None = None,
    ) -> PersonalDevApplyReservation:
        """Terminalize one attempt before an irreversible activation intent."""
        if failure_reason not in _PUBLIC_FAILURE_REASONS:
            raise ValueError("failure_reason is not an allowed public lifecycle reason")
        now = now or datetime.now(UTC)
        operation, environment = await self._locked_current_operation(
            operation_id,
            operation_epoch,
        )
        if operation.state == "failed":
            if operation.failure_reason != failure_reason:
                await self.session.rollback()
                raise PersonalDevEnvironmentConflictError(
                    "failure reason changed for the same lifecycle attempt",
                )
            result = self._reservation(environment, operation, acquired=False)
            await self.session.commit()
            return result
        if operation.kind not in {"create", "update"} or operation.state != "running":
            await self.session.rollback()
            raise PersonalDevEnvironmentOperationFencedError(
                "personal-dev operation is not pre-activation",
            )
        attempt = await self._locked_current_attempt(operation)
        if attempt.state != "running":
            await self.session.rollback()
            raise PersonalDevEnvironmentOperationFencedError(
                "personal-dev lifecycle attempt was superseded",
            )
        operation.state = "failed"
        operation.checkpoint = "failed"
        operation.failure_reason = failure_reason
        operation.finished_at = now
        operation.updated_at = now
        attempt.state = "failed"
        attempt.checkpoint = "failed"
        attempt.failure_reason = failure_reason
        attempt.finished_at = now
        attempt.updated_at = now
        environment.status = "failed" if operation.kind == "create" else "ready"
        environment.operation_step = "failed"
        environment.failure_reason = failure_reason
        environment.updated_at = now
        await self.session.flush()
        result = self._reservation(environment, operation, acquired=True)
        await self.session.commit()
        return result

    async def get(self, name: str) -> PersonalDevEnvironmentRecord | None:
        row = await self.session.get(DevInstance, name)
        return _environment_record(row) if row is not None else None

    async def get_operation(
        self,
        operation_id: UUID,
    ) -> PersonalDevLifecycleOperationRecord | None:
        row = await self.session.get(DevLifecycleOperation, operation_id)
        return _operation_record(row) if row is not None else None

    async def _retry_failed_operation(
        self,
        operation: DevLifecycleOperation,
        environment: DevInstance,
        *,
        now: datetime,
    ) -> PersonalDevApplyReservation:
        if operation.kind not in {"create", "update"}:
            raise PersonalDevEnvironmentConflictError(
                "terminal capacity operation cannot be retried",
            )
        expected_environment_status = "failed" if operation.kind == "create" else "ready"
        if (
            environment.operation_id != operation.id
            or environment.operation_epoch != operation.operation_epoch
            or environment.status != expected_environment_status
        ):
            raise PersonalDevEnvironmentOperationFencedError(
                "personal-dev lifecycle operation was superseded",
            )
        attempt_id = uuid4()
        attempt_sequence = operation.attempt_sequence + 1
        operation.state = "running"
        operation.attempt_id = attempt_id
        operation.attempt_sequence = attempt_sequence
        operation.checkpoint = "candidate_build"
        operation.failure_reason = None
        operation.readiness_evidence_sha256 = None
        operation.activation_acknowledgement_sha256 = None
        operation.started_at = now
        operation.finished_at = None
        operation.updated_at = now
        environment.status = "provisioning" if operation.kind == "create" else "updating"
        environment.operation_step = "candidate_build"
        environment.failure_reason = None
        environment.updated_at = now
        self.session.add(
            DevLifecycleOperationAttempt(
                id=attempt_id,
                operation_id=operation.id,
                subject_id=operation.subject_id,
                subject_incarnation=operation.subject_incarnation,
                operation_epoch=operation.operation_epoch,
                attempt_sequence=attempt_sequence,
                state="running",
                checkpoint="candidate_build",
                created_at=now,
                updated_at=now,
                started_at=now,
            ),
        )
        await self.session.flush()
        return PersonalDevApplyReservation(
            environment=_environment_record(environment),
            operation=_operation_record(operation),
            acquired=True,
            requires_build_binding=True,
        )

    async def _locked_current_operation(
        self,
        operation_id: UUID,
        operation_epoch: int,
    ) -> tuple[DevLifecycleOperation, DevInstance]:
        operation = (
            await self.session.execute(
                select(DevLifecycleOperation)
                .where(
                    DevLifecycleOperation.id == operation_id,
                    DevLifecycleOperation.operation_epoch == operation_epoch,
                )
                .with_for_update(),
            )
        ).scalar_one_or_none()
        if operation is None:
            await self.session.rollback()
            raise PersonalDevEnvironmentOperationFencedError(
                "personal-dev lifecycle operation was superseded",
            )
        environment = await self._locked_environment(operation.environment_name)
        if (
            environment is None
            or environment.subject_id != operation.subject_id
            or environment.subject_incarnation != operation.subject_incarnation
            or environment.operation_id != operation.id
            or environment.operation_epoch != operation.operation_epoch
        ):
            await self.session.rollback()
            raise PersonalDevEnvironmentOperationFencedError(
                "personal-dev lifecycle operation was superseded",
            )
        return operation, environment

    async def _locked_current_attempt(
        self,
        operation: DevLifecycleOperation,
    ) -> DevLifecycleOperationAttempt:
        attempt = (
            await self.session.execute(
                select(DevLifecycleOperationAttempt)
                .where(
                    DevLifecycleOperationAttempt.id == operation.attempt_id,
                    DevLifecycleOperationAttempt.operation_id == operation.id,
                    DevLifecycleOperationAttempt.attempt_sequence
                    == operation.attempt_sequence,
                    DevLifecycleOperationAttempt.subject_id == operation.subject_id,
                    DevLifecycleOperationAttempt.subject_incarnation
                    == operation.subject_incarnation,
                    DevLifecycleOperationAttempt.operation_epoch == operation.operation_epoch,
                )
                .with_for_update(),
            )
        ).scalar_one_or_none()
        if attempt is None:
            await self.session.rollback()
            raise PersonalDevEnvironmentOperationFencedError(
                "personal-dev lifecycle attempt was superseded",
            )
        return attempt

    async def _assert_limits(
        self,
        requested: PersonalDevEnvironmentApplyRequest,
        *,
        replacing_name: str | None,
    ) -> None:
        live = (
            (
                await self.session.execute(
                    select(DevInstance)
                    .where(DevInstance.status != "deleted")
                    .with_for_update(),
                )
            )
            .scalars()
            .all()
        )
        owner_others = [
            row
            for row in live
            if row.owner_user_id == requested.owner_user_id and row.name != replacing_name
        ]
        adding_live_instance = replacing_name is None
        if len(live) + int(adding_live_instance) > self._limits.global_live_instances:
            raise PersonalDevEnvironmentConflictError(
                "personal-dev global live-instance limit is exhausted",
            )
        if len(owner_others) + 1 > self._limits.per_owner_live_instances:
            raise PersonalDevEnvironmentConflictError(
                "personal-dev per-owner live-instance limit is exhausted",
            )
        aggregate_min = sum(row.min_slots for row in owner_others) + requested.min_slots
        aggregate_max = sum(row.max_slots for row in owner_others) + requested.max_slots
        if aggregate_min > self._limits.per_owner_aggregate_min_slots:
            raise PersonalDevEnvironmentConflictError(
                "personal-dev per-owner aggregate min_slots limit is exhausted",
            )
        if aggregate_max > self._limits.per_owner_aggregate_max_slots:
            raise PersonalDevEnvironmentConflictError(
                "personal-dev per-owner aggregate max_slots limit is exhausted",
            )

    async def _locked_environment(self, name: str) -> DevInstance | None:
        return (
            await self.session.execute(
                select(DevInstance).where(DevInstance.name == name).with_for_update(),
            )
        ).scalar_one_or_none()

    @staticmethod
    def _reservation(
        environment: DevInstance,
        operation: DevLifecycleOperation,
        *,
        acquired: bool,
    ) -> PersonalDevApplyReservation:
        return PersonalDevApplyReservation(
            environment=_environment_record(environment),
            operation=_operation_record(operation),
            acquired=acquired,
            requires_build_binding=False,
        )


__all__ = [
    "PersonalDevEnvironmentConflictError",
    "PersonalDevEnvironmentEpochFencedError",
    "PersonalDevEnvironmentNotFoundError",
    "PersonalDevEnvironmentOperationFencedError",
    "SqlAlchemyPersonalDevEnvironmentAuthority",
]
