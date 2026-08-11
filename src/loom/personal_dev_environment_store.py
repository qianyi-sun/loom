"""Transactional personal-development environment lifecycle authority."""

from __future__ import annotations

import hashlib
import re
from datetime import UTC, datetime, timedelta
from typing import Literal, cast
from uuid import UUID, uuid4

from sqlalchemy import and_, func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from loom.db.schema import (
    DevInstance,
    DevLifecycleActivationAcknowledgement,
    DevLifecycleOperation,
    DevLifecycleOperationAttempt,
    PersonalDevCandidate,
)
from loom.personal_dev_activation import (
    PersonalDevActivationIntent,
    VerifiedPersonalDevActivationAcknowledgement,
)
from loom.personal_dev_candidate import (
    CandidateArtifactState,
    CandidateStatus,
    PersonalDevCandidateRecord,
    validate_personal_dev_candidate_publication,
)
from loom.personal_dev_candidate_store import SqlAlchemyPersonalDevCandidateStore
from loom.personal_dev_capacity import PersonalDevCapacityProjectionResult
from loom.personal_dev_environment import (
    PersonalDevAccessBinding,
    PersonalDevApplyReservation,
    PersonalDevEnvironmentApplyRequest,
    PersonalDevEnvironmentDestroyRequest,
    PersonalDevEnvironmentRecord,
    PersonalDevEnvironmentStatus,
    PersonalDevLifecycleAttemptRecord,
    PersonalDevLifecycleLimits,
    PersonalDevLifecycleOperationRecord,
    PersonalDevOperationKind,
    PersonalDevOperationState,
    PersonalDevReconciliationClaim,
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


class SqlAlchemyPersonalDevActivationIntentReader:
    """Read current irreversible intents without acquiring mutation authority."""

    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def next_intent(
        self,
        *,
        operation_id: UUID | None = None,
        exclude_operation_ids: tuple[UUID, ...] = (),
    ) -> PersonalDevActivationIntent | None:
        if len(exclude_operation_ids) > 16:
            raise ValueError("activation exclusion set exceeds the bounded limit")
        statement = (
            select(
                DevLifecycleOperation,
                DevInstance,
                DevLifecycleOperationAttempt,
                PersonalDevCandidate,
            )
            .join(
                DevInstance,
                and_(
                    DevInstance.name == DevLifecycleOperation.environment_name,
                    DevInstance.operation_id == DevLifecycleOperation.id,
                    DevInstance.operation_epoch == DevLifecycleOperation.operation_epoch,
                ),
            )
            .join(
                DevLifecycleOperationAttempt,
                and_(
                    DevLifecycleOperationAttempt.id == DevLifecycleOperation.attempt_id,
                    DevLifecycleOperationAttempt.operation_id == DevLifecycleOperation.id,
                    DevLifecycleOperationAttempt.operation_epoch
                    == DevLifecycleOperation.operation_epoch,
                ),
            )
            .join(
                PersonalDevCandidate,
                PersonalDevCandidate.id == DevLifecycleOperation.candidate_id,
            )
            .where(
                DevLifecycleOperation.kind.in_(("create", "update")),
                DevLifecycleOperation.state == "activating",
                DevLifecycleOperation.checkpoint == "activation_intent",
                DevLifecycleOperation.activation_acknowledgement_sha256.is_(None),
                DevLifecycleOperation.readiness_evidence_sha256.is_not(None),
                DevLifecycleOperationAttempt.state == "activating",
                DevLifecycleOperationAttempt.checkpoint == "activation_intent",
                DevInstance.status == "activating",
                DevInstance.operation_step == "activation_intent",
                PersonalDevCandidate.status == "ready",
            )
            .order_by(DevLifecycleOperation.updated_at, DevLifecycleOperation.id)
            .limit(1)
        )
        if operation_id is not None:
            statement = statement.where(DevLifecycleOperation.id == operation_id)
        elif exclude_operation_ids:
            statement = statement.where(
                DevLifecycleOperation.id.not_in(exclude_operation_ids),
            )
        row = (await self.session.execute(statement)).one_or_none()
        if row is None:
            return None
        operation, environment, attempt, candidate = row
        if (
            operation.subject_id != environment.subject_id
            or operation.subject_incarnation != environment.subject_incarnation
            or operation.subject_id != attempt.subject_id
            or operation.subject_incarnation != attempt.subject_incarnation
            or operation.attempt_sequence != attempt.attempt_sequence
            or operation.candidate_sha != candidate.candidate_sha
            or candidate.publication_json is None
            or candidate.publication_sha256 is None
            or operation.readiness_evidence_sha256 is None
        ):
            raise PersonalDevEnvironmentOperationFencedError(
                "personal-dev activation intent bindings are inconsistent",
            )
        candidate_record = _candidate_record(candidate)
        publication, publication_sha256, _image_set_digest = (
            validate_personal_dev_candidate_publication(
                candidate_record,
                candidate.publication_json,
            )
        )
        if publication_sha256 != candidate.publication_sha256:
            raise PersonalDevEnvironmentOperationFencedError(
                "personal-dev activation publication digest is inconsistent",
            )
        raw_images = publication["images"]
        if not isinstance(raw_images, dict):  # pragma: no cover - validator proves this
            raise PersonalDevEnvironmentOperationFencedError(
                "personal-dev activation image publication is inconsistent",
            )
        images = {component: str(value["index"]) for component, value in raw_images.items()}
        return PersonalDevActivationIntent(
            environment_name=operation.environment_name,
            subject_id=operation.subject_id,
            subject_incarnation=operation.subject_incarnation,
            operation_id=operation.id,
            operation_epoch=operation.operation_epoch,
            attempt_id=operation.attempt_id,
            attempt_sequence=operation.attempt_sequence,
            candidate_id=operation.candidate_id,
            candidate_sha=operation.candidate_sha,
            candidate_publication_sha256=candidate.publication_sha256,
            deployment_generation=operation.deployment_generation,
            readiness_evidence_sha256=operation.readiness_evidence_sha256,
            min_slots=operation.min_slots,
            max_slots=operation.max_slots,
            images=images,
            intent_created_at=operation.updated_at,
        )


def _lock_keys(name: str) -> tuple[int, int]:
    digest = hashlib.sha256(b"loom-personal-dev-environment-v1\0" + name.encode("ascii")).digest()
    return (
        int.from_bytes(digest[:4], byteorder="big", signed=True),
        int.from_bytes(digest[4:8], byteorder="big", signed=True),
    )


def _capacity_capabilities(
    raw: object,
    *,
    allowed: frozenset[str],
) -> tuple[str, ...] | None:
    if raw is None:
        return None
    if not isinstance(raw, list) or any(not isinstance(item, str) for item in raw):
        raise PersonalDevEnvironmentOperationFencedError(
            "personal-dev capacity capability evidence is invalid"
        )
    values = tuple(raw)
    if not values or values != tuple(sorted(set(values))) or not set(values) <= allowed:
        raise PersonalDevEnvironmentOperationFencedError(
            "personal-dev capacity capability evidence is invalid"
        )
    return values


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
        keep_data=row.keep_data,
        ready_at=row.ready_at,
        deleted_at=row.deleted_at,
        capacity_configuration_epoch=row.capacity_configuration_epoch,
        capacity_configuration_sha256=row.capacity_configuration_sha256,
        capacity_reporter_incarnation=row.capacity_reporter_incarnation,
        capacity_reporter_token_sha256=row.capacity_reporter_token_sha256,
        local_activation_sha256=row.local_activation_sha256,
        protected_admission_sha256=row.protected_admission_sha256,
        capacity_agent_installation_sha256=(row.capacity_agent_installation_sha256),
        capacity_supported_pool_ids=cast(
            tuple[Literal["oldlab", "gb10"], ...] | None,
            _capacity_capabilities(
                row.capacity_supported_pool_ids,
                allowed=frozenset({"oldlab", "gb10"}),
            ),
        ),
        capacity_supported_architectures=cast(
            tuple[Literal["x86_64", "arm64"], ...] | None,
            _capacity_capabilities(
                row.capacity_supported_architectures,
                allowed=frozenset({"x86_64", "arm64"}),
            ),
        ),
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
        activation_acknowledgement_sha256=(row.activation_acknowledgement_sha256),
        local_activation_sha256=row.local_activation_sha256,
        capacity_expected_configuration_epoch=(row.capacity_expected_configuration_epoch),
        capacity_projection_request_sha256=row.capacity_projection_request_sha256,
        capacity_configuration_epoch=row.capacity_configuration_epoch,
        capacity_configuration_sha256=row.capacity_configuration_sha256,
        capacity_reporter_incarnation=row.capacity_reporter_incarnation,
        capacity_reporter_token_sha256=row.capacity_reporter_token_sha256,
        protected_admission_sha256=row.protected_admission_sha256,
        capacity_agent_installation_sha256=(row.capacity_agent_installation_sha256),
        capacity_supported_pool_ids=cast(
            tuple[Literal["oldlab", "gb10"], ...] | None,
            _capacity_capabilities(
                row.capacity_supported_pool_ids,
                allowed=frozenset({"oldlab", "gb10"}),
            ),
        ),
        capacity_supported_architectures=cast(
            tuple[Literal["x86_64", "arm64"], ...] | None,
            _capacity_capabilities(
                row.capacity_supported_architectures,
                allowed=frozenset({"x86_64", "arm64"}),
            ),
        ),
        created_at=row.created_at,
        updated_at=row.updated_at,
        keep_data=row.keep_data,
        started_at=row.started_at,
        finished_at=row.finished_at,
    )


def _attempt_record(row: DevLifecycleOperationAttempt) -> PersonalDevLifecycleAttemptRecord:
    if row.credential_binding_version != 1:
        raise PersonalDevEnvironmentOperationFencedError(
            "personal-dev lifecycle attempt has no supported credential binding",
        )
    if row.bootstrap_auth_kind not in {"bearer", "session"}:
        raise PersonalDevEnvironmentOperationFencedError(
            "personal-dev lifecycle attempt credential binding is invalid",
        )
    return PersonalDevLifecycleAttemptRecord(
        id=row.id,
        operation_id=row.operation_id,
        subject_id=row.subject_id,
        subject_incarnation=row.subject_incarnation,
        operation_epoch=row.operation_epoch,
        attempt_sequence=row.attempt_sequence,
        state=cast(
            Literal["running", "activating", "succeeded", "failed", "cancelled"],
            row.state,
        ),
        checkpoint=row.checkpoint,
        access_binding=PersonalDevAccessBinding(
            auth_kind=cast(Literal["bearer", "session"], row.bootstrap_auth_kind),
            credential_hash=bytes(row.bootstrap_credential_hash),
        ),
        lease_epoch=row.lease_epoch,
        claimed_by=row.claimed_by,
        lease_expires_at=row.lease_expires_at,
        failure_reason=row.failure_reason,
        created_at=row.created_at,
        updated_at=row.updated_at,
        started_at=row.started_at,
        finished_at=row.finished_at,
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
        access_binding: PersonalDevAccessBinding,
        now: datetime | None = None,
    ) -> PersonalDevApplyReservation:
        now = now or datetime.now(UTC)
        try:
            reservation = await self._claim_apply(
                requested,
                access_binding=access_binding,
                now=now,
            )
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

    async def destroy(
        self,
        requested: PersonalDevEnvironmentDestroyRequest,
        *,
        access_binding: PersonalDevAccessBinding,
        now: datetime | None = None,
    ) -> PersonalDevApplyReservation:
        """Durably retire manager authority before any local resource deletion."""

        now = now or datetime.now(UTC)
        try:
            reservation = await self._claim_destroy(
                requested,
                access_binding=access_binding,
                now=now,
            )
            await self.session.commit()
            return reservation
        except Exception:
            if self.session.in_transaction():
                await self.session.rollback()
            raise

    async def _claim_destroy(
        self,
        requested: PersonalDevEnvironmentDestroyRequest,
        *,
        access_binding: PersonalDevAccessBinding,
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
                )
            )
        ).scalar_one_or_none()
        if prior is not None:
            if (
                prior.kind != "destroy"
                or prior.environment_name != requested.name
                or prior.owner_team_id != requested.owner_team_id
                or prior.request_sha256 != requested.request_sha256
                or prior.keep_data != requested.keep_data
            ):
                raise PersonalDevEnvironmentConflictError(
                    "idempotency key is already bound to a different request"
                )
            environment = await self._locked_environment(requested.name)
            if (
                environment is None
                or environment.owner_user_id != requested.owner_user_id
                or environment.subject_id != prior.subject_id
                or environment.subject_incarnation != prior.subject_incarnation
                or environment.operation_id != prior.id
                or environment.operation_epoch != prior.operation_epoch
            ):
                raise PersonalDevEnvironmentOperationFencedError(
                    "personal-dev destroy operation was superseded"
                )
            return PersonalDevApplyReservation(
                environment=_environment_record(environment),
                operation=_operation_record(prior),
                acquired=False,
                requires_build_binding=False,
            )

        environment = await self._locked_environment(requested.name)
        if (
            environment is None
            or environment.owner_user_id != requested.owner_user_id
            or environment.owner_team_id != requested.owner_team_id
        ):
            raise PersonalDevEnvironmentNotFoundError(
                "personal-dev environment not found"
            )
        if environment.operation_epoch != requested.expected_operation_epoch:
            raise PersonalDevEnvironmentEpochFencedError(
                "personal-dev environment operation epoch changed"
            )
        if environment.status != "ready" or environment.candidate_id is None:
            raise PersonalDevEnvironmentConflictError(
                f"personal-dev environment cannot be destroyed while {environment.status}"
            )
        active = (
            await self.session.execute(
                select(DevLifecycleOperation.id).where(
                    DevLifecycleOperation.environment_name == requested.name,
                    DevLifecycleOperation.state.in_(_ACTIVE_OPERATION_STATES),
                )
            )
        ).scalar_one_or_none()
        if active is not None:
            raise PersonalDevEnvironmentConflictError(
                "another personal-dev lifecycle operation is active"
            )
        supported_pool_ids = _capacity_capabilities(
            environment.capacity_supported_pool_ids,
            allowed=frozenset({"oldlab", "gb10"}),
        )
        supported_architectures = _capacity_capabilities(
            environment.capacity_supported_architectures,
            allowed=frozenset({"x86_64", "arm64"}),
        )
        evidence = (
            environment.capacity_configuration_epoch,
            environment.capacity_configuration_sha256,
            environment.capacity_reporter_incarnation,
            environment.capacity_reporter_token_sha256,
            environment.local_activation_sha256,
            environment.protected_admission_sha256,
            environment.capacity_agent_installation_sha256,
            supported_pool_ids,
            supported_architectures,
        )
        if any(value is None for value in evidence):
            raise PersonalDevEnvironmentConflictError(
                "personal-dev environment has no complete capacity retirement evidence"
            )
        assert supported_pool_ids is not None
        assert supported_architectures is not None
        candidate = (
            await self.session.execute(
                select(PersonalDevCandidate)
                .where(
                    PersonalDevCandidate.id == environment.candidate_id,
                    PersonalDevCandidate.owner_user_id == requested.owner_user_id,
                    PersonalDevCandidate.owner_team_id == requested.owner_team_id,
                    PersonalDevCandidate.candidate_sha == environment.candidate_sha,
                    PersonalDevCandidate.status == "ready",
                )
                .with_for_update()
            )
        ).scalar_one_or_none()
        if candidate is None or candidate.publication_sha256 is None:
            raise PersonalDevEnvironmentConflictError(
                "personal-dev destroy candidate evidence is unavailable"
            )

        operation_id = uuid4()
        attempt_id = uuid4()
        operation_epoch = environment.operation_epoch + 1
        operation = DevLifecycleOperation(
            id=operation_id,
            idempotency_key=requested.idempotency_key,
            environment_name=environment.name,
            subject_id=environment.subject_id,
            subject_incarnation=environment.subject_incarnation,
            owner_user_id=environment.owner_user_id,
            owner_team_id=environment.owner_team_id,
            operation_epoch=operation_epoch,
            expected_operation_epoch=requested.expected_operation_epoch,
            kind="destroy",
            state="running",
            attempt_id=attempt_id,
            attempt_sequence=0,
            request_sha256=requested.request_sha256,
            candidate_id=environment.candidate_id,
            candidate_sha=environment.candidate_sha,
            min_slots=environment.min_slots,
            max_slots=environment.max_slots,
            deployment_generation=environment.deployment_generation,
            local_activation_sha256=environment.local_activation_sha256,
            capacity_reporter_incarnation=environment.capacity_reporter_incarnation,
            capacity_reporter_token_sha256=(environment.capacity_reporter_token_sha256),
            protected_admission_sha256=environment.protected_admission_sha256,
            capacity_agent_installation_sha256=(
                environment.capacity_agent_installation_sha256
            ),
            capacity_supported_pool_ids=list(supported_pool_ids),
            capacity_supported_architectures=list(supported_architectures),
            keep_data=requested.keep_data,
            checkpoint="capacity_retirement_requested",
            created_at=now,
            updated_at=now,
            started_at=now,
        )
        self.session.add(operation)
        environment.status = "deleting"
        environment.operation_epoch = operation_epoch
        environment.operation_id = operation_id
        environment.operation_step = "capacity_retirement_requested"
        environment.keep_data = requested.keep_data
        environment.failure_reason = None
        environment.updated_at = now
        await self.session.flush()
        self.session.add(
            DevLifecycleOperationAttempt(
                id=attempt_id,
                operation_id=operation_id,
                subject_id=environment.subject_id,
                subject_incarnation=environment.subject_incarnation,
                operation_epoch=operation_epoch,
                attempt_sequence=0,
                state="running",
                checkpoint="capacity_retirement_requested",
                credential_binding_version=1,
                bootstrap_auth_kind=access_binding.auth_kind,
                bootstrap_credential_hash=access_binding.credential_hash,
                created_at=now,
                updated_at=now,
                started_at=now,
            )
        )
        await self.session.flush()
        return PersonalDevApplyReservation(
            environment=_environment_record(environment),
            operation=_operation_record(operation),
            acquired=True,
            requires_build_binding=False,
        )

    async def _claim_apply(
        self,
        requested: PersonalDevEnvironmentApplyRequest,
        *,
        access_binding: PersonalDevAccessBinding,
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
                return await self._retry_failed_operation(
                    prior,
                    environment,
                    access_binding=access_binding,
                    now=now,
                )
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
            or candidate.artifact_state != "retained"
        ):
            raise PersonalDevEnvironmentNotFoundError("personal-dev candidate not found")
        candidate.artifact_gc_unreferenced_at = None
        candidate.updated_at = now

        environment = await self._locked_environment(requested.name)
        operation_id = uuid4()
        local_activation_sha256: str | None = None
        capacity_configuration_epoch: int | None = None
        capacity_configuration_sha256: str | None = None
        capacity_reporter_incarnation: UUID | None = None
        capacity_reporter_token_sha256: str | None = None
        protected_admission_sha256: str | None = None
        capacity_agent_installation_sha256: str | None = None
        capacity_supported_pool_ids: list[str] | None = None
        capacity_supported_architectures: list[str] | None = None
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
                        access_binding=access_binding,
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
                environment.capacity_configuration_epoch = None
                environment.capacity_configuration_sha256 = None
                environment.capacity_reporter_incarnation = None
                environment.capacity_reporter_token_sha256 = None
                environment.local_activation_sha256 = None
                environment.protected_admission_sha256 = None
                environment.capacity_agent_installation_sha256 = None
                environment.capacity_supported_pool_ids = None
                environment.capacity_supported_architectures = None
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
                    state = "running"
                    local_activation_sha256 = environment.local_activation_sha256
                    if local_activation_sha256 is None:
                        prior_acknowledgement = await self.session.get(
                            DevLifecycleActivationAcknowledgement,
                            environment.operation_id,
                        )
                        if prior_acknowledgement is not None:
                            local_activation_sha256 = prior_acknowledgement.local_activation_sha256
                    if local_activation_sha256 is None:
                        raise PersonalDevEnvironmentConflictError(
                            "personal-dev capacity update has no trusted activation evidence"
                        )
                    environment.status = "updating"
                    environment.operation_epoch = operation_epoch
                    environment.operation_id = operation_id
                    environment.operation_step = "capacity_projection_requested"
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
        checkpoint = (
            "complete"
            if state == "succeeded"
            else "capacity_projection_requested"
            if kind == "capacity"
            else "candidate_build"
        )
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
            local_activation_sha256=local_activation_sha256,
            capacity_configuration_epoch=capacity_configuration_epoch,
            capacity_configuration_sha256=capacity_configuration_sha256,
            capacity_reporter_incarnation=capacity_reporter_incarnation,
            capacity_reporter_token_sha256=capacity_reporter_token_sha256,
            protected_admission_sha256=protected_admission_sha256,
            capacity_agent_installation_sha256=(capacity_agent_installation_sha256),
            capacity_supported_pool_ids=capacity_supported_pool_ids,
            capacity_supported_architectures=capacity_supported_architectures,
            keep_data=False,
            checkpoint=checkpoint,
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
                checkpoint=checkpoint,
                credential_binding_version=1,
                bootstrap_auth_kind=access_binding.auth_kind,
                bootstrap_credential_hash=access_binding.credential_hash,
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

    async def claim_next_reconciliation(
        self,
        *,
        reconciler_id: str,
        now: datetime,
        lease_seconds: int,
    ) -> PersonalDevReconciliationClaim | None:
        """Lease one build-terminal or activation-acknowledged attempt."""
        if not reconciler_id or reconciler_id.strip() != reconciler_id or len(reconciler_id) > 128:
            raise ValueError("reconciler_id must be a non-empty bounded identifier")
        if type(lease_seconds) is not int or lease_seconds <= 0:
            raise ValueError("lease_seconds must be a positive integer")
        eligible_checkpoint = or_(
            and_(
                DevLifecycleOperation.state == "running",
                DevLifecycleOperation.checkpoint == "candidate_build",
                PersonalDevCandidate.status.in_(("ready", "failed")),
            ),
            and_(
                DevLifecycleOperation.state == "activating",
                DevLifecycleOperation.checkpoint == "activation_acknowledged",
                DevLifecycleOperation.activation_acknowledgement_sha256.is_not(None),
            ),
            and_(
                DevLifecycleOperation.kind == "capacity",
                DevLifecycleOperation.state == "running",
                DevLifecycleOperation.checkpoint == "capacity_projection_requested",
                DevLifecycleOperation.local_activation_sha256.is_not(None),
            ),
            and_(
                DevLifecycleOperation.state.in_(("running", "activating")),
                DevLifecycleOperation.checkpoint == "capacity_projection_pending",
                DevLifecycleOperation.capacity_expected_configuration_epoch.is_not(None),
            ),
            and_(
                DevLifecycleOperation.state == "activating",
                DevLifecycleOperation.checkpoint == "capacity_projected",
                DevLifecycleOperation.capacity_configuration_epoch.is_not(None),
            ),
            and_(
                DevLifecycleOperation.kind == "destroy",
                DevLifecycleOperation.state == "running",
                DevLifecycleOperation.checkpoint.in_(
                    (
                        "capacity_retirement_requested",
                        "capacity_retired",
                        "local_authority_sealed",
                        "namespace_deleted",
                        "database_deleted",
                        "buckets_deleted",
                        "tenant_deleted",
                    )
                ),
            ),
        )
        statement = (
            select(
                DevLifecycleOperationAttempt,
                DevLifecycleOperation,
                DevInstance,
                PersonalDevCandidate,
            )
            .join(
                DevLifecycleOperation,
                DevLifecycleOperation.id == DevLifecycleOperationAttempt.operation_id,
            )
            .join(DevInstance, DevInstance.name == DevLifecycleOperation.environment_name)
            .join(
                PersonalDevCandidate, PersonalDevCandidate.id == DevLifecycleOperation.candidate_id
            )
            .where(
                DevLifecycleOperation.attempt_id == DevLifecycleOperationAttempt.id,
                DevLifecycleOperation.attempt_sequence
                == DevLifecycleOperationAttempt.attempt_sequence,
                DevLifecycleOperation.subject_id == DevLifecycleOperationAttempt.subject_id,
                DevLifecycleOperation.subject_incarnation
                == DevLifecycleOperationAttempt.subject_incarnation,
                DevLifecycleOperation.operation_epoch
                == DevLifecycleOperationAttempt.operation_epoch,
                DevInstance.operation_id == DevLifecycleOperation.id,
                DevInstance.subject_id == DevLifecycleOperation.subject_id,
                DevInstance.subject_incarnation == DevLifecycleOperation.subject_incarnation,
                DevInstance.operation_epoch == DevLifecycleOperation.operation_epoch,
                DevLifecycleOperationAttempt.state.in_(("running", "activating")),
                or_(
                    DevLifecycleOperationAttempt.claimed_by.is_(None),
                    DevLifecycleOperationAttempt.lease_expires_at <= now,
                ),
                eligible_checkpoint,
            )
            .order_by(
                DevLifecycleOperationAttempt.created_at,
                DevLifecycleOperationAttempt.id,
            )
            .with_for_update(of=DevLifecycleOperationAttempt, skip_locked=True)
            .limit(1)
        )
        row = (await self.session.execute(statement)).one_or_none()
        if row is None:
            await self.session.rollback()
            return None
        attempt, operation, environment, candidate = row
        # Validate credential material before making the lease externally visible.
        _attempt_record(attempt)
        attempt.lease_epoch += 1
        attempt.claimed_by = reconciler_id
        attempt.lease_expires_at = now + timedelta(seconds=lease_seconds)
        attempt.updated_at = now
        await self.session.flush()
        claim = PersonalDevReconciliationClaim(
            environment=_environment_record(environment),
            operation=_operation_record(operation),
            attempt=_attempt_record(attempt),
            candidate=_candidate_record(candidate),
        )
        await self.session.commit()
        return claim

    async def begin_activation(
        self,
        *,
        operation_id: UUID,
        operation_epoch: int,
        attempt_id: UUID,
        reconciler_id: str,
        lease_epoch: int,
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
        attempt = await self._locked_current_attempt_lease(
            operation,
            attempt_id=attempt_id,
            reconciler_id=reconciler_id,
            lease_epoch=lease_epoch,
            now=now,
        )
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
        attempt.claimed_by = None
        attempt.lease_expires_at = None
        attempt.updated_at = now
        environment.status = "activating"
        environment.operation_step = "activation_intent"
        environment.updated_at = now
        await self.session.flush()
        result = self._reservation(environment, operation, acquired=True)
        await self.session.commit()
        return result

    async def heartbeat_reconciliation(
        self,
        *,
        operation_id: UUID,
        operation_epoch: int,
        attempt_id: UUID,
        reconciler_id: str,
        lease_epoch: int,
        now: datetime,
        lease_seconds: int,
    ) -> PersonalDevLifecycleAttemptRecord:
        """Extend a live current lease without allowing expired resurrection."""
        if type(lease_seconds) is not int or lease_seconds <= 0:
            raise ValueError("lease_seconds must be a positive integer")
        operation, _environment = await self._locked_current_operation(
            operation_id,
            operation_epoch,
        )
        attempt = await self._locked_current_attempt_lease(
            operation,
            attempt_id=attempt_id,
            reconciler_id=reconciler_id,
            lease_epoch=lease_epoch,
            now=now,
        )
        attempt.lease_expires_at = now + timedelta(seconds=lease_seconds)
        attempt.updated_at = now
        await self.session.flush()
        record = _attempt_record(attempt)
        await self.session.commit()
        return record

    async def acknowledge_activation(
        self,
        *,
        verified: VerifiedPersonalDevActivationAcknowledgement,
        now: datetime | None = None,
    ) -> PersonalDevApplyReservation:
        now = now or datetime.now(UTC)
        acknowledgement = verified.acknowledgement
        operation, environment = await self._locked_current_operation(
            acknowledgement.operation_id,
            acknowledgement.operation_epoch,
        )
        bindings_match = (
            acknowledgement.environment_name == operation.environment_name
            and acknowledgement.subject_id == operation.subject_id
            and acknowledgement.subject_incarnation == operation.subject_incarnation
            and acknowledgement.attempt_id == operation.attempt_id
            and acknowledgement.candidate_id == operation.candidate_id
            and acknowledgement.candidate_sha == operation.candidate_sha
            and acknowledgement.deployment_generation == operation.deployment_generation
            and acknowledgement.readiness_evidence_sha256 == operation.readiness_evidence_sha256
            and acknowledgement.observed_at >= operation.updated_at
        )
        if not bindings_match:
            await self.session.rollback()
            raise PersonalDevEnvironmentOperationFencedError(
                "personal-dev activation acknowledgement binding was superseded",
            )
        existing_acknowledgement = await self.session.get(
            DevLifecycleActivationAcknowledgement,
            operation.id,
        )
        if existing_acknowledgement is not None:
            if (
                existing_acknowledgement.payload_sha256 != verified.payload_sha256
                or existing_acknowledgement.signature_sha256 != verified.signature_sha256
                or operation.activation_acknowledgement_sha256 != verified.payload_sha256
            ):
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
        if existing is not None:
            await self.session.rollback()
            raise PersonalDevEnvironmentConflictError(
                "activation acknowledgement changed for the same operation",
            )
        self.session.add(
            DevLifecycleActivationAcknowledgement(
                operation_id=operation.id,
                environment_name=acknowledgement.environment_name,
                subject_id=acknowledgement.subject_id,
                subject_incarnation=acknowledgement.subject_incarnation,
                operation_epoch=acknowledgement.operation_epoch,
                attempt_id=acknowledgement.attempt_id,
                candidate_id=acknowledgement.candidate_id,
                candidate_sha=acknowledgement.candidate_sha,
                deployment_generation=acknowledgement.deployment_generation,
                readiness_evidence_sha256=(acknowledgement.readiness_evidence_sha256),
                local_activation_sha256=acknowledgement.local_activation_sha256,
                payload_sha256=verified.payload_sha256,
                signature_sha256=verified.signature_sha256,
                agent_key_id=acknowledgement.agent_key_id,
                observed_at=acknowledgement.observed_at,
                received_at=now,
            ),
        )
        operation.activation_acknowledgement_sha256 = verified.payload_sha256
        operation.local_activation_sha256 = acknowledgement.local_activation_sha256
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

    async def prepare_capacity_projection(
        self,
        *,
        operation_id: UUID,
        operation_epoch: int,
        attempt_id: UUID,
        reconciler_id: str,
        lease_epoch: int,
        expected_configuration_epoch: int,
        projection_request_sha256: str,
        reporter_incarnation: UUID,
        reporter_token_sha256: str,
        protected_admission_sha256: str,
        capacity_agent_installation_sha256: str,
        supported_pool_ids: tuple[str, ...],
        supported_architectures: tuple[str, ...],
        now: datetime | None = None,
    ) -> PersonalDevApplyReservation:
        """Persist the exact outbound manager request before network mutation."""

        if type(expected_configuration_epoch) is not int or expected_configuration_epoch <= 0:
            raise ValueError("capacity configuration epoch must be positive")
        digests = (
            projection_request_sha256,
            reporter_token_sha256,
            protected_admission_sha256,
            capacity_agent_installation_sha256,
        )
        if any(_DIGEST_RE.fullmatch(value) is None for value in digests):
            raise ValueError("capacity projection evidence must use SHA-256 digests")
        if (
            supported_pool_ids != tuple(sorted(set(supported_pool_ids)))
            or not supported_pool_ids
            or not set(supported_pool_ids) <= {"oldlab", "gb10"}
            or supported_architectures != tuple(sorted(set(supported_architectures)))
            or not supported_architectures
            or not set(supported_architectures) <= {"x86_64", "arm64"}
        ):
            raise ValueError("capacity projection capabilities are invalid")
        now = now or datetime.now(UTC)
        operation, environment = await self._locked_current_operation(
            operation_id,
            operation_epoch,
        )
        exact = (
            operation.checkpoint == "capacity_projection_pending"
            and operation.capacity_expected_configuration_epoch == expected_configuration_epoch
            and operation.capacity_projection_request_sha256 == projection_request_sha256
            and operation.capacity_reporter_incarnation == reporter_incarnation
            and operation.capacity_reporter_token_sha256 == reporter_token_sha256
            and operation.protected_admission_sha256 == protected_admission_sha256
            and operation.capacity_agent_installation_sha256 == capacity_agent_installation_sha256
            and tuple(operation.capacity_supported_pool_ids or ()) == supported_pool_ids
            and tuple(operation.capacity_supported_architectures or ())
            == supported_architectures
        )
        if exact:
            result = self._reservation(environment, operation, acquired=False)
            await self.session.commit()
            return result
        allowed = (
            operation.kind in {"create", "update"}
            and operation.state == "activating"
            and operation.checkpoint == "activation_acknowledged"
            and operation.activation_acknowledgement_sha256 is not None
            and operation.local_activation_sha256 is not None
        ) or (
            operation.kind == "capacity"
            and operation.state == "running"
            and operation.checkpoint == "capacity_projection_requested"
            and operation.local_activation_sha256 is not None
        ) or (
            operation.kind == "destroy"
            and operation.state == "running"
            and operation.checkpoint == "capacity_retirement_requested"
            and operation.local_activation_sha256 is not None
        )
        if not allowed:
            await self.session.rollback()
            raise PersonalDevEnvironmentOperationFencedError(
                "personal-dev operation cannot prepare capacity projection"
            )
        if operation.kind in {"capacity", "destroy"} and (
            environment.capacity_configuration_epoch is not None
        ):
            if (
                environment.capacity_reporter_incarnation != reporter_incarnation
                or environment.capacity_reporter_token_sha256 != reporter_token_sha256
                or environment.local_activation_sha256 != operation.local_activation_sha256
                or environment.protected_admission_sha256 != protected_admission_sha256
                or environment.capacity_agent_installation_sha256
                != capacity_agent_installation_sha256
                or tuple(environment.capacity_supported_pool_ids or ())
                != supported_pool_ids
                or tuple(environment.capacity_supported_architectures or ())
                != supported_architectures
            ):
                await self.session.rollback()
                raise PersonalDevEnvironmentConflictError(
                    "capacity-only update changed trusted deployment evidence"
                )
        attempt = await self._locked_current_attempt_lease(
            operation,
            attempt_id=attempt_id,
            reconciler_id=reconciler_id,
            lease_epoch=lease_epoch,
            now=now,
        )
        expected_attempt_state = "activating" if operation.state == "activating" else "running"
        if attempt.state != expected_attempt_state:
            await self.session.rollback()
            raise PersonalDevEnvironmentOperationFencedError(
                "personal-dev lifecycle attempt was superseded"
            )
        operation.capacity_expected_configuration_epoch = expected_configuration_epoch
        operation.capacity_projection_request_sha256 = projection_request_sha256
        operation.capacity_configuration_epoch = None
        operation.capacity_configuration_sha256 = None
        operation.capacity_reporter_incarnation = reporter_incarnation
        operation.capacity_reporter_token_sha256 = reporter_token_sha256
        operation.protected_admission_sha256 = protected_admission_sha256
        operation.capacity_agent_installation_sha256 = capacity_agent_installation_sha256
        operation.capacity_supported_pool_ids = list(supported_pool_ids)
        operation.capacity_supported_architectures = list(supported_architectures)
        operation.checkpoint = "capacity_projection_pending"
        operation.updated_at = now
        attempt.checkpoint = "capacity_projection_pending"
        attempt.claimed_by = None
        attempt.lease_expires_at = None
        attempt.updated_at = now
        environment.operation_step = "capacity_projection_pending"
        environment.updated_at = now
        await self.session.flush()
        result = self._reservation(environment, operation, acquired=True)
        await self.session.commit()
        return result

    async def refresh_capacity_projection_epoch(
        self,
        *,
        operation_id: UUID,
        operation_epoch: int,
        attempt_id: UUID,
        reconciler_id: str,
        lease_epoch: int,
        expected_configuration_epoch: int,
        projection_request_sha256: str,
        now: datetime | None = None,
    ) -> PersonalDevApplyReservation:
        """Advance only a manager-rejected pending request to a newer global epoch."""

        if type(expected_configuration_epoch) is not int or expected_configuration_epoch <= 0:
            raise ValueError("capacity configuration epoch must be positive")
        if _DIGEST_RE.fullmatch(projection_request_sha256) is None:
            raise ValueError("capacity projection request digest must be SHA-256")
        now = now or datetime.now(UTC)
        operation, environment = await self._locked_current_operation(
            operation_id,
            operation_epoch,
        )
        if (
            operation.checkpoint != "capacity_projection_pending"
            or operation.capacity_expected_configuration_epoch is None
            or expected_configuration_epoch <= operation.capacity_expected_configuration_epoch
            or operation.capacity_configuration_epoch is not None
        ):
            await self.session.rollback()
            raise PersonalDevEnvironmentOperationFencedError(
                "capacity projection epoch did not advance"
            )
        attempt = await self._locked_current_attempt_lease(
            operation,
            attempt_id=attempt_id,
            reconciler_id=reconciler_id,
            lease_epoch=lease_epoch,
            now=now,
        )
        if attempt.checkpoint != "capacity_projection_pending":
            await self.session.rollback()
            raise PersonalDevEnvironmentOperationFencedError(
                "personal-dev lifecycle attempt was superseded"
            )
        operation.capacity_expected_configuration_epoch = expected_configuration_epoch
        operation.capacity_projection_request_sha256 = projection_request_sha256
        operation.updated_at = now
        attempt.claimed_by = None
        attempt.lease_expires_at = None
        attempt.updated_at = now
        await self.session.flush()
        result = self._reservation(environment, operation, acquired=True)
        await self.session.commit()
        return result

    async def record_capacity_projection(
        self,
        *,
        operation_id: UUID,
        operation_epoch: int,
        attempt_id: UUID,
        reconciler_id: str,
        lease_epoch: int,
        result: PersonalDevCapacityProjectionResult,
        now: datetime | None = None,
    ) -> PersonalDevApplyReservation:
        """Record the exact manager acknowledgement and expose readiness only afterward."""

        if not isinstance(result, PersonalDevCapacityProjectionResult):
            raise TypeError("capacity projection result is invalid")
        now = now or datetime.now(UTC)
        operation, environment = await self._locked_current_operation(
            operation_id,
            operation_epoch,
        )
        binding_matches = (
            result.subject_id == operation.subject_id
            and result.subject_incarnation == operation.subject_incarnation
            and result.configuration_generation == operation.operation_epoch
            and result.deployment_generation == operation.deployment_generation
            and result.reporter_incarnation == operation.capacity_reporter_incarnation
        )
        exact = (
            operation.capacity_configuration_epoch == result.configuration_epoch
            and operation.capacity_configuration_sha256 == result.configuration_digest
            and binding_matches
        )
        if exact and operation.checkpoint in {
            "capacity_projected",
            "capacity_retired",
            "complete",
        }:
            reservation = self._reservation(environment, operation, acquired=False)
            await self.session.commit()
            return reservation
        if (
            operation.checkpoint != "capacity_projection_pending"
            or operation.capacity_expected_configuration_epoch is None
            or operation.capacity_projection_request_sha256 is None
            or operation.capacity_reporter_incarnation is None
            or operation.capacity_reporter_token_sha256 is None
            or operation.protected_admission_sha256 is None
            or operation.capacity_agent_installation_sha256 is None
            or operation.capacity_supported_pool_ids is None
            or operation.capacity_supported_architectures is None
            or result.configuration_epoch != operation.capacity_expected_configuration_epoch + 1
            or not binding_matches
        ):
            await self.session.rollback()
            raise PersonalDevEnvironmentOperationFencedError(
                "capacity projection acknowledgement binding was superseded"
            )
        attempt = await self._locked_current_attempt_lease(
            operation,
            attempt_id=attempt_id,
            reconciler_id=reconciler_id,
            lease_epoch=lease_epoch,
            now=now,
        )
        operation.capacity_configuration_epoch = result.configuration_epoch
        operation.capacity_configuration_sha256 = result.configuration_digest
        operation.updated_at = now
        if operation.kind == "capacity":
            environment.min_slots = operation.min_slots
            environment.max_slots = operation.max_slots
            environment.status = "ready"
            environment.operation_step = "complete"
            environment.failure_reason = None
            environment.ready_at = now
            operation.state = "succeeded"
            operation.checkpoint = "complete"
            operation.finished_at = now
            attempt.state = "succeeded"
            attempt.checkpoint = "complete"
            attempt.finished_at = now
        elif operation.kind == "destroy":
            operation.checkpoint = "capacity_retired"
            attempt.checkpoint = "capacity_retired"
            environment.status = "deleting"
            environment.operation_step = "capacity_retired"
        else:
            operation.checkpoint = "capacity_projected"
            attempt.checkpoint = "capacity_projected"
            environment.operation_step = "capacity_projected"
        environment.capacity_configuration_epoch = result.configuration_epoch
        environment.capacity_configuration_sha256 = result.configuration_digest
        environment.capacity_reporter_incarnation = operation.capacity_reporter_incarnation
        environment.capacity_reporter_token_sha256 = operation.capacity_reporter_token_sha256
        environment.local_activation_sha256 = operation.local_activation_sha256
        environment.protected_admission_sha256 = operation.protected_admission_sha256
        environment.capacity_agent_installation_sha256 = (
            operation.capacity_agent_installation_sha256
        )
        environment.capacity_supported_pool_ids = operation.capacity_supported_pool_ids
        environment.capacity_supported_architectures = (
            operation.capacity_supported_architectures
        )
        environment.updated_at = now
        attempt.claimed_by = None
        attempt.lease_expires_at = None
        attempt.updated_at = now
        await self.session.flush()
        reservation = self._reservation(environment, operation, acquired=True)
        await self.session.commit()
        return reservation

    async def complete_activation(
        self,
        *,
        operation_id: UUID,
        operation_epoch: int,
        attempt_id: UUID,
        reconciler_id: str,
        lease_epoch: int,
        now: datetime | None = None,
    ) -> PersonalDevApplyReservation:
        now = now or datetime.now(UTC)
        operation, environment = await self._locked_current_operation(
            operation_id,
            operation_epoch,
        )
        if operation.state == "succeeded":
            if (
                operation.kind not in {"create", "update"}
                or operation.checkpoint != "complete"
                or environment.status != "ready"
                or environment.candidate_id != operation.candidate_id
                or environment.candidate_sha != operation.candidate_sha
                or environment.min_slots != operation.min_slots
                or environment.max_slots != operation.max_slots
                or environment.deployment_generation != operation.deployment_generation
                or operation.capacity_configuration_epoch is None
                or operation.capacity_configuration_sha256 is None
                or operation.capacity_reporter_incarnation is None
                or operation.capacity_reporter_token_sha256 is None
                or operation.local_activation_sha256 is None
                or operation.protected_admission_sha256 is None
                or operation.capacity_agent_installation_sha256 is None
                or operation.capacity_supported_pool_ids is None
                or operation.capacity_supported_architectures is None
                or environment.capacity_configuration_epoch
                != operation.capacity_configuration_epoch
                or environment.capacity_configuration_sha256
                != operation.capacity_configuration_sha256
                or environment.capacity_reporter_incarnation
                != operation.capacity_reporter_incarnation
                or environment.capacity_reporter_token_sha256
                != operation.capacity_reporter_token_sha256
                or environment.local_activation_sha256 != operation.local_activation_sha256
                or environment.protected_admission_sha256
                != operation.protected_admission_sha256
                or environment.capacity_agent_installation_sha256
                != operation.capacity_agent_installation_sha256
                or environment.capacity_supported_pool_ids
                != operation.capacity_supported_pool_ids
                or environment.capacity_supported_architectures
                != operation.capacity_supported_architectures
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
            or operation.checkpoint != "capacity_projected"
            or operation.readiness_evidence_sha256 is None
            or operation.activation_acknowledgement_sha256 is None
            or operation.local_activation_sha256 is None
            or operation.capacity_configuration_epoch is None
            or operation.capacity_configuration_sha256 is None
            or operation.capacity_reporter_incarnation is None
            or operation.capacity_reporter_token_sha256 is None
            or operation.protected_admission_sha256 is None
            or operation.capacity_agent_installation_sha256 is None
            or operation.capacity_supported_pool_ids is None
            or operation.capacity_supported_architectures is None
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
        attempt = await self._locked_current_attempt_lease(
            operation,
            attempt_id=attempt_id,
            reconciler_id=reconciler_id,
            lease_epoch=lease_epoch,
            now=now,
        )
        if attempt.state != "activating" or attempt.checkpoint != "capacity_projected":
            await self.session.rollback()
            raise PersonalDevEnvironmentOperationFencedError(
                "personal-dev lifecycle attempt was superseded",
            )
        environment.candidate_id = operation.candidate_id
        environment.candidate_sha = operation.candidate_sha
        environment.min_slots = operation.min_slots
        environment.max_slots = operation.max_slots
        environment.deployment_generation = operation.deployment_generation
        environment.capacity_configuration_epoch = operation.capacity_configuration_epoch
        environment.capacity_configuration_sha256 = operation.capacity_configuration_sha256
        environment.capacity_reporter_incarnation = operation.capacity_reporter_incarnation
        environment.capacity_reporter_token_sha256 = operation.capacity_reporter_token_sha256
        environment.local_activation_sha256 = operation.local_activation_sha256
        environment.protected_admission_sha256 = operation.protected_admission_sha256
        environment.capacity_agent_installation_sha256 = (
            operation.capacity_agent_installation_sha256
        )
        environment.capacity_supported_pool_ids = operation.capacity_supported_pool_ids
        environment.capacity_supported_architectures = (
            operation.capacity_supported_architectures
        )
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
        attempt.claimed_by = None
        attempt.lease_expires_at = None
        attempt.finished_at = now
        attempt.updated_at = now
        await self.session.flush()
        result = self._reservation(environment, operation, acquired=True)
        await self.session.commit()
        return result

    async def advance_destroy_checkpoint(
        self,
        *,
        operation_id: UUID,
        operation_epoch: int,
        attempt_id: UUID,
        reconciler_id: str,
        lease_epoch: int,
        expected_checkpoint: str,
        checkpoint: str,
        now: datetime | None = None,
    ) -> PersonalDevApplyReservation:
        """Commit exactly one completed, idempotent teardown side effect."""

        now = now or datetime.now(UTC)
        operation, environment = await self._locked_current_operation(
            operation_id,
            operation_epoch,
        )
        transitions = {
            "capacity_retired": "local_authority_sealed",
            "local_authority_sealed": "namespace_deleted",
            "database_deleted": "buckets_deleted",
            "buckets_deleted": "tenant_deleted",
            "tenant_deleted": "complete",
        }
        transitions["namespace_deleted"] = (
            "tenant_deleted" if operation.keep_data else "database_deleted"
        )
        if (
            operation.kind != "destroy"
            or operation.state != "running"
            or operation.checkpoint != expected_checkpoint
            or transitions.get(expected_checkpoint) != checkpoint
            or environment.status != "deleting"
            or environment.keep_data != operation.keep_data
        ):
            await self.session.rollback()
            raise PersonalDevEnvironmentOperationFencedError(
                "personal-dev destroy checkpoint was superseded"
            )
        attempt = await self._locked_current_attempt_lease(
            operation,
            attempt_id=attempt_id,
            reconciler_id=reconciler_id,
            lease_epoch=lease_epoch,
            now=now,
        )
        if attempt.state != "running" or attempt.checkpoint != expected_checkpoint:
            await self.session.rollback()
            raise PersonalDevEnvironmentOperationFencedError(
                "personal-dev destroy attempt was superseded"
            )
        operation.checkpoint = checkpoint
        operation.updated_at = now
        attempt.checkpoint = checkpoint
        attempt.claimed_by = None
        attempt.lease_expires_at = None
        attempt.updated_at = now
        environment.operation_step = checkpoint
        environment.updated_at = now
        if checkpoint == "complete":
            operation.state = "succeeded"
            operation.finished_at = now
            attempt.state = "succeeded"
            attempt.finished_at = now
            environment.status = "deleted"
            environment.deleted_at = now
            environment.ready_at = None
            environment.failure_reason = None
        await self.session.flush()
        result = self._reservation(environment, operation, acquired=True)
        await self.session.commit()
        return result

    async def fail_pre_activation(
        self,
        *,
        operation_id: UUID,
        operation_epoch: int,
        attempt_id: UUID,
        reconciler_id: str,
        lease_epoch: int,
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
        attempt = await self._locked_current_attempt_lease(
            operation,
            attempt_id=attempt_id,
            reconciler_id=reconciler_id,
            lease_epoch=lease_epoch,
            now=now,
        )
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
        attempt.claimed_by = None
        attempt.lease_expires_at = None
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
        access_binding: PersonalDevAccessBinding,
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
                credential_binding_version=1,
                bootstrap_auth_kind=access_binding.auth_kind,
                bootstrap_credential_hash=access_binding.credential_hash,
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
                    DevLifecycleOperationAttempt.attempt_sequence == operation.attempt_sequence,
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

    async def _locked_current_attempt_lease(
        self,
        operation: DevLifecycleOperation,
        *,
        attempt_id: UUID,
        reconciler_id: str,
        lease_epoch: int,
        now: datetime,
    ) -> DevLifecycleOperationAttempt:
        attempt = await self._locked_current_attempt(operation)
        if (
            attempt.id != attempt_id
            or attempt.claimed_by != reconciler_id
            or attempt.lease_epoch != lease_epoch
            or attempt.lease_expires_at is None
            or attempt.lease_expires_at <= now
        ):
            await self.session.rollback()
            raise PersonalDevEnvironmentOperationFencedError(
                "personal-dev lifecycle reconciliation lease was superseded",
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
                    select(DevInstance).where(DevInstance.status != "deleted").with_for_update(),
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
    "SqlAlchemyPersonalDevActivationIntentReader",
    "SqlAlchemyPersonalDevEnvironmentAuthority",
]
