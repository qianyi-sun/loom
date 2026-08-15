"""Strict inert contracts for prepared local admission state.

Every model in this module is database-gated to disabled, prepared, and
non-executable values. They can bind future manager decisions without
authorizing assignment, claims, credentials, or physical capacity.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Annotated, Literal
from uuid import UUID

from pydantic import Field, field_validator, model_validator

from loom_capacity_agent.contracts import AgentRegistrationV1
from loom_capacity_guard.contracts import (
    Digest,
    NonNegativeSequence,
    PositiveGeneration,
    StrictGuardModel,
)
from loom_capacity_manager.contracts import (
    MAX_FIXED_CLAIMS_PER_REPORT,
    Identifier,
    WorkerShapeV1,
    canonical_digest,
)
from loom_capacity_manager.executable_contracts import (
    ExecutableIntentBindingV2,
    ExecutableProtectedReleaseV2,
    StrictV2Model,
    canonical_executable_digest,
)

PhysicalPool = Literal["oldlab", "gb10"]


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("timestamp must be timezone-aware")
    return value.astimezone(UTC)


def _parse_datetime(value: datetime | str) -> datetime | str:
    if isinstance(value, str):
        timestamp = f"{value[:-1]}+00:00" if value.endswith("Z") else value
        try:
            return datetime.fromisoformat(timestamp)
        except ValueError:
            return value
    return value


class PreparedWorkerShapeV1(StrictGuardModel):
    """One manager-selected shape instance with no launch authority."""

    shape_instance_id: Identifier
    submission_intent_id: UUID
    pool_id: PhysicalPool
    pool_generation: PositiveGeneration
    profile_id: Identifier
    profile_generation: PositiveGeneration
    profile_digest: Digest
    protocol_generation: PositiveGeneration
    protocol_digest: Digest
    worker_shape: WorkerShapeV1
    worker_shape_digest: Digest
    bootstrap_registration_epoch: PositiveGeneration
    shape_state: Literal["prepared"] = "prepared"
    executable: Literal[False] = False

    @model_validator(mode="after")
    def _shape_digest(self) -> PreparedWorkerShapeV1:
        if canonical_digest(self.worker_shape) != self.worker_shape_digest:
            raise ValueError("worker shape digest does not match its exact contract")
        return self


class PreparedPlacementAllowanceV1(StrictGuardModel):
    """One inert manager-matched attempt-to-physical-slot binding."""

    allowance_id: UUID
    protected_attempt_id: UUID
    execution_generation: PositiveGeneration
    requirements_digest: Digest
    pool_id: PhysicalPool
    shape_instance_id: Identifier
    shape_slot_index: NonNegativeSequence
    submission_intent_id: UUID
    allowance_state: Literal["prepared"] = "prepared"
    executable: Literal[False] = False


class PreparedAdmissionPlanV1(AgentRegistrationV1):
    """One complete prepared local plan under still-disabled authority."""

    plan_id: UUID
    admission_incarnation: UUID
    manager_authority_incarnation: UUID
    manager_writer_epoch: NonNegativeSequence
    manager_allocation_epoch: PositiveGeneration
    manager_input_digest: Digest
    manager_allocation_digest: Digest
    pool_id: PhysicalPool
    pool_generation: PositiveGeneration
    profile_id: Identifier
    profile_generation: PositiveGeneration
    profile_digest: Digest
    protocol_generation: PositiveGeneration
    protocol_digest: Digest
    lease_not_after: datetime
    plan_state: Literal["prepared"] = "prepared"
    executable: Literal[False] = False
    worker_shapes: Annotated[
        tuple[PreparedWorkerShapeV1, ...],
        Field(min_length=1, max_length=MAX_FIXED_CLAIMS_PER_REPORT),
    ]
    placement_allowances: Annotated[
        tuple[PreparedPlacementAllowanceV1, ...],
        Field(max_length=MAX_FIXED_CLAIMS_PER_REPORT),
    ] = ()

    @field_validator("lease_not_after", mode="before")
    @classmethod
    def _parse_lease_time(cls, value: datetime | str) -> datetime | str:
        return _parse_datetime(value)

    @field_validator("lease_not_after")
    @classmethod
    def _lease_time(cls, value: datetime) -> datetime:
        return _utc(value)

    @field_validator("worker_shapes")
    @classmethod
    def _canonical_shapes(
        cls, value: tuple[PreparedWorkerShapeV1, ...]
    ) -> tuple[PreparedWorkerShapeV1, ...]:
        shape_instances = [item.shape_instance_id for item in value]
        intents = [item.submission_intent_id for item in value]
        if len(shape_instances) != len(set(shape_instances)):
            raise ValueError("duplicate prepared shape instance")
        if len(intents) != len(set(intents)):
            raise ValueError("duplicate prepared submission intent")
        return tuple(sorted(value, key=lambda item: item.shape_instance_id))

    @field_validator("placement_allowances")
    @classmethod
    def _canonical_allowances(
        cls, value: tuple[PreparedPlacementAllowanceV1, ...]
    ) -> tuple[PreparedPlacementAllowanceV1, ...]:
        identities = [item.allowance_id for item in value]
        attempts = [item.protected_attempt_id for item in value]
        slots = [(item.shape_instance_id, item.shape_slot_index) for item in value]
        if len(identities) != len(set(identities)):
            raise ValueError("duplicate prepared allowance identity")
        if len(attempts) != len(set(attempts)):
            raise ValueError("duplicate prepared allowance attempt")
        if len(slots) != len(set(slots)):
            raise ValueError("duplicate prepared physical slot")
        return tuple(sorted(value, key=lambda item: item.allowance_id.hex))

    @model_validator(mode="after")
    def _complete_bindings(self) -> PreparedAdmissionPlanV1:
        identities = {
            self.subject_id,
            self.subject_incarnation,
            self.authority_incarnation,
            self.agent_incarnation,
            self.reporter_incarnation,
            self.plan_id,
            self.admission_incarnation,
            self.manager_authority_incarnation,
        }
        if len(identities) != 8:
            raise ValueError("prepared plan identities must be distinct")
        plan_binding = (
            self.pool_id,
            self.pool_generation,
            self.profile_id,
            self.profile_generation,
            self.profile_digest,
            self.protocol_generation,
            self.protocol_digest,
        )
        shapes: dict[str, PreparedWorkerShapeV1] = {}
        for shape in self.worker_shapes:
            shape_binding = (
                shape.pool_id,
                shape.pool_generation,
                shape.profile_id,
                shape.profile_generation,
                shape.profile_digest,
                shape.protocol_generation,
                shape.protocol_digest,
            )
            if shape_binding != plan_binding:
                raise ValueError("prepared worker shape differs from its plan binding")
            shapes[shape.shape_instance_id] = shape
        for allowance in self.placement_allowances:
            prepared_shape = shapes.get(allowance.shape_instance_id)
            if prepared_shape is None:
                raise ValueError("prepared allowance references a missing worker shape")
            if (
                allowance.pool_id != self.pool_id
                or allowance.submission_intent_id != prepared_shape.submission_intent_id
            ):
                raise ValueError("prepared allowance differs from its exact shape binding")
            if allowance.shape_slot_index >= prepared_shape.worker_shape.concurrency_slots:
                raise ValueError("prepared allowance slot is outside its exact worker shape")
        return self


class PreparedBootstrapBindingV1(AgentRegistrationV1):
    """Hash-only, submission-bound bootstrap registration with no exchange authority."""

    bootstrap_id: UUID
    plan_id: UUID
    admission_incarnation: UUID
    manager_allocation_epoch: PositiveGeneration
    pool_id: PhysicalPool
    shape_instance_id: Identifier
    submission_intent_id: UUID
    bootstrap_registration_epoch: PositiveGeneration
    bootstrap_digest: Digest
    expires_at: datetime
    bootstrap_state: Literal["registered"] = "registered"
    executable: Literal[False] = False

    @field_validator("expires_at", mode="before")
    @classmethod
    def _parse_expiry(cls, value: datetime | str) -> datetime | str:
        return _parse_datetime(value)

    @field_validator("expires_at")
    @classmethod
    def _expiry(cls, value: datetime) -> datetime:
        return _utc(value)

    @model_validator(mode="after")
    def _bootstrap_identities(self) -> PreparedBootstrapBindingV1:
        identities = {
            self.subject_id,
            self.subject_incarnation,
            self.authority_incarnation,
            self.agent_incarnation,
            self.reporter_incarnation,
            self.bootstrap_id,
            self.plan_id,
            self.admission_incarnation,
            self.submission_intent_id,
        }
        if len(identities) != 9:
            raise ValueError("prepared bootstrap identities must be distinct")
        return self


class PreparedWorkerBindingV1(AgentRegistrationV1):
    """Ownership evidence and credential hash for a nonclaimable worker."""

    worker_id: UUID
    worker_incarnation: UUID
    bootstrap_id: UUID
    plan_id: UUID
    admission_incarnation: UUID
    manager_allocation_epoch: PositiveGeneration
    pool_id: PhysicalPool
    shape_instance_id: Identifier
    submission_intent_id: UUID
    bootstrap_registration_epoch: PositiveGeneration
    slurm_job_id: Identifier
    ownership_evidence_digest: Digest
    worker_credential_digest: Digest
    claim_authorization_epoch: Literal[0] = 0
    worker_state: Literal["prepared"] = "prepared"
    executable: Literal[False] = False

    @model_validator(mode="after")
    def _worker_identities(self) -> PreparedWorkerBindingV1:
        identities = {
            self.subject_id,
            self.subject_incarnation,
            self.authority_incarnation,
            self.agent_incarnation,
            self.reporter_incarnation,
            self.worker_id,
            self.worker_incarnation,
            self.bootstrap_id,
            self.plan_id,
            self.admission_incarnation,
            self.submission_intent_id,
        }
        if len(identities) != 11:
            raise ValueError("prepared worker identities must be distinct")
        return self


class PreparedProtectedReleaseV1(AgentRegistrationV1):
    """Append-only local proof that one inert shape is fenced from registration."""

    release_id: UUID
    plan_id: UUID
    admission_incarnation: UUID
    manager_authority_incarnation: UUID
    manager_writer_epoch: PositiveGeneration
    manager_configuration_epoch: PositiveGeneration
    manager_allocation_epoch: PositiveGeneration
    tranche_id: UUID
    pool_id: PhysicalPool
    pool_generation: PositiveGeneration
    shape_instance_id: Identifier
    submission_intent_id: UUID
    bootstrap_registration_epoch: NonNegativeSequence
    protected_registration_epoch: PositiveGeneration
    bootstrap_revoked: Literal[True]
    release_state: Literal["acknowledged"] = "acknowledged"
    executable: Literal[False] = False

    @model_validator(mode="after")
    def _release_fence(self) -> PreparedProtectedReleaseV1:
        if self.protected_registration_epoch <= self.bootstrap_registration_epoch:
            raise ValueError("protected registration epoch must advance past bootstrap")
        identities = {
            self.subject_id,
            self.subject_incarnation,
            self.authority_incarnation,
            self.agent_incarnation,
            self.reporter_incarnation,
            self.release_id,
            self.plan_id,
            self.admission_incarnation,
            self.manager_authority_incarnation,
            self.tranche_id,
            self.submission_intent_id,
        }
        if len(identities) != 11:
            raise ValueError("protected release identities must be distinct")
        return self


class PhysicalJobBindingV2(StrictV2Model):
    """Bind one prepared executable intent to its exact scheduler identity."""

    operation_id: UUID
    binding: ExecutableIntentBindingV2
    bootstrap_registration_epoch: PositiveGeneration
    slurm_job_id: Identifier
    ownership_evidence_sha256: Digest
    executable: Literal[True] = True


class ExecutableWorkerRegistrationV2(StrictV2Model):
    """Exchange or requeue one worker identity after physical binding."""

    operation_id: UUID
    binding: ExecutableIntentBindingV2
    bootstrap_registration_epoch: PositiveGeneration
    protected_registration_epoch: PositiveGeneration
    slurm_job_id: Identifier
    worker_id: UUID
    worker_incarnation: UUID
    worker_credential_sha256: Digest
    predecessor_worker_incarnation: UUID | None = None
    executable: Literal[True] = True

    @model_validator(mode="after")
    def _registration_epoch(self) -> ExecutableWorkerRegistrationV2:
        if self.protected_registration_epoch <= self.bootstrap_registration_epoch:
            raise ValueError("worker registration epoch must advance past bootstrap")
        identities = {
            self.operation_id,
            self.binding.intent_id,
            self.binding.tranche_id,
            self.worker_id,
            self.worker_incarnation,
        }
        if self.predecessor_worker_incarnation is not None:
            identities.add(self.predecessor_worker_incarnation)
        if len(identities) != 5 + (self.predecessor_worker_incarnation is not None):
            raise ValueError("worker registration identities must be distinct")
        return self


class ExecutableDrainRequestV2(StrictV2Model):
    """Monotonically stop claims for one exact live worker incarnation."""

    operation_id: UUID
    binding: ExecutableIntentBindingV2
    worker_id: UUID
    worker_incarnation: UUID
    expected_claim_high_water: NonNegativeSequence
    drain_epoch: PositiveGeneration
    executable: Literal[True] = True


class ExecutableWorkerWithdrawalRequestV2(StrictV2Model):
    """Fence delayed registration for one bound scheduler job with no worker."""

    operation_id: UUID
    binding: ExecutableIntentBindingV2
    bootstrap_registration_epoch: PositiveGeneration
    protected_registration_epoch: PositiveGeneration
    slurm_job_id: Identifier
    ownership_evidence_sha256: Digest
    expected_claim_high_water: Literal[0] = 0
    executable: Literal[True] = True

    @model_validator(mode="after")
    def _withdrawal_epoch(self) -> ExecutableWorkerWithdrawalRequestV2:
        if self.protected_registration_epoch <= self.bootstrap_registration_epoch:
            raise ValueError("withdrawal registration epoch must advance past bootstrap")
        return self


class ExecutablePreparedBootstrapRevocationV2(StrictV2Model):
    """Fence one prepared bootstrap before any physical scheduler binding."""

    operation_id: UUID
    binding: ExecutableIntentBindingV2
    bootstrap_registration_epoch: PositiveGeneration
    protected_registration_epoch: PositiveGeneration
    expected_claim_high_water: Literal[0] = 0
    executable: Literal[True] = True

    @model_validator(mode="after")
    def _revocation_epoch(self) -> ExecutablePreparedBootstrapRevocationV2:
        if self.protected_registration_epoch <= self.bootstrap_registration_epoch:
            raise ValueError("prepared revocation epoch must advance past bootstrap")
        return self


class ExecutableReleaseRequestV2(StrictV2Model):
    """Fence delayed registration after protected claims and credentials close."""

    operation_id: UUID
    binding: ExecutableIntentBindingV2
    reporter_incarnation: UUID
    bootstrap_registration_epoch: PositiveGeneration
    expected_claim_high_water: NonNegativeSequence
    protected_registration_epoch: PositiveGeneration
    release_epoch: PositiveGeneration
    executable: Literal[True] = True


class PreparedExecutableAdmissionV2(StrictV2Model):
    """Exact protected receipt for one sealed executable bootstrap digest."""

    subject_id: UUID
    subject_incarnation: UUID
    intent_id: UUID
    bootstrap_registration_epoch: PositiveGeneration
    bootstrap_sha256: Digest
    request_digest: Digest
    admission_digest: Digest
    protected_high_water: PositiveGeneration
    admission_state: Literal["prepared"] = "prepared"
    executable: Literal[True] = True


class BoundExecutableWorkerV2(StrictV2Model):
    """Exact protected receipt for a physical scheduler binding."""

    subject_id: UUID
    subject_incarnation: UUID
    intent_id: UUID
    bootstrap_registration_epoch: PositiveGeneration
    slurm_job_id: Identifier
    ownership_evidence_sha256: Digest
    request_digest: Digest
    binding_digest: Digest
    protected_high_water: PositiveGeneration
    binding_state: Literal["bound"] = "bound"
    executable: Literal[True] = True


class RegisteredExecutableWorkerV2(StrictV2Model):
    """Exact receipt for one live worker and any revoked predecessor."""

    subject_id: UUID
    subject_incarnation: UUID
    intent_id: UUID
    worker_id: UUID
    worker_incarnation: UUID
    predecessor_worker_incarnation: UUID | None = None
    protected_registration_epoch: PositiveGeneration
    request_digest: Digest
    registration_digest: Digest
    protected_high_water: PositiveGeneration
    registration_state: Literal["registered"] = "registered"
    executable: Literal[True] = True


class DrainedExecutableWorkerV2(StrictV2Model):
    """Exact receipt proving new claims are fenced without closing live claims."""

    subject_id: UUID
    subject_incarnation: UUID
    intent_id: UUID
    worker_id: UUID
    worker_incarnation: UUID
    claim_high_water: NonNegativeSequence
    live_claim_count: NonNegativeSequence
    drain_epoch: PositiveGeneration
    request_digest: Digest
    drain_digest: Digest
    protected_high_water: PositiveGeneration
    worker_state: Literal["draining"] = "draining"
    executable: Literal[True] = True


class WithdrawnExecutableWorkerV2(StrictV2Model):
    """Exact receipt proving bootstrap was revoked before worker registration."""

    subject_id: UUID
    subject_incarnation: UUID
    intent_id: UUID
    bootstrap_registration_epoch: PositiveGeneration
    protected_registration_epoch: PositiveGeneration
    slurm_job_id: Identifier
    ownership_evidence_sha256: Digest
    claim_high_water: Literal[0] = 0
    live_claim_count: Literal[0] = 0
    bootstrap_revoked: Literal[True] = True
    request_digest: Digest
    withdrawal_digest: Digest
    protected_high_water: PositiveGeneration
    withdrawal_state: Literal["withdrawn"] = "withdrawn"
    executable: Literal[True] = True


class RevokedExecutableBootstrapV2(StrictV2Model):
    """Append-only protected proof that an unbound bootstrap was revoked."""

    binding: ExecutableIntentBindingV2
    reporter_incarnation: UUID
    bootstrap_registration_epoch: PositiveGeneration
    protected_registration_epoch: PositiveGeneration
    claim_high_water: Literal[0] = 0
    live_claim_count: Literal[0] = 0
    bootstrap_revoked: Literal[True] = True
    request_digest: Digest
    protected_release_sha256: Digest
    protected_high_water: PositiveGeneration
    revocation_state: Literal["revoked"] = "revoked"
    executable: Literal[True] = True

    @model_validator(mode="after")
    def _revocation_epoch(self) -> RevokedExecutableBootstrapV2:
        if self.protected_registration_epoch <= self.bootstrap_registration_epoch:
            raise ValueError("prepared revocation epoch must advance past bootstrap")
        return self


class PublishableExecutableProtectedReleaseV2(StrictV2Model):
    """One guard outbox event normalized to the manager protected-release contract."""

    event_id: PositiveGeneration
    event_kind: Literal["released", "withdrawn", "prepared-revoked"]
    release: ExecutableProtectedReleaseV2
    publication_digest: Digest

    @model_validator(mode="after")
    def _publication_digest(self) -> PublishableExecutableProtectedReleaseV2:
        if canonical_executable_digest(self.release) != self.publication_digest:
            raise ValueError("publication digest does not match manager release contract")
        return self


class ProtectedReleasePublicationCheckpointV2(StrictV2Model):
    """Append-only receipt that the manager acknowledged one outbox publication."""

    event_id: PositiveGeneration
    event_kind: Literal["released", "withdrawn", "prepared-revoked"]
    publication_digest: Digest
    manager_acknowledgement_digest: Digest


class ExecutableReleaseReceiptV2(StrictV2Model):
    """Append-only protected release proof consumed by the manager."""

    binding: ExecutableIntentBindingV2
    reporter_incarnation: UUID
    bootstrap_registration_epoch: PositiveGeneration
    protected_registration_epoch: PositiveGeneration
    claim_high_water: NonNegativeSequence
    live_claim_count: Literal[0] = 0
    release_epoch: PositiveGeneration
    bootstrap_revoked: Literal[True] = True
    worker_credentials_revoked: Literal[True] = True
    request_digest: Digest
    protected_release_sha256: Digest
    protected_high_water: PositiveGeneration
    release_state: Literal["acknowledged"] = "acknowledged"
    executable: Literal[True] = True


class ProtectedIntentObservationV2(StrictV2Model):
    """Read-only exact protected state for one pool-managed intent."""

    binding: ExecutableIntentBindingV2
    bootstrap_registration_epoch: NonNegativeSequence = 0
    worker_id: UUID | None = None
    worker_incarnation: UUID | None = None
    protected_registration_epoch: NonNegativeSequence = 0
    claim_high_water: NonNegativeSequence = 0
    drain: DrainedExecutableWorkerV2 | None = None
    release: ExecutableReleaseReceiptV2 | None = None
    prepared_revocation: RevokedExecutableBootstrapV2 | None = None
    executable: Literal[True] = True

    @model_validator(mode="after")
    def _coherent_worker_state(self) -> ProtectedIntentObservationV2:
        if (self.worker_id is None) != (self.worker_incarnation is None):
            raise ValueError("protected worker identities must be present together")
        if self.worker_id is None and (
            self.protected_registration_epoch != 0
            or self.drain is not None
            or self.release is not None
        ):
            raise ValueError("protected worker evidence requires a worker identity")
        if self.drain is not None and (
            self.drain.intent_id != self.binding.intent_id
            or self.drain.worker_id != self.worker_id
            or self.drain.worker_incarnation != self.worker_incarnation
            or self.drain.claim_high_water != self.claim_high_water
        ):
            raise ValueError("protected drain differs from current intent observation")
        if self.release is not None and (
            self.release.binding != self.binding
            or self.release.claim_high_water != self.claim_high_water
        ):
            raise ValueError("protected release differs from current intent observation")
        if self.prepared_revocation is not None and (
            self.worker_id is not None
            or self.prepared_revocation.binding != self.binding
            or self.prepared_revocation.bootstrap_registration_epoch
            != self.bootstrap_registration_epoch
            or self.prepared_revocation.claim_high_water != self.claim_high_water
        ):
            raise ValueError("prepared revocation differs from current intent observation")
        return self


__all__ = [
    "BoundExecutableWorkerV2",
    "DrainedExecutableWorkerV2",
    "ExecutableDrainRequestV2",
    "ExecutablePreparedBootstrapRevocationV2",
    "ExecutableReleaseReceiptV2",
    "ExecutableReleaseRequestV2",
    "ExecutableWorkerRegistrationV2",
    "ExecutableWorkerWithdrawalRequestV2",
    "PhysicalJobBindingV2",
    "PreparedAdmissionPlanV1",
    "PreparedBootstrapBindingV1",
    "PreparedExecutableAdmissionV2",
    "PreparedPlacementAllowanceV1",
    "PreparedProtectedReleaseV1",
    "PreparedWorkerBindingV1",
    "PreparedWorkerShapeV1",
    "ProtectedIntentObservationV2",
    "ProtectedReleasePublicationCheckpointV2",
    "PublishableExecutableProtectedReleaseV2",
    "RegisteredExecutableWorkerV2",
    "RevokedExecutableBootstrapV2",
    "WithdrawnExecutableWorkerV2",
]
