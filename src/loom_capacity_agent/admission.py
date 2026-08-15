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
from loom_capacity_manager.executable_contracts import StrictV2Model

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


class ProtectedExecutableBootstrapRegistrationV2(StrictV2Model):
    """Protected local receipt for one executor-proposed bootstrap hash."""

    subject_id: UUID
    subject_incarnation: UUID
    intent_id: UUID
    proposal_epoch: PositiveGeneration
    proposal_digest: Digest
    bootstrap_registration_epoch: PositiveGeneration
    bootstrap_sha256: Digest
    protected_admission_sha256: Digest
    protected_high_water: PositiveGeneration
    registration_state: Literal["registered"] = "registered"
    executable: Literal[False] = False


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


__all__ = [
    "PreparedAdmissionPlanV1",
    "PreparedBootstrapBindingV1",
    "PreparedPlacementAllowanceV1",
    "PreparedProtectedReleaseV1",
    "PreparedWorkerBindingV1",
    "PreparedWorkerShapeV1",
    "ProtectedExecutableBootstrapRegistrationV2",
]
