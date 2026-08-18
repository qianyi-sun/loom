"""Strict executable contracts for the fenced global-capacity bridge."""

from __future__ import annotations

import base64
import binascii
import hashlib
import json
import re
from datetime import UTC, datetime
from typing import Annotated, Any, Literal, get_origin
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from loom_capacity_manager.contracts import (
    MAX_CONTRACT_BYTES,
    MAX_FIXED_CLAIMS_PER_REPORT,
    MAX_SHAPES_PER_PROFILE,
    MAX_SUBJECTS,
    Digest,
    Identifier,
    PositiveQuantity,
    Quantity,
    ResourceVectorV1,
    WorkerShapeV1,
    canonical_digest,
)
from loom_capacity_manager.grant_contracts import ReservationShapeV1

_LOWER_HEX = re.compile(r"^[0-9a-f]+$")
_EMPTY_JOURNAL_DIGEST = "0" * 64
MAX_EXECUTABLE_ADMISSION_WORK_BYTES = MAX_CONTRACT_BYTES
_MAX_EXECUTABLE_ADMISSION_CLOSURE_ENVELOPE_BYTES = len(
    json.dumps(
        {
            "schema_version": 2,
            "closure_id": str(UUID(int=0)),
            "proposal": {},
            "close_reason": "allocation-superseded",
            "executable": False,
        },
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("ascii")
) - len(b"{}")
MAX_EXECUTABLE_ADMISSION_PROPOSAL_BYTES = (
    MAX_EXECUTABLE_ADMISSION_WORK_BYTES
    - _MAX_EXECUTABLE_ADMISSION_CLOSURE_ENVELOPE_BYTES
)


def _validate_journal_head(sequence: int, digest: str) -> None:
    if (sequence == 0) != (digest == _EMPTY_JOURNAL_DIGEST):
        raise ValueError("an empty journal must use the canonical zero digest exactly")


def _utc_time(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("contract time must be timezone-aware")
    return value.astimezone(UTC)


class StrictV2Model(BaseModel):
    """Frozen, strict base for executable bridge protocol documents."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    schema_version: Literal[2] = 2

    @model_validator(mode="before")
    @classmethod
    def _json_values_to_strict_types(cls, value: Any) -> Any:
        if not isinstance(value, dict):
            return value
        normalized = dict(value)
        for name, field in cls.model_fields.items():
            if get_origin(field.annotation) is tuple and isinstance(normalized.get(name), list):
                normalized[name] = tuple(normalized[name])
            if field.annotation is datetime and isinstance(normalized.get(name), str):
                timestamp = normalized[name]
                if timestamp.endswith("Z"):
                    timestamp = f"{timestamp[:-1]}+00:00"
                try:
                    normalized[name] = datetime.fromisoformat(timestamp)
                except ValueError:
                    pass
        return normalized


class CandidateBindingV2(StrictV2Model):
    """One immutable candidate identity without cross-algorithm translation."""

    algorithm: Literal["git-sha1", "source-sha256"]
    identity: Annotated[str, Field(min_length=40, max_length=64)]
    publication_sha256: Digest

    @model_validator(mode="after")
    def _exact_algorithm_length(self) -> CandidateBindingV2:
        expected = 40 if self.algorithm == "git-sha1" else 64
        if len(self.identity) != expected or _LOWER_HEX.fullmatch(self.identity) is None:
            raise ValueError("candidate identity does not match its algorithm")
        return self


class ExecutionContextV2(StrictV2Model):
    """Prepared or authorized execution epoch independent of one plan."""

    authority_incarnation: UUID
    writer_epoch: PositiveQuantity
    configuration_epoch: PositiveQuantity
    execution_epoch: PositiveQuantity
    execution_manifest_sha256: Digest
    execution_state: Literal["prepared", "active", "drain-only"]
    executable_new_capacity_ceiling: Quantity
    executable_new_capacity_rate_per_minute: Quantity
    trusted_fleet_release_sha256: Digest

    @model_validator(mode="after")
    def _state_matches_ceiling(self) -> ExecutionContextV2:
        if self.execution_state == "prepared" and self.executable_new_capacity_ceiling != 0:
            raise ValueError("prepared execution context requires a zero ceiling")
        if self.execution_state == "prepared" and self.executable_new_capacity_rate_per_minute != 0:
            raise ValueError("prepared execution context requires a zero rate")
        if self.execution_state == "active" and self.executable_new_capacity_ceiling == 0:
            raise ValueError("active execution authority requires a positive ceiling")
        if self.execution_state == "active" and self.executable_new_capacity_rate_per_minute == 0:
            raise ValueError("active execution authority requires a positive rate")
        if self.execution_state == "drain-only" and self.executable_new_capacity_ceiling != 0:
            raise ValueError("drain-only execution authority requires a zero ceiling")
        if (
            self.execution_state == "drain-only"
            and self.executable_new_capacity_rate_per_minute != 0
        ):
            raise ValueError("drain-only execution authority requires a zero rate")
        return self


class ExecutionAuthorityV2(ExecutionContextV2):
    """Active or drain-only authority independent of one allocation plan."""

    execution_state: Literal["active", "drain-only"]
    executable: Literal[True] = True


class ExecutionFenceV2(ExecutionAuthorityV2):
    """Execution authority bound to one committed allocation epoch."""

    allocation_epoch: PositiveQuantity


class PreparedExecutorBindingV2(StrictV2Model):
    """One exact controller-local executor admitted to a prepared epoch."""

    pool_id: Literal["gb10", "oldlab"]
    pool_generation: PositiveQuantity
    executor_id: Identifier
    executor_incarnation: UUID
    signing_key_sha256: Digest
    local_authority_sha256: Digest
    controller_authority_sha256: Digest


class SubjectExecutionAcknowledgementV2(StrictV2Model):
    """Protected subject acknowledgement of one prepared global authority."""

    subject_id: UUID
    subject_incarnation: UUID
    configuration_generation: PositiveQuantity
    deployment_generation: PositiveQuantity
    candidate: CandidateBindingV2
    reporter_incarnation: UUID
    protected_admission_sha256: Digest
    legacy_writer_high_water: Quantity
    acknowledgement_sha256: Digest


class LegacyWriterFenceV2(StrictV2Model):
    """Exact high-water and freeze evidence for one legacy mutation writer."""

    writer_id: Identifier
    writer_kind: Literal[
        "allocation",
        "submission",
        "claim",
        "pressure",
        "cancellation",
        "release",
    ]
    scope_kind: Literal["global", "pool", "environment"]
    scope_id: Identifier
    high_water: Quantity
    freeze_evidence_sha256: Digest
    state: Literal["frozen", "retired"]


class PoolControllerAuthorityV2(StrictV2Model):
    """Operator-owned controller trust root for one physical pool."""

    pool_id: Literal["gb10", "oldlab"]
    controller_authority_sha256: Digest


class ExecutionPreparationPolicyV2(StrictV2Model):
    """Owner-only policy that makes execution preparation possible."""

    trusted_fleet_release_sha256: Digest
    executable_new_capacity_ceiling: PositiveQuantity
    executable_new_capacity_rate_per_minute: PositiveQuantity
    executors: Annotated[
        tuple[PreparedExecutorBindingV2, ...],
        Field(min_length=2, max_length=2),
    ]
    subject_acknowledgements: Annotated[
        tuple[SubjectExecutionAcknowledgementV2, ...],
        Field(max_length=MAX_SUBJECTS),
    ]
    rollback_evidence_sha256: Digest
    controller_authorities: Annotated[
        tuple[PoolControllerAuthorityV2, ...],
        Field(min_length=2, max_length=2),
    ]
    legacy_writer_fences: Annotated[
        tuple[LegacyWriterFenceV2, ...],
        Field(min_length=1, max_length=MAX_FIXED_CLAIMS_PER_REPORT),
    ]

    @field_validator("executors")
    @classmethod
    def _complete_executors(
        cls,
        value: tuple[PreparedExecutorBindingV2, ...],
    ) -> tuple[PreparedExecutorBindingV2, ...]:
        return ExecutionPreparationV2._complete_executors(value)

    @field_validator("subject_acknowledgements")
    @classmethod
    def _canonical_subjects(
        cls,
        value: tuple[SubjectExecutionAcknowledgementV2, ...],
    ) -> tuple[SubjectExecutionAcknowledgementV2, ...]:
        subject_ids = [item.subject_id for item in value]
        if len(subject_ids) != len(set(subject_ids)):
            raise ValueError("execution policy contains duplicate subject acknowledgement")
        return tuple(sorted(value, key=lambda item: item.subject_id.int))

    @field_validator("controller_authorities")
    @classmethod
    def _complete_controllers(
        cls,
        value: tuple[PoolControllerAuthorityV2, ...],
    ) -> tuple[PoolControllerAuthorityV2, ...]:
        if {item.pool_id for item in value} != {"gb10", "oldlab"}:
            raise ValueError("execution policy requires exactly gb10 and oldlab")
        return tuple(sorted(value, key=lambda item: item.pool_id))

    @field_validator("legacy_writer_fences")
    @classmethod
    def _complete_legacy_writer_inventory(
        cls,
        value: tuple[LegacyWriterFenceV2, ...],
    ) -> tuple[LegacyWriterFenceV2, ...]:
        keys = [
            (item.scope_kind, item.scope_id, item.writer_kind, item.writer_id) for item in value
        ]
        if not keys or len(keys) != len(set(keys)):
            raise ValueError("execution policy requires a complete legacy writer inventory")
        return tuple(
            sorted(
                value,
                key=lambda item: (
                    item.scope_kind,
                    item.scope_id,
                    item.writer_kind,
                    item.writer_id,
                ),
            )
        )


class ExecutionPreparationV2(StrictV2Model):
    """Complete immutable evidence needed to prepare an execution epoch."""

    authority_incarnation: UUID
    expected_writer_epoch: PositiveQuantity
    configuration_epoch: PositiveQuantity
    fleet_generation: PositiveQuantity
    fleet_digest: Digest
    trusted_fleet_release_sha256: Digest
    requested_ceiling: PositiveQuantity
    requested_rate_per_minute: PositiveQuantity
    executors: Annotated[
        tuple[PreparedExecutorBindingV2, ...],
        Field(min_length=2, max_length=2),
    ]
    subject_acknowledgements: Annotated[
        tuple[SubjectExecutionAcknowledgementV2, ...],
        Field(max_length=MAX_SUBJECTS),
    ] = ()
    legacy_writer_fences: Annotated[
        tuple[LegacyWriterFenceV2, ...],
        Field(max_length=MAX_FIXED_CLAIMS_PER_REPORT),
    ] = ()
    rollback_evidence_sha256: Digest
    executable: Literal[True] = True

    @field_validator("executors")
    @classmethod
    def _complete_executors(
        cls,
        value: tuple[PreparedExecutorBindingV2, ...],
    ) -> tuple[PreparedExecutorBindingV2, ...]:
        if {item.pool_id for item in value} != {"gb10", "oldlab"}:
            raise ValueError("execution preparation requires exactly gb10 and oldlab")
        if len({item.executor_id for item in value}) != len(value) or len(
            {item.executor_incarnation for item in value}
        ) != len(value):
            raise ValueError("execution preparation requires distinct pool executors")
        return tuple(sorted(value, key=lambda item: item.pool_id))

    @field_validator("subject_acknowledgements")
    @classmethod
    def _canonical_subjects(
        cls,
        value: tuple[SubjectExecutionAcknowledgementV2, ...],
    ) -> tuple[SubjectExecutionAcknowledgementV2, ...]:
        subject_ids = [item.subject_id for item in value]
        if len(subject_ids) != len(set(subject_ids)):
            raise ValueError("duplicate subject acknowledgement")
        return tuple(sorted(value, key=lambda item: item.subject_id.int))

    @field_validator("legacy_writer_fences")
    @classmethod
    def _canonical_legacy_writers(
        cls,
        value: tuple[LegacyWriterFenceV2, ...],
    ) -> tuple[LegacyWriterFenceV2, ...]:
        writer_keys = [
            (item.scope_kind, item.scope_id, item.writer_kind, item.writer_id) for item in value
        ]
        if len(writer_keys) != len(set(writer_keys)):
            raise ValueError("duplicate legacy writer fence")
        return tuple(
            sorted(
                value,
                key=lambda item: (
                    item.scope_kind,
                    item.scope_id,
                    item.writer_kind,
                    item.writer_id,
                ),
            )
        )


class ExecutionActivationV2(StrictV2Model):
    """One explicit bounded transition of an exact prepared epoch."""

    authority_incarnation: UUID
    expected_writer_epoch: PositiveQuantity
    execution_epoch: PositiveQuantity
    execution_manifest_sha256: Digest
    prepared_readiness_sha256: Digest
    executable_new_capacity_ceiling: PositiveQuantity
    executable_new_capacity_rate_per_minute: PositiveQuantity
    executable: Literal[True] = True

    @field_validator("prepared_readiness_sha256")
    @classmethod
    def _nonzero_prepared_readiness_digest(cls, value: str) -> str:
        if value == _EMPTY_JOURNAL_DIGEST:
            raise ValueError("prepared readiness digest must be nonzero")
        return value


class ExecutionPreparationAbortV2(StrictV2Model):
    """Retire one exact zero-ceiling prepared epoch without activating it."""

    authority_incarnation: UUID
    expected_writer_epoch: PositiveQuantity
    execution_epoch: PositiveQuantity
    execution_manifest_sha256: Digest
    executable: Literal[True] = True


class ExecutionDrainV2(StrictV2Model):
    """One explicit zeroing transition of an exact active epoch."""

    authority_incarnation: UUID
    expected_writer_epoch: PositiveQuantity
    execution_epoch: PositiveQuantity
    execution_manifest_sha256: Digest
    expected_executable_new_capacity_ceiling: PositiveQuantity
    expected_executable_new_capacity_rate_per_minute: PositiveQuantity
    executable: Literal[True] = True


class ExecutionRetirementExecutorCheckpointV2(StrictV2Model):
    """One executor's exact final heartbeat, journal, and inventory evidence."""

    executor_id: Identifier
    executor_incarnation: UUID
    pool_id: Literal["gb10", "oldlab"]
    pool_generation: PositiveQuantity
    heartbeat_sequence: PositiveQuantity
    command_sequence: Quantity
    journal_sequence: Quantity
    journal_digest: Digest
    inventory_sequence: PositiveQuantity
    inventory_digest: Digest

    @model_validator(mode="after")
    def _canonical_journal(self) -> ExecutionRetirementExecutorCheckpointV2:
        _validate_journal_head(self.journal_sequence, self.journal_digest)
        return self


class ExecutionRetirementV2(StrictV2Model):
    """Retire one exact drain-only epoch against both final pool checkpoints."""

    authority_incarnation: UUID
    expected_writer_epoch: PositiveQuantity
    execution_epoch: PositiveQuantity
    execution_manifest_sha256: Digest
    executor_checkpoints: Annotated[
        tuple[ExecutionRetirementExecutorCheckpointV2, ...],
        Field(min_length=2, max_length=2),
    ]
    executable: Literal[True] = True

    @field_validator("executor_checkpoints")
    @classmethod
    def _canonical_executor_checkpoints(
        cls,
        value: tuple[ExecutionRetirementExecutorCheckpointV2, ...],
    ) -> tuple[ExecutionRetirementExecutorCheckpointV2, ...]:
        pool_ids = tuple(item.pool_id for item in value)
        if set(pool_ids) != {"gb10", "oldlab"}:
            raise ValueError("execution retirement requires exactly gb10 and oldlab")
        if pool_ids != ("gb10", "oldlab"):
            raise ValueError("execution retirement requires canonical pool order")
        if (
            len({item.executor_id for item in value}) != 2
            or len({item.executor_incarnation for item in value}) != 2
        ):
            raise ValueError("execution retirement requires distinct pool executors")
        return value


class ExecutableReservationProposalV2(StrictV2Model):
    """Manager-authored exact capacity proposal under an execution fence."""

    tranche_id: UUID
    execution: ExecutionFenceV2
    subject_id: UUID
    subject_incarnation: UUID
    account_id: Identifier
    tier_id: Literal["production", "staging", "development"]
    candidate: CandidateBindingV2
    candidate_generation: PositiveQuantity
    deployment_generation: PositiveQuantity
    pool_id: Identifier
    pool_generation: PositiveQuantity
    executor_id: Identifier
    executor_incarnation: UUID
    shapes: Annotated[
        tuple[ReservationShapeV1, ...],
        Field(min_length=1, max_length=MAX_SHAPES_PER_PROFILE),
    ]
    executable: Literal[True] = True

    @field_validator("shapes")
    @classmethod
    def _canonical_shapes(
        cls,
        value: tuple[ReservationShapeV1, ...],
    ) -> tuple[ReservationShapeV1, ...]:
        shape_ids = [shape.shape_instance_id for shape in value]
        if len(shape_ids) != len(set(shape_ids)):
            raise ValueError("duplicate reservation shape identity")
        intent_ids = [shape.intent_id for shape in value]
        if len(intent_ids) != len(set(intent_ids)):
            raise ValueError("duplicate submission intent identity")
        return tuple(sorted(value, key=lambda shape: shape.shape_instance_id))


class ExecutableIntentBindingV2(StrictV2Model):
    """Complete immutable binding for one executable submission intent."""

    execution: ExecutionFenceV2
    tranche_id: UUID
    intent_id: UUID
    shape_instance_id: Identifier
    subject_id: UUID
    subject_incarnation: UUID
    account_id: Identifier
    tier_id: Literal["production", "staging", "development"]
    candidate: CandidateBindingV2
    candidate_generation: PositiveQuantity
    deployment_generation: PositiveQuantity
    pool_id: Identifier
    pool_generation: PositiveQuantity
    executor_id: Identifier
    executor_incarnation: UUID
    shape_id: Identifier
    profile_id: Identifier
    profile_generation: PositiveQuantity
    profile_digest: Digest
    concurrency_slots: PositiveQuantity
    resources: ResourceVectorV1
    node_ids: Annotated[tuple[Identifier, ...], Field(min_length=1)]
    rollout_surge_slots: Quantity = 0
    old_shape_backing_id: Identifier | None = None

    @field_validator("node_ids")
    @classmethod
    def _canonical_nodes(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if len(value) != len(set(value)):
            raise ValueError("duplicate intent node identity")
        return tuple(sorted(value))

    @model_validator(mode="after")
    def _resource_slots_match(self) -> ExecutableIntentBindingV2:
        if self.resources.slots != self.concurrency_slots:
            raise ValueError("intent resource slots must equal concurrency slots")
        if self.rollout_surge_slots > self.concurrency_slots:
            raise ValueError("intent rollout surge exceeds concurrency")
        if (self.rollout_surge_slots > 0) != (self.old_shape_backing_id is not None):
            raise ValueError("intent rollout surge backing must be present exactly for surge slots")
        return self


class ExecutableAdmissionAllowanceV2(StrictV2Model):
    """Manager-owned placement identity without environment-local attempt facts."""

    allowance_id: UUID
    protected_attempt_id: UUID
    shape_instance_id: Identifier
    shape_slot_index: Quantity
    submission_intent_id: UUID


class ExecutableAdmissionShapeV2(StrictV2Model):
    """One complete intent and worker-shape binding in a protected plan proposal."""

    binding: ExecutableIntentBindingV2
    protocol_generation: PositiveQuantity
    protocol_digest: Digest
    worker_shape: WorkerShapeV1
    worker_shape_digest: Digest
    bootstrap_registration_epoch: PositiveQuantity

    @model_validator(mode="after")
    def _exact_worker_shape(self) -> ExecutableAdmissionShapeV2:
        if (
            canonical_digest(self.worker_shape) != self.worker_shape_digest
            or self.worker_shape.shape_id != self.binding.shape_id
            or self.worker_shape.concurrency_slots != self.binding.concurrency_slots
            or self.worker_shape.total_resources != self.binding.resources
            or len(self.worker_shape.node_resources) != len(self.binding.node_ids)
        ):
            raise ValueError("admission worker shape differs from its executable intent")
        return self


class ExecutableAdmissionPlanProposalV2(StrictV2Model):
    """Complete manager-authored plan awaiting protected local attempt enrichment."""

    proposal_id: UUID
    plan_id: UUID
    admission_incarnation: UUID
    reporter_incarnation: UUID
    protected_admission_sha256: Digest
    manager_input_digest: Digest
    manager_allocation_digest: Digest
    lease_not_after: datetime
    shapes: Annotated[
        tuple[ExecutableAdmissionShapeV2, ...],
        Field(min_length=1, max_length=MAX_FIXED_CLAIMS_PER_REPORT),
    ]
    allowances: Annotated[
        tuple[ExecutableAdmissionAllowanceV2, ...],
        Field(max_length=MAX_FIXED_CLAIMS_PER_REPORT),
    ] = ()
    executable: Literal[True] = True

    _lease_not_after_utc = field_validator("lease_not_after")(_utc_time)

    @field_validator("shapes")
    @classmethod
    def _canonical_shapes(
        cls,
        value: tuple[ExecutableAdmissionShapeV2, ...],
    ) -> tuple[ExecutableAdmissionShapeV2, ...]:
        shape_ids = [item.binding.shape_instance_id for item in value]
        if len(shape_ids) != len(set(shape_ids)):
            raise ValueError("duplicate admission shape identity")
        intent_ids = [item.binding.intent_id for item in value]
        if len(intent_ids) != len(set(intent_ids)):
            raise ValueError("duplicate admission intent identity")
        return tuple(sorted(value, key=lambda item: item.binding.shape_instance_id))

    @field_validator("allowances")
    @classmethod
    def _canonical_allowances(
        cls,
        value: tuple[ExecutableAdmissionAllowanceV2, ...],
    ) -> tuple[ExecutableAdmissionAllowanceV2, ...]:
        allowance_ids = [item.allowance_id for item in value]
        if len(allowance_ids) != len(set(allowance_ids)):
            raise ValueError("duplicate admission allowance identity")
        attempt_ids = [item.protected_attempt_id for item in value]
        if len(attempt_ids) != len(set(attempt_ids)):
            raise ValueError("duplicate admission protected attempt")
        slots = [(item.shape_instance_id, item.shape_slot_index) for item in value]
        if len(slots) != len(set(slots)):
            raise ValueError("duplicate admission shape slot")
        return tuple(sorted(value, key=lambda item: item.allowance_id.int))

    @model_validator(mode="after")
    def _complete_plan_binding(self) -> ExecutableAdmissionPlanProposalV2:
        first = self.shapes[0]
        anchor = first.binding
        plan_binding = (
            anchor.execution,
            anchor.tranche_id,
            anchor.subject_id,
            anchor.subject_incarnation,
            anchor.candidate,
            anchor.candidate_generation,
            anchor.deployment_generation,
            anchor.pool_id,
            anchor.pool_generation,
            anchor.executor_id,
            anchor.executor_incarnation,
            anchor.profile_id,
            anchor.profile_generation,
            anchor.profile_digest,
            first.protocol_generation,
            first.protocol_digest,
        )
        for shape in self.shapes[1:]:
            binding = shape.binding
            if (
                binding.execution,
                binding.tranche_id,
                binding.subject_id,
                binding.subject_incarnation,
                binding.candidate,
                binding.candidate_generation,
                binding.deployment_generation,
                binding.pool_id,
                binding.pool_generation,
                binding.executor_id,
                binding.executor_incarnation,
                binding.profile_id,
                binding.profile_generation,
                binding.profile_digest,
                shape.protocol_generation,
                shape.protocol_digest,
            ) != plan_binding:
                raise ValueError("admission shapes do not share one exact plan binding")
        shapes = {item.binding.shape_instance_id: item for item in self.shapes}
        for allowance in self.allowances:
            prepared_shape = shapes.get(allowance.shape_instance_id)
            if (
                prepared_shape is None
                or allowance.submission_intent_id != prepared_shape.binding.intent_id
                or allowance.shape_slot_index >= prepared_shape.binding.concurrency_slots
            ):
                raise ValueError("allowance differs from its exact admission shape")
        plan_identities = {
            self.proposal_id,
            self.plan_id,
            self.admission_incarnation,
            self.reporter_incarnation,
            anchor.execution.authority_incarnation,
            anchor.tranche_id,
            anchor.subject_id,
            anchor.subject_incarnation,
            anchor.executor_incarnation,
        }
        if len(plan_identities) != 9:
            raise ValueError("admission plan authority identities must be distinct")
        return self


class ExecutableAdmissionPlanClosureV2(StrictV2Model):
    """Durable manager evidence that one exact unacknowledged plan is closed."""

    closure_id: UUID
    proposal: ExecutableAdmissionPlanProposalV2
    close_reason: Literal["expired", "allocation-superseded", "manager-closed"]
    executable: Literal[False] = False

    @model_validator(mode="after")
    def _distinct_closure_identity(self) -> ExecutableAdmissionPlanClosureV2:
        proposal = self.proposal
        anchor = proposal.shapes[0].binding
        if self.closure_id in {
            proposal.proposal_id,
            proposal.plan_id,
            proposal.admission_incarnation,
            anchor.tranche_id,
            anchor.subject_id,
            anchor.subject_incarnation,
        }:
            raise ValueError("admission closure identity must be distinct")
        return self


class ExecutableAdmissionPlanClosureAcknowledgementV2(StrictV2Model):
    """Protected-agent evidence that one exact manager closure was converged locally."""

    closure_id: UUID
    proposal_id: UUID
    proposal_digest: Digest
    plan_id: UUID
    admission_incarnation: UUID
    subject_id: UUID
    subject_incarnation: UUID
    reporter_incarnation: UUID
    protected_admission_sha256: Digest
    close_reason: Literal["expired", "allocation-superseded", "manager-closed"]
    disposition_kind: Literal["abandoned", "never-converged"]
    disposition_digest: Digest
    executable: Literal[False] = False

    @model_validator(mode="after")
    def _distinct_cleanup_identities(
        self,
    ) -> ExecutableAdmissionPlanClosureAcknowledgementV2:
        identities = {
            self.closure_id,
            self.proposal_id,
            self.plan_id,
            self.admission_incarnation,
            self.subject_id,
            self.subject_incarnation,
            self.reporter_incarnation,
        }
        if len(identities) != 7:
            raise ValueError("admission closure acknowledgement identities must be distinct")
        return self


class ProtectedAdmissionAssignmentV2(StrictV2Model):
    """Protected local attempt facts joined to one manager-owned allowance."""

    transition_id: UUID
    allowance_id: UUID
    protected_attempt_id: UUID
    execution_generation: PositiveQuantity
    requirements_digest: Digest
    shape_instance_id: Identifier
    shape_slot_index: Quantity
    submission_intent_id: UUID
    lifecycle_sequence: PositiveQuantity


class ExecutableAdmissionAcknowledgementV2(StrictV2Model):
    """Exact evidence that one manager plan was committed by the protected agent."""

    execution: ExecutionFenceV2
    tranche_id: UUID
    proposal_id: UUID
    plan_id: UUID
    admission_incarnation: UUID
    subject_id: UUID
    subject_incarnation: UUID
    pool_id: Literal["oldlab", "gb10"]
    reporter_incarnation: UUID
    protected_admission_sha256: Digest
    proposal_digest: Digest
    prepared_plan_digest: Digest
    assignment_count: Quantity
    assignments: Annotated[
        tuple[ProtectedAdmissionAssignmentV2, ...],
        Field(max_length=MAX_FIXED_CLAIMS_PER_REPORT),
    ] = ()
    executable: Literal[True] = True

    @field_validator("assignments")
    @classmethod
    def _canonical_assignments(
        cls,
        value: tuple[ProtectedAdmissionAssignmentV2, ...],
    ) -> tuple[ProtectedAdmissionAssignmentV2, ...]:
        for attribute, label in (
            ("transition_id", "transition identity"),
            ("allowance_id", "allowance identity"),
            ("protected_attempt_id", "protected attempt"),
        ):
            identities = [getattr(item, attribute) for item in value]
            if len(identities) != len(set(identities)):
                raise ValueError(f"duplicate protected admission {label}")
        slots = [(item.shape_instance_id, item.shape_slot_index) for item in value]
        if len(slots) != len(set(slots)):
            raise ValueError("duplicate protected admission shape slot")
        return tuple(sorted(value, key=lambda item: item.allowance_id.int))

    @model_validator(mode="after")
    def _complete_acknowledgement(self) -> ExecutableAdmissionAcknowledgementV2:
        if self.assignment_count != len(self.assignments):
            raise ValueError("protected admission assignment count changed")
        identities = {
            self.execution.authority_incarnation,
            self.tranche_id,
            self.proposal_id,
            self.plan_id,
            self.admission_incarnation,
            self.subject_id,
            self.subject_incarnation,
            self.reporter_incarnation,
        }
        if len(identities) != 8:
            raise ValueError("protected admission acknowledgement identities must be distinct")
        return self


class ExecutableExecutorRegistrationV2(StrictV2Model):
    """Register one controller-local executor against a prepared epoch."""

    execution: ExecutionContextV2
    executor_id: Identifier
    executor_incarnation: UUID
    pool_id: Identifier
    pool_generation: PositiveQuantity
    signing_key_id: Identifier
    signing_key_sha256: Digest
    local_authority_sha256: Digest
    controller_authority_sha256: Digest
    executable: Literal[True] = True


class ExecutableExecutorHeartbeatV2(StrictV2Model):
    """Renew one executor lease with an exact local journal checkpoint."""

    execution: ExecutionContextV2
    executor_id: Identifier
    executor_incarnation: UUID
    pool_id: Identifier
    pool_generation: PositiveQuantity
    heartbeat_sequence: PositiveQuantity
    journal_sequence: Quantity
    journal_digest: Digest
    journal_checkpoint_sequence: Quantity = 0
    journal_checkpoint_digest: Digest = _EMPTY_JOURNAL_DIGEST
    executable: Literal[True] = True

    @model_validator(mode="after")
    def _canonical_journal(self) -> ExecutableExecutorHeartbeatV2:
        _validate_journal_head(self.journal_sequence, self.journal_digest)
        _validate_journal_head(
            self.journal_checkpoint_sequence,
            self.journal_checkpoint_digest,
        )
        if self.journal_checkpoint_sequence > self.journal_sequence:
            raise ValueError("journal checkpoint cannot exceed the reported head")
        return self


class ExecutableOwnershipMetadataV2(StrictV2Model):
    """Complete signed controller metadata for one executable intent."""

    binding: ExecutableIntentBindingV2
    controller_authority_sha256: Digest
    trusted_launcher_sha256: Digest
    slurm_cluster: Identifier
    submitter_identity: Identifier
    association: Identifier
    submitted_at: datetime

    _submitted_at_utc = field_validator("submitted_at")(_utc_time)


class SignedExecutableOwnershipProofV2(StrictV2Model):
    """Ed25519 signature over executable ownership metadata."""

    metadata: ExecutableOwnershipMetadataV2
    signing_key_id: Identifier
    signature_base64: Annotated[str, Field(min_length=88, max_length=88)]

    @field_validator("signature_base64")
    @classmethod
    def _canonical_signature(cls, value: str) -> str:
        try:
            decoded = base64.b64decode(value, validate=True)
        except (ValueError, binascii.Error) as exc:
            raise ValueError("ownership signature must be canonical base64") from exc
        if len(decoded) != 64 or base64.b64encode(decoded).decode("ascii") != value:
            raise ValueError("ownership signature must encode exactly 64 bytes")
        return value


class ExecutableInventoryRecordV2(StrictV2Model):
    """One physical observation classified under executable ownership rules."""

    physical_identity: Identifier
    physical_kind: Literal["slurm-job", "worker"]
    authority_scope: Literal[
        "registered-loom",
        "dedicated-loom-association",
        "foreign",
    ]
    state: Literal["pending", "active", "draining", "terminal", "unknown"]
    resources: ResourceVectorV1
    node_ids: tuple[Identifier, ...] = ()
    controller_evidence_sha256: Digest
    ownership_proof: SignedExecutableOwnershipProofV2 | None = None
    terminal_evidence_sha256: Digest | None = None

    @field_validator("node_ids")
    @classmethod
    def _canonical_nodes(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if len(value) != len(set(value)):
            raise ValueError("duplicate executor inventory node identity")
        return tuple(sorted(value))

    @model_validator(mode="after")
    def _proof_and_terminal_evidence(self) -> ExecutableInventoryRecordV2:
        if self.resources.slots == 0:
            raise ValueError("executor inventory must charge positive slots")
        if self.authority_scope == "foreign" and self.ownership_proof is not None:
            raise ValueError("foreign inventory cannot carry Loom ownership proof")
        if (self.state == "terminal") != (self.terminal_evidence_sha256 is not None):
            raise ValueError("terminal evidence is required exactly for terminal inventory")
        return self


class ExecutableExecutorInventoryV2(StrictV2Model):
    """Complete executor inventory under a prepared or authorized epoch."""

    execution: ExecutionContextV2
    executor_id: Identifier
    executor_incarnation: UUID
    pool_id: Identifier
    pool_generation: PositiveQuantity
    inventory_sequence: PositiveQuantity
    journal_sequence: Quantity
    journal_digest: Digest
    journal_checkpoint_sequence: Quantity = 0
    journal_checkpoint_digest: Digest = _EMPTY_JOURNAL_DIGEST
    complete: Literal[True] = True
    records: Annotated[
        tuple[ExecutableInventoryRecordV2, ...],
        Field(max_length=MAX_FIXED_CLAIMS_PER_REPORT),
    ] = ()
    executable: Literal[True] = True

    @field_validator("records")
    @classmethod
    def _canonical_records(
        cls,
        value: tuple[ExecutableInventoryRecordV2, ...],
    ) -> tuple[ExecutableInventoryRecordV2, ...]:
        identities = [record.physical_identity for record in value]
        if len(identities) != len(set(identities)):
            raise ValueError("duplicate executor inventory identity")
        return tuple(sorted(value, key=lambda record: record.physical_identity))

    @model_validator(mode="after")
    def _canonical_journal(self) -> ExecutableExecutorInventoryV2:
        _validate_journal_head(self.journal_sequence, self.journal_digest)
        _validate_journal_head(
            self.journal_checkpoint_sequence,
            self.journal_checkpoint_digest,
        )
        if self.journal_checkpoint_sequence > self.journal_sequence:
            raise ValueError("journal checkpoint cannot exceed the reported head")
        for record in self.records:
            proof = record.ownership_proof
            if proof is None:
                continue
            binding = proof.metadata.binding
            if (
                binding.execution.authority_incarnation != self.execution.authority_incarnation
                or binding.execution.writer_epoch != self.execution.writer_epoch
                or binding.execution.configuration_epoch != self.execution.configuration_epoch
                or binding.execution.execution_epoch != self.execution.execution_epoch
                or binding.execution.execution_manifest_sha256
                != self.execution.execution_manifest_sha256
                or binding.execution.trusted_fleet_release_sha256
                != self.execution.trusted_fleet_release_sha256
                or binding.executor_id != self.executor_id
                or binding.executor_incarnation != self.executor_incarnation
                or binding.pool_id != self.pool_id
                or binding.pool_generation != self.pool_generation
            ):
                raise ValueError("inventory ownership proof has another executor binding")
        return self


class ExecutableReservationAcceptanceV2(StrictV2Model):
    """Executor acceptance of one exact executable proposal."""

    execution: ExecutionFenceV2
    tranche_id: UUID
    proposal_digest: Digest
    pool_id: Identifier
    pool_generation: PositiveQuantity
    executor_id: Identifier
    executor_incarnation: UUID
    command_sequence: PositiveQuantity
    executable: Literal[True] = True


class ExecutableBootstrapRegistrationV2(StrictV2Model):
    """Prepared environment bootstrap digest for one intent."""

    binding: ExecutableIntentBindingV2
    command_sequence: PositiveQuantity
    bootstrap_registration_epoch: PositiveQuantity
    bootstrap_evidence_sha256: Digest
    executable: Literal[True] = True


class ExecutableBootstrapProposalV2(StrictV2Model):
    """Executor proposal containing only a hash of one bootstrap secret."""

    binding: ExecutableIntentBindingV2
    command_sequence: PositiveQuantity
    proposal_epoch: PositiveQuantity
    bootstrap_sha256: Digest
    expires_at: datetime
    executable: Literal[True] = True

    _expires_at_utc = field_validator("expires_at")(_utc_time)


class ExecutableBootstrapAcknowledgementV2(StrictV2Model):
    """Subject-agent proof that the exact bootstrap hash is protected locally."""

    binding: ExecutableIntentBindingV2
    proposal_epoch: PositiveQuantity
    proposal_digest: Digest
    reporter_incarnation: UUID
    bootstrap_registration_epoch: PositiveQuantity
    bootstrap_evidence_sha256: Digest
    protected_admission_sha256: Digest
    executable: Literal[True] = True


class ExecutableLaunchPermitV2(StrictV2Model):
    """Manager launch order and bounded scheduler authorization."""

    permit_id: UUID
    binding: ExecutableIntentBindingV2
    permit_epoch: PositiveQuantity
    launch_rank: PositiveQuantity
    expires_at: datetime
    bootstrap_registration_epoch: PositiveQuantity
    bootstrap_evidence_sha256: Digest
    executable: Literal[True] = True

    _expires_at_utc = field_validator("expires_at")(_utc_time)


class ExecutablePermitConsumptionV2(StrictV2Model):
    """Journaled transition immediately before a scheduler submission."""

    permit_id: UUID
    permit_digest: Digest
    binding: ExecutableIntentBindingV2
    command_sequence: PositiveQuantity
    executable: Literal[True] = True


class ExecutableSubmissionRecoveryV2(StrictV2Model):
    """Authoritative post-crash proof that a consumed permit was never submitted."""

    binding: ExecutableIntentBindingV2
    permit_id: UUID
    permit_digest: Digest
    command_sequence: PositiveQuantity
    inventory_sequence: PositiveQuantity
    inventory_digest: Digest
    controller_query_completed_at: datetime
    submit_process_absent: Literal[True]
    scheduler_submission_absent: Literal[True]
    controller_evidence_sha256: Digest
    executable: Literal[True] = True

    _controller_query_completed_at_utc = field_validator("controller_query_completed_at")(_utc_time)


class ExecutableIntentCloseV2(StrictV2Model):
    """Central close fence for one unused or terminal intent."""

    binding: ExecutableIntentBindingV2
    command_sequence: PositiveQuantity
    bootstrap_registration_epoch: PositiveQuantity | None = None
    bootstrap_evidence_sha256: Digest | None = None
    executable: Literal[True] = True

    @model_validator(mode="after")
    def _bootstrap_evidence_pair(self) -> ExecutableIntentCloseV2:
        if (self.bootstrap_registration_epoch is None) != (self.bootstrap_evidence_sha256 is None):
            raise ValueError("close bootstrap registration epoch and evidence must appear together")
        return self


class ExecutableProtectedReleaseV2(StrictV2Model):
    """Protected environment proof that a worker can no longer claim."""

    binding: ExecutableIntentBindingV2
    reporter_incarnation: UUID
    bootstrap_registration_epoch: Quantity
    protected_registration_epoch: PositiveQuantity
    bootstrap_revoked: Literal[True]
    protected_release_sha256: Digest
    executable: Literal[True] = True

    @model_validator(mode="after")
    def _release_epoch_advanced(self) -> ExecutableProtectedReleaseV2:
        if self.protected_registration_epoch <= self.bootstrap_registration_epoch:
            raise ValueError("protected release epoch must advance past bootstrap")
        return self


class ExecutableReleasedShapeV2(StrictV2Model):
    """Physical terminal evidence retaining the complete intent binding."""

    binding: ExecutableIntentBindingV2
    inventory_sequence: PositiveQuantity
    terminal_kind: Literal["unused", "slurm-job", "worker"]
    terminal_identity: Identifier
    terminal_evidence_sha256: Digest
    protected_registration_epoch: PositiveQuantity
    bootstrap_revoked: Literal[True]
    protected_release_sha256: Digest


class ExecutablePartialReleaseV2(StrictV2Model):
    """Monotonic physical terminal evidence for exact reservation shapes."""

    execution: ExecutionFenceV2
    tranche_id: UUID
    executor_id: Identifier
    executor_incarnation: UUID
    command_sequence: PositiveQuantity
    releases: Annotated[
        tuple[ExecutableReleasedShapeV2, ...],
        Field(min_length=1, max_length=MAX_SHAPES_PER_PROFILE),
    ]
    executable: Literal[True] = True

    @field_validator("releases")
    @classmethod
    def _canonical_releases(
        cls,
        value: tuple[ExecutableReleasedShapeV2, ...],
    ) -> tuple[ExecutableReleasedShapeV2, ...]:
        shape_ids = [release.binding.shape_instance_id for release in value]
        if len(shape_ids) != len(set(shape_ids)):
            raise ValueError("duplicate release shape identity")
        terminals = [(release.terminal_kind, release.terminal_identity) for release in value]
        if len(terminals) != len(set(terminals)):
            raise ValueError("duplicate terminal identity")
        return tuple(sorted(value, key=lambda release: release.binding.shape_instance_id))

    @model_validator(mode="after")
    def _releases_match_command(self) -> ExecutablePartialReleaseV2:
        for release in self.releases:
            binding = release.binding
            if (
                binding.execution != self.execution
                or binding.tranche_id != self.tranche_id
                or binding.executor_id != self.executor_id
                or binding.executor_incarnation != self.executor_incarnation
            ):
                raise ValueError("released shape has another command binding")
        return self


def canonical_executable_bytes(contract: StrictV2Model) -> bytes:
    """Encode one schema-v2 contract with bounded deterministic JSON."""

    if not isinstance(contract, StrictV2Model):
        raise ValueError("canonical executable encoding requires a schema-v2 model")
    encoded = json.dumps(
        contract.model_dump(mode="json", exclude_none=False),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("ascii")
    if len(encoded) > MAX_CONTRACT_BYTES:
        raise ValueError("canonical executable contract exceeds maximum byte size")
    return encoded


def validate_executable_admission_work_size(payload: bytes) -> bytes:
    """Accept one admission response at the shared limit and reject one byte more."""

    if not isinstance(payload, bytes):
        raise ValueError("executable admission work payload must be bytes")
    if len(payload) > MAX_EXECUTABLE_ADMISSION_WORK_BYTES:
        raise ValueError("executable admission work exceeds its response byte bound")
    return payload


def canonical_executable_admission_work_bytes(
    work: ExecutableAdmissionPlanProposalV2 | ExecutableAdmissionPlanClosureV2,
) -> bytes:
    """Encode one exact manager admission response under its shared wire bound."""

    if not isinstance(
        work,
        (ExecutableAdmissionPlanProposalV2, ExecutableAdmissionPlanClosureV2),
    ):
        raise ValueError("executable admission work has an invalid contract")
    return validate_executable_admission_work_size(canonical_executable_bytes(work))


def canonical_executable_digest(contract: StrictV2Model) -> str:
    """Return the SHA-256 digest of one canonical executable contract."""

    return hashlib.sha256(canonical_executable_bytes(contract)).hexdigest()


def canonical_inventory_confirmation_journal_head(
    inventory: ExecutableExecutorInventoryV2,
) -> tuple[int, str]:
    """Derive the exact journal head after requested/confirmed publication."""

    if not isinstance(inventory, ExecutableExecutorInventoryV2):
        raise ValueError("inventory confirmation requires an executable inventory")
    payload = canonical_executable_bytes(inventory)
    payload_digest = hashlib.sha256(payload).hexdigest()
    payload_base64 = base64.b64encode(payload).decode("ascii")
    object_id = str(inventory.executor_incarnation)

    def record_digest(*, sequence: int, previous_digest: str, event_kind: str) -> str:
        record = {
            "schema_version": 2,
            "sequence": sequence,
            "previous_digest": previous_digest,
            "event_kind": event_kind,
            "object_kind": "inventory",
            "object_id": object_id,
            "payload_digest": payload_digest,
            "payload_base64": payload_base64,
        }
        encoded = json.dumps(
            record,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            allow_nan=False,
        ).encode("ascii")
        return hashlib.sha256(encoded).hexdigest()

    requested_sequence = inventory.journal_sequence + 1
    requested_digest = record_digest(
        sequence=requested_sequence,
        previous_digest=inventory.journal_digest,
        event_kind="inventory-publish-requested",
    )
    confirmation_sequence = requested_sequence + 1
    confirmation_digest = record_digest(
        sequence=confirmation_sequence,
        previous_digest=requested_digest,
        event_kind="inventory-publish-confirmed",
    )
    return confirmation_sequence, confirmation_digest


__all__ = [
    "MAX_EXECUTABLE_ADMISSION_PROPOSAL_BYTES",
    "MAX_EXECUTABLE_ADMISSION_WORK_BYTES",
    "CandidateBindingV2",
    "ExecutableAdmissionAcknowledgementV2",
    "ExecutableAdmissionAllowanceV2",
    "ExecutableAdmissionPlanClosureAcknowledgementV2",
    "ExecutableAdmissionPlanClosureV2",
    "ExecutableAdmissionPlanProposalV2",
    "ExecutableAdmissionShapeV2",
    "ExecutableBootstrapAcknowledgementV2",
    "ExecutableBootstrapProposalV2",
    "ExecutableBootstrapRegistrationV2",
    "ExecutableExecutorHeartbeatV2",
    "ExecutableExecutorInventoryV2",
    "ExecutableExecutorRegistrationV2",
    "ExecutableIntentBindingV2",
    "ExecutableIntentCloseV2",
    "ExecutableInventoryRecordV2",
    "ExecutableLaunchPermitV2",
    "ExecutableOwnershipMetadataV2",
    "ExecutablePartialReleaseV2",
    "ExecutablePermitConsumptionV2",
    "ExecutableProtectedReleaseV2",
    "ExecutableReleasedShapeV2",
    "ExecutableReservationAcceptanceV2",
    "ExecutableReservationProposalV2",
    "ExecutableSubmissionRecoveryV2",
    "ExecutionActivationV2",
    "ExecutionAuthorityV2",
    "ExecutionContextV2",
    "ExecutionDrainV2",
    "ExecutionFenceV2",
    "ExecutionPreparationAbortV2",
    "ExecutionPreparationPolicyV2",
    "ExecutionPreparationV2",
    "ExecutionRetirementExecutorCheckpointV2",
    "ExecutionRetirementV2",
    "LegacyWriterFenceV2",
    "PoolControllerAuthorityV2",
    "PreparedExecutorBindingV2",
    "ProtectedAdmissionAssignmentV2",
    "SignedExecutableOwnershipProofV2",
    "StrictV2Model",
    "SubjectExecutionAcknowledgementV2",
    "canonical_executable_admission_work_bytes",
    "canonical_executable_bytes",
    "canonical_executable_digest",
    "canonical_inventory_confirmation_journal_head",
    "validate_executable_admission_work_size",
]
