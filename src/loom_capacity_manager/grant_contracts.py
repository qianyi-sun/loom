"""Strict dry-run grant contracts for Package 3 of the global fleet manager.

The contracts in this module can reserve and account for identities, but every
top-level operation is permanently non-executable.  No value here can be
interpreted as permission to invoke a pool scheduler.
"""

from __future__ import annotations

import base64
import binascii
from typing import Annotated, Literal
from uuid import UUID

from pydantic import Field, field_validator, model_validator

from loom_capacity_manager.contracts import (
    MAX_FIXED_CLAIMS_PER_REPORT,
    MAX_SHAPES_PER_PROFILE,
    Digest,
    Identifier,
    PositiveQuantity,
    Quantity,
    ResourceVectorV1,
    StrictV1Model,
    canonical_digest,
)

_EMPTY_JOURNAL_DIGEST = "0" * 64


def _validate_journal_head(sequence: int, digest: str) -> None:
    if (sequence == 0) != (digest == _EMPTY_JOURNAL_DIGEST):
        raise ValueError("an empty journal must use the canonical zero digest exactly")


class ReservationShapeV1(StrictV1Model):
    """One immutable worker-shape identity inside a reservation tranche."""

    shape_instance_id: Identifier
    intent_id: UUID
    shape_id: Identifier
    profile_id: Identifier
    profile_generation: PositiveQuantity
    profile_digest: Digest
    concurrency_slots: PositiveQuantity
    resources: ResourceVectorV1
    node_ids: Annotated[tuple[Identifier, ...], Field(min_length=1)]
    rollout_surge_slots: Quantity = 0
    old_shape_backing_id: Identifier | None = None

    @model_validator(mode="after")
    def _exact_resource_and_surge_binding(self) -> ReservationShapeV1:
        if self.resources.slots != self.concurrency_slots:
            raise ValueError("reservation resource slots must equal concurrency slots")
        if self.rollout_surge_slots > self.concurrency_slots:
            raise ValueError("rollout surge exceeds reservation concurrency")
        if (self.rollout_surge_slots > 0) != (self.old_shape_backing_id is not None):
            raise ValueError("rollout surge backing must be present exactly for surge slots")
        return self

    @field_validator("node_ids")
    @classmethod
    def _node_ids(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if len(value) != len(set(value)):
            raise ValueError("duplicate reservation node identity")
        return tuple(sorted(value))


class DryRunReservationProposalV1(StrictV1Model):
    """Manager-authored identity set that can never authorize execution."""

    tranche_id: UUID
    authority_incarnation: UUID
    writer_epoch: PositiveQuantity
    configuration_epoch: PositiveQuantity
    allocation_epoch: PositiveQuantity
    subject_id: UUID
    subject_incarnation: UUID
    account_id: Identifier
    tier_id: Literal["production", "staging", "development"]
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
    executable: Literal[False] = False

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


class DryRunExecutorRegistrationV1(StrictV1Model):
    """Trusted registration input for one non-executable pool executor."""

    executor_id: Identifier
    executor_incarnation: UUID
    pool_id: Identifier
    pool_generation: PositiveQuantity
    signing_key_id: Identifier
    signing_key_sha256: Digest
    local_authority_sha256: Digest
    executable: Literal[False] = False


class DryRunExecutorHeartbeatV1(StrictV1Model):
    """Lease renewal bound to one authority and durable local journal head."""

    authority_incarnation: UUID
    writer_epoch: PositiveQuantity
    executor_id: Identifier
    executor_incarnation: UUID
    pool_id: Identifier
    pool_generation: PositiveQuantity
    heartbeat_sequence: PositiveQuantity
    journal_sequence: Quantity
    journal_digest: Digest
    journal_checkpoint_sequence: Quantity = 0
    journal_checkpoint_digest: Digest = _EMPTY_JOURNAL_DIGEST
    executable: Literal[False] = False

    @model_validator(mode="after")
    def _canonical_journal_head(self) -> DryRunExecutorHeartbeatV1:
        _validate_journal_head(self.journal_sequence, self.journal_digest)
        _validate_journal_head(
            self.journal_checkpoint_sequence,
            self.journal_checkpoint_digest,
        )
        if self.journal_checkpoint_sequence > self.journal_sequence:
            raise ValueError("journal checkpoint cannot exceed the reported head")
        return self


class OwnershipMetadataV1(StrictV1Model):
    """Manager-derived immutable fields embedded in one executor-owned job."""

    authority_incarnation: UUID
    writer_epoch: PositiveQuantity
    configuration_epoch: PositiveQuantity
    allocation_epoch: PositiveQuantity
    tranche_id: UUID
    intent_id: UUID
    shape_instance_id: Identifier
    subject_id: UUID
    subject_incarnation: UUID
    account_id: Identifier
    tier_id: Literal["production", "staging", "development"]
    candidate_generation: PositiveQuantity
    deployment_generation: PositiveQuantity
    pool_id: Identifier
    pool_generation: PositiveQuantity
    shape_id: Identifier
    profile_id: Identifier
    profile_generation: PositiveQuantity
    profile_digest: Digest
    concurrency_slots: PositiveQuantity
    resources: ResourceVectorV1
    node_ids: Annotated[tuple[Identifier, ...], Field(min_length=1)]
    executor_id: Identifier
    executor_incarnation: UUID
    executable: Literal[False] = False

    @model_validator(mode="after")
    def _resource_slots_match(self) -> OwnershipMetadataV1:
        if self.resources.slots != self.concurrency_slots:
            raise ValueError("ownership resource slots must equal concurrency slots")
        return self

    @field_validator("node_ids")
    @classmethod
    def _ownership_nodes(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if len(value) != len(set(value)):
            raise ValueError("duplicate ownership node identity")
        return tuple(sorted(value))


class SignedOwnershipProofV1(StrictV1Model):
    """Ed25519 signature over the canonical manager-owned metadata."""

    metadata: OwnershipMetadataV1
    signing_key_id: Identifier
    signature_base64: Annotated[str, Field(min_length=88, max_length=88)]

    @field_validator("signature_base64")
    @classmethod
    def _signature(cls, value: str) -> str:
        try:
            decoded = base64.b64decode(value, validate=True)
        except (ValueError, binascii.Error) as exc:
            raise ValueError("ownership signature must be canonical base64") from exc
        if len(decoded) != 64 or base64.b64encode(decoded).decode("ascii") != value:
            raise ValueError("ownership signature must encode exactly 64 bytes")
        return value


class ExecutorInventoryRecordV1(StrictV1Model):
    """One exact controller observation; ownership is classified centrally."""

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
    ownership_proof: SignedOwnershipProofV1 | None = None
    terminal_evidence_sha256: Digest | None = None

    @field_validator("node_ids")
    @classmethod
    def _nodes(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if len(value) != len(set(value)):
            raise ValueError("duplicate inventory node identity")
        return tuple(sorted(value))

    @model_validator(mode="after")
    def _proof_and_terminal_evidence(self) -> ExecutorInventoryRecordV1:
        if self.resources.slots == 0:
            raise ValueError("executor inventory must charge positive slots")
        if self.authority_scope == "foreign" and self.ownership_proof is not None:
            raise ValueError("foreign inventory cannot carry Loom ownership proof")
        if (self.state == "terminal") != (self.terminal_evidence_sha256 is not None):
            raise ValueError("terminal evidence is required exactly for terminal inventory")
        return self


class DryRunExecutorInventoryV1(StrictV1Model):
    """One complete, monotonic pool inventory with no mutation authority."""

    authority_incarnation: UUID
    writer_epoch: PositiveQuantity
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
        tuple[ExecutorInventoryRecordV1, ...],
        Field(max_length=MAX_FIXED_CLAIMS_PER_REPORT),
    ] = ()
    executable: Literal[False] = False

    @field_validator("records")
    @classmethod
    def _records(
        cls,
        value: tuple[ExecutorInventoryRecordV1, ...],
    ) -> tuple[ExecutorInventoryRecordV1, ...]:
        identities = [item.physical_identity for item in value]
        if len(identities) != len(set(identities)):
            raise ValueError("duplicate executor inventory identity")
        return tuple(sorted(value, key=lambda item: item.physical_identity))

    @model_validator(mode="after")
    def _proofs_match_executor(self) -> DryRunExecutorInventoryV1:
        _validate_journal_head(self.journal_sequence, self.journal_digest)
        _validate_journal_head(
            self.journal_checkpoint_sequence,
            self.journal_checkpoint_digest,
        )
        if self.journal_checkpoint_sequence > self.journal_sequence:
            raise ValueError("journal checkpoint cannot exceed the reported head")
        for record in self.records:
            proof = record.ownership_proof
            if proof is not None and (
                proof.metadata.authority_incarnation != self.authority_incarnation
                or proof.metadata.writer_epoch != self.writer_epoch
                or proof.metadata.executor_id != self.executor_id
                or proof.metadata.executor_incarnation != self.executor_incarnation
                or proof.metadata.pool_id != self.pool_id
                or proof.metadata.pool_generation != self.pool_generation
            ):
                raise ValueError("inventory ownership proof has another executor binding")
        return self


class DryRunReservationAcceptanceV1(StrictV1Model):
    """Executor acknowledgement of one exact, still-current proposal."""

    tranche_id: UUID
    proposal_digest: Digest
    executor_id: Identifier
    executor_incarnation: UUID
    command_sequence: PositiveQuantity
    executable: Literal[False] = False


class DryRunBootstrapRegistrationV1(StrictV1Model):
    """Journal-fsynced bootstrap evidence for one prepared intent."""

    tranche_id: UUID
    intent_id: UUID
    executor_id: Identifier
    executor_incarnation: UUID
    command_sequence: PositiveQuantity
    bootstrap_registration_epoch: PositiveQuantity
    bootstrap_evidence_sha256: Digest
    executable: Literal[False] = False


class DryRunLaunchPermitV1(StrictV1Model):
    """Manager-authored launch ordering record with no scheduler authority."""

    permit_id: UUID
    intent_id: UUID
    allocation_epoch: PositiveQuantity
    configuration_epoch: PositiveQuantity
    executor_id: Identifier
    executor_incarnation: UUID
    permit_epoch: PositiveQuantity
    launch_rank: PositiveQuantity
    executable: Literal[False] = False


class DryRunPermitConsumptionV1(StrictV1Model):
    """Executor request to account for a permit without invoking a scheduler."""

    permit_id: UUID
    permit_digest: Digest
    intent_id: UUID
    executor_id: Identifier
    executor_incarnation: UUID
    command_sequence: PositiveQuantity
    executable: Literal[False] = False


class DryRunIntentCloseV1(StrictV1Model):
    """Central pre-release fence for one not-yet-submitted intent."""

    tranche_id: UUID
    intent_id: UUID
    executor_id: Identifier
    executor_incarnation: UUID
    command_sequence: PositiveQuantity
    executable: Literal[False] = False


class ReleasedShapeV1(StrictV1Model):
    """Exact terminal evidence for one accepted reservation shape."""

    shape_instance_id: Identifier
    intent_id: UUID
    inventory_sequence: PositiveQuantity
    terminal_kind: Literal["unused", "slurm-job", "worker"]
    terminal_identity: Identifier
    terminal_evidence_sha256: Digest
    protected_registration_epoch: PositiveQuantity
    bootstrap_revoked: Literal[True]
    protected_release_sha256: Digest


class DryRunProtectedReleaseAcknowledgementV1(StrictV1Model):
    """Subject-agent proof that one bootstrap can no longer register or claim."""

    authority_incarnation: UUID
    writer_epoch: PositiveQuantity
    configuration_epoch: PositiveQuantity
    allocation_epoch: PositiveQuantity
    tranche_id: UUID
    shape_instance_id: Identifier
    intent_id: UUID
    subject_id: UUID
    subject_incarnation: UUID
    reporter_incarnation: UUID
    deployment_generation: PositiveQuantity
    pool_id: Identifier
    pool_generation: PositiveQuantity
    bootstrap_registration_epoch: Quantity
    protected_registration_epoch: PositiveQuantity
    bootstrap_revoked: Literal[True]
    protected_release_sha256: Digest
    executable: Literal[False] = False

    @model_validator(mode="after")
    def _registration_epoch_advanced(self) -> DryRunProtectedReleaseAcknowledgementV1:
        if self.protected_registration_epoch <= self.bootstrap_registration_epoch:
            raise ValueError("protected release epoch must advance past bootstrap")
        return self


class DryRunPartialReleaseV1(StrictV1Model):
    """Monotonic evidence set that releases only its named shape identities."""

    tranche_id: UUID
    executor_id: Identifier
    executor_incarnation: UUID
    command_sequence: PositiveQuantity
    releases: Annotated[
        tuple[ReleasedShapeV1, ...],
        Field(min_length=1, max_length=MAX_SHAPES_PER_PROFILE),
    ]
    executable: Literal[False] = False

    @field_validator("releases")
    @classmethod
    def _canonical_releases(
        cls,
        value: tuple[ReleasedShapeV1, ...],
    ) -> tuple[ReleasedShapeV1, ...]:
        shape_ids = [release.shape_instance_id for release in value]
        if len(shape_ids) != len(set(shape_ids)):
            raise ValueError("duplicate release shape identity")
        terminal_ids = [(release.terminal_kind, release.terminal_identity) for release in value]
        if len(terminal_ids) != len(set(terminal_ids)):
            raise ValueError("duplicate terminal identity")
        return tuple(sorted(value, key=lambda release: release.shape_instance_id))


def canonical_grant_digest(contract: StrictV1Model) -> str:
    """Return the shared bounded canonical digest for one Package 3 contract."""

    return canonical_digest(contract)


__all__ = [
    "DryRunBootstrapRegistrationV1",
    "DryRunExecutorHeartbeatV1",
    "DryRunExecutorInventoryV1",
    "DryRunExecutorRegistrationV1",
    "DryRunIntentCloseV1",
    "DryRunLaunchPermitV1",
    "DryRunPartialReleaseV1",
    "DryRunPermitConsumptionV1",
    "DryRunProtectedReleaseAcknowledgementV1",
    "DryRunReservationAcceptanceV1",
    "DryRunReservationProposalV1",
    "ExecutorInventoryRecordV1",
    "OwnershipMetadataV1",
    "ReleasedShapeV1",
    "ReservationShapeV1",
    "SignedOwnershipProofV1",
    "canonical_grant_digest",
]
