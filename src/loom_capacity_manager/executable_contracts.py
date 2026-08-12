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
    Digest,
    Identifier,
    PositiveQuantity,
    Quantity,
    ResourceVectorV1,
)
from loom_capacity_manager.grant_contracts import ReservationShapeV1

_LOWER_HEX = re.compile(r"^[0-9a-f]+$")
_EMPTY_JOURNAL_DIGEST = "0" * 64


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
    trusted_fleet_release_sha256: Digest

    @model_validator(mode="after")
    def _state_matches_ceiling(self) -> ExecutionContextV2:
        if self.execution_state == "prepared" and self.executable_new_capacity_ceiling != 0:
            raise ValueError("prepared execution context requires a zero ceiling")
        if self.execution_state == "active" and self.executable_new_capacity_ceiling == 0:
            raise ValueError("active execution authority requires a positive ceiling")
        return self


class ExecutionAuthorityV2(ExecutionContextV2):
    """Active or drain-only authority independent of one allocation plan."""

    execution_state: Literal["active", "drain-only"]


class ExecutionFenceV2(ExecutionAuthorityV2):
    """Execution authority bound to one committed allocation epoch."""

    allocation_epoch: PositiveQuantity


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


class ExecutableLaunchPermitV2(StrictV2Model):
    """Manager launch order and bounded scheduler authorization."""

    permit_id: UUID
    binding: ExecutableIntentBindingV2
    permit_epoch: PositiveQuantity
    launch_rank: PositiveQuantity
    expires_at: datetime
    executable: Literal[True] = True

    _expires_at_utc = field_validator("expires_at")(_utc_time)


class ExecutablePermitConsumptionV2(StrictV2Model):
    """Journaled transition immediately before a scheduler submission."""

    permit_id: UUID
    permit_digest: Digest
    binding: ExecutableIntentBindingV2
    command_sequence: PositiveQuantity
    executable: Literal[True] = True


class ExecutableIntentCloseV2(StrictV2Model):
    """Central close fence for one unused or terminal intent."""

    binding: ExecutableIntentBindingV2
    command_sequence: PositiveQuantity
    executable: Literal[True] = True


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


def canonical_executable_digest(contract: StrictV2Model) -> str:
    """Return the SHA-256 digest of one canonical executable contract."""

    return hashlib.sha256(canonical_executable_bytes(contract)).hexdigest()


__all__ = [
    "CandidateBindingV2",
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
    "ExecutionAuthorityV2",
    "ExecutionContextV2",
    "ExecutionFenceV2",
    "SignedExecutableOwnershipProofV2",
    "StrictV2Model",
    "canonical_executable_bytes",
    "canonical_executable_digest",
]
