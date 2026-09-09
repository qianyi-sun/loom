"""Exact retained-data lineage; these records grant no storage capability.

The lifecycle must authenticate the source's persisted release and the current
destination operation before using this recipe. A digest is not that authority.
"""

from __future__ import annotations

from typing import Literal
from uuid import UUID

from pydantic import Field, field_validator, model_validator

from loom.personal_dev_incarnation_storage import (
    PersonalDevStorageBindingV1,
    parse_personal_dev_storage_binding,
)
from loom_capacity_manager.contracts import Digest, StrictV1Model, canonical_bytes, canonical_digest

_MAX_TRANSFER_BINDING_BYTES = 32 * 1024
StorageBucketPurpose = Literal["tasks", "trajectories", "artifacts"]


class PersonalDevStorageTransferBindingV1(StrictV1Model):
    source: PersonalDevStorageBindingV1
    destination: PersonalDevStorageBindingV1
    source_destroy_operation_id: UUID
    source_destroy_operation_epoch: int = Field(gt=0, le=2**63 - 1)
    source_release_sha256: Digest
    destination_operation_id: UUID
    destination_operation_epoch: int = Field(gt=0, le=2**63 - 1)

    @field_validator("source_destroy_operation_id", "destination_operation_id")
    @classmethod
    def _operation_id(cls, value: UUID) -> UUID:
        if value.int == 0:
            raise ValueError("storage transfer operation identity must be nonzero")
        return value

    @field_validator("source", "destination")
    @classmethod
    def _bound_storage(cls, value: PersonalDevStorageBindingV1) -> PersonalDevStorageBindingV1:
        # Nested unchecked model_copy/model_construct objects are not trusted.
        binding = parse_personal_dev_storage_binding(canonical_bytes(value), expected_sha256=canonical_digest(value))
        if binding.layout != "incarnation-v1":
            raise ValueError("storage transfer requires explicit incarnation storage")
        return binding

    @model_validator(mode="after")
    def _same_owner_successor(self) -> PersonalDevStorageTransferBindingV1:
        if any(getattr(self.source, field) != getattr(self.destination, field) for field in (
            "environment_name", "subject_id", "owner_user_id", "owner_team_id",
        )):
            raise ValueError("storage transfer must remain within one owner's subject lineage")
        if (
            self.source.subject_incarnation == self.destination.subject_incarnation
            or self.source_destroy_operation_id == self.destination_operation_id
            or self.destination_operation_epoch <= self.source_destroy_operation_epoch
            or self.source_release_sha256 == "0" * 64
        ):
            raise ValueError("storage transfer requires fresh storage and later operation authority")
        return self


def parse_storage_transfer_binding(payload: bytes, *, expected_sha256: str) -> PersonalDevStorageTransferBindingV1:
    if not isinstance(payload, bytes) or not 0 < len(payload) <= _MAX_TRANSFER_BINDING_BYTES:
        raise ValueError("storage transfer binding exceeds its byte bound")
    binding = PersonalDevStorageTransferBindingV1.model_validate_json(payload)
    if canonical_bytes(binding) != payload or canonical_digest(binding) != expected_sha256:
        raise ValueError("storage transfer differs from its pinned canonical binding")
    return binding


def storage_transfer_bucket_pairs(binding: PersonalDevStorageTransferBindingV1) -> tuple[tuple[StorageBucketPurpose, str, str], ...]:
    binding = parse_storage_transfer_binding(canonical_bytes(binding), expected_sha256=canonical_digest(binding))
    source, destination = binding.source.identity, binding.destination.identity
    return (
        ("tasks", source.task_bucket, destination.task_bucket),
        ("trajectories", source.trajectories_bucket, destination.trajectories_bucket),
        ("artifacts", source.artifacts_bucket, destination.artifacts_bucket),
    )
