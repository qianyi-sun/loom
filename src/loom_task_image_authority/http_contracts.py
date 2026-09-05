"""Strict bounded HTTP response envelopes for rootless materialization leases."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Annotated, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from loom.task_image_build_plan import TaskImageBuildPlanV1
from loom_task_image_authority.contracts import (
    Digest,
    ManifestDigest,
    NonzeroUUID,
    PositiveSignedBigint,
    RegistryCredentialGeneration,
    TaskImageComponent,
)
from loom_task_image_authority.registry_token import publication_repository


class _StrictResponse(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)


class TaskImageMaterializationClaimResponseV1(_StrictResponse):
    schema_version: Literal["loom.task-image-materialization-claim.v1"] = (
        "loom.task-image-materialization-claim.v1"
    )
    claim_id: UUID
    materialization_id: UUID
    attempt_id: UUID
    lease_epoch: Annotated[int, Field(gt=0)]
    state: Literal["claimed", "running"]
    deterministic_failure_count: Annotated[int, Field(ge=0)]
    lease_expires_at: datetime
    plan: TaskImageBuildPlanV1

    @field_validator("claim_id", "materialization_id", "attempt_id")
    @classmethod
    def _ids_are_nonzero(cls, value: UUID) -> UUID:
        if value.int == 0:
            raise ValueError("claim response UUID must be nonzero")
        return value

    @field_validator("lease_expires_at")
    @classmethod
    def _expiry_is_utc(cls, value: datetime) -> datetime:
        if value.utcoffset() is None:
            raise ValueError("claim response expiry must be timezone-aware")
        return value.astimezone(UTC)

    @model_validator(mode="after")
    def _plan_matches_lease(self) -> TaskImageMaterializationClaimResponseV1:
        if self.plan.materialization_id != self.materialization_id:
            raise ValueError("claim response plan differs from materialization")
        return self


class TaskImageMaterializationOperationResponseV1(_StrictResponse):
    schema_version: Literal["loom.task-image-materialization-operation.v1"] = (
        "loom.task-image-materialization-operation.v1"
    )
    operation: Literal[
        "start",
        "heartbeat",
        "release",
        "containment_release",
        "deterministic_fail",
    ]
    operation_id: UUID
    materialization_id: UUID
    attempt_id: UUID
    lease_epoch: Annotated[int, Field(gt=0)]
    state: Literal["claimed", "running", "queued", "failed"]
    deterministic_failure_count: Annotated[int, Field(ge=0)]
    lease_expires_at: datetime | None

    @field_validator("operation_id", "materialization_id", "attempt_id")
    @classmethod
    def _ids_are_nonzero(cls, value: UUID) -> UUID:
        if value.int == 0:
            raise ValueError("operation response UUID must be nonzero")
        return value

    @field_validator("lease_expires_at")
    @classmethod
    def _expiry_is_utc(cls, value: datetime | None) -> datetime | None:
        if value is None:
            return None
        if value.utcoffset() is None:
            raise ValueError("operation response expiry must be timezone-aware")
        return value.astimezone(UTC)

    @model_validator(mode="after")
    def _lease_state_is_exact(self) -> TaskImageMaterializationOperationResponseV1:
        if (self.state in {"claimed", "running"}) != (self.lease_expires_at is not None):
            raise ValueError("operation response state and lease expiry disagree")
        return self


class TaskImagePublicationCandidateResponseV1(_StrictResponse):
    """Immutable acknowledgement of one inert publication candidate."""

    schema_version: Literal["loom.task-image-publication-candidate.v1"] = (
        "loom.task-image-publication-candidate.v1"
    )
    candidate_id: NonzeroUUID
    operation_id: NonzeroUUID
    credential_id: NonzeroUUID
    credential_generation: RegistryCredentialGeneration
    grant_id: NonzeroUUID
    session_id: NonzeroUUID
    session_generation: PositiveSignedBigint
    materialization_id: NonzeroUUID
    attempt_id: NonzeroUUID
    attempt_number: PositiveSignedBigint
    lease_epoch: PositiveSignedBigint
    builder_id: Annotated[str, Field(pattern=r"^rootless:[0-9a-f]{32}$")]
    component: TaskImageComponent
    repository: Annotated[str, Field(min_length=1, max_length=255)]
    manifest_digest: ManifestDigest
    manifest_size: PositiveSignedBigint
    oci_file_sha256: Digest
    oci_file_size: PositiveSignedBigint
    platform: Literal["linux/amd64", "linux/arm64"]
    recorded_at: datetime

    @field_validator("recorded_at")
    @classmethod
    def _recorded_at_is_utc(cls, value: datetime) -> datetime:
        if value.utcoffset() is None:
            raise ValueError("publication candidate timestamp must be timezone-aware")
        return value.astimezone(UTC)

    @model_validator(mode="after")
    def _repository_is_exact(self) -> TaskImagePublicationCandidateResponseV1:
        cpu_arch = "x86_64" if self.platform == "linux/amd64" else "arm64"
        expected = publication_repository(
            purpose="production",
            shadow_campaign_id=None,
            cpu_arch=cpu_arch,
            attempt_id=self.attempt_id,
            component=self.component,
        )
        if self.repository != expected:
            raise ValueError("publication candidate repository binding is invalid")
        return self


__all__ = [
    "TaskImageMaterializationClaimResponseV1",
    "TaskImageMaterializationOperationResponseV1",
    "TaskImagePublicationCandidateResponseV1",
]
