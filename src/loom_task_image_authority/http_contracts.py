"""Immutable historical publication candidate response schemas."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from loom_task_image_authority.contracts import (
    Digest,
    ManifestDigest,
    NonzeroUUID,
    PositiveSignedBigint,
    RegistryCredentialGeneration,
    TaskImageBaseResolutionEvidenceV1,
    TaskImageComponent,
)
from loom_task_image_authority.registry_token import publication_repository


class _StrictResponse(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)


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


class TaskImagePublicationCandidateResponseV2(TaskImagePublicationCandidateResponseV1):
    """Canonical acknowledgement binding the required same-build observations."""

    schema_version: Literal["loom.task-image-publication-candidate.v2"] = Field(...)  # type: ignore[assignment]
    base_resolution: TaskImageBaseResolutionEvidenceV1

    @model_validator(mode="after")
    def _metadata_matches_output(self) -> TaskImagePublicationCandidateResponseV2:
        if (
            self.base_resolution.output_digest != self.manifest_digest
            or self.base_resolution.platform != self.platform
        ):
            raise ValueError("candidate base-resolution binding is invalid")
        return self
