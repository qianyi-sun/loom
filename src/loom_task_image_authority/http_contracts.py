"""Strict bounded HTTP response envelopes for rootless materialization leases."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Annotated, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from loom.task_image_build_plan import TaskImageBuildPlanV1


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


__all__ = [
    "TaskImageMaterializationClaimResponseV1",
    "TaskImageMaterializationOperationResponseV1",
]
