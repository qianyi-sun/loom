"""Closed Pipeline lifecycle enums and StageResult v1 validation."""

from __future__ import annotations

import math
import unicodedata
from enum import StrEnum
from typing import Annotated, Literal, TypeVar
from uuid import UUID

from pydantic import Field, StringConstraints, field_validator, model_validator

from loom.pipeline.spec import (
    ArtifactType,
    BindingName,
    ContainerNodeV1,
    Digest,
    PipelineModel,
    PlatformFanoutIndexV1,
    reject_secret_literals,
)


class PipelineRunState(StrEnum):
    SUBMITTED = "submitted"
    RUNNING = "running"
    CANCELLING = "cancelling"
    FINISHED = "finished"


class PipelineRunResult(StrEnum):
    SUCCEEDED = "succeeded"
    PARTIAL_FAILED = "partial_failed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    BUDGET_EXHAUSTED = "budget_exhausted"


class PipelineStageRunState(StrEnum):
    BLOCKED = "blocked"
    READY = "ready"
    QUEUED = "queued"
    CLAIMED = "claimed"
    RUNNING = "running"
    RETRY_WAIT = "retry_wait"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"
    SKIPPED = "skipped"


class ExecutionAttemptState(StrEnum):
    FAULT_PENDING = "fault_pending"
    QUEUED = "queued"
    CLAIMED = "claimed"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"
    LOST = "lost"


class RetryClass(StrEnum):
    NONE = "none"
    CONTRACT_ERROR = "contract_error"
    PROVIDER_TRANSIENT = "provider_transient"
    INFRASTRUCTURE_TRANSIENT = "infrastructure_transient"
    INTERNAL_DEFECT = "internal_defect"
    CANCELLED = "cancelled"


RUN_TRANSITIONS: dict[PipelineRunState, frozenset[PipelineRunState]] = {
    PipelineRunState.SUBMITTED: frozenset(
        {PipelineRunState.RUNNING, PipelineRunState.CANCELLING, PipelineRunState.FINISHED}
    ),
    PipelineRunState.RUNNING: frozenset(
        {PipelineRunState.CANCELLING, PipelineRunState.FINISHED}
    ),
    PipelineRunState.CANCELLING: frozenset({PipelineRunState.FINISHED}),
    PipelineRunState.FINISHED: frozenset(),
}

STAGE_TRANSITIONS: dict[PipelineStageRunState, frozenset[PipelineStageRunState]] = {
    PipelineStageRunState.BLOCKED: frozenset(
        {
            PipelineStageRunState.READY,
            PipelineStageRunState.FAILED,
            PipelineStageRunState.CANCELLED,
            PipelineStageRunState.SKIPPED,
        }
    ),
    PipelineStageRunState.READY: frozenset(
        {
            PipelineStageRunState.QUEUED,
            PipelineStageRunState.FAILED,
            PipelineStageRunState.CANCELLED,
        }
    ),
    PipelineStageRunState.QUEUED: frozenset(
        {PipelineStageRunState.CLAIMED, PipelineStageRunState.CANCELLED}
    ),
    PipelineStageRunState.CLAIMED: frozenset(
        {
            PipelineStageRunState.RUNNING,
            PipelineStageRunState.RETRY_WAIT,
            PipelineStageRunState.FAILED,
            PipelineStageRunState.CANCELLED,
        }
    ),
    PipelineStageRunState.RUNNING: frozenset(
        {
            PipelineStageRunState.SUCCEEDED,
            PipelineStageRunState.RETRY_WAIT,
            PipelineStageRunState.FAILED,
            PipelineStageRunState.CANCELLED,
        }
    ),
    PipelineStageRunState.RETRY_WAIT: frozenset(
        {PipelineStageRunState.QUEUED, PipelineStageRunState.FAILED, PipelineStageRunState.CANCELLED}
    ),
    PipelineStageRunState.SUCCEEDED: frozenset(),
    PipelineStageRunState.FAILED: frozenset(),
    PipelineStageRunState.CANCELLED: frozenset(),
    PipelineStageRunState.SKIPPED: frozenset(),
}

ATTEMPT_TRANSITIONS: dict[ExecutionAttemptState, frozenset[ExecutionAttemptState]] = {
    ExecutionAttemptState.FAULT_PENDING: frozenset(
        {ExecutionAttemptState.QUEUED, ExecutionAttemptState.CANCELLED}
    ),
    ExecutionAttemptState.QUEUED: frozenset(
        {ExecutionAttemptState.CLAIMED, ExecutionAttemptState.CANCELLED}
    ),
    ExecutionAttemptState.CLAIMED: frozenset(
        {
            ExecutionAttemptState.RUNNING,
            ExecutionAttemptState.FAILED,
            ExecutionAttemptState.CANCELLED,
            ExecutionAttemptState.LOST,
        }
    ),
    ExecutionAttemptState.RUNNING: frozenset(
        {
            ExecutionAttemptState.SUCCEEDED,
            ExecutionAttemptState.FAILED,
            ExecutionAttemptState.CANCELLED,
            ExecutionAttemptState.LOST,
        }
    ),
    ExecutionAttemptState.SUCCEEDED: frozenset(),
    ExecutionAttemptState.FAILED: frozenset(),
    ExecutionAttemptState.CANCELLED: frozenset(),
    ExecutionAttemptState.LOST: frozenset(),
}


StateT = TypeVar("StateT", bound=StrEnum)


def require_transition(
    current: StateT,
    target: StateT,
    table: dict[StateT, frozenset[StateT]],
) -> None:
    """Raise on an illegal lifecycle edge; terminal states are immutable."""

    if target not in table[current]:
        raise ValueError(f"illegal transition: {current.value} -> {target.value}")


class StageResultInputV1(PipelineModel):
    binding_name: BindingName
    item_key: str
    artifact_id: UUID
    artifact_type: ArtifactType
    manifest_sha256: Digest

    @field_validator("item_key")
    @classmethod
    def normalize_item_key(cls, value: str) -> str:
        value = unicodedata.normalize("NFC", value)
        if not value or len(value.encode("utf-8")) > 128:
            raise ValueError("item_key must be 1..128 UTF-8 bytes")
        return value


class StageResultOutputV1(PipelineModel):
    name: BindingName
    artifact_type: ArtifactType


class StageResultProvenanceV1(PipelineModel):
    pipeline_run_id: Annotated[UUID, Field(strict=False)]
    stage_run_id: Annotated[UUID, Field(strict=False)]
    execution_attempt_id: Annotated[UUID, Field(strict=False)]
    recipe_digest: Digest
    execution_spec_digest: Digest
    image_digest: Digest


class StageResultErrorV1(PipelineModel):
    code: Annotated[str, StringConstraints(pattern=r"^[a-z][a-z0-9_]{0,127}$")]
    message: str

    @field_validator("message")
    @classmethod
    def validate_message(cls, value: str) -> str:
        if len(value.encode("utf-8", errors="strict")) > 4096:
            raise ValueError("error message exceeds 4096 UTF-8 bytes")
        return value


class StageResultV1(PipelineModel):
    schema_version: Literal["loom.stage-result.v1"]
    domain_outcome: str | None
    reason_code: Annotated[str, StringConstraints(pattern=r"^[a-z][a-z0-9_]{0,127}$")]
    retry_class: Annotated[RetryClass, Field(strict=False)]
    inputs: list[StageResultInputV1]
    outputs: list[StageResultOutputV1]
    metrics: dict[str, int | float]
    provenance: StageResultProvenanceV1
    error: StageResultErrorV1 | None

    @field_validator("domain_outcome")
    @classmethod
    def validate_outcome(cls, value: str | None) -> str | None:
        if value is None:
            return None
        value = unicodedata.normalize("NFC", value)
        if not value or len(value.encode("utf-8")) > 128:
            raise ValueError("domain_outcome must be 1..128 UTF-8 bytes")
        return value

    @field_validator("outputs")
    @classmethod
    def outputs_are_canonical(cls, values: list[StageResultOutputV1]) -> list[StageResultOutputV1]:
        names = [item.name for item in values]
        if names != sorted(names, key=lambda item: item.encode("utf-8")) or len(names) != len(
            set(names)
        ):
            raise ValueError("StageResult outputs must be unique and bytewise sorted")
        return values

    @field_validator("metrics")
    @classmethod
    def validate_metrics(cls, values: dict[str, int | float]) -> dict[str, int | float]:
        if len(values) > 128:
            raise ValueError("metrics exceeds 128 keys")
        normalized: dict[str, int | float] = {}
        for key, value in values.items():
            key = unicodedata.normalize("NFC", key)
            if key in normalized:
                raise ValueError("metric keys collide after NFC normalization")
            try:
                UUID(key)
            except ValueError:
                pass
            else:
                raise ValueError("metric keys cannot be IDs")
            if not key or len(key.encode("utf-8")) > 128:
                raise ValueError("metric key is invalid")
            if isinstance(value, float) and not math.isfinite(value):
                raise ValueError("metric values must be finite")
            if isinstance(value, int) and not -(2**53 - 1) <= value <= 2**53 - 1:
                raise ValueError("metric integer is outside the interoperable JSON range")
            normalized[key] = value
        return normalized

    @model_validator(mode="after")
    def validate_result_semantics(self) -> StageResultV1:
        reject_secret_literals(self)
        if self.domain_outcome is not None:
            if self.retry_class is not RetryClass.NONE or self.error is not None:
                raise ValueError("domain outcome requires retry_class=none and error=null")
        elif self.retry_class is RetryClass.NONE and self.error is None:
            raise ValueError("result without a domain outcome must explain its failure")
        return self


def validate_stage_result(
    result: StageResultV1 | None,
    *,
    exit_code: int,
    node: ContainerNodeV1,
    expected_inputs: list[StageResultInputV1],
    expected_provenance: StageResultProvenanceV1,
    fanout_index: PlatformFanoutIndexV1 | None = None,
) -> StageResultV1 | None:
    """Apply process- and node-bound validation before result persistence."""

    if result is None:
        if exit_code == 0:
            raise ValueError("rc=0 requires StageResult")
        return None
    if result.inputs != expected_inputs:
        raise ValueError("StageResult inputs do not match the frozen claim")
    if result.provenance != expected_provenance:
        raise ValueError("StageResult provenance does not match the frozen claim")
    if exit_code == 0:
        if result.domain_outcome is None or result.retry_class is not RetryClass.NONE or result.error:
            raise ValueError("rc=0 requires a valid domain result")
    elif result.domain_outcome is not None:
        raise ValueError("nonzero exit cannot report a domain outcome")

    declared = {output.name: output for output in node.outputs if output.producer == "container"}
    item_template_name = node.fanout_commit.item_binding_name if node.fanout_commit else None
    literal = {name: output for name, output in declared.items() if name != item_template_name}
    actual = {output.name: output for output in result.outputs}
    if node.fanout_commit is None:
        if fanout_index is not None:
            raise ValueError("fanout index is invalid without fanout_commit")
        dynamic_names: set[str] = set()
    else:
        if fanout_index is None:
            raise ValueError("fanout_commit StageResult requires the validated index")
        indexed_names = {item.output_name for item in fanout_index.items}
        if indexed_names & set(literal):
            raise ValueError("fanout index item collides with a literal output")
        dynamic_names = set(actual) - set(literal)
        if dynamic_names != indexed_names:
            raise ValueError("StageResult dynamic outputs do not match the fanout index")
    for name, output in actual.items():
        declaration = literal.get(name)
        if declaration is None and item_template_name is not None and name in dynamic_names:
            declaration = declared[item_template_name]
        if declaration is None or declaration.artifact_type != output.artifact_type:
            raise ValueError("StageResult lists an undeclared output")
    missing = [
        name
        for name, output in literal.items()
        if output.required and name not in actual
    ]
    if exit_code == 0 and missing:
        raise ValueError(f"StageResult is missing required outputs: {missing}")
    if any(output.producer == "platform" and output.name in actual for output in node.outputs):
        raise ValueError("StageResult cannot list platform outputs")
    return result
