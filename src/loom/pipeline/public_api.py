"""Closed public Pipeline API contracts and controller-only submission ports.

This module intentionally contains no database, HTTP, authentication, or object-store
implementation.  It is the shared typed boundary used by those adapters.
"""

from __future__ import annotations

import unicodedata
from datetime import datetime
from enum import StrEnum
from typing import TYPE_CHECKING, Annotated, Any, Literal, Protocol
from uuid import UUID

from pydantic import (
    AfterValidator,
    BaseModel,
    ConfigDict,
    Field,
    StringConstraints,
    field_validator,
    model_validator,
)

from loom.pipeline.keys import canonical_digest
from loom.pipeline.spec import (
    BindingName,
    Digest,
    NodeKey,
    NonNegativeSafeInt,
    PipelineModel,
    PositiveSafeInt,
    PositiveVersion,
    ProviderAttemptLimitsV1,
    RecipeIdentityV1,
    RunBudgetV1,
)
from loom.pipeline.state import PipelineRunResult, PipelineRunState, PipelineStageRunState

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession


def validate_idempotency_key(value: str) -> str:
    """Validate the durable lookup key without normalizing or rewriting it."""

    if not isinstance(value, str):
        raise TypeError("Idempotency-Key must be a string")
    if not 1 <= len(value) <= 128 or not value.isascii():
        raise ValueError("Idempotency-Key must be 1..128 printable ASCII characters")
    if value != value.strip() or any(
        ord(character) < 0x20 or ord(character) > 0x7E for character in value
    ):
        raise ValueError(
            "Idempotency-Key must not contain surrounding whitespace or control characters"
        )
    return value


IdempotencyKey = Annotated[str, AfterValidator(validate_idempotency_key)]


class PipelineIdempotencyEndpoint(StrEnum):
    """Route-registration-owned names for team-scoped Pipeline mutations."""

    PIPELINE_RUN_SUBMIT = "pipeline_run_submit"
    PIPELINE_STAGE_RETRY = "pipeline_stage_retry"
    PIPELINE_INPUT_IMPORT_CREATE = "pipeline_input_import_create"
    PIPELINE_INPUT_IMPORT_COMPLETE = "pipeline_input_import_complete"
    PIPELINE_INPUT_IMPORT_ABORT = "pipeline_input_import_abort"
    PIPELINE_INPUTS_MATERIALIZE = "pipeline_inputs_materialize"


def pipeline_request_digest(
    *,
    endpoint: PipelineIdempotencyEndpoint,
    team_id: UUID,
    request: PipelineModel,
) -> Digest:
    """Hash one parsed/default-expanded team request using the #1211 document rules."""

    if not isinstance(endpoint, PipelineIdempotencyEndpoint):
        raise TypeError("endpoint must be selected by route registration")
    if not isinstance(team_id, UUID):
        raise TypeError("team_id must be the authenticated team UUID")
    if not isinstance(request, PipelineModel):
        raise TypeError("request must be a parsed strict PipelineModel")
    return canonical_digest(
        {
            "endpoint": endpoint.value,
            "team_id": team_id,
            "request": request.model_dump(mode="json", exclude_none=False),
        }
    )


# Explicit aliases keep adapters from growing endpoint-local digest variants.
canonical_pipeline_request_digest = pipeline_request_digest
compute_pipeline_request_digest = pipeline_request_digest


RecipeRef = Annotated[
    str,
    StringConstraints(pattern=r"^[a-z][a-z0-9-]{0,127}@[1-9][0-9]{0,9}$"),
]
DisplayName = Annotated[str, StringConstraints(min_length=1)]
CancelReason = Annotated[str, StringConstraints(min_length=1)]

class PipelineReadModel(BaseModel):
    """Closed response model that accepts adapter serialization primitives."""

    model_config = ConfigDict(extra="forbid")


def _normalize_bounded_text(value: str, *, label: str, maximum_bytes: int) -> str:
    normalized = unicodedata.normalize("NFC", value)
    encoded = normalized.encode("utf-8", errors="strict")
    if not encoded or len(encoded) > maximum_bytes:
        raise ValueError(f"{label} must be 1..{maximum_bytes} UTF-8 bytes")
    return normalized


def _aware(value: datetime, *, label: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{label} must include a timezone")
    return value


class PipelineRunSubmitRequestV1(PipelineModel):
    budget: RunBudgetV1
    display_name: DisplayName | None = None
    inputs: Annotated[dict[BindingName, UUID], Field(max_length=128)]
    parameters: dict[str, Any]
    recipe: RecipeRef
    judge_profile_id: UUID | None = None

    @field_validator("display_name")
    @classmethod
    def normalize_display_name(cls, value: str | None) -> str | None:
        if value is None:
            return None
        return _normalize_bounded_text(value, label="display_name", maximum_bytes=200)

    @field_validator("recipe")
    @classmethod
    def normalize_recipe(cls, value: str) -> str:
        return unicodedata.normalize("NFC", value)

    @field_validator("inputs")
    @classmethod
    def inputs_are_bytewise_ordered(cls, value: dict[str, UUID]) -> dict[str, UUID]:
        # JSON object order is irrelevant to the digest, but requiring no duplicate
        # normalized names keeps validation behavior deterministic across adapters.
        normalized = [unicodedata.normalize("NFC", key) for key in value]
        if len(normalized) != len(set(normalized)):
            raise ValueError("input names must be unique after NFC normalization")
        return value


class PipelineRunRetryRequestV1(PipelineModel):
    budget: RunBudgetV1
    display_name: DisplayName | None = None

    @field_validator("display_name")
    @classmethod
    def normalize_display_name(cls, value: str | None) -> str | None:
        if value is None:
            return None
        return _normalize_bounded_text(value, label="display_name", maximum_bytes=200)


# Endpoint terminology uses retry-stage while the operation creates a new Run.
PipelineStageRetryRequestV1 = PipelineRunRetryRequestV1


class PipelineRunCancelRequestV1(PipelineModel):
    reason: CancelReason

    @field_validator("reason")
    @classmethod
    def normalize_reason(cls, value: str) -> str:
        return _normalize_bounded_text(value, label="reason", maximum_bytes=500)


class PipelineRunListQueryV1(PipelineModel):
    state: PipelineRunState | None = None
    result: PipelineRunResult | None = None
    recipe: RecipeRef | None = None
    created_after: datetime | None = None
    created_before: datetime | None = None
    cursor: Annotated[str, StringConstraints(min_length=1, max_length=4096)] | None = None
    limit: Annotated[int, Field(strict=True, ge=1, le=100)] = 100

    @field_validator("created_after", "created_before")
    @classmethod
    def timestamps_are_aware(cls, value: datetime | None) -> datetime | None:
        if value is None:
            return None
        return _aware(value, label="list timestamp")

    @model_validator(mode="after")
    def timestamp_window_is_ordered(self) -> PipelineRunListQueryV1:
        if (
            self.created_after is not None
            and self.created_before is not None
            and self.created_after >= self.created_before
        ):
            raise ValueError("created_after must be earlier than created_before")
        return self


class PipelineRunEventsQueryV1(PipelineModel):
    after_seq: NonNegativeSafeInt = 0
    limit: Annotated[int, Field(strict=True, ge=1, le=500)] = 200


class PipelineRunEventV1(PipelineReadModel):
    seq: PositiveSafeInt
    stage_run_id: UUID | None
    execution_attempt_id: UUID | None
    event_type: Annotated[str, StringConstraints(pattern=r"^[a-z][a-z0-9_]{0,127}$")]
    payload: dict[str, Any]
    created_at: datetime

    @field_validator("created_at")
    @classmethod
    def created_at_is_aware(cls, value: datetime) -> datetime:
        return _aware(value, label="event created_at")


class PipelineRunEventsResponseV1(PipelineReadModel):
    events: Annotated[list[PipelineRunEventV1], Field(max_length=500)]
    next_after_seq: NonNegativeSafeInt
    terminal: bool
    retry_after_ms: Literal[1000] | None

    @model_validator(mode="after")
    def cursor_and_events_are_consistent(self) -> PipelineRunEventsResponseV1:
        sequences = [event.seq for event in self.events]
        if sequences and sequences != list(range(sequences[0], sequences[0] + len(sequences))):
            raise ValueError("event sequences must be strictly increasing and gap-free")
        if sequences and self.next_after_seq != sequences[-1]:
            raise ValueError("next_after_seq must equal the last returned sequence")
        if not self.terminal and not sequences and self.retry_after_ms != 1000:
            raise ValueError("an empty nonterminal response must request a 1000 ms retry")
        if self.terminal and self.retry_after_ms is not None:
            raise ValueError("a terminal event response cannot request another poll")
        return self


# Public read projections intentionally live beside the mutation contracts.  This
# gives FastAPI/OpenAPI one closed source of truth and keeps the browser from
# recreating server response shapes with handwritten interfaces (#1217).
PipelineNodeKind = Literal["container", "gate"]
PipelineResourceClass = Literal["controller", "cpu", "gpu"]
PipelineRetryIneligibleReason = Literal[
    "hosted_pipeline_execution_unsupported",
    "run_not_retryable",
    "stage_not_failed",
    "recipe_snapshot_unavailable",
    "input_drift",
    "input_not_reusable",
    "binding_drift",
    "budget_invalid",
]
ExecutionAttemptState = Literal[
    "fault_pending", "queued", "claimed", "running", "succeeded", "failed", "cancelled", "lost"
]


class PipelineRecipeIdentityProjectionV1(PipelineReadModel):
    name: str
    version: PositiveVersion
    digest: str


class PipelineStageRunSummaryV1(PipelineReadModel):
    id: UUID
    node_key: str
    shard_key: str
    node_kind: PipelineNodeKind
    topological_level: NonNegativeSafeInt
    upstream_node_keys: list[str]
    state: PipelineStageRunState
    domain_outcome: str | None
    reason_code: str | None
    attempt_count: NonNegativeSafeInt
    resource_profile_name: str | None
    resource_class: PipelineResourceClass
    retry_allowed: bool
    retry_ineligible_reason: PipelineRetryIneligibleReason | None


class PipelineNodeProgressV1(PipelineReadModel):
    total_stage_runs: NonNegativeSafeInt
    completed_stage_runs: NonNegativeSafeInt
    states: dict[PipelineStageRunState, NonNegativeSafeInt]
    domain_outcomes: dict[str, NonNegativeSafeInt]


class PipelineNodeTopologyV1(PipelineReadModel):
    node_key: NodeKey
    node_kind: PipelineNodeKind
    topological_level: NonNegativeSafeInt
    upstream_node_keys: list[NodeKey]


class PipelineRunProgressV1(PipelineReadModel):
    total_stage_runs: NonNegativeSafeInt
    completed_stage_runs: NonNegativeSafeInt
    states: dict[PipelineStageRunState, NonNegativeSafeInt]
    domain_outcomes: dict[str, NonNegativeSafeInt]
    nodes: dict[str, PipelineNodeProgressV1]


class PipelineStageRunListQueryV1(PipelineModel):
    node_key: NodeKey | None = None
    state: PipelineStageRunState | None = None
    domain_outcome: (
        Annotated[str, StringConstraints(pattern=r"^[a-z][a-z0-9_.-]{0,127}$")] | None
    ) = None
    cursor: Annotated[str, StringConstraints(min_length=1, max_length=4096)] | None = None
    limit: Annotated[int, Field(strict=True, ge=1, le=500)] = 200


class PipelineStageRunListResponseV1(PipelineReadModel):
    items: Annotated[list[PipelineStageRunSummaryV1], Field(max_length=500)]
    next_cursor: str | None
    progress: PipelineRunProgressV1


class PipelineArtifactSummaryV1(PipelineReadModel):
    id: UUID
    name: str
    artifact_type: str
    content_sha256: str
    manifest_sha256: str | None
    stored_size_bytes: NonNegativeSafeInt | None
    file_count: NonNegativeSafeInt | None
    safety_state: str
    visibility: str
    share_status: str
    access_class: Literal["team_runtime", "authoring_restricted", "sanitized_audit"]
    download_path: str
    pipeline_run_id: UUID | None
    pipeline_stage_run_id: UUID | None
    execution_attempt_id: UUID | None
    producer_kind: str | None
    detail_path: str


class PipelineArtifactListQueryV1(PipelineModel):
    cursor: Annotated[str, StringConstraints(min_length=1, max_length=4096)] | None = None
    limit: Annotated[int, Field(strict=True, ge=1, le=200)] = 100


class PipelineArtifactListResponseV1(PipelineReadModel):
    items: Annotated[list[PipelineArtifactSummaryV1], Field(max_length=200)]
    next_cursor: str | None


class PipelineArtifactFileProjectionV1(PipelineReadModel):
    file_index: NonNegativeSafeInt
    relative_path: str
    role: str
    media_type: str
    size_bytes: NonNegativeSafeInt
    sha256: str
    download_path: str


class PipelineArtifactDetailV1(PipelineArtifactSummaryV1):
    created_at: datetime
    lineage_artifact_ids: list[UUID]
    lineage_digests: list[str]
    files: list[PipelineArtifactFileProjectionV1]


class PipelineBudgetCounterV1(PipelineReadModel):
    limit: NonNegativeSafeInt
    reserved: NonNegativeSafeInt
    settled: NonNegativeSafeInt
    remaining: int


class PipelineBudgetLedgerProjectionV1(PipelineReadModel):
    max_wall_seconds: PipelineBudgetCounterV1
    max_gpu_seconds: PipelineBudgetCounterV1
    max_provider_cost_usd: PipelineBudgetCounterV1
    max_artifact_bytes: PipelineBudgetCounterV1
    max_stage_runs: PipelineBudgetCounterV1
    max_attempts_total: PipelineBudgetCounterV1
    wall_deadline_at: datetime | None
    terminal_cause: str | None


class PipelineRunListItemV1(PipelineReadModel):
    id: UUID
    display_name: str | None
    recipe: PipelineRecipeIdentityProjectionV1
    state: PipelineRunState
    result: PipelineRunResult | None
    completed_stage_runs: NonNegativeSafeInt
    total_stage_runs: NonNegativeSafeInt
    domain_outcomes: dict[str, NonNegativeSafeInt]
    budget: PipelineBudgetLedgerProjectionV1 | None
    created_at: datetime
    finished_at: datetime | None


class PipelineRunListResponseV1(PipelineReadModel):
    items: list[PipelineRunListItemV1]
    next_cursor: str | None


class PipelineRunDetailV1(PipelineReadModel):
    id: UUID
    display_name: str | None
    recipe: PipelineRecipeIdentityProjectionV1
    graph_digest: str
    control_binding_snapshots_digest: str
    parameters_digest: str
    request_digest: str
    state: PipelineRunState
    result: PipelineRunResult | None
    reason: str | None
    created_by_user_id: UUID | None
    retry_of_pipeline_run_id: UUID | None
    retry_from_stage_run_id: UUID | None
    created_at: datetime
    started_at: datetime | None
    finished_at: datetime | None
    cancellation_requested_at: datetime | None
    source_budget: RunBudgetV1
    progress: PipelineRunProgressV1
    topology: Annotated[list[PipelineNodeTopologyV1], Field(max_length=128)]
    stages: Annotated[list[PipelineStageRunSummaryV1], Field(max_length=500)]
    stages_next_cursor: str | None
    artifacts: Annotated[list[PipelineArtifactSummaryV1], Field(max_length=200)]
    artifacts_next_cursor: str | None
    budget: PipelineBudgetLedgerProjectionV1 | None


class PipelineStageRunDetailV1(PipelineStageRunSummaryV1):
    pipeline_run_id: UUID
    execution_spec_digest: str | None
    input_bindings_digest: str | None
    resource_profile_digest: str | None
    request_renderer_digest: str | None
    latest_checkpoint_artifact_id: UUID | None
    live_preview_eligible: bool
    artifacts: list[PipelineArtifactSummaryV1]


class PipelineExecutionAttemptV1(PipelineReadModel):
    id: UUID
    attempt_number: PositiveSafeInt
    state: ExecutionAttemptState
    worker_id: UUID | None
    worker_pool_class: str | None
    queued_at: datetime | None
    claimed_at: datetime | None
    started_at: datetime | None
    finished_at: datetime | None
    exit_code: int | None
    retry_class: str | None
    reason_code: str | None
    stage_request_digest: str | None
    result_manifest_digest: str | None
    resumed_checkpoint_artifact_id: UUID | None
    cancellation_observed_at: datetime | None
    cancellation_outcome: str | None
    cleanup_acknowledged_at: datetime | None
    cleanup_proof_digest: str | None


class PipelineExecutionAttemptListV1(PipelineReadModel):
    items: list[PipelineExecutionAttemptV1]


class PipelineMutationResultV1(PipelineModel):
    pipeline_run_id: UUID
    request_digest: Digest
    idempotent_replay: bool


class PipelineCancelResultV1(PipelineModel):
    pipeline_run_id: UUID
    state: PipelineRunState
    result: PipelineRunResult | None
    cancellation_requested_at: datetime | None
    terminal_cause: str | None

    @field_validator("cancellation_requested_at")
    @classmethod
    def cancellation_time_is_aware(cls, value: datetime | None) -> datetime | None:
        if value is None:
            return None
        return _aware(value, label="cancellation_requested_at")


class ResolvedRecipeControlBindingV1(PipelineModel):
    logical_name: BindingName
    kind: Literal["judge_profile", "provider"]
    node_key: NodeKey
    object_id: UUID
    version: PositiveVersion
    snapshot_sha256: Digest
    provider_limits: ProviderAttemptLimitsV1


class ResolvedRecipeControlBindingsV1(PipelineModel):
    items: Annotated[list[ResolvedRecipeControlBindingV1], Field(max_length=128)]

    @model_validator(mode="after")
    def bindings_are_ordered_and_unique(self) -> ResolvedRecipeControlBindingsV1:
        logical_names = [item.logical_name for item in self.items]
        if logical_names != sorted(logical_names, key=lambda value: value.encode("utf-8")):
            raise ValueError("control bindings must be bytewise ordered")
        if len(logical_names) != len(set(logical_names)):
            raise ValueError("control binding logical names must be unique")
        node_keys = [item.node_key for item in self.items]
        if len(node_keys) != len(set(node_keys)):
            raise ValueError("control binding node keys must be unique")
        return self


class PipelineRecipeBindingResolver(Protocol):
    async def resolve(
        self,
        team_id: UUID,
        recipe_identity: RecipeIdentityV1,
        judge_profile_id: UUID | None,
        logical_slots: tuple[str, ...],
        *,
        session: AsyncSession | None = None,
    ) -> ResolvedRecipeControlBindingsV1: ...

    async def persist_run_bindings(
        self,
        session: AsyncSession,
        *,
        pipeline_run_id: UUID,
        items: ResolvedRecipeControlBindingsV1,
    ) -> None: ...


__all__ = [
    "IdempotencyKey",
    "PipelineArtifactListQueryV1",
    "PipelineArtifactListResponseV1",
    "PipelineCancelResultV1",
    "PipelineExecutionAttemptListV1",
    "PipelineExecutionAttemptV1",
    "PipelineIdempotencyEndpoint",
    "PipelineMutationResultV1",
    "PipelineNodeProgressV1",
    "PipelineNodeTopologyV1",
    "PipelineRecipeBindingResolver",
    "PipelineRunCancelRequestV1",
    "PipelineRunDetailV1",
    "PipelineRunEventV1",
    "PipelineRunEventsQueryV1",
    "PipelineRunEventsResponseV1",
    "PipelineRunListQueryV1",
    "PipelineRunListResponseV1",
    "PipelineRunProgressV1",
    "PipelineRunRetryRequestV1",
    "PipelineRunSubmitRequestV1",
    "PipelineStageRetryRequestV1",
    "PipelineStageRunDetailV1",
    "PipelineStageRunListQueryV1",
    "PipelineStageRunListResponseV1",
    "PipelineStageRunSummaryV1",
    "ResolvedRecipeControlBindingV1",
    "ResolvedRecipeControlBindingsV1",
    "canonical_pipeline_request_digest",
    "compute_pipeline_request_digest",
    "pipeline_request_digest",
    "validate_idempotency_key",
]
