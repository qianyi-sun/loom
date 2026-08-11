"""Closed Pydantic contracts for every BEHAVIOR Pipeline wire document.

The generic Pipeline owns canonical JSON, attempt budgets, resolved input
bindings, terminal descriptors, and StageResult.  This module intentionally
imports and reuses those authorities; it adds only BEHAVIOR-specific shape and
cross-field validation.
"""

from __future__ import annotations

import math
import re
import unicodedata
from collections import Counter
from enum import StrEnum
from pathlib import Path, PurePosixPath
from typing import Annotated, Literal, TypeAlias
from uuid import UUID

from pydantic import Field, StringConstraints, field_validator, model_validator

from loom.pipeline.keys import MAX_SAFE_INTEGER, canonical_digest
from loom.pipeline.spec import (
    ArtifactType,
    BindingItemV1,
    BindingName,
    BindingSetV1,
    CommittedOutputDescriptorV1,
    Digest,
    NodeKey,
    NonNegativeSafeInt,
    PipelineModel,
    PositiveVersion,
    StageBudgetV1,
    TerminalStageDescriptorV1,
    reject_secret_literals,
)
from loom.pipeline.state import RetryClass, StageResultV1

UINT16_MAX = 65_535
UINT32_MAX = 4_294_967_295
UINT64_MAX = MAX_SAFE_INTEGER
MAX_STAGE_REQUEST_BYTES = 16_777_216
MAX_ARTIFACT_DOCUMENT_BYTES = 67_108_864

UInt16 = Annotated[int, Field(strict=True, ge=0, le=UINT16_MAX)]
UInt32 = Annotated[int, Field(strict=True, ge=0, le=UINT32_MAX)]
PositiveUInt32 = Annotated[int, Field(strict=True, ge=1, le=UINT32_MAX)]
UInt64 = Annotated[int, Field(strict=True, ge=0, le=UINT64_MAX)]
PositiveUInt64 = Annotated[int, Field(strict=True, ge=1, le=UINT64_MAX)]
GitCommit = Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{40}$")]
ImageDigest = Annotated[
    str,
    StringConstraints(
        pattern=(
            r"^[a-z0-9]+(?:[._-][a-z0-9]+)*(?::[0-9]+)?"
            r"(?:/[a-z0-9]+(?:[._-][a-z0-9]+)*)+@sha256:[0-9a-f]{64}$"
        )
    ),
]
Sha256Hex = Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{64}$")]
SnakeReason = Annotated[str, StringConstraints(pattern=r"^[a-z][a-z0-9_]{0,127}$")]

# Explicit aliases document that BEHAVIOR has no divergent wire variants.
StageRequestBindingItemV1 = BindingItemV1
StageRequestBindingSetV1 = BindingSetV1
StageResultDocumentV1 = StageResultV1


def _nfc(value: str, *, label: str = "string", max_bytes: int | None = None) -> str:
    normalized = unicodedata.normalize("NFC", value)
    if normalized != value:
        raise ValueError(f"{label} must already be NFC")
    encoded = value.encode("utf-8", errors="strict")
    if not value or (max_bytes is not None and len(encoded) > max_bytes):
        raise ValueError(f"{label} is empty or exceeds its UTF-8 limit")
    return value


def _reject_non_nfc_strings(value: object) -> None:
    if isinstance(value, str):
        _nfc(value)
    elif isinstance(value, dict):
        for key, item in value.items():
            _nfc(str(key))
            _reject_non_nfc_strings(item)
    elif isinstance(value, list | tuple):
        for item in value:
            _reject_non_nfc_strings(item)


def _bytewise(values: list[str], label: str) -> list[str]:
    if values != sorted(values, key=lambda item: item.encode("utf-8")):
        raise ValueError(f"{label} must be bytewise sorted")
    if len(values) != len(set(values)):
        raise ValueError(f"{label} must be unique")
    return values


def _uuid_bytes(values: list[UUID], label: str) -> list[UUID]:
    if values != sorted(values, key=lambda value: value.bytes):
        raise ValueError(f"{label} must be sorted by canonical UUID bytes")
    if len(values) != len(set(values)):
        raise ValueError(f"{label} must be unique")
    return values


class BehaviorStage(StrEnum):
    INPUT_PREFLIGHT = "input_preflight"
    ROLLOUT = "rollout"
    OFFLINE_JUDGE = "offline_judge"
    FAILURE_MATERIALIZE = "failure_materialize"
    FRAME_AUTHOR = "frame_author"
    RECOVERY = "recovery"
    AGGREGATE = "aggregate"
    DATASET_BUILD = "dataset_build"


class EmptyParametersV1(PipelineModel):
    pass


class BehaviorRolloutParametersV1(PipelineModel):
    eval_instance_index: UInt32
    episode_index: UInt32
    seed: UInt32
    record_depth: Literal[False]
    recording_fps: Literal[30]


class OfflineJudgeParametersV1(PipelineModel):
    inspection_mode: Literal["whole_episode_predicate_log"]


class PrimitiveRecoveryParametersV1(PipelineModel):
    stream: Literal["primitive"]
    sample_id: UUID


class MopRecoveryParametersV1(PipelineModel):
    stream: Literal["mop"]
    sample_id: UUID


RecoveryParametersV1: TypeAlias = PrimitiveRecoveryParametersV1 | MopRecoveryParametersV1


class DatasetBuildParametersV1(PipelineModel):
    format: Literal["lerobot_v2.1"]


StageParametersV1: TypeAlias = (
    EmptyParametersV1
    | BehaviorRolloutParametersV1
    | OfflineJudgeParametersV1
    | PrimitiveRecoveryParametersV1
    | MopRecoveryParametersV1
    | DatasetBuildParametersV1
)


class JudgeProfileProvenanceV1(PipelineModel):
    logical_name: Literal["behavior_offline_judge"]
    kind: Literal["judge_profile"]
    node_key: Literal["offline_judge"]
    object_id: UUID
    version: PositiveVersion
    snapshot_sha256: Digest
    provider_asset_manifest_sha256: Digest
    judge_profile_id: UUID
    judge_profile_version: PositiveVersion
    agent: str
    agent_version: str
    provider: str
    model: str

    _normalize_agent = field_validator("agent")(_nfc)
    _normalize_agent_version = field_validator("agent_version")(_nfc)
    _normalize_provider = field_validator("provider")(_nfc)
    _normalize_model = field_validator("model")(_nfc)


class PrimitiveProviderProvenanceV1(PipelineModel):
    logical_name: Literal["behavior_recovery_primitive"]
    kind: Literal["provider"]
    node_key: Literal["recovery_primitive"]
    object_id: UUID
    version: PositiveVersion
    snapshot_sha256: Digest
    provider_asset_manifest_sha256: Digest


ControlBindingProvenanceV1: TypeAlias = JudgeProfileProvenanceV1 | PrimitiveProviderProvenanceV1


class StageRequestProvenanceV1(PipelineModel):
    recipe_digest: Digest
    resolved_input_bindings_digest: Digest
    execution_spec_digest: Digest
    image_digest: ImageDigest
    loom_commit_sha: GitCommit
    control_binding: ControlBindingProvenanceV1 | None
    compatibility_manifest_sha256: Digest


class TerminalStageSetV1(PipelineModel):
    schema_version: Literal["behavior.terminal-stage-set.v1"]
    pipeline_run_id: UUID
    run_graph_digest: Digest
    snapshot_id: UUID
    terminal_stage_keys: Annotated[list[NodeKey], Field(min_length=6, max_length=7)]
    stages: list[TerminalStageDescriptorV1]

    @model_validator(mode="after")
    def validate_terminal_set(self) -> TerminalStageSetV1:
        common = [
            "input_preflight",
            "rollout",
            "offline_judge",
            "failure_materialize",
            "frame_author",
        ]
        valid = (
            [*common, "recovery_mop"],
            [*common, "recovery_primitive"],
            [*common, "recovery_mop", "recovery_primitive"],
        )
        if self.terminal_stage_keys not in valid:
            raise ValueError("terminal_stage_keys is not a complete strategy expansion")
        order = {key: index for index, key in enumerate(self.terminal_stage_keys)}
        if any(stage.node_key not in order for stage in self.stages):
            raise ValueError("terminal set contains an undeclared or gate stage")
        identities = [(stage.node_key, stage.shard_key) for stage in self.stages]
        if len(identities) != len(set(identities)):
            raise ValueError("terminal stage identities must be unique")
        if len({stage.stage_run_id for stage in self.stages}) != len(self.stages):
            raise ValueError("terminal stage_run_id values must be unique")
        expected = sorted(
            self.stages,
            key=lambda stage: (order[stage.node_key], stage.shard_key.encode("utf-8")),
        )
        if self.stages != expected:
            raise ValueError("terminal stages must be in signed terminal-key/shard order")
        from loom.pipeline.keys import canonical_document

        if len(canonical_document(self)) > MAX_STAGE_REQUEST_BYTES:
            raise ValueError("terminal stage set exceeds 16 MiB")
        return self


_PARAMETER_MODELS: dict[BehaviorStage, type[PipelineModel]] = {
    BehaviorStage.INPUT_PREFLIGHT: EmptyParametersV1,
    BehaviorStage.ROLLOUT: BehaviorRolloutParametersV1,
    BehaviorStage.OFFLINE_JUDGE: OfflineJudgeParametersV1,
    BehaviorStage.FAILURE_MATERIALIZE: EmptyParametersV1,
    BehaviorStage.FRAME_AUTHOR: EmptyParametersV1,
    BehaviorStage.AGGREGATE: EmptyParametersV1,
    BehaviorStage.DATASET_BUILD: DatasetBuildParametersV1,
}


class StageRequestV1(PipelineModel):
    schema_version: Literal["behavior.stage-request.v1"]
    stage: BehaviorStage
    run_id: UUID
    stage_run_id: UUID
    attempt_id: UUID
    idempotency_key: Digest
    inputs: Annotated[list[StageRequestBindingSetV1], Field(max_length=128)]
    parameters: StageParametersV1
    budget: StageBudgetV1
    provenance: StageRequestProvenanceV1
    orchestration: TerminalStageSetV1 | None

    @model_validator(mode="before")
    @classmethod
    def parse_stage_parameters(cls, value: object) -> object:
        _reject_non_nfc_strings(value)
        if not isinstance(value, dict):
            return value
        raw_stage = value.get("stage")
        raw_parameters = value.get("parameters")
        if not isinstance(raw_stage, str):
            return value
        try:
            stage = BehaviorStage(raw_stage)
        except (TypeError, ValueError):
            return value
        parsed = dict(value)
        model: type[PipelineModel]
        if stage is BehaviorStage.RECOVERY:
            if not isinstance(raw_parameters, dict):
                return value
            model = (
                PrimitiveRecoveryParametersV1
                if raw_parameters.get("stream") == "primitive"
                else MopRecoveryParametersV1
            )
        else:
            model = _PARAMETER_MODELS[stage]
        from loom.pipeline.keys import canonical_document

        parsed["parameters"] = model.model_validate_json(canonical_document(raw_parameters))
        return parsed

    @model_validator(mode="after")
    def validate_request(self) -> StageRequestV1:
        binding_names = [binding.binding_name for binding in self.inputs]
        if len(binding_names) != len(set(binding_names)):
            raise ValueError("StageRequest binding_name values must be unique")
        if any(len(binding.items) > 5_000 for binding in self.inputs):
            raise ValueError("StageRequest many binding exceeds 5,000 items")
        if canonical_digest(self.inputs) != self.provenance.resolved_input_bindings_digest:
            raise ValueError("resolved_input_bindings_digest does not match ordered inputs")
        preimage = {
            "attempt_id": str(self.attempt_id),
            "execution_spec_digest": self.provenance.execution_spec_digest,
            "resolved_input_bindings_digest": self.provenance.resolved_input_bindings_digest,
            "stage_run_id": str(self.stage_run_id),
        }
        if canonical_digest(preimage, persisted=False) != self.idempotency_key:
            raise ValueError("idempotency_key does not match the Attempt-scoped preimage")
        if self.stage is BehaviorStage.AGGREGATE:
            if self.orchestration is None or self.orchestration.pipeline_run_id != self.run_id:
                raise ValueError("aggregate requires matching terminal orchestration")
        elif self.orchestration is not None:
            raise ValueError("only aggregate may receive orchestration")
        control = self.provenance.control_binding
        if self.stage is BehaviorStage.OFFLINE_JUDGE:
            if not isinstance(control, JudgeProfileProvenanceV1) or self.budget.provider is None:
                raise ValueError("offline_judge requires its judge profile and provider budget")
            limits = self.budget.provider
            if (
                limits.provider_request_limit_per_attempt > 256
                or limits.provider_cost_limit_microusd_per_attempt > 30_000_000
                or limits.per_call_timeout_seconds > 60
            ):
                raise ValueError("offline_judge Provider limits exceed the fixed profile caps")
        elif self.stage is BehaviorStage.RECOVERY and isinstance(
            self.parameters, PrimitiveRecoveryParametersV1
        ):
            if (
                not isinstance(control, PrimitiveProviderProvenanceV1)
                or self.budget.provider is None
            ):
                raise ValueError("primitive recovery requires its provider binding and budget")
            limits = self.budget.provider
            if (
                limits.provider_request_limit_per_attempt,
                limits.provider_cost_limit_microusd_per_attempt,
                limits.per_call_timeout_seconds,
            ) != (512, 30_000_000, 600):
                raise ValueError("primitive Provider limits must be exactly 512/30000000/600")
        elif control is not None or self.budget.provider is not None:
            raise ValueError("this stage must be Provider-null")
        is_mop = self.stage is BehaviorStage.RECOVERY and isinstance(
            self.parameters, MopRecoveryParametersV1
        )
        expected_checkpoint_bytes = 16_777_216 if is_mop else 0
        if self.budget.checkpoint_bytes_limit != expected_checkpoint_bytes:
            raise ValueError("checkpoint_bytes_limit does not match the stage checkpoint contract")
        reject_secret_literals(self)
        return self


def validate_stage_request(value: object) -> StageRequestV1:
    """Validate an already decoded StageRequest document."""

    from loom.pipeline.keys import canonical_document

    _reject_non_nfc_strings(value)
    return StageRequestV1.model_validate_json(canonical_document(value))


def validate_stage_result_document(value: object) -> StageResultV1:
    """Validate the one generic StageResult authority without a BEHAVIOR fork."""

    from loom.pipeline.keys import canonical_document

    _reject_non_nfc_strings(value)
    return StageResultV1.model_validate_json(canonical_document(value))


_STAGE_OUTCOMES: dict[BehaviorStage, frozenset[str]] = {
    BehaviorStage.INPUT_PREFLIGHT: frozenset({"compatible"}),
    BehaviorStage.ROLLOUT: frozenset({"rollout_success", "rollout_failure"}),
    BehaviorStage.OFFLINE_JUDGE: frozenset({"judged"}),
    BehaviorStage.FAILURE_MATERIALIZE: frozenset({"no_failure", "failure_cases_created"}),
    BehaviorStage.FRAME_AUTHOR: frozenset({"authored"}),
    BehaviorStage.RECOVERY: frozenset(
        {"recorded", "not_recorded", "inconclusive", "budget_exhausted"}
    ),
    BehaviorStage.AGGREGATE: frozenset({"recorded_recoveries", "no_recorded_recovery"}),
    BehaviorStage.DATASET_BUILD: frozenset({"snapshot_created"}),
}
_TRANSIENT_REASONS = {
    "provider_429": RetryClass.PROVIDER_TRANSIENT,
    "provider_5xx": RetryClass.PROVIDER_TRANSIENT,
    "gateway_transport": RetryClass.PROVIDER_TRANSIENT,
    "container_start_transient": RetryClass.INFRASTRUCTURE_TRANSIENT,
    "stage_helper_transient": RetryClass.INFRASTRUCTURE_TRANSIENT,
}
_EXIT_RETRY_CLASS = {
    20: RetryClass.CONTRACT_ERROR,
    21: RetryClass.PROVIDER_TRANSIENT,
    22: RetryClass.INFRASTRUCTURE_TRANSIENT,
    23: RetryClass.INTERNAL_DEFECT,
    130: RetryClass.CANCELLED,
    143: RetryClass.CANCELLED,
}


def validate_behavior_stage_result(
    value: object,
    *,
    stage: BehaviorStage,
    exit_code: int,
) -> StageResultV1:
    """Apply BEHAVIOR outcome and fixed process-exit semantics to StageResult v1."""

    result = validate_stage_result_document(value)
    if exit_code == 0:
        if result.domain_outcome not in _STAGE_OUTCOMES[stage]:
            raise ValueError("domain_outcome is not legal for the BEHAVIOR stage")
        if result.retry_class is not RetryClass.NONE or result.error is not None:
            raise ValueError("rc=0 requires retry_class=none and error=null")
        if (
            stage is BehaviorStage.RECOVERY
            and result.domain_outcome == "inconclusive"
            and result.reason_code != "primitive_collision_unknown"
        ):
            raise ValueError("primitive collision unknown requires its canonical reason code")
        return result
    expected = _EXIT_RETRY_CLASS.get(exit_code)
    if expected is None or result.retry_class is not expected or result.domain_outcome is not None:
        raise ValueError("StageResult retry class does not match process exit")
    if result.retry_class in {RetryClass.PROVIDER_TRANSIENT, RetryClass.INFRASTRUCTURE_TRANSIENT}:
        if _TRANSIENT_REASONS.get(result.reason_code) is not result.retry_class:
            raise ValueError("transient StageResult uses a non-canonical retry reason")
    elif result.reason_code in _TRANSIENT_REASONS:
        raise ValueError("non-retryable StageResult cannot use a transient reason")
    return result


class ProviderAssetFileV1(PipelineModel):
    relative_path: str
    sha256: Digest
    size_bytes: NonNegativeSafeInt

    @field_validator("relative_path")
    @classmethod
    def validate_path(cls, value: str) -> str:
        return _safe_relative_path(value, prefix=None)


class ProviderAssetManifestV1(PipelineModel):
    schema_version: Literal["behavior.provider-assets.v1"]
    logical_name: Literal["behavior_offline_judge", "behavior_recovery_primitive"]
    files: Annotated[list[ProviderAssetFileV1], Field(min_length=1)]

    @model_validator(mode="after")
    def validate_inventory(self) -> ProviderAssetManifestV1:
        paths = _bytewise([item.relative_path for item in self.files], "provider asset paths")
        expected = {
            "behavior_offline_judge": [
                "inspect_rollout.md",
                "looking.md",
                "mcp-lock.json",
                "runner-lock.json",
                "seed.schema.json",
                "skill_vocabulary.md",
                "system.md",
                "tools/mosaic.py",
                "validate_outputs.py",
            ],
            "behavior_recovery_primitive": [
                "codegen.md",
                "mcp-lock.json",
                "planner_skill.md",
                "reviewer.md",
                "runner-lock.json",
                "verify_phase1.md",
                "verify_phase2.md",
            ],
        }
        if paths != expected[self.logical_name]:
            raise ValueError("provider asset inventory is not the fixed slot file set")
        return self


class PrimitiveRunnerLockV1(PipelineModel):
    schema_version: Literal["behavior.primitive-runner-lock.v1"]
    planner_dispatch_limit: Literal[4]
    codegen_dispatch_limit: Literal[8]
    phase2_dispatch_limit: Literal[432]
    phase1_dispatch_limit: Literal[68]
    total_provider_dispatch_limit: Literal[512]

    @model_validator(mode="after")
    def validate_total(self) -> PrimitiveRunnerLockV1:
        if (
            self.planner_dispatch_limit
            + self.codegen_dispatch_limit
            + self.phase2_dispatch_limit
            + self.phase1_dispatch_limit
            != self.total_provider_dispatch_limit
        ):
            raise ValueError("primitive dispatch limits do not sum to 512")
        return self


def _safe_relative_path(value: str, *, prefix: str | None = "payload") -> str:
    value = _nfc(value, label="relative path", max_bytes=4096)
    if "\\" in value or "\x00" in value or value.startswith("/"):
        raise ValueError("path must be a relative POSIX path")
    path = PurePosixPath(value)
    if any(part in {"", ".", ".."} for part in path.parts):
        raise ValueError("path contains an invalid component")
    if prefix is not None and (not path.parts or path.parts[0] != prefix):
        raise ValueError(f"path must be below {prefix}/")
    return value


class ArtifactFileV1(PipelineModel):
    name: BindingName
    relative_path: str
    sha256: Digest
    size_bytes: NonNegativeSafeInt
    media_type: str
    required: bool

    @field_validator("relative_path")
    @classmethod
    def validate_relative_path(cls, value: str) -> str:
        return _safe_relative_path(value)

    @field_validator("media_type")
    @classmethod
    def validate_media_type(cls, value: str) -> str:
        value = _nfc(value, label="media_type", max_bytes=256)
        if not re.fullmatch(r"[a-z0-9][a-z0-9!#$&^_.+-]*/[a-z0-9][a-z0-9!#$&^_.+-]*", value):
            raise ValueError("media_type is not a lowercase MIME type")
        return value


class ArtifactRefV1(PipelineModel):
    artifact_id: UUID
    artifact_type: ArtifactType
    manifest_sha256: Digest


class ContentArtifactRefV1(ArtifactRefV1):
    content_sha256: Digest


class MaterializedFanoutRefV1(PipelineModel):
    artifact_id: UUID
    artifact_type: Literal["loom.fanout-manifest.v1"]
    content_sha256: Digest


class PipelineArtifactProvenanceV1(PipelineModel):
    producer_kind: Literal["pipeline"]
    loom_commit_sha: GitCommit
    pipeline_run_id: UUID
    stage_run_id: UUID
    execution_attempt_id: UUID
    recipe_digest: Digest
    execution_spec_digest: Digest
    image_digest: ImageDigest
    compatibility_manifest_sha256: Digest
    control_binding: ControlBindingProvenanceV1 | None
    source_artifacts: list[ArtifactRefV1]

    @field_validator("source_artifacts")
    @classmethod
    def source_refs_sorted(cls, values: list[ArtifactRefV1]) -> list[ArtifactRefV1]:
        if values != sorted(values, key=lambda item: item.artifact_id.bytes):
            raise ValueError("source_artifacts must be sorted by Artifact UUID")
        if len({item.artifact_id for item in values}) != len(values):
            raise ValueError("source_artifacts must be unique")
        return values


class ControlArtifactProvenanceV1(PipelineModel):
    producer_kind: Literal["control"]
    loom_commit_sha: GitCommit
    control_event_id: UUID
    actor_id: UUID
    recipe_digest: Digest
    source_artifacts: list[ArtifactRefV1]

    @field_validator("source_artifacts")
    @classmethod
    def source_refs_sorted(cls, values: list[ArtifactRefV1]) -> list[ArtifactRefV1]:
        if values != sorted(values, key=lambda item: item.artifact_id.bytes):
            raise ValueError("source_artifacts must be sorted by Artifact UUID")
        if len({item.artifact_id for item in values}) != len(values):
            raise ValueError("source_artifacts must be unique")
        return values


ArtifactProvenanceV1: TypeAlias = PipelineArtifactProvenanceV1 | ControlArtifactProvenanceV1


class InputSummaryV1(PipelineModel):
    binding_name: BindingName
    artifact_id: UUID
    artifact_type: ArtifactType
    content_sha256: Digest
    manifest_sha256: Digest
    unpacked_size_bytes: NonNegativeSafeInt
    file_count: NonNegativeSafeInt


class TasksetPreflightSummaryV1(PipelineModel):
    task_instance_count: Annotated[int, Field(strict=True, ge=1, le=200)]
    ordered_child_payloads_sha256: Digest
    fanout_artifact_id: UUID
    fanout_content_sha256: Digest
    fanout_manifest_sha256: Digest
    ordered_fanout_items_sha256: Digest
    max_failure_cases_per_task: Literal[4]
    max_failure_cases_total: Literal[800]


class RuntimeRootSummaryV1(PipelineModel):
    name: Literal["behavior-1k-assets", "2025-challenge-task-instances"]
    relative_path: str

    @field_validator("relative_path")
    @classmethod
    def validate_path(cls, value: str) -> str:
        return _safe_relative_path(value, prefix=None)


class DatasetPreflightSummaryV1(PipelineModel):
    artifact_type: Literal["behavior_dataset_snapshot.v1"]
    omnigibson_version: Literal["3.8"]
    dataset_schema_version: Literal["behavior_dataset_snapshot.v1"]
    omnigibson_dataset_layout_version: Literal["behavior_1k_runtime_v1"]
    omnigibson_dataset_root: Literal["payload/omnigibson"]
    runtime_roots: Annotated[list[RuntimeRootSummaryV1], Field(min_length=2, max_length=2)]
    task_universe_sha256: Digest
    task_count: Annotated[int, Field(strict=True, ge=1, le=1000)]
    agentic_task_cards_sha256: Digest
    agentic_demo_video_sets_sha256: Digest
    test_instance_sets_sha256: Digest

    @model_validator(mode="after")
    def validate_roots(self) -> DatasetPreflightSummaryV1:
        expected = [
            ("behavior-1k-assets", "omnigibson/behavior-1k-assets"),
            ("2025-challenge-task-instances", "omnigibson/2025-challenge-task-instances"),
        ]
        if [(root.name, root.relative_path) for root in self.runtime_roots] != expected:
            raise ValueError("dataset runtime_roots do not match the fixed ABI")
        return self


class PolicyPreflightSummaryV1(PipelineModel):
    artifact_type: Literal["behavior_policy_checkpoint.v1"]
    architecture: Literal["pi_behavior_b1k_fast"]
    action_dim: Literal[23]
    state_dim: Literal[23]
    robot_action_dim: Literal[25]
    checkpoint_format: Literal["openpi_checkpoint_directory_v1"]
    checkpoint_root: Literal["payload/checkpoint"]
    model_identifier: str
    vla_interface_version: Literal["behavior_b1k_websocket_v1"]
    controller_adapter_version: Literal["r1pro_25_to_pi23_v1"]

    _normalize_model = field_validator("model_identifier")(_nfc)


MOP_REQUIRED_COLUMNS = [
    "meta",
    "episode_id",
    "step",
    "kind",
    "object",
    "category",
    "manip_object",
    "corrected_end_step",
    "stage_frac",
    "joint_positions",
    "base_rel",
    "standoff_left",
    "standoff_right",
    "eef_rel_pos_left",
    "eef_rel_quat_left",
    "eef_rel_pos_right",
    "eef_rel_quat_right",
]


class MopBankPreflightSummaryV1(PipelineModel):
    artifact_type: Literal["behavior_mop_bank.v1"]
    bank_schema_version: Literal["training_bank_v2_pickle_free"]
    bank_path_template: Literal["banks/task-<NNNN>/task_<decimal>_training_bank.npz"]
    column_contract_version: Literal["behavior_mop_bank_npz_v1"]
    pose_dim: Literal[28]
    action_dim: Literal[23]
    task_universe_sha256: Digest
    source_revision: str
    sampling_mode: Literal["event_and_temporal"]
    required_columns: Annotated[list[str], Field(min_length=17, max_length=17)]
    row_count: PositiveUInt64
    bank_files_sha256: Digest
    task_coverage_sha256: Digest

    _normalize_revision = field_validator("source_revision")(_nfc)

    @field_validator("required_columns")
    @classmethod
    def validate_columns(cls, values: list[str]) -> list[str]:
        if values != MOP_REQUIRED_COLUMNS:
            raise ValueError("required_columns does not match the fixed MOP ABI")
        return values


class ArtifactTotalsV1(PipelineModel):
    stored_size_bytes: UInt64
    unpacked_size_bytes: UInt64
    file_count: UInt64


class BehaviorInputPreflightPayloadV1(PipelineModel):
    inputs: Annotated[list[InputSummaryV1], Field(min_length=5, max_length=5)]
    compatibility_manifest_sha256: Digest
    taskset: TasksetPreflightSummaryV1
    dataset: DatasetPreflightSummaryV1
    policy: PolicyPreflightSummaryV1
    mop_bank: MopBankPreflightSummaryV1
    totals: ArtifactTotalsV1

    @field_validator("inputs")
    @classmethod
    def inputs_sorted(cls, values: list[InputSummaryV1]) -> list[InputSummaryV1]:
        _bytewise([item.binding_name for item in values], "preflight inputs")
        return values


BEHAVIOR_ARTIFACT_TYPES = frozenset(
    {
        "behavior_taskset_snapshot.v1",
        "behavior_task_instance.v1",
        "behavior_dataset_snapshot.v1",
        "behavior_policy_checkpoint.v1",
        "behavior_mop_bank.v1",
        "behavior_rollout_bundle.v1",
        "behavior_inspection_report.v1",
        "behavior_failure_case.v1",
        "behavior_restore_bundle.v1",
        "behavior_recovery_attempt.v1",
        "behavior_recovery_episode.v1",
        "behavior_input_preflight.v1",
        "behavior_recovery_index.v1",
        "behavior_training_dataset.v1",
    }
)


class BehaviorInputPreflightArtifactV1(PipelineModel):
    schema_version: Literal["behavior_input_preflight.v1"]
    payload: BehaviorInputPreflightPayloadV1
    files: list[ArtifactFileV1]
    provenance: PipelineArtifactProvenanceV1

    @model_validator(mode="after")
    def validate_files(self) -> BehaviorInputPreflightArtifactV1:
        if self.files:
            raise ValueError("behavior_input_preflight.v1 is JSON-only")
        return self


class SourceProvenanceV1(PipelineModel):
    type: str
    locator: str
    revision: str

    @field_validator("type", "locator")
    @classmethod
    def normalize_identity(cls, value: str) -> str:
        return _nfc(value, label="source identity", max_bytes=256)

    @field_validator("revision")
    @classmethod
    def normalize_revision(cls, value: str) -> str:
        return _nfc(value, label="source revision", max_bytes=512)


class RuntimeTreeFileV1(PipelineModel):
    relative_path: str
    sha256: Digest
    size_bytes: UInt64

    @field_validator("relative_path")
    @classmethod
    def validate_path(cls, value: str) -> str:
        return _safe_relative_path(value, prefix=None)


class BehaviorAssetsRuntimeRootV1(PipelineModel):
    name: Literal["behavior-1k-assets"]
    relative_path: Literal["omnigibson/behavior-1k-assets"]
    revision: str
    tree_sha256: Digest

    _normalize_revision = field_validator("revision")(_nfc)


class ChallengeInstancesRuntimeRootV1(PipelineModel):
    name: Literal["2025-challenge-task-instances"]
    relative_path: Literal["omnigibson/2025-challenge-task-instances"]
    revision: str
    tree_sha256: Digest
    episodes_jsonl_sha256: Digest
    test_instances_csv_sha256: Digest
    scenes_tree_sha256: Digest

    _normalize_revision = field_validator("revision")(_nfc)


class AgenticTaskCardV1(PipelineModel):
    behavior_task_id: Annotated[int, Field(strict=True, ge=0, le=9999)]
    task_name: str
    relative_path: str
    sha256: Digest
    size_bytes: Annotated[int, Field(strict=True, ge=1, le=1_048_576)]

    _normalize_task = field_validator("task_name")(_nfc)

    @model_validator(mode="after")
    def validate_path(self) -> AgenticTaskCardV1:
        expected = f"agentic_sweep/task_cards/task-{self.behavior_task_id:04d}.md"
        if self.relative_path != expected:
            raise ValueError("task-card path does not match behavior_task_id")
        return self


class DemoVideoFileV1(PipelineModel):
    camera: Literal["head", "left_wrist", "right_wrist"]
    relative_path: str
    sha256: Digest
    size_bytes: Annotated[int, Field(strict=True, ge=1, le=17_179_869_184)]
    frame_count: Annotated[int, Field(strict=True, ge=1, le=10_000_000)]


class DemoVideoEpisodeV1(PipelineModel):
    episode_id: Annotated[str, StringConstraints(pattern=r"^episode_[0-9]{8}$")]
    files: Annotated[list[DemoVideoFileV1], Field(min_length=3, max_length=3)]

    @model_validator(mode="after")
    def validate_files(self) -> DemoVideoEpisodeV1:
        if [item.camera for item in self.files] != ["head", "left_wrist", "right_wrist"]:
            raise ValueError("demo episode requires the fixed camera order")
        if len({item.frame_count for item in self.files}) != 1:
            raise ValueError("demo camera frame counts must agree")
        return self


class AgenticDemoVideoSetV1(PipelineModel):
    behavior_task_id: Annotated[int, Field(strict=True, ge=0, le=9999)]
    episodes: Annotated[list[DemoVideoEpisodeV1], Field(min_length=1, max_length=1000)]

    @field_validator("episodes")
    @classmethod
    def episodes_sorted(cls, values: list[DemoVideoEpisodeV1]) -> list[DemoVideoEpisodeV1]:
        _bytewise([item.episode_id for item in values], "demo episodes")
        return values


class TestInstanceSetV1(PipelineModel):
    behavior_task_id: Annotated[int, Field(strict=True, ge=0, le=9999)]
    task_name: str
    engine_task_instance_ids: Annotated[
        list[Annotated[int, Field(strict=True, ge=0, le=999)]], Field(min_length=1, max_length=1000)
    ]

    _normalize_task = field_validator("task_name")(_nfc)

    @field_validator("engine_task_instance_ids")
    @classmethod
    def engine_ids_unique(cls, values: list[int]) -> list[int]:
        if len(values) != len(set(values)):
            raise ValueError("engine_task_instance_ids must be unique in source order")
        return values


class DatasetCompatibilityV1(PipelineModel):
    kind: Literal["dataset"]
    omnigibson_version: Literal["3.8"]
    dataset_schema_version: Literal["behavior_dataset_snapshot.v1"]
    omnigibson_dataset_layout_version: Literal["behavior_1k_runtime_v1"]
    omnigibson_dataset_root: Literal["payload/omnigibson"]
    runtime_roots: Annotated[
        list[BehaviorAssetsRuntimeRootV1 | ChallengeInstancesRuntimeRootV1],
        Field(min_length=2, max_length=2),
    ]
    task_universe_sha256: Digest
    task_count: Annotated[int, Field(strict=True, ge=1, le=1000)]
    agentic_task_cards: list[AgenticTaskCardV1]
    agentic_task_cards_sha256: Digest
    agentic_demo_video_sets: list[AgenticDemoVideoSetV1]
    agentic_demo_video_sets_sha256: Digest
    test_instance_sets: list[TestInstanceSetV1]
    test_instance_sets_sha256: Digest

    @model_validator(mode="after")
    def validate_universe(self) -> DatasetCompatibilityV1:
        if not isinstance(self.runtime_roots[0], BehaviorAssetsRuntimeRootV1) or not isinstance(
            self.runtime_roots[1], ChallengeInstancesRuntimeRootV1
        ):
            raise ValueError("dataset runtime roots are not in fixed ABI order")
        ids = [item.behavior_task_id for item in self.agentic_task_cards]
        demo_ids = [item.behavior_task_id for item in self.agentic_demo_video_sets]
        instance_ids = [item.behavior_task_id for item in self.test_instance_sets]
        if ids != sorted(ids) or ids != demo_ids or ids != instance_ids:
            raise ValueError("dataset semantic indexes must share numeric task order")
        if len(ids) != self.task_count or len(ids) != len(set(ids)):
            raise ValueError("dataset task_count or task identity is inconsistent")
        if canonical_digest(ids, persisted=False) != self.task_universe_sha256:
            raise ValueError("dataset task_universe_sha256 does not match task IDs")
        if (
            canonical_digest(self.agentic_task_cards, persisted=False)
            != self.agentic_task_cards_sha256
        ):
            raise ValueError("agentic_task_cards_sha256 drift")
        if (
            canonical_digest(self.agentic_demo_video_sets, persisted=False)
            != self.agentic_demo_video_sets_sha256
        ):
            raise ValueError("agentic_demo_video_sets_sha256 drift")
        if (
            canonical_digest(self.test_instance_sets, persisted=False)
            != self.test_instance_sets_sha256
        ):
            raise ValueError("test_instance_sets_sha256 drift")
        return self


class PolicyCompatibilityV1(PipelineModel):
    kind: Literal["policy"]
    architecture: Literal["pi_behavior_b1k_fast"]
    action_dim: Literal[23]
    state_dim: Literal[23]
    robot_action_dim: Literal[25]
    checkpoint_format: Literal["openpi_checkpoint_directory_v1"]
    checkpoint_root: Literal["payload/checkpoint"]
    checkpoint_tree_sha256: Digest
    model_identifier: str
    vla_interface_version: Literal["behavior_b1k_websocket_v1"]
    controller_adapter_version: Literal["r1pro_25_to_pi23_v1"]

    @field_validator("model_identifier")
    @classmethod
    def normalize_model(cls, value: str) -> str:
        return _nfc(value, label="model_identifier", max_bytes=256)


class MopBankFileV1(PipelineModel):
    behavior_task_id: Annotated[int, Field(strict=True, ge=0, le=9999)]
    relative_path: str
    sha256: Digest
    size_bytes: PositiveUInt64
    row_count: Annotated[int, Field(strict=True, ge=1, le=10_000_000)]

    @model_validator(mode="after")
    def validate_path(self) -> MopBankFileV1:
        expected = (
            f"banks/task-{self.behavior_task_id:04d}/task_{self.behavior_task_id}_training_bank.npz"
        )
        if self.relative_path != expected:
            raise ValueError("MOP bank path does not match behavior_task_id")
        return self


class TaskCoverageV1(PipelineModel):
    behavior_task_id: Annotated[int, Field(strict=True, ge=0, le=9999)]
    annotated_subtask_count: PositiveUInt64
    event_row_count: PositiveUInt64
    temporal_row_count: PositiveUInt64


class MopBankCompatibilityV1(PipelineModel):
    kind: Literal["mop_bank"]
    bank_schema_version: Literal["training_bank_v2_pickle_free"]
    column_contract_version: Literal["behavior_mop_bank_npz_v1"]
    pose_dim: Literal[28]
    action_dim: Literal[23]
    task_universe_sha256: Digest
    source_revision: str
    sampling_mode: Literal["event_and_temporal"]
    required_columns: Annotated[list[str], Field(min_length=17, max_length=17)]
    row_count: PositiveUInt64
    bank_files: list[MopBankFileV1]
    bank_files_sha256: Digest
    task_coverage: list[TaskCoverageV1]
    task_coverage_sha256: Digest

    @field_validator("source_revision")
    @classmethod
    def normalize_revision(cls, value: str) -> str:
        return _nfc(value, label="source_revision", max_bytes=512)

    @field_validator("required_columns")
    @classmethod
    def validate_columns(cls, values: list[str]) -> list[str]:
        if values != MOP_REQUIRED_COLUMNS:
            raise ValueError("MOP columns do not match the fixed ABI")
        return values

    @model_validator(mode="after")
    def validate_bank(self) -> MopBankCompatibilityV1:
        file_ids = [item.behavior_task_id for item in self.bank_files]
        coverage_ids = [item.behavior_task_id for item in self.task_coverage]
        if (
            file_ids != sorted(file_ids)
            or file_ids != coverage_ids
            or len(file_ids) != len(set(file_ids))
        ):
            raise ValueError("MOP bank and coverage universes must share numeric order")
        if sum(item.row_count for item in self.bank_files) != self.row_count:
            raise ValueError("MOP row_count does not equal bank-file rows")
        if canonical_digest(file_ids, persisted=False) != self.task_universe_sha256:
            raise ValueError("MOP task_universe_sha256 drift")
        if canonical_digest(self.bank_files, persisted=False) != self.bank_files_sha256:
            raise ValueError("bank_files_sha256 drift")
        if canonical_digest(self.task_coverage, persisted=False) != self.task_coverage_sha256:
            raise ValueError("task_coverage_sha256 drift")
        return self


class ImportedInputPayloadV1(PipelineModel):
    name: str
    version: str
    source_provenance: SourceProvenanceV1
    compatibility: DatasetCompatibilityV1 | PolicyCompatibilityV1 | MopBankCompatibilityV1

    @field_validator("name", "version")
    @classmethod
    def normalize_name_version(cls, value: str) -> str:
        return _nfc(value, label="input name/version", max_bytes=256)


class BehaviorDatasetSnapshotArtifactV1(PipelineModel):
    schema_version: Literal["behavior_dataset_snapshot.v1"]
    payload: ImportedInputPayloadV1
    files: Annotated[list[ArtifactFileV1], Field(max_length=100_000)]
    provenance: ControlArtifactProvenanceV1

    @model_validator(mode="after")
    def validate_kind(self) -> BehaviorDatasetSnapshotArtifactV1:
        if not isinstance(self.payload.compatibility, DatasetCompatibilityV1):
            raise ValueError("dataset Artifact requires dataset compatibility")
        _bytewise([item.relative_path for item in self.files], "dataset files")
        return self


class BehaviorPolicyCheckpointArtifactV1(PipelineModel):
    schema_version: Literal["behavior_policy_checkpoint.v1"]
    payload: ImportedInputPayloadV1
    files: Annotated[list[ArtifactFileV1], Field(min_length=1, max_length=10_000)]
    provenance: ControlArtifactProvenanceV1

    @model_validator(mode="after")
    def validate_kind(self) -> BehaviorPolicyCheckpointArtifactV1:
        if not isinstance(self.payload.compatibility, PolicyCompatibilityV1):
            raise ValueError("policy Artifact requires policy compatibility")
        _bytewise([item.relative_path for item in self.files], "policy files")
        return self


class BehaviorMopBankArtifactV1(PipelineModel):
    schema_version: Literal["behavior_mop_bank.v1"]
    payload: ImportedInputPayloadV1
    files: Annotated[list[ArtifactFileV1], Field(min_length=1, max_length=10_000)]
    provenance: ControlArtifactProvenanceV1

    @model_validator(mode="after")
    def validate_kind(self) -> BehaviorMopBankArtifactV1:
        if not isinstance(self.payload.compatibility, MopBankCompatibilityV1):
            raise ValueError("MOP Artifact requires mop_bank compatibility")
        _bytewise([item.relative_path for item in self.files], "MOP files")
        return self


class SourceTaskSetRefV1(PipelineModel):
    task_set_id: str
    owning_team_id: UUID
    manifest_generation: UInt64
    manifest_sha256: Digest
    intents: list[Literal["trajectory_generation", "evaluation"]]
    evaluation_ready: Literal[True]

    @field_validator("task_set_id")
    @classmethod
    def normalize_taskset(cls, value: str) -> str:
        return _nfc(value, label="task_set_id", max_bytes=256)

    @field_validator("intents")
    @classmethod
    def validate_intents(cls, values: list[str]) -> list[str]:
        _bytewise(values, "TaskSet intents")
        if not values or "trajectory_generation" not in values:
            raise ValueError("TaskSet intents require trajectory_generation")
        return values


class ArtifactTaskBundleRefV1(PipelineModel):
    kind: Literal["artifact"]
    artifact_id: UUID
    artifact_type: ArtifactType
    manifest_sha256: Digest
    content_sha256: Digest
    size_bytes: PositiveUInt64


class ObjectTaskBundleRefV1(PipelineModel):
    kind: Literal["object"]
    object_sha256: Digest
    size_bytes: PositiveUInt64


TaskBundleRefV1: TypeAlias = ArtifactTaskBundleRefV1 | ObjectTaskBundleRefV1


class RecipeRefV1(PipelineModel):
    name: Literal["behavior-recovery"]
    version: Literal[1]
    digest: Digest


class MaterializationRefV1(PipelineModel):
    episodes_per_instance: Annotated[int, Field(strict=True, ge=1, le=10)]
    seed_base: UInt32
    request_sha256: Digest


class TaskInstanceLineageV1(PipelineModel):
    source_task_set_manifest_sha256: Digest
    task_bundle: TaskBundleRefV1
    materialization_request_sha256: Digest
    dataset_content_sha256: Digest
    policy_content_sha256: Digest
    mop_bank_content_sha256: Digest


class BehaviorTaskInstancePayloadV1(PipelineModel):
    source_task_set: SourceTaskSetRefV1
    loom_task_id: str
    behavior_task_id: Annotated[int, Field(strict=True, ge=0, le=9999)]
    task_name: str
    semantic_task_id: str
    task_checksum: Digest
    task_bundle_digest: Digest
    task_bundle: TaskBundleRefV1
    source_bddl_path: str
    eval_instance_index: UInt16
    engine_task_instance_id: Annotated[int, Field(strict=True, ge=0, le=999)]
    episode_index: Annotated[int, Field(strict=True, ge=0, le=9)]
    demo_id: UInt32
    demo_stem: Annotated[str, StringConstraints(pattern=r"^episode_[0-9]{8}$")]
    seed: UInt32
    task_instance_identity: Sha256Hex
    materialization: MaterializationRefV1
    recipe: RecipeRefV1
    lineage: TaskInstanceLineageV1

    @field_validator("loom_task_id", "task_name", "semantic_task_id")
    @classmethod
    def normalize_identity(cls, value: str) -> str:
        return _nfc(value, label="task identity", max_bytes=256)

    @field_validator("source_bddl_path")
    @classmethod
    def validate_bddl_path(cls, value: str) -> str:
        return _safe_relative_path(value, prefix=None)

    @model_validator(mode="after")
    def validate_identity(self) -> BehaviorTaskInstancePayloadV1:
        bundle_digest = (
            self.task_bundle.content_sha256
            if isinstance(self.task_bundle, ArtifactTaskBundleRefV1)
            else self.task_bundle.object_sha256
        )
        if bundle_digest != self.task_bundle_digest:
            raise ValueError("task_bundle_digest does not match TaskBundle branch")
        expected_demo = (
            self.behavior_task_id * 10_000 + self.engine_task_instance_id * 10 + self.episode_index
        )
        if self.demo_id != expected_demo or self.demo_stem != f"episode_{expected_demo:08d}":
            raise ValueError("demo_id/demo_stem does not match engine identity")
        seed_preimage = {
            "engine_task_instance_id": self.engine_task_instance_id,
            "episode_index": self.episode_index,
            "eval_instance_index": self.eval_instance_index,
            "seed_base": self.materialization.seed_base,
            "task_checksum": self.task_checksum,
        }
        import hashlib

        expected_seed = int.from_bytes(hashlib.sha256(_raw_jcs(seed_preimage)).digest()[:4], "big")
        if self.seed != expected_seed:
            raise ValueError("seed does not match the signed task tuple")
        identity_preimage = {
            "behavior_task_id": self.behavior_task_id,
            "demo_id": self.demo_id,
            "engine_task_instance_id": self.engine_task_instance_id,
            "episode_index": self.episode_index,
            "eval_instance_index": self.eval_instance_index,
            "recipe_digest": self.recipe.digest,
            "seed": self.seed,
            "task_bundle_digest": self.task_bundle_digest,
        }
        if (
            canonical_digest(identity_preimage, persisted=False).removeprefix("sha256:")
            != self.task_instance_identity
        ):
            raise ValueError("task_instance_identity drift")
        return self


def _raw_jcs(value: object) -> bytes:
    from loom.pipeline.keys import canonical_identity

    return canonical_identity(value)


class TaskSnapshotRowV1(PipelineModel):
    loom_task_id: str
    behavior_task_id: Annotated[int, Field(strict=True, ge=0, le=9999)]
    task_name: str
    semantic_task_id: str
    task_checksum: Digest
    source_bddl_path: str
    eligible_eval_instance_ids: Annotated[list[UInt16], Field(min_length=1)]
    engine_task_instance_ids: Annotated[
        list[Annotated[int, Field(strict=True, ge=0, le=999)]], Field(min_length=1)
    ]
    task_bundle: TaskBundleRefV1

    @field_validator("loom_task_id", "task_name", "semantic_task_id")
    @classmethod
    def normalize_identity(cls, value: str) -> str:
        return _nfc(value, label="task identity", max_bytes=256)

    @field_validator("source_bddl_path")
    @classmethod
    def validate_bddl_path(cls, value: str) -> str:
        return _safe_relative_path(value, prefix=None)

    @model_validator(mode="after")
    def validate_selectors(self) -> TaskSnapshotRowV1:
        if self.eligible_eval_instance_ids != sorted(set(self.eligible_eval_instance_ids)):
            raise ValueError("eligible selectors must be numeric-sorted and unique")
        if len(self.engine_task_instance_ids) != len(set(self.engine_task_instance_ids)):
            raise ValueError("engine task IDs must be unique in signed source order")
        if any(
            index >= len(self.engine_task_instance_ids) for index in self.eligible_eval_instance_ids
        ):
            raise ValueError("eligible selector is outside engine ID array")
        return self


class CompanionInputsV1(PipelineModel):
    dataset: ContentArtifactRefV1
    policy: ContentArtifactRefV1
    mop_bank: ContentArtifactRefV1

    @model_validator(mode="after")
    def validate_types(self) -> CompanionInputsV1:
        expected = {
            "dataset": "behavior_dataset_snapshot.v1",
            "policy": "behavior_policy_checkpoint.v1",
            "mop_bank": "behavior_mop_bank.v1",
        }
        for name, artifact_type in expected.items():
            if getattr(self, name).artifact_type != artifact_type:
                raise ValueError(f"{name} companion has the wrong Artifact type")
        return self


class EmbeddedTaskInstanceV1(PipelineModel):
    artifact_id: UUID
    payload_sha256: Digest
    payload: BehaviorTaskInstancePayloadV1

    @model_validator(mode="after")
    def validate_payload_digest(self) -> EmbeddedTaskInstanceV1:
        if canonical_digest(self.payload) != self.payload_sha256:
            raise ValueError("embedded task payload digest drift")
        return self


class DownstreamLimitsV1(PipelineModel):
    max_failure_cases_per_task: Literal[4]
    max_failure_cases_total: Literal[800]


class BehaviorTasksetSnapshotPayloadV1(PipelineModel):
    source_task_set: SourceTaskSetRefV1
    tasks: Annotated[list[TaskSnapshotRowV1], Field(min_length=1, max_length=200)]
    companion_inputs: CompanionInputsV1
    materialization: MaterializationRefV1
    task_instances: Annotated[list[EmbeddedTaskInstanceV1], Field(min_length=1, max_length=200)]
    task_instances_fanout: MaterializedFanoutRefV1
    downstream_limits: DownstreamLimitsV1
    recipe: RecipeRefV1
    loom_commit: GitCommit
    created_by: UUID

    @model_validator(mode="after")
    def validate_order(self) -> BehaviorTasksetSnapshotPayloadV1:
        _bytewise([item.loom_task_id for item in self.tasks], "TaskSet task rows")
        if len({item.behavior_task_id for item in self.tasks}) != len(self.tasks):
            raise ValueError("TaskSet behavior_task_id values must be unique")
        identities = [item.payload.task_instance_identity for item in self.task_instances]
        _bytewise(identities, "embedded task instances")
        if len({item.artifact_id for item in self.task_instances}) != len(self.task_instances):
            raise ValueError("embedded child Artifact IDs must be unique")
        return self


class BehaviorTasksetSnapshotArtifactV1(PipelineModel):
    schema_version: Literal["behavior_taskset_snapshot.v1"]
    payload: BehaviorTasksetSnapshotPayloadV1
    files: list[ArtifactFileV1]
    provenance: ControlArtifactProvenanceV1

    @model_validator(mode="after")
    def json_only(self) -> BehaviorTasksetSnapshotArtifactV1:
        if self.files:
            raise ValueError("TaskSet snapshot is JSON-only")
        return self


class BehaviorTaskInstanceArtifactV1(PipelineModel):
    schema_version: Literal["behavior_task_instance.v1"]
    payload: BehaviorTaskInstancePayloadV1
    files: list[ArtifactFileV1]
    provenance: ControlArtifactProvenanceV1

    @model_validator(mode="after")
    def json_only(self) -> BehaviorTaskInstanceArtifactV1:
        if self.files:
            raise ValueError("TaskInstance is JSON-only")
        return self


def _validate_artifact_files(files: list[ArtifactFileV1], *, label: str) -> list[ArtifactFileV1]:
    paths = [item.relative_path for item in files]
    _bytewise(paths, f"{label} file paths")
    names = [item.name for item in files]
    if len(names) != len(set(names)):
        raise ValueError(f"{label} file names must be unique")
    return files


class RolloutPolicyIdentityV1(PipelineModel):
    architecture: Literal["pi_behavior_b1k_fast"]
    checkpoint_format: Literal["openpi_checkpoint_directory_v1"]
    checkpoint_root: Literal["payload/checkpoint"]
    checkpoint_tree_sha256: Digest
    vla_interface_version: Literal["behavior_b1k_websocket_v1"]
    controller_adapter_version: Literal["r1pro_25_to_pi23_v1"]
    model_identifier: str

    _normalize_model = field_validator("model_identifier")(_nfc)


class RolloutRuntimeV1(PipelineModel):
    loom_commit_sha: GitCommit
    image_digest: ImageDigest
    compatibility_manifest_sha256: Digest


class RolloutDataFileDescriptorV1(PipelineModel):
    role: Literal["rollout_hdf5", "bddl_transitions", "scene_metadata", "predicate_catalog"]
    relative_path: str
    sha256: Digest
    size_bytes: PositiveUInt64
    media_type: str

    _validate_path = field_validator("relative_path")(_safe_relative_path)
    _validate_media = field_validator("media_type")(_nfc)


class RolloutVideoFileDescriptorV1(PipelineModel):
    role: Literal["rgb_head", "rgb_left_wrist", "rgb_right_wrist", "rgb_composite"]
    relative_path: str
    sha256: Digest
    size_bytes: PositiveUInt64
    media_type: Literal["video/mp4"]
    frame_count: PositiveUInt64
    fps: Literal[30]
    codec: Literal["h264"]
    pixel_format: Literal["yuv420p"]
    width: PositiveUInt32
    height: PositiveUInt32

    _validate_path = field_validator("relative_path")(_safe_relative_path)

    @model_validator(mode="after")
    def validate_dimensions(self) -> RolloutVideoFileDescriptorV1:
        if self.role == "rgb_composite" and (self.width, self.height) != (672, 448):
            raise ValueError("rgb_composite must be 672x448")
        return self


RolloutFileDescriptorV1: TypeAlias = RolloutDataFileDescriptorV1 | RolloutVideoFileDescriptorV1


class BehaviorRolloutBundlePayloadV1(PipelineModel):
    task_instance_identity: Sha256Hex
    loom_task_id: str
    behavior_task_id: Annotated[int, Field(strict=True, ge=0, le=9999)]
    task_name: str
    task_checksum: Digest
    task_bundle_digest: Digest
    source_bddl_path: str
    eval_instance_index: Annotated[int, Field(strict=True, ge=0, le=999)]
    engine_task_instance_id: Annotated[int, Field(strict=True, ge=0, le=999)]
    episode_index: Annotated[int, Field(strict=True, ge=0, le=9)]
    demo_id: UInt32
    demo_stem: Annotated[str, StringConstraints(pattern=r"^episode_[0-9]{8}$")]
    seed: UInt32
    domain_outcome: Literal["rollout_success", "rollout_failure"]
    success: bool
    step_count: PositiveUInt64
    recording_fps: Literal[30]
    dataset: ContentArtifactRefV1
    policy: ContentArtifactRefV1
    policy_identity: RolloutPolicyIdentityV1
    runtime: RolloutRuntimeV1
    required_file_descriptors: Annotated[
        list[RolloutFileDescriptorV1], Field(min_length=7, max_length=7)
    ]
    optional_audit_files: Annotated[list[RolloutDataFileDescriptorV1], Field(max_length=1)]

    @field_validator("loom_task_id", "task_name")
    @classmethod
    def normalize_identity(cls, value: str) -> str:
        return _nfc(value, label="rollout identity", max_bytes=256)

    @field_validator("source_bddl_path")
    @classmethod
    def validate_bddl_path(cls, value: str) -> str:
        return _safe_relative_path(value, prefix=None)

    @model_validator(mode="after")
    def validate_rollout(self) -> BehaviorRolloutBundlePayloadV1:
        if self.success != (self.domain_outcome == "rollout_success"):
            raise ValueError("rollout success flag and domain_outcome disagree")
        expected_demo = (
            self.behavior_task_id * 10_000 + self.engine_task_instance_id * 10 + self.episode_index
        )
        if self.demo_id != expected_demo or self.demo_stem != f"episode_{expected_demo:08d}":
            raise ValueError("rollout demo identity drift")
        if (
            self.dataset.artifact_type != "behavior_dataset_snapshot.v1"
            or self.policy.artifact_type != "behavior_policy_checkpoint.v1"
        ):
            raise ValueError("rollout dataset/policy Artifact types drift")
        expected_roles = [
            "rollout_hdf5",
            "bddl_transitions",
            "scene_metadata",
            "rgb_head",
            "rgb_left_wrist",
            "rgb_right_wrist",
            "rgb_composite",
        ]
        if [item.role for item in self.required_file_descriptors] != expected_roles:
            raise ValueError("rollout required descriptors are not in fixed role order")
        if self.optional_audit_files and self.optional_audit_files[0].role != "predicate_catalog":
            raise ValueError("predicate_catalog is the only optional audit descriptor")
        task_tag = f"task-{self.behavior_task_id:04d}"
        expected_paths = [
            f"payload/trajectories/{task_tag}/{self.demo_stem}.hdf5",
            f"payload/meta/episodes/{task_tag}/{self.demo_stem}_bddl_transitions.json",
            f"payload/meta/episodes/{task_tag}/{self.demo_stem}_scene.json",
            f"payload/videos/{task_tag}/observation.images.rgb.head/{self.demo_stem}.mp4",
            (
                f"payload/videos/{task_tag}/observation.images.rgb.left_wrist/"
                f"{self.demo_stem}.mp4"
            ),
            (
                f"payload/videos/{task_tag}/observation.images.rgb.right_wrist/"
                f"{self.demo_stem}.mp4"
            ),
            "payload/rgb_composite.mp4",
        ]
        if [item.relative_path for item in self.required_file_descriptors] != expected_paths:
            raise ValueError("rollout required descriptor paths drift from the packed identity")
        expected_media = [
            "application/x-hdf5",
            "application/json",
            "application/json",
            "video/mp4",
            "video/mp4",
            "video/mp4",
            "video/mp4",
        ]
        if [item.media_type for item in self.required_file_descriptors] != expected_media:
            raise ValueError("rollout required descriptor media types drift")
        if self.optional_audit_files:
            expected_optional = (
                f"payload/meta/episodes/{task_tag}/{self.demo_stem}_predicate_catalog.json"
            )
            if (
                self.optional_audit_files[0].relative_path != expected_optional
                or self.optional_audit_files[0].media_type != "application/json"
            ):
                raise ValueError("predicate_catalog path or media type drift")
        return self


class BehaviorRolloutBundleArtifactV1(PipelineModel):
    schema_version: Literal["behavior_rollout_bundle.v1"]
    payload: BehaviorRolloutBundlePayloadV1
    files: Annotated[list[ArtifactFileV1], Field(min_length=7, max_length=8)]
    provenance: PipelineArtifactProvenanceV1

    @model_validator(mode="after")
    def validate_inventory(self) -> BehaviorRolloutBundleArtifactV1:
        _validate_artifact_files(self.files, label="rollout")
        descriptors = [*self.payload.required_file_descriptors, *self.payload.optional_audit_files]
        if len({item.relative_path for item in descriptors}) != len(descriptors):
            raise ValueError("rollout descriptor paths must be unique")
        by_path = {item.relative_path: item for item in self.files}
        if set(by_path) != {item.relative_path for item in descriptors}:
            raise ValueError("rollout descriptors and outer file inventory disagree")
        for descriptor in descriptors:
            outer = by_path[descriptor.relative_path]
            if (outer.sha256, outer.size_bytes, outer.media_type) != (
                descriptor.sha256,
                descriptor.size_bytes,
                descriptor.media_type,
            ):
                raise ValueError("rollout descriptor metadata disagrees with outer inventory")
            expected_required = descriptor.role != "predicate_catalog"
            if outer.required is not expected_required or outer.name != descriptor.role:
                raise ValueError("rollout outer file role/required authority drift")
        return self


class ProviderUsageV1(PipelineModel):
    request_count: UInt64
    input_tokens: UInt64
    cache_read_tokens: UInt64
    output_tokens: UInt64
    cost_microusd: UInt64


class BehaviorInspectionReportPayloadV1(PipelineModel):
    task_instance_identity: Sha256Hex
    task_id: Annotated[int, Field(strict=True, ge=0, le=9999)]
    eval_instance_index: UInt32
    engine_task_instance_id: Annotated[int, Field(strict=True, ge=0, le=999)]
    episode_index: UInt32
    demo_id: UInt32
    episode: UInt32
    n_steps: PositiveUInt64
    rollout_artifact_id: UUID
    task_card_sha256: Digest
    prompt_sha256: Digest
    report_sha256: Digest
    seed_sha256: Digest
    judge_profile_id: UUID
    judge_profile_version: PositiveVersion
    judge_profile_sha256: Digest
    agent: str
    agent_version: str
    provider: str
    model: str
    control_binding_sha256: Digest
    mcp_server_locks_sha256: Digest
    usage: ProviderUsageV1
    seed_count: UInt64
    learn_count: UInt64

    @field_validator("agent", "agent_version", "provider", "model")
    @classmethod
    def normalize_provider_identity(cls, value: str) -> str:
        return _nfc(value, label="provider identity", max_bytes=512)

    @model_validator(mode="after")
    def validate_episode(self) -> BehaviorInspectionReportPayloadV1:
        if self.episode != self.demo_id:
            raise ValueError("inspection episode must equal demo_id")
        return self


class BehaviorInspectionReportArtifactV1(PipelineModel):
    schema_version: Literal["behavior_inspection_report.v1"]
    payload: BehaviorInspectionReportPayloadV1
    files: Annotated[list[ArtifactFileV1], Field(min_length=2, max_length=2)]
    provenance: PipelineArtifactProvenanceV1

    @model_validator(mode="after")
    def validate_inventory(self) -> BehaviorInspectionReportArtifactV1:
        _validate_artifact_files(self.files, label="inspection")
        paths = [item.relative_path for item in self.files]
        if paths != ["payload/report.md", "payload/seed.json"]:
            raise ValueError("inspection output requires only report.md and seed.json")
        if {item.sha256 for item in self.files} != {
            self.payload.report_sha256,
            self.payload.seed_sha256,
        }:
            raise ValueError("inspection payload hashes do not match file inventory")
        return self


class SeedChunkV1(PipelineModel):
    span: Annotated[list[UInt64], Field(min_length=2, max_length=2)]
    learn: None
    seed: UInt64
    reason: str
    skill_label: str
    object: str
    target: str
    arm: Literal["left", "right", "either"]

    @field_validator("reason", "skill_label", "object", "target")
    @classmethod
    def normalize_text(cls, value: str) -> str:
        return _nfc(value, label="seed chunk text", max_bytes=4096)

    @model_validator(mode="after")
    def validate_span(self) -> SeedChunkV1:
        if self.span[0] > self.seed or self.seed > self.span[1]:
            raise ValueError("seed must lie inside its inclusive span")
        return self


class CurrentFailureCaseV1(PipelineModel):
    rollout_id: UUID
    output_dir: Literal[""]
    task_name: str
    task_id: Annotated[int, Field(strict=True, ge=0, le=9999)]
    instance_id: Annotated[int, Field(strict=True, ge=0, le=999)]
    episode_id: UInt32
    subtask_idx: UInt32
    skill_description: str
    skill_type: str
    instruction: str
    failure_step: UInt64
    failure_t_seconds: float
    clip_start_step: UInt64
    clip_end_step: UInt64
    clip_end_t_seconds: float
    composite_clip_mp4_rel: None
    composite_clip_mp4_abs: None
    clip_hdf5: None
    clip_parquet: None
    clip_meta: None
    seed_hdf5_path: str
    seed_hdf5_step: UInt64
    last_success_hdf5_path: str | None
    last_success_hdf5_step: UInt64 | None
    last_succeeded_subtask_idx: UInt32 | None
    evidence: list[str]
    video_evidence: list[str]
    failure_reason_tag: Literal["execution", "ordering", "no_progress"]
    reference_segments: list[str]
    failure_videos_abs: dict[str, str]
    failure_videos_rel: dict[str, str]

    @field_validator("task_name", "skill_description", "skill_type", "instruction")
    @classmethod
    def normalize_text(cls, value: str) -> str:
        return _nfc(value, label="failure text", max_bytes=4096)

    @field_validator("seed_hdf5_path")
    @classmethod
    def validate_seed_path(cls, value: str) -> str:
        return _safe_relative_path(value)

    @model_validator(mode="after")
    def validate_empty_legacy_fields(self) -> CurrentFailureCaseV1:
        if (
            self.video_evidence
            or self.reference_segments
            or self.failure_videos_abs
            or self.failure_videos_rel
        ):
            raise ValueError("legacy video/reference fields must be empty")
        if not math.isclose(self.failure_t_seconds, self.failure_step / 30, rel_tol=0, abs_tol=0):
            raise ValueError("failure time must be step/30")
        if self.clip_end_step != self.failure_step or not math.isclose(
            self.clip_end_t_seconds, self.clip_end_step / 30, rel_tol=0, abs_tol=0
        ):
            raise ValueError("clip/failure step projection drift")
        if (self.last_success_hdf5_path is None) != (self.last_success_hdf5_step is None) or (
            self.last_success_hdf5_path is None
        ) != (self.last_succeeded_subtask_idx is None):
            raise ValueError("last-success fields must be all-null or all-present")
        return self


class BehaviorFailureCasePayloadV1(PipelineModel):
    case_id: UUID
    loom_task_id: str
    behavior_task_id: Annotated[int, Field(strict=True, ge=0, le=9999)]
    task_instance_identity: Sha256Hex
    eval_instance_index: UInt32
    engine_task_instance_id: Annotated[int, Field(strict=True, ge=0, le=999)]
    episode_index: UInt32
    demo_id: UInt32
    inspection_artifact: ContentArtifactRefV1
    rollout_artifact: ContentArtifactRefV1
    seed_chunk: SeedChunkV1
    seed_chunk_sha256: Digest
    scene_object: str
    scene_target: str
    failure_observed_step: UInt64
    recovery_seed_step: UInt64
    current_failure_case: CurrentFailureCaseV1

    @field_validator("loom_task_id", "scene_object", "scene_target")
    @classmethod
    def normalize_text(cls, value: str) -> str:
        return _nfc(value, label="failure identity", max_bytes=512)

    @model_validator(mode="after")
    def validate_case(self) -> BehaviorFailureCasePayloadV1:
        if canonical_digest(self.seed_chunk, persisted=False) != self.seed_chunk_sha256:
            raise ValueError("seed_chunk_sha256 drift")
        if (
            self.failure_observed_step != self.seed_chunk.span[1]
            or self.recovery_seed_step != self.seed_chunk.seed
        ):
            raise ValueError("FailureCase step projection drift")
        if self.current_failure_case.seed_hdf5_step != self.recovery_seed_step:
            raise ValueError("current FailureCase seed step drift")
        from loom.pipeline.keys import canonical_uuid5

        namespace = UUID("2cd41ee7-8f92-5f0a-9ec7-703013231224")
        expected = canonical_uuid5(
            namespace,
            {
                "inspection_content_sha256": self.inspection_artifact.content_sha256,
                "rollout_content_sha256": self.rollout_artifact.content_sha256,
                "subtask_index": self.current_failure_case.subtask_idx,
            },
        )
        if self.case_id != expected:
            raise ValueError("FailureCase UUIDv5 drift")
        return self


class BehaviorFailureCaseArtifactV1(PipelineModel):
    schema_version: Literal["behavior_failure_case.v1"]
    payload: BehaviorFailureCasePayloadV1
    files: list[ArtifactFileV1]
    provenance: PipelineArtifactProvenanceV1

    @model_validator(mode="after")
    def json_only(self) -> BehaviorFailureCaseArtifactV1:
        if self.files:
            raise ValueError("FailureCase is JSON-only")
        return self


class IdentityResolutionV1(PipelineModel):
    judged_object: str
    judged_target: str
    resolved_object: str
    resolved_destination: str | None
    arm: Literal["left", "right"]
    resolved_by: Literal["inst_to_name identity lookup"]

    @field_validator("judged_object", "judged_target", "resolved_object", "resolved_destination")
    @classmethod
    def normalize_names(cls, value: str | None) -> str | None:
        return None if value is None else _nfc(value, label="scene identity", max_bytes=512)


class BehaviorRestoreBundlePayloadV1(PipelineModel):
    failure_case_id: UUID
    recovery_seed_step: UInt64
    task_instance_identity: Sha256Hex
    task_id: Annotated[int, Field(strict=True, ge=0, le=9999)]
    task_name: str
    eval_instance_index: UInt32
    engine_task_instance_id: Annotated[int, Field(strict=True, ge=0, le=999)]
    episode_index: UInt32
    demo_id: UInt32
    episode_id: UInt32
    rollout_artifact_id: UUID
    inspection_artifact_id: UUID
    restore_state_sha256: Digest
    recovery_target_sha256: Digest
    collision_policy_sha256: Digest
    recovery_eligible: Literal[True]
    identity_resolution: IdentityResolutionV1

    _normalize_task = field_validator("task_name")(_nfc)

    @model_validator(mode="after")
    def validate_episode(self) -> BehaviorRestoreBundlePayloadV1:
        if self.episode_id != self.demo_id:
            raise ValueError("restore episode_id must equal demo_id")
        return self


class BehaviorRestoreBundleArtifactV1(PipelineModel):
    schema_version: Literal["behavior_restore_bundle.v1"]
    payload: BehaviorRestoreBundlePayloadV1
    files: Annotated[list[ArtifactFileV1], Field(min_length=3, max_length=3)]
    provenance: PipelineArtifactProvenanceV1

    @model_validator(mode="after")
    def validate_inventory(self) -> BehaviorRestoreBundleArtifactV1:
        _validate_artifact_files(self.files, label="restore")
        expected = [
            "payload/collision_policy.json",
            "payload/recovery_target.json",
            "payload/restore_state.json",
        ]
        if [item.relative_path for item in self.files] != expected:
            raise ValueError("restore bundle requires exactly three canonical JSON files")
        expected_hashes = {
            "payload/collision_policy.json": self.payload.collision_policy_sha256,
            "payload/recovery_target.json": self.payload.recovery_target_sha256,
            "payload/restore_state.json": self.payload.restore_state_sha256,
        }
        if any(item.sha256 != expected_hashes[item.relative_path] for item in self.files):
            raise ValueError("restore bundle file digest drift")
        return self


class BehaviorRecoveryAttemptPayloadV1(PipelineModel):
    recovery_attempt_id: UUID
    sample_id: UUID
    stream: Literal["primitive", "mop"]
    loom_task_id: str
    behavior_task_id: UInt32
    failure_case: ArtifactRefV1
    restore_bundle: ArtifactRefV1
    domain_outcome: Literal["recorded", "not_recorded", "inconclusive", "budget_exhausted"]
    reason_code: SnakeReason
    n_candidates: UInt64
    n_success: UInt64
    provider_cost_microusd: UInt64
    q_score_delta: float | None
    episodes: list[UUID]
    checkpoint_sequences: list[UInt64]

    @field_validator("loom_task_id")
    @classmethod
    def normalize_task(cls, value: str) -> str:
        return _nfc(value, label="loom_task_id", max_bytes=256)

    @field_validator("episodes")
    @classmethod
    def episodes_sorted(cls, values: list[UUID]) -> list[UUID]:
        return _uuid_bytes(values, "recovery episode IDs")

    @field_validator("checkpoint_sequences")
    @classmethod
    def checkpoint_sequences_sorted(cls, values: list[int]) -> list[int]:
        if values != sorted(values) or len(values) != len(set(values)):
            raise ValueError("checkpoint sequences must be sorted and unique")
        return values

    @model_validator(mode="after")
    def validate_outcome(self) -> BehaviorRecoveryAttemptPayloadV1:
        if self.n_success > self.n_candidates:
            raise ValueError("n_success cannot exceed n_candidates")
        if self.domain_outcome == "recorded" and self.n_success == 0:
            raise ValueError("recorded recovery requires at least one success")
        if self.stream == "primitive":
            if self.q_score_delta is not None:
                raise ValueError("primitive q_score_delta must be null")
        elif self.domain_outcome == "recorded" and (
            self.q_score_delta is None
            or not math.isfinite(self.q_score_delta)
            or self.q_score_delta <= 0
        ):
            raise ValueError("recorded MOP recovery requires positive finite q_score_delta")
        if (
            self.domain_outcome == "inconclusive"
            and self.reason_code != "primitive_collision_unknown"
        ):
            raise ValueError("collision-unknown uses the one canonical reason code")
        return self


class BehaviorRecoveryAttemptArtifactV1(PipelineModel):
    schema_version: Literal["behavior_recovery_attempt.v1"]
    payload: BehaviorRecoveryAttemptPayloadV1
    files: list[ArtifactFileV1]
    provenance: PipelineArtifactProvenanceV1

    @model_validator(mode="after")
    def json_only(self) -> BehaviorRecoveryAttemptArtifactV1:
        if self.files:
            raise ValueError("recovery attempt is JSON-only")
        return self


class VerifierResultV1(PipelineModel):
    phase1_verdict: Literal["success"]
    phase1_confidence: Literal["high", "medium", "low"]
    phase1_evidence_sha256: Digest
    phase2_status: Literal["skipped_phase1_success"]
    phase2_evidence_sha256: None
    collision_audit_scope: Literal["full_episode", "mop_prefix_only"]
    collision_audit_outcome: Literal["pass"]


class EpisodeFileV1(PipelineModel):
    role: Literal["hdf5", "video", "contact_trace", "handover", "metadata"]
    file_name: str
    sha256: Digest
    size_bytes: PositiveUInt64
    media_type: str

    @field_validator("file_name")
    @classmethod
    def validate_file_name(cls, value: str) -> str:
        value = _nfc(value, label="episode file name", max_bytes=256)
        if "/" in value or "\\" in value or value in {".", ".."}:
            raise ValueError("episode file_name must be a basename")
        return value

    _normalize_media = field_validator("media_type")(_nfc)


class BehaviorRecoveryEpisodePayloadV1(PipelineModel):
    recovery_episode_id: UUID
    stream: Literal["primitive", "mop"]
    loom_task_id: str
    behavior_task_id: UInt32
    task_name: str
    eval_instance_index: UInt32
    source_episode_index: UInt32
    source_demo_id: UInt32
    failure_case: ArtifactRefV1
    restore_bundle: ArtifactRefV1
    recovery_attempt_id: UUID
    candidate_id: str
    sample_id: UUID
    seed: UInt32
    verifier_policy: Literal["phase1_authoritative_v1"]
    verifier_result: VerifierResultV1
    q_score_delta: float | None
    frame_count: PositiveUInt64
    episode_files: list[EpisodeFileV1]

    @field_validator("loom_task_id", "task_name", "candidate_id")
    @classmethod
    def normalize_identity(cls, value: str) -> str:
        return _nfc(value, label="recovery identity", max_bytes=256)

    @model_validator(mode="after")
    def validate_stream(self) -> BehaviorRecoveryEpisodePayloadV1:
        roles = [item.role for item in self.episode_files]
        if (
            roles.count("hdf5") != 1
            or roles.count("video") != 1
            or roles.count("contact_trace") != 1
        ):
            raise ValueError("recovery episode requires one HDF5, video, and contact trace")
        conditional: Literal["metadata", "handover"] = (
            "metadata" if self.stream == "primitive" else "handover"
        )
        forbidden: Literal["handover", "metadata"] = (
            "handover" if self.stream == "primitive" else "metadata"
        )
        if roles.count(conditional) != 1 or forbidden in roles or len(roles) != 4:
            raise ValueError("recovery episode stream-conditional files drift")
        expected_scope = "full_episode" if self.stream == "primitive" else "mop_prefix_only"
        if self.verifier_result.collision_audit_scope != expected_scope:
            raise ValueError("verifier collision scope does not match recovery stream")
        if self.stream == "primitive":
            if self.q_score_delta is not None or self.candidate_id != "primitive:0":
                raise ValueError("primitive episode identity or q_score_delta drift")
        elif (
            self.q_score_delta is None
            or not math.isfinite(self.q_score_delta)
            or self.q_score_delta <= 0
        ):
            raise ValueError("MOP episode requires positive finite q_score_delta")
        from loom.pipeline.keys import canonical_uuid5

        expected_id = canonical_uuid5(
            UUID("d2b673d4-57c1-5a9f-b6f5-703013231226"),
            {
                "candidate_id": self.candidate_id,
                "recovery_attempt_id": str(self.recovery_attempt_id),
            },
        )
        if self.recovery_episode_id != expected_id:
            raise ValueError("recovery_episode_id UUIDv5 drift")
        return self


class BehaviorRecoveryEpisodeArtifactV1(PipelineModel):
    schema_version: Literal["behavior_recovery_episode.v1"]
    payload: BehaviorRecoveryEpisodePayloadV1
    files: Annotated[list[ArtifactFileV1], Field(min_length=4, max_length=4)]
    provenance: PipelineArtifactProvenanceV1

    @model_validator(mode="after")
    def validate_inventory(self) -> BehaviorRecoveryEpisodeArtifactV1:
        _validate_artifact_files(self.files, label="recovery episode")
        descriptors = {item.file_name: item for item in self.payload.episode_files}
        by_name = {PurePosixPath(item.relative_path).name: item for item in self.files}
        if set(descriptors) != set(by_name):
            raise ValueError("recovery episode descriptors and files disagree")
        for name, descriptor in descriptors.items():
            outer = by_name[name]
            if (descriptor.sha256, descriptor.size_bytes, descriptor.media_type) != (
                outer.sha256,
                outer.size_bytes,
                outer.media_type,
            ):
                raise ValueError("recovery episode file metadata drift")
        return self


class ExecutedV1(PipelineModel):
    node_key: NodeKey
    shard_key: str
    stage_run_id: UUID
    execution_attempt_id: UUID
    stage_result_sha256: Digest
    stage: BehaviorStage
    domain_outcome: str
    reason_code: SnakeReason
    committed_outputs: list[CommittedOutputDescriptorV1]

    @field_validator("domain_outcome")
    @classmethod
    def normalize_outcome(cls, value: str) -> str:
        return _nfc(value, label="domain_outcome", max_bytes=128)


class FailedV1(PipelineModel):
    node_key: NodeKey
    shard_key: str
    stage_run_id: UUID
    execution_attempt_id: UUID | None
    reason_code: SnakeReason
    stage_result_sha256: Digest | None


class SkippedV1(PipelineModel):
    node_key: NodeKey
    shard_key: str
    stage_run_id: UUID
    reason_code: SnakeReason


class RecordedAttemptV1(PipelineModel):
    recovery_attempt_id: UUID
    artifact: ArtifactRefV1


class RecordedEpisodeV1(PipelineModel):
    recovery_episode_id: UUID
    recovery_attempt_id: UUID
    artifact: ArtifactRefV1


class ReasonCountV1(PipelineModel):
    reason_code: SnakeReason
    count: UInt32


class OutcomeCountV1(PipelineModel):
    domain_outcome: str
    count: UInt32

    @field_validator("domain_outcome")
    @classmethod
    def normalize_outcome(cls, value: str) -> str:
        return _nfc(value, label="domain_outcome", max_bytes=128)


class RecoveryIndexCountsV1(PipelineModel):
    terminal_total: UInt32
    executed_total: UInt32
    failed_total: UInt32
    skipped_total: UInt32
    recorded_attempt_count: UInt32
    recorded_episode_count: UInt32
    domain_outcomes: list[OutcomeCountV1]

    @field_validator("domain_outcomes")
    @classmethod
    def outcomes_sorted(cls, values: list[OutcomeCountV1]) -> list[OutcomeCountV1]:
        _bytewise([item.domain_outcome for item in values], "domain outcome counts")
        return values


class BehaviorRecoveryIndexPayloadV1(PipelineModel):
    outcome: Literal["recorded_recoveries", "no_recorded_recovery"]
    strategy: Literal["mop_only", "primitive_only", "mop_then_primitive"]
    terminal_stage_keys: list[NodeKey]
    executed: list[ExecutedV1]
    failed: list[FailedV1]
    skipped: list[SkippedV1]
    recorded_attempts: list[RecordedAttemptV1]
    recorded_episodes: list[RecordedEpisodeV1]
    reason_histogram: list[ReasonCountV1]
    counts: RecoveryIndexCountsV1

    @model_validator(mode="after")
    def validate_index(self) -> BehaviorRecoveryIndexPayloadV1:
        common = [
            "input_preflight",
            "rollout",
            "offline_judge",
            "failure_materialize",
            "frame_author",
        ]
        recovery = {
            "mop_only": ["recovery_mop"],
            "primitive_only": ["recovery_primitive"],
            "mop_then_primitive": ["recovery_mop", "recovery_primitive"],
        }
        if self.terminal_stage_keys != common + recovery[self.strategy]:
            raise ValueError("recovery index terminal_stage_keys drift")
        order = {key: index for index, key in enumerate(self.terminal_stage_keys)}
        def validate_terminal_rows(
            rows: list[ExecutedV1] | list[FailedV1] | list[SkippedV1],
        ) -> None:
            if any(item.node_key not in order for item in rows):
                raise ValueError("recovery index contains an undeclared terminal node")
            expected_rows = sorted(
                rows,
                key=lambda item: (order[item.node_key], item.shard_key.encode("utf-8")),
            )
            if rows != expected_rows:
                raise ValueError("recovery terminal arrays must preserve snapshot order")

        validate_terminal_rows(self.executed)
        validate_terminal_rows(self.failed)
        validate_terminal_rows(self.skipped)
        identities = (
            [(item.node_key, item.shard_key) for item in self.executed]
            + [(item.node_key, item.shard_key) for item in self.failed]
            + [(item.node_key, item.shard_key) for item in self.skipped]
        )
        stage_run_ids = (
            [item.stage_run_id for item in self.executed]
            + [item.stage_run_id for item in self.failed]
            + [item.stage_run_id for item in self.skipped]
        )
        if len(identities) != len(set(identities)) or len(stage_run_ids) != len(
            set(stage_run_ids)
        ):
            raise ValueError("recovery terminal identities must be unique across arrays")
        attempt_ids = [item.recovery_attempt_id for item in self.recorded_attempts]
        episode_ids = [item.recovery_episode_id for item in self.recorded_episodes]
        _uuid_bytes(attempt_ids, "recorded attempts")
        _uuid_bytes(episode_ids, "recorded episodes")
        attempt_artifacts = [item.artifact.artifact_id for item in self.recorded_attempts]
        episode_artifacts = [item.artifact.artifact_id for item in self.recorded_episodes]
        if len(attempt_artifacts) != len(set(attempt_artifacts)) or len(episode_artifacts) != len(
            set(episode_artifacts)
        ):
            raise ValueError("recorded Artifact IDs must be unique")
        if any(item.recovery_attempt_id not in set(attempt_ids) for item in self.recorded_episodes):
            raise ValueError("recorded episode has no recorded attempt")
        if any(
            item.artifact.artifact_type != "behavior_recovery_attempt.v1"
            for item in self.recorded_attempts
        ) or any(
            item.artifact.artifact_type != "behavior_recovery_episode.v1"
            for item in self.recorded_episodes
        ):
            raise ValueError("recorded recovery Artifact type drift")
        _bytewise([item.reason_code for item in self.reason_histogram], "reason histogram")
        reason_codes = (
            [item.reason_code for item in self.executed]
            + [item.reason_code for item in self.failed]
            + [item.reason_code for item in self.skipped]
        )
        expected_reasons = Counter(reason_codes)
        if {item.reason_code: item.count for item in self.reason_histogram} != expected_reasons:
            raise ValueError("reason histogram does not match terminal arrays")
        expected_outcomes = Counter(item.domain_outcome for item in self.executed)
        if {
            item.domain_outcome: item.count for item in self.counts.domain_outcomes
        } != expected_outcomes:
            raise ValueError("domain outcome counts do not match executed rows")
        terminal_total = len(self.executed) + len(self.failed) + len(self.skipped)
        if (
            self.counts.terminal_total != terminal_total
            or self.counts.executed_total != len(self.executed)
            or self.counts.failed_total != len(self.failed)
            or self.counts.skipped_total != len(self.skipped)
            or self.counts.recorded_attempt_count != len(self.recorded_attempts)
            or self.counts.recorded_episode_count != len(self.recorded_episodes)
        ):
            raise ValueError("recovery index count drift")
        expected_outcome = (
            "recorded_recoveries" if self.recorded_attempts else "no_recorded_recovery"
        )
        if self.outcome != expected_outcome:
            raise ValueError("recovery index outcome drift")
        return self


class BehaviorRecoveryIndexArtifactV1(PipelineModel):
    schema_version: Literal["behavior_recovery_index.v1"]
    payload: BehaviorRecoveryIndexPayloadV1
    files: list[ArtifactFileV1]
    provenance: PipelineArtifactProvenanceV1

    @model_validator(mode="after")
    def json_only(self) -> BehaviorRecoveryIndexArtifactV1:
        if self.files:
            raise ValueError("recovery index is JSON-only")
        return self


class TrainingVideoFileV1(PipelineModel):
    video_key: Literal[
        "observation.images.rgb.head",
        "observation.images.rgb.left_wrist",
        "observation.images.rgb.right_wrist",
    ]
    relative_path: str
    sha256: Digest
    size_bytes: PositiveUInt64
    frame_count: PositiveUInt64

    _validate_path = field_validator("relative_path")(_safe_relative_path)


class TrainingSourceArtifactsV1(PipelineModel):
    attempt: ContentArtifactRefV1
    episode: ContentArtifactRefV1
    failure_case: ContentArtifactRefV1
    restore_bundle: ContentArtifactRefV1


class TrainingEpisodeV1(PipelineModel):
    episode_index: UInt32
    behavior_episode_key: UInt64
    recovery_episode_id: UUID
    recovery_attempt_id: UUID
    behavior_task_id: UInt32
    loom_task_id: str
    task_index: UInt32
    stream: Literal["primitive", "mop"]
    candidate_id: str
    sample_id: UUID
    frame_count: PositiveUInt64
    data_relative_path: str
    video_files: Annotated[list[TrainingVideoFileV1], Field(min_length=3, max_length=3)]
    source_artifacts: TrainingSourceArtifactsV1

    @field_validator("loom_task_id", "candidate_id")
    @classmethod
    def normalize_identity(cls, value: str) -> str:
        return _nfc(value, label="training identity", max_bytes=256)

    _validate_data_path = field_validator("data_relative_path")(_safe_relative_path)

    @model_validator(mode="after")
    def validate_videos(self) -> TrainingEpisodeV1:
        expected = [
            "observation.images.rgb.head",
            "observation.images.rgb.left_wrist",
            "observation.images.rgb.right_wrist",
        ]
        if [item.video_key for item in self.video_files] != expected:
            raise ValueError("training video files are not in fixed camera order")
        if any(item.frame_count != self.frame_count for item in self.video_files):
            raise ValueError("training video frame count drift")
        return self


class BehaviorTrainingDatasetPayloadV1(PipelineModel):
    format: Literal["lerobot_v2.1_episode"]
    writer_lock_sha256: Digest
    dataset_root: Literal["payload/lerobot"]
    fps: Literal[30]
    chunks_size: Literal[1000]
    episode_count: PositiveUInt32
    frame_count: PositiveUInt64
    task_count: PositiveUInt32
    episodes: list[TrainingEpisodeV1]
    tree_sha256: Digest
    safety_declaration: Literal["eligible_internal"]

    @model_validator(mode="after")
    def validate_episode_order(self) -> BehaviorTrainingDatasetPayloadV1:
        if len(self.episodes) != self.episode_count:
            raise ValueError("training episode_count drift")
        if [item.episode_index for item in self.episodes] != list(range(len(self.episodes))):
            raise ValueError("training episode indexes must be contiguous")
        if sum(item.frame_count for item in self.episodes) != self.frame_count:
            raise ValueError("training frame_count drift")
        if len({item.behavior_task_id for item in self.episodes}) != self.task_count:
            raise ValueError("training task_count drift")
        return self


class BehaviorTrainingDatasetArtifactV1(PipelineModel):
    schema_version: Literal["behavior_training_dataset.v1"]
    payload: BehaviorTrainingDatasetPayloadV1
    files: Annotated[list[ArtifactFileV1], Field(min_length=1)]
    provenance: PipelineArtifactProvenanceV1

    @model_validator(mode="after")
    def validate_inventory(self) -> BehaviorTrainingDatasetArtifactV1:
        _validate_artifact_files(self.files, label="training dataset")
        return self


ARTIFACT_MODELS: dict[str, type[PipelineModel]] = {
    "behavior_taskset_snapshot.v1": BehaviorTasksetSnapshotArtifactV1,
    "behavior_task_instance.v1": BehaviorTaskInstanceArtifactV1,
    "behavior_dataset_snapshot.v1": BehaviorDatasetSnapshotArtifactV1,
    "behavior_policy_checkpoint.v1": BehaviorPolicyCheckpointArtifactV1,
    "behavior_mop_bank.v1": BehaviorMopBankArtifactV1,
    "behavior_rollout_bundle.v1": BehaviorRolloutBundleArtifactV1,
    "behavior_inspection_report.v1": BehaviorInspectionReportArtifactV1,
    "behavior_failure_case.v1": BehaviorFailureCaseArtifactV1,
    "behavior_restore_bundle.v1": BehaviorRestoreBundleArtifactV1,
    "behavior_recovery_attempt.v1": BehaviorRecoveryAttemptArtifactV1,
    "behavior_recovery_episode.v1": BehaviorRecoveryEpisodeArtifactV1,
    "behavior_input_preflight.v1": BehaviorInputPreflightArtifactV1,
    "behavior_recovery_index.v1": BehaviorRecoveryIndexArtifactV1,
    "behavior_training_dataset.v1": BehaviorTrainingDatasetArtifactV1,
}


def validate_artifact_document(value: object) -> PipelineModel:
    """Select the exact closed Artifact model from its registered schema version."""

    if not isinstance(value, dict):
        raise ValueError("Artifact document must be a JSON object")
    schema_version = value.get("schema_version")
    if schema_version not in BEHAVIOR_ARTIFACT_TYPES:
        raise ValueError("unknown BEHAVIOR artifact type")
    model = ARTIFACT_MODELS.get(str(schema_version))
    if model is None:
        raise ValueError(f"artifact schema is not implemented: {schema_version}")
    from loom.pipeline.keys import canonical_document

    _reject_non_nfc_strings(value)
    result = model.model_validate_json(canonical_document(value))
    reject_secret_literals(result)
    return result


SCHEMA_MODELS: dict[str, type[PipelineModel]] = {
    "behavior.stage-request.v1": StageRequestV1,
    "behavior_rollout_parameters.v1": BehaviorRolloutParametersV1,
    "behavior.provider-assets.v1": ProviderAssetManifestV1,
    "behavior.primitive-runner-lock.v1": PrimitiveRunnerLockV1,
    "behavior.terminal-stage-set.v1": TerminalStageSetV1,
    "loom.stage-result.v1": StageResultV1,
    "behavior_taskset_snapshot.v1": BehaviorTasksetSnapshotArtifactV1,
    "behavior_task_instance.v1": BehaviorTaskInstanceArtifactV1,
    "behavior_dataset_snapshot.v1": BehaviorDatasetSnapshotArtifactV1,
    "behavior_policy_checkpoint.v1": BehaviorPolicyCheckpointArtifactV1,
    "behavior_mop_bank.v1": BehaviorMopBankArtifactV1,
    "behavior_rollout_bundle.v1": BehaviorRolloutBundleArtifactV1,
    "behavior_inspection_report.v1": BehaviorInspectionReportArtifactV1,
    "behavior_failure_case.v1": BehaviorFailureCaseArtifactV1,
    "behavior_restore_bundle.v1": BehaviorRestoreBundleArtifactV1,
    "behavior_recovery_attempt.v1": BehaviorRecoveryAttemptArtifactV1,
    "behavior_recovery_episode.v1": BehaviorRecoveryEpisodeArtifactV1,
    "behavior_input_preflight.v1": BehaviorInputPreflightArtifactV1,
    "behavior_recovery_index.v1": BehaviorRecoveryIndexArtifactV1,
    "behavior_training_dataset.v1": BehaviorTrainingDatasetArtifactV1,
}


def write_json_schemas(directory: Path) -> None:
    """Generate checked-in schemas deterministically from the Pydantic authorities."""

    from loom.pipeline.keys import canonical_document

    directory.mkdir(parents=True, exist_ok=True)
    for name, model in sorted(SCHEMA_MODELS.items()):
        target = directory / f"{name}.json"
        schema = model.model_json_schema(mode="validation")
        target.write_bytes(canonical_document(schema))
