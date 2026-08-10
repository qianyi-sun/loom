"""Typed immutable input for the BEHAVIOR StageRequest renderer."""

from __future__ import annotations

from typing import Annotated
from uuid import UUID

from pydantic import Field, model_validator

from loom.integrations.behavior.contracts import (
    BehaviorRolloutParametersV1,
    BehaviorStage,
    DatasetBuildParametersV1,
    EmptyParametersV1,
    MopRecoveryParametersV1,
    OfflineJudgeParametersV1,
    PrimitiveRecoveryParametersV1,
    StageParametersV1,
    StageRequestBindingSetV1,
    StageRequestProvenanceV1,
    TerminalStageSetV1,
)
from loom.pipeline.spec import PipelineModel, StageBudgetV1


class BehaviorStageRequestRenderInputV1(PipelineModel):
    stage: BehaviorStage
    run_id: UUID
    stage_run_id: UUID
    attempt_id: UUID
    inputs: Annotated[list[StageRequestBindingSetV1], Field(max_length=128)]
    parameters: StageParametersV1
    budget: StageBudgetV1
    provenance: StageRequestProvenanceV1
    orchestration: TerminalStageSetV1 | None

    @model_validator(mode="before")
    @classmethod
    def parse_parameters(cls, value: object) -> object:
        if not isinstance(value, dict):
            return value
        raw_stage = value.get("stage")
        if not isinstance(raw_stage, str):
            return value
        try:
            stage = BehaviorStage(raw_stage)
        except (TypeError, ValueError):
            return value
        raw = value.get("parameters")
        models: dict[BehaviorStage, type[PipelineModel]] = {
            BehaviorStage.INPUT_PREFLIGHT: EmptyParametersV1,
            BehaviorStage.ROLLOUT: BehaviorRolloutParametersV1,
            BehaviorStage.OFFLINE_JUDGE: OfflineJudgeParametersV1,
            BehaviorStage.FAILURE_MATERIALIZE: EmptyParametersV1,
            BehaviorStage.FRAME_AUTHOR: EmptyParametersV1,
            BehaviorStage.AGGREGATE: EmptyParametersV1,
            BehaviorStage.DATASET_BUILD: DatasetBuildParametersV1,
        }
        model: type[PipelineModel]
        if stage is BehaviorStage.RECOVERY:
            if not isinstance(raw, dict):
                return value
            model = (
                PrimitiveRecoveryParametersV1
                if raw.get("stream") == "primitive"
                else MopRecoveryParametersV1
            )
        else:
            model = models[stage]
        parsed = dict(value)
        from loom.pipeline.keys import canonical_document

        parsed["parameters"] = model.model_validate_json(canonical_document(raw))
        return parsed
