"""Strict, versioned contracts for the Loom-native BEHAVIOR Pipeline."""

from loom.integrations.behavior.contracts import (
    BEHAVIOR_ARTIFACT_TYPES,
    StageRequestV1,
    validate_artifact_document,
    validate_behavior_stage_result,
    validate_stage_request,
    validate_stage_result_document,
)
from loom.integrations.behavior.provider import (
    PipelineAnthropicClient,
    PipelineProviderAuthError,
    build_pipeline_anthropic_client,
)
from loom.integrations.behavior.stage_credentials import (
    StageProviderAuthority,
    provider_authority_for_request,
)

__all__ = [
    "BEHAVIOR_ARTIFACT_TYPES",
    "PipelineAnthropicClient",
    "PipelineProviderAuthError",
    "StageProviderAuthority",
    "StageRequestV1",
    "build_pipeline_anthropic_client",
    "provider_authority_for_request",
    "validate_artifact_document",
    "validate_behavior_stage_result",
    "validate_stage_request",
    "validate_stage_result_document",
]
