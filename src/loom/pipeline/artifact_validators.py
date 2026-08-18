"""Closed code-owned Artifact document validator registry.

Workers validate by the frozen output Artifact type.  A new Recipe cannot make
an arbitrary schema executable merely by choosing a namespace-looking string;
its validator must be added to this code-owned registry first.
"""

from __future__ import annotations

from typing import Annotated, Any, Literal

from pydantic import Field, model_validator

from loom.integrations.behavior.contracts import (
    BEHAVIOR_ARTIFACT_TYPES,
)
from loom.integrations.behavior.contracts import (
    validate_artifact_document as validate_behavior_artifact,
)
from loom.integrations.terminalgen.artifacts import (
    ARTIFACT_MODELS as TERMINALGEN_ARTIFACT_MODELS,
)
from loom.integrations.terminalgen.artifacts import (
    validate_artifact_document as validate_terminalgen_artifact,
)
from loom.pipeline.keys import canonical_digest
from loom.pipeline.spec import Digest, PipelineModel, reject_secret_literals

_PIPELINE_CORE_TYPES = frozenset(
    {
        "loom.pipeline-core-seed.v1",
        "loom.pipeline-core-item.v1",
        "loom.pipeline-core-transformed.v1",
        "loom.pipeline-core-aggregate.v1",
        "loom.pipeline-core-receipt.v1",
    }
)


class PipelineCorePayloadV1(PipelineModel):
    value: Any
    value_sha256: Digest

    @model_validator(mode="after")
    def value_digest_matches(self) -> PipelineCorePayloadV1:
        if canonical_digest(self.value) != self.value_sha256:
            raise ValueError("pipeline core payload digest drift")
        reject_secret_literals(self.value)
        return self


class PipelineCoreArtifactV1(PipelineModel):
    schema_version: Literal[
        "loom.pipeline-core-seed.v1",
        "loom.pipeline-core-item.v1",
        "loom.pipeline-core-transformed.v1",
        "loom.pipeline-core-aggregate.v1",
        "loom.pipeline-core-receipt.v1",
    ]
    payload: PipelineCorePayloadV1
    files: Annotated[list[object], Field(max_length=0)]


def validate_official_artifact_document(
    artifact_type: str,
    value: object,
) -> PipelineModel:
    """Validate one document using only its exact code-owned Artifact type."""

    if artifact_type in BEHAVIOR_ARTIFACT_TYPES:
        result = validate_behavior_artifact(value)
    elif artifact_type in TERMINALGEN_ARTIFACT_MODELS:
        result = validate_terminalgen_artifact(value)
    elif artifact_type in _PIPELINE_CORE_TYPES:
        result = PipelineCoreArtifactV1.model_validate(value)
    else:
        raise ValueError("Artifact type has no installed official validator")
    if getattr(result, "schema_version", None) != artifact_type:
        raise ValueError("Artifact document schema does not match its frozen output type")
    return result


__all__ = [
    "PipelineCoreArtifactV1",
    "PipelineCorePayloadV1",
    "validate_official_artifact_document",
]
