"""Pure renderer for ``behavior_stage_request@1``."""

from __future__ import annotations

from collections.abc import Mapping

from loom.integrations.behavior.contracts import StageRequestV1
from loom.pipeline.keys import canonical_digest, canonical_document
from loom.pipeline.renderers.behavior_stage_request_models import (
    BehaviorStageRequestRenderInputV1,
)


def render(value: BehaviorStageRequestRenderInputV1 | Mapping[str, object]) -> StageRequestV1:
    """Render and fully validate one immutable BEHAVIOR StageRequest."""

    source = (
        value
        if isinstance(value, BehaviorStageRequestRenderInputV1)
        else BehaviorStageRequestRenderInputV1.model_validate_json(canonical_document(value))
    )
    preimage = {
        "attempt_id": str(source.attempt_id),
        "execution_spec_digest": source.provenance.execution_spec_digest,
        "resolved_input_bindings_digest": source.provenance.resolved_input_bindings_digest,
        "stage_run_id": str(source.stage_run_id),
    }
    return StageRequestV1(
        schema_version="behavior.stage-request.v1",
        stage=source.stage,
        run_id=source.run_id,
        stage_run_id=source.stage_run_id,
        attempt_id=source.attempt_id,
        idempotency_key=canonical_digest(preimage, persisted=False),
        inputs=source.inputs,
        parameters=source.parameters,
        budget=source.budget,
        provenance=source.provenance,
        orchestration=source.orchestration,
    )
