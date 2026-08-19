"""Attempt-scoped request contract for Loom-owned TerminalGen stages."""

from __future__ import annotations

from typing import Any, Literal
from uuid import UUID

from pydantic import Field, model_validator

from loom.pipeline.control_bindings import ControlBindingSnapshotDocumentV1
from loom.pipeline.keys import canonical_digest, canonical_document
from loom.pipeline.recipes import reject_secret_literals
from loom.pipeline.spec import (
    BindingSetV1,
    Digest,
    FanoutManifestItemV1,
    PipelineModel,
    PipelineTerminalSnapshotDocumentV1,
    StageBudgetV1,
)


class TerminalGenStageRequestProvenanceV1(PipelineModel):
    recipe_digest: Digest
    run_graph_digest: Digest
    resolved_input_bindings_digest: Digest
    execution_spec_digest: Digest
    resource_profile_digest: Digest
    image_runtime_contract_digest: Digest
    resolved_image_manifest_digest: Digest
    request_renderer_digest: Digest
    control_binding: ControlBindingSnapshotDocumentV1 | None


class TerminalGenStageRequestV1(PipelineModel):
    schema_version: Literal["terminalgen.stage-request.v1"]
    run_id: UUID
    stage_run_id: UUID
    attempt_id: UUID
    node_key: str
    shard_key: str
    idempotency_key: Digest
    inputs: list[BindingSetV1] = Field(max_length=128)
    parameters: dict[str, Any]
    fanout_item: FanoutManifestItemV1 | None
    budget: StageBudgetV1
    provenance: TerminalGenStageRequestProvenanceV1
    orchestration: PipelineTerminalSnapshotDocumentV1 | None

    @model_validator(mode="after")
    def closed_request(self) -> TerminalGenStageRequestV1:
        reject_secret_literals(self.parameters)
        if self.fanout_item is not None:
            reject_secret_literals(self.fanout_item.parameters)
        if self.shard_key == "singleton" and self.fanout_item is not None:
            raise ValueError("singleton request cannot carry a fanout item")
        if self.shard_key != "singleton":
            if self.fanout_item is None or self.fanout_item.shard_key != self.shard_key:
                raise ValueError("expanded request requires its exact fanout item")
        preimage = self.model_dump(mode="json", exclude={"idempotency_key"})
        if canonical_digest(preimage, persisted=False) != self.idempotency_key:
            raise ValueError("idempotency_key does not match the Attempt-scoped request")
        return self


def render(value: TerminalGenStageRequestV1 | dict[str, Any]) -> bytes:
    request = (
        value
        if isinstance(value, TerminalGenStageRequestV1)
        else TerminalGenStageRequestV1.model_validate(value)
    )
    return canonical_document(request.model_dump(mode="json"))


__all__ = [
    "TerminalGenStageRequestProvenanceV1",
    "TerminalGenStageRequestV1",
    "render",
]
