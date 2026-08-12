"""Canonical Pipeline execution-checkpoint contracts.

The inner checkpoint directory belongs to the stage adapter's
``AttemptWorkspace``.  This module defines the distinct, worker-generated
outer envelope and its compatibility identity.  Keeping those authorities
separate prevents an adapter from choosing the identity used for resume.
"""

from __future__ import annotations

import unicodedata
from pathlib import PurePosixPath
from typing import Annotated, Literal
from uuid import UUID

from pydantic import Field, field_validator, model_validator

from loom.pipeline.keys import MAX_SAFE_INTEGER, canonical_digest, canonical_document
from loom.pipeline.spec import Digest, PipelineModel

EXECUTION_CHECKPOINT_ARTIFACT_TYPE = "loom.execution-checkpoint.v1"
INFRASTRUCTURE_RESUME_REASONS = frozenset(
    {
        "worker_lost",
        "node_setup_health",
        "object_store_transport",
        "container_start_transient",
        "stage_helper_transient",
    }
)


def _checkpoint_relative_path(value: str) -> str:
    normalized = unicodedata.normalize("NFC", value)
    normalized.encode("utf-8", errors="strict")
    path = PurePosixPath(normalized)
    if (
        normalized != value
        or not value
        or path.is_absolute()
        or "\\" in value
        or "\x00" in value
        or any(part in {"", ".", ".."} for part in value.split("/"))
        or (
            value not in {"COMPLETE.json", "ledger.json"}
            and not value.startswith("payload/")
        )
    ):
        raise ValueError("checkpoint file path is outside the committed checkpoint root")
    return value


class CheckpointPayloadFileV1(PipelineModel):
    """One exact file in the inner committed checkpoint view."""

    relative_path: str
    size_bytes: Annotated[int, Field(strict=True, ge=0, le=MAX_SAFE_INTEGER)]
    sha256: Digest

    _path_is_confined = field_validator("relative_path")(_checkpoint_relative_path)


def resume_compatibility_key(
    *,
    recipe_digest: str,
    resolved_input_bindings_digest: str,
    execution_spec_digest: str,
    image_digest: str,
) -> str:
    """Derive the raw-JCS (no LF) five-field resume identity."""

    # Validate the digest spelling through the closed model before hashing it.
    identity = _ResumeCompatibilityIdentityV1(
        checkpoint_schema="loom.execution-checkpoint.v1",
        execution_spec_digest=execution_spec_digest,
        image_digest=image_digest,
        resolved_input_bindings_digest=resolved_input_bindings_digest,
        recipe_digest=recipe_digest,
    )
    return canonical_digest(identity, persisted=False)


class _ResumeCompatibilityIdentityV1(PipelineModel):
    checkpoint_schema: Literal["loom.execution-checkpoint.v1"]
    execution_spec_digest: Digest
    image_digest: Digest
    resolved_input_bindings_digest: Digest
    recipe_digest: Digest


class ExecutionCheckpointV1(PipelineModel):
    """Exact worker-generated semantic document for a checkpoint Artifact."""

    schema_version: Literal["loom.execution-checkpoint.v1"]
    pipeline_run_id: UUID
    stage_run_id: UUID
    attempt_id: UUID
    sequence: Annotated[int, Field(strict=True, ge=0, le=MAX_SAFE_INTEGER)]
    recipe_digest: Digest
    resolved_input_bindings_digest: Digest
    execution_spec_digest: Digest
    image_digest: Digest
    resume_compatibility_key: Digest
    inner_ledger_sha256: Digest
    inner_complete_sha256: Digest
    files: Annotated[list[CheckpointPayloadFileV1], Field(min_length=2)]

    @model_validator(mode="after")
    def identity_and_inventory_are_exact(self) -> ExecutionCheckpointV1:
        paths = [item.relative_path for item in self.files]
        if paths != sorted(paths, key=lambda value: value.encode("utf-8")):
            raise ValueError("checkpoint files must be bytewise sorted")
        if len(paths) != len(set(paths)):
            raise ValueError("checkpoint files must be unique")
        by_path = {item.relative_path: item for item in self.files}
        if "COMPLETE.json" not in by_path or "ledger.json" not in by_path:
            raise ValueError("checkpoint inventory must include COMPLETE.json and ledger.json")
        if by_path["COMPLETE.json"].sha256 != self.inner_complete_sha256:
            raise ValueError("inner COMPLETE digest drifts from the file inventory")
        if by_path["ledger.json"].sha256 != self.inner_ledger_sha256:
            raise ValueError("inner ledger digest drifts from the file inventory")
        expected = resume_compatibility_key(
            recipe_digest=self.recipe_digest,
            resolved_input_bindings_digest=self.resolved_input_bindings_digest,
            execution_spec_digest=self.execution_spec_digest,
            image_digest=self.image_digest,
        )
        if self.resume_compatibility_key != expected:
            raise ValueError("resume compatibility key does not match the frozen claim")
        return self

    def persisted_bytes(self) -> bytes:
        """Return the normative RFC8785 JCS document with one trailing LF."""

        return canonical_document(self)

    @property
    def exact_artifact_data_bytes(self) -> int:
        """Bytes charged to the checkpoint policy, excluding platform markers."""

        return len(self.persisted_bytes()) + sum(item.size_bytes for item in self.files)

    def require_within(self, max_bytes: int) -> None:
        if (
            isinstance(max_bytes, bool)
            or not isinstance(max_bytes, int)
            or not 1 <= max_bytes <= MAX_SAFE_INTEGER
        ):
            raise ValueError("checkpoint max_bytes is outside the interoperable range")
        if self.exact_artifact_data_bytes > max_bytes:
            raise ValueError("checkpoint_too_large")


def execution_checkpoint_bytes(value: ExecutionCheckpointV1) -> bytes:
    """Compatibility helper for callers that do not retain the model method."""

    return value.persisted_bytes()


def checkpoint_is_resume_compatible(
    *,
    previous_reason: str | None,
    recipe_digest: str,
    resolved_input_bindings_digest: str,
    execution_spec_digest: str,
    image_digest: str,
    observed_recipe_digest: str,
    observed_input_bindings_digest: str,
    observed_execution_spec_digest: str,
    observed_image_digest: str,
    observed_resume_compatibility_key: str,
) -> bool:
    """Apply the closed automatic-infrastructure-resume allowlist and five-key fence."""

    if previous_reason not in INFRASTRUCTURE_RESUME_REASONS:
        return False
    expected_key = resume_compatibility_key(
        recipe_digest=recipe_digest,
        resolved_input_bindings_digest=resolved_input_bindings_digest,
        execution_spec_digest=execution_spec_digest,
        image_digest=image_digest,
    )
    return (
        observed_recipe_digest,
        observed_input_bindings_digest,
        observed_execution_spec_digest,
        observed_image_digest,
        observed_resume_compatibility_key,
    ) == (
        recipe_digest,
        resolved_input_bindings_digest,
        execution_spec_digest,
        image_digest,
        expected_key,
    )


__all__ = [
    "EXECUTION_CHECKPOINT_ARTIFACT_TYPE",
    "INFRASTRUCTURE_RESUME_REASONS",
    "CheckpointPayloadFileV1",
    "ExecutionCheckpointV1",
    "checkpoint_is_resume_compatible",
    "execution_checkpoint_bytes",
    "resume_compatibility_key",
]
