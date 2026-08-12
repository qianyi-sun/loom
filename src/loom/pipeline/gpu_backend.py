"""Immutable Pipeline GPU backend selection and scope validation."""

from __future__ import annotations

from datetime import datetime
from hashlib import sha256
from typing import Literal
from uuid import UUID

from pydantic import field_validator, model_validator

from loom.pipeline.keys import canonical_digest
from loom.pipeline.spec import Digest, PipelineModel

GPU_VARIANT_ORDER = ("gb10-shared-1gpu", "oldlab-rtx5080-2gpu")


class PipelineGpuSelectionError(ValueError):
    pass


class PipelineRunGpuBackendSelectionV1(PipelineModel):
    pipeline_run_id: UUID
    scope: Literal["all_gpu_nodes", "oldlab_preflight", "gb10_preflight"]
    variant_id: Literal["gb10-shared-1gpu", "oldlab-rtx5080-2gpu"]
    policy_id: Literal["behavior-gpu-gb10", "behavior-gpu-oldlab"]
    selection_source: Literal[
        "recipe_hash", "acceptance_authority", "profile_calibration_authority"
    ]
    selected_at: datetime

    @field_validator("selected_at")
    @classmethod
    def timestamp_is_aware(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("selected_at must include a timezone")
        return value

    @model_validator(mode="after")
    def authority_scope_is_exact(self) -> PipelineRunGpuBackendSelectionV1:
        expected_policy = {
            "gb10-shared-1gpu": "behavior-gpu-gb10",
            "oldlab-rtx5080-2gpu": "behavior-gpu-oldlab",
        }[self.variant_id]
        if self.policy_id != expected_policy:
            raise ValueError("GPU variant and policy drift")
        if self.selection_source == "recipe_hash":
            if self.scope != "all_gpu_nodes":
                raise ValueError("ordinary recipe hashing owns only all_gpu_nodes")
        elif self.scope != "all_gpu_nodes":
            expected_scope = {
                "gb10-shared-1gpu": "gb10_preflight",
                "oldlab-rtx5080-2gpu": "oldlab_preflight",
            }[self.variant_id]
            if self.scope != expected_scope:
                raise ValueError("service authority scope and GPU variant drift")
        return self

    @property
    def gpu_backend_selection_sha256(self) -> Digest:
        return canonical_digest(self.model_dump(mode="json"))


def recipe_hash_variant(*, recipe_digest: str, pipeline_run_id: UUID) -> str:
    """Choose one fixed backend from the first eight big-endian hash bytes."""

    if not recipe_digest.startswith("sha256:") or len(recipe_digest) != 71:
        raise PipelineGpuSelectionError("recipe digest must be a canonical SHA-256 digest")
    material = recipe_digest.encode("ascii") + b"\x00" + str(pipeline_run_id).encode("ascii")
    index = int.from_bytes(sha256(material).digest()[:8], "big") % len(GPU_VARIANT_ORDER)
    return GPU_VARIANT_ORDER[index]


def select_ordinary_gpu_backend(
    *, recipe_digest: str, pipeline_run_id: UUID, selected_at: datetime
) -> PipelineRunGpuBackendSelectionV1:
    variant = recipe_hash_variant(recipe_digest=recipe_digest, pipeline_run_id=pipeline_run_id)
    policy = (
        "behavior-gpu-gb10"
        if variant == "gb10-shared-1gpu"
        else "behavior-gpu-oldlab"
    )
    return PipelineRunGpuBackendSelectionV1(
        pipeline_run_id=pipeline_run_id,
        scope="all_gpu_nodes",
        variant_id=variant,  # type: ignore[arg-type]
        policy_id=policy,  # type: ignore[arg-type]
        selection_source="recipe_hash",
        selected_at=selected_at,
    )


def validate_gpu_selection_set(
    *,
    recipe_name: str,
    selections: list[PipelineRunGpuBackendSelectionV1],
) -> None:
    scopes = [selection.scope for selection in selections]
    if len(scopes) != len(set(scopes)):
        raise PipelineGpuSelectionError("GPU backend selection scopes must be unique")
    if recipe_name == "behavior-recovery-acceptance-preflight":
        if set(scopes) != {"oldlab_preflight", "gb10_preflight"} or len(scopes) != 2:
            raise PipelineGpuSelectionError("hidden preflight requires exactly two GPU scopes")
        if any(selection.selection_source == "recipe_hash" for selection in selections):
            raise PipelineGpuSelectionError("hidden preflight requires a closed service authority")
    elif scopes != ["all_gpu_nodes"]:
        raise PipelineGpuSelectionError("ordinary and official Runs require one GPU scope")
