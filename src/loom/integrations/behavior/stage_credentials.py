"""Registered BEHAVIOR stage credential authorities.

This registry is intentionally smaller than the execution registry owned by
the stage implementation issues.  It answers one security-critical question:
which immutable control binding, if any, may authorize Provider I/O for this
request.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Literal
from uuid import UUID

from loom.integrations.behavior.contracts import (
    BehaviorStage,
    JudgeProfileProvenanceV1,
    MopRecoveryParametersV1,
    PrimitiveProviderProvenanceV1,
    PrimitiveRecoveryParametersV1,
    StageRequestV1,
)
from loom.integrations.behavior.provider import (
    OFFLINE_JUDGE_STEP_ID,
    PIPELINE_STEP_JWT_PATH,
    PRIMITIVE_STEP_ID,
)

ProviderMode = Literal["none", "offline_judge", "primitive"]


class StageProviderContractError(ValueError):
    """A StageRequest attempted to cross a registered credential seam."""


@dataclass(frozen=True)
class StageProviderAuthority:
    mode: ProviderMode
    attempt_id: UUID
    step_id: str | None
    binding_sha256: str | None
    token_path: Path | None


def provider_authority_for_request(request: StageRequestV1) -> StageProviderAuthority:
    """Resolve the one legal Provider authority for a validated request."""

    control = request.provenance.control_binding
    if request.stage is BehaviorStage.OFFLINE_JUDGE:
        if not isinstance(control, JudgeProfileProvenanceV1):
            raise StageProviderContractError("offline judge requires a judge-profile snapshot")
        return StageProviderAuthority(
            mode="offline_judge",
            attempt_id=request.attempt_id,
            step_id=OFFLINE_JUDGE_STEP_ID,
            binding_sha256=control.snapshot_sha256,
            token_path=PIPELINE_STEP_JWT_PATH,
        )

    if request.stage is BehaviorStage.RECOVERY and isinstance(
        request.parameters, PrimitiveRecoveryParametersV1
    ):
        if not isinstance(control, PrimitiveProviderProvenanceV1):
            raise StageProviderContractError("primitive recovery requires its Provider snapshot")
        return StageProviderAuthority(
            mode="primitive",
            attempt_id=request.attempt_id,
            step_id=PRIMITIVE_STEP_ID,
            binding_sha256=control.snapshot_sha256,
            token_path=PIPELINE_STEP_JWT_PATH,
        )

    if (
        control is not None
        or request.budget.provider is not None
        or (
            request.stage is BehaviorStage.RECOVERY
            and not isinstance(request.parameters, MopRecoveryParametersV1)
        )
    ):
        raise StageProviderContractError("Provider-null stage contains credential authority")
    return StageProviderAuthority(
        mode="none",
        attempt_id=request.attempt_id,
        step_id=None,
        binding_sha256=None,
        token_path=None,
    )


REGISTERED_STAGE_PROVIDER_MODES: dict[BehaviorStage, frozenset[ProviderMode]] = {
    BehaviorStage.INPUT_PREFLIGHT: frozenset({"none"}),
    BehaviorStage.ROLLOUT: frozenset({"none"}),
    BehaviorStage.OFFLINE_JUDGE: frozenset({"offline_judge"}),
    BehaviorStage.FAILURE_MATERIALIZE: frozenset({"none"}),
    BehaviorStage.FRAME_AUTHOR: frozenset({"none"}),
    BehaviorStage.RECOVERY: frozenset({"primitive", "none"}),
    BehaviorStage.AGGREGATE: frozenset({"none"}),
    BehaviorStage.DATASET_BUILD: frozenset({"none"}),
}


__all__ = [
    "REGISTERED_STAGE_PROVIDER_MODES",
    "ProviderMode",
    "StageProviderAuthority",
    "StageProviderContractError",
    "provider_authority_for_request",
]
