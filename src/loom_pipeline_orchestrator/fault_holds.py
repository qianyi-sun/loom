"""Acceptance-only S06-S09 one-shot fault-hold contracts (#1212/#1232)."""

from __future__ import annotations

from datetime import datetime
from typing import Literal, Protocol
from uuid import UUID

from pydantic import model_validator

from loom.pipeline.spec import Digest, PipelineModel, PositiveSafeInt


class PipelineAcceptanceFaultArmV1(PipelineModel):
    id: UUID
    authorization_id: UUID
    campaign_id: UUID
    candidate_sha256: Digest
    matrix_sha256: Digest
    scenario_id: Literal["S06", "S07", "S08", "S09"]
    pipeline_run_id: UUID
    target_node_key: str
    target_shard_key: str
    target_stage_run_id: UUID
    target_attempt_id: UUID
    target_attempt_ordinal: Literal[1]
    seam: str
    hook: str
    timing: str
    action: str
    fixture_repo_path: str
    fixture_sha256: Digest
    state: Literal["armed", "fired", "acked"]
    version: PositiveSafeInt
    armed_at: datetime
    fired_at: datetime | None
    acked_at: datetime | None
    fired_by_component: str | None
    ack_sha256: Digest | None

    @model_validator(mode="after")
    def timestamps_match_state(self) -> PipelineAcceptanceFaultArmV1:
        if self.state == "armed" and any(
            value is not None
            for value in (
                self.fired_at,
                self.acked_at,
                self.fired_by_component,
                self.ack_sha256,
            )
        ):
            raise ValueError("armed fault holds have no fire/ack fields")
        if self.state == "fired" and not (
            self.fired_at is not None
            and self.fired_by_component is not None
            and self.acked_at is None
            and self.ack_sha256 is None
        ):
            raise ValueError("fired fault holds require only fire fields")
        if self.state == "acked" and not (
            self.fired_at is not None
            and self.acked_at is not None
            and self.fired_by_component is not None
            and self.ack_sha256 is not None
        ):
            raise ValueError("acked fault holds require complete fire/ack fields")
        return self


class AcceptanceFaultArmRepositoryV1(Protocol):
    async def insert_for_attempt(
        self,
        *,
        pipeline_run_id: UUID,
        target_stage_run_id: UUID,
        target_attempt_id: UUID,
        target_attempt_ordinal: int,
    ) -> PipelineAcceptanceFaultArmV1: ...


FAULT_SELECTORS: dict[str, tuple[str, str, int, str]] = {
    "S06": (
        "recovery_primitive",
        "bytewise-max-failure-shard",
        1,
        "worker.before_stage_result_validation",
    ),
    "S07": ("recovery_mop", "only-failure-shard", 1, "worker.after_checkpoint_commit_readback"),
    "S08": ("dataset_build", "singleton", 1, "artifact.multipart.after_part_persisted"),
    "S09": ("offline_judge", "task-instance", 1, "gateway.after_dispatch_before_response"),
}


def validate_fault_hold_target(
    *, scenario_id: str, node_key: str, shard_selector: str, attempt_ordinal: int, seam: str
) -> None:
    expected = FAULT_SELECTORS.get(scenario_id)
    if expected is None or (node_key, shard_selector, attempt_ordinal, seam) != expected:
        raise ValueError("Attempt is not the exact acceptance-only S06-S09 fault target")
