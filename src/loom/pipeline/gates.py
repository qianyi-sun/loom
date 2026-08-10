"""Deterministic outcome-gate and strict-AND target truth (#1212)."""

from __future__ import annotations

from enum import StrEnum

from loom.pipeline.state import PipelineStageRunState


class GateSelection(StrEnum):
    PENDING = "pending"
    SELECTED = "selected"
    NOT_SELECTED = "not_selected"
    SUBJECT_NOT_SUCCEEDED = "subject_not_succeeded"


def project_outcome_gate(
    *, subject_state: PipelineStageRunState, domain_outcome: str | None, match_outcomes: list[str]
) -> GateSelection:
    if subject_state is PipelineStageRunState.SUCCEEDED:
        if domain_outcome is None:
            raise ValueError("succeeded gate subject requires a domain outcome")
        return (
            GateSelection.SELECTED
            if domain_outcome in match_outcomes
            else GateSelection.NOT_SELECTED
        )
    if subject_state in {
        PipelineStageRunState.FAILED,
        PipelineStageRunState.CANCELLED,
        PipelineStageRunState.SKIPPED,
    }:
        return GateSelection.SUBJECT_NOT_SUCCEEDED
    return GateSelection.PENDING


def strict_and_gate_target(selections: list[GateSelection]) -> GateSelection:
    if not selections:
        return GateSelection.SELECTED
    if any(
        value in {GateSelection.NOT_SELECTED, GateSelection.SUBJECT_NOT_SUCCEEDED}
        for value in selections
    ):
        return GateSelection.NOT_SELECTED
    if any(value is GateSelection.PENDING for value in selections):
        return GateSelection.PENDING
    return GateSelection.SELECTED
