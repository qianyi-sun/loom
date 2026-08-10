"""Convergent StageRun and PipelineRun result projection (#1212)."""

from __future__ import annotations

from dataclasses import dataclass

from loom.pipeline.budget import TerminalCause
from loom.pipeline.state import PipelineRunResult, PipelineStageRunState


@dataclass(frozen=True, slots=True)
class StageTerminalProjection:
    state: PipelineStageRunState
    selected: bool
    failure_policy: str | None


def project_pipeline_result(
    stages: list[StageTerminalProjection], *, terminal_cause: TerminalCause | None
) -> tuple[PipelineRunResult, str | None]:
    """Apply the issue truth table in precedence order."""

    if terminal_cause is TerminalCause.USER_CANCEL:
        return PipelineRunResult.CANCELLED, TerminalCause.USER_CANCEL.value
    if terminal_cause is not None:
        return PipelineRunResult.BUDGET_EXHAUSTED, terminal_cause.value
    selected = [stage for stage in stages if stage.selected]
    if any(stage.state is PipelineStageRunState.CANCELLED for stage in selected):
        return PipelineRunResult.FAILED, "controller_invariant_violation"
    failures = [stage for stage in selected if stage.state is PipelineStageRunState.FAILED]
    successes = [stage for stage in selected if stage.state is PipelineStageRunState.SUCCEEDED]
    if any(stage.failure_policy == "fail_run" for stage in failures):
        return PipelineRunResult.FAILED, "selected_fail_run_stage_failed"
    if failures and not successes:
        return PipelineRunResult.FAILED, "no_selected_stage_succeeded"
    if failures and successes:
        return PipelineRunResult.PARTIAL_FAILED, "selected_continue_stage_failed"
    return PipelineRunResult.SUCCEEDED, None


def all_selected_terminal(stages: list[StageTerminalProjection]) -> bool:
    terminal = {
        PipelineStageRunState.SUCCEEDED,
        PipelineStageRunState.FAILED,
        PipelineStageRunState.CANCELLED,
        PipelineStageRunState.SKIPPED,
    }
    return all(not stage.selected or stage.state in terminal for stage in stages)
