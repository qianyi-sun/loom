from __future__ import annotations

from loom.pipeline.projection import StageTerminalProjection, project_pipeline_result
from loom.pipeline.state import PipelineRunResult, PipelineStageRunState


def test_continue_failure_with_success_is_partial_failed() -> None:
    result, _reason = project_pipeline_result(
        [
            StageTerminalProjection(PipelineStageRunState.SUCCEEDED, True, "continue"),
            StageTerminalProjection(PipelineStageRunState.FAILED, True, "continue"),
        ],
        terminal_cause=None,
    )
    assert result is PipelineRunResult.PARTIAL_FAILED
