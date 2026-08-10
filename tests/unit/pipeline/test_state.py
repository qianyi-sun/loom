from __future__ import annotations

import pytest

from loom.pipeline.state import (
    ATTEMPT_TRANSITIONS,
    RUN_TRANSITIONS,
    STAGE_TRANSITIONS,
    ExecutionAttemptState,
    PipelineRunResult,
    PipelineRunState,
    PipelineStageRunState,
    require_transition,
)


def test_lifecycle_values_are_closed_and_stable() -> None:
    assert {state.value for state in PipelineRunState} == {
        "submitted",
        "running",
        "cancelling",
        "finished",
    }
    assert {result.value for result in PipelineRunResult} == {
        "succeeded",
        "partial_failed",
        "failed",
        "cancelled",
        "budget_exhausted",
    }
    assert {state.value for state in PipelineStageRunState} == {
        "blocked",
        "ready",
        "queued",
        "claimed",
        "running",
        "retry_wait",
        "succeeded",
        "failed",
        "cancelled",
        "skipped",
    }
    assert {state.value for state in ExecutionAttemptState} == {
        "fault_pending",
        "queued",
        "claimed",
        "running",
        "succeeded",
        "failed",
        "cancelled",
        "lost",
    }


@pytest.mark.parametrize(
    ("current", "target", "table"),
    [
        (PipelineRunState.SUBMITTED, PipelineRunState.RUNNING, RUN_TRANSITIONS),
        (PipelineRunState.SUBMITTED, PipelineRunState.FINISHED, RUN_TRANSITIONS),
        (PipelineRunState.CANCELLING, PipelineRunState.FINISHED, RUN_TRANSITIONS),
        (PipelineStageRunState.BLOCKED, PipelineStageRunState.READY, STAGE_TRANSITIONS),
        (PipelineStageRunState.BLOCKED, PipelineStageRunState.SKIPPED, STAGE_TRANSITIONS),
        (PipelineStageRunState.RUNNING, PipelineStageRunState.RETRY_WAIT, STAGE_TRANSITIONS),
        (PipelineStageRunState.RETRY_WAIT, PipelineStageRunState.QUEUED, STAGE_TRANSITIONS),
        (
            ExecutionAttemptState.FAULT_PENDING,
            ExecutionAttemptState.QUEUED,
            ATTEMPT_TRANSITIONS,
        ),
        (ExecutionAttemptState.CLAIMED, ExecutionAttemptState.LOST, ATTEMPT_TRANSITIONS),
        (ExecutionAttemptState.RUNNING, ExecutionAttemptState.SUCCEEDED, ATTEMPT_TRANSITIONS),
    ],
)
def test_allowed_lifecycle_edges_succeed(current: object, target: object, table: object) -> None:
    require_transition(current, target, table)  # type: ignore[arg-type]


@pytest.mark.parametrize(
    ("current", "target", "table"),
    [
        (PipelineRunState.SUBMITTED, PipelineRunState.SUBMITTED, RUN_TRANSITIONS),
        (PipelineRunState.RUNNING, PipelineRunState.SUBMITTED, RUN_TRANSITIONS),
        (PipelineRunState.FINISHED, PipelineRunState.RUNNING, RUN_TRANSITIONS),
        (PipelineStageRunState.BLOCKED, PipelineStageRunState.QUEUED, STAGE_TRANSITIONS),
        (PipelineStageRunState.READY, PipelineStageRunState.SUCCEEDED, STAGE_TRANSITIONS),
        (PipelineStageRunState.SUCCEEDED, PipelineStageRunState.RETRY_WAIT, STAGE_TRANSITIONS),
        (PipelineStageRunState.SKIPPED, PipelineStageRunState.READY, STAGE_TRANSITIONS),
        (ExecutionAttemptState.FAULT_PENDING, ExecutionAttemptState.RUNNING, ATTEMPT_TRANSITIONS),
        (ExecutionAttemptState.QUEUED, ExecutionAttemptState.SUCCEEDED, ATTEMPT_TRANSITIONS),
        (ExecutionAttemptState.LOST, ExecutionAttemptState.QUEUED, ATTEMPT_TRANSITIONS),
    ],
)
def test_illegal_lifecycle_edges_fail_closed(
    current: object, target: object, table: object
) -> None:
    with pytest.raises(ValueError, match=f"illegal transition: {current.value} -> {target.value}"):
        require_transition(current, target, table)  # type: ignore[arg-type,union-attr]


@pytest.mark.parametrize(
    ("terminal", "table"),
    [
        (PipelineRunState.FINISHED, RUN_TRANSITIONS),
        (PipelineStageRunState.SUCCEEDED, STAGE_TRANSITIONS),
        (PipelineStageRunState.FAILED, STAGE_TRANSITIONS),
        (PipelineStageRunState.CANCELLED, STAGE_TRANSITIONS),
        (PipelineStageRunState.SKIPPED, STAGE_TRANSITIONS),
        (ExecutionAttemptState.SUCCEEDED, ATTEMPT_TRANSITIONS),
        (ExecutionAttemptState.FAILED, ATTEMPT_TRANSITIONS),
        (ExecutionAttemptState.CANCELLED, ATTEMPT_TRANSITIONS),
        (ExecutionAttemptState.LOST, ATTEMPT_TRANSITIONS),
    ],
)
def test_terminal_states_have_no_outgoing_edges(terminal: object, table: object) -> None:
    assert table[terminal] == frozenset()  # type: ignore[index]
