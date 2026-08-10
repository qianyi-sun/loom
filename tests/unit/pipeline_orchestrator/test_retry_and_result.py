from __future__ import annotations

from uuid import uuid4

import pytest

from loom.pipeline.budget import TerminalCause
from loom.pipeline.closure import (
    TerminalBarrierDecision,
    build_terminal_snapshot,
    terminal_barrier_decision,
)
from loom.pipeline.gates import GateSelection, project_outcome_gate, strict_and_gate_target
from loom.pipeline.projection import StageTerminalProjection, project_pipeline_result
from loom.pipeline.retry import retry_class_for_exit, retry_decision
from loom.pipeline.spec import TerminalStageDescriptorV1
from loom.pipeline.state import PipelineRunResult, PipelineStageRunState, RetryClass


def test_retry_allowlist_and_exact_delays() -> None:
    first = retry_decision(
        completed_attempt_number=1,
        max_attempts=3,
        retry_class=RetryClass.PROVIDER_TRANSIENT,
        reason_code="provider_429",
        terminal_cause=None,
    )
    second = retry_decision(
        completed_attempt_number=2,
        max_attempts=3,
        retry_class=RetryClass.INFRASTRUCTURE_TRANSIENT,
        reason_code="object_store_transport",
        terminal_cause=None,
    )
    assert (first.retry, first.delay_seconds) == (True, 30)
    assert (second.retry, second.delay_seconds) == (True, 120)
    assert retry_class_for_exit(21, "gateway_transport") is RetryClass.PROVIDER_TRANSIENT
    with pytest.raises(ValueError):
        retry_class_for_exit(21, "object_store_transport")


def test_worker_lost_waits_for_cleanup_and_terminal_latch_blocks_retry() -> None:
    pending = retry_decision(
        completed_attempt_number=1,
        max_attempts=2,
        retry_class=RetryClass.INFRASTRUCTURE_TRANSIENT,
        reason_code="worker_lost",
        terminal_cause=None,
        cleanup_acknowledged=False,
    )
    cancelled = retry_decision(
        completed_attempt_number=1,
        max_attempts=2,
        retry_class=RetryClass.INFRASTRUCTURE_TRANSIENT,
        reason_code="worker_lost",
        terminal_cause="user_cancel",
    )
    assert pending.reason == "worker_cleanup_pending"
    assert cancelled.reason == "run_terminal_cause"


def test_gate_and_is_strict_and_subject_failure_selects_neither() -> None:
    assert project_outcome_gate(
        subject_state=PipelineStageRunState.SUCCEEDED,
        domain_outcome="authored",
        match_outcomes=["authored"],
    ) is GateSelection.SELECTED
    assert project_outcome_gate(
        subject_state=PipelineStageRunState.FAILED,
        domain_outcome=None,
        match_outcomes=["authored"],
    ) is GateSelection.SUBJECT_NOT_SUCCEEDED
    assert strict_and_gate_target(
        [GateSelection.SELECTED, GateSelection.PENDING]
    ) is GateSelection.PENDING
    assert strict_and_gate_target(
        [GateSelection.SELECTED, GateSelection.NOT_SELECTED]
    ) is GateSelection.NOT_SELECTED


@pytest.mark.parametrize(
    ("cause", "stages", "expected"),
    [
        (TerminalCause.USER_CANCEL, [], PipelineRunResult.CANCELLED),
        (TerminalCause.WALL_BUDGET, [], PipelineRunResult.BUDGET_EXHAUSTED),
        (
            None,
            [StageTerminalProjection(PipelineStageRunState.FAILED, True, "fail_run")],
            PipelineRunResult.FAILED,
        ),
        (
            None,
            [
                StageTerminalProjection(PipelineStageRunState.SUCCEEDED, True, "continue"),
                StageTerminalProjection(PipelineStageRunState.FAILED, True, "continue"),
            ],
            PipelineRunResult.PARTIAL_FAILED,
        ),
        (
            None,
            [
                StageTerminalProjection(PipelineStageRunState.SUCCEEDED, True, "fail_run"),
                StageTerminalProjection(PipelineStageRunState.SKIPPED, True, None),
            ],
            PipelineRunResult.SUCCEEDED,
        ),
    ],
)
def test_result_truth_table(
    cause: TerminalCause | None,
    stages: list[StageTerminalProjection],
    expected: PipelineRunResult,
) -> None:
    assert project_pipeline_result(stages, terminal_cause=cause)[0] is expected


def test_terminal_barrier_requires_all_gates_and_complete_closure() -> None:
    common = {
        "declared_rows_closed": True,
        "all_expansion_markers_present": True,
        "active_attempt_or_retry": False,
        "pending_cancel_or_cleanup": False,
        "run_terminal_cause": None,
    }
    assert terminal_barrier_decision(
        gate_selections=[GateSelection.SELECTED, GateSelection.PENDING], **common
    ) is TerminalBarrierDecision.BLOCKED
    assert terminal_barrier_decision(
        gate_selections=[GateSelection.SELECTED, GateSelection.NOT_SELECTED], **common
    ) is TerminalBarrierDecision.SKIPPED
    assert terminal_barrier_decision(
        gate_selections=[GateSelection.SELECTED, GateSelection.SELECTED], **common
    ) is TerminalBarrierDecision.READY


def test_terminal_snapshot_is_deterministic_and_compact() -> None:
    run_id = uuid4()
    descriptor = TerminalStageDescriptorV1(
        node_key="stage",
        shard_key="s00",
        stage_run_id=uuid4(),
        terminal_state="skipped",
        execution_attempt_id=None,
        stage_result_sha256=None,
        domain_outcome=None,
        reason_code="gate_not_selected",
        committed_outputs=[],
    )
    document, encoded, digest = build_terminal_snapshot(
        pipeline_run_id=run_id,
        run_graph_digest="sha256:" + "a" * 64,
        snapshot_id=uuid4(),
        terminal_stage_keys=["stage"],
        stages=[descriptor],
    )
    assert encoded.endswith(b"\n")
    assert digest.startswith("sha256:")
    assert document.stages == [descriptor]
