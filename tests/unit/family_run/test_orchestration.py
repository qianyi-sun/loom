"""Orchestration helpers: pure state-machine transitions."""

from __future__ import annotations

from dataclasses import dataclass, field
from uuid import uuid4

from loom.family_run.orchestration import (
    apply_advance_decision,
)
from loom.family_run.spec import AdvanceDecision


@dataclass
class _Family:
    batch_id: object = field(default_factory=uuid4)
    family_key: str = "fam"
    task_sequence: list[str] = field(default_factory=lambda: ["t/1", "t/2", "t/3"])
    current_index: int = 0
    attempt_count: int = 0
    state: str = "running"


def test_advance_moves_to_adapting():
    next_state = apply_advance_decision(_Family(), AdvanceDecision.ADVANCE)
    assert next_state.state == "adapting"
    assert next_state.current_index == 0  # unchanged; orchestrator bumps
    assert next_state.attempt_count == 0


def test_retry_keeps_index_and_bumps_attempt():
    next_state = apply_advance_decision(
        _Family(attempt_count=1), AdvanceDecision.RETRY,
    )
    assert next_state.state == "pending"
    assert next_state.current_index == 0
    assert next_state.attempt_count == 2


def test_skip_bumps_index_and_returns_pending_when_not_last():
    fam = _Family(current_index=0)
    next_state = apply_advance_decision(fam, AdvanceDecision.SKIP)
    assert next_state.state == "pending"
    assert next_state.current_index == 1
    assert next_state.attempt_count == 0


def test_skip_returns_done_when_last_task_finished():
    fam = _Family(current_index=2)  # final index in a 3-task sequence
    next_state = apply_advance_decision(fam, AdvanceDecision.SKIP)
    assert next_state.state == "done"
    assert next_state.current_index == 3


def test_abort_returns_aborted():
    next_state = apply_advance_decision(_Family(), AdvanceDecision.ABORT)
    assert next_state.state == "aborted"
