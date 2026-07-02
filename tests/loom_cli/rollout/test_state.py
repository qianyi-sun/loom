"""State machine + JSON persistence for the rollout driver (#340).

Tests cover the tiny per-step FSM (not_started → running → verifying → done | failed)
and the top-level RolloutState that owns the sequence of steps + persists to
`state.json` in the evidence directory.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from loom_cli.rollout.state import (
    RolloutState,
    StepRecord,
    StepState,
)


class TestStepState:
    """Per-step state enum."""

    def test_terminal_states(self) -> None:
        assert StepState.DONE.is_terminal()
        assert StepState.FAILED.is_terminal()
        assert not StepState.NOT_STARTED.is_terminal()
        assert not StepState.RUNNING.is_terminal()
        assert not StepState.VERIFYING.is_terminal()

    def test_is_success_only_for_done(self) -> None:
        assert StepState.DONE.is_success()
        for state in (
            StepState.NOT_STARTED, StepState.RUNNING,
            StepState.VERIFYING, StepState.FAILED,
        ):
            assert not state.is_success()

    def test_str_repr_is_lowercase(self) -> None:
        # JSON persistence uses .value; must be the lowercase snake form.
        assert StepState.NOT_STARTED.value == "not_started"
        assert StepState.RUNNING.value == "running"
        assert StepState.VERIFYING.value == "verifying"
        assert StepState.DONE.value == "done"
        assert StepState.FAILED.value == "failed"


class TestStepRecord:
    """A single row in state.json's `steps` list."""

    def test_dataclass_defaults(self) -> None:
        rec = StepRecord(number=4, name="kind-load")
        assert rec.state is StepState.NOT_STARTED
        assert rec.inputs_hash is None
        assert rec.started_at is None
        assert rec.finished_at is None
        assert rec.error is None

    def test_to_dict_roundtrip(self) -> None:
        rec = StepRecord(
            number=4, name="kind-load",
            state=StepState.DONE,
            inputs_hash="abc123",
            started_at="2026-07-02T22:00:00Z",
            finished_at="2026-07-02T22:00:05Z",
        )
        data = rec.to_dict()
        assert data == {
            "number": 4,
            "name": "kind-load",
            "state": "done",
            "inputs_hash": "abc123",
            "started_at": "2026-07-02T22:00:00Z",
            "finished_at": "2026-07-02T22:00:05Z",
            "error": None,
        }
        assert StepRecord.from_dict(data) == rec

    def test_from_dict_rejects_unknown_state(self) -> None:
        with pytest.raises(ValueError, match="unknown step state"):
            StepRecord.from_dict({
                "number": 4, "name": "x", "state": "chugging",
                "inputs_hash": None, "started_at": None,
                "finished_at": None, "error": None,
            })


class TestRolloutState:
    """Top-level state.json handling."""

    def test_new_starts_all_not_started(self) -> None:
        state = RolloutState.new(
            rollout_id="20260702t235959z-public-beta-abc",
            steps=[(0, "resolve-target"), (1, "worktree")],
        )
        assert state.rollout_id == "20260702t235959z-public-beta-abc"
        assert state.status == "running"
        assert state.current_step is None
        assert [s.name for s in state.steps] == ["resolve-target", "worktree"]
        assert all(s.state is StepState.NOT_STARTED for s in state.steps)

    def test_mark_step_running_sets_current(self) -> None:
        state = RolloutState.new(
            rollout_id="rid", steps=[(0, "a"), (1, "b")],
        )
        state.mark_step_running(0, started_at="2026-07-02T22:00:00Z")
        assert state.current_step == 0
        assert state.steps[0].state is StepState.RUNNING
        assert state.steps[0].started_at == "2026-07-02T22:00:00Z"

    def test_mark_step_done_bumps_current_step_on_next_running(self) -> None:
        state = RolloutState.new(
            rollout_id="rid", steps=[(0, "a"), (1, "b")],
        )
        state.mark_step_running(0, started_at="2026-07-02T22:00:00Z")
        state.mark_step_done(
            0,
            finished_at="2026-07-02T22:00:05Z",
            inputs_hash="hash-0",
        )
        assert state.steps[0].state is StepState.DONE
        assert state.steps[0].finished_at == "2026-07-02T22:00:05Z"
        assert state.steps[0].inputs_hash == "hash-0"

    def test_mark_step_failed_sets_status_failed(self) -> None:
        state = RolloutState.new(
            rollout_id="rid", steps=[(0, "a"), (1, "b")],
        )
        state.mark_step_running(0, started_at="t0")
        state.mark_step_failed(0, finished_at="t1", error="docker daemon unreachable")
        assert state.steps[0].state is StepState.FAILED
        assert state.steps[0].error == "docker daemon unreachable"
        assert state.status == "failed"

    def test_all_done_marks_rollout_done(self) -> None:
        state = RolloutState.new(
            rollout_id="rid", steps=[(0, "a"), (1, "b")],
        )
        state.mark_step_running(0, started_at="t0")
        state.mark_step_done(0, finished_at="t1", inputs_hash="h0")
        state.mark_step_running(1, started_at="t2")
        state.mark_step_done(1, finished_at="t3", inputs_hash="h1")
        assert state.status == "done"

    def test_json_persistence_roundtrip(self, tmp_path: Path) -> None:
        state = RolloutState.new(
            rollout_id="20260702t235959z-x",
            steps=[(0, "a"), (1, "b")],
        )
        state.mark_step_running(0, started_at="t0")
        state.mark_step_done(0, finished_at="t1", inputs_hash="h0")

        path = tmp_path / "state.json"
        state.save(path)
        loaded = RolloutState.load(path)
        assert loaded == state

    def test_load_rejects_wrong_schema_version(self, tmp_path: Path) -> None:
        path = tmp_path / "state.json"
        path.write_text(json.dumps({
            "version": 999,
            "rollout_id": "rid",
            "status": "running",
            "current_step": None,
            "steps": [],
        }))
        with pytest.raises(ValueError, match=r"unsupported state\.json version"):
            RolloutState.load(path)

    def test_current_running_step(self) -> None:
        state = RolloutState.new(
            rollout_id="rid", steps=[(0, "a"), (1, "b"), (2, "c")],
        )
        state.mark_step_running(0, started_at="t0")
        state.mark_step_done(0, finished_at="t0d", inputs_hash="h0")
        state.mark_step_running(1, started_at="t1")
        assert state.current_running_step() == 1

        state.mark_step_done(1, finished_at="t1d", inputs_hash="h1")
        assert state.current_running_step() is None

    def test_needs_verify_for_running_step(self) -> None:
        """Resume: a step in `running` needs verify() before we decide."""
        state = RolloutState.new(
            rollout_id="rid", steps=[(0, "a"), (1, "b")],
        )
        state.mark_step_running(0, started_at="t0")
        state.mark_step_verifying(0)
        assert state.steps[0].state is StepState.VERIFYING

    def test_reset_step_to_running_for_retry(self) -> None:
        """After verify says MISMATCH, we retry the run."""
        state = RolloutState.new(
            rollout_id="rid", steps=[(0, "a")],
        )
        state.mark_step_running(0, started_at="t0")
        state.mark_step_verifying(0)
        state.reset_step_for_retry(0, started_at="t1")
        assert state.steps[0].state is StepState.RUNNING
        assert state.steps[0].started_at == "t1"
