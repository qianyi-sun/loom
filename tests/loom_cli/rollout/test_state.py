"""State machine + JSON persistence for the rollout driver (#340).

Tests cover the tiny per-step FSM (not_started → running → verifying → done | failed)
and the top-level RolloutState that owns the sequence of steps + persists to
`state.json` in the evidence directory.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from loom_cli.rollout.operator.redaction import rollout_redaction_scope
from loom_cli.rollout.state import (
    DriverRecord,
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
            StepState.NOT_STARTED,
            StepState.RUNNING,
            StepState.VERIFYING,
            StepState.FAILED,
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
        rec = StepRecord(number=4, name="publish-images")
        assert rec.state is StepState.NOT_STARTED
        assert rec.inputs_hash is None
        assert rec.started_at is None
        assert rec.finished_at is None
        assert rec.error is None

    def test_to_dict_roundtrip(self) -> None:
        rec = StepRecord(
            number=4,
            name="publish-images",
            state=StepState.DONE,
            inputs_hash="abc123",
            started_at="2026-07-02T22:00:00Z",
            finished_at="2026-07-02T22:00:05Z",
        )
        data = rec.to_dict()
        assert data == {
            "number": 4,
            "name": "publish-images",
            "state": "done",
            "inputs_hash": "abc123",
            "started_at": "2026-07-02T22:00:00Z",
            "finished_at": "2026-07-02T22:00:05Z",
            "error": None,
        }
        assert StepRecord.from_dict(data) == rec

    def test_from_dict_rejects_unknown_state(self) -> None:
        with pytest.raises(ValueError, match="unknown step state"):
            StepRecord.from_dict(
                {
                    "number": 4,
                    "name": "x",
                    "state": "chugging",
                    "inputs_hash": None,
                    "started_at": None,
                    "finished_at": None,
                    "error": None,
                }
            )


class TestRolloutState:
    """Top-level state.json handling."""

    def test_new_starts_all_not_started(self) -> None:
        state = RolloutState.new(
            rollout_id="20260702t235959z-staging-abc",
            steps=[(0, "resolve-target"), (1, "worktree")],
        )
        assert state.rollout_id == "20260702t235959z-staging-abc"
        assert state.status == "running"
        assert state.current_step is None
        assert [s.name for s in state.steps] == ["resolve-target", "worktree"]
        assert all(s.state is StepState.NOT_STARTED for s in state.steps)

    def test_mark_step_running_sets_current(self) -> None:
        state = RolloutState.new(
            rollout_id="rid",
            steps=[(0, "a"), (1, "b")],
        )
        state.mark_step_running(0, started_at="2026-07-02T22:00:00Z")
        assert state.current_step == 0
        assert state.steps[0].state is StepState.RUNNING
        assert state.steps[0].started_at == "2026-07-02T22:00:00Z"

    def test_mark_step_done_bumps_current_step_on_next_running(self) -> None:
        state = RolloutState.new(
            rollout_id="rid",
            steps=[(0, "a"), (1, "b")],
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
            rollout_id="rid",
            steps=[(0, "a"), (1, "b")],
        )
        state.mark_step_running(0, started_at="t0")
        state.mark_step_failed(0, finished_at="t1", error="docker daemon unreachable")
        assert state.steps[0].state is StepState.FAILED
        assert state.steps[0].error == "docker daemon unreachable"
        assert state.status == "failed"

    def test_all_done_marks_rollout_done(self) -> None:
        state = RolloutState.new(
            rollout_id="rid",
            steps=[(0, "a"), (1, "b")],
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

    def test_driver_record_persists_with_state(self, tmp_path: Path) -> None:
        state = RolloutState.new(
            rollout_id="20260702t235959z-x",
            steps=[(0, "a")],
        )
        state.mark_driver_active(
            DriverRecord(
                pid=1234,
                hostname="platform-dev",
                boot_id="boot-1",
                started_at="2026-07-07T00:00:00Z",
                updated_at="2026-07-07T00:00:05Z",
            )
        )

        path = tmp_path / "state.json"
        state.save(path)
        raw = json.loads(path.read_text())
        loaded = RolloutState.load(path)

        assert raw["driver"] == {
            "pid": 1234,
            "hostname": "platform-dev",
            "boot_id": "boot-1",
            "started_at": "2026-07-07T00:00:00Z",
            "updated_at": "2026-07-07T00:00:05Z",
            "attempt_number": None,
            "attempt_operator": None,
            "attempt_uid": None,
        }
        assert loaded.driver == state.driver

    def test_version_two_persists_all_six_attribution_fields_and_driver_attempt(
        self,
        tmp_path: Path,
    ) -> None:
        state = RolloutState.new(
            rollout_id="staging-abcdef1",
            steps=[(0, "a")],
            request_id="stg-20260713-abcdef12",
            initiating_operator="hongjian",
            initiating_uid=2002,
            attempt_number=2,
            attempt_operator="devansh",
            attempt_uid=2501,
        )
        state.mark_driver_active(
            DriverRecord(
                pid=1234,
                hostname="platform-dev",
                boot_id="boot-1",
                started_at="2026-07-13T20:00:00Z",
                updated_at="2026-07-13T20:00:05Z",
                attempt_number=2,
                attempt_operator="devansh",
                attempt_uid=2501,
            )
        )

        path = tmp_path / "state.json"
        state.save(path)
        raw = json.loads(path.read_text())

        assert raw["version"] == 2
        assert {
            key: raw[key]
            for key in (
                "request_id",
                "initiating_operator",
                "initiating_uid",
                "attempt_number",
                "attempt_operator",
                "attempt_uid",
            )
        } == {
            "request_id": "stg-20260713-abcdef12",
            "initiating_operator": "hongjian",
            "initiating_uid": 2002,
            "attempt_number": 2,
            "attempt_operator": "devansh",
            "attempt_uid": 2501,
        }
        assert raw["driver"]["attempt_number"] == 2
        assert raw["driver"]["attempt_operator"] == "devansh"
        assert raw["driver"]["attempt_uid"] == 2501
        assert RolloutState.load(path) == state

    def test_version_one_loads_without_attribution_and_next_save_is_version_two(
        self,
        tmp_path: Path,
    ) -> None:
        path = tmp_path / "state.json"
        path.write_text(
            json.dumps(
                {
                    "version": 1,
                    "rollout_id": "legacy-rollout",
                    "status": "running",
                    "current_step": None,
                    "driver": None,
                    "steps": [],
                }
            ),
            encoding="utf-8",
        )

        state = RolloutState.load(path)

        assert state.request_id is None
        assert state.initiating_operator is None
        assert state.initiating_uid is None
        assert state.attempt_number is None
        assert state.attempt_operator is None
        assert state.attempt_uid is None
        state.save(path)
        rewritten = json.loads(path.read_text())
        assert rewritten["version"] == 2
        assert rewritten["request_id"] is None

    def test_load_rejects_wrong_schema_version(self, tmp_path: Path) -> None:
        path = tmp_path / "state.json"
        path.write_text(
            json.dumps(
                {
                    "version": 999,
                    "rollout_id": "rid",
                    "status": "running",
                    "current_step": None,
                    "steps": [],
                }
            )
        )
        with pytest.raises(ValueError, match=r"unsupported state\.json version"):
            RolloutState.load(path)

    @pytest.mark.parametrize("version", ["2", True, 2.0])
    def test_load_rejects_non_integer_version(self, version: object) -> None:
        with pytest.raises(ValueError, match="version"):
            RolloutState.from_dict(
                {
                    "version": version,
                    "rollout_id": "rid",
                    "status": "running",
                    "current_step": None,
                    "driver": None,
                    "request_id": None,
                    "initiating_operator": None,
                    "initiating_uid": None,
                    "attempt_number": None,
                    "attempt_operator": None,
                    "attempt_uid": None,
                    "steps": [],
                }
            )

    def test_version_two_rejects_partial_or_coerced_attribution(self) -> None:
        base = {
            "version": 2,
            "rollout_id": "rid",
            "status": "running",
            "current_step": None,
            "driver": None,
            "request_id": "stg-20260713-abcdef12",
            "initiating_operator": "hongjian",
            "initiating_uid": 2002,
            "attempt_number": 1,
            "attempt_operator": "hongjian",
            "attempt_uid": 2002,
            "steps": [],
        }
        for key, invalid in (
            ("initiating_uid", "2002"),
            ("attempt_number", True),
            ("attempt_uid", None),
            ("attempt_operator", None),
        ):
            payload = dict(base)
            payload[key] = invalid
            with pytest.raises(ValueError, match="attribution"):
                RolloutState.from_dict(payload)

    def test_driver_record_rejects_partial_or_coerced_attempt_attribution(self) -> None:
        base = {
            "pid": 123,
            "hostname": "platform-dev",
            "boot_id": "boot",
            "started_at": "t0",
            "updated_at": "t1",
            "attempt_number": 1,
            "attempt_operator": "hongjian",
            "attempt_uid": 2002,
        }
        for key, invalid in (
            ("attempt_number", "1"),
            ("attempt_uid", True),
            ("attempt_operator", None),
        ):
            payload = dict(base)
            payload[key] = invalid
            with pytest.raises(ValueError, match="attempt attribution"):
                DriverRecord.from_dict(payload)

    @pytest.mark.parametrize(
        ("attempt_number", "attempt_operator", "attempt_uid"),
        [
            (None, None, None),
            (2, "devansh", 2501),
        ],
    )
    def test_broker_state_rejects_unattributed_or_mismatched_active_driver(
        self,
        attempt_number: int | None,
        attempt_operator: str | None,
        attempt_uid: int | None,
    ) -> None:
        payload = {
            "version": 2,
            "rollout_id": "staging-abcdef1",
            "status": "running",
            "current_step": 0,
            "request_id": "stg-20260713-abcdef12",
            "initiating_operator": "hongjian",
            "initiating_uid": 2002,
            "attempt_number": 1,
            "attempt_operator": "hongjian",
            "attempt_uid": 2002,
            "driver": {
                "pid": 123,
                "hostname": "platform-dev",
                "boot_id": "boot",
                "started_at": "t0",
                "updated_at": "t1",
                "attempt_number": attempt_number,
                "attempt_operator": attempt_operator,
                "attempt_uid": attempt_uid,
            },
            "steps": [],
        }

        with pytest.raises(ValueError, match=r"driver.*attempt attribution"):
            RolloutState.from_dict(payload)

    def test_save_rechecks_broker_driver_attempt_attribution(self, tmp_path: Path) -> None:
        state = RolloutState.new(
            rollout_id="staging-abcdef1",
            steps=[],
            request_id="stg-20260713-abcdef12",
            initiating_operator="hongjian",
            initiating_uid=2002,
            attempt_number=1,
            attempt_operator="hongjian",
            attempt_uid=2002,
        )
        state.driver = DriverRecord(
            pid=123,
            hostname="platform-dev",
            boot_id="boot",
            started_at="t0",
            updated_at="t1",
            attempt_number=2,
            attempt_operator="devansh",
            attempt_uid=2501,
        )

        with pytest.raises(ValueError, match=r"driver.*attempt attribution"):
            state.save(tmp_path / "state.json")

    def test_state_save_redacts_step_error_at_persistence_boundary(
        self,
        tmp_path: Path,
    ) -> None:
        secret = "opaque-state-error-secret"
        state = RolloutState.new(rollout_id="rid", steps=[(0, "step")])
        state.mark_step_failed(0, finished_at="t1", error=f"failure {secret}")

        with rollout_redaction_scope((secret,)):
            state.save(tmp_path / "state.json")

        raw = (tmp_path / "state.json").read_text(encoding="utf-8")
        assert secret not in raw
        assert "[REDACTED:known-secret]" in raw

    def test_current_running_step(self) -> None:
        state = RolloutState.new(
            rollout_id="rid",
            steps=[(0, "a"), (1, "b"), (2, "c")],
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
            rollout_id="rid",
            steps=[(0, "a"), (1, "b")],
        )
        state.mark_step_running(0, started_at="t0")
        state.mark_step_verifying(0)
        assert state.steps[0].state is StepState.VERIFYING

    def test_reset_step_to_running_for_retry(self) -> None:
        """After verify says MISMATCH, we retry the run."""
        state = RolloutState.new(
            rollout_id="rid",
            steps=[(0, "a")],
        )
        state.mark_step_running(0, started_at="t0")
        state.mark_step_verifying(0)
        state.reset_step_for_retry(0, started_at="t1")
        assert state.steps[0].state is StepState.RUNNING
        assert state.steps[0].started_at == "t1"

    def test_reset_step_for_retry_restores_top_level_running_status(self) -> None:
        state = RolloutState.new(
            rollout_id="rid",
            steps=[(0, "a"), (1, "b")],
        )
        state.mark_step_failed(0, finished_at="t1", error="planned failure")

        state.reset_step_for_retry(0, started_at="t2")

        assert state.status == "running"
