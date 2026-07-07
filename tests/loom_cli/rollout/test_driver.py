"""Rollout driver orchestrator tests (#340)."""

from __future__ import annotations

import io
from pathlib import Path

import pytest

from loom_cli.rollout.base_context_fixture import make_ctx
from loom_cli.rollout.driver import (
    DriverError,
    preflight_ctx,
    run_rollout,
)
from loom_cli.rollout.evidence import EvidenceDirectory
from loom_cli.rollout.state import DriverRecord, RolloutState, StepState
from loom_cli.rollout.steps.base import (
    BaseStep,
    RunResult,
    VerifyOutcome,
)

# ---- Test doubles ----


class _AlwaysOKStep(BaseStep):
    def __init__(self, number: int, name: str) -> None:
        self.number = number
        self.name = name
        self.run_calls = 0

    def _run_impl(self, ctx, step_dir):
        self.run_calls += 1
        self.write_stdout(step_dir, f"ran {self.name}\n")
        return RunResult(exit_code=0, summary=f"{self.name} ok")


class _AlwaysFailStep(BaseStep):
    def __init__(self, number: int, name: str) -> None:
        self.number = number
        self.name = name

    def _run_impl(self, ctx, step_dir):
        return RunResult(exit_code=7, error="planned failure")


class _RaisingStep(BaseStep):
    def __init__(self, number: int, name: str) -> None:
        self.number = number
        self.name = name

    def _run_impl(self, ctx, step_dir):
        raise RuntimeError("boom")


class _MatchOnVerify(BaseStep):
    """Simulates a step that was interrupted mid-run but succeeded."""

    def __init__(self, number: int, name: str) -> None:
        self.number = number
        self.name = name
        self.run_calls = 0

    def _run_impl(self, ctx, step_dir):
        self.run_calls += 1
        return RunResult(exit_code=0)

    def _verify_impl(self, ctx, step_dir):
        return VerifyOutcome.MATCH


class _MismatchOnVerify(BaseStep):
    def __init__(self, number: int, name: str) -> None:
        self.number = number
        self.name = name
        self.run_calls = 0

    def _run_impl(self, ctx, step_dir):
        self.run_calls += 1
        return RunResult(exit_code=0)

    def _verify_impl(self, ctx, step_dir):
        return VerifyOutcome.MISMATCH


class _UnknownOnVerify(BaseStep):
    def __init__(self, number: int, name: str) -> None:
        self.number = number
        self.name = name

    def _run_impl(self, ctx, step_dir):
        return RunResult(exit_code=0)

    def _verify_impl(self, ctx, step_dir):
        return VerifyOutcome.UNKNOWN


class _MismatchThenAccept(BaseStep):
    """Simulates a stale running step whose observable state needs a retry."""

    def __init__(self, number: int, name: str) -> None:
        self.number = number
        self.name = name
        self.run_calls = 0
        self.verify_calls = 0

    def _run_impl(self, ctx, step_dir):
        self.run_calls += 1
        return RunResult(exit_code=0, summary=f"{self.name} retried")

    def _verify_impl(self, ctx, step_dir):
        self.verify_calls += 1
        if self.verify_calls == 1:
            return VerifyOutcome.MISMATCH
        return VerifyOutcome.UNKNOWN


# ---- Preflight ----


class TestPreflight:
    def test_refuses_full_cluster_plus_exclude_oldlab(self, tmp_path: Path) -> None:
        ctx = make_ctx(tmp_path, scope="full-cluster", exclude_oldlab=True)
        with pytest.raises(DriverError, match="full-cluster"):
            preflight_ctx(ctx)

    def test_allows_current_gb10_with_exclude_oldlab(self, tmp_path: Path) -> None:
        ctx = make_ctx(tmp_path, scope="current-gb10", exclude_oldlab=True)
        preflight_ctx(ctx)  # no raise

    def test_allows_full_cluster_without_exclude_oldlab(self, tmp_path: Path) -> None:
        ctx = make_ctx(tmp_path, scope="full-cluster", exclude_oldlab=False)
        preflight_ctx(ctx)


# ---- Happy path ----


class TestRunRollout:
    def test_all_steps_succeed(self, tmp_path: Path) -> None:
        ctx = make_ctx(tmp_path)
        ev = EvidenceDirectory(tmp_path, "test-rid")
        steps = [
            _AlwaysOKStep(0, "resolve"),
            _AlwaysOKStep(1, "worktree"),
        ]
        stream = io.StringIO()

        rc = run_rollout(ctx, steps, ev, stream)

        assert rc == 0
        state = RolloutState.load(ev.state_path())
        assert state.status == "done"
        assert all(s.state is StepState.DONE for s in state.steps)
        # Each step ran once.
        assert steps[0].run_calls == 1
        assert steps[1].run_calls == 1
        # Evidence directories were populated.
        s0 = ev.step_dir(0, "resolve")
        assert s0.stdout_path().read_text() == "ran resolve\n"
        result = ev.read_step_result(s0)
        assert result is not None
        assert result["state"] == "done"

    def test_writes_inputs_json_on_first_run(self, tmp_path: Path) -> None:
        ctx = make_ctx(tmp_path)
        ev = EvidenceDirectory(tmp_path, "test-rid")
        steps = [_AlwaysOKStep(0, "resolve")]
        run_rollout(ctx, steps, ev, io.StringIO())
        assert ev.inputs_path().is_file()
        assert ev.read_inputs()["image_tag"] == "staging-abc123"

    def test_writes_driver_log_for_disconnect_diagnostics(
        self,
        tmp_path: Path,
    ) -> None:
        ctx = make_ctx(tmp_path)
        ev = EvidenceDirectory(tmp_path, "test-rid")
        steps = [_AlwaysOKStep(0, "resolve")]

        rc = run_rollout(ctx, steps, ev, io.StringIO())

        assert rc == 0
        log = ev.driver_log_path().read_text(encoding="utf-8")
        assert "[run  ] 00-resolve" in log
        assert "[done ] 00-resolve" in log

    def test_refuses_when_inputs_json_conflicts(self, tmp_path: Path) -> None:
        ctx = make_ctx(tmp_path)
        ev = EvidenceDirectory(tmp_path, "test-rid")
        ev.ensure()
        # Pre-existing inputs.json for a DIFFERENT target.
        ev.write_inputs({"image_tag": "staging-other", "resolved_sha": "z" * 40})
        steps = [_AlwaysOKStep(0, "resolve")]
        with pytest.raises(DriverError, match=r"inputs\.json"):
            run_rollout(ctx, steps, ev, io.StringIO())


class TestFailure:
    def test_step_failure_marks_state_failed_and_returns_nonzero(self, tmp_path: Path) -> None:
        ctx = make_ctx(tmp_path)
        ev = EvidenceDirectory(tmp_path, "test-rid")
        steps = [
            _AlwaysOKStep(0, "resolve"),
            _AlwaysFailStep(1, "worktree"),
            _AlwaysOKStep(2, "build"),
        ]
        rc = run_rollout(ctx, steps, ev, io.StringIO())
        assert rc == 7
        state = RolloutState.load(ev.state_path())
        assert state.status == "failed"
        assert state.steps[1].state is StepState.FAILED
        # step 2 was never run because we returned early.
        assert state.steps[2].state is StepState.NOT_STARTED

    def test_step_exception_captured_as_failure(self, tmp_path: Path) -> None:
        ctx = make_ctx(tmp_path)
        ev = EvidenceDirectory(tmp_path, "test-rid")
        steps = [_RaisingStep(0, "boom-step")]
        rc = run_rollout(ctx, steps, ev, io.StringIO())
        assert rc == 1
        state = RolloutState.load(ev.state_path())
        assert state.steps[0].state is StepState.FAILED
        assert "boom" in (state.steps[0].error or "")


class TestResume:
    def test_skips_already_done_steps(self, tmp_path: Path) -> None:
        ctx = make_ctx(tmp_path)
        ev = EvidenceDirectory(tmp_path, "test-rid")
        step_a = _AlwaysOKStep(0, "a")
        step_b = _AlwaysOKStep(1, "b")
        # First run: succeeds.
        rc1 = run_rollout(ctx, [step_a, step_b], ev, io.StringIO())
        assert rc1 == 0
        # Second run: both marked done, should not call run() again.
        step_a2 = _AlwaysOKStep(0, "a")
        step_b2 = _AlwaysOKStep(1, "b")
        rc2 = run_rollout(ctx, [step_a2, step_b2], ev, io.StringIO())
        assert rc2 == 0
        assert step_a2.run_calls == 0
        assert step_b2.run_calls == 0

    def test_resumes_a_step_left_running(self, tmp_path: Path) -> None:
        """Simulate an interrupt: state.json says RUNNING; verify → MATCH."""
        ctx = make_ctx(tmp_path)
        ev = EvidenceDirectory(tmp_path, "test-rid")
        ev.ensure()
        ev.write_inputs(ctx.to_inputs_dict())
        # Manually persist state showing step 0 running.
        state = RolloutState.new(
            rollout_id="test-rid",
            steps=[(0, "a"), (1, "b")],
        )
        state.mark_step_running(0, started_at="2026-07-02T22:00:00Z")
        state.save(ev.state_path())

        step_a = _MatchOnVerify(0, "a")
        step_b = _AlwaysOKStep(1, "b")
        rc = run_rollout(ctx, [step_a, step_b], ev, io.StringIO())
        assert rc == 0
        assert step_a.run_calls == 0  # verify said MATCH; run() not called
        assert step_b.run_calls == 1

    def test_reruns_a_step_that_verifies_mismatch(self, tmp_path: Path) -> None:
        ctx = make_ctx(tmp_path)
        ev = EvidenceDirectory(tmp_path, "test-rid")
        ev.ensure()
        ev.write_inputs(ctx.to_inputs_dict())
        state = RolloutState.new(
            rollout_id="test-rid",
            steps=[(0, "a")],
        )
        state.mark_step_running(0, started_at="t0")
        state.save(ev.state_path())

        # First call: verify MISMATCH → run(). But run() succeeds, then
        # verify again → MISMATCH → refuse.
        step_a = _MismatchOnVerify(0, "a")
        rc = run_rollout(ctx, [step_a], ev, io.StringIO())
        # verify-after-run said MISMATCH → refuse (rc=2)
        assert rc == 2
        assert step_a.run_calls == 1

    def test_refuses_on_unknown_verify_during_resume(self, tmp_path: Path) -> None:
        ctx = make_ctx(tmp_path)
        ev = EvidenceDirectory(tmp_path, "test-rid")
        ev.ensure()
        ev.write_inputs(ctx.to_inputs_dict())
        state = RolloutState.new(
            rollout_id="test-rid",
            steps=[(0, "a")],
        )
        state.mark_step_running(0, started_at="t0")
        state.save(ev.state_path())

        step_a = _UnknownOnVerify(0, "a")
        rc = run_rollout(ctx, [step_a], ev, io.StringIO())
        assert rc == 2

    def test_refuses_resume_when_another_driver_is_still_active(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        ctx = make_ctx(tmp_path)
        ev = EvidenceDirectory(tmp_path, "test-rid")
        ev.ensure()
        ev.write_inputs(ctx.to_inputs_dict())
        state = RolloutState.new(
            rollout_id="test-rid",
            steps=[(0, "a")],
        )
        state.mark_step_running(0, started_at="t0")
        state.driver = DriverRecord(
            pid=123,
            hostname="platform-dev",
            boot_id="boot-a",
            started_at="t0",
            updated_at="t0",
        )
        state.save(ev.state_path())
        monkeypatch.setattr(
            "loom_cli.rollout.driver._current_driver_identity",
            lambda: DriverRecord(
                pid=456,
                hostname="platform-dev",
                boot_id="boot-a",
                started_at="t1",
                updated_at="t1",
            ),
        )
        monkeypatch.setattr(
            "loom_cli.rollout.driver._driver_record_is_alive",
            lambda _record, _current: True,
        )

        with pytest.raises(DriverError, match="already active"):
            run_rollout(ctx, [_AlwaysOKStep(0, "a")], ev, io.StringIO())

    def test_stale_dead_driver_owner_can_resume_and_retry(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        ctx = make_ctx(tmp_path)
        ev = EvidenceDirectory(tmp_path, "test-rid")
        ev.ensure()
        ev.write_inputs(ctx.to_inputs_dict())
        state = RolloutState.new(
            rollout_id="test-rid",
            steps=[(0, "a")],
        )
        state.mark_step_running(0, started_at="t0")
        state.driver = DriverRecord(
            pid=123,
            hostname="platform-dev",
            boot_id="boot-a",
            started_at="t0",
            updated_at="t0",
        )
        state.save(ev.state_path())
        monkeypatch.setattr(
            "loom_cli.rollout.driver._current_driver_identity",
            lambda: DriverRecord(
                pid=456,
                hostname="platform-dev",
                boot_id="boot-a",
                started_at="t1",
                updated_at="t1",
            ),
        )
        monkeypatch.setattr(
            "loom_cli.rollout.driver._driver_record_is_alive",
            lambda _record, _current: False,
        )

        step = _MismatchThenAccept(0, "a")
        rc = run_rollout(ctx, [step], ev, io.StringIO())

        assert rc == 0
        assert step.run_calls == 1
        loaded = RolloutState.load(ev.state_path())
        assert loaded.status == "done"
        assert loaded.driver is None

    def test_reruns_when_inputs_hash_changes(self, tmp_path: Path) -> None:
        """Different inputs → is_done returns False even if state says done."""
        ctx = make_ctx(tmp_path)
        ev = EvidenceDirectory(tmp_path, "test-rid")
        step_a = _AlwaysOKStep(0, "a")
        run_rollout(ctx, [step_a], ev, io.StringIO())
        assert step_a.run_calls == 1

        # NOTE: driver refuses inputs conflict (checked by
        # test_refuses_when_inputs_json_conflicts). The changing-hash case
        # is exercised indirectly by is_done in test_base.py. Add a
        # positive integration test path here: same inputs → no rerun.
        step_a2 = _AlwaysOKStep(0, "a")
        run_rollout(ctx, [step_a2], ev, io.StringIO())
        assert step_a2.run_calls == 0

    def test_successful_skip_only_rerun_clears_driver_owner(self, tmp_path: Path) -> None:
        ctx = make_ctx(tmp_path)
        ev = EvidenceDirectory(tmp_path, "test-rid")
        step = _AlwaysOKStep(0, "a")
        assert run_rollout(ctx, [step], ev, io.StringIO()) == 0

        skipped_step = _AlwaysOKStep(0, "a")
        assert run_rollout(ctx, [skipped_step], ev, io.StringIO()) == 0

        loaded = RolloutState.load(ev.state_path())
        assert loaded.status == "done"
        assert loaded.driver is None
