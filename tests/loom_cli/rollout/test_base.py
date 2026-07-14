"""Step Protocol + BaseStep tests (#340)."""

from __future__ import annotations

from pathlib import Path

import pytest

from loom_cli.rollout.base_context_fixture import make_ctx
from loom_cli.rollout.evidence import EvidenceDirectory
from loom_cli.rollout.operator.redaction import rollout_redaction_scope
from loom_cli.rollout.steps.base import (
    BaseStep,
    RunResult,
    Step,
    VerifyOutcome,
    step_result_dict,
)


class _NoOpStep(BaseStep):
    number = 42
    name = "noop"

    def _run_impl(self, ctx, step_dir):
        self.write_stdout(step_dir, "ran\n")
        return RunResult(exit_code=0, summary="did nothing")


class _MatchStep(BaseStep):
    """Overrides verify to MATCH — used by driver tests."""

    number = 43
    name = "matcher"

    def _run_impl(self, ctx, step_dir):
        return RunResult(exit_code=0)

    def _verify_impl(self, ctx, step_dir):
        return VerifyOutcome.MATCH


class TestVerifyOutcome:
    def test_enum_values(self) -> None:
        assert VerifyOutcome.MATCH.value == "match"
        assert VerifyOutcome.MISMATCH.value == "mismatch"
        assert VerifyOutcome.UNKNOWN.value == "unknown"


class TestRunResult:
    def test_success_predicate(self) -> None:
        assert RunResult(exit_code=0).is_success()
        assert not RunResult(exit_code=1).is_success()


class TestBaseStepInputsHash:
    def test_deterministic(self, tmp_path: Path) -> None:
        step = _NoOpStep()
        ctx = make_ctx(tmp_path)
        h1 = step.inputs_hash(ctx)
        h2 = step.inputs_hash(ctx)
        assert h1 == h2
        assert len(h1) == 64  # sha256 hex

    def test_changes_when_input_changes(self, tmp_path: Path) -> None:
        step = _NoOpStep()
        ctx_a = make_ctx(tmp_path, image_tag="staging-a")
        ctx_b = make_ctx(tmp_path, image_tag="staging-b")
        assert step.inputs_hash(ctx_a) != step.inputs_hash(ctx_b)


class TestBaseStepIsDone:
    def test_returns_false_when_result_missing(self, tmp_path: Path) -> None:
        step = _NoOpStep()
        ctx = make_ctx(tmp_path)
        ev = EvidenceDirectory(tmp_path, "rid")
        ev.ensure()
        step_dir = ev.step_dir(42, "noop")
        assert not step.is_done(ctx, step_dir)

    def test_returns_true_when_result_matches_hash(self, tmp_path: Path) -> None:
        step = _NoOpStep()
        ctx = make_ctx(tmp_path)
        ev = EvidenceDirectory(tmp_path, "rid")
        ev.ensure()
        step_dir = ev.step_dir(42, "noop")
        ev.write_step_result(
            step_dir,
            {
                "state": "done",
                "inputs_hash": step.inputs_hash(ctx),
            },
        )
        assert step.is_done(ctx, step_dir)

    def test_returns_false_when_hash_stale(self, tmp_path: Path) -> None:
        step = _NoOpStep()
        ctx = make_ctx(tmp_path)
        ev = EvidenceDirectory(tmp_path, "rid")
        ev.ensure()
        step_dir = ev.step_dir(42, "noop")
        ev.write_step_result(
            step_dir,
            {
                "state": "done",
                "inputs_hash": "wrong-hash",
            },
        )
        assert not step.is_done(ctx, step_dir)

    def test_returns_false_when_state_not_done(self, tmp_path: Path) -> None:
        step = _NoOpStep()
        ctx = make_ctx(tmp_path)
        ev = EvidenceDirectory(tmp_path, "rid")
        ev.ensure()
        step_dir = ev.step_dir(42, "noop")
        ev.write_step_result(
            step_dir,
            {
                "state": "running",
                "inputs_hash": step.inputs_hash(ctx),
            },
        )
        assert not step.is_done(ctx, step_dir)


class TestBaseStepDefaults:
    def test_default_verify_is_unknown(self, tmp_path: Path) -> None:
        step = _NoOpStep()
        ctx = make_ctx(tmp_path)
        ev = EvidenceDirectory(tmp_path, "rid")
        ev.ensure()
        step_dir = ev.step_dir(42, "noop")
        assert step.verify(ctx, step_dir) is VerifyOutcome.UNKNOWN

    def test_run_dispatches_to_impl(self, tmp_path: Path) -> None:
        step = _NoOpStep()
        ctx = make_ctx(tmp_path)
        ev = EvidenceDirectory(tmp_path, "rid")
        ev.ensure()
        step_dir = ev.step_dir(42, "noop")
        result = step.run(ctx, step_dir)
        assert result.exit_code == 0
        assert step_dir.stdout_path().read_text() == "ran\n"

    def test_base_step_run_impl_raises(self, tmp_path: Path) -> None:
        class NoImpl(BaseStep):
            number = 1
            name = "no-impl"

        step = NoImpl()
        ctx = make_ctx(tmp_path)
        ev = EvidenceDirectory(tmp_path, "rid")
        ev.ensure()
        step_dir = ev.step_dir(1, "no-impl")
        with pytest.raises(NotImplementedError, match="no-impl"):
            step.run(ctx, step_dir)

    def test_diagnostic_text_helpers_redact_exact_known_secret(
        self,
        tmp_path: Path,
    ) -> None:
        secret = "opaque-base-step-secret"
        step = _NoOpStep()
        ev = EvidenceDirectory(tmp_path, "rid")
        ev.ensure()
        step_dir = ev.step_dir(42, "noop")

        with rollout_redaction_scope((secret,)):
            step.write_stdout(step_dir, f"stdout {secret}\n")
            step.write_stderr(step_dir, f"stderr {secret}\n")

        persisted = step_dir.stdout_path().read_text() + step_dir.stderr_path().read_text()
        assert secret not in persisted

    def test_functional_text_and_binary_artifacts_are_not_rewritten(
        self,
        tmp_path: Path,
    ) -> None:
        payload = "functional-artifact-payload"
        step = _NoOpStep()
        ev = EvidenceDirectory(tmp_path, "rid")
        ev.ensure()
        step_dir = ev.step_dir(42, "noop")

        with rollout_redaction_scope((payload,)):
            step.write_artifact(step_dir, "rendered.yaml", payload)
            step.write_artifact(step_dir, "archive.bin", payload.encode())

        assert step_dir.artifact_path("rendered.yaml").read_text() == payload
        assert step_dir.artifact_path("archive.bin").read_bytes() == payload.encode()


class TestBaseStepConformsToProtocol:
    def test_noop_step_is_a_step(self) -> None:
        step = _NoOpStep()
        assert isinstance(step, Step)


class TestStepResultDict:
    def test_shape(self, tmp_path: Path) -> None:
        step = _NoOpStep()
        result = step_result_dict(
            step=step,
            state="done",
            inputs_hash="abc",
            started_at="t0",
            finished_at="t1",
            summary="ok",
        )
        assert result == {
            "number": 42,
            "name": "noop",
            "state": "done",
            "inputs_hash": "abc",
            "started_at": "t0",
            "finished_at": "t1",
            "exit_code": 0,
            "error": None,
            "summary": "ok",
            "artifacts": {},
        }

    def test_run_result_and_payload_redact_diagnostic_fields(self) -> None:
        secret = "opaque-result-secret"
        step = _NoOpStep()

        with rollout_redaction_scope((secret,)):
            run_result = RunResult(
                exit_code=1,
                summary=f"summary {secret}",
                error=f"error {secret}",
            )
            result = step_result_dict(
                step=step,
                state="failed",
                inputs_hash="abc",
                started_at="t0",
                finished_at="t1",
                exit_code=1,
                error=f"error {secret}",
                summary=f"summary {secret}",
                artifacts={"diagnostic": f"artifact {secret}"},
            )

        assert secret not in run_result.summary
        assert secret not in (run_result.error or "")
        assert secret not in str(result)
