"""Rollout driver orchestrator tests (#340)."""

from __future__ import annotations

import io
import json
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
from loom_cli.rollout.steps.s99_summary import SummaryStep

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


class _InputsHashRaisingStep(BaseStep):
    def __init__(self, *, run_behavior: str, secret: str) -> None:
        self.number = 0
        self.name = f"inputs-hash-{run_behavior}"
        self.run_behavior = run_behavior
        self.secret = secret
        self.inputs_hash_calls = 0

    def inputs_hash(self, ctx):
        self.inputs_hash_calls += 1
        raise RuntimeError(f"could not hash {self.secret}")

    def _run_impl(self, ctx, step_dir):
        if self.run_behavior == "raises":
            raise RuntimeError("planned run exception")
        if self.run_behavior == "nonzero":
            return RunResult(exit_code=7, error="planned nonzero result")
        return RunResult(exit_code=0, summary="fresh success")


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
    def test_driver_persists_state_and_logs_through_evidence_apis(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        ctx = make_ctx(tmp_path)
        ev = EvidenceDirectory(tmp_path, "test-rid")
        state_documents: list[dict[str, object]] = []
        log_messages: list[str] = []
        original_write_state = ev.write_state
        original_append_driver_log = ev.append_driver_log

        def capture_state(document: dict[str, object]) -> None:
            state_documents.append(document)
            original_write_state(document)

        def capture_log(text: str) -> None:
            log_messages.append(text)
            original_append_driver_log(text)

        monkeypatch.setattr(ev, "write_state", capture_state)
        monkeypatch.setattr(ev, "append_driver_log", capture_log)

        assert run_rollout(ctx, [_AlwaysOKStep(0, "a")], ev, io.StringIO()) == 0

        assert state_documents
        assert state_documents[-1]["status"] == "done"
        assert state_documents[-1]["driver"] is None
        assert any(message.startswith("[run  ]") for message in log_messages)
        assert any(message.startswith("[done ]") for message in log_messages)

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

    def test_broker_inputs_include_original_request_but_exclude_current_attempt(
        self,
        tmp_path: Path,
    ) -> None:
        manifest = tmp_path / "backup-manifest.json"
        manifest.write_text("{}\n", encoding="utf-8")
        ctx = make_ctx(
            tmp_path,
            request_id="stg-20260713-abcdef12",
            initiating_operator="hongjian",
            initiating_uid=2002,
            attempt_number=2,
            attempt_operator="devansh",
            attempt_uid=2501,
            backup_manifest_path=manifest,
            backup_manifest_sha256="a" * 64,
            runner_config_sha256="b" * 64,
        )

        inputs = ctx.to_inputs_dict()

        assert inputs["request_id"] == "stg-20260713-abcdef12"
        assert inputs["initiating_operator"] == "hongjian"
        assert inputs["initiating_uid"] == 2002
        assert inputs["backup_manifest_path"] == str(manifest)
        assert inputs["backup_manifest_sha256"] == "a" * 64
        assert inputs["runner_config_sha256"] == "b" * 64
        assert "attempt_number" not in inputs
        assert "attempt_operator" not in inputs
        assert "attempt_uid" not in inputs
        assert str(ctx.admin_token_source) not in json.dumps(inputs)

    def test_manual_inputs_keep_historical_shape_and_token_sources(
        self,
        tmp_path: Path,
    ) -> None:
        ctx = make_ctx(tmp_path)

        inputs = ctx.to_inputs_dict()

        assert inputs["admin_token_source"] == ctx.admin_token_source
        assert "request_id" not in inputs
        assert "backup_manifest_path" not in inputs
        assert "backup_manifest_sha256" not in inputs
        assert "runner_config_sha256" not in inputs

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

    def test_all_persisted_diagnostics_redact_secret_sentinels(
        self,
        tmp_path: Path,
    ) -> None:
        secret = "opaque-line-one\nopaque-line-two"
        token_file = tmp_path / "admin-token"
        token_file.write_text(secret, encoding="utf-8")
        token_file.chmod(0o600)
        pem_body = "b3BlbnNzaC1zZW50aW5lbC1ib2R5"
        credential_password = "credential-url-password"

        class SecretDiagnosticStep(BaseStep):
            number = 0
            name = "secret-diagnostic"

            def _run_impl(self, ctx, step_dir):
                escaped = json.dumps({"error": secret})
                self.write_stdout(step_dir, f"stdout {secret}\n{escaped}\n")
                self.write_stderr(
                    step_dir,
                    "password: plain-stderr-password\n"
                    "-----BEGIN OPENSSH PRIVATE KEY-----\n"
                    f"{pem_body}\n"
                    "-----END OPENSSH PRIVATE KEY-----\n",
                )
                return RunResult(
                    exit_code=0,
                    summary=(
                        f"{secret} {escaped} "
                        f"postgresql://loom:{credential_password}@db.example/loom"
                    ),
                )

        ctx = make_ctx(
            tmp_path,
            admin_token_source=f"file:{token_file}",
            request_id="stg-20260713-abcdef12",
            initiating_operator="hongjian",
            initiating_uid=2002,
            attempt_number=1,
            attempt_operator="hongjian",
            attempt_uid=2002,
        )
        ev = EvidenceDirectory(tmp_path, "test-rid")
        stream = io.StringIO()

        rc = run_rollout(ctx, [SecretDiagnosticStep(), SummaryStep()], ev, stream)

        assert rc == 0
        rendered = stream.getvalue()
        for path in ev.path.rglob("*"):
            if path.is_file():
                rendered += path.read_text(encoding="utf-8")
        for forbidden in (
            "opaque-line-one",
            "opaque-line-two",
            pem_body,
            credential_password,
            "plain-stderr-password",
            str(token_file),
        ):
            assert forbidden not in rendered

    def test_raised_step_exception_scrubs_every_diagnostic_sink(
        self,
        tmp_path: Path,
    ) -> None:
        exact_secret = "opaque-raised-step-secret"
        credential_password = "raised-url-password"
        pem_body = "cmFpc2VkLXBlbS1ib2R5"
        token_file = tmp_path / "admin-token"
        token_file.write_text(exact_secret, encoding="utf-8")
        token_file.chmod(0o600)
        payload = (
            f"{exact_secret}\n"
            f"redis://:{credential_password}@cache.example/0\n"
            "-----BEGIN RSA PRIVATE KEY-----\n"
            f"{pem_body}\n"
        )

        class RaisingDiagnosticStep(BaseStep):
            number = 0
            name = "raising-diagnostic"

            def _run_impl(self, ctx, step_dir):
                step_dir.stdout_path().write_text(payload, encoding="utf-8")
                step_dir.stderr_path().write_text(payload, encoding="utf-8")
                raise RuntimeError(payload)

        ctx = make_ctx(
            tmp_path,
            admin_token_source=f"file:{token_file}",
            request_id="stg-20260713-abcdef12",
            initiating_operator="hongjian",
            initiating_uid=2002,
            attempt_number=1,
            attempt_operator="hongjian",
            attempt_uid=2002,
        )
        ev = EvidenceDirectory(tmp_path, "test-rid")
        stream = io.StringIO()

        assert run_rollout(ctx, [RaisingDiagnosticStep()], ev, stream) == 1

        rendered = stream.getvalue()
        for path in ev.path.rglob("*"):
            if path.is_file():
                rendered += path.read_text(encoding="utf-8")
        for forbidden in (exact_secret, credential_password, pem_body):
            assert forbidden not in rendered

    @pytest.mark.parametrize(
        "callback",
        ["is_done", "resume_verify", "post_run_verify"],
    )
    def test_callback_exception_fails_safely_and_clears_driver(
        self,
        tmp_path: Path,
        callback: str,
    ) -> None:
        secret = "opaque-known-secret-before-truncate"
        leaked_prefix = secret[:10]
        token_file = tmp_path / "admin-token"
        token_file.write_text(secret, encoding="utf-8")
        token_file.chmod(0o600)
        payload = ("x" * 1992) + secret + " after-secret"

        class CallbackRaisingStep(BaseStep):
            number = 0
            name = "callback-raising"

            def is_done(self, ctx, step_dir):
                if callback == "is_done":
                    raise RuntimeError(payload)
                return super().is_done(ctx, step_dir)

            def _verify_impl(self, ctx, step_dir):
                if callback in {"resume_verify", "post_run_verify"}:
                    raise RuntimeError(payload)
                return VerifyOutcome.UNKNOWN

            def _run_impl(self, ctx, step_dir):
                return RunResult(exit_code=0, summary="callback run completed")

        ctx = make_ctx(tmp_path, admin_token_source=f"file:{token_file}")
        ev = EvidenceDirectory(tmp_path, "test-rid")
        ev.ensure()
        ev.write_inputs(ctx.to_inputs_dict())
        state = RolloutState.new(
            rollout_id="test-rid",
            steps=[(0, "callback-raising")],
        )
        if callback == "is_done":
            state.mark_step_done(
                0,
                finished_at="2026-07-13T00:00:00Z",
                inputs_hash="previous-inputs-hash",
            )
        elif callback == "resume_verify":
            state.mark_step_running(0, started_at="2026-07-13T00:00:00Z")
        state.save(ev.state_path())
        stream = io.StringIO()

        rc = run_rollout(ctx, [CallbackRaisingStep()], ev, stream)

        assert rc == 1
        loaded = RolloutState.load(ev.state_path())
        assert loaded.status == "failed"
        assert loaded.driver is None
        assert loaded.steps[0].state is StepState.FAILED
        assert loaded.steps[0].error is not None
        assert len(loaded.steps[0].error) <= 2000
        step_dir = ev.existing_step_dir(0, "callback-raising")
        assert step_dir is not None
        result = ev.read_step_result(step_dir)
        assert result is not None
        assert result["state"] == "failed"

        rendered = stream.getvalue()
        for path in ev.path.rglob("*"):
            if path.is_file():
                rendered += path.read_text(encoding="utf-8")
        assert leaked_prefix not in rendered
        assert "Traceback (most recent call last)" not in rendered

    def test_is_done_inputs_hash_exception_does_not_reenter_failing_hash(
        self,
        tmp_path: Path,
    ) -> None:
        secret = "opaque-inputs-hash-secret"
        token_file = tmp_path / "admin-token"
        token_file.write_text(secret, encoding="utf-8")
        token_file.chmod(0o600)

        class InputsHashRaisingStep(BaseStep):
            number = 0
            name = "inputs-hash-raising"

            def inputs_hash(self, ctx):
                raise RuntimeError(f"could not hash {secret}")

            def _run_impl(self, ctx, step_dir):
                return RunResult(exit_code=0)

        ctx = make_ctx(tmp_path, admin_token_source=f"file:{token_file}")
        ev = EvidenceDirectory(tmp_path, "test-rid")
        ev.ensure()
        ev.write_inputs(ctx.to_inputs_dict())
        state = RolloutState.new(
            rollout_id="test-rid",
            steps=[(0, "inputs-hash-raising")],
        )
        state.mark_step_done(
            0,
            finished_at="2026-07-13T00:00:00Z",
            inputs_hash="previous-inputs-hash",
        )
        state.save(ev.state_path())
        step_dir = ev.step_dir(0, "inputs-hash-raising")
        ev.write_step_result(
            step_dir,
            {
                "state": "done",
                "inputs_hash": "previous-inputs-hash",
                "started_at": "2026-07-13T00:00:00Z",
                "finished_at": "2026-07-13T00:00:01Z",
                "exit_code": 0,
            },
        )
        stream = io.StringIO()

        rc = run_rollout(ctx, [InputsHashRaisingStep()], ev, stream)

        assert rc == 1
        loaded = RolloutState.load(ev.state_path())
        assert loaded.status == "failed"
        assert loaded.driver is None
        result = ev.read_step_result(step_dir)
        assert result is not None
        assert result["state"] == "failed"
        assert result["inputs_hash"] == "unavailable-after-callback-exception"
        rendered = stream.getvalue()
        for path in ev.path.rglob("*"):
            if path.is_file():
                rendered += path.read_text(encoding="utf-8")
        assert secret not in rendered

    @pytest.mark.parametrize(
        ("run_behavior", "expected_rc", "expected_hash_calls", "expected_hash"),
        [
            ("success", 1, 1, "unavailable-after-callback-exception"),
            ("raises", 1, 0, "unavailable-after-step-failure"),
            ("nonzero", 7, 0, "unavailable-after-step-failure"),
        ],
    )
    def test_inputs_hash_failure_is_terminal_safe_and_not_reentered(
        self,
        tmp_path: Path,
        run_behavior: str,
        expected_rc: int,
        expected_hash_calls: int,
        expected_hash: str,
    ) -> None:
        secret = f"opaque-inputs-hash-{run_behavior}-secret"
        token_file = tmp_path / "admin-token"
        token_file.write_text(secret, encoding="utf-8")
        token_file.chmod(0o600)
        ctx = make_ctx(tmp_path, admin_token_source=f"file:{token_file}")
        ev = EvidenceDirectory(tmp_path, "test-rid")
        step = _InputsHashRaisingStep(run_behavior=run_behavior, secret=secret)
        stream = io.StringIO()

        rc = run_rollout(ctx, [step], ev, stream)

        assert rc == expected_rc
        assert step.inputs_hash_calls == expected_hash_calls
        loaded = RolloutState.load(ev.state_path())
        assert loaded.status == "failed"
        assert loaded.driver is None
        assert loaded.steps[0].state is StepState.FAILED
        step_dir = ev.existing_step_dir(0, step.name)
        assert step_dir is not None
        result = ev.read_step_result(step_dir)
        assert result is not None
        assert result["state"] == "failed"
        assert result["inputs_hash"] == expected_hash

        rendered = stream.getvalue()
        for path in ev.path.rglob("*"):
            if path.is_file():
                rendered += path.read_text(encoding="utf-8")
        assert secret not in rendered
        assert "Traceback (most recent call last)" not in rendered


class TestResume:
    @staticmethod
    def _broker_ctx(tmp_path: Path):
        manifest = tmp_path / "backup-manifest.json"
        manifest.write_text("{}\n", encoding="utf-8")
        return make_ctx(
            tmp_path,
            request_id="stg-20260713-abcdef12",
            initiating_operator="hongjian",
            initiating_uid=2002,
            attempt_number=2,
            attempt_operator="devansh",
            attempt_uid=2501,
            backup_manifest_path=manifest,
            backup_manifest_sha256="a" * 64,
            runner_config_sha256="b" * 64,
            resume=True,
        )

    @staticmethod
    def _valid_broker_state(ctx, rollout_id: str) -> RolloutState:
        state = RolloutState.new(
            rollout_id=rollout_id,
            steps=[(0, "a")],
            request_id=ctx.request_id,
            initiating_operator=ctx.initiating_operator,
            initiating_uid=ctx.initiating_uid,
            attempt_number=1,
            attempt_operator=ctx.initiating_operator,
            attempt_uid=ctx.initiating_uid,
        )
        state.mark_step_failed(
            0,
            finished_at="2026-07-13T00:00:00Z",
            error="previous failure",
        )
        return state

    @staticmethod
    def _write_valid_manual_resume(
        ctx,
        evidence: EvidenceDirectory,
        steps: list[BaseStep],
    ) -> None:
        evidence.ensure()
        evidence.write_inputs(ctx.to_inputs_dict())
        state = RolloutState.new(
            rollout_id=evidence.rollout_id,
            steps=[(step.number, step.name) for step in steps],
        )
        if steps:
            state.mark_step_failed(
                steps[0].number,
                finished_at="2026-07-13T00:00:00Z",
                error="previous failure",
            )
        state.save(evidence.state_path())

    def test_broker_resume_refuses_absent_evidence_without_creating_it(
        self,
        tmp_path: Path,
    ) -> None:
        ctx = self._broker_ctx(tmp_path)
        ev = EvidenceDirectory(tmp_path, "test-rid")

        with pytest.raises(DriverError, match=r"intact inputs\.json and state\.json"):
            run_rollout(ctx, [_AlwaysOKStep(0, "a")], ev, io.StringIO())

        assert not ev.path.exists()

    @pytest.mark.parametrize(
        "artifact_case",
        [
            "inputs_missing",
            "state_missing",
            "inputs_symlink",
            "state_symlink",
            "inputs_invalid_json",
            "state_invalid_json",
            "inputs_wrong_shape",
            "state_wrong_schema",
            "state_invalid_driver_shape",
            "state_invalid_status",
            "state_invalid_current_step",
            "state_invalid_steps_shape",
        ],
    )
    def test_broker_resume_refuses_missing_symlink_or_invalid_evidence_without_writes(
        self,
        tmp_path: Path,
        artifact_case: str,
    ) -> None:
        ctx = self._broker_ctx(tmp_path)
        ev = EvidenceDirectory(tmp_path, "test-rid")
        ev.path.mkdir(parents=True)
        valid_inputs = json.dumps(ctx.to_inputs_dict(), sort_keys=True) + "\n"
        valid_state = (
            json.dumps(
                self._valid_broker_state(ctx, ev.rollout_id).to_dict(),
                sort_keys=True,
            )
            + "\n"
        )
        inputs_path = ev.inputs_path()
        state_path = ev.state_path()

        if artifact_case != "inputs_missing":
            inputs_path.write_text(valid_inputs, encoding="utf-8")
        if artifact_case != "state_missing":
            state_path.write_text(valid_state, encoding="utf-8")

        if artifact_case == "inputs_symlink":
            inputs_path.unlink()
            target = tmp_path / "outside-inputs.json"
            target.write_text(valid_inputs, encoding="utf-8")
            inputs_path.symlink_to(target)
        elif artifact_case == "state_symlink":
            state_path.unlink()
            target = tmp_path / "outside-state.json"
            target.write_text(valid_state, encoding="utf-8")
            state_path.symlink_to(target)
        elif artifact_case == "inputs_invalid_json":
            inputs_path.write_text("{ invalid", encoding="utf-8")
        elif artifact_case == "state_invalid_json":
            state_path.write_text("{ invalid", encoding="utf-8")
        elif artifact_case == "inputs_wrong_shape":
            inputs_path.write_text("[]\n", encoding="utf-8")
        elif artifact_case == "state_wrong_schema":
            state_path.write_text('{"version": 999}\n', encoding="utf-8")
        elif artifact_case == "state_invalid_driver_shape":
            document = json.loads(valid_state)
            document["driver"] = []
            state_path.write_text(json.dumps(document) + "\n", encoding="utf-8")
        elif artifact_case == "state_invalid_status":
            document = json.loads(valid_state)
            document["status"] = "mystery"
            state_path.write_text(json.dumps(document) + "\n", encoding="utf-8")
        elif artifact_case == "state_invalid_current_step":
            document = json.loads(valid_state)
            document["current_step"] = "0"
            state_path.write_text(json.dumps(document) + "\n", encoding="utf-8")
        elif artifact_case == "state_invalid_steps_shape":
            document = json.loads(valid_state)
            document["steps"] = {"unexpected": "mapping"}
            state_path.write_text(json.dumps(document) + "\n", encoding="utf-8")

        before = {
            path.name: (
                ("symlink", str(path.readlink()))
                if path.is_symlink()
                else ("file", path.read_bytes())
            )
            for path in ev.path.iterdir()
        }

        with pytest.raises(DriverError, match=r"intact inputs\.json and state\.json"):
            run_rollout(ctx, [_AlwaysOKStep(0, "a")], ev, io.StringIO())

        after = {
            path.name: (
                ("symlink", str(path.readlink()))
                if path.is_symlink()
                else ("file", path.read_bytes())
            )
            for path in ev.path.iterdir()
        }
        assert after == before
        assert not (ev.path / "logs").exists()

    def test_manual_resume_still_initializes_missing_evidence(self, tmp_path: Path) -> None:
        ctx = make_ctx(tmp_path, resume=True)
        ev = EvidenceDirectory(tmp_path, "test-rid")

        assert run_rollout(ctx, [_AlwaysOKStep(0, "a")], ev, io.StringIO()) == 0

        assert ev.inputs_path().is_file()
        assert ev.state_path().is_file()

    def test_manual_resume_consumes_preloaded_state_without_reopening_it(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        ctx = make_ctx(tmp_path, resume=True)
        ev = EvidenceDirectory(tmp_path, "test-rid")
        steps = [_AlwaysOKStep(0, "a")]
        self._write_valid_manual_resume(ctx, ev, steps)

        def refuse_path_reopen(cls, path):
            raise AssertionError(f"state path was reopened: {path}")

        monkeypatch.setattr(RolloutState, "load", classmethod(refuse_path_reopen))

        assert run_rollout(ctx, steps, ev, io.StringIO()) == 0
        assert steps[0].run_calls == 1

    def test_manual_resume_rejects_state_swapped_after_safe_discovery(
        self,
        tmp_path: Path,
    ) -> None:
        ctx = make_ctx(tmp_path, resume=True)
        ev = EvidenceDirectory(tmp_path, "20260714t120000z-staging-abc123")
        steps = [_AlwaysOKStep(0, "a")]
        self._write_valid_manual_resume(ctx, ev, steps)

        discovered = EvidenceDirectory.find_in_progress(tmp_path, ctx.image_tag)
        assert discovered is not None
        assert discovered.rollout_id == ev.rollout_id

        outside = tmp_path / "attacker-state.json"
        attacker_state = RolloutState.new(
            rollout_id="attacker-rid",
            steps=[(0, "a")],
        )
        outside.write_text(
            json.dumps(attacker_state.to_dict()) + "\n",
            encoding="utf-8",
        )
        ev.state_path().unlink()
        ev.state_path().symlink_to(outside)

        with pytest.raises(
            DriverError,
            match=r"manual resume requires intact inputs\.json and state\.json",
        ):
            run_rollout(ctx, steps, discovered, io.StringIO())

        assert ev.state_path().is_symlink()
        assert ev.state_path().readlink() == outside
        assert steps[0].run_calls == 0

    @pytest.mark.parametrize(
        "artifact_case",
        [
            "inputs_missing",
            "state_missing",
            "inputs_symlink",
            "state_symlink",
            "inputs_invalid_json",
            "state_invalid_json",
            "inputs_wrong_shape",
            "state_wrong_schema",
            "state_invalid_status",
            "state_wrong_rollout_id",
            "state_wrong_inventory",
        ],
    )
    def test_manual_resume_rejects_unsafe_or_inconsistent_snapshot_without_writes(
        self,
        tmp_path: Path,
        artifact_case: str,
    ) -> None:
        secret = "manual-resume-secret-MUST-NOT-LEAK"
        token_file = tmp_path / "admin-token"
        token_file.write_text(secret, encoding="utf-8")
        token_file.chmod(0o600)
        ctx = make_ctx(
            tmp_path,
            resume=True,
            admin_token_source=f"file:{token_file}",
        )
        ev = EvidenceDirectory(tmp_path, "test-rid")
        steps = [_AlwaysOKStep(0, "a")]
        self._write_valid_manual_resume(ctx, ev, steps)
        inputs_path = ev.inputs_path()
        state_path = ev.state_path()

        if artifact_case == "inputs_missing":
            inputs_path.unlink()
        elif artifact_case == "state_missing":
            state_path.unlink()
        elif artifact_case == "inputs_symlink":
            inputs_path.unlink()
            outside = tmp_path / "outside-inputs.json"
            outside.write_text(json.dumps(ctx.to_inputs_dict()) + "\n", encoding="utf-8")
            inputs_path.symlink_to(outside)
        elif artifact_case == "state_symlink":
            state_path.unlink()
            outside = tmp_path / "outside-state.json"
            attacker_state = RolloutState.new(
                rollout_id="attacker-rid",
                steps=[(0, "a")],
            )
            outside.write_text(
                json.dumps(attacker_state.to_dict()) + "\n",
                encoding="utf-8",
            )
            state_path.symlink_to(outside)
        elif artifact_case == "inputs_invalid_json":
            inputs_path.write_text("{ invalid", encoding="utf-8")
        elif artifact_case == "state_invalid_json":
            state_path.write_text("{ invalid", encoding="utf-8")
        elif artifact_case == "inputs_wrong_shape":
            inputs_path.write_text("[]\n", encoding="utf-8")
        else:
            document = json.loads(state_path.read_text(encoding="utf-8"))
            if artifact_case == "state_wrong_schema":
                document["version"] = 999
            elif artifact_case == "state_invalid_status":
                document["status"] = "mystery"
            elif artifact_case == "state_wrong_rollout_id":
                document["rollout_id"] = "attacker-rid"
            elif artifact_case == "state_wrong_inventory":
                document["steps"][0]["name"] = "different-step"
            state_path.write_text(json.dumps(document) + "\n", encoding="utf-8")

        def snapshot(path: Path) -> tuple[str, object]:
            if not path.exists() and not path.is_symlink():
                return ("missing", None)
            if path.is_symlink():
                return ("symlink", str(path.readlink()))
            return ("file", path.read_bytes())

        before = {
            "inputs": snapshot(inputs_path),
            "state": snapshot(state_path),
            "driver_log": snapshot(ev.driver_log_path()),
        }

        with pytest.raises(
            DriverError,
            match=r"manual resume requires intact inputs\.json and state\.json",
        ) as exc_info:
            run_rollout(ctx, steps, ev, io.StringIO())

        after = {
            "inputs": snapshot(inputs_path),
            "state": snapshot(state_path),
            "driver_log": snapshot(ev.driver_log_path()),
        }
        assert after == before
        assert steps[0].run_calls == 0
        assert secret not in str(exc_info.value)
        assert "Traceback (most recent call last)" not in str(exc_info.value)

    def test_broker_resume_rejects_unattributed_legacy_state(
        self,
        tmp_path: Path,
    ) -> None:
        ctx = make_ctx(
            tmp_path,
            request_id="stg-20260713-abcdef12",
            initiating_operator="hongjian",
            initiating_uid=2002,
            attempt_number=2,
            attempt_operator="devansh",
            attempt_uid=2501,
            resume=True,
        )
        ev = EvidenceDirectory(tmp_path, "test-rid")
        ev.ensure()
        ev.write_inputs(ctx.to_inputs_dict())
        ev.state_path().write_text(
            json.dumps(
                {
                    "version": 1,
                    "rollout_id": "test-rid",
                    "status": "failed",
                    "current_step": 0,
                    "driver": None,
                    "steps": [
                        {
                            "number": 0,
                            "name": "a",
                            "state": "failed",
                            "inputs_hash": None,
                            "started_at": "t0",
                            "finished_at": "t1",
                            "error": "legacy failure",
                        }
                    ],
                }
            ),
            encoding="utf-8",
        )

        with pytest.raises(DriverError, match="unattributed legacy state"):
            run_rollout(ctx, [_AlwaysOKStep(0, "a")], ev, io.StringIO())

    def test_broker_resume_rejects_attribution_mismatch(
        self,
        tmp_path: Path,
    ) -> None:
        ctx = make_ctx(
            tmp_path,
            request_id="stg-20260713-abcdef12",
            initiating_operator="hongjian",
            initiating_uid=2002,
            attempt_number=2,
            attempt_operator="devansh",
            attempt_uid=2501,
            resume=True,
        )
        ev = EvidenceDirectory(tmp_path, "test-rid")
        ev.ensure()
        ev.write_inputs(ctx.to_inputs_dict())
        state = RolloutState.new(
            rollout_id="test-rid",
            steps=[(0, "a")],
            request_id=ctx.request_id,
            initiating_operator="qianyi",
            initiating_uid=1000,
            attempt_number=1,
            attempt_operator="hongjian",
            attempt_uid=2002,
        )
        state.mark_step_failed(0, finished_at="t1", error="failure")
        state.save(ev.state_path())

        with pytest.raises(DriverError, match="state attribution does not match"):
            run_rollout(ctx, [_AlwaysOKStep(0, "a")], ev, io.StringIO())

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
            lambda _ctx: DriverRecord(
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
            lambda _ctx: DriverRecord(
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
