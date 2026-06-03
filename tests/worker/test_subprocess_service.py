import json
import subprocess
import tempfile
import unittest
from io import StringIO
from pathlib import Path
from unittest.mock import patch

from sqlalchemy.pool import StaticPool

from agentic_data_platform.artifacts.store import ArtifactPersistence, LocalArtifactStore
from agentic_data_platform.domain.run_records import (
    BenchmarkTaskInstance,
    EvaluatorConfig,
    EvaluatorResult,
    JudgeConfig,
    ModelConfig,
    RunRecord,
    RunStatus,
    RunnerConfig,
)
from agentic_data_platform.persistence.database import create_database_engine, session_scope
from agentic_data_platform.persistence.migrations import upgrade_database
from agentic_data_platform.persistence.repositories import IdentityRepository, ProjectRepository, RunRepository
from agentic_data_platform.sandbox.docker_terminal import DockerOwnedContainerCleanupResult
from agentic_data_platform.service.config import load_service_settings
from agentic_data_platform.worker.executors import FixtureTerminalBenchmarkExecutor
from agentic_data_platform.worker.service import (
    SubprocessRunWorker,
    build_configured_worker,
    execute_claimed_run,
    main,
)


class SubprocessRunWorkerTest(unittest.TestCase):
    def setUp(self):
        self.engine = create_database_engine(
            "sqlite+pysqlite:///:memory:",
            connect_args={"check_same_thread": False},
            poolclass=StaticPool,
        )
        upgrade_database(self.engine)
        with session_scope(self.engine) as session:
            IdentityRepository(session).create_team(
                team_id="pilot-project",
                name="pilot group",
            )
            IdentityRepository(session).create_user(
                user_id="[REDACTED_OWNER]",
                email="[REDACTED_OWNER]@example.com",
                display_name="[REDACTED_OWNER]",
                team_id="pilot-project",
            )
            ProjectRepository(session).create_project(
                project_id="pilot-project",
                name="pilot group",
                owner_team_id="pilot-project",
                created_by_user_id="[REDACTED_OWNER]",
            )

    def tearDown(self):
        self.engine.dispose()

    def test_subprocess_worker_claims_run_and_delegates_execution_to_child_command(self):
        _create_run(self.engine, "run_subprocess_delegate_001")
        command_runner = FakeWorkerSubprocessCommandRunner(self.engine, terminal_status=RunStatus.SUCCEEDED)
        worker = SubprocessRunWorker(
            engine=self.engine,
            worker_id="worker-subprocess-test",
            command_runner=command_runner,
            timeout_seconds=123,
        )

        result = worker.run_once(request_id="req-subprocess-delegate-001")

        self.assertIsNotNone(result)
        self.assertEqual(result.status, "succeeded")
        self.assertEqual(len(command_runner.calls), 1)
        call = command_runner.calls[0]
        args = call["args"]
        self.assertEqual(call["timeout"], 123)
        self.assertIn("-m", args)
        self.assertIn("agentic_data_platform.worker.execution_child", args)
        self.assertEqual(args[args.index("--run-id") + 1], "run_subprocess_delegate_001")
        self.assertEqual(args[args.index("--worker-id") + 1], "worker-subprocess-test")
        self.assertEqual(args[args.index("--request-id") + 1], "req-subprocess-delegate-001")

    def test_subprocess_worker_records_child_start_and_completion_events(self):
        _create_run(self.engine, "run_subprocess_events_001")
        command_runner = FakeWorkerSubprocessCommandRunner(self.engine, terminal_status=RunStatus.SUCCEEDED)
        worker = SubprocessRunWorker(
            engine=self.engine,
            worker_id="worker-subprocess-events-test",
            command_runner=command_runner,
            timeout_seconds=123,
        )

        result = worker.run_once(request_id="req-subprocess-events-001")

        self.assertIsNotNone(result)
        self.assertEqual(result.status, "succeeded")
        with session_scope(self.engine) as session:
            events = RunRepository(session).list_status_events("run_subprocess_events_001")
        subprocess_events = [
            event for event in events if event.event_type.startswith("worker.subprocess_")
        ]
        self.assertEqual(
            [event.event_type for event in subprocess_events],
            ["worker.subprocess_started", "worker.subprocess_completed"],
        )
        started, completed = subprocess_events
        self.assertEqual(started.from_status, RunStatus.PROVISIONING)
        self.assertEqual(started.to_status, RunStatus.PROVISIONING)
        self.assertEqual(started.metadata["worker_id"], "worker-subprocess-events-test")
        self.assertEqual(started.metadata["execution_task_id"], "run_subprocess_events_001:attempt:1")
        self.assertEqual(started.metadata["timeout_seconds"], 123)
        self.assertEqual(started.metadata["child_entrypoint"], "agentic_data_platform.worker.execution_child")
        self.assertNotIn("child_args", started.metadata)
        self.assertEqual(completed.from_status, RunStatus.SUCCEEDED)
        self.assertEqual(completed.to_status, RunStatus.SUCCEEDED)
        self.assertEqual(completed.metadata["worker_id"], "worker-subprocess-events-test")
        self.assertEqual(completed.metadata["execution_task_id"], "run_subprocess_events_001:attempt:1")
        self.assertEqual(completed.metadata["return_code"], 0)
        self.assertEqual(completed.metadata["child_entrypoint"], "agentic_data_platform.worker.execution_child")

    def test_subprocess_worker_marks_run_failed_when_child_exits_without_terminal_result(self):
        _create_run(self.engine, "run_subprocess_child_crash_001")
        command_runner = FakeWorkerSubprocessCommandRunner(self.engine, returncode=70, terminal_status=None)
        worker = SubprocessRunWorker(
            engine=self.engine,
            worker_id="worker-subprocess-crash-test",
            command_runner=command_runner,
            timeout_seconds=123,
        )

        result = worker.run_once(request_id="req-subprocess-crash-001")

        self.assertIsNotNone(result)
        self.assertEqual(result.status, "failed")
        with session_scope(self.engine) as session:
            run = RunRepository(session).get_run("run_subprocess_child_crash_001")
        self.assertIn("Worker subprocess exited with code 70", run.failure_reason)
        self.assertEqual(run.metadata["failure"]["category"], "worker_subprocess_failed")

    def test_subprocess_worker_failure_records_redacted_child_log_tail(self):
        _create_run(self.engine, "run_subprocess_child_diagnostics_001")
        command_runner = FakeWorkerSubprocessCommandRunner(
            self.engine,
            returncode=71,
            terminal_status=None,
            stdout=("setup line\n" * 140) + "OPENAI_API_KEY=sk-subprocess-secret\nlast stdout line\n",
            stderr="Traceback line\nAuthorization: Bearer provider-token-123\nlast stderr line\n",
        )
        worker = SubprocessRunWorker(
            engine=self.engine,
            worker_id="worker-subprocess-diagnostics-test",
            command_runner=command_runner,
            timeout_seconds=123,
        )

        result = worker.run_once(request_id="req-subprocess-diagnostics-001")

        self.assertIsNotNone(result)
        self.assertEqual(result.status, "failed")
        with session_scope(self.engine) as session:
            run = RunRepository(session).get_run("run_subprocess_child_diagnostics_001")
        failure = run.metadata["failure"]
        diagnostics = failure["metadata"]["child_process"]
        rendered_failure = json.dumps(failure, sort_keys=True)
        self.assertEqual(diagnostics["return_code"], 71)
        self.assertEqual(diagnostics["stage"], "worker_subprocess_exit")
        self.assertLessEqual(len(diagnostics["stdout_tail"]), 1000)
        self.assertIn("OPENAI_API_KEY=[redacted]", diagnostics["stdout_tail"])
        self.assertIn("last stdout line", diagnostics["stdout_tail"])
        self.assertIn("Bearer [redacted]", diagnostics["stderr_tail"])
        self.assertIn("last stderr line", diagnostics["stderr_tail"])
        self.assertNotIn("sk-subprocess-secret", rendered_failure)
        self.assertNotIn("provider-token-123", rendered_failure)

    def test_subprocess_worker_timeout_records_redacted_child_log_tail(self):
        _create_run(self.engine, "run_subprocess_child_timeout_diagnostics_001")
        command_runner = TimeoutWorkerSubprocessCommandRunner(
            output=b"booting\nMODEL_PROVIDER_API_KEY=sk-timeout-secret\nlast timeout stdout\n",
            stderr=b"Authorization: Bearer timeout-token-123\nlast timeout stderr\n",
        )
        worker = SubprocessRunWorker(
            engine=self.engine,
            worker_id="worker-subprocess-timeout-diagnostics-test",
            command_runner=command_runner,
            timeout_seconds=123,
        )

        result = worker.run_once(request_id="req-subprocess-timeout-diagnostics-001")

        self.assertIsNotNone(result)
        self.assertEqual(result.status, "failed")
        with session_scope(self.engine) as session:
            run = RunRepository(session).get_run("run_subprocess_child_timeout_diagnostics_001")
        failure = run.metadata["failure"]
        diagnostics = failure["metadata"]["child_process"]
        rendered_failure = json.dumps(failure, sort_keys=True)
        self.assertIsNone(diagnostics["return_code"])
        self.assertEqual(diagnostics["stage"], "worker_subprocess_timeout")
        self.assertIn("MODEL_PROVIDER_API_KEY=[redacted]", diagnostics["stdout_tail"])
        self.assertIn("last timeout stdout", diagnostics["stdout_tail"])
        self.assertIn("Bearer [redacted]", diagnostics["stderr_tail"])
        self.assertIn("last timeout stderr", diagnostics["stderr_tail"])
        self.assertNotIn("sk-timeout-secret", rendered_failure)
        self.assertNotIn("timeout-token-123", rendered_failure)

    def test_subprocess_worker_marks_run_failed_when_child_returns_without_terminalizing(self):
        _create_run(self.engine, "run_subprocess_child_incomplete_001")
        command_runner = FakeWorkerSubprocessCommandRunner(self.engine, returncode=0, terminal_status=None)
        worker = SubprocessRunWorker(
            engine=self.engine,
            worker_id="worker-subprocess-incomplete-test",
            command_runner=command_runner,
            timeout_seconds=123,
        )

        result = worker.run_once(request_id="req-subprocess-incomplete-001")

        self.assertIsNotNone(result)
        self.assertEqual(result.status, "failed")
        with session_scope(self.engine) as session:
            run = RunRepository(session).get_run("run_subprocess_child_incomplete_001")
        self.assertIn("Worker subprocess exited without saving a terminal run result", run.failure_reason)
        self.assertEqual(run.metadata["failure"]["category"], "worker_subprocess_failed")
        diagnostics = run.metadata["failure"]["metadata"]["child_process"]
        self.assertEqual(diagnostics["stage"], "worker_subprocess_incomplete_result")
        self.assertIn("child complete", diagnostics["stdout_tail"])

    def test_subprocess_worker_terminates_child_when_run_is_canceled(self):
        _create_run(self.engine, "run_subprocess_cancel_001")
        command_runner = CancelingManagedCommandRunner(self.engine)
        worker = SubprocessRunWorker(
            engine=self.engine,
            worker_id="worker-subprocess-cancel-test",
            command_runner=command_runner,
            timeout_seconds=123,
            cancel_poll_interval_seconds=0.001,
        )

        result = worker.run_once(request_id="req-subprocess-cancel-001")

        self.assertIsNotNone(result)
        self.assertEqual(result.status, "canceled")
        self.assertEqual(command_runner.process.terminated, True)
        self.assertEqual(command_runner.process.killed, False)
        with session_scope(self.engine) as session:
            run = RunRepository(session).get_run("run_subprocess_cancel_001")
        self.assertEqual(run.status, RunStatus.CANCELED)

    def test_execute_claimed_run_saves_executor_result_in_child_boundary(self):
        _create_run(self.engine, "run_subprocess_child_execute_001")
        with session_scope(self.engine) as session:
            RunRepository(session).claim_next_queued_run(
                worker_id="worker-child-boundary-test",
                request_id="req-child-claim-001",
            )
            execution_task_id = RunRepository(session).current_execution_task_id(
                "run_subprocess_child_execute_001"
            )
        with tempfile.TemporaryDirectory() as temp_dir:
            result = execute_claimed_run(
                engine=self.engine,
                worker_id="worker-child-boundary-test",
                run_id="run_subprocess_child_execute_001",
                execution_task_id=execution_task_id,
                request_id="req-child-execute-001",
                executor=FixtureTerminalBenchmarkExecutor(
                    artifact_persistence=ArtifactPersistence(LocalArtifactStore(Path(temp_dir))),
                ),
            )

        self.assertEqual(result.status, "succeeded")
        with session_scope(self.engine) as session:
            loaded = RunRepository(session).get_run("run_subprocess_child_execute_001")
        self.assertEqual(loaded.status, RunStatus.SUCCEEDED)
        self.assertEqual(len(loaded.trajectory), 1)

    def test_execute_claimed_run_skips_executor_when_execution_task_is_stale(self):
        _create_run(self.engine, "run_subprocess_child_stale_001")
        with session_scope(self.engine) as session:
            repository = RunRepository(session)
            repository.claim_next_queued_run(
                worker_id="worker-child-stale-test",
                request_id="req-child-stale-claim-001",
            )
            stale_execution_task_id = repository.current_execution_task_id(
                "run_subprocess_child_stale_001"
            )
            repository.cancel_run(
                "run_subprocess_child_stale_001",
                reason="operator canceled stale child",
                request_id="req-child-stale-cancel-001",
            )
            repository.retry_run(
                "run_subprocess_child_stale_001",
                reason="retry after stale child",
                request_id="req-child-stale-retry-001",
            )

        result = execute_claimed_run(
            engine=self.engine,
            worker_id="worker-child-stale-test",
            run_id="run_subprocess_child_stale_001",
            execution_task_id=stale_execution_task_id,
            request_id="req-child-stale-execute-001",
            executor=ExecutorThatMustNotRun(),
        )

        self.assertEqual(result.status, "queued")
        with session_scope(self.engine) as session:
            loaded = RunRepository(session).get_run("run_subprocess_child_stale_001")
            events = RunRepository(session).list_status_events("run_subprocess_child_stale_001")
        self.assertEqual(loaded.status, RunStatus.QUEUED)
        event_types = [event.event_type for event in events]
        self.assertEqual(
            event_types,
            ["run.created", "run.claimed", "run.canceled", "run.retried"],
        )
        self.assertNotIn("worker.subprocess_completed", event_types)

    def test_execute_claimed_run_skips_duplicate_delivery_after_first_child_locks_task(self):
        _create_run(self.engine, "run_subprocess_child_duplicate_001")
        with session_scope(self.engine) as session:
            repository = RunRepository(session)
            repository.claim_next_queued_run(
                worker_id="worker-child-duplicate-test",
                request_id="req-child-duplicate-claim-001",
            )
            execution_task_id = repository.current_execution_task_id(
                "run_subprocess_child_duplicate_001"
            )

        executor = ExecutorThatAttemptsDuplicateDelivery(
            engine=self.engine,
            worker_id="worker-child-duplicate-test",
            run_id="run_subprocess_child_duplicate_001",
            execution_task_id=execution_task_id,
        )
        result = execute_claimed_run(
            engine=self.engine,
            worker_id="worker-child-duplicate-test",
            run_id="run_subprocess_child_duplicate_001",
            execution_task_id=execution_task_id,
            request_id="req-child-duplicate-execute-001",
            executor=executor,
        )

        self.assertEqual(result.status, "succeeded")
        self.assertIsNotNone(executor.duplicate_result)
        self.assertEqual(executor.duplicate_result.status, "provisioning")
        with session_scope(self.engine) as session:
            loaded = RunRepository(session).get_run("run_subprocess_child_duplicate_001")
            events = RunRepository(session).list_status_events("run_subprocess_child_duplicate_001")
        self.assertEqual(loaded.status, RunStatus.SUCCEEDED)
        self.assertEqual(
            [event.event_type for event in events],
            [
                "run.created",
                "run.claimed",
                "run.started",
                "run.evaluating",
                "evaluator.completed",
                "run.succeeded",
            ],
        )

    def test_subprocess_worker_does_not_fail_new_attempt_when_child_task_is_stale(self):
        _create_run(self.engine, "run_subprocess_parent_stale_001")
        command_runner = StaleExecutionTaskCommandRunner(self.engine)
        worker = SubprocessRunWorker(
            engine=self.engine,
            worker_id="worker-subprocess-stale-test",
            command_runner=command_runner,
            timeout_seconds=123,
        )

        result = worker.run_once(request_id="req-subprocess-stale-001")

        self.assertIsNotNone(result)
        self.assertEqual(result.status, "queued")
        self.assertEqual(len(command_runner.calls), 1)
        args = command_runner.calls[0]["args"]
        self.assertIn("--execution-task-id", args)
        self.assertEqual(
            args[args.index("--execution-task-id") + 1],
            "run_subprocess_parent_stale_001:attempt:1",
        )
        with session_scope(self.engine) as session:
            loaded = RunRepository(session).get_run("run_subprocess_parent_stale_001")
            events = RunRepository(session).list_status_events("run_subprocess_parent_stale_001")
            current_execution_task_id = RunRepository(session).current_execution_task_id(
                "run_subprocess_parent_stale_001"
            )
        self.assertEqual(loaded.status, RunStatus.QUEUED)
        self.assertEqual(current_execution_task_id, "run_subprocess_parent_stale_001:attempt:2")
        event_types = [event.event_type for event in events]
        self.assertEqual(
            event_types,
            [
                "run.created",
                "run.claimed",
                "worker.heartbeat",
                "worker.subprocess_started",
                "run.canceled",
                "run.retried",
            ],
        )
        self.assertNotIn("worker.subprocess_completed", event_types)

    def test_configured_subprocess_worker_does_not_build_parent_executor(self):
        settings = load_service_settings(
            {
                "DATABASE_URL": "sqlite+pysqlite:///:memory:",
                "WORKER_SUBPROCESS_ISOLATION_ENABLED": "true",
                "WORKER_SUBPROCESS_TIMEOUT_SECONDS": "456",
            }
        )

        with patch(
            "agentic_data_platform.worker.service.build_configured_executor",
            side_effect=AssertionError("parent should not build execution resources"),
        ):
            worker = build_configured_worker(
                settings,
                worker_id="worker-subprocess-configured-test",
            )

        self.assertIsInstance(worker, SubprocessRunWorker)
        self.assertEqual(worker.timeout_seconds, 456)

    def test_worker_cli_can_cleanup_owned_docker_containers_for_run(self):
        cleanup_result = DockerOwnedContainerCleanupResult(
            run_id="run_cli_cleanup_001",
            attempt_id="run_cli_cleanup_001:attempt:1",
            container_ids=["container-one"],
            removed_container_ids=["container-one"],
            list_exit_code=0,
            removal_exit_code=0,
        )

        with patch("agentic_data_platform.worker.service.DockerOwnedContainerCleaner") as cleaner_cls:
            cleaner_cls.return_value.cleanup_run.return_value = cleanup_result
            output = StringIO()
            with patch("sys.stdout", output):
                exit_code = main(
                    [
                        "--cleanup-run-containers",
                        "run_cli_cleanup_001",
                        "--cleanup-attempt-id",
                        "run_cli_cleanup_001:attempt:1",
                    ]
                )

        self.assertEqual(exit_code, 0)
        cleaner_cls.return_value.cleanup_run.assert_called_once_with(
            run_id="run_cli_cleanup_001",
            attempt_id="run_cli_cleanup_001:attempt:1",
        )
        payload = json.loads(output.getvalue())
        self.assertEqual(payload["run_id"], "run_cli_cleanup_001")
        self.assertEqual(payload["attempt_id"], "run_cli_cleanup_001:attempt:1")
        self.assertEqual(payload["removed_container_ids"], ["container-one"])


def _create_run(engine, run_id: str) -> None:
    payload = _run_create_payload(run_id)
    with session_scope(engine) as session:
        RunRepository(session).create_run(
            RunRecord.create(
                run_id=run_id,
                project_id=payload["project_id"],
                owner_team=payload["owner_team"],
                created_by_user_id=payload["created_by_user_id"],
                task=BenchmarkTaskInstance(**payload["task"]),
                model=ModelConfig(**payload["model"]),
                runner=RunnerConfig(**payload["runner"]),
                evaluator_configs=[
                    EvaluatorConfig(
                        evaluator_id=item["evaluator_id"],
                        mode=item["mode"],
                        judge=JudgeConfig(**item["judge"]) if item.get("judge") else None,
                    )
                    for item in payload["evaluators"]
                ],
                metadata=payload["metadata"],
            )
        )


def _run_create_payload(run_id: str) -> dict:
    return {
        "run_id": run_id,
        "project_id": "pilot-project",
        "owner_team": "pilot group",
        "created_by_user_id": "[REDACTED_OWNER]",
        "task": {
            "benchmark_suite": "SkillLearnBench",
            "benchmark_version": "git:cxcscmu/SkillLearnBench@abc123",
            "task_family": "spreadsheet-from-documents",
            "instance_id": "conference-expense-03",
            "source_uri": "https://github.com/cxcscmu/SkillLearnBench",
            "input_artifact_refs": ["s3://agentic-data-shared dev/benchmarks/skilllearnbench/input.tar.zst"],
            "required_artifacts": ["trajectory", "workspace_snapshot", "evaluator_report"],
            "metadata": {"instruction": "Read receipts and create receipts.xlsx."},
        },
        "model": {
            "provider": "mock-api",
            "model_name": "scripted-terminal-agent",
            "mode": "api",
            "prompt_template_version": "terminal-agent-v0",
        },
        "runner": {
            "kind": "original_benchmark",
            "sandbox_backend": "docker_terminal",
            "image": "python:3.12-slim",
            "entrypoint": ["python", "-m", "agentic_data_platform.benchmark_wrappers.skilllearnbench"],
            "internet_access": True,
            "resource_limits": {"cpu": 2, "memory_gib": 8, "timeout_seconds": 3600},
            "metadata": {"runner_contract": "skilllearnbench-original-wrapper-v0"},
        },
        "evaluators": [
            {
                "evaluator_id": "mock-judge-v0",
                "mode": "llm_judge",
                "judge": {
                    "provider": "mock",
                    "model_name": "deterministic-judge",
                    "rubric_version": "latent-skill-v0",
                },
            }
        ],
        "metadata": {"worker_fixture_commands": ["python solve.py"]},
    }


class FakeWorkerSubprocessCommandRunner:
    def __init__(
        self,
        engine,
        *,
        returncode: int = 0,
        terminal_status: RunStatus | None = RunStatus.SUCCEEDED,
        stdout: str = "child complete\n",
        stderr: str = "",
    ) -> None:
        self.engine = engine
        self.returncode = returncode
        self.terminal_status = terminal_status
        self.stdout = stdout
        self.stderr = stderr
        self.calls: list[dict[str, object]] = []

    def run(
        self,
        args: list[str],
        *,
        timeout: int,
        env: dict[str, str] | None = None,
    ) -> subprocess.CompletedProcess[str]:
        self.calls.append({"args": args, "timeout": timeout, "env": env})
        if self.terminal_status is not None:
            run_id = args[args.index("--run-id") + 1]
            worker_id = args[args.index("--worker-id") + 1]
            request_id = args[args.index("--request-id") + 1]
            with session_scope(self.engine) as session:
                repository = RunRepository(session)
                run = repository.get_run(run_id)
                run.transition_to(RunStatus.RUNNING)
                if self.terminal_status is RunStatus.SUCCEEDED:
                    run.transition_to(RunStatus.EVALUATING)
                elif self.terminal_status is RunStatus.FAILED:
                    run.failure_reason = "child failed"
                run.transition_to(self.terminal_status)
                repository.save_worker_result(run, worker_id=worker_id, request_id=request_id)
        return subprocess.CompletedProcess(
            args=args,
            returncode=self.returncode,
            stdout=self.stdout,
            stderr=self.stderr,
        )


class TimeoutWorkerSubprocessCommandRunner:
    def __init__(self, *, output, stderr) -> None:
        self.output = output
        self.stderr = stderr
        self.calls: list[dict[str, object]] = []

    def run(
        self,
        args: list[str],
        *,
        timeout: int,
        env: dict[str, str] | None = None,
    ) -> subprocess.CompletedProcess[str]:
        self.calls.append({"args": args, "timeout": timeout, "env": env})
        raise subprocess.TimeoutExpired(
            cmd=args,
            timeout=timeout,
            output=self.output,
            stderr=self.stderr,
        )


class StaleExecutionTaskCommandRunner:
    def __init__(self, engine) -> None:
        self.engine = engine
        self.calls: list[dict[str, object]] = []

    def run(
        self,
        args: list[str],
        *,
        timeout: int,
        env: dict[str, str] | None = None,
    ) -> subprocess.CompletedProcess[str]:
        self.calls.append({"args": args, "timeout": timeout, "env": env})
        run_id = args[args.index("--run-id") + 1]
        with session_scope(self.engine) as session:
            repository = RunRepository(session)
            repository.cancel_run(
                run_id,
                reason="operator canceled stale child",
                request_id="req-stale-child-cancel-001",
            )
            repository.retry_run(
                run_id,
                reason="retry after stale child",
                request_id="req-stale-child-retry-001",
            )
        return subprocess.CompletedProcess(
            args=args,
            returncode=0,
            stdout="stale child skipped\n",
            stderr="",
        )


class ExecutorThatMustNotRun:
    def execute(self, run):
        raise AssertionError("stale execution task should not execute")


class ExecutorThatAttemptsDuplicateDelivery:
    def __init__(self, *, engine, worker_id: str, run_id: str, execution_task_id: str) -> None:
        self.engine = engine
        self.worker_id = worker_id
        self.run_id = run_id
        self.execution_task_id = execution_task_id
        self.duplicate_result = None

    def execute(self, run):
        self.duplicate_result = execute_claimed_run(
            engine=self.engine,
            worker_id=self.worker_id,
            run_id=self.run_id,
            execution_task_id=self.execution_task_id,
            request_id="req-child-duplicate-delivery-001",
            executor=ExecutorThatMustNotRun(),
        )
        run.transition_to(RunStatus.RUNNING)
        run.transition_to(RunStatus.EVALUATING)
        run.attach_evaluator_result(
            EvaluatorResult(
                evaluator_id="mock-judge-v0",
                status="completed",
                score=1.0,
                metrics={"task_success": True},
                verbal_feedback="Primary execution completed after duplicate delivery was skipped.",
                judge=JudgeConfig(
                    provider="mock",
                    model_name="deterministic-judge",
                    rubric_version="latent-skill-v0",
                ),
                artifact_refs=[],
            )
        )
        run.transition_to(RunStatus.SUCCEEDED)
        return run


class CancelingManagedCommandRunner:
    def __init__(self, engine) -> None:
        self.engine = engine
        self.process = CancelingManagedProcess(engine)

    def start(
        self,
        args: list[str],
        *,
        env: dict[str, str] | None = None,
    ):
        self.process.args = args
        return self.process

    def run(
        self,
        args: list[str],
        *,
        timeout: int,
        env: dict[str, str] | None = None,
    ) -> subprocess.CompletedProcess[str]:
        raise AssertionError("managed runner should use start for cancellation monitoring")


class CancelingManagedProcess:
    def __init__(self, engine) -> None:
        self.engine = engine
        self.args: list[str] = []
        self.returncode: int | None = None
        self.terminated = False
        self.killed = False
        self._canceled = False

    def poll(self) -> int | None:
        if not self._canceled:
            self._canceled = True
            run_id = self.args[self.args.index("--run-id") + 1]
            with session_scope(self.engine) as session:
                RunRepository(session).cancel_run(
                    run_id,
                    reason="user canceled while child was running",
                )
        return self.returncode

    def terminate(self) -> None:
        self.terminated = True
        self.returncode = -15

    def kill(self) -> None:
        self.killed = True
        self.returncode = -9

    def communicate(self, timeout: float | None = None):
        return "", ""


if __name__ == "__main__":
    unittest.main()
