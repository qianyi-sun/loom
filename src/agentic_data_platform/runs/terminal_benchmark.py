from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Protocol

from agentic_data_platform.artifacts.store import ArtifactPersistence
from agentic_data_platform.benchmarks.adapters import BenchmarkRegistration
from agentic_data_platform.dashboard.projections import RunDashboardProjection
from agentic_data_platform.domain.run_records import (
    ArtifactRef,
    EvaluatorResult,
    JudgeConfig,
    RunRecord,
    RunStatus,
)
from agentic_data_platform.evaluation.types import EvaluatorAdapter, EvaluatorInput
from agentic_data_platform.models.providers import ModelProvider, ModelProviderContext
from agentic_data_platform.providers.errors import ProviderBoundaryError, normalize_provider_error
from agentic_data_platform.sandbox.docker_terminal import SandboxCommandResult, WorkspaceSnapshot


class TerminalSandbox(Protocol):
    def execute(self, command: str, *, cwd: str | None = None, timeout_seconds: int | None = None) -> SandboxCommandResult:
        ...

    def capture_workspace(self) -> WorkspaceSnapshot:
        ...


@dataclass(frozen=True)
class TerminalBenchmarkRunRequest:
    run_id: str
    project_id: str
    owner_team: str
    registration: BenchmarkRegistration
    model_provider: ModelProvider
    judge: JudgeConfig
    rubric_id: str
    sandbox: TerminalSandbox


@dataclass(frozen=True)
class TerminalBenchmarkRunResult:
    run: RunRecord
    dashboard: dict[str, object]


class TerminalBenchmarkRunner:
    def __init__(
        self,
        *,
        artifact_persistence: ArtifactPersistence,
        evaluator: EvaluatorAdapter,
    ) -> None:
        self.artifact_persistence = artifact_persistence
        self.evaluator = evaluator

    def run(self, request: TerminalBenchmarkRunRequest) -> TerminalBenchmarkRunResult:
        run = RunRecord.create(
            run_id=request.run_id,
            project_id=request.project_id,
            owner_team=request.owner_team,
            task=request.registration.task,
            model=request.model_provider.model,
            runner=request.registration.runner,
            metadata={"benchmark_adapter": request.registration.task.benchmark_suite},
        )
        return self.run_existing(run, request)

    def run_existing(
        self,
        run: RunRecord,
        request: TerminalBenchmarkRunRequest,
    ) -> TerminalBenchmarkRunResult:
        if run.status is not RunStatus.PROVISIONING:
            run.transition_to(RunStatus.PROVISIONING)
        run.transition_to(RunStatus.RUNNING)

        timeout_seconds = int(request.registration.runner.resource_limits.get("timeout_seconds", 3600))
        failure_reason = self._execute_model_commands(run, request, timeout_seconds=timeout_seconds)
        if failure_reason is not None:
            self._persist_execution_artifacts(run, request)
            run.failure_reason = failure_reason
            run.transition_to(RunStatus.FAILED)
            return TerminalBenchmarkRunResult(
                run=run,
                dashboard=RunDashboardProjection.from_run(run).to_dict(),
            )

        run.transition_to(RunStatus.EVALUATING)
        trajectory_ref, workspace_ref = self._persist_execution_artifacts(run, request)

        evaluator_result = self.evaluator.evaluate(
            EvaluatorInput.from_run(
                run,
                trajectory_ref=trajectory_ref,
                workspace_ref=workspace_ref,
                artifact_refs=[],
                rubric_id=request.rubric_id,
                judge=request.judge,
            )
        )
        try:
            evaluator_report_ref = self.artifact_persistence.persist_evaluator_report(
                run_id=run.run_id,
                task_instance_id=run.task.instance_id,
                result=evaluator_result,
            )
        except Exception as exc:
            evaluator_report_ref = self.artifact_persistence.failed_evaluator_report_ref(
                run_id=run.run_id,
                task_instance_id=run.task.instance_id,
                result=evaluator_result,
                error=exc,
            )
        run.attach_artifact(evaluator_report_ref)
        run.attach_evaluator_result(evaluator_result)

        if evaluator_result.status == "completed":
            run.transition_to(RunStatus.SUCCEEDED)
        else:
            run.failure_reason = evaluator_result.failure_reason
            run.transition_to(RunStatus.FAILED)

        return TerminalBenchmarkRunResult(
            run=run,
            dashboard=RunDashboardProjection.from_run(run).to_dict(),
        )

    def _persist_execution_artifacts(
        self,
        run: RunRecord,
        request: TerminalBenchmarkRunRequest,
    ) -> tuple[ArtifactRef, ArtifactRef]:
        trajectory_ref = self.artifact_persistence.persist_trajectory(
            run_id=run.run_id,
            task_instance_id=run.task.instance_id,
            turns=run.trajectory,
        )
        workspace_ref = self.artifact_persistence.persist_workspace_snapshot(
            run_id=run.run_id,
            task_instance_id=run.task.instance_id,
            snapshot=request.sandbox.capture_workspace(),
        )
        run.attach_artifact(trajectory_ref)
        run.attach_artifact(workspace_ref)
        return trajectory_ref, workspace_ref

    def _execute_model_commands(
        self,
        run: RunRecord,
        request: TerminalBenchmarkRunRequest,
        *,
        timeout_seconds: int,
    ) -> str | None:
        while True:
            try:
                command = request.model_provider.next_command(
                    ModelProviderContext(
                        run_id=run.run_id,
                        task_instruction=run.task.metadata["instruction"],
                        turns=list(run.trajectory),
                    )
                )
            except ProviderBoundaryError as exc:
                return _provider_failure_reason(exc)
            except Exception as exc:
                return _provider_failure_reason(normalize_provider_error(exc))
            if command is None:
                return None

            result = request.sandbox.execute(
                command.command,
                cwd=command.cwd,
                timeout_seconds=timeout_seconds,
            )
            turn = result.to_terminal_turn(
                turn_index=len(run.trajectory),
                model_call_id=command.model_call_id,
            )
            run.add_turn(turn)
            if result.timed_out:
                return f"Terminal command timed out after {timeout_seconds} seconds: {command.command}"
            if result.exit_code != 0:
                return f"Terminal command failed with exit code {result.exit_code}: {command.command}"


def _provider_failure_reason(error: ProviderBoundaryError) -> str:
    status = f" status {error.status_code}" if error.status_code is not None else ""
    return f"model provider {error.code.value}{status}: {_redact_failure_message(error.message)}"


def _redact_failure_message(message: str) -> str:
    redacted = re.sub(r"sk-[A-Za-z0-9_-]+", "[redacted]", message)
    redacted = re.sub(r"Bearer\s+\S+", "Bearer [redacted]", redacted, flags=re.IGNORECASE)
    return redacted
