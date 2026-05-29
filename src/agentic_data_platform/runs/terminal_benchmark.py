from __future__ import annotations

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
        self._execute_model_commands(run, request, timeout_seconds=timeout_seconds)

        run.transition_to(RunStatus.EVALUATING)
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
        evaluator_report_ref = self.artifact_persistence.persist_evaluator_report(
            run_id=run.run_id,
            task_instance_id=run.task.instance_id,
            result=evaluator_result,
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

    def _execute_model_commands(
        self,
        run: RunRecord,
        request: TerminalBenchmarkRunRequest,
        *,
        timeout_seconds: int,
    ) -> None:
        while True:
            command = request.model_provider.next_command(
                ModelProviderContext(
                    run_id=run.run_id,
                    task_instruction=run.task.metadata["instruction"],
                    turns=list(run.trajectory),
                )
            )
            if command is None:
                return

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
