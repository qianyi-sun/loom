from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Protocol

from agentic_data_platform.artifacts.store import ArtifactPersistence
from agentic_data_platform.benchmarks.adapters import BenchmarkRegistration
from agentic_data_platform.domain.run_records import JudgeConfig, RunRecord
from agentic_data_platform.evaluation.mock import MockEvaluatorAdapter
from agentic_data_platform.models.providers import ModelCommand, ScriptedModelProvider
from agentic_data_platform.providers.config import DevProviderConfigRegistry
from agentic_data_platform.runs.terminal_benchmark import TerminalBenchmarkRunner, TerminalBenchmarkRunRequest
from agentic_data_platform.sandbox.docker_terminal import SandboxCommandResult, WorkspaceFile, WorkspaceSnapshot


class WorkerRunExecutor(Protocol):
    def execute(self, run: RunRecord) -> RunRecord:
        ...


@dataclass(frozen=True)
class FixtureTerminalBenchmarkExecutor:
    artifact_persistence: ArtifactPersistence
    evaluator_score: float = 0.75
    evaluator_feedback: str = "Mock evaluator feedback: fixture worker trajectory and workspace were reviewed."
    provider_registry: DevProviderConfigRegistry | None = None

    def execute(self, run: RunRecord) -> RunRecord:
        self._resolve_provider_refs(run)
        judge = _judge_for_run(run)
        evaluator_id = _evaluator_id_for_run(run)
        runner = TerminalBenchmarkRunner(
            artifact_persistence=self.artifact_persistence,
            evaluator=MockEvaluatorAdapter(
                evaluator_id=evaluator_id,
                score=self.evaluator_score,
                verbal_feedback=self.evaluator_feedback,
            ),
        )
        registration = BenchmarkRegistration(task=run.task, runner=run.runner)
        result = runner.run_existing(
            run,
            TerminalBenchmarkRunRequest(
                run_id=run.run_id,
                project_id=run.project_id,
                owner_team=run.owner_team,
                registration=registration,
                model_provider=ScriptedModelProvider(
                    model=run.model,
                    commands=_commands_for_run(run),
                ),
                judge=judge,
                rubric_id=judge.rubric_version,
                sandbox=FixtureTerminalSandbox(run_id=run.run_id),
            ),
        )
        return result.run

    def _resolve_provider_refs(self, run: RunRecord) -> None:
        if self.provider_registry is None:
            return
        _resolve_metadata_ref(self.provider_registry, run.model.metadata)
        for config in run.evaluator_configs:
            _resolve_metadata_ref(self.provider_registry, config.metadata)


class FixtureTerminalSandbox:
    def __init__(self, *, run_id: str) -> None:
        self.run_id = run_id
        self.commands: list[str] = []

    def execute(self, command: str, *, cwd: str | None = None, timeout_seconds: int | None = None) -> SandboxCommandResult:
        self.commands.append(command)
        started = datetime(2026, 5, 28, 12, len(self.commands), 0, tzinfo=timezone.utc)
        return SandboxCommandResult(
            run_id=self.run_id,
            command=command,
            cwd=cwd or "/workspace",
            started_at=started,
            completed_at=started,
            exit_code=0,
            stdout=f"fixture worker ran {command}\n",
            stderr="",
            changed_paths=["receipts.xlsx"],
            metadata={"timeout_seconds": timeout_seconds},
        )

    def capture_workspace(self) -> WorkspaceSnapshot:
        return WorkspaceSnapshot(
            run_id=self.run_id,
            workspace_path="/workspace",
            captured_at=datetime(2026, 5, 28, 12, 30, 0, tzinfo=timezone.utc),
            files=[WorkspaceFile(path="receipts.xlsx", size_bytes=128, sha256="3" * 64)],
        )


def _commands_for_run(run: RunRecord) -> list[ModelCommand]:
    configured = run.metadata.get("worker_fixture_commands")
    if isinstance(configured, list) and configured:
        return [_model_command(item, index=index) for index, item in enumerate(configured, start=1)]

    return [ModelCommand(command="python solve.py", cwd="/workspace", model_call_id="fixture-call-1")]


def _model_command(value: object, *, index: int) -> ModelCommand:
    if isinstance(value, str):
        return ModelCommand(command=value, cwd="/workspace", model_call_id=f"fixture-call-{index}")
    if isinstance(value, dict):
        return ModelCommand(
            command=str(value["command"]),
            cwd=str(value.get("cwd") or "/workspace"),
            model_call_id=str(value.get("model_call_id") or f"fixture-call-{index}"),
        )
    raise ValueError("worker_fixture_commands must contain command strings or command objects")


def _judge_for_run(run: RunRecord) -> JudgeConfig:
    for config in run.evaluator_configs:
        if config.judge is not None:
            return config.judge
    return JudgeConfig(provider="mock", model_name="deterministic-judge", rubric_version="worker-fixture-v0")


def _evaluator_id_for_run(run: RunRecord) -> str:
    if run.evaluator_configs:
        return run.evaluator_configs[0].evaluator_id
    return "mock-judge-v0"


def _resolve_metadata_ref(registry: DevProviderConfigRegistry, metadata: dict[str, object]) -> None:
    config_id = metadata.get("provider_config_id")
    if isinstance(config_id, str):
        registry.get(config_id)
    secret_ref = metadata.get("secret_ref")
    if isinstance(secret_ref, str):
        registry.resolve_secret(secret_ref)
