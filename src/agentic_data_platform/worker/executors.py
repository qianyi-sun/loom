from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Protocol

from agentic_data_platform.artifacts.store import ArtifactPersistence
from agentic_data_platform.benchmarks.adapters import BenchmarkRegistration
from agentic_data_platform.domain.run_records import ArtifactKind, ArtifactRef, JudgeConfig, RunRecord, RunStatus
from agentic_data_platform.evaluation.mock import MockEvaluatorAdapter
from agentic_data_platform.harbor.ingestion import HarborIngestionResult, HarborResultIngestor
from agentic_data_platform.harbor.runner import HarborRunSpec, HarborRunnerBackend
from agentic_data_platform.harbor.smoke import write_harbor_cli_smoke_task
from agentic_data_platform.models.providers import ModelCommand, ScriptedModelProvider
from agentic_data_platform.providers.config import DevProviderConfigRegistry
from agentic_data_platform.runs.terminal_benchmark import TerminalBenchmarkRunner, TerminalBenchmarkRunRequest
from agentic_data_platform.sandbox.docker_terminal import (
    CommandRunner,
    DockerTerminalSandbox,
    DockerTerminalSandboxConfig,
    SandboxCommandResult,
    WorkspaceFile,
    WorkspaceSnapshot,
)


class WorkerRunExecutor(Protocol):
    def execute(self, run: RunRecord) -> RunRecord:
        ...


@dataclass(frozen=True)
class DockerTerminalWorkerExecutor:
    artifact_persistence: ArtifactPersistence
    workspace_root: Path
    host_workspace_root: Path | None = None
    evaluator_score: float = 0.75
    evaluator_feedback: str = "Mock evaluator feedback: Docker terminal trajectory and workspace were reviewed."
    provider_registry: DevProviderConfigRegistry | None = None
    command_runner: CommandRunner | None = None
    harbor_command_runner: CommandRunner | None = None

    def execute(self, run: RunRecord) -> RunRecord:
        self._resolve_provider_refs(run)
        harbor_spec = _harbor_run_spec_for_run(run, workspace_root=self.workspace_root)
        if harbor_spec is not None:
            return self._execute_harbor_run(run, harbor_spec)

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
                    commands=_commands_for_run(run, metadata_keys=("worker_commands", "worker_fixture_commands")),
                ),
                judge=judge,
                rubric_id=judge.rubric_version,
                sandbox=DockerTerminalSandbox(
                    _sandbox_config_for_run(
                        run,
                        workspace_root=self.workspace_root,
                        host_workspace_root=self.host_workspace_root,
                    ),
                    runner=self.command_runner,
                ),
            ),
        )
        return result.run

    def _execute_harbor_run(self, run: RunRecord, spec: HarborRunSpec) -> RunRecord:
        if run.status is not RunStatus.PROVISIONING:
            run.transition_to(RunStatus.PROVISIONING)
        run.transition_to(RunStatus.RUNNING)

        result = HarborRunnerBackend(command_runner=self.harbor_command_runner).run(spec)
        run.attach_artifact(
            self.artifact_persistence.persist_harbor_runner_report(
                run_id=run.run_id,
                task_instance_id=run.task.instance_id,
                report=result.to_report(),
            )
        )
        if result.exit_code != 0:
            run.failure_reason = _harbor_failure_reason(result)
            run.transition_to(RunStatus.FAILED)
            return run

        ingested = HarborResultIngestor(artifact_persistence=self.artifact_persistence).ingest(
            run_id=run.run_id,
            task_instance_id=run.task.instance_id,
            jobs_dir=result.jobs_dir,
            trial_name=spec.trial_name,
        )
        _attach_harbor_ingestion(run, ingested)
        run.attach_artifact(
            self.artifact_persistence.persist_workspace_snapshot(
                run_id=run.run_id,
                task_instance_id=run.task.instance_id,
                snapshot=_workspace_snapshot_from_harbor(run, ingested),
            )
        )
        run.metadata["harbor_runner"] = {
            "jobs_dir": result.jobs_dir.name,
            "exit_code": result.exit_code,
            "timed_out": result.timed_out,
            "duration_seconds": result.duration_seconds,
        }
        run.transition_to(RunStatus.EVALUATING)
        for evaluator_result in ingested.evaluator_results:
            run.attach_evaluator_result(evaluator_result)

        if all(result.status == "completed" for result in ingested.evaluator_results):
            run.transition_to(RunStatus.SUCCEEDED)
        else:
            run.failure_reason = "Harbor verifier did not complete"
            run.transition_to(RunStatus.FAILED)
        return run

    def _resolve_provider_refs(self, run: RunRecord) -> None:
        if self.provider_registry is None:
            return
        _resolve_metadata_ref(self.provider_registry, run.model.metadata)
        for config in run.evaluator_configs:
            _resolve_metadata_ref(self.provider_registry, config.metadata)


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
                    commands=_commands_for_run(run, metadata_keys=("worker_fixture_commands",)),
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


def _commands_for_run(run: RunRecord, *, metadata_keys: tuple[str, ...]) -> list[ModelCommand]:
    for key in metadata_keys:
        configured = run.metadata.get(key)
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


def _harbor_run_spec_for_run(run: RunRecord, *, workspace_root: Path) -> HarborRunSpec | None:
    configured = run.metadata.get("harbor_run")
    if not isinstance(configured, dict):
        return None

    dataset_ref = _optional_str(configured.get("dataset_ref"))
    task_path = _harbor_task_path_for_run(run, configured=configured, workspace_root=workspace_root)
    agent_import_path_value = _optional_str(configured.get("agent_import_path"))
    agent_import_path = Path(agent_import_path_value) if agent_import_path_value is not None else None
    timeout_seconds = _int_or_default(
        configured.get("timeout_seconds") or run.runner.resource_limits.get("timeout_seconds"),
        3600,
    )
    jobs_dir_value = _optional_str(configured.get("jobs_dir"))
    jobs_dir = Path(jobs_dir_value) if jobs_dir_value is not None else workspace_root / run.run_id / "harbor-jobs"
    return HarborRunSpec(
        run_id=run.run_id,
        task_instance_id=run.task.instance_id,
        dataset_ref=dataset_ref,
        task_path=task_path,
        agent=_optional_str(configured.get("agent")) or _optional_str(configured.get("agent_id")) or "oracle",
        agent_import_path=agent_import_path,
        model_name=_optional_str(configured.get("model_name")) or run.model.model_name,
        environment=(
            _optional_str(configured.get("environment"))
            or _optional_str(configured.get("env"))
            or _optional_str(configured.get("sandbox"))
            or "docker"
        ),
        jobs_dir=jobs_dir,
        trial_name=_optional_str(configured.get("trial_name")),
        timeout_seconds=timeout_seconds,
        auto_confirm=bool(configured.get("auto_confirm", True)),
        agent_env=_string_list(configured.get("agent_env", []), field_name="harbor_run.agent_env"),
        verifier_env=_string_list(configured.get("verifier_env", []), field_name="harbor_run.verifier_env"),
        extra_args=_string_list(configured.get("extra_args", []), field_name="harbor_run.extra_args"),
    )


def _harbor_task_path_for_run(run: RunRecord, *, configured: dict[str, object], workspace_root: Path) -> Path | None:
    task_path_value = _optional_str(configured.get("task_path"))
    task_template = _optional_str(configured.get("task_template"))
    if task_path_value is not None and task_template is not None:
        raise ValueError("harbor_run must not set both task_path and task_template")

    if task_path_value is not None:
        return Path(task_path_value)

    if task_template is None:
        return None

    if task_template != "harbor-cli-smoke":
        raise ValueError(f"unsupported harbor_run.task_template: {task_template}")

    task_path = workspace_root / run.run_id / "harbor-task"
    write_harbor_cli_smoke_task(task_path)
    return task_path


def _attach_harbor_ingestion(run: RunRecord, ingested: HarborIngestionResult) -> None:
    for turn in ingested.turns:
        run.add_turn(replace(turn, turn_index=len(run.trajectory)))
    for artifact in ingested.artifacts:
        run.attach_artifact(artifact)


def _workspace_snapshot_from_harbor(run: RunRecord, ingested: HarborIngestionResult) -> WorkspaceSnapshot:
    files = []
    for artifact in ingested.artifacts:
        if artifact.kind is not ArtifactKind.GENERATED_FILE:
            continue
        destination = artifact.metadata.get("destination")
        if not isinstance(destination, str) or artifact.size_bytes is None or artifact.sha256 is None:
            continue
        files.append(WorkspaceFile(path=destination, size_bytes=artifact.size_bytes, sha256=artifact.sha256))
    files.sort(key=lambda item: item.path)
    return WorkspaceSnapshot(
        run_id=run.run_id,
        workspace_path=f"harbor://{ingested.job_name}/{ingested.trial_name}/artifacts",
        captured_at=datetime.now(timezone.utc),
        files=files,
    )


def _harbor_failure_reason(result) -> str:
    if result.timed_out:
        return f"Harbor run timed out with exit code {result.exit_code}"
    return f"Harbor run failed with exit code {result.exit_code}"


def _optional_str(value: object) -> str | None:
    return value if isinstance(value, str) and value.strip() else None


def _string_list(value: object, *, field_name: str) -> list[str]:
    if value is None:
        return []
    if not isinstance(value, list) or any(not isinstance(item, str) or not item.strip() for item in value):
        raise ValueError(f"{field_name} must be a list of non-empty strings")
    return value


def _sandbox_config_for_run(
    run: RunRecord,
    *,
    workspace_root: Path,
    host_workspace_root: Path | None,
) -> DockerTerminalSandboxConfig:
    limits = run.runner.resource_limits
    memory_mb = _memory_limit_mb(limits)
    return DockerTerminalSandboxConfig(
        run_id=run.run_id,
        image=run.runner.image,
        workspace_root=workspace_root,
        host_workspace_root=host_workspace_root,
        cpu_limit=limits.get("cpu") or limits.get("cpu_limit"),
        memory_mb=memory_mb,
        pids_limit=_int_or_none(limits.get("pids_limit")),
        timeout_seconds=_int_or_default(limits.get("timeout_seconds"), 3600),
        internet_access=run.runner.internet_access,
    )


def _memory_limit_mb(limits: dict[str, int | float]) -> int | None:
    if "memory_mb" in limits:
        return _int_or_none(limits["memory_mb"])
    if "memory_gib" in limits:
        return int(float(limits["memory_gib"]) * 1024)
    return None


def _int_or_default(value: object, default: int) -> int:
    if value is None:
        return default
    return int(value)


def _int_or_none(value: object) -> int | None:
    if value is None:
        return None
    return int(value)
