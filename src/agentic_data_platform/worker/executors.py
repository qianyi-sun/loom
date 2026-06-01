from __future__ import annotations

import hashlib
import json
import mimetypes
import shlex
import subprocess
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Protocol

import httpx

from agentic_data_platform.artifacts.store import Artifacpilot groupjectStore, ArtifactPersistence
from agentic_data_platform.benchmarks.adapters import BenchmarkRegistration
from agentic_data_platform.domain.run_records import (
    ArtifactKind,
    ArtifactRef,
    EvaluatorResult,
    JudgeConfig,
    ModelConfig,
    RunRecord,
    RunStatus,
    RunnerKind,
    TerminalTurn,
)
from agentic_data_platform.evaluation.mock import MockEvaluatorAdapter
from agentic_data_platform.harbor.ingestion import HarborIngestionResult, HarborResultIngestor
from agentic_data_platform.harbor.runner import HarborRunSpec, HarborRunnerBackend
from agentic_data_platform.harbor.smoke import write_harbor_cli_smoke_task
from agentic_data_platform.harbor.task_uploads import materialize_harbor_task_archive
from agentic_data_platform.models.providers import (
    ModelCommand,
    ModelProvider,
    ModelProviderContext,
    OpenAICompatibleModelProvider,
    ScriptedModelProvider,
)
from agentic_data_platform.providers.config import DevProviderConfigRegistry
from agentic_data_platform.providers.errors import ProviderBoundaryError, ProviderErrorCode
from agentic_data_platform.runs.terminal_benchmark import TerminalBenchmarkRunner, TerminalBenchmarkRunRequest
from agentic_data_platform.sandbox.docker_terminal import (
    CommandRunner,
    DockerTerminalSandbox,
    DockerTerminalSandboxConfig,
    SandboxCommandResult,
    SubprocessCommandRunner,
    WorkspaceFile,
    WorkspaceSnapshot,
)


_ORIGINAL_WRAPPER_CONTRACTS = {
    "skillflow-original-wrapper-v0",
    "skilllearnbench-original-wrapper-v0",
}


class WorkerRunExecutor(Protocol):
    def execute(self, run: RunRecord) -> RunRecord:
        ...


@dataclass(frozen=True)
class OriginalWrapperRunSpec:
    runner_contract: str
    dry_run: bool
    upstream_root: Path | None
    timeout_seconds: int


@dataclass(frozen=True)
class DockerTerminalWorkerExecutor:
    artifact_persistence: ArtifactPersistence
    workspace_root: Path
    host_workspace_root: Path | None = None
    evaluator_score: float = 0.75
    evaluator_feedback: str = "Mock evaluator feedback: Docker terminal trajectory and workspace were reviewed."
    provider_registry: DevProviderConfigRegistry | None = None
    model_http_client: httpx.Client | None = None
    command_runner: CommandRunner | None = None
    harbor_command_runner: CommandRunner | None = None
    wrapper_command_runner: CommandRunner | None = None

    def execute(self, run: RunRecord) -> RunRecord:
        self._resolve_provider_refs(run)
        harbor_spec = _harbor_run_spec_for_run(
            run,
            workspace_root=self.workspace_root,
            artifact_store=self.artifact_persistence.store,
        )
        if harbor_spec is not None:
            return self._execute_harbor_run(run, harbor_spec)
        wrapper_spec = _original_wrapper_run_spec_for_run(run)
        if wrapper_spec is not None:
            return self._execute_original_wrapper_run(run, wrapper_spec)

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
                model_provider=_model_provider_for_run(
                    run,
                    registry=self.provider_registry,
                    http_client=self.model_http_client,
                    metadata_keys=("worker_commands", "worker_fixture_commands"),
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

    def _execute_original_wrapper_run(self, run: RunRecord, spec: OriginalWrapperRunSpec) -> RunRecord:
        if run.status is not RunStatus.PROVISIONING:
            run.transition_to(RunStatus.PROVISIONING)
        run.transition_to(RunStatus.RUNNING)

        run_root = self.workspace_root / run.run_id / "original-wrapper"
        workspace = run_root / "workspace"
        output_dir = run_root / "output"
        artifacts_dir = run_root / "artifacts"
        manifest_path = run_root / "task-manifest.json"
        result_path = run_root / "wrapper-result.json"
        for path in (workspace, output_dir, artifacts_dir):
            path.mkdir(parents=True, exist_ok=True)
        _write_original_wrapper_manifest(
            run,
            manifest_path=manifest_path,
            workspace=workspace,
            output_dir=output_dir,
            artifacts_dir=artifacts_dir,
            provider_registry=self.provider_registry,
        )

        args = [
            *run.runner.entrypoint,
            "--task-manifest",
            str(manifest_path),
            "--workspace",
            str(workspace),
            "--output",
            str(result_path),
            "--artifacts-dir",
            str(artifacts_dir),
            "--timeout-seconds",
            str(spec.timeout_seconds),
        ]
        if spec.dry_run:
            args.append("--dry-run")
        elif spec.upstream_root is not None:
            args.extend(["--upstream-root", str(spec.upstream_root)])

        started_at = datetime.now(timezone.utc)
        try:
            process = (self.wrapper_command_runner or SubprocessCommandRunner()).run(
                args,
                timeout=spec.timeout_seconds,
            )
            completed_at = datetime.now(timezone.utc)
        except subprocess.TimeoutExpired as exc:
            completed_at = datetime.now(timezone.utc)
            process = subprocess.CompletedProcess(
                args=args,
                returncode=124,
                stdout=_process_output(exc.output),
                stderr=(
                    _process_output(exc.stderr)
                    or f"Original benchmark wrapper timed out after {spec.timeout_seconds} seconds\n"
                ),
            )

        run.add_turn(
            TerminalTurn(
                turn_index=len(run.trajectory),
                command=shlex.join(args),
                cwd=str(run_root),
                started_at=started_at,
                completed_at=completed_at,
                exit_code=process.returncode,
                stdout=_process_output(process.stdout),
                stderr=_process_output(process.stderr),
                changed_paths=_workspace_paths(workspace),
                metadata={
                    "runner_contract": spec.runner_contract,
                    "dry_run": spec.dry_run,
                    "timeout_seconds": spec.timeout_seconds,
                },
            )
        )
        run.attach_artifact(
            self.artifact_persistence.persist_trajectory(
                run_id=run.run_id,
                task_instance_id=run.task.instance_id,
                turns=run.trajectory,
            )
        )
        run.attach_artifact(
            self.artifact_persistence.persist_workspace_snapshot(
                run_id=run.run_id,
                task_instance_id=run.task.instance_id,
                snapshot=_workspace_snapshot_from_path(run, workspace),
            )
        )

        if not result_path.is_file():
            run.failure_reason = _missing_original_wrapper_result_reason(process)
            run.transition_to(RunStatus.FAILED)
            return run

        try:
            wrapper_result = _read_json_object(result_path)
        except ValueError as exc:
            run.failure_reason = str(exc)
            run.transition_to(RunStatus.FAILED)
            return run

        result_artifact = self.artifact_persistence.persist_wrapper_artifact(
            run_id=run.run_id,
            task_instance_id=run.task.instance_id,
            local_path=result_path,
            artifact_path="wrapper-result.json",
            kind=ArtifactKind.LOG,
            media_type="application/json",
            metadata={
                "content_type": "original_wrapper_result",
                "runner_contract": spec.runner_contract,
            },
        )
        run.attach_artifact(result_artifact)

        try:
            wrapper_artifacts = _persist_original_wrapper_artifacts(
                run=run,
                artifact_persistence=self.artifact_persistence,
                wrapper_result=wrapper_result,
                workspace=workspace,
                output_dir=output_dir,
                artifacts_dir=artifacts_dir,
                runner_contract=spec.runner_contract,
            )
        except ValueError as exc:
            run.failure_reason = str(exc)
            run.transition_to(RunStatus.FAILED)
            return run
        for artifact in wrapper_artifacts:
            run.attach_artifact(artifact)

        if process.returncode != 0 or wrapper_result.get("status") != "completed":
            run.failure_reason = _original_wrapper_failure_reason(process=process, wrapper_result=wrapper_result)
            run.transition_to(RunStatus.FAILED)
            return run

        run.transition_to(RunStatus.EVALUATING)
        run.attach_evaluator_result(
            _original_wrapper_evaluator_result(
                run,
                wrapper_result=wrapper_result,
                artifacts=wrapper_artifacts,
                artifacts_dir=artifacts_dir,
            )
        )
        run.metadata["original_wrapper"] = {
            "runner_contract": spec.runner_contract,
            "exit_code": process.returncode,
            "dry_run": spec.dry_run,
            "duration_seconds": (completed_at - started_at).total_seconds(),
        }
        run.transition_to(RunStatus.SUCCEEDED)
        return run

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
    configured_commands = _configured_commands_for_run(run, metadata_keys=metadata_keys)
    if configured_commands is not None:
        return configured_commands

    return [ModelCommand(command="python solve.py", cwd="/workspace", model_call_id="fixture-call-1")]


def _configured_commands_for_run(run: RunRecord, *, metadata_keys: tuple[str, ...]) -> list[ModelCommand] | None:
    for key in metadata_keys:
        configured = run.metadata.get(key)
        if isinstance(configured, list) and configured:
            return [_model_command(item, index=index) for index, item in enumerate(configured, start=1)]
    return None


def _model_provider_for_run(
    run: RunRecord,
    *,
    registry: DevProviderConfigRegistry | None,
    http_client: httpx.Client | None,
    metadata_keys: tuple[str, ...],
) -> ModelProvider:
    configured_commands = _configured_commands_for_run(run, metadata_keys=metadata_keys)
    if configured_commands is not None:
        return ScriptedModelProvider(model=run.model, commands=configured_commands)

    provider_config_id = _optional_str(run.model.metadata.get("provider_config_id"))
    secret_ref = _optional_str(run.model.metadata.get("secret_ref"))
    if provider_config_id is None and secret_ref is None:
        return ScriptedModelProvider(
            model=run.model,
            commands=[
                ModelCommand(
                    command="python solve.py",
                    cwd="/workspace",
                    model_call_id="fixture-call-1",
                )
            ],
        )
    if provider_config_id is None:
        return _FailingModelProvider(
            model=run.model,
            error=ProviderBoundaryError(
                code=ProviderErrorCode.INVALID_REQUEST,
                message="model provider requires provider_config_id",
            ),
        )
    if registry is None:
        return _FailingModelProvider(
            model=run.model,
            error=ProviderBoundaryError(
                code=ProviderErrorCode.INVALID_REQUEST,
                message="model provider registry is not configured",
            ),
        )

    try:
        ref = registry.get(provider_config_id)
    except (KeyError, ValueError) as exc:
        return _FailingModelProvider(
            model=run.model,
            error=ProviderBoundaryError(
                code=ProviderErrorCode.INVALID_REQUEST,
                message=str(exc).strip("'"),
            ),
        )
    try:
        secret = registry.resolve_secret(secret_ref or ref.secret_ref)
    except (KeyError, ValueError) as exc:
        return _FailingModelProvider(
            model=run.model,
            error=ProviderBoundaryError(
                code=ProviderErrorCode.INVALID_REQUEST,
                message=str(exc).strip("'"),
            ),
        )
    if not ref.base_url:
        return _FailingModelProvider(
            model=run.model,
            error=ProviderBoundaryError(
                code=ProviderErrorCode.INVALID_REQUEST,
                message="model provider base_url is not configured",
            ),
        )
    return OpenAICompatibleModelProvider(
        model=run.model,
        base_url=ref.base_url,
        api_key=secret.value,
        http_client=http_client,
    )


@dataclass(frozen=True)
class _FailingModelProvider:
    model: ModelConfig
    error: ProviderBoundaryError

    def next_command(self, context: ModelProviderContext) -> ModelCommand | None:
        raise self.error


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


def _original_wrapper_run_spec_for_run(run: RunRecord) -> OriginalWrapperRunSpec | None:
    if run.runner.kind is not RunnerKind.ORIGINAL_BENCHMARK:
        return None
    runner_contract = _optional_str(run.runner.metadata.get("runner_contract"))
    if runner_contract not in _ORIGINAL_WRAPPER_CONTRACTS:
        return None
    configured = run.metadata.get("wrapper_run")
    if not isinstance(configured, dict):
        return None

    timeout_seconds = _int_or_default(
        configured.get("timeout_seconds") or run.runner.resource_limits.get("timeout_seconds"),
        3600,
    )
    if timeout_seconds <= 0:
        raise ValueError("wrapper_run.timeout_seconds must be positive")

    upstream_root_value = _optional_str(configured.get("upstream_root"))
    return OriginalWrapperRunSpec(
        runner_contract=runner_contract,
        dry_run=bool(configured.get("dry_run", False)),
        upstream_root=Path(upstream_root_value) if upstream_root_value is not None else None,
        timeout_seconds=timeout_seconds,
    )


def _write_original_wrapper_manifest(
    run: RunRecord,
    *,
    manifest_path: Path,
    workspace: Path,
    output_dir: Path,
    artifacts_dir: Path,
    provider_registry: DevProviderConfigRegistry | None,
) -> None:
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest = {
        "schema_version": "adp.worker_wrapper_task.v1",
        "run_id": run.run_id,
        "suite_name": run.task.benchmark_suite,
        "benchmark_version": run.task.benchmark_version,
        "source_uri": run.task.source_uri,
        "source_version": _optional_str(run.task.metadata.get("source_version")) or run.task.benchmark_version,
        "task_family": run.task.task_family,
        "instance_id": run.task.instance_id,
        "instruction_ref": (
            _optional_str(run.task.metadata.get("instruction_ref"))
            or "inline:task.metadata.instruction"
        ),
        "input_files": list(run.task.input_artifact_refs),
        "model": _original_wrapper_model_manifest(run, provider_registry=provider_registry),
        "output_dir": str(output_dir),
        "artifacts_dir": str(artifacts_dir),
        "workspace": str(workspace),
    }
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _original_wrapper_model_manifest(
    run: RunRecord,
    *,
    provider_registry: DevProviderConfigRegistry | None,
) -> dict[str, Any]:
    model = {
        "provider": run.model.provider,
        "model_name": run.model.model_name,
        "mode": run.model.mode.value,
        "prompt_template_version": run.model.prompt_template_version,
        **dict(run.model.metadata),
    }
    if run.model.model_version is not None:
        model["model_version"] = run.model.model_version

    provider_config_id = _optional_str(model.get("provider_config_id"))
    if provider_config_id is not None and provider_registry is not None:
        ref = provider_registry.get(provider_config_id)
        model.setdefault("secret_ref", ref.secret_ref)
        if ref.base_url:
            model.setdefault("base_url", ref.base_url)
    return model


def _persist_original_wrapper_artifacts(
    *,
    run: RunRecord,
    artifact_persistence: ArtifactPersistence,
    wrapper_result: dict[str, Any],
    workspace: Path,
    output_dir: Path,
    artifacts_dir: Path,
    runner_contract: str,
) -> list[ArtifactRef]:
    artifacts_payload = wrapper_result.get("artifacts", [])
    if artifacts_payload is None:
        return []
    if not isinstance(artifacts_payload, list):
        raise ValueError("original wrapper result artifacts must be a list")

    artifacts: list[ArtifactRef] = []
    for item in artifacts_payload:
        if not isinstance(item, dict):
            raise ValueError("original wrapper artifact entries must be objects")
        artifact_path = _optional_str(item.get("path"))
        if artifact_path is None:
            raise ValueError("original wrapper artifact path must be a non-empty string")
        local_path = _original_wrapper_artifact_path(
            artifact_path,
            workspace=workspace,
            output_dir=output_dir,
            artifacts_dir=artifacts_dir,
        )
        kind_name = _optional_str(item.get("kind")) or "log"
        media_type = (
            _optional_str(item.get("media_type"))
            or mimetypes.guess_type(local_path.name)[0]
            or "application/octet-stream"
        )
        artifacts.append(
            artifact_persistence.persist_wrapper_artifact(
                run_id=run.run_id,
                task_instance_id=run.task.instance_id,
                local_path=local_path,
                artifact_path=artifact_path,
                kind=_original_wrapper_artifact_kind(kind_name),
                media_type=media_type,
                metadata={
                    "content_type": "original_wrapper_artifact",
                    "runner_contract": runner_contract,
                    "wrapper_artifact_kind": kind_name,
                },
            )
        )
    return artifacts


def _original_wrapper_artifact_path(
    artifact_path: str,
    *,
    workspace: Path,
    output_dir: Path,
    artifacts_dir: Path,
) -> Path:
    relative = Path(artifact_path)
    if relative.is_absolute() or any(part in {"", ".", ".."} for part in relative.parts):
        raise ValueError(f"unsafe original wrapper artifact path: {artifact_path}")
    root_name = relative.parts[0]
    remainder = relative.parts[1:]
    if root_name == "artifacts":
        return artifacts_dir.joinpath(*remainder)
    if root_name == "workspace":
        return workspace.joinpath(*remainder)
    if root_name == "output":
        return output_dir.joinpath(*remainder)
    raise ValueError("original wrapper artifact path must start with artifacts/, workspace/, or output/")


def _original_wrapper_artifact_kind(kind_name: str) -> ArtifactKind:
    normalized = kind_name.strip().lower().replace("-", "_")
    if normalized in {"log", "runner_config"}:
        return ArtifactKind.LOG
    if normalized == "evaluator_report":
        return ArtifactKind.EVALUATOR_REPORT
    if normalized in {"upstream_output", "generated_file"}:
        return ArtifactKind.GENERATED_FILE
    if normalized == "trajectory":
        return ArtifactKind.TRAJECTORY
    if normalized == "workspace_snapshot":
        return ArtifactKind.WORKSPACE_SNAPSHOT
    raise ValueError(f"unsupported original wrapper artifact kind: {kind_name}")


def _original_wrapper_evaluator_result(
    run: RunRecord,
    *,
    wrapper_result: dict[str, Any],
    artifacts: list[ArtifactRef],
    artifacts_dir: Path,
) -> EvaluatorResult:
    metrics = _dict_value(wrapper_result.get("metrics"))
    feedback = _feedback_values(wrapper_result)
    evaluator_report_ref = _optional_str(wrapper_result.get("evaluator_report_ref"))
    if evaluator_report_ref is not None:
        report_path = _original_wrapper_artifact_path(
            evaluator_report_ref,
            workspace=artifacts_dir,
            output_dir=artifacts_dir,
            artifacts_dir=artifacts_dir,
        )
        if report_path.is_file():
            try:
                report = _read_json_object(report_path)
            except ValueError:
                report = {}
            metrics = {**_dict_value(report.get("metrics")), **metrics}
            feedback.extend(_feedback_values(report))

    report_artifact_refs = [
        artifact.uri
        for artifact in artifacts
        if artifact.kind is ArtifactKind.EVALUATOR_REPORT
    ]
    return EvaluatorResult(
        evaluator_id=_original_wrapper_evaluator_id(run),
        mode="harbor_verifier",
        status="completed",
        score=_score_from_metrics(metrics),
        metrics=metrics,
        verbal_feedback="\n".join(dict.fromkeys(feedback)),
        judge=None,
        artifact_refs=report_artifact_refs,
        metadata={
            "runner_contract": _optional_str(run.runner.metadata.get("runner_contract")) or "unknown",
            "wrapper_status": wrapper_result.get("status"),
            "wrapper_exit_code": wrapper_result.get("exit_code"),
        },
    )


def _original_wrapper_evaluator_id(run: RunRecord) -> str:
    for config in run.evaluator_configs:
        if config.mode == "harbor_verifier":
            return config.evaluator_id
    return "original-wrapper-verifier"


def _original_wrapper_failure_reason(
    *,
    process: subprocess.CompletedProcess[str],
    wrapper_result: dict[str, Any],
) -> str:
    configured_reason = _optional_str(wrapper_result.get("failure_reason"))
    if configured_reason is not None:
        return configured_reason
    if process.returncode != 0:
        return f"Original benchmark wrapper exited with code {process.returncode}"
    return "Original benchmark wrapper reported a failed status"


def _missing_original_wrapper_result_reason(process: subprocess.CompletedProcess[str]) -> str:
    if process.returncode == 124:
        return "Original benchmark wrapper timed out before writing a result file"
    if process.returncode != 0:
        return f"Original benchmark wrapper exited with code {process.returncode} before writing a result file"
    return "Original benchmark wrapper did not write a result file"


def _harbor_run_spec_for_run(
    run: RunRecord,
    *,
    workspace_root: Path,
    artifact_store: Artifacpilot groupjectStore,
) -> HarborRunSpec | None:
    configured = run.metadata.get("harbor_run")
    if not isinstance(configured, dict):
        return None

    dataset_ref = _optional_str(configured.get("dataset_ref"))
    task_path = _harbor_task_path_for_run(
        run,
        configured=configured,
        workspace_root=workspace_root,
        artifact_store=artifact_store,
    )
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


def _harbor_task_path_for_run(
    run: RunRecord,
    *,
    configured: dict[str, object],
    workspace_root: Path,
    artifact_store: Artifacpilot groupjectStore,
) -> Path | None:
    task_path_value = _optional_str(configured.get("task_path"))
    task_template = _optional_str(configured.get("task_template"))
    task_archive_storage_key = _optional_str(configured.get("task_archive_storage_key"))
    configured_sources = [
        value
        for value in (task_path_value, task_template, task_archive_storage_key)
        if value is not None
    ]
    if len(configured_sources) > 1:
        raise ValueError("harbor_run must set only one of task_path, task_template, or task_archive_storage_key")

    if task_path_value is not None:
        return Path(task_path_value)

    if task_archive_storage_key is not None:
        task_path = workspace_root / run.run_id / "harbor-task-upload"
        materialize_harbor_task_archive(
            artifact_store.get_bytes(task_archive_storage_key),
            filename=Path(task_archive_storage_key).name,
            destination=task_path,
        )
        return task_path

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


def _workspace_snapshot_from_path(run: RunRecord, workspace: Path) -> WorkspaceSnapshot:
    return WorkspaceSnapshot(
        run_id=run.run_id,
        workspace_path=str(workspace),
        captured_at=datetime.now(timezone.utc),
        files=[
            WorkspaceFile(path=relative_path, size_bytes=size_bytes, sha256=sha256)
            for relative_path, size_bytes, sha256 in _workspace_file_records(workspace)
        ],
    )


def _workspace_paths(workspace: Path) -> list[str]:
    return [relative_path for relative_path, _, _ in _workspace_file_records(workspace)]


def _workspace_file_records(workspace: Path) -> list[tuple[str, int, str]]:
    if not workspace.exists():
        return []
    records: list[tuple[str, int, str]] = []
    for path in sorted(workspace.rglob("*")):
        if not path.is_file() or path.is_symlink():
            continue
        relative_path = path.relative_to(workspace).as_posix()
        records.append((relative_path, path.stat().st_size, _sha256_file(path)))
    return records


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _read_json_object(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise ValueError(f"missing JSON file: {path}") from exc
    except json.JSONDecodeError as exc:
        raise ValueError(f"invalid JSON file: {path}") from exc
    if not isinstance(payload, dict):
        raise ValueError(f"JSON file must contain an object: {path}")
    return payload


def _dict_value(value: object) -> dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


def _feedback_values(payload: dict[str, Any]) -> list[str]:
    values: list[str] = []
    for key in ("verbal_feedback", "feedback", "summary", "message", "comment"):
        value = payload.get(key)
        if isinstance(value, str) and value.strip():
            values.append(value.strip())
        elif isinstance(value, list):
            values.extend(str(item).strip() for item in value if str(item).strip())
    return values


def _score_from_metrics(metrics: dict[str, Any]) -> float | None:
    for key in (
        "score",
        "reward",
        "accuracy",
        "upstream_score",
        "upstream_score_mean",
        "upstream_success_rate",
        "task_success",
    ):
        if key not in metrics:
            continue
        value = metrics[key]
        if isinstance(value, bool):
            return 1.0 if value else 0.0
        if isinstance(value, (int, float)):
            score = float(value)
            if 0 <= score <= 1:
                return score
    return None


def _process_output(output: bytes | str | None) -> str:
    if output is None:
        return ""
    if isinstance(output, bytes):
        return output.decode("utf-8", errors="replace")
    return output


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
