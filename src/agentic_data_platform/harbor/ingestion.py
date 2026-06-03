from __future__ import annotations

import json
import re
import hashlib
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
from typing import Any
from urllib.parse import quote

from agentic_data_platform.artifacts.store import ArtifactPersistence
from agentic_data_platform.domain.run_records import (
    ArtifactKind,
    ArtifactRef,
    EvaluatorResult,
    TerminalTurn,
)
from agentic_data_platform.domain.provider_usage import normalize_model_provider_usage
from agentic_data_platform.providers.config import redact_sensitive_metadata


@dataclass(frozen=True)
class HarborIngestionResult:
    job_name: str
    trial_name: str
    job_config: dict[str, Any]
    job_result: dict[str, Any]
    trial_config: dict[str, Any]
    trial_result: dict[str, Any]
    turns: list[TerminalTurn]
    artifacts: list[ArtifactRef]
    evaluator_results: list[EvaluatorResult]

    def to_dict(self) -> dict[str, Any]:
        return {
            "job_name": self.job_name,
            "trial_name": self.trial_name,
            "job_config": self.job_config,
            "job_result": self.job_result,
            "trial_config": self.trial_config,
            "trial_result": self.trial_result,
            "turns": [_turn_payload(turn) for turn in self.turns],
            "artifacts": [_artifact_payload(artifact) for artifact in self.artifacts],
            "evaluator_results": [_evaluator_payload(result) for result in self.evaluator_results],
        }


@dataclass(frozen=True)
class HarborIngestionFailureDiagnostics:
    category: str
    message: str
    turns: list[TerminalTurn]
    artifacts: list[ArtifactRef]
    metadata: dict[str, Any]

    def failure_payload(self) -> dict[str, Any]:
        return {
            "category": self.category,
            "source": "harbor_ingestion",
            "message": self.message,
            "metadata": dict(self.metadata),
        }


class HarborResultIngestor:
    def __init__(self, *, artifact_persistence: ArtifactPersistence) -> None:
        self.artifact_persistence = artifact_persistence

    def ingest(
        self,
        *,
        run_id: str,
        task_instance_id: str,
        jobs_dir: Path,
        trial_name: str | None = None,
    ) -> HarborIngestionResult:
        job_dir = _resolve_job_dir(jobs_dir)
        trial_dir = _resolve_trial_dir(job_dir, trial_name=trial_name)
        job_name = job_dir.name
        resolved_trial_name = trial_dir.name

        job_config = _read_json_object(job_dir / "config.json")
        job_result = _read_json_object(job_dir / "result.json")
        trial_config = _read_json_object(trial_dir / "config.json")
        trial_result = _read_json_object(trial_dir / "result.json")
        trajectory_path = trial_dir / "agent" / "trajectory.json"
        turns = _read_trajectory(trajectory_path)
        provider_usage = _harbor_provider_usage(
            job_config=job_config,
            job_result=job_result,
            trial_result=trial_result,
            trajectory_path=trajectory_path,
        )

        raw_jobs_ref = _persist_harbor_jobs_archive_ref(
            self.artifact_persistence,
            run_id=run_id,
            task_instance_id=task_instance_id,
            job_name=job_name,
            jobs_dir=job_dir,
        )
        trajectory_ref = self.artifact_persistence.persist_trajectory(
            run_id=run_id,
            task_instance_id=task_instance_id,
            turns=turns,
        )

        verifier_result = _verifier_result(
            job_name=job_name,
            trial_name=resolved_trial_name,
            trial_dir=trial_dir,
            trial_config=trial_config,
            trial_result=trial_result,
            provider_usage=provider_usage,
        )
        evaluator_report_ref = _persist_evaluator_report_ref(
            self.artifact_persistence,
            run_id=run_id,
            task_instance_id=task_instance_id,
            result=verifier_result,
        )
        verifier_result = _verifier_result(
            job_name=job_name,
            trial_name=resolved_trial_name,
            trial_dir=trial_dir,
            trial_config=trial_config,
            trial_result=trial_result,
            artifact_refs=[evaluator_report_ref.uri],
            provider_usage=provider_usage,
        )

        artifacts = [
            raw_jobs_ref,
            trajectory_ref,
            *_collected_artifact_refs(job_name=job_name, trial_name=resolved_trial_name, trial_dir=trial_dir),
            evaluator_report_ref,
        ]
        return HarborIngestionResult(
            job_name=job_name,
            trial_name=resolved_trial_name,
            job_config=job_config,
            job_result=job_result,
            trial_config=trial_config,
            trial_result=trial_result,
            turns=turns,
            artifacts=artifacts,
            evaluator_results=[verifier_result],
        )

    def failure_diagnostics(
        self,
        *,
        run_id: str,
        task_instance_id: str,
        jobs_dir: Path,
        error: Exception,
        trial_name: str | None = None,
    ) -> HarborIngestionFailureDiagnostics:
        message = str(error)
        category = _failure_category(message)
        diagnostics: dict[str, Any] = {
            "schema_version": "harbor-ingestion-diagnostics-v1",
            "category": category,
            "source": "harbor_ingestion",
            "message": message,
            "jobs_dir": jobs_dir.name,
        }
        artifacts: list[ArtifactRef] = []
        turns: list[TerminalTurn] = []

        job_dir = _optional_job_dir(jobs_dir)
        if job_dir is None:
            diagnostics["jobs_dir_status"] = "missing"
            diagnostics_ref = self.artifact_persistence.persist_harbor_ingestion_diagnostics(
                run_id=run_id,
                task_instance_id=task_instance_id,
                diagnostics=diagnostics,
            )
            return HarborIngestionFailureDiagnostics(
                category=category,
                message=message,
                turns=[],
                artifacts=[diagnostics_ref],
                metadata=_failure_metadata(diagnostics),
            )

        diagnostics["jobs_dir_status"] = "found"
        diagnostics["job_name"] = job_dir.name
        diagnostics["job_config"] = redact_sensitive_metadata(
            _optional_json_object(job_dir / "config.json")
        )
        diagnostics["job_result"] = redact_sensitive_metadata(
            _optional_json_object(job_dir / "result.json")
        )
        artifacts.append(
            _persist_harbor_jobs_archive_ref(
                self.artifact_persistence,
                run_id=run_id,
                task_instance_id=task_instance_id,
                job_name=job_dir.name,
                jobs_dir=job_dir,
            )
        )

        trial_dir = _optional_trial_dir(job_dir, trial_name=trial_name)
        if trial_dir is not None:
            diagnostics["trial_name"] = trial_dir.name
            trial_result = _optional_json_object(trial_dir / "result.json")
            diagnostics["trial_config"] = redact_sensitive_metadata(
                _optional_json_object(trial_dir / "config.json")
            )
            diagnostics["trial_result"] = redact_sensitive_metadata(
                trial_result
            )
            diagnostics["artifact_manifest"] = redact_sensitive_metadata(
                _optional_json_value(trial_dir / "artifacts" / "manifest.json")
            )
            runtime_failure = _trial_runtime_failure(message=message, trial_result=trial_result)
            if runtime_failure is not None:
                category = runtime_failure["category"]
                message = runtime_failure["message"]
                diagnostics["category"] = category
                diagnostics["message"] = message
                diagnostics.update(runtime_failure["metadata"])
            try:
                turns = _read_trajectory(trial_dir / "agent" / "trajectory.json")
            except ValueError as exc:
                diagnostics["trajectory_error"] = str(exc)
            else:
                artifacts.append(
                    self.artifact_persistence.persist_trajectory(
                        run_id=run_id,
                        task_instance_id=task_instance_id,
                        turns=turns,
                    )
                )
        else:
            diagnostics["trial_status"] = "missing"
            if trial_name:
                diagnostics["requested_trial_name"] = trial_name

        diagnostics_ref = self.artifact_persistence.persist_harbor_ingestion_diagnostics(
            run_id=run_id,
            task_instance_id=task_instance_id,
            diagnostics=diagnostics,
        )
        artifacts.append(diagnostics_ref)
        return HarborIngestionFailureDiagnostics(
            category=category,
            message=message,
            turns=turns,
            artifacts=artifacts,
            metadata=_failure_metadata(diagnostics),
        )


def _persist_harbor_jobs_archive_ref(
    artifact_persistence: ArtifactPersistence,
    *,
    run_id: str,
    task_instance_id: str,
    job_name: str,
    jobs_dir: Path,
) -> ArtifactRef:
    try:
        return artifact_persistence.persist_harbor_jobs_archive(
            run_id=run_id,
            task_instance_id=task_instance_id,
            job_name=job_name,
            jobs_dir=jobs_dir,
        )
    except Exception as exc:
        return artifact_persistence.failed_harbor_jobs_archive_ref(
            run_id=run_id,
            task_instance_id=task_instance_id,
            job_name=job_name,
            error=exc,
        )


def _persist_evaluator_report_ref(
    artifact_persistence: ArtifactPersistence,
    *,
    run_id: str,
    task_instance_id: str,
    result: EvaluatorResult,
) -> ArtifactRef:
    try:
        return artifact_persistence.persist_evaluator_report(
            run_id=run_id,
            task_instance_id=task_instance_id,
            result=result,
        )
    except Exception as exc:
        return artifact_persistence.failed_evaluator_report_ref(
            run_id=run_id,
            task_instance_id=task_instance_id,
            result=result,
            error=exc,
        )


def _resolve_job_dir(jobs_dir: Path) -> Path:
    if _is_harbor_job_dir(jobs_dir):
        return jobs_dir

    job_dirs = [path for path in sorted(jobs_dir.iterdir()) if _is_harbor_job_dir(path)]
    if not job_dirs:
        raise ValueError(f"No Harbor job directory found under: {jobs_dir}")
    if len(job_dirs) > 1:
        raise ValueError("multiple Harbor jobs found; pass a single job directory")
    return job_dirs[0]


def _optional_job_dir(jobs_dir: Path) -> Path | None:
    try:
        return _resolve_job_dir(jobs_dir)
    except ValueError:
        return None


def _resolve_trial_dir(job_dir: Path, *, trial_name: str | None) -> Path:
    if trial_name is not None:
        trial_dir = job_dir / trial_name
        if not _is_harbor_trial_dir(trial_dir):
            raise ValueError(f"Unknown Harbor trial: {trial_name}")
        return trial_dir

    trial_dirs = [path for path in sorted(job_dir.iterdir()) if _is_harbor_trial_dir(path)]
    if not trial_dirs:
        raise ValueError(f"No Harbor trial directory found under: {job_dir.name}")
    if len(trial_dirs) > 1:
        raise ValueError("multiple Harbor trials found; pass trial_name to select one")
    return trial_dirs[0]


def _optional_trial_dir(job_dir: Path, *, trial_name: str | None) -> Path | None:
    try:
        return _resolve_trial_dir(job_dir, trial_name=trial_name)
    except ValueError:
        return None


def _is_harbor_job_dir(path: Path) -> bool:
    return path.is_dir() and (path / "config.json").is_file() and (path / "result.json").is_file()


def _is_harbor_trial_dir(path: Path) -> bool:
    return _is_harbor_job_dir(path)


def _read_json_object(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise ValueError(f"Missing Harbor JSON file: {path.name}") from exc
    except json.JSONDecodeError as exc:
        raise ValueError(f"Invalid Harbor JSON file: {path.name}") from exc
    if not isinstance(payload, dict):
        raise ValueError(f"Harbor JSON file must contain an object: {path.name}")
    return payload


def _optional_json_object(path: Path) -> dict[str, Any] | None:
    try:
        return _read_json_object(path)
    except ValueError:
        return None


def _optional_json_value(path: Path) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError):
        return None


def _read_trajectory(path: Path) -> list[TerminalTurn]:
    if not path.exists():
        return []
    payload = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(payload, dict) and isinstance(payload.get("steps"), list):
        return _read_atif_trajectory(payload)
    if not isinstance(payload, list):
        raise ValueError("Harbor trajectory.json must contain a list")

    turns: list[TerminalTurn] = []
    for index, item in enumerate(payload):
        if not isinstance(item, dict):
            raise ValueError("Harbor trajectory entries must be objects")
        turns.append(
            TerminalTurn(
                turn_index=index,
                command=_required_str(item, "command"),
                cwd=str(item.get("cwd") or "/workspace"),
                started_at=_parse_datetime(item.get("started_at")),
                completed_at=_parse_datetime(item.get("completed_at")),
                exit_code=int(item.get("exit_code", 0)),
                stdout=str(item.get("stdout") or ""),
                stderr=str(item.get("stderr") or ""),
                changed_paths=_string_list(item.get("changed_paths", []), field_name="changed_paths"),
                model_call_id=_optional_str(item.get("model_call_id")),
                metadata={key: value for key, value in item.items() if key not in _TRAJECTORY_FIELDS},
            )
        )
    return turns


def _read_atif_trajectory(payload: dict[str, Any]) -> list[TerminalTurn]:
    steps = payload.get("steps")
    if not isinstance(steps, list):
        raise ValueError("Harbor ATIF trajectory steps must contain a list")
    schema_version = str(payload.get("schema_version") or "ATIF")
    session_id = _optional_str(payload.get("session_id"))
    agent = payload.get("agent") if isinstance(payload.get("agent"), dict) else {}
    cwd = "/workspace"
    if isinstance(agent.get("extra"), dict):
        cwd = str(agent["extra"].get("cwd") or cwd)

    turns: list[TerminalTurn] = []
    for step in steps:
        if not isinstance(step, dict):
            raise ValueError("Harbor ATIF trajectory steps must be objects")
        tool_calls = step.get("tool_calls")
        if not isinstance(tool_calls, list):
            if _is_atif_agent_message(step):
                turns.append(
                    _atif_message_turn(
                        step,
                        turn_index=len(turns),
                        cwd=cwd,
                        schema_version=schema_version,
                        session_id=session_id,
                    )
                )
            continue
        observation_by_call_id = _atif_observations_by_call_id(step.get("observation"))
        for tool_call in tool_calls:
            if not isinstance(tool_call, dict):
                raise ValueError("Harbor ATIF trajectory tool calls must be objects")
            call_id = _optional_str(tool_call.get("tool_call_id"))
            observation = observation_by_call_id.get(call_id or "", "")
            turns.append(
                TerminalTurn(
                    turn_index=len(turns),
                    command=_atif_command(tool_call),
                    cwd=cwd,
                    started_at=_parse_datetime(step.get("timestamp")),
                    completed_at=_parse_datetime(step.get("timestamp")),
                    exit_code=_parse_atif_exit_code(observation),
                    stdout=_parse_atif_stdout(observation),
                    stderr="",
                    changed_paths=[],
                    model_call_id=call_id,
                    metadata={
                        "trajectory_schema": schema_version,
                        "session_id": session_id,
                        "source": step.get("source"),
                        "message": step.get("message"),
                        "function_name": tool_call.get("function_name"),
                    },
                )
            )
    return turns


def _is_atif_agent_message(step: dict[str, Any]) -> bool:
    return (
        step.get("source") == "agent"
        and isinstance(step.get("message"), str)
        and bool(str(step.get("message")).strip())
    )


def _atif_message_turn(
    step: dict[str, Any],
    *,
    turn_index: int,
    cwd: str,
    schema_version: str,
    session_id: str | None,
) -> TerminalTurn:
    return TerminalTurn(
        turn_index=turn_index,
        command="agent_message",
        cwd=cwd,
        started_at=_parse_datetime(step.get("timestamp")),
        completed_at=_parse_datetime(step.get("timestamp")),
        exit_code=0,
        stdout=str(step.get("message") or ""),
        stderr="",
        changed_paths=[],
        model_call_id=None,
        metadata={
            "trajectory_schema": schema_version,
            "session_id": session_id,
            "source": step.get("source"),
            "model_name": step.get("model_name"),
            "event_type": "agent_message",
        },
    )


def _atif_observations_by_call_id(observation: Any) -> dict[str, str]:
    if not isinstance(observation, dict):
        return {}
    results = observation.get("results")
    if not isinstance(results, list):
        return {}
    observations: dict[str, str] = {}
    for result in results:
        if not isinstance(result, dict):
            continue
        call_id = _optional_str(result.get("source_call_id"))
        if call_id:
            observations[call_id] = str(result.get("content") or "")
    return observations


def _atif_command(tool_call: dict[str, Any]) -> str:
    arguments = tool_call.get("arguments")
    if isinstance(arguments, dict):
        command = arguments.get("cmd") or arguments.get("command")
        if isinstance(command, str) and command.strip():
            return command
        return json.dumps(arguments, sort_keys=True)
    function_name = tool_call.get("function_name")
    if isinstance(function_name, str) and function_name.strip():
        return function_name
    return "atif_tool_call"


def _parse_atif_exit_code(observation: str) -> int:
    match = re.search(r"Process exited with code (-?\d+)", observation)
    if not match:
        return 0
    return int(match.group(1))


def _parse_atif_stdout(observation: str) -> str:
    marker = "\nOutput:\n"
    if marker not in observation:
        return observation
    return observation.split(marker, 1)[1]


def _verifier_result(
    *,
    job_name: str,
    trial_name: str,
    trial_dir: Path,
    trial_config: dict[str, Any],
    trial_result: dict[str, Any],
    artifact_refs: list[str] | None = None,
    provider_usage: dict[str, Any] | None = None,
) -> EvaluatorResult:
    reward_payload = _read_reward_payload(trial_dir=trial_dir, trial_result=trial_result)
    reward = reward_payload["reward"]
    verifier_version = str(
        reward_payload.get("verifier_version")
        or trial_config.get("verifier_version")
        or trial_result.get("verifier_version")
        or "unknown"
    )
    metadata: dict[str, Any] = {
        "job_name": job_name,
        "trial_name": trial_name,
        "verifier_version": verifier_version,
    }
    if provider_usage is not None:
        metadata["provider_usage"] = dict(provider_usage)

    return EvaluatorResult(
        evaluator_id="harbor-verifier",
        mode="harbor_verifier",
        status="completed",
        score=reward,
        metrics=reward_payload,
        verbal_feedback="",
        judge=None,
        artifact_refs=list(artifact_refs or []),
        metadata=metadata,
    )


def _harbor_provider_usage(
    *,
    job_config: dict[str, Any],
    job_result: dict[str, Any],
    trial_result: dict[str, Any],
    trajectory_path: Path,
) -> dict[str, Any] | None:
    trajectory_payload = _optional_json_value(trajectory_path)
    model_name = _model_name_from_trajectory(trajectory_payload) or _model_name_from_job_config(job_config)
    provider = _provider_from_job_config(job_config)
    candidates: list[tuple[str, Any]] = []
    if isinstance(trajectory_payload, dict):
        candidates.append(("harbor_atif_final_metrics", trajectory_payload.get("final_metrics")))
    candidates.extend(
        [
            ("harbor_trial_final_metrics", trial_result.get("final_metrics")),
            ("harbor_job_final_metrics", job_result.get("final_metrics")),
        ]
    )
    for source, metrics in candidates:
        if not isinstance(metrics, dict):
            continue
        usage = normalize_model_provider_usage(
            metrics,
            source=source,
            provider=provider,
            model_name=model_name,
        )
        if usage is not None:
            return usage
    return None


def _model_name_from_trajectory(payload: Any) -> str | None:
    if not isinstance(payload, dict):
        return None
    agent = payload.get("agent")
    if not isinstance(agent, dict):
        return None
    return _optional_str(agent.get("model_name"))


def _model_name_from_job_config(job_config: dict[str, Any]) -> str | None:
    model = job_config.get("model")
    if isinstance(model, str):
        return _optional_str(model)
    if isinstance(model, dict):
        return (
            _optional_str(model.get("model_name"))
            or _optional_str(model.get("name"))
            or _optional_str(model.get("id"))
        )
    return None


def _provider_from_job_config(job_config: dict[str, Any]) -> str | None:
    provider = _optional_str(job_config.get("provider"))
    if provider is not None:
        return provider
    model = job_config.get("model")
    if isinstance(model, dict):
        return _optional_str(model.get("provider"))
    return None


def _read_reward_payload(*, trial_dir: Path, trial_result: dict[str, Any]) -> dict[str, Any]:
    reward_json = trial_dir / "verifier" / "reward.json"
    reward_txt = trial_dir / "verifier" / "reward.txt"
    payload: dict[str, Any]
    if reward_json.exists():
        payload = _read_json_object(reward_json)
        raw_reward = payload.get("reward", payload.get("score"))
    elif reward_txt.exists():
        payload = {}
        raw_reward = reward_txt.read_text(encoding="utf-8").strip()
    elif (payload := _trial_result_reward_payload(trial_result)) is not None:
        raw_reward = payload.get("reward", payload.get("score"))
    else:
        raise ValueError("Missing Harbor verifier reward.txt or reward.json")

    try:
        reward = float(raw_reward)
    except (TypeError, ValueError) as exc:
        raise ValueError("Harbor verifier reward must be numeric") from exc
    if not 0 <= reward <= 1:
        raise ValueError("Harbor verifier reward must be between 0 and 1")

    return {"reward": reward, **payload}


def _trial_result_reward_payload(trial_result: dict[str, Any]) -> dict[str, Any] | None:
    verifier_result = trial_result.get("verifier_result")
    if not isinstance(verifier_result, dict):
        return None
    rewards = verifier_result.get("rewards")
    if not isinstance(rewards, dict):
        return None
    return dict(rewards)


def _failure_category(message: str) -> str:
    if "Missing Harbor verifier reward" in message:
        return "harbor_verifier_missing_reward"
    if "Harbor verifier reward must be numeric" in message or "Harbor verifier reward must be between" in message:
        return "harbor_verifier_invalid_reward"
    if "No Harbor job directory" in message:
        return "harbor_jobs_missing"
    if "No Harbor trial" in message or "Unknown Harbor trial" in message or "multiple Harbor trials" in message:
        return "harbor_trial_resolution_failed"
    if "trajectory" in message:
        return "harbor_trajectory_parse_failed"
    return "harbor_ingestion_failed"


def _trial_runtime_failure(*, message: str, trial_result: dict[str, Any] | None) -> dict[str, Any] | None:
    if "Missing Harbor verifier reward" not in message:
        return None
    if not isinstance(trial_result, dict):
        return None
    exception_info = trial_result.get("exception_info")
    if not isinstance(exception_info, dict):
        return None
    exception_type = _optional_str(exception_info.get("exception_type")) or "HarborTrialException"
    exception_message = _clean_exception_message(
        _optional_str(exception_info.get("exception_message")) or "Harbor trial failed before verifier"
    )
    return {
        "category": "harbor_agent_runtime_failed",
        "message": f"Harbor trial failed before verifier: {exception_type}: {exception_message}",
        "metadata": {
            "trial_exception_type": exception_type,
            "trial_exception_message": exception_message,
        },
    }


def _clean_exception_message(message: str) -> str:
    redacted = re.sub(
        r"(?i)\\b([A-Z0-9_]*(?:API_KEY|TOKEN|SECRET|PASSWORD|AUTHORIZATION)[A-Z0-9_]*=)[^\\s,;]+",
        r"\\1[redacted]",
        message,
    )
    redacted = re.sub(r"(?i)Bearer\\s+[^\\s,;]+", "Bearer [redacted]", redacted)
    return redacted[:1000] + (" ... [truncated]" if len(redacted) > 1000 else "")


def _failure_metadata(diagnostics: dict[str, Any]) -> dict[str, Any]:
    keys = [
        "schema_version",
        "category",
        "source",
        "message",
        "jobs_dir_status",
        "job_name",
        "trial_name",
        "trial_status",
        "requested_trial_name",
        "trajectory_error",
        "trial_exception_type",
        "trial_exception_message",
    ]
    return {key: diagnostics[key] for key in keys if key in diagnostics}


def _collected_artifact_refs(*, job_name: str, trial_name: str, trial_dir: Path) -> list[ArtifactRef]:
    manifest_path = trial_dir / "artifacts" / "manifest.json"
    if not manifest_path.exists():
        return []
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    if not isinstance(payload, list):
        raise ValueError("Harbor artifacts manifest must contain a list")

    artifacts: list[ArtifactRef] = []
    for index, item in enumerate(payload):
        if not isinstance(item, dict):
            raise ValueError("Harbor artifact manifest entries must be objects")
        destination = _safe_relative_path(str(item.get("destination") or f"artifact-{index}"))
        artifact_path = trial_dir / destination
        size_bytes = artifact_path.stat().st_size if artifact_path.is_file() else None
        sha256 = _sha256(artifact_path) if artifact_path.is_file() else None
        artifacts.append(
            ArtifactRef(
                artifact_id=f"{_safe_component(job_name)}-{_safe_component(trial_name)}-artifact-{index}",
                kind=ArtifactKind.GENERATED_FILE,
                uri=f"harbor-artifact://{_safe_component(job_name)}/{_safe_component(trial_name)}/{destination}",
                media_type="application/octet-stream",
                sha256=sha256,
                size_bytes=size_bytes,
                metadata={
                    "job_name": job_name,
                    "trial_name": trial_name,
                    "destination": destination,
                    "type": item.get("type"),
                    "status": item.get("status"),
                },
            )
        )
    return artifacts


def _required_str(item: dict[str, Any], field_name: str) -> str:
    value = item.get(field_name)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"Harbor trajectory field must be a non-empty string: {field_name}")
    return value


def _optional_str(value: Any) -> str | None:
    return value if isinstance(value, str) and value.strip() else None


def _string_list(value: Any, *, field_name: str) -> list[str]:
    if not isinstance(value, list) or any(not isinstance(item, str) or not item.strip() for item in value):
        raise ValueError(f"Harbor trajectory field must be a list of strings: {field_name}")
    return value


def _parse_datetime(value: Any) -> datetime:
    if not isinstance(value, str) or not value.strip():
        return datetime.now(timezone.utc)
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"Invalid Harbor trajectory timestamp: {value}") from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed


def _safe_relative_path(value: str) -> str:
    path = PurePosixPath(value)
    if path.is_absolute() or any(part in {"", ".", ".."} for part in path.parts):
        raise ValueError(f"Unsafe Harbor artifact destination: {value}")
    return path.as_posix()


def _safe_component(value: str) -> str:
    safe = re.sub(r"[^A-Za-z0-9_.-]+", "-", value.strip()).strip(".-")
    if not safe:
        raise ValueError("path component must contain at least one safe character")
    return quote(safe, safe="A-Za-z0-9_.-")


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _turn_payload(turn: TerminalTurn) -> dict[str, Any]:
    return {
        "turn_index": turn.turn_index,
        "command": turn.command,
        "cwd": turn.cwd,
        "started_at": _datetime(turn.started_at),
        "completed_at": _datetime(turn.completed_at),
        "exit_code": turn.exit_code,
        "stdout": turn.stdout,
        "stderr": turn.stderr,
        "changed_paths": list(turn.changed_paths),
        "model_call_id": turn.model_call_id,
        "metadata": dict(turn.metadata),
    }


def _artifact_payload(artifact: ArtifactRef) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "artifact_id": artifact.artifact_id,
        "kind": artifact.kind.value,
        "media_type": artifact.media_type,
        "metadata": dict(artifact.metadata),
    }
    if not artifact.uri.startswith("file:"):
        payload["uri"] = artifact.uri
    if artifact.size_bytes is not None:
        payload["size_bytes"] = artifact.size_bytes
    if artifact.sha256 is not None:
        payload["sha256"] = artifact.sha256
    return payload


def _evaluator_payload(result: EvaluatorResult) -> dict[str, Any]:
    return {
        "evaluator_id": result.evaluator_id,
        "mode": result.mode,
        "status": result.status,
        "score": result.score,
        "metrics": dict(result.metrics),
        "verbal_feedback": result.verbal_feedback,
        "artifact_refs": [_safe_artifact_ref(ref) for ref in result.artifact_refs],
        "metadata": dict(result.metadata),
        "created_at": _datetime(result.created_at),
    }


def _safe_artifact_ref(ref: str) -> str:
    if ref.startswith("file:"):
        return PurePosixPath(ref).name
    return ref


def _datetime(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


_TRAJECTORY_FIELDS = {
    "command",
    "cwd",
    "started_at",
    "completed_at",
    "exit_code",
    "stdout",
    "stderr",
    "changed_paths",
    "model_call_id",
}
