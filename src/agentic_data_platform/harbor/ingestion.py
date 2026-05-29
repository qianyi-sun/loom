from __future__ import annotations

import json
import re
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
        turns = _read_trajectory(trial_dir / "agent" / "trajectory.json")

        raw_jobs_ref = self.artifact_persistence.persist_harbor_jobs_archive(
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
        )
        evaluator_report_ref = self.artifact_persistence.persist_evaluator_report(
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


def _resolve_job_dir(jobs_dir: Path) -> Path:
    if _is_harbor_job_dir(jobs_dir):
        return jobs_dir

    job_dirs = [path for path in sorted(jobs_dir.iterdir()) if _is_harbor_job_dir(path)]
    if not job_dirs:
        raise ValueError(f"No Harbor job directory found under: {jobs_dir}")
    if len(job_dirs) > 1:
        raise ValueError("multiple Harbor jobs found; pass a single job directory")
    return job_dirs[0]


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


def _read_trajectory(path: Path) -> list[TerminalTurn]:
    if not path.exists():
        return []
    payload = json.loads(path.read_text(encoding="utf-8"))
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


def _verifier_result(
    *,
    job_name: str,
    trial_name: str,
    trial_dir: Path,
    trial_config: dict[str, Any],
    trial_result: dict[str, Any],
    artifact_refs: list[str] | None = None,
) -> EvaluatorResult:
    reward_payload = _read_reward_payload(trial_dir)
    reward = reward_payload["reward"]
    verifier_version = str(
        reward_payload.get("verifier_version")
        or trial_config.get("verifier_version")
        or trial_result.get("verifier_version")
        or "unknown"
    )
    return EvaluatorResult(
        evaluator_id="harbor-verifier",
        mode="harbor_verifier",
        status="completed",
        score=reward,
        metrics=reward_payload,
        verbal_feedback="",
        judge=None,
        artifact_refs=list(artifact_refs or []),
        metadata={
            "job_name": job_name,
            "trial_name": trial_name,
            "verifier_version": verifier_version,
        },
    )


def _read_reward_payload(trial_dir: Path) -> dict[str, Any]:
    reward_json = trial_dir / "verifier" / "reward.json"
    reward_txt = trial_dir / "verifier" / "reward.txt"
    payload: dict[str, Any]
    if reward_json.exists():
        payload = _read_json_object(reward_json)
        raw_reward = payload.get("reward", payload.get("score"))
    elif reward_txt.exists():
        payload = {}
        raw_reward = reward_txt.read_text(encoding="utf-8").strip()
    else:
        raise ValueError("Missing Harbor verifier reward.txt or reward.json")

    try:
        reward = float(raw_reward)
    except (TypeError, ValueError) as exc:
        raise ValueError("Harbor verifier reward must be numeric") from exc
    if not 0 <= reward <= 1:
        raise ValueError("Harbor verifier reward must be between 0 and 1")

    return {"reward": reward, **payload}


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
        artifacts.append(
            ArtifactRef(
                artifact_id=f"{_safe_component(job_name)}-{_safe_component(trial_name)}-artifact-{index}",
                kind=ArtifactKind.GENERATED_FILE,
                uri=f"harbor-artifact://{_safe_component(job_name)}/{_safe_component(trial_name)}/{destination}",
                media_type="application/octet-stream",
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
