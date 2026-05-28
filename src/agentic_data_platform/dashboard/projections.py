from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import PurePosixPath
import re
from typing import Any
from urllib.parse import urlparse, urlunparse

from agentic_data_platform.domain.run_records import ArtifactRef, EvaluatorConfig, EvaluatorResult, RunRecord, RunStatus


@dataclass(frozen=True)
class RunDashboardProjection:
    run: RunRecord

    @classmethod
    def from_run(cls, run: RunRecord) -> RunDashboardProjection:
        return cls(run=run)

    def to_dict(self) -> dict[str, Any]:
        run = self.run
        payload: dict[str, Any] = {
            "run_id": run.run_id,
            "project": {
                "project_id": run.project_id,
                "owner_team": run.owner_team,
            },
            "created_by_user_id": run.created_by_user_id,
            "status": run.status.value,
            "failure_reason": run.failure_reason,
            "progress": _progress(run),
            "task": {
                "benchmark_suite": run.task.benchmark_suite,
                "benchmark_version": run.task.benchmark_version,
                "task_family": run.task.task_family,
                "instance_id": run.task.instance_id,
                "source_uri": run.task.source_uri,
            },
            "model": {
                "provider": run.model.provider,
                "model_name": run.model.model_name,
                "model_version": run.model.model_version,
                "prompt_template_version": run.model.prompt_template_version,
            },
            "runner": {
                "kind": run.runner.kind.value,
                "sandbox_backend": run.runner.sandbox_backend.value,
                "image": run.runner.image,
                "internet_access": run.runner.internet_access,
            },
            "evaluators": [_evaluator_config(config) for config in run.evaluator_configs],
            "artifacts": [_artifact_link(ref) for ref in run.artifacts],
            "created_at": _datetime(run.created_at),
            "updated_at": _datetime(run.updated_at),
        }

        if run.evaluator_result is not None:
            payload["evaluator"] = _evaluator_summary(run.evaluator_result)

        return payload


def _progress(run: RunRecord) -> dict[str, Any]:
    return {
        "status": run.status.value,
        "is_terminal": run.status in {RunStatus.SUCCEEDED, RunStatus.FAILED, RunStatus.CANCELED},
        "turn_count": len(run.trajectory),
        "artifact_count": len(run.artifacts),
        "updated_at": _datetime(run.updated_at),
    }


def _artifact_link(ref: ArtifactRef) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "artifact_id": ref.artifact_id,
        "kind": ref.kind.value,
        "media_type": ref.media_type,
    }

    if ref.size_bytes is not None:
        payload["size_bytes"] = ref.size_bytes

    storage_key = _safe_storage_key(ref.metadata.get("storage_key"))
    if storage_key:
        payload["storage_key"] = storage_key

    safe_uri = _safe_external_uri(ref.uri)
    if safe_uri is not None:
        payload["uri"] = safe_uri

    return payload


def _evaluator_summary(result: EvaluatorResult) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "evaluator_id": result.evaluator_id,
        "status": result.status,
        "metrics": result.metrics,
        "verbal_feedback_summary": _feedback_summary(result.verbal_feedback),
        "judge": {
            "provider": result.judge.provider,
            "model_name": result.judge.model_name,
            "model_version": result.judge.model_version,
            "rubric_version": result.judge.rubric_version,
        },
        "artifact_refs": [_safe_artifact_ref(ref) for ref in result.artifact_refs],
    }

    if result.score is not None:
        payload["score"] = result.score

    if result.failure_reason:
        payload["failure_reason"] = result.failure_reason

    return payload


def _evaluator_config(config: EvaluatorConfig) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "evaluator_id": config.evaluator_id,
        "mode": config.mode,
        "metadata": dict(config.metadata),
    }
    if config.judge is not None:
        payload["judge"] = {
            "provider": config.judge.provider,
            "model_name": config.judge.model_name,
            "model_version": config.judge.model_version,
            "rubric_version": config.judge.rubric_version,
        }
    return payload


def _feedback_summary(feedback: str, *, max_chars: int = 240) -> str:
    feedback = " ".join(feedback.split())
    if len(feedback) <= max_chars:
        return feedback
    return feedback[: max_chars - 3].rstrip() + "..."


def _safe_artifact_ref(ref: str) -> str:
    safe_uri = _safe_external_uri(ref)
    return safe_uri if safe_uri is not None else _basename_or_ref(ref)


def _safe_external_uri(uri: str) -> str | None:
    parsed = urlparse(uri)
    if parsed.scheme == "file":
        return None

    if parsed.scheme == "" and uri.startswith("/"):
        return None

    if parsed.query or parsed.fragment:
        return urlunparse((parsed.scheme, parsed.netloc, parsed.path, "", "", ""))

    return uri


def _safe_storage_key(value: object) -> str | None:
    if not isinstance(value, str):
        return None

    key = value.strip().replace("\\", "/")
    if not key:
        return None

    parsed = urlparse(key)
    if parsed.scheme or parsed.netloc or parsed.query or parsed.fragment:
        return None

    if key.startswith("/") or key.startswith("~") or re.match(r"^[A-Za-z]:", key):
        return None

    parts = PurePosixPath(key).parts
    if any(part in {"", ".", ".."} for part in parts):
        return None

    return key


def _basename_or_ref(ref: str) -> str:
    parsed = urlparse(ref)
    path = parsed.path or ref
    return path.rstrip("/").split("/")[-1] or ref


def _datetime(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
