from __future__ import annotations

from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

from fastapi import Depends, FastAPI, HTTPException, Request
from pydantic import BaseModel, Field, field_validator
from sqlalchemy.orm import Session

from agentic_data_platform.dashboard.projections import RunDashboardProjection
from agentic_data_platform.domain.run_records import (
    BenchmarkTaskInstance,
    EvaluatorConfig,
    JudgeConfig,
    ModelConfig,
    RunnerConfig,
    RunRecord,
    RunStatus,
)
from agentic_data_platform.persistence.repositories import IdentityRepository, ProjectRepository, RunRepository


class BenchmarkTaskInstanceRequest(BaseModel):
    benchmark_suite: str
    benchmark_version: str
    task_family: str
    instance_id: str
    source_uri: str
    input_artifact_refs: list[str]
    required_artifacts: list[str]
    metadata: dict[str, Any] = Field(default_factory=dict)


class ModelConfigRequest(BaseModel):
    provider: str
    model_name: str
    mode: str
    prompt_template_version: str
    model_version: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class RunnerConfigRequest(BaseModel):
    kind: str
    sandbox_backend: str
    image: str
    entrypoint: list[str]
    internet_access: bool
    resource_limits: dict[str, int | float]
    metadata: dict[str, Any] = Field(default_factory=dict)


class JudgeConfigRequest(BaseModel):
    provider: str
    model_name: str
    rubric_version: str
    model_version: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class EvaluatorConfigRequest(BaseModel):
    evaluator_id: str
    mode: str
    judge: JudgeConfigRequest | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class RunCreateRequest(BaseModel):
    run_id: str | None = None
    project_id: str
    owner_team: str
    task: BenchmarkTaskInstanceRequest
    model: ModelConfigRequest
    runner: RunnerConfigRequest
    evaluators: list[EvaluatorConfigRequest]
    created_by_user_id: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class RunActionRequest(BaseModel):
    reason: str
    actor_user_id: str | None = None

    @field_validator("reason")
    @classmethod
    def require_non_blank_reason(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("reason must be a non-empty string")
        return value


def register_run_routes(app: FastAPI, session_dependency) -> None:
    @app.post("/runs", status_code=201, tags=["runs"], responses=_example_response(_RUN_CREATED_EXAMPLE, status_code=201))
    def create_run(
        payload: RunCreateRequest,
        request: Request,
        session: Session = Depends(session_dependency),
    ) -> dict[str, Any]:
        try:
            ProjectRepository(session).get_project(payload.project_id)
        except KeyError as exc:
            raise HTTPException(status_code=404, detail="Project not found") from exc
        _validate_user_or_404(session, payload.created_by_user_id)

        try:
            run = _run_from_create_request(payload)
        except (TypeError, ValueError) as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc

        try:
            created = RunRepository(session).create_run(
                run,
                created_by_user_id=payload.created_by_user_id,
                request_id=_request_id(request),
            )
        except ValueError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc

        return _run_detail_payload(request, session, created)

    @app.get("/runs", tags=["runs"], responses=_example_response(_RUNS_EXAMPLE))
    def list_runs(
        request: Request,
        project_id: str | None = None,
        status: str | None = None,
        benchmark_suite: str | None = None,
        task_family: str | None = None,
        task_instance_id: str | None = None,
        created_by_user_id: str | None = None,
        created_after: datetime | None = None,
        created_before: datetime | None = None,
        session: Session = Depends(session_dependency),
    ) -> dict[str, Any]:
        repository = RunRepository(session)
        projections = [
            RunDashboardProjection.from_run(run).to_dict()
            for run in repository.list_runs(
                project_id=project_id,
                status=status,
                benchmark_suite=benchmark_suite,
                task_family=task_family,
                task_instance_id=task_instance_id,
                created_by_user_id=created_by_user_id,
                created_after=created_after,
                created_before=created_before,
            )
        ]
        return _with_request_id(request, {"runs": projections})

    @app.get("/runs/{run_id}", tags=["runs"], responses=_example_response(_RUN_EXAMPLE))
    def get_run(
        run_id: str,
        request: Request,
        session: Session = Depends(session_dependency),
    ) -> dict[str, Any]:
        run = _get_run_or_404(session, run_id)
        return _run_detail_payload(request, session, run)

    @app.post("/runs/{run_id}/cancel", tags=["runs"], responses=_example_response(_RUN_CANCELED_EXAMPLE))
    def cancel_run(
        run_id: str,
        payload: RunActionRequest,
        request: Request,
        session: Session = Depends(session_dependency),
    ) -> dict[str, Any]:
        _validate_user_or_404(session, payload.actor_user_id)
        try:
            run = RunRepository(session).cancel_run(
                run_id,
                reason=payload.reason,
                actor_user_id=payload.actor_user_id,
                request_id=_request_id(request),
            )
        except KeyError as exc:
            raise HTTPException(status_code=404, detail=f"Unknown run: {run_id}") from exc
        except ValueError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        return _run_detail_payload(request, session, run)

    @app.post("/runs/{run_id}/retry", tags=["runs"], responses=_example_response(_RUN_RETRIED_EXAMPLE))
    def retry_run(
        run_id: str,
        payload: RunActionRequest,
        request: Request,
        session: Session = Depends(session_dependency),
    ) -> dict[str, Any]:
        _validate_user_or_404(session, payload.actor_user_id)
        try:
            run = RunRepository(session).retry_run(
                run_id,
                reason=payload.reason,
                actor_user_id=payload.actor_user_id,
                request_id=_request_id(request),
            )
        except KeyError as exc:
            raise HTTPException(status_code=404, detail=f"Unknown run: {run_id}") from exc
        except ValueError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        return _run_detail_payload(request, session, run)

    @app.get("/runs/{run_id}/artifacts", tags=["runs"], responses=_example_response(_ARTIFACTS_EXAMPLE))
    def list_run_artifacts(
        run_id: str,
        request: Request,
        session: Session = Depends(session_dependency),
    ) -> dict[str, Any]:
        run = _get_run_or_404(session, run_id)
        artifacts = RunDashboardProjection.from_run(run).to_dict()["artifacts"]
        return _with_request_id(request, {"run_id": run_id, "artifacts": artifacts})

    @app.get("/runs/{run_id}/evaluation", tags=["runs"], responses=_example_response(_EVALUATION_EXAMPLE))
    def get_run_evaluation(
        run_id: str,
        request: Request,
        session: Session = Depends(session_dependency),
    ) -> dict[str, Any]:
        run = _get_run_or_404(session, run_id)
        projection = RunDashboardProjection.from_run(run).to_dict()
        evaluation = projection.get("evaluator")
        if evaluation is None:
            raise HTTPException(status_code=404, detail=f"Run has no evaluation: {run_id}")
        return _with_request_id(request, {"run_id": run_id, "evaluation": evaluation})


def _get_run_or_404(session: Session, run_id: str):
    try:
        return RunRepository(session).get_run(run_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=f"Unknown run: {run_id}") from exc


def _run_from_create_request(payload: RunCreateRequest) -> RunRecord:
    return RunRecord.create(
        run_id=payload.run_id or f"run_{uuid4().hex}",
        project_id=payload.project_id,
        owner_team=payload.owner_team,
        task=BenchmarkTaskInstance(**payload.task.model_dump()),
        model=ModelConfig(**payload.model.model_dump()),
        runner=RunnerConfig(**payload.runner.model_dump()),
        evaluator_configs=[_evaluator_config(config) for config in payload.evaluators],
        created_by_user_id=payload.created_by_user_id,
        metadata=dict(payload.metadata),
    )


def _evaluator_config(payload: EvaluatorConfigRequest) -> EvaluatorConfig:
    judge = JudgeConfig(**payload.judge.model_dump()) if payload.judge is not None else None
    return EvaluatorConfig(
        evaluator_id=payload.evaluator_id,
        mode=payload.mode,
        judge=judge,
        metadata=dict(payload.metadata),
    )


def _validate_user_or_404(session: Session, user_id: str | None) -> None:
    if user_id is None:
        return
    try:
        IdentityRepository(session).get_user(user_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=f"User not found: {user_id}") from exc


def _run_detail_payload(request: Request, session: Session, run: RunRecord) -> dict[str, Any]:
    repository = RunRepository(session)
    events = [_status_event_payload(event) for event in repository.list_status_events(run.run_id)]
    return _with_request_id(
        request,
        {
            "run": RunDashboardProjection.from_run(run).to_dict(),
            "lifecycle_events": events,
        },
    )


def _status_event_payload(event) -> dict[str, Any]:
    return {
        "event_id": event.event_id,
        "run_id": event.run_id,
        "attempt_id": event.attempt_id,
        "event_type": event.event_type,
        "from_status": event.from_status.value if isinstance(event.from_status, RunStatus) else event.from_status,
        "to_status": event.to_status.value,
        "reason": event.reason,
        "actor_user_id": event.actor_user_id,
        "request_id": event.request_id,
        "metadata": dict(event.metadata),
        "created_at": _datetime(event.created_at),
    }


def _with_request_id(request: Request, payload: dict[str, Any]) -> dict[str, Any]:
    request_id = getattr(request.state, "request_id", None)
    if request_id:
        payload["request_id"] = request_id
    return payload


def _example_response(example: dict[str, Any], *, status_code: int = 200) -> dict[int, dict[str, Any]]:
    return {status_code: {"content": {"application/json": {"example": example}}}}


def _request_id(request: Request) -> str | None:
    return getattr(request.state, "request_id", None)


def _datetime(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


_RUN_PAYLOAD_EXAMPLE: dict[str, Any] = {
    "run_id": "run_001",
    "project": {"project_id": "latent-skill-pilot", "owner_team": "pilot group"},
    "created_by_user_id": "[REDACTED_OWNER]",
    "status": "succeeded",
    "failure_reason": None,
    "progress": {
        "status": "succeeded",
        "is_terminal": True,
        "turn_count": 12,
        "artifact_count": 3,
        "updated_at": "2026-05-28T12:30:00Z",
    },
    "task": {
        "benchmark_suite": "SkillFlow",
        "benchmark_version": "hf:zhang-ziao/SkillFlow-Task@2026-05-28",
        "task_family": "OCR-Data-Extraction",
        "instance_id": "task_family_invoice_images",
        "source_uri": "https://huggingface.co/datasets/zhang-ziao/SkillFlow-Task",
    },
    "model": {
        "provider": "openai",
        "model_name": "gpt-5",
        "model_version": "2026-05-28",
        "prompt_template_version": "terminal-agent-v0",
    },
    "runner": {
        "kind": "original_benchmark",
        "sandbox_backend": "docker_terminal",
        "image": "python:3.12-slim",
        "internet_access": True,
    },
    "evaluators": [
        {
            "evaluator_id": "llm-judge-v0",
            "mode": "llm_judge",
            "metadata": {"evaluation_mode": "llm_judge"},
            "judge": {
                "provider": "openai",
                "model_name": "gpt-5",
                "model_version": None,
                "rubric_version": "latent-skill-benchmark-2026-05-28",
            },
        }
    ],
    "artifacts": [
        {
            "artifact_id": "run_001-trajectory",
            "kind": "trajectory",
            "media_type": "application/x-ndjson",
            "size_bytes": 512,
            "storage_key": "runs/run_001/tasks/task_family_invoice_images/trajectory/trajectory.jsonl",
            "uri": "minio://runs/run_001/tasks/task_family_invoice_images/trajectory/trajectory.jsonl",
        }
    ],
    "evaluator": {
        "evaluator_id": "llm-judge-v0",
        "status": "completed",
        "score": 0.91,
        "metrics": {"task_success": True},
        "verbal_feedback_summary": "The generated workbook is correct.",
        "judge": {
            "provider": "openai",
            "model_name": "gpt-5",
            "model_version": None,
            "rubric_version": "latent-skill-benchmark-2026-05-28",
        },
        "artifact_refs": ["report.json"],
    },
    "created_at": "2026-05-28T12:00:00Z",
    "updated_at": "2026-05-28T12:30:00Z",
}
_LIFECYCLE_EVENT_EXAMPLE = {
    "event_id": "evt_001",
    "run_id": "run_001",
    "attempt_id": "run_001:attempt:1",
    "event_type": "run.created",
    "from_status": None,
    "to_status": "queued",
    "reason": None,
    "actor_user_id": "[REDACTED_OWNER]",
    "request_id": "req_123",
    "metadata": {},
    "created_at": "2026-05-28T12:00:00Z",
}
_RUNS_EXAMPLE = {"runs": [_RUN_PAYLOAD_EXAMPLE], "request_id": "req_123"}
_RUN_EXAMPLE = {
    "run": _RUN_PAYLOAD_EXAMPLE,
    "lifecycle_events": [_LIFECYCLE_EVENT_EXAMPLE],
    "request_id": "req_123",
}
_RUN_CREATED_EXAMPLE = _RUN_EXAMPLE
_RUN_CANCELED_EXAMPLE = {
    "run": {**_RUN_PAYLOAD_EXAMPLE, "status": "canceled", "failure_reason": "user requested cancellation"},
    "lifecycle_events": [
        _LIFECYCLE_EVENT_EXAMPLE,
        {**_LIFECYCLE_EVENT_EXAMPLE, "event_id": "evt_002", "event_type": "run.canceled", "from_status": "queued", "to_status": "canceled"},
    ],
    "request_id": "req_123",
}
_RUN_RETRIED_EXAMPLE = {
    "run": {**_RUN_PAYLOAD_EXAMPLE, "status": "queued", "failure_reason": None},
    "lifecycle_events": [
        _LIFECYCLE_EVENT_EXAMPLE,
        {**_LIFECYCLE_EVENT_EXAMPLE, "event_id": "evt_003", "attempt_id": "run_001:attempt:2", "event_type": "run.retried", "from_status": "canceled", "to_status": "queued"},
    ],
    "request_id": "req_123",
}
_ARTIFACTS_EXAMPLE = {
    "run_id": "run_001",
    "artifacts": _RUN_PAYLOAD_EXAMPLE["artifacts"],
    "request_id": "req_123",
}
_EVALUATION_EXAMPLE = {
    "run_id": "run_001",
    "evaluation": _RUN_PAYLOAD_EXAMPLE["evaluator"],
    "request_id": "req_123",
}
