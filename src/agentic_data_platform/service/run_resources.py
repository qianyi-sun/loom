from __future__ import annotations

from typing import Any

from fastapi import Depends, FastAPI, HTTPException, Request
from sqlalchemy.orm import Session

from agentic_data_platform.dashboard.projections import RunDashboardProjection
from agentic_data_platform.persistence.repositories import RunRepository


def register_run_routes(app: FastAPI, session_dependency) -> None:
    @app.get("/runs", tags=["runs"], responses=_example_response(_RUNS_EXAMPLE))
    def list_runs(
        request: Request,
        project_id: str | None = None,
        status: str | None = None,
        session: Session = Depends(session_dependency),
    ) -> dict[str, Any]:
        repository = RunRepository(session)
        projections = [
            RunDashboardProjection.from_run(run).to_dict()
            for run in repository.list_runs(project_id=project_id, status=status)
        ]
        return _with_request_id(request, {"runs": projections})

    @app.get("/runs/{run_id}", tags=["runs"], responses=_example_response(_RUN_EXAMPLE))
    def get_run(
        run_id: str,
        request: Request,
        session: Session = Depends(session_dependency),
    ) -> dict[str, Any]:
        run = _get_run_or_404(session, run_id)
        return _with_request_id(request, {"run": RunDashboardProjection.from_run(run).to_dict()})

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


def _with_request_id(request: Request, payload: dict[str, Any]) -> dict[str, Any]:
    request_id = getattr(request.state, "request_id", None)
    if request_id:
        payload["request_id"] = request_id
    return payload


def _example_response(example: dict[str, Any]) -> dict[int, dict[str, Any]]:
    return {200: {"content": {"application/json": {"example": example}}}}


_RUN_PAYLOAD_EXAMPLE: dict[str, Any] = {
    "run_id": "run_001",
    "project": {"project_id": "latent-skill-pilot", "owner_team": "pilot group"},
    "status": "succeeded",
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
_RUNS_EXAMPLE = {"runs": [_RUN_PAYLOAD_EXAMPLE], "request_id": "req_123"}
_RUN_EXAMPLE = {"run": _RUN_PAYLOAD_EXAMPLE, "request_id": "req_123"}
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
