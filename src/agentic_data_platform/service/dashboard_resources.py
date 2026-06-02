from __future__ import annotations

from collections import Counter, defaultdict
from datetime import datetime, timezone
from typing import Any, Callable

from fastapi import Depends, FastAPI, Request
from sqlalchemy.orm import Session

from agentic_data_platform.domain.run_records import RunStatus
from agentic_data_platform.persistence.repositories import RunDashboardProgressRecord, RunRepository
from agentic_data_platform.service.security import (
    accessible_project_ids,
    require_authenticated_user,
    require_project_role,
)


def register_dashboard_routes(app: FastAPI, session_dependency: Callable) -> None:
    @app.get("/dashboard/progress", tags=["dashboard"], responses=_example_response(_PROGRESS_EXAMPLE))
    def get_dashboard_progress(
        request: Request,
        project_id: str | None = None,
        owner_team: str | None = None,
        session: Session = Depends(session_dependency),
    ) -> dict[str, Any]:
        auth = require_authenticated_user(request, session)
        if project_id is not None:
            require_project_role(session, auth, project_id, minimum_role="viewer")
            allowed_project_ids = {project_id}
        else:
            allowed_project_ids = accessible_project_ids(session, auth)

        visible_runs = [
            run
            for run in RunRepository(session).list_dashboard_progress_records(
                project_ids=allowed_project_ids,
                project_id=project_id,
            )
            if owner_team is None or run.owner_team == owner_team
        ]
        return _with_request_id(
            request,
            {
                "summary": _progress_summary(visible_runs),
                "projects": _project_progress(visible_runs),
            },
        )


def _project_progress(runs: list[RunDashboardProgressRecord]) -> list[dict[str, Any]]:
    grouped: dict[str, list[RunDashboardProgressRecord]] = defaultdict(list)
    for run in runs:
        grouped[run.project_id].append(run)

    return [
        {
            "project_id": project_id,
            "owner_team": _latest_owner_team(project_runs),
            **_progress_summary(project_runs),
        }
        for project_id, project_runs in sorted(grouped.items())
    ]


def _progress_summary(runs: list[RunDashboardProgressRecord]) -> dict[str, Any]:
    counts = Counter(run.status for run in runs)
    scores = [run.evaluator_score for run in runs if run.evaluator_score is not None]
    latest_updated_at = max((run.updated_at for run in runs), default=None)
    return {
        "total_runs": len(runs),
        "runs_by_status": {status.value: counts.get(status.value, 0) for status in RunStatus},
        "queue_depth": counts.get(RunStatus.QUEUED.value, 0),
        "terminal_runs": sum(1 for run in runs if run.is_terminal),
        "artifact_count": sum(run.artifact_count for run in runs),
        "turn_count": sum(run.turn_count for run in runs),
        "evaluator_completed_count": sum(1 for run in runs if run.evaluator_completed),
        "average_evaluator_score": round(sum(scores) / len(scores), 4) if scores else None,
        "latest_updated_at": _datetime(latest_updated_at) if latest_updated_at is not None else None,
    }


def _latest_owner_team(runs: list[RunDashboardProgressRecord]) -> str | None:
    if not runs:
        return None
    return max(runs, key=lambda run: run.updated_at).owner_team


def _with_request_id(request: Request, payload: dict[str, Any]) -> dict[str, Any]:
    request_id = getattr(request.state, "request_id", None)
    if request_id:
        payload["request_id"] = request_id
    return payload


def _datetime(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _example_response(example: dict[str, Any]) -> dict[int, dict[str, Any]]:
    return {200: {"content": {"application/json": {"example": example}}}}

_RUNS_BY_STATUS_EXAMPLE = {
    "queued": 4,
    "dispatched": 1,
    "provisioning": 1,
    "running": 2,
    "evaluating": 1,
    "succeeded": 12,
    "failed": 2,
    "canceled": 1,
}
_PROGRESS_SUMMARY_EXAMPLE = {
    "total_runs": 23,
    "runs_by_status": _RUNS_BY_STATUS_EXAMPLE,
    "queue_depth": 4,
    "terminal_runs": 15,
    "artifact_count": 45,
    "turn_count": 126,
    "evaluator_completed_count": 12,
    "average_evaluator_score": 0.8125,
    "latest_updated_at": "2026-05-28T12:30:00Z",
}
_PROGRESS_EXAMPLE = {
    "summary": _PROGRESS_SUMMARY_EXAMPLE,
    "projects": [
        {
            "project_id": "pilot-project",
            "owner_team": "pilot group",
            **_PROGRESS_SUMMARY_EXAMPLE,
        }
    ],
    "request_id": "req_123",
}
