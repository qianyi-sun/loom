from __future__ import annotations

from collections import Counter
from typing import Any, Callable

from fastapi import Depends, FastAPI, Request
from sqlalchemy.orm import Session

from agentic_data_platform.domain.run_records import RunStatus
from agentic_data_platform.persistence.repositories import RunRepository
from agentic_data_platform.service.security import accessible_project_ids, require_authenticated_user


def register_ops_routes(app: FastAPI, session_dependency: Callable) -> None:
    @app.get("/ops/metrics", tags=["operations"], responses=_example_response(_METRICS_EXAMPLE))
    def get_metrics(request: Request, session: Session = Depends(session_dependency)) -> dict[str, Any]:
        auth = require_authenticated_user(request, session)
        allowed_project_ids = accessible_project_ids(session, auth)
        runs = [run for run in RunRepository(session).list_runs() if run.project_id in allowed_project_ids]
        counts = Counter(run.status.value for run in runs)
        return _with_request_id(
            request,
            {
                "runs_by_status": {status.value: counts.get(status.value, 0) for status in RunStatus},
                "queue_depth": counts.get(RunStatus.QUEUED.value, 0),
                "visible_project_count": len(allowed_project_ids),
            },
        )


def _with_request_id(request: Request, payload: dict[str, Any]) -> dict[str, Any]:
    request_id = getattr(request.state, "request_id", None)
    if request_id:
        payload["request_id"] = request_id
    return payload


def _example_response(example: dict[str, Any]) -> dict[int, dict[str, Any]]:
    return {200: {"content": {"application/json": {"example": example}}}}


_METRICS_EXAMPLE = {
    "runs_by_status": {
        "queued": 4,
        "provisioning": 1,
        "running": 2,
        "evaluating": 1,
        "succeeded": 12,
        "failed": 2,
        "canceled": 1,
    },
    "queue_depth": 4,
    "visible_project_count": 3,
    "request_id": "req_123",
}
