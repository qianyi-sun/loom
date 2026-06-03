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
        repository = RunRepository(session)
        runs = [run for run in repository.list_runs() if run.project_id in allowed_project_ids]
        counts = Counter(run.status.value for run in runs)
        capacity_blocked = repository.list_scheduler_capacity_blocks(project_ids=allowed_project_ids, limit=25)
        blocked_by_dimension = Counter(block.dimension for block in capacity_blocked)
        return _with_request_id(
            request,
            {
                "runs_by_status": {status.value: counts.get(status.value, 0) for status in RunStatus},
                "queue_depth": counts.get(RunStatus.QUEUED.value, 0),
                "visible_project_count": len(allowed_project_ids),
                "scheduler_capacity_blocked_count": len(capacity_blocked),
                "scheduler_capacity_blocked_by_dimension": dict(blocked_by_dimension),
                "scheduler_capacity_blocked_runs": [block.to_dict() for block in capacity_blocked],
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
        "dispatched": 1,
        "provisioning": 1,
        "running": 2,
        "evaluating": 1,
        "succeeded": 12,
        "failed": 2,
        "canceled": 1,
    },
    "queue_depth": 4,
    "visible_project_count": 3,
    "scheduler_capacity_blocked_count": 1,
    "scheduler_capacity_blocked_by_dimension": {"provider": 1},
    "scheduler_capacity_blocked_runs": [
        {
            "run_id": "run_123",
            "project_id": "pilot-project",
            "scheduler_id": "scheduler-dev-1",
            "execution_task_id": "run_123:attempt:1",
            "dimension": "provider",
            "key": "openai",
            "metric": "active_runs",
            "active_count": 2,
            "limit": 2,
            "reason": "provider capacity reached",
            "observed_at": "2026-06-01T12:00:00Z",
            "backend_key": "harbor-local-docker",
            "provider_key": "openai",
            "model_key": "gpt-5",
            "agent_key": "codex",
            "benchmark_key": "terminal-bench@2.0",
        }
    ],
    "request_id": "req_123",
}
