from __future__ import annotations

from typing import Any, Callable

from fastapi import Depends, FastAPI, Request
from sqlalchemy.orm import Session

from agentic_data_platform.service.security import require_authenticated_user, require_project_role


def register_harness_routes(app: FastAPI, session_dependency: Callable) -> None:
    @app.get("/harnesses", tags=["launch"], responses=_example_response(_HARNESSES_EXAMPLE))
    def list_harnesses(
        request: Request,
        project_id: str | None = None,
        session: Session = Depends(session_dependency),
    ) -> dict[str, Any]:
        auth = require_authenticated_user(request, session)
        if project_id is not None:
            require_project_role(session, auth, project_id, minimum_role="viewer")
        return _with_request_id(request, {"harnesses": harness_catalog()})


def harness_catalog() -> list[dict[str, Any]]:
    return [
        {
            "harness_id": "docker-terminal",
            "display_name": "Docker terminal sandbox",
            "description": "Platform-native terminal-agent execution inside a local Docker sandbox.",
            "runner_kind": "original_benchmark",
            "sandbox_backend": "docker_terminal",
            "default_image": "python:3.12-slim",
            "internet_access": True,
            "resource_limits": {"cpu": 1, "memory_mb": 512, "pids_limit": 128, "timeout_seconds": 60},
            "metadata": {
                "runner_contract": "docker-terminal-v0",
                "harbor_compatible": False,
                "status": "ready",
            },
        },
        {
            "harness_id": "harbor-local-docker",
            "display_name": "Harbor local Docker",
            "description": "Harbor-compatible local Docker harness surface for the frontend MVP.",
            "runner_kind": "original_benchmark",
            "sandbox_backend": "docker_terminal",
            "default_image": "python:3.12-slim",
            "internet_access": True,
            "resource_limits": {"cpu": 1, "memory_mb": 512, "pids_limit": 128, "timeout_seconds": 60},
            "metadata": {
                "runner_contract": "harbor-local-docker-v0",
                "harbor_compatible": True,
                "status": "ready",
                "harbor_task_template": "harbor-cli-smoke",
                "harbor_agent": "oracle",
                "default_agent_id": "harbor:oracle",
                "agent_catalog_endpoint": "/agents?harness_id=harbor-local-docker",
                "harbor_model_name": "smoke/noop",
                "harbor_environment": "docker",
                "harbor_extra_args": ["--n-tasks", "1", "--quiet"],
                "follow_up_issues": ["#62", "#63", "#67"],
            },
        },
    ]


def _with_request_id(request: Request, payload: dict[str, Any]) -> dict[str, Any]:
    request_id = getattr(request.state, "request_id", None)
    if request_id:
        payload["request_id"] = request_id
    return payload


def _example_response(example: dict[str, Any]) -> dict[int, dict[str, Any]]:
    return {200: {"content": {"application/json": {"example": example}}}}


_HARNESSES_EXAMPLE = {"harnesses": harness_catalog(), "request_id": "req_123"}
