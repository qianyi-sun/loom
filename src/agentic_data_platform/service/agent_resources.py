from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Callable

from fastapi import Depends, FastAPI, HTTPException, Request
from sqlalchemy.orm import Session

from agentic_data_platform.agents.providers import AgentCatalogEntry
from agentic_data_platform.harbor.agent_provider import HarborAgentProvider
from agentic_data_platform.service.security import require_authenticated_user, require_project_role


def register_agent_routes(app: FastAPI, session_dependency: Callable) -> None:
    @app.get("/agents", tags=["launch"], responses=_example_response(_AGENTS_EXAMPLE))
    def list_agents(
        request: Request,
        project_id: str | None = None,
        harness_id: str | None = None,
        agent_import_path: str | None = None,
        session: Session = Depends(session_dependency),
    ) -> dict[str, Any]:
        auth = require_authenticated_user(request, session)
        if project_id is not None:
            require_project_role(session, auth, project_id, minimum_role="viewer")

        provider = HarborAgentProvider()
        try:
            agents = provider.list_agents()
            if agent_import_path:
                agents.append(provider.agent_for_import_path(agent_import_path))
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc

        if harness_id:
            agents = [agent for agent in agents if harness_id in agent.supported_harness_ids]

        return _with_request_id(
            request,
            {
                "agents": [_agent_payload(agent) for agent in agents],
                "errors": [],
                "checked_at": _now(),
            },
        )


def _agent_payload(agent: AgentCatalogEntry) -> dict[str, Any]:
    return agent.to_payload()


def _with_request_id(request: Request, payload: dict[str, Any]) -> dict[str, Any]:
    request_id = getattr(request.state, "request_id", None)
    if request_id:
        payload["request_id"] = request_id
    return payload


def _now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _example_response(example: dict[str, Any]) -> dict[int, dict[str, Any]]:
    return {200: {"content": {"application/json": {"example": example}}}}


_AGENT_EXAMPLE = {
    "agent_id": "harbor:codex",
    "display_name": "Codex",
    "provider": "harbor",
    "source": "harbor_builtin",
    "runner_kind": "harbor",
    "execution_mode": "external_cli",
    "supported_harness_ids": ["harbor-local-docker"],
    "supported_sandbox_backends": ["docker_terminal"],
    "required_secret_refs": ["env:OPENAI_API_KEY"],
    "supports_trajectory": True,
    "capabilities": ["terminal-agent", "harbor-run", "harbor-trial-events"],
    "metadata": {
        "provider": "harbor",
        "harbor_agent_name": "codex",
        "harbor_cli_args": ["--agent", "codex"],
        "backend_modes": ["cli", "native"],
    },
}
_AGENTS_EXAMPLE = {
    "agents": [_AGENT_EXAMPLE],
    "errors": [],
    "checked_at": "2026-06-01T12:00:00Z",
    "request_id": "req_123",
}
