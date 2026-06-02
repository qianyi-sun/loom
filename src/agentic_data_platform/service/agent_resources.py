from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Callable

from fastapi import Depends, FastAPI, HTTPException, Request
from sqlalchemy.orm import Session

from agentic_data_platform.agents.providers import AgentCatalogEntry
from agentic_data_platform.harbor.agent_adapters import (
    adapter_for_agent,
    build_agent_model_invocation,
    provider_dialect_gap,
)
from agentic_data_platform.harbor.agent_provider import HarborAgentProvider
from agentic_data_platform.providers.config import DevProviderConfigRegistry, ProviderRole
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

    @app.get(
        "/harbor/agent-adaptation",
        tags=["launch"],
        responses=_example_response(_AGENT_ADAPTATION_EXAMPLE),
    )
    def get_agent_model_adaptation(
        request: Request,
        agent_id: str,
        model_id: str,
        project_id: str | None = None,
        harness_id: str = "harbor-local-docker",
        provider_config_id: str | None = None,
        session: Session = Depends(session_dependency),
    ) -> dict[str, Any]:
        auth = require_authenticated_user(request, session)
        if project_id is not None:
            require_project_role(session, auth, project_id, minimum_role="viewer")

        provider = HarborAgentProvider()
        try:
            agent = provider.resolve_agent(agent_id=agent_id)
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc

        payload = _agent_model_adaptation_payload(
            agent=agent,
            model_id=model_id,
            harness_id=harness_id,
            provider_config_id=provider_config_id,
            settings=request.app.state.settings,
        )
        return _with_request_id(request, payload)


def _agent_payload(agent: AgentCatalogEntry) -> dict[str, Any]:
    return agent.to_payload()


def _agent_model_adaptation_payload(
    *,
    agent: AgentCatalogEntry,
    model_id: str,
    harness_id: str,
    provider_config_id: str | None,
    settings: Any,
) -> dict[str, Any]:
    gaps: list[dict[str, str]] = []
    harbor_agent_name = agent.metadata.get("harbor_agent_name") if isinstance(agent.metadata, dict) else None
    adapter = adapter_for_agent(harbor_agent_name)
    if harness_id not in agent.supported_harness_ids:
        gaps.append(
            {
                "code": "unsupported_harness",
                "message": f"{agent.agent_id} is not exposed for harness {harness_id}.",
            }
        )
    if adapter is None and agent.execution_mode == "external_cli":
        gaps.append(
            {
                "code": "missing_agent_model_adapter",
                "message": (
                    f"{agent.agent_id} does not yet have a model-provider adapter. "
                    "Track this as an adapter gap before using it for model-backed Harbor runs."
                ),
            }
        )

    registry = DevProviderConfigRegistry.from_settings(settings)
    refs = registry.list_refs(role=ProviderRole.AGENT_MODEL)
    selected_provider_config_id = provider_config_id or (refs[0].config_id if refs else "")
    selected_ref = None
    selected_secret = None
    if adapter is not None:
        try:
            selected_ref = registry.get(selected_provider_config_id)
        except KeyError:
            gaps.append(
                {
                    "code": "missing_provider_config",
                    "message": (
                        "A configured API model provider is required before this Harbor agent/model "
                        "combination can run."
                    ),
                }
            )
        else:
            if selected_ref.role is not ProviderRole.AGENT_MODEL:
                gaps.append(
                    {
                        "code": "invalid_provider_role",
                        "message": (
                            f"Provider config {selected_provider_config_id} has role "
                            f"{selected_ref.role.value}; Harbor agent runs require agent_model."
                        ),
                    }
                )
            elif gap := provider_dialect_gap(adapter=adapter, provider_ref=selected_ref):
                gaps.append(
                    {
                        "code": "provider_dialect_mismatch",
                        "message": gap,
                    }
                )
            else:
                try:
                    selected_secret = registry.resolve_secret(selected_ref.secret_ref)
                except KeyError:
                    gaps.append(
                        {
                            "code": "missing_provider_secret",
                            "message": (
                                "The selected API model provider secret is not available in this environment."
                            ),
                        }
                    )

    env_preview = []
    process_env_preview = []
    agent_kwargs_preview = []
    harbor_model_name = model_id
    if adapter is not None and selected_ref is not None and selected_secret is not None:
        invocation = build_agent_model_invocation(
            agent_name=harbor_agent_name if isinstance(harbor_agent_name, str) else agent.agent_id,
            model_id=model_id,
            provider_ref=selected_ref,
            provider_secret=selected_secret,
            existing_agent_env=[],
        )
        harbor_model_name = invocation.harbor_model_name
        env_preview = _env_preview(invocation.agent_env, selected_ref=selected_ref)
        process_env_preview = _env_preview(invocation.process_env, selected_ref=selected_ref)
        agent_kwargs_preview = _agent_kwargs_preview(invocation.agent_kwargs)

    return {
        "status": "blocked" if gaps else "ready",
        "agent_id": agent.agent_id,
        "model_id": model_id,
        "harbor_model_name": harbor_model_name,
        "harness_id": harness_id,
        "provider_config_id": selected_provider_config_id or None,
        "adapter": adapter.to_metadata() if adapter is not None else None,
        "required_secret_refs": (
            adapter.required_secret_refs
            if adapter is not None
            else list(agent.required_secret_refs)
        ),
        "env_preview": env_preview,
        "process_env_preview": process_env_preview,
        "agent_kwargs_preview": agent_kwargs_preview,
        "gaps": gaps,
        "checked_at": _now(),
    }


def _env_preview(values: list[str], *, selected_ref) -> list[dict[str, str]]:
    previews = []
    for value in values:
        name, _, _ = value.partition("=")
        if not name:
            continue
        source = "provider_base_url" if ("BASE_URL" in name or "API_BASE" in name) else selected_ref.secret_ref
        previews.append({"name": name, "source": source})
    return previews


def _agent_kwargs_preview(values: list[str]) -> list[dict[str, str]]:
    previews = []
    for value in values:
        name, _, _ = value.partition("=")
        if name:
            source = "provider_base_url" if name in {"base_url", "api_base"} else "adapter_default"
            previews.append({"name": name, "source": source})
    return previews


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
_AGENT_ADAPTATION_EXAMPLE = {
    "status": "ready",
    "agent_id": "harbor:opencode",
    "model_id": "deepseek-v4-flash",
    "harness_id": "harbor-local-docker",
    "provider_config_id": "default-agent-model",
    "adapter": {
        "adapter_id": "opencode-openai-compatible",
        "endpoint_dialects": ["openai_compatible"],
        "api_key_env_names": ["OPENAI_API_KEY"],
        "base_url_env_names": ["OPENAI_BASE_URL", "OPENAI_API_BASE"],
    },
    "required_secret_refs": ["env:OPENAI_API_KEY"],
    "env_preview": [
        {"name": "OPENAI_API_KEY", "source": "env:MODEL_PROVIDER_API_KEY"},
        {"name": "OPENAI_BASE_URL", "source": "provider_base_url"},
    ],
    "gaps": [],
    "checked_at": "2026-06-01T12:00:00Z",
    "request_id": "req_123",
}
