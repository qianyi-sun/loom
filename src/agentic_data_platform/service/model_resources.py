from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Callable

import httpx
from fastapi import Depends, FastAPI, Request
from sqlalchemy.orm import Session

from agentic_data_platform.providers.config import DevProviderConfigRegistry, ProviderConfigRef, ProviderRole
from agentic_data_platform.service.security import require_authenticated_user, require_project_role


def register_model_routes(app: FastAPI, session_dependency: Callable) -> None:
    @app.get("/models", tags=["models"], responses=_example_response(_MODELS_EXAMPLE))
    def list_models(
        request: Request,
        project_id: str | None = None,
        session: Session = Depends(session_dependency),
    ) -> dict[str, Any]:
        auth = require_authenticated_user(request, session)
        if project_id is not None:
            require_project_role(session, auth, project_id, minimum_role="viewer")
        payload = discover_model_catalog(request.app.state.settings)
        return _with_request_id(request, payload)


def discover_model_catalog(settings) -> dict[str, Any]:
    registry = DevProviderConfigRegistry.from_settings(settings)
    refs = registry.list_refs(role=ProviderRole.AGENT_MODEL)
    default_ref = refs[0] if refs else _default_agent_ref(settings)
    include_provider_config = bool(refs)

    static_models = _parse_static_models(settings.model_provider_models)
    if static_models:
        return {
            "models": [
                _model_payload(
                    default_ref,
                    model_id=model_id,
                    source="static_config",
                    include_provider_config=include_provider_config,
                )
                for model_id in static_models
            ],
            "errors": [],
            "checked_at": _now(),
        }

    if settings.model_provider_base_url and settings.model_provider_api_key:
        try:
            model_ids = _fetch_openai_compatible_models(
                base_url=settings.model_provider_base_url,
                api_key=settings.model_provider_api_key,
            )
        except Exception as exc:  # pragma: no cover - concrete failure type depends on provider/httpx
            return {
                "models": [
                    _model_payload(
                        default_ref,
                        model_id=default_ref.model_name,
                        source="openai_compatible_discovery",
                        include_provider_config=include_provider_config,
                        disabled=True,
                        error=str(exc),
                    )
                ],
                "errors": [{"provider_config_id": default_ref.config_id, "message": str(exc)}],
                "checked_at": _now(),
            }
        return {
            "models": [
                _model_payload(default_ref, model_id=model_id, source="openai_compatible_discovery")
                for model_id in model_ids
            ],
            "errors": [],
            "checked_at": _now(),
        }

    return {
        "models": [
            _model_payload(
                default_ref,
                model_id="scripted-terminal-agent",
                source="dev_fallback",
                include_provider_config=False,
                disabled=False,
            )
        ],
        "errors": [{"message": "No API model provider is configured; using dev scripted model fallback."}],
        "checked_at": _now(),
    }


def _fetch_openai_compatible_models(*, base_url: str, api_key: str) -> list[str]:
    url = f"{base_url.rstrip('/')}/models"
    with httpx.Client(timeout=httpx.Timeout(5.0)) as client:
        response = client.get(url, headers={"Authorization": f"Bearer {api_key}"})
        response.raise_for_status()
        payload = response.json()
    data = payload.get("data")
    if not isinstance(data, list):
        raise ValueError("provider /models response did not include a data array")
    model_ids = [str(item.get("id")) for item in data if isinstance(item, dict) and item.get("id")]
    if not model_ids:
        raise ValueError("provider /models response did not include selectable models")
    return model_ids


def _parse_static_models(raw_value: str) -> list[str]:
    return [item.strip() for item in raw_value.split(",") if item.strip()]


def _default_agent_ref(settings) -> ProviderConfigRef:
    return ProviderConfigRef(
        config_id="default-agent-model",
        role=ProviderRole.AGENT_MODEL,
        provider="dev-api-provider" if settings.model_provider_base_url or settings.model_provider_api_key else "mock-api",
        model_name="configured-agent-model" if settings.model_provider_base_url else "scripted-terminal-agent",
        base_url=settings.model_provider_base_url,
        secret_ref="env:MODEL_PROVIDER_API_KEY" if settings.model_provider_api_key else "env:MODEL_PROVIDER_API_KEY",
    )


def _model_payload(
    ref: ProviderConfigRef,
    *,
    model_id: str,
    source: str,
    include_provider_config: bool = True,
    disabled: bool = False,
    error: str | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "provider": ref.provider,
        "provider_id": ref.config_id,
        "model_id": model_id,
        "model_name": model_id,
        "display_name": model_id,
        "mode": "api",
        "capabilities": ["terminal-agent"],
        "source": source,
        "disabled": disabled,
    }
    if include_provider_config:
        payload["provider_config_id"] = ref.config_id
    if error:
        payload["error"] = error
    return payload


def _with_request_id(request: Request, payload: dict[str, Any]) -> dict[str, Any]:
    request_id = getattr(request.state, "request_id", None)
    if request_id:
        payload["request_id"] = request_id
    return payload


def _now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _example_response(example: dict[str, Any]) -> dict[int, dict[str, Any]]:
    return {200: {"content": {"application/json": {"example": example}}}}


_MODELS_EXAMPLE = {
    "models": [
        {
            "provider_config_id": "default-agent-model",
            "provider": "dev-api-provider",
            "provider_id": "default-agent-model",
            "model_id": "gpt-5",
            "model_name": "gpt-5",
            "display_name": "gpt-5",
            "mode": "api",
            "capabilities": ["terminal-agent"],
            "source": "static_config",
            "disabled": False,
        }
    ],
    "errors": [],
    "checked_at": "2026-05-29T12:00:00Z",
    "request_id": "req_123",
}
