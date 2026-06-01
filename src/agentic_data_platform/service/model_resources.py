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
    if settings.model_provider_base_url and settings.model_provider_api_key:
        try:
            discovered_model_ids = _fetch_openai_compatible_models(
                base_url=settings.model_provider_base_url,
                api_key=settings.model_provider_api_key,
            )
        except Exception as exc:  # pragma: no cover - concrete failure type depends on provider/httpx
            if static_models:
                return {
                    "models": [
                        _model_payload(
                            default_ref,
                            model_id=model_id,
                            source="static_config_fallback",
                            include_provider_config=include_provider_config,
                        )
                        for model_id in static_models
                    ],
                    "errors": [{"provider_config_id": default_ref.config_id, "message": str(exc)}],
                    "catalog": _catalog_payload(
                        status="fallback_static_config",
                        source="static_config_fallback",
                        provider_config_id=default_ref.config_id,
                        message="Provider model discovery failed; using MODEL_PROVIDER_MODELS fallback.",
                    ),
                    "checked_at": _now(),
                }
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
                "catalog": _catalog_payload(
                    status="discovery_failed",
                    source="openai_compatible_discovery",
                    provider_config_id=default_ref.config_id,
                    message="Provider model discovery failed and no static fallback is configured.",
                ),
                "checked_at": _now(),
            }

        if static_models:
            discovered_set = set(discovered_model_ids)
            allowlisted_model_ids = [model_id for model_id in static_models if model_id in discovered_set]
            if allowlisted_model_ids:
                return {
                    "models": [
                        _model_payload(
                            default_ref,
                            model_id=model_id,
                            source="openai_compatible_discovery_allowlist",
                        )
                        for model_id in allowlisted_model_ids
                    ],
                    "errors": [],
                    "catalog": _catalog_payload(
                        status="discovered_allowlisted",
                        source="openai_compatible_discovery",
                        provider_config_id=default_ref.config_id,
                        message="Provider models discovered and filtered by MODEL_PROVIDER_MODELS allowlist.",
                    ),
                    "checked_at": _now(),
                }
            return {
                "models": [
                    _model_payload(
                        default_ref,
                        model_id=model_id,
                        source="static_config_fallback",
                        include_provider_config=include_provider_config,
                    )
                    for model_id in static_models
                ],
                "errors": [
                    {
                        "provider_config_id": default_ref.config_id,
                        "message": "Provider discovery returned no models matching MODEL_PROVIDER_MODELS; using static fallback.",
                    }
                ],
                "catalog": _catalog_payload(
                    status="fallback_static_config",
                    source="static_config_fallback",
                    provider_config_id=default_ref.config_id,
                    message="Provider discovery returned no allowlisted models; using MODEL_PROVIDER_MODELS fallback.",
                ),
                "checked_at": _now(),
            }

        return {
            "models": [
                _model_payload(
                    default_ref,
                    model_id=model_id,
                    source="openai_compatible_discovery",
                )
                for model_id in discovered_model_ids
            ],
            "errors": [],
            "catalog": _catalog_payload(
                status="discovered",
                source="openai_compatible_discovery",
                provider_config_id=default_ref.config_id,
                message="Provider models discovered from OpenAI-compatible /models.",
            ),
            "checked_at": _now(),
        }

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
            "catalog": _catalog_payload(
                status="static_config",
                source="static_config",
                provider_config_id=default_ref.config_id,
                message="Using MODEL_PROVIDER_MODELS static model list.",
            ),
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
        "catalog": _catalog_payload(
            status="dev_fallback",
            source="dev_fallback",
            provider_config_id=default_ref.config_id,
            message="No API model provider configured; using dev scripted model fallback.",
        ),
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
        "metadata": _model_metadata(model_id=model_id, source=source),
        "source": source,
        "disabled": disabled,
    }
    if include_provider_config:
        payload["provider_config_id"] = ref.config_id
    if error:
        payload["error"] = error
    return payload


def _model_metadata(*, model_id: str, source: str) -> dict[str, Any]:
    family = _infer_model_family(model_id)
    return {
        "family": family,
        "endpoint_dialects": ["openai_compatible"],
        "agent_capable": True,
        "mainstream": family != "other",
        "source": source,
    }


def _infer_model_family(model_id: str) -> str:
    normalized = model_id.lower()
    family_markers = [
        ("deepseek", ("deepseek",)),
        ("claude", ("claude", "anthropic")),
        ("gemini", ("gemini", "google")),
        ("qwen", ("qwen", "qwq")),
        ("kimi", ("kimi", "moonshot")),
        ("glm", ("glm", "zhipu")),
        ("grok", ("grok", "xai")),
        ("minimax", ("minimax", "abab")),
        ("openai", ("gpt", "o1", "o3", "o4", "chatgpt")),
    ]
    for family, markers in family_markers:
        if any(marker in normalized for marker in markers):
            return family
    return "other"


def _catalog_payload(*, status: str, source: str, provider_config_id: str, message: str) -> dict[str, Any]:
    return {
        "status": status,
        "source": source,
        "provider_config_id": provider_config_id,
        "message": message,
    }


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
            "source": "openai_compatible_discovery",
            "disabled": False,
        }
    ],
    "errors": [],
    "catalog": {
        "status": "discovered",
        "source": "openai_compatible_discovery",
        "provider_config_id": "default-agent-model",
        "message": "Provider models discovered from OpenAI-compatible /models.",
    },
    "checked_at": "2026-05-29T12:00:00Z",
    "request_id": "req_123",
}
