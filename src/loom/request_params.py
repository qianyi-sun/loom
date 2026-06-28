"""Normalized, redacted LLM request-parameter audit payloads."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

_AVAILABLE = "available"
_UNAVAILABLE_LEGACY = "unavailable_legacy"

_ALLOWED_PARAMETER_KEYS = frozenset(
    {
        "best_of",
        "do_sample",
        "frequency_penalty",
        "length_penalty",
        "logprobs",
        "max_completion_tokens",
        "max_new_tokens",
        "max_output_tokens",
        "max_tokens",
        "min_p",
        "min_tokens",
        "mirostat",
        "mirostat_eta",
        "mirostat_tau",
        "n",
        "num_beams",
        "parallel_tool_calls",
        "presence_penalty",
        "reasoning",
        "reasoning_effort",
        "repetition_penalty",
        "response_format",
        "seed",
        "stop",
        "stop_sequences",
        "stream",
        "temperature",
        "tool_choice",
        "top_k",
        "top_logprobs",
        "top_p",
        "typical_p",
        "verbosity",
    }
)
_EXTRA_CONTAINERS = ("extra_body", "generation_config", "request_options")
_SENSITIVE_KEY_FRAGMENTS = (
    "api_key",
    "apikey",
    "authorization",
    "bearer",
    "credential",
    "header",
    "key",
    "password",
    "prompt",
    "secret",
    "token",
)
_OMITTED_PAYLOAD_KEYS = frozenset(
    {
        "input",
        "instructions",
        "messages",
        "prompt",
        "prompts",
        "system",
    }
)


def legacy_request_params() -> dict[str, Any]:
    """Explicit marker for rows created before request params were stored."""
    return {"status": _UNAVAILABLE_LEGACY, "parameters": {}}


def normalize_request_params(
    payload: Mapping[str, Any] | None,
    *,
    defaults: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Return the public audit shape for non-sensitive generation controls.

    The input is the effective provider request payload after route-level
    transformations. Only allowlisted decoding/behavior knobs are copied.
    Prompt content, message arrays, headers, credentials, and secret-looking
    nested keys are omitted.
    """
    parameters: dict[str, Any] = {}
    _merge_allowed(parameters, defaults or {})
    _merge_allowed(parameters, payload or {})
    for container in _EXTRA_CONTAINERS:
        value = (payload or {}).get(container)
        if isinstance(value, Mapping):
            _merge_allowed(parameters, value)
    return {"status": _AVAILABLE, "parameters": parameters}


def coerce_request_params(value: Mapping[str, Any] | None) -> dict[str, Any]:
    """Coerce stored DB/API values to the public request-param shape."""
    if not isinstance(value, Mapping):
        return legacy_request_params()
    status = value.get("status")
    parameters = value.get("parameters")
    if status != _AVAILABLE or not isinstance(parameters, Mapping):
        if status == _UNAVAILABLE_LEGACY:
            return legacy_request_params()
        return legacy_request_params()
    return {
        "status": _AVAILABLE,
        "parameters": _sanitize_mapping(parameters, allow_parameter_keys=True),
    }


def _merge_allowed(target: dict[str, Any], source: Mapping[str, Any]) -> None:
    for key, value in source.items():
        normalized_key = str(key)
        if normalized_key in _OMITTED_PAYLOAD_KEYS:
            continue
        if normalized_key not in _ALLOWED_PARAMETER_KEYS:
            continue
        sanitized = _sanitize_value(value)
        if sanitized is not None:
            target[normalized_key] = sanitized


def _sensitive_key(key: str) -> bool:
    lowered = key.lower()
    return any(fragment in lowered for fragment in _SENSITIVE_KEY_FRAGMENTS)


def _sanitize_mapping(
    value: Mapping[str, Any],
    *,
    allow_parameter_keys: bool = False,
) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for key, raw in value.items():
        normalized_key = str(key)
        if allow_parameter_keys and normalized_key not in _ALLOWED_PARAMETER_KEYS:
            continue
        if (
            not (allow_parameter_keys and normalized_key in _ALLOWED_PARAMETER_KEYS)
            and _sensitive_key(normalized_key)
        ):
            continue
        sanitized = _sanitize_value(raw)
        if sanitized is not None:
            out[normalized_key] = sanitized
    return out


def _sanitize_value(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, str | int | float | bool):
        return value
    if isinstance(value, Mapping):
        return _sanitize_mapping(value)
    if isinstance(value, list):
        return [
            item
            for raw in value
            if (item := _sanitize_value(raw)) is not None
        ]
    if isinstance(value, tuple):
        return [
            item
            for raw in value
            if (item := _sanitize_value(raw)) is not None
        ]
    return str(value)
