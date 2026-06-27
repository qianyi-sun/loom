"""DialectAdapter — extracts token-usage facts from each dialect's
native response shape (Plan 9 Task 5).

Four dialects: openai_chat, openai_responses, anthropic, gemini. Each
adapter knows where its dialect puts input/output/cache token counts;
the result is a `TokenUsage` that feeds `compute_cost_usd` + the
`llm_calls` table's `provider_extras` JSONB column.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

USAGE_STATUS_KEY = "_loom_usage_status"
USAGE_MISSING_KEYS_KEY = "_loom_missing_usage_keys"
USAGE_MALFORMED_KEYS_KEY = "_loom_malformed_usage_keys"
USAGE_PROVIDER_USAGE_KEY = "_loom_provider_usage"


@dataclass(frozen=True)
class TokenUsage:
    input_tokens: int
    output_tokens: int
    # Dialect-specific extra counters (cache_creation_input_tokens,
    # cache_read_input_tokens, reasoning_tokens, thoughtsTokenCount, etc.).
    # Stored verbatim into llm_calls.provider_extras.
    provider_extras: dict[str, Any] = field(default_factory=dict)

    @property
    def cached_input_tokens(self) -> int:
        """Sum of all "this read from cache" counters across dialects."""
        return (
            _coerce_token_count(
                self.provider_extras.get("cache_read_input_tokens"),
            )
            + _coerce_token_count(
                self.provider_extras.get("cachedContentTokenCount"),
            )
        )

    @property
    def cache_write_tokens(self) -> int:
        """Sum of all "this wrote to cache" counters across dialects."""
        return _coerce_token_count(
            self.provider_extras.get("cache_creation_input_tokens"),
        )


@dataclass(frozen=True)
class DialectAdapter:
    name: str
    extract_tokens: Callable[[dict[str, Any]], TokenUsage]


def _coerce_token_count(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _safe_provider_usage(value: dict[str, Any]) -> dict[str, Any]:
    """Keep only JSON-safe primitive provider usage fields for audit."""

    def _safe(item: Any) -> Any:
        if item is None or isinstance(item, (str, int, float, bool)):
            return item
        if isinstance(item, list):
            return [_safe(child) for child in item[:20]]
        if isinstance(item, dict):
            return {
                str(key): _safe(child)
                for key, child in list(item.items())[:50]
                if isinstance(key, str)
            }
        return str(item)

    return {
        str(key): _safe(child)
        for key, child in value.items()
        if isinstance(key, str)
    }


def usage_reporting_extras(
    usage_body: Any,
    *,
    required_keys: tuple[str, ...],
) -> dict[str, Any]:
    if not isinstance(usage_body, dict):
        return {USAGE_STATUS_KEY: "missing"}

    missing: list[str] = []
    malformed: list[str] = []
    for key in required_keys:
        if key not in usage_body or usage_body.get(key) is None:
            missing.append(key)
            continue
        try:
            int(usage_body[key])
        except (TypeError, ValueError):
            malformed.append(key)

    if not missing and not malformed:
        return {}

    extras: dict[str, Any] = {
        USAGE_STATUS_KEY: "partial",
        USAGE_PROVIDER_USAGE_KEY: _safe_provider_usage(usage_body),
    }
    if missing:
        extras[USAGE_MISSING_KEYS_KEY] = missing
    if malformed:
        extras[USAGE_MALFORMED_KEYS_KEY] = malformed
    return extras


def _openai_chat(r: dict[str, Any]) -> TokenUsage:
    raw = r.get("usage")
    u = raw if isinstance(raw, dict) else {}
    return TokenUsage(
        input_tokens=_coerce_token_count(u.get("prompt_tokens")),
        output_tokens=_coerce_token_count(u.get("completion_tokens")),
        provider_extras=usage_reporting_extras(
            raw,
            required_keys=("prompt_tokens", "completion_tokens"),
        ),
    )


def _openai_responses(r: dict[str, Any]) -> TokenUsage:
    raw = r.get("usage")
    u = raw if isinstance(raw, dict) else {}
    extras: dict[str, Any] = usage_reporting_extras(
        raw,
        required_keys=("input_tokens", "output_tokens"),
    )
    details = u.get("output_tokens_details") or {}
    rt = details.get("reasoning_tokens")
    if rt is not None:
        extras["reasoning_tokens"] = _coerce_token_count(rt)
    return TokenUsage(
        input_tokens=_coerce_token_count(u.get("input_tokens")),
        output_tokens=_coerce_token_count(u.get("output_tokens")),
        provider_extras=extras,
    )


def _anthropic(r: dict[str, Any]) -> TokenUsage:
    raw = r.get("usage")
    u = raw if isinstance(raw, dict) else {}
    extras: dict[str, Any] = usage_reporting_extras(
        raw,
        required_keys=("input_tokens", "output_tokens"),
    )
    for k in ("cache_creation_input_tokens", "cache_read_input_tokens"):
        v = u.get(k)
        if v is not None:
            extras[k] = _coerce_token_count(v)
    return TokenUsage(
        input_tokens=_coerce_token_count(u.get("input_tokens")),
        output_tokens=_coerce_token_count(u.get("output_tokens")),
        provider_extras=extras,
    )


def _gemini(r: dict[str, Any]) -> TokenUsage:
    raw = r.get("usageMetadata")
    u = raw if isinstance(raw, dict) else {}
    extras: dict[str, Any] = usage_reporting_extras(
        raw,
        required_keys=("promptTokenCount", "candidatesTokenCount"),
    )
    for k in ("cachedContentTokenCount", "thoughtsTokenCount"):
        v = u.get(k)
        if v is not None:
            extras[k] = _coerce_token_count(v)
    return TokenUsage(
        input_tokens=_coerce_token_count(u.get("promptTokenCount")),
        output_tokens=_coerce_token_count(u.get("candidatesTokenCount")),
        provider_extras=extras,
    )


DIALECTS: dict[str, DialectAdapter] = {
    "openai_chat":      DialectAdapter("openai_chat",      _openai_chat),
    "openai_responses": DialectAdapter("openai_responses", _openai_responses),
    "anthropic":        DialectAdapter("anthropic",        _anthropic),
    "gemini":           DialectAdapter("gemini",           _gemini),
}
