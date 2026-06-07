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


@dataclass(frozen=True)
class TokenUsage:
    input_tokens: int
    output_tokens: int
    # Dialect-specific extra counters (cache_creation_input_tokens,
    # cache_read_input_tokens, reasoning_tokens, thoughtsTokenCount, etc.).
    # Stored verbatim into llm_calls.provider_extras.
    provider_extras: dict[str, int] = field(default_factory=dict)

    @property
    def cached_input_tokens(self) -> int:
        """Sum of all "this read from cache" counters across dialects."""
        return (
            self.provider_extras.get("cache_read_input_tokens", 0)
            + self.provider_extras.get("cachedContentTokenCount", 0)
        )

    @property
    def cache_write_tokens(self) -> int:
        """Sum of all "this wrote to cache" counters across dialects."""
        return self.provider_extras.get("cache_creation_input_tokens", 0)


@dataclass(frozen=True)
class DialectAdapter:
    name: str
    extract_tokens: Callable[[dict[str, Any]], TokenUsage]


def _openai_chat(r: dict[str, Any]) -> TokenUsage:
    u = r.get("usage", {}) or {}
    return TokenUsage(
        input_tokens=int(u.get("prompt_tokens", 0)),
        output_tokens=int(u.get("completion_tokens", 0)),
        provider_extras={},
    )


def _openai_responses(r: dict[str, Any]) -> TokenUsage:
    u = r.get("usage", {}) or {}
    extras: dict[str, int] = {}
    details = u.get("output_tokens_details") or {}
    rt = details.get("reasoning_tokens")
    if rt is not None:
        extras["reasoning_tokens"] = int(rt)
    return TokenUsage(
        input_tokens=int(u.get("input_tokens", 0)),
        output_tokens=int(u.get("output_tokens", 0)),
        provider_extras=extras,
    )


def _anthropic(r: dict[str, Any]) -> TokenUsage:
    u = r.get("usage", {}) or {}
    extras: dict[str, int] = {}
    for k in ("cache_creation_input_tokens", "cache_read_input_tokens"):
        v = u.get(k)
        if v is not None:
            extras[k] = int(v)
    return TokenUsage(
        input_tokens=int(u.get("input_tokens", 0)),
        output_tokens=int(u.get("output_tokens", 0)),
        provider_extras=extras,
    )


def _gemini(r: dict[str, Any]) -> TokenUsage:
    u = r.get("usageMetadata", {}) or {}
    extras: dict[str, int] = {}
    for k in ("cachedContentTokenCount", "thoughtsTokenCount"):
        v = u.get(k)
        if v is not None:
            extras[k] = int(v)
    return TokenUsage(
        input_tokens=int(u.get("promptTokenCount", 0)),
        output_tokens=int(u.get("candidatesTokenCount", 0)),
        provider_extras=extras,
    )


DIALECTS: dict[str, DialectAdapter] = {
    "openai_chat":      DialectAdapter("openai_chat",      _openai_chat),
    "openai_responses": DialectAdapter("openai_responses", _openai_responses),
    "anthropic":        DialectAdapter("anthropic",        _anthropic),
    "gemini":           DialectAdapter("gemini",           _gemini),
}
