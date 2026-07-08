"""Shared projection from persisted gateway usage rows to trajectory events."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any
from uuid import UUID

from loom.models.trajectory import ChatMessage, LLMCallEvent
from loom.models.types import ModelSpec
from loom.request_params import coerce_request_params

_PROVIDER_BY_DIALECT = {
    "openai_chat": "openai",
    "openai_responses": "openai",
    "openai_facade": "openai",
    "anthropic_facade": "anthropic",
    "anthropic": "anthropic",
    "gemini_facade": "google",
    "gemini": "google",
}


def llm_call_row_to_event(
    row: Mapping[str, Any],
    *,
    trial_id: UUID,
    seq: int,
) -> LLMCallEvent:
    """Project one `llm_calls` row into the synthetic event used for exports.

    The gateway persists accounting facts, not full prompt/response bodies.
    Message-level semantics remain the adapter trajectory's responsibility; this
    projection preserves model, token, retry, request-param, and cost facts.
    """

    extras = row.get("provider_extras") or {}
    if not isinstance(extras, Mapping):
        extras = {}
    return LLMCallEvent(
        emitted_at=_captured_at(row.get("captured_at")),
        trial_id=trial_id,
        step_id=str(row.get("step_id") or "__trial__"),
        seq=seq,
        model=ModelSpec(
            provider=_PROVIDER_BY_DIALECT.get(str(row.get("dialect") or ""), "unknown"),
            name=str(row.get("model") or "unknown"),
        ),
        rate_card_hash=str(row.get("rate_card_hash") or ""),
        system_prompt=None,
        messages=[],
        response=ChatMessage(role="assistant", content=""),
        finish_reason="synthetic",
        input_tokens=int(row.get("input_tokens") or 0),
        cached_input_tokens=int(
            _numeric_counter(extras.get("cache_read_input_tokens"))
            + _numeric_counter(extras.get("cachedContentTokenCount")),
        ),
        cache_write_tokens=int(
            _numeric_counter(extras.get("cache_creation_input_tokens")),
        ),
        output_tokens=int(row.get("output_tokens") or 0),
        thinking_tokens=int(
            _numeric_counter(extras.get("reasoning_tokens"))
            + _numeric_counter(extras.get("thoughtsTokenCount")),
        ),
        provider_extras={
            str(k): int(v)
            for k, v in extras.items()
            if isinstance(v, int | float) and not isinstance(v, bool)
        },
        request_params=coerce_request_params(row.get("request_params")),
        cost_usd_snapshot=float(row.get("cost_usd") or 0.0),
        duration_sec=0.0,
        streamed=False,
        time_to_first_token_sec=None,
        gateway_request_id=str(row.get("id") or ""),
        attempt=int(row.get("attempt") or 1),
    )


def _captured_at(raw: Any) -> datetime:
    if isinstance(raw, datetime):
        return raw if raw.tzinfo is not None else raw.replace(tzinfo=UTC)
    if raw:
        return datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
    return datetime.now(UTC)


def _numeric_counter(raw: Any) -> int | float:
    return raw if isinstance(raw, int | float) and not isinstance(raw, bool) else 0
