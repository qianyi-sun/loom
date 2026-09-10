"""Shared public serialization for the authoritative Gateway call ledger."""

from __future__ import annotations

from typing import Any

from loom.db.schema import LlmCall
from loom.request_params import coerce_request_params


def serialize_llm_call(r: LlmCall, *, include_provider_log: bool = True) -> dict[str, Any]:
    """Keep worker and lease-scoped ledger field semantics identical."""
    payload = {
        "id": str(r.id),
        "trial_id": str(r.trial_id),
        "step_id": r.step_id,
        "dialect": r.dialect,
        "model": r.model,
        "input_tokens": r.input_tokens,
        "output_tokens": r.output_tokens,
        "provider_extras": r.provider_extras,
        "request_params": coerce_request_params(r.request_params),
        "cost_usd": float(r.cost_usd),
        "rate_card_hash": r.rate_card_hash,
        "captured_at": r.captured_at.isoformat(),
        # #298 Slice B: gateway-internal retry attempt that
        # produced this row. Defaults to 1 for pre-#298 rows.
        "attempt": r.attempt,
        "client_call_id": str(r.client_call_id) if r.client_call_id else None,
        "episode": r.episode,
        "call_ordinal": r.call_ordinal,
        "requested_model": r.requested_model,
        "response_model": r.response_model,
        "role": r.role,
        "correlation_status": r.correlation_status,
    }
    if not include_provider_log:
        payload["provider_extras"] = {
            key: value
            for key, value in (r.provider_extras or {}).items()
            if key != "_loom_raw_provider_log"
        }
    return payload
