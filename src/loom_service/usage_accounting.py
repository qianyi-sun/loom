"""Shared LLM usage and cost-status projection helpers."""

from __future__ import annotations

from decimal import Decimal
from typing import Any


def _float_cost(value: Decimal | int | float | str | None) -> float:
    if value is None:
        return 0.0
    return float(Decimal(str(value)))


def summarize_usage_counts(
    *,
    llm_calls_count: int,
    total_prompt_tokens: int,
    total_completion_tokens: int,
    total_cost_usd: Decimal | int | float | str | None,
    priced_llm_calls_count: int,
    token_only_llm_calls_count: int,
    price_unknown_llm_calls_count: int,
) -> dict[str, Any]:
    """Return the API projection for an aggregate of LLM call rows.

    `total_cost_usd` is kept for backwards compatibility with existing
    clients. New clients should prefer `estimated_cost_usd` plus
    `cost_status`: token-only self-deployed/private API calls have no monetary
    amount even though their legacy `total_cost_usd` is necessarily zero.
    """

    llm_calls = max(int(llm_calls_count or 0), 0)
    prompt = max(int(total_prompt_tokens or 0), 0)
    completion = max(int(total_completion_tokens or 0), 0)
    priced = max(int(priced_llm_calls_count or 0), 0)
    token_only = max(int(token_only_llm_calls_count or 0), 0)
    price_unknown = max(int(price_unknown_llm_calls_count or 0), 0)
    cost = _float_cost(total_cost_usd)

    modes: list[str] = []
    if priced:
        modes.append("priced")
    if token_only:
        modes.append("tokens-only")
    if price_unknown:
        modes.append("price-unknown")

    if llm_calls == 0:
        cost_status = "no_usage"
        estimated_cost_usd: float | None = None
    elif len(modes) > 1:
        cost_status = "mixed"
        estimated_cost_usd = cost if priced else None
    elif price_unknown:
        cost_status = "price_unknown"
        estimated_cost_usd = None
    elif token_only:
        cost_status = "not_applicable"
        estimated_cost_usd = None
    else:
        cost_status = "estimated"
        estimated_cost_usd = cost
        if not modes:
            modes.append("priced")

    return {
        "total_prompt_tokens": prompt,
        "total_completion_tokens": completion,
        "total_tokens": prompt + completion,
        "llm_calls_count": llm_calls,
        "total_cost_usd": cost,
        "estimated_cost_usd": estimated_cost_usd,
        "cost_currency": "USD" if estimated_cost_usd is not None else None,
        "cost_status": cost_status,
        "pricing_modes": modes,
        "priced_llm_calls_count": priced,
        "token_only_llm_calls_count": token_only,
        "price_unknown_llm_calls_count": price_unknown,
    }


def empty_usage_projection() -> dict[str, Any]:
    return summarize_usage_counts(
        llm_calls_count=0,
        total_prompt_tokens=0,
        total_completion_tokens=0,
        total_cost_usd=Decimal("0"),
        priced_llm_calls_count=0,
        token_only_llm_calls_count=0,
        price_unknown_llm_calls_count=0,
    )
