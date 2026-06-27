"""Shared LLM usage and cost-status projection helpers."""

from __future__ import annotations

from decimal import Decimal
from typing import Any
from uuid import UUID

from sqlalchemy import select

from loom.db.schema import LlmCall, RateCard
from loom_llm_gateway.dialect import USAGE_STATUS_KEY
from loom_llm_gateway.rate_card import RateCardTable, hash_table


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
    partial_usage_llm_calls_count: int = 0,
    missing_usage_llm_calls_count: int = 0,
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
    partial_usage = max(int(partial_usage_llm_calls_count or 0), 0)
    missing_usage = max(int(missing_usage_llm_calls_count or 0), 0)
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

    if llm_calls == 0:
        usage_reporting_status = "no_usage"
        usage_estimate_confidence = "none"
    elif missing_usage >= llm_calls:
        usage_reporting_status = "missing"
        usage_estimate_confidence = "missing"
    elif missing_usage or partial_usage:
        usage_reporting_status = "partial"
        usage_estimate_confidence = "partial"
    else:
        usage_reporting_status = "complete"
        usage_estimate_confidence = "high"

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
        "partial_usage_llm_calls_count": partial_usage,
        "missing_usage_llm_calls_count": missing_usage,
        "usage_reporting_status": usage_reporting_status,
        "usage_estimate_confidence": usage_estimate_confidence,
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


def usage_status_filter(status: str) -> Any:
    return LlmCall.provider_extras.op("->>")(USAGE_STATUS_KEY) == status


def _is_facade_rate_card_hash(value: str) -> bool:
    return value.startswith("facade:")


def _rate_card_snapshot(
    *,
    rate_card_hash: str,
    table: RateCardTable | None,
) -> dict[str, Any]:
    return {
        "rate_card_hash": rate_card_hash,
        "rate_card_id": table.id if table is not None else None,
        "resolved": table is not None,
        "provider": table.provider if table is not None else None,
        "source_url": table.source_url if table is not None else None,
        "pricing_version": table.pricing_version if table is not None else None,
        "last_checked_at": (
            table.last_checked_at.isoformat()
            if table is not None and table.last_checked_at is not None
            else None
        ),
        "currency": table.currency if table is not None else None,
        "group": table.group if table is not None else None,
        "group_ratio": table.group_ratio if table is not None else None,
    }


async def price_snapshots_for_hashes(
    session: Any,
    hashes: list[str] | set[str] | tuple[str, ...],
) -> list[dict[str, Any]]:
    """Resolve persisted LLM-call rate-card hashes to source snapshots.

    Facade pseudo-hashes such as `facade:tokens-only` are cost-status signals,
    not immutable hosted-pricing snapshots, so they are intentionally omitted.
    """

    wanted = sorted(
        {
            str(value)
            for value in hashes
            if isinstance(value, str) and value and not _is_facade_rate_card_hash(value)
        }
    )
    if not wanted:
        return []

    tables_by_hash: dict[str, RateCardTable] = {}
    rows = (await session.execute(select(RateCard))).scalars().all()
    for row in rows:
        payload = dict(row.table or {})
        payload["id"] = row.id
        payload["captured_at"] = row.captured_at
        try:
            table = RateCardTable.model_validate(payload)
        except ValueError:
            continue
        table_hash = hash_table(table)
        if table_hash in wanted:
            tables_by_hash[table_hash] = table

    return [
        _rate_card_snapshot(
            rate_card_hash=rate_card_hash,
            table=tables_by_hash.get(rate_card_hash),
        )
        for rate_card_hash in wanted
    ]


async def price_snapshots_for_trials(
    session: Any,
    trial_ids: list[UUID] | tuple[UUID, ...] | set[UUID],
) -> list[dict[str, Any]]:
    if not trial_ids:
        return []
    hashes = (
        await session.execute(
            select(LlmCall.rate_card_hash)
            .where(LlmCall.trial_id.in_(list(trial_ids)))
            .distinct(),
        )
    ).scalars()
    return await price_snapshots_for_hashes(session, set(hashes))
