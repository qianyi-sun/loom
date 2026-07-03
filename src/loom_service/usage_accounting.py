"""Shared LLM usage and cost-status projection helpers."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any
from uuid import UUID

from sqlalchemy import select

from loom.db.schema import LlmCall, ProviderConnection, RateCard
from loom.models.types import ModelSpec
from loom_llm_gateway.dialect import USAGE_STATUS_KEY
from loom_llm_gateway.errors import RateCardNotFoundError
from loom_llm_gateway.rate_card import (
    RateCardTable,
    compute_cost_usd,
    hash_table,
    lookup_entry,
)
from loom_llm_gateway.yibuapi_pricing import (
    YIBUAPI_RATE_CARD_PROVIDER,
    normalize_yibuapi_model_name,
)

_TYPE_TO_DEFAULT_RATE_CARD_PROVIDER = {
    "anthropic": "anthropic",
    "google": "google",
    "openai-compatible": "openai",
}

BUDGET_HARD_LIMIT_REASON = "budget_hard_limit_exceeded"


@dataclass(frozen=True)
class PreRunBudgetEstimate:
    budget_usd: float | None
    budget_policy: str
    pre_run_estimated_cost_usd: float | None
    cost_estimate_source: str
    cost_estimate_confidence: str
    pre_run_estimated_llm_calls_count: int
    pre_run_estimated_prompt_tokens: int
    pre_run_estimated_completion_tokens: int
    unpriced_reason: str | None = None

    def as_api_dict(self) -> dict[str, Any]:
        return {
            "budget_usd": self.budget_usd,
            "budget_policy": self.budget_policy,
            "pre_run_estimated_cost_usd": self.pre_run_estimated_cost_usd,
            "cost_estimate_source": self.cost_estimate_source,
            "cost_estimate_confidence": self.cost_estimate_confidence,
            "pre_run_estimated_llm_calls_count": (
                self.pre_run_estimated_llm_calls_count
            ),
            "pre_run_estimated_prompt_tokens": (
                self.pre_run_estimated_prompt_tokens
            ),
            "pre_run_estimated_completion_tokens": (
                self.pre_run_estimated_completion_tokens
            ),
            "unpriced_reason": self.unpriced_reason,
        }


def _float_cost(value: Decimal | int | float | str | None) -> float:
    if value is None:
        return 0.0
    return float(Decimal(str(value)))


def _dominant_count_key(
    counts: Mapping[str, int] | None,
    *,
    fallback: str,
) -> str:
    if not counts:
        return fallback
    nonzero = {
        str(key): int(value or 0)
        for key, value in counts.items()
        if int(value or 0) > 0
    }
    if not nonzero:
        return fallback
    if len(nonzero) == 1:
        return next(iter(nonzero))
    return "mixed"


def summarize_usage_counts(
    *,
    llm_calls_count: int,
    total_prompt_tokens: int,
    total_completion_tokens: int,
    total_cost_usd: Decimal | int | float | str | None,
    priced_llm_calls_count: int,
    token_only_llm_calls_count: int,
    price_unknown_llm_calls_count: int,
    failed_upstream_llm_calls_count: int = 0,
    partial_usage_llm_calls_count: int = 0,
    missing_usage_llm_calls_count: int = 0,
    cost_source_counts: Mapping[str, int] | None = None,
    cost_confidence_counts: Mapping[str, int] | None = None,
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
    failed_upstream = max(int(failed_upstream_llm_calls_count or 0), 0)
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
    if failed_upstream:
        modes.append("failed-upstream")

    if llm_calls == 0:
        cost_status = "no_usage"
        estimated_cost_usd: float | None = None
        cost_estimate_source = "none"
        cost_estimate_confidence = "none"
    elif len(modes) > 1:
        cost_status = "mixed"
        estimated_cost_usd = cost if priced else None
        cost_estimate_source = _dominant_count_key(
            cost_source_counts,
            fallback="mixed",
        )
        cost_estimate_confidence = _dominant_count_key(
            cost_confidence_counts,
            fallback="mixed",
        )
    elif price_unknown:
        cost_status = "price_unknown"
        estimated_cost_usd = None
        cost_estimate_source = _dominant_count_key(
            cost_source_counts,
            fallback="unpriced",
        )
        cost_estimate_confidence = _dominant_count_key(
            cost_confidence_counts,
            fallback="unavailable",
        )
    elif token_only:
        cost_status = "not_applicable"
        estimated_cost_usd = None
        cost_estimate_source = _dominant_count_key(
            cost_source_counts,
            fallback="tokens-only",
        )
        cost_estimate_confidence = _dominant_count_key(
            cost_confidence_counts,
            fallback="not_applicable",
        )
    elif failed_upstream:
        cost_status = "failed_upstream"
        estimated_cost_usd = None
        cost_estimate_source = _dominant_count_key(
            cost_source_counts,
            fallback="failed-upstream",
        )
        cost_estimate_confidence = _dominant_count_key(
            cost_confidence_counts,
            fallback="unavailable",
        )
    else:
        cost_status = "estimated"
        estimated_cost_usd = cost
        if not modes:
            modes.append("priced")
        cost_estimate_source = _dominant_count_key(
            cost_source_counts,
            fallback="rate-card",
        )
        cost_estimate_confidence = _dominant_count_key(
            cost_confidence_counts,
            fallback="configured",
        )

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
        "cost_estimate_source": cost_estimate_source,
        "cost_estimate_confidence": cost_estimate_confidence,
        "pricing_modes": modes,
        "priced_llm_calls_count": priced,
        "token_only_llm_calls_count": token_only,
        "price_unknown_llm_calls_count": price_unknown,
        "failed_upstream_llm_calls_count": failed_upstream,
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


def _nullable_float(value: Decimal | int | float | str | None) -> float | None:
    if value is None:
        return None
    return float(Decimal(str(value)))


def _settings_int(settings: Any, name: str, default: int) -> int:
    try:
        value = int(getattr(settings, name))
    except (AttributeError, TypeError, ValueError):
        return default
    return max(value, 0)


def _operator_cost_usd(
    pricing_data: Mapping[str, Any] | None,
    *,
    input_tokens: int,
    output_tokens: int,
) -> float | None:
    if not pricing_data:
        return None
    try:
        input_per_1m = float(pricing_data.get("input_usd_per_1m", 0) or 0)
        output_per_1m = float(pricing_data.get("output_usd_per_1m", 0) or 0)
    except (TypeError, ValueError):
        return None
    return (
        (max(input_tokens, 0) / 1_000_000.0) * input_per_1m
        + (max(output_tokens, 0) / 1_000_000.0) * output_per_1m
    )


async def _latest_rate_card_table(session: Any) -> RateCardTable | None:
    row = (
        await session.execute(
            select(RateCard).order_by(RateCard.captured_at.desc()).limit(1),
        )
    ).scalar_one_or_none()
    if row is None:
        return None
    payload = dict(row.table or {})
    payload["id"] = row.id
    payload["captured_at"] = row.captured_at
    return RateCardTable(**payload)


async def estimate_pre_run_batch_budget(
    session: Any,
    *,
    provider_connection: ProviderConnection | None,
    provider_model_id: str | None,
    expected_trial_count: int,
    settings: Any,
    budget_usd: Decimal | int | float | str | None,
    budget_policy: str,
) -> PreRunBudgetEstimate:
    calls_per_trial = _settings_int(
        settings,
        "batch_budget_estimate_llm_calls_per_trial",
        1,
    )
    input_per_call = _settings_int(
        settings,
        "batch_budget_estimate_input_tokens_per_call",
        1_000_000,
    )
    output_per_call = _settings_int(
        settings,
        "batch_budget_estimate_output_tokens_per_call",
        0,
    )
    estimated_calls = max(int(expected_trial_count or 0), 0) * calls_per_trial
    estimated_input_tokens = estimated_calls * input_per_call
    estimated_output_tokens = estimated_calls * output_per_call
    budget_value = _nullable_float(budget_usd)
    policy = str(budget_policy or "none")

    if provider_connection is None or not provider_model_id:
        return PreRunBudgetEstimate(
            budget_usd=budget_value,
            budget_policy=policy,
            pre_run_estimated_cost_usd=None,
            cost_estimate_source="none",
            cost_estimate_confidence="unavailable",
            pre_run_estimated_llm_calls_count=estimated_calls,
            pre_run_estimated_prompt_tokens=estimated_input_tokens,
            pre_run_estimated_completion_tokens=estimated_output_tokens,
            unpriced_reason="provider_model_not_configured",
        )

    pricing_source = str(provider_connection.pricing_source or "tokens-only")
    if pricing_source == "operator-supplied":
        cost = _operator_cost_usd(
            provider_connection.pricing_data,
            input_tokens=estimated_input_tokens,
            output_tokens=estimated_output_tokens,
        )
        if cost is None:
            return PreRunBudgetEstimate(
                budget_usd=budget_value,
                budget_policy=policy,
                pre_run_estimated_cost_usd=None,
                cost_estimate_source="unpriced",
                cost_estimate_confidence="unavailable",
                pre_run_estimated_llm_calls_count=estimated_calls,
                pre_run_estimated_prompt_tokens=estimated_input_tokens,
                pre_run_estimated_completion_tokens=estimated_output_tokens,
                unpriced_reason="invalid_operator_pricing",
            )
        return PreRunBudgetEstimate(
            budget_usd=budget_value,
            budget_policy=policy,
            pre_run_estimated_cost_usd=cost,
            cost_estimate_source="operator-supplied",
            cost_estimate_confidence="configured",
            pre_run_estimated_llm_calls_count=estimated_calls,
            pre_run_estimated_prompt_tokens=estimated_input_tokens,
            pre_run_estimated_completion_tokens=estimated_output_tokens,
        )

    if pricing_source != "rate-card":
        return PreRunBudgetEstimate(
            budget_usd=budget_value,
            budget_policy=policy,
            pre_run_estimated_cost_usd=None,
            cost_estimate_source="tokens-only",
            cost_estimate_confidence="not_applicable",
            pre_run_estimated_llm_calls_count=estimated_calls,
            pre_run_estimated_prompt_tokens=estimated_input_tokens,
            pre_run_estimated_completion_tokens=estimated_output_tokens,
        )

    lookup_provider = (
        provider_connection.rate_card_provider
        or _TYPE_TO_DEFAULT_RATE_CARD_PROVIDER.get(provider_connection.provider_type)
    )
    if lookup_provider is None:
        return PreRunBudgetEstimate(
            budget_usd=budget_value,
            budget_policy=policy,
            pre_run_estimated_cost_usd=None,
            cost_estimate_source="unpriced",
            cost_estimate_confidence="unavailable",
            pre_run_estimated_llm_calls_count=estimated_calls,
            pre_run_estimated_prompt_tokens=estimated_input_tokens,
            pre_run_estimated_completion_tokens=estimated_output_tokens,
            unpriced_reason="rate_card_provider_not_configured",
        )

    table = await _latest_rate_card_table(session)
    if table is None:
        return PreRunBudgetEstimate(
            budget_usd=budget_value,
            budget_policy=policy,
            pre_run_estimated_cost_usd=None,
            cost_estimate_source="unpriced",
            cost_estimate_confidence="unavailable",
            pre_run_estimated_llm_calls_count=estimated_calls,
            pre_run_estimated_prompt_tokens=estimated_input_tokens,
            pre_run_estimated_completion_tokens=estimated_output_tokens,
            unpriced_reason="rate_card_unavailable",
        )

    lookup_model = (
        normalize_yibuapi_model_name(provider_model_id)
        if lookup_provider == YIBUAPI_RATE_CARD_PROVIDER
        else provider_model_id
    )
    try:
        entry = lookup_entry(
            table,
            ModelSpec(provider=lookup_provider, name=lookup_model),
        )
    except RateCardNotFoundError:
        return PreRunBudgetEstimate(
            budget_usd=budget_value,
            budget_policy=policy,
            pre_run_estimated_cost_usd=None,
            cost_estimate_source="unpriced",
            cost_estimate_confidence="unavailable",
            pre_run_estimated_llm_calls_count=estimated_calls,
            pre_run_estimated_prompt_tokens=estimated_input_tokens,
            pre_run_estimated_completion_tokens=estimated_output_tokens,
            unpriced_reason="missing_rate_card_entry",
        )

    cost = compute_cost_usd(
        entry,
        input_tokens=estimated_input_tokens,
        output_tokens=estimated_output_tokens,
        cached_input_tokens=0,
        cache_write_tokens=0,
    )
    return PreRunBudgetEstimate(
        budget_usd=budget_value,
        budget_policy=policy,
        pre_run_estimated_cost_usd=cost,
        cost_estimate_source="rate-card",
        cost_estimate_confidence="configured",
        pre_run_estimated_llm_calls_count=estimated_calls,
        pre_run_estimated_prompt_tokens=estimated_input_tokens,
        pre_run_estimated_completion_tokens=estimated_output_tokens,
    )


def project_batch_budget(
    batch: Any,
    usage: Mapping[str, Any],
) -> dict[str, Any]:
    budget_usd = _nullable_float(getattr(batch, "budget_usd", None))
    policy = str(getattr(batch, "budget_policy", "none") or "none")
    pre_run_cost = _nullable_float(
        getattr(batch, "pre_run_estimated_cost_usd", None),
    )
    consumed = usage.get("estimated_cost_usd")
    consumed_float = float(consumed) if isinstance(consumed, (int, float)) else None
    llm_calls = int(usage.get("llm_calls_count") or 0)
    if budget_usd is None or policy == "none":
        remaining: float | None = None
        status = "none"
    elif consumed_float is not None:
        remaining = budget_usd - consumed_float
        if consumed_float > budget_usd:
            status = f"{policy}_budget_exceeded"
        else:
            status = "within_budget"
    elif llm_calls == 0:
        remaining = budget_usd
        if pre_run_cost is not None and pre_run_cost > budget_usd:
            status = f"{policy}_over_pre_run_estimate"
        else:
            status = "within_budget"
    else:
        remaining = None
        status = "unknown_live_cost"

    return {
        "budget_usd": budget_usd,
        "budget_policy": policy,
        "budget_remaining_usd": remaining,
        "budget_consumed_usd": consumed_float,
        "budget_status": status,
        "pre_run_estimated_cost_usd": pre_run_cost,
        "pre_run_cost_estimate_source": (
            getattr(batch, "pre_run_cost_estimate_source", None)
        ),
        "pre_run_cost_estimate_confidence": (
            getattr(batch, "pre_run_cost_estimate_confidence", None)
        ),
        "budget_diagnostics": getattr(batch, "budget_diagnostics", None) or [],
    }


def hard_budget_exceeded_diagnostic(
    *,
    batch_id: UUID,
    budget_usd: float,
    estimated_cost_usd: float,
) -> dict[str, Any]:
    return {
        "reason": BUDGET_HARD_LIMIT_REASON,
        "batch_id": str(batch_id),
        "budget_usd": budget_usd,
        "estimated_cost_usd": estimated_cost_usd,
        "seen_at": datetime.now(UTC).isoformat(),
    }


_TERMINAL_TRIAL_STATES: frozenset[str] = frozenset(
    {"succeeded", "failed", "cancelled"},
)


def _model_from_config(config: Any) -> Mapping[str, Any] | None:
    if not isinstance(config, Mapping):
        return None
    model = config.get("agent_model")
    if isinstance(model, Mapping) and model:
        return model
    agent = config.get("agent")
    if isinstance(agent, Mapping):
        legacy_model = agent.get("model")
        if isinstance(legacy_model, Mapping) and legacy_model:
            return legacy_model
    return None


def trial_requires_llm(trial: Any) -> bool:
    """Return True when a trial is configured to call a real model provider."""

    if getattr(trial, "provider_connection_id", None) is not None:
        return True
    provider_model_id = getattr(trial, "provider_model_id", None)
    if isinstance(provider_model_id, str) and provider_model_id:
        return True
    return _model_from_config(getattr(trial, "config", None)) is not None


def project_trial_llm_evidence(
    trial: Any,
    *,
    llm_calls_count: int,
) -> dict[str, Any]:
    """Project whether persisted LLM-call evidence is usable for a trial."""

    requires_llm = trial_requires_llm(trial)
    state = str(getattr(trial, "state", ""))
    calls = max(int(llm_calls_count or 0), 0)
    if not requires_llm:
        return {
            "llm_evidence_status": "not_applicable",
            "no_call": False,
        }
    if state not in _TERMINAL_TRIAL_STATES:
        return {
            "llm_evidence_status": "pending",
            "no_call": False,
        }
    if calls == 0:
        return {
            "llm_evidence_status": "no_calls_invalid",
            "no_call": True,
        }
    return {
        "llm_evidence_status": "calls_observed",
        "no_call": False,
    }


def llm_call_counts_by_trial_id(
    llm_calls: Sequence[LlmCall],
) -> dict[UUID, int]:
    counts: dict[UUID, int] = {}
    for call in llm_calls:
        if call.trial_id is None:
            continue
        counts[call.trial_id] = counts.get(call.trial_id, 0) + 1
    return counts


def summarize_llm_evidence_for_trials(
    trials: Sequence[Any],
    *,
    llm_call_counts: Mapping[UUID, int],
) -> dict[str, Any]:
    terminal_model_backed = 0
    no_call = 0
    for trial in trials:
        if str(getattr(trial, "state", "")) not in _TERMINAL_TRIAL_STATES:
            continue
        if not trial_requires_llm(trial):
            continue
        terminal_model_backed += 1
        calls = int(llm_call_counts.get(trial.id, 0) or 0)
        if calls == 0:
            no_call += 1

    total_calls = sum(max(int(value or 0), 0) for value in llm_call_counts.values())
    if terminal_model_backed == 0:
        status = "not_applicable"
    elif no_call == 0:
        status = "calls_observed" if total_calls > 0 else "pending"
    elif total_calls == 0:
        status = "no_calls_invalid"
    else:
        status = "partial_no_calls"

    return {
        "llm_evidence_status": status,
        "no_call_trial_count": no_call,
        "model_backed_terminal_trial_count": terminal_model_backed,
    }


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
            select(LlmCall.rate_card_hash).where(LlmCall.trial_id.in_(list(trial_ids))).distinct(),
        )
    ).scalars()
    return await price_snapshots_for_hashes(session, set(hashes))
