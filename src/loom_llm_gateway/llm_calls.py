"""llm_calls insert helper — one writer for all dialect endpoints
(Plan 9 Task 6)."""

from __future__ import annotations

from typing import Any
from uuid import UUID

from sqlalchemy import insert
from sqlalchemy.ext.asyncio import AsyncSession

from loom.db.schema import LlmCall
from loom.request_params import coerce_request_params, normalize_request_params
from loom_llm_gateway.dialect import TokenUsage
from loom_llm_gateway.metrics import COST_USD_TOTAL, LLM_CALLS_TOTAL


async def record_call(
    session: AsyncSession,
    *,
    team_id: UUID,
    trial_id: UUID,
    step_id: str,
    dialect: str,
    model: str,
    usage: TokenUsage,
    cost_usd: float,
    rate_card_hash: str,
    provider: str | None = None,
    attempt: int = 1,
    request_params: dict[str, Any] | None = None,
) -> None:
    """Insert one row into `llm_calls`. Called by every dialect endpoint
    (chat / messages / responses / gemini) AFTER the upstream provider
    returns successfully. The trial's worker reads these rows at finalize
    (via the CP endpoint) and projects each into an LLMCallEvent.

    Side effect (#81 slice B-2): emits prometheus counters for
    `loom_gateway_llm_calls_total{result="ok"}` and
    `loom_gateway_cost_usd_total`. `provider` is the connection's
    `provider_type` (openai, anthropic, google, openai-compatible,
    custom) — defaults to `dialect` if not supplied for backwards
    compatibility with callers that haven't been updated.

    `attempt` is the gateway-internal attempt number that produced
    this successful row (#298 Slice B). Defaults to 1 so callers that
    don't go through the retry helper keep the historical semantics.
    """
    audit_request_params = (
        normalize_request_params({})
        if request_params is None
        else coerce_request_params(request_params)
    )
    await session.execute(
        insert(LlmCall).values(
            team_id=team_id,
            trial_id=trial_id,
            step_id=step_id,
            dialect=dialect,
            model=model,
            input_tokens=usage.input_tokens,
            output_tokens=usage.output_tokens,
            provider_extras=usage.provider_extras,
            request_params=audit_request_params,
            cost_usd=cost_usd,
            rate_card_hash=rate_card_hash,
            attempt=attempt,
        )
    )
    await session.commit()
    provider_label = provider if provider is not None else dialect
    LLM_CALLS_TOTAL.labels(
        provider=provider_label,
        dialect=dialect,
        result="ok",
    ).inc()
    if cost_usd > 0:
        COST_USD_TOTAL.labels(
            team_id=str(team_id),
            provider=provider_label,
        ).inc(cost_usd)


async def record_failed_call(
    session: AsyncSession,
    *,
    team_id: UUID,
    trial_id: UUID,
    step_id: str,
    dialect: str,
    model: str,
    provider: str | None = None,
    attempt: int = 1,
    request_params: dict[str, Any] | None = None,
    failure_category: str,
    failure_status_code: int | None = None,
    failure_error_type: str | None = None,
) -> None:
    """Insert one zero-token audit row for an attempted upstream call.

    This is intentionally separate from `record_call`: successful calls carry
    usage/cost, while failed upstream attempts preserve attribution and safe
    request controls so debug surfaces can distinguish "no request attempted"
    from "request attempted and failed upstream".
    """
    audit_request_params = (
        normalize_request_params({})
        if request_params is None
        else coerce_request_params(request_params)
    )
    provider_extras: dict[str, Any] = {
        "_loom_call_status": "failed",
        "_loom_failure_category": failure_category,
        "_loom_usage_status": "missing",
    }
    if failure_status_code is not None:
        provider_extras["_loom_failure_status_code"] = int(failure_status_code)
    if failure_error_type:
        provider_extras["_loom_failure_error_type"] = failure_error_type

    await session.execute(
        insert(LlmCall).values(
            team_id=team_id,
            trial_id=trial_id,
            step_id=step_id,
            dialect=dialect,
            model=model,
            input_tokens=0,
            output_tokens=0,
            provider_extras=provider_extras,
            request_params=audit_request_params,
            cost_usd=0,
            rate_card_hash="failed-upstream",
            attempt=max(int(attempt or 1), 1),
        )
    )
    await session.commit()
    provider_label = provider if provider is not None else dialect
    result = "timeout" if failure_category == "upstream_timeout" else "upstream_error"
    LLM_CALLS_TOTAL.labels(
        provider=provider_label,
        dialect=dialect,
        result=result,
    ).inc()
