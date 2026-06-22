"""POST /v1/responses — OpenAI Responses dialect native passthrough
(Plan 9 Task 8 + A9.1).

LiteLLM does not yet have a stable Responses adapter; we passthrough to
OpenAI's native endpoint and preserve the reasoning content type +
output_tokens_details.reasoning_tokens counter.
"""

from __future__ import annotations

from typing import Any

import httpx
from fastapi import APIRouter, Header, HTTPException, Request

from loom.auth import verify_bearer_token
from loom.models.types import ModelSpec
from loom_llm_gateway.dialect import DIALECTS
from loom_llm_gateway.llm_calls import record_call
from loom_llm_gateway.rate_card import (
    compute_cost_usd,
    hash_table,
    lookup_entry,
)
from loom_llm_gateway.retry import send_with_retry

router = APIRouter()

OPENAI_BASE_URL = "https://api.openai.com"


@router.post("/v1/responses")
async def responses(
    request: Request,
    payload: dict[str, Any],
    authorization: str | None = Header(default=None),
) -> dict[str, Any]:
    settings = request.app.state.settings
    signing_key = settings.step_jwt_signing_key.get_secret_value()
    async with request.app.state.session_factory() as session:
        ctx = await verify_bearer_token(
            session, authorization, signing_key=signing_key,
        )
    if ctx is None or "llm:call" not in ctx.scopes:
        raise HTTPException(status_code=401, detail="not authorized")
    if ctx.trial_id is None or ctx.step_id is None or ctx.team_id is None:
        raise HTTPException(
            status_code=403, detail="step-scoped token required",
        )
    if settings.openai_api_key is None:
        raise HTTPException(
            status_code=503, detail="openai_api_key not configured on Gateway",
        )
    model_name = payload.get("model")
    if not isinstance(model_name, str) or not model_name:
        raise HTTPException(status_code=400, detail="`model` is required")
    if payload.get("stream"):
        raise HTTPException(
            status_code=400,
            detail="stream=true not supported on the Gateway in v1 "
                   "(cost attribution requires the final usage block)",
        )

    upstream: httpx.AsyncClient = request.app.state.upstream_client
    upstream_response = await send_with_retry(
        lambda: upstream.post(
            f"{OPENAI_BASE_URL}/v1/responses",
            json=payload,
            headers={
                "Authorization": f"Bearer {settings.openai_api_key.get_secret_value()}",
                "content-type": "application/json",
            },
            timeout=settings.upstream_timeout_sec,
        ),
        settings=settings, dialect="responses",
    )
    if upstream_response.status_code >= 400:
        raise HTTPException(
            status_code=upstream_response.status_code,
            detail=f"openai responses upstream returned "
                   f"{upstream_response.status_code}: "
                   f"{upstream_response.text[:500]}",
        )
    body: dict[str, Any] = upstream_response.json()

    usage = DIALECTS["openai_responses"].extract_tokens(body)
    if usage.input_tokens == 0 and usage.output_tokens == 0:
        raise HTTPException(
            status_code=502,
            detail="openai responses 200 missing usage block; "
                   "cost cannot be attributed",
        )
    table = await request.app.state.rate_card_cache.get()
    entry = lookup_entry(table, ModelSpec(provider="openai", name=model_name))
    cost = compute_cost_usd(
        entry,
        input_tokens=usage.input_tokens,
        output_tokens=usage.output_tokens,
        cached_input_tokens=usage.cached_input_tokens,
        cache_write_tokens=usage.cache_write_tokens,
    )
    async with request.app.state.session_factory() as session:
        await record_call(
            session,
            team_id=ctx.team_id,
            trial_id=ctx.trial_id,
            step_id=ctx.step_id,
            dialect="openai_responses",
            model=model_name,
            usage=usage,
            cost_usd=cost,
            rate_card_hash=hash_table(table),
        )
    return body
