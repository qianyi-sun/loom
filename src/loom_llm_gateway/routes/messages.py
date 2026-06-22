"""POST /v1/messages — Anthropic native passthrough (Plan 9 Task 7).

Per amendment A9.1, this route does NOT round-trip through LiteLLM
(which would lose cache_control, tool_use blocks, and system-block
separation). Instead, we httpx-passthrough to Anthropic's native
endpoint and forward the response verbatim. The Gateway extracts the
`usage` block for cost attribution + llm_calls insertion.
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
from loom_llm_gateway.routes._facade_common import redact_api_key

router = APIRouter()

ANTHROPIC_BASE_URL = "https://api.anthropic.com"


@router.post("/v1/messages")
async def messages(
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
            status_code=403,
            detail="step-scoped token required (loom_step_<jwt>)",
        )

    if settings.anthropic_api_key is None:
        raise HTTPException(
            status_code=503,
            detail="anthropic_api_key not configured on Gateway",
        )
    api_key = settings.anthropic_api_key.get_secret_value()
    model_name = payload.get("model")
    if not isinstance(model_name, str) or not model_name:
        raise HTTPException(status_code=400, detail="`model` is required")

    # Plan 9 audit fix: streaming responses lose the final usage block on
    # mid-stream connection close + can't be cost-attributed atomically.
    # v1 refuses stream=true; clients that want SSE must wait for v1.5.
    if payload.get("stream"):
        raise HTTPException(
            status_code=400,
            detail="stream=true not supported on the Gateway in v1 "
                   "(cost attribution requires the final usage block; "
                   "v1.5 will add SSE passthrough)",
        )

    # 1. Forward to Anthropic native endpoint.
    upstream: httpx.AsyncClient = request.app.state.upstream_client
    outcome = await send_with_retry(
        lambda: upstream.post(
            f"{ANTHROPIC_BASE_URL}/v1/messages",
            json=payload,
            headers={
                "x-api-key": api_key,
                "anthropic-version": "2023-06-01",
                "content-type": "application/json",
            },
            timeout=settings.upstream_timeout_sec,
        ),
        settings=settings, dialect="anthropic",
    )
    upstream_response = outcome.response
    if upstream_response.status_code >= 400:
        excerpt = redact_api_key(upstream_response.text, api_key)
        raise HTTPException(
            status_code=upstream_response.status_code,
            detail=f"anthropic upstream returned {upstream_response.status_code}: "
                   f"{excerpt}",
        )
    body: dict[str, Any] = upstream_response.json()

    # 2. Extract usage + record cost. Anthropic 200 responses always
    # include a usage block; if it's missing we treat this as a
    # surprising upstream contract violation and 502 — better than
    # silently inserting a zero-cost row.
    usage = DIALECTS["anthropic"].extract_tokens(body)
    if usage.input_tokens == 0 and usage.output_tokens == 0:
        raise HTTPException(
            status_code=502,
            detail="anthropic 200 response missing usage block; "
                   "cost cannot be attributed",
        )
    table = await request.app.state.rate_card_cache.get()
    entry = lookup_entry(table, ModelSpec(provider="anthropic", name=model_name))
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
            dialect="anthropic",
            model=model_name,
            usage=usage,
            cost_usd=cost,
            rate_card_hash=hash_table(table),
            attempt=outcome.attempt,
        )
    return body
