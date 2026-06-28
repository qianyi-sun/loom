"""POST /v1beta/models/{model}:generateContent — Gemini native passthrough
(Plan 9 Task 9 + A9.1).

Gemini's native shape uses `contents`/`parts` (not OpenAI `messages`),
`functionCall` parts (not `tool_calls`), and `usageMetadata` (not
`usage`). LiteLLM's round-trip would lose multi-part inlineData
attachments. We passthrough natively.
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
from loom_llm_gateway.request_params import normalize_request_params
from loom_llm_gateway.retry import send_with_retry

router = APIRouter()

GEMINI_BASE_URL = "https://generativelanguage.googleapis.com"


@router.post("/v1beta/models/{model_path}")
async def gemini_generate_content(
    model_path: str,
    request: Request,
    payload: dict[str, Any],
    authorization: str | None = Header(default=None),
) -> dict[str, Any]:
    """The colon in the path (`:generateContent` etc.) lives inside the
    `model_path` capture — Gemini supports `:generateContent`,
    `:streamGenerateContent`, `:countTokens`, etc. We forward whatever
    the caller asked for and only cost-attribute the response if it
    contained a `usageMetadata` block."""
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
    if settings.google_api_key is None:
        raise HTTPException(
            status_code=503, detail="google_api_key not configured on Gateway",
        )

    # Parse the model name out of "<name>:<action>" — e.g. the path
    # `gemini-2.0-flash:generateContent` yields model_name=gemini-2.0-flash,
    # action=generateContent. We record cost against model_name.
    if ":" not in model_path:
        raise HTTPException(
            status_code=400,
            detail="path must be <model>:<action>, e.g. gemini-2.0-flash:generateContent",
        )
    model_name, action = model_path.split(":", 1)
    # Plan 9 audit fix: streaming variant loses the final usage block on
    # mid-stream connection close; refuse in v1.
    if action.startswith("stream"):
        raise HTTPException(
            status_code=400,
            detail=f"action {action!r} not supported on the Gateway in v1 "
                   "(cost attribution requires the final usage block)",
        )

    upstream: httpx.AsyncClient = request.app.state.upstream_client
    outcome = await send_with_retry(
        lambda: upstream.post(
            f"{GEMINI_BASE_URL}/v1beta/models/{model_path}",
            json=payload,
            params={"key": settings.google_api_key.get_secret_value()},
            headers={"content-type": "application/json"},
            timeout=settings.upstream_timeout_sec,
        ),
        settings=settings, dialect="gemini",
    )
    upstream_response = outcome.response
    if upstream_response.status_code >= 400:
        raise HTTPException(
            status_code=upstream_response.status_code,
            detail=f"gemini upstream returned "
                   f"{upstream_response.status_code}: "
                   f"{upstream_response.text[:500]}",
        )
    body: dict[str, Any] = upstream_response.json()

    usage = DIALECTS["gemini"].extract_tokens(body)
    # `countTokens` legitimately has no usageMetadata block — return
    # the response without inserting a cost row. `generateContent` MUST
    # carry it; a missing block on that action signals an upstream
    # contract violation.
    if usage.input_tokens == 0 and usage.output_tokens == 0:
        if action == "countTokens":
            return body
        raise HTTPException(
            status_code=502,
            detail=f"gemini 200 response missing usageMetadata for action "
                   f"{action!r}; cost cannot be attributed",
        )

    table = await request.app.state.rate_card_cache.get()
    entry = lookup_entry(
        table, ModelSpec(provider="google", name=model_name),
    )
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
            dialect="gemini",
            model=model_name,
            usage=usage,
            cost_usd=cost,
            rate_card_hash=hash_table(table),
            attempt=outcome.attempt,
            request_params=normalize_request_params(payload),
        )
    return body
