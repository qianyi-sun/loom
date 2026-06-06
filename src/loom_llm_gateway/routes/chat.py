"""POST /v1/chat/completions — OpenAI-compatible endpoint with Loom attribution."""

from __future__ import annotations

import time
import uuid as uuid_lib
from typing import Any

from fastapi import APIRouter, Header, HTTPException, Request

from loom.models.types import ModelSpec
from loom_llm_gateway import litellm_wrapper
from loom_llm_gateway.auth import verify_bearer_token
from loom_llm_gateway.errors import RateCardNotFoundError
from loom_llm_gateway.rate_card import (
    compute_cost_usd,
    hash_table,
    lookup_entry,
)

router = APIRouter()


@router.post("/v1/chat/completions")
async def chat_completions(
    request: Request,
    authorization: str | None = Header(default=None),
) -> dict[str, Any]:
    body = await request.json()

    # Authentication.
    async with request.app.state.session_factory() as session:
        ctx = await verify_bearer_token(session, authorization)
    if ctx is None:
        raise HTTPException(status_code=401, detail="invalid bearer token")

    # Loom metadata block (required so emitted billing events can be attributed).
    loom = body.get("loom")
    if not isinstance(loom, dict):
        raise HTTPException(status_code=400, detail="missing `loom` metadata block")
    for k in ("team_id", "trial_id", "step_id"):
        if k not in loom:
            raise HTTPException(status_code=400, detail=f"loom.{k} missing")

    # Provider extraction: "provider/name" or bare "name" (defaults openai).
    raw_model = body["model"]
    if "/" in raw_model:
        provider, model_name = raw_model.split("/", 1)
    else:
        provider, model_name = "openai", raw_model

    # Rate card lookup (cached).
    table = await request.app.state.rate_card_cache.get()
    spec = ModelSpec(
        provider=provider, name=model_name,
        tier=loom.get("tier"), region=loom.get("region"),
    )
    try:
        entry = lookup_entry(table, spec)
    except RateCardNotFoundError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    # Forward to provider.
    settings = request.app.state.settings
    api_key = _pick_api_key(provider, settings)
    started = time.monotonic()
    raw = await litellm_wrapper.acompletion(
        model=raw_model,
        messages=body["messages"],
        api_key=api_key,
        timeout=settings.upstream_timeout_sec,
        **{
            k: v for k, v in body.items()
            if k not in ("model", "messages", "loom")
        },
    )
    duration_sec = time.monotonic() - started

    # Parse + cost.
    parsed = litellm_wrapper.parse_litellm_response(raw, provider=provider)
    cost = compute_cost_usd(
        entry,
        input_tokens=parsed.input_tokens,
        output_tokens=parsed.output_tokens,
        cached_input_tokens=parsed.cached_input_tokens,
        cache_write_tokens=parsed.cache_write_tokens,
    )

    # Build response.
    loom_block = {
        "input_tokens": parsed.input_tokens,
        "cached_input_tokens": parsed.cached_input_tokens,
        "cache_write_tokens": parsed.cache_write_tokens,
        "output_tokens": parsed.output_tokens,
        "thinking_tokens": parsed.thinking_tokens,
        "provider_extras": parsed.provider_extras,
        "cost_usd": cost,
        "rate_card_hash": hash_table(table),
        "finish_reason": parsed.finish_reason,
        "duration_sec": duration_sec,
        "streamed": False,
        "time_to_first_token_sec": None,
        "gateway_request_id": str(uuid_lib.uuid4()),
    }
    response = dict(parsed.raw_response)
    response["loom"] = loom_block
    return response


def _pick_api_key(provider: str, settings: Any) -> str | None:
    if provider == "anthropic" and settings.anthropic_api_key is not None:
        return str(settings.anthropic_api_key.get_secret_value())
    if provider == "openai" and settings.openai_api_key is not None:
        return str(settings.openai_api_key.get_secret_value())
    if provider == "together" and settings.together_api_key is not None:
        return str(settings.together_api_key.get_secret_value())
    return None
