"""POST /v1/chat/completions — OpenAI-compatible endpoint with Loom attribution."""

from __future__ import annotations

import time
import uuid as uuid_lib
from typing import Any

from fastapi import APIRouter, Header, HTTPException, Request
from pydantic import BaseModel, ConfigDict, Field, ValidationError

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

# Reserved kwargs we never forward as a splat from `body`. The chat route
# passes these explicitly to acompletion; if a client (accidentally or
# maliciously) includes them in the body, splatting would shadow our named
# args and TypeError out with a 500.
_RESERVED_BODY_KEYS = frozenset(
    {"model", "messages", "loom", "api_key", "timeout"},
)

# Providers we accept on the wire. The Gateway only holds keys for these;
# any other provider in `model="X/Y"` is rejected with 400 upfront rather
# than silently passing api_key=None into LiteLLM.
_SUPPORTED_PROVIDERS = frozenset({"anthropic", "openai", "together"})


class _LoomBlock(BaseModel):
    """Required Loom attribution block on every chat request."""

    model_config = ConfigDict(extra="allow")  # allow future-proof extras
    team_id: str = Field(min_length=1)
    trial_id: str = Field(min_length=1)
    step_id: str = Field(min_length=1)
    tier: str | None = None
    region: str | None = None


class ChatRequest(BaseModel):
    """Required-shape envelope for /v1/chat/completions.

    extra="allow" so OpenAI-compatible extras (temperature, max_tokens,
    tools, ...) pass through to LiteLLM via the splat path.
    """

    model_config = ConfigDict(extra="allow")
    model: str = Field(min_length=1)
    messages: list[dict[str, Any]] = Field(min_length=1)
    loom: _LoomBlock


@router.post("/v1/chat/completions")
async def chat_completions(
    request: Request,
    authorization: str | None = Header(default=None),
) -> dict[str, Any]:
    raw_body = await request.json()
    # Bugs 3 + 4 fix: validate shape upfront → 400 with structured detail
    # instead of KeyError → 500, and exclude reserved kwargs from the splat.
    try:
        req = ChatRequest.model_validate(raw_body)
    except ValidationError as exc:
        raise HTTPException(status_code=400, detail=exc.errors()) from exc

    # Authentication.
    async with request.app.state.session_factory() as session:
        ctx = await verify_bearer_token(session, authorization)
    if ctx is None:
        raise HTTPException(status_code=401, detail="invalid bearer token")

    # Bug 1 fix: the bearer token's team_id is the source of truth. If the
    # token is team-scoped (ctx.team_id is not None), the client-supplied
    # loom.team_id must match — otherwise team A could attribute spend to
    # team B by lying in the body.
    if ctx.team_id is not None and req.loom.team_id != str(ctx.team_id):
        raise HTTPException(
            status_code=403,
            detail="loom.team_id does not match token's team",
        )

    # Provider extraction: "provider/name" or bare "name" (defaults openai).
    raw_model = req.model
    if "/" in raw_model:
        provider, model_name = raw_model.split("/", 1)
    else:
        provider, model_name = "openai", raw_model

    # Bug 6 fix: reject unsupported providers upfront with a clear message
    # rather than passing api_key=None into LiteLLM and getting a cryptic
    # upstream error.
    if provider not in _SUPPORTED_PROVIDERS:
        raise HTTPException(
            status_code=400,
            detail=(
                f"unsupported provider {provider!r}; "
                f"allowed: {sorted(_SUPPORTED_PROVIDERS)}"
            ),
        )

    # Rate card lookup (cached).
    table = await request.app.state.rate_card_cache.get()
    spec = ModelSpec(
        provider=provider, name=model_name,
        tier=req.loom.tier, region=req.loom.region,
    )
    try:
        entry = lookup_entry(table, spec)
    except RateCardNotFoundError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    # Forward to provider.
    settings = request.app.state.settings
    api_key = _pick_api_key(provider, settings)
    extra_kwargs = {
        k: v for k, v in raw_body.items()
        if k not in _RESERVED_BODY_KEYS
    }
    started = time.monotonic()
    raw = await litellm_wrapper.acompletion(
        model=raw_model,
        messages=req.messages,
        api_key=api_key,
        timeout=settings.upstream_timeout_sec,
        **extra_kwargs,
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
