"""POST /openai/v1/chat/completions — provider-connection facade
(cluster-deploy.md §Component map: `loom-llm-gateway`).

This is the route the **in-sandbox agent SDK** targets when it speaks
the OpenAI dialect. The intended consumer is the agent process
running inside a per-trial Docker sandbox (Phase 3, not yet wired):
the sandbox SDK is configured with `OPENAI_BASE_URL=...` pointing at
`loom-sandbox-gateway.local`, which proxies to this route. That code
path is built out alongside `loom-llm-gateway-sandbox` + the per-trial
Docker bridges.

Today's in-process agent runtimes (`HttpLLMGatewayClient`) still hit
the legacy `/v1/chat/completions` — they don't go through this
facade because they live in the worker process, not the sandbox.
Phase 3 closes that loop end-to-end.

Difference from `/v1/chat/completions`:
- `/v1/chat/completions`: routes by `model="provider/name"`, uses
  gateway-resident provider keys, wraps the response with a `loom`
  attribution block.
- `/openai/v1/chat/completions` (this route): resolves a
  `provider_connection_id` to the operator's stored credential,
  decrypts it in-process, forwards verbatim, and returns the raw
  upstream OpenAI body so the unmodified SDK can parse it. The
  agent never sees the upstream key.

Wire shape (MVP):
- `Authorization: Bearer loom_step_<jwt>` — step-scoped JWT minted
  by control-plane (existing `verify_bearer_token` path).
- `x-loom-provider-connection-id: <uuid>` — required header.
  (Eventually folded into the JWT scope alongside trial_id/step_id;
  see cluster-deploy.md §Authentication. For now an explicit header
  keeps this route focused on the facade plumbing.)
- Body: OpenAI-shape chat completion request, passed through.

Out of scope for this route (explicit follow-ups):
- Streaming (`stream=true` returns 501 — see comment below).
- `/anthropic/v1/messages` + `/google/v1beta/...` facade variants
  (each provider's dialect needs its own request/response shape).
- Egress proxy routing (the upstream POST goes direct from the
  gateway pod today; egress-proxy IP allowlisting is Phase 3).
"""

from __future__ import annotations

from typing import Any

import httpx
from fastapi import APIRouter, Header, HTTPException, Request

from loom.security.secret_store import LocalEncryptedSecretStore
from loom_llm_gateway.dialect import TokenUsage
from loom_llm_gateway.llm_calls import record_call
from loom_llm_gateway.retry import send_with_retry
from loom_llm_gateway.routes._facade_common import (
    compute_facade_cost_usd,
    redact_api_key,
    resolve_facade_connection,
    resolve_provider_connection_id,
    verify_facade_auth,
)

router = APIRouter()

# Provider types this facade can serve. Anthropic-native + Google
# Gemini speak different dialects and need their own facade routes;
# routing them through this one would fail at the upstream with a
# 400 once it tries to parse a chat-completion request.
#
# `custom` is treated as openai-compatible because that's the most
# common shape behind operator-supplied base URLs (vLLM, Together,
# Mistral, Fireworks all speak it). Operators who need a non-OpenAI
# `custom` provider use the dialect-specific facade once it lands.
_OPENAI_SHAPED_TYPES = frozenset({"openai-compatible", "custom"})

# String stored in `llm_calls.dialect` for facade-routed traffic.
# Snake-case to match the existing convention (openai_chat,
# openai_responses, anthropic, gemini) so trial.py's projection
# map (_provider_by_dialect) maps facade rows to provider="openai"
# rather than the "unknown" fallback. The `rate_card_hash` field
# carries the facade marker for downstream billing/audit
# (see `record_call(..., rate_card_hash=f"facade:{pricing_source}")`).
_FACADE_DIALECT = "openai_facade"


@router.post("/openai/v1/chat/completions")
async def openai_chat_facade(
    request: Request,
    payload: dict[str, Any],
    authorization: str | None = Header(default=None),
    x_loom_provider_connection_id: str | None = Header(
        default=None, alias="x-loom-provider-connection-id",
    ),
) -> dict[str, Any]:
    settings = request.app.state.settings
    signing_key = settings.step_jwt_signing_key.get_secret_value()

    # Step-scoped JWT auth — same path as /v1/messages so the trial_id
    # + step_id + team_id are pulled from the JWT (not the body).
    async with request.app.state.session_factory() as session:
        ctx = await verify_facade_auth(
            session, authorization, signing_key,
        )
    assert ctx.team_id is not None  # narrowed by verify_facade_auth
    assert ctx.trial_id is not None
    assert ctx.step_id is not None
    connection_id = resolve_provider_connection_id(
        ctx, x_loom_provider_connection_id,
    )

    # Streaming is rejected (501) for the same reason /v1/messages
    # rejects it: cost attribution requires the final usage block,
    # which SSE breaks. v1.5 ships SSE passthrough — when it does,
    # this route should refuse stream=true for connections without
    # operator-supplied pricing rather than blanket-reject.
    if payload.get("stream"):
        raise HTTPException(
            status_code=501,
            detail=(
                "stream=true not yet supported on the facade "
                "(cost attribution needs the final usage block). "
                "Set stream=false."
            ),
        )

    if not isinstance(payload.get("model"), str) or not payload["model"]:
        raise HTTPException(status_code=400, detail="`model` is required")

    # Resolve + decrypt. Both happen inside one session so the
    # SecretStore.get sees the same transaction the lookup did.
    async with request.app.state.session_factory() as session:
        row = await resolve_facade_connection(
            session, connection_id, ctx.team_id,
            supported_types=_OPENAI_SHAPED_TYPES,
            dialect_label="/openai/v1/chat/completions",
        )
        store = LocalEncryptedSecretStore(session)
        api_key = await store.get(row.encrypted_api_key_ref)

    upstream_url = f"{row.base_url.rstrip('/')}/chat/completions"
    upstream_headers = {
        "Authorization": f"Bearer {api_key}",
        "content-type": "application/json",
    }
    # #190 PR-C2: per-connection egress proxy when LOOM_GW_EGRESS_PROXY_URL set.
    upstream: httpx.AsyncClient = await (
        request.app.state.egress_client_pool.get(connection_id)
    )
    try:
        outcome = await send_with_retry(
            lambda: upstream.post(
                upstream_url,
                json=payload,
                headers=upstream_headers,
                timeout=settings.upstream_timeout_sec,
                follow_redirects=False,
            ),
            settings=settings, dialect="facade_openai",
        )
        upstream_response = outcome.response
    except httpx.TimeoutException as e:
        raise HTTPException(
            status_code=504,
            detail=f"upstream timeout against {upstream_url}: {e}",
        ) from e
    except httpx.RequestError as e:
        raise HTTPException(
            status_code=502,
            detail=(
                f"upstream request error against {upstream_url}: "
                f"{type(e).__name__}: {e}"
            ),
        ) from e

    if upstream_response.status_code >= 400:
        excerpt = redact_api_key(upstream_response.text, api_key)
        raise HTTPException(
            status_code=upstream_response.status_code,
            detail=(
                f"upstream returned {upstream_response.status_code}: "
                f"{excerpt}"
            ),
        )

    try:
        body: dict[str, Any] = upstream_response.json()
    except ValueError as e:
        raise HTTPException(
            status_code=502,
            detail=f"upstream returned non-JSON body: {e}",
        ) from e

    # Token usage — OpenAI shape is `usage.{prompt,completion,total}_tokens`.
    # A non-2xx already 4xx'd above, so this is the happy path. Missing
    # `usage` (some operator endpoints omit it) → 0/0 with a 0 cost row
    # so attribution still exists for debug.
    usage_d = body.get("usage") if isinstance(body, dict) else None
    if isinstance(usage_d, dict):
        try:
            input_tokens = int(usage_d.get("prompt_tokens", 0) or 0)
            output_tokens = int(usage_d.get("completion_tokens", 0) or 0)
        except (TypeError, ValueError):
            input_tokens = output_tokens = 0
    else:
        input_tokens = output_tokens = 0

    usage = TokenUsage(
        input_tokens=input_tokens,
        output_tokens=output_tokens,
        provider_extras={},
    )
    cost_usd, rate_card_hash = await compute_facade_cost_usd(
        row,
        payload["model"],
        usage,
        rate_card_cache=request.app.state.rate_card_cache,
    )

    # Audit. We use a fresh session so the upstream call's latency
    # didn't keep the connection-lookup session open longer than
    # needed. `rate_card_hash` carries the pricing_source string —
    # the field is non-null so we store a useful marker.
    async with request.app.state.session_factory() as audit_session:
        await record_call(
            audit_session,
            team_id=ctx.team_id,
            trial_id=ctx.trial_id,
            step_id=ctx.step_id,
            dialect=_FACADE_DIALECT,
            model=payload["model"],
            usage=usage,
            cost_usd=cost_usd,
            rate_card_hash=rate_card_hash,
            provider=row.provider_type,
            attempt=outcome.attempt,
        )

    return body
