"""POST /anthropic/v1/messages — provider-connection facade
(cluster-deploy.md §Component map: `loom-llm-gateway`).

Sibling of `/openai/v1/chat/completions` (`facade_openai.py`) for
Anthropic-typed connections. The in-sandbox Anthropic SDK is
configured with `ANTHROPIC_BASE_URL=...` pointing at
`loom-sandbox-gateway.local`, which proxies to this route. Today's
in-process runtimes still hit the legacy `/v1/messages` route which
uses gateway-resident keys; that path is unchanged.

Difference from `/v1/messages`:
- `/v1/messages`: uses gateway-resident `LOOM_GW_ANTHROPIC_API_KEY`,
  rate-card lookup via `(provider, model)` keys, returns the body
  with cost recorded.
- `/anthropic/v1/messages` (this route): resolves a
  `provider_connection_id` to the operator's stored credential,
  decrypts it in-process, forwards verbatim to the connection's
  `base_url + /v1/messages`, and returns the raw upstream Anthropic
  body so the unmodified SDK can parse it. The agent never sees
  the upstream key.

Wire shape (MVP):
- `Authorization: Bearer loom_step_<jwt>` — step-scoped JWT minted
  by control-plane.
- `x-loom-provider-connection-id: <uuid>` — required header.
  (Eventually folded into the JWT scope; see #72.)
- Body: Anthropic Messages API shape, passed through.

Out of scope for this route:
- Streaming (`stream=true` returns 501 — Anthropic streams via SSE
  and the final `usage` block is needed for cost attribution).
- Egress proxy routing (Phase 3).
"""

from __future__ import annotations

from typing import Any

import httpx
from fastapi import APIRouter, Header, HTTPException, Request

from loom.security.secret_store import LocalEncryptedSecretStore
from loom_llm_gateway.dialect import DIALECTS
from loom_llm_gateway.llm_calls import record_call
from loom_llm_gateway.routes._facade_common import (
    compute_facade_cost_usd,
    redact_api_key,
    resolve_facade_connection,
    resolve_provider_connection_id,
    verify_facade_auth,
)

router = APIRouter()

# Only `anthropic` connections route through this facade. A `custom`
# connection that happens to speak Anthropic is a misconfiguration:
# the operator should re-create as `provider_type='anthropic'` so the
# rest of the system (probe, models cache, pricing defaults) treats
# it correctly.
_ANTHROPIC_TYPES = frozenset({"anthropic"})

# llm_calls.dialect — snake_case for consistency with siblings; the
# projection map in `loom/trial/trial.py` MUST contain a matching
# entry (regression caught by PR #65). `rate_card_hash` carries the
# facade marker for downstream billing/audit.
_FACADE_DIALECT = "anthropic_facade"

# Anthropic API version. Matches the existing `/v1/messages` route's
# header so connections to non-Anthropic upstreams speaking the
# Anthropic shape behave identically.
_ANTHROPIC_VERSION = "2023-06-01"


@router.post("/anthropic/v1/messages")
async def anthropic_messages_facade(
    request: Request,
    payload: dict[str, Any],
    authorization: str | None = Header(default=None),
    x_loom_provider_connection_id: str | None = Header(
        default=None, alias="x-loom-provider-connection-id",
    ),
) -> dict[str, Any]:
    settings = request.app.state.settings
    signing_key = settings.step_jwt_signing_key.get_secret_value()

    async with request.app.state.session_factory() as session:
        ctx = await verify_facade_auth(
            session, authorization, signing_key,
        )
    assert ctx.team_id is not None
    assert ctx.trial_id is not None
    assert ctx.step_id is not None
    connection_id = resolve_provider_connection_id(
        ctx, x_loom_provider_connection_id,
    )

    # Same rationale as openai facade: SSE breaks cost attribution
    # because the final `usage` block needs to be visible at the route
    # level. v1.5 ships SSE passthrough.
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

    async with request.app.state.session_factory() as session:
        row = await resolve_facade_connection(
            session, connection_id, ctx.team_id,
            supported_types=_ANTHROPIC_TYPES,
            dialect_label="/anthropic/v1/messages",
        )
        store = LocalEncryptedSecretStore(session)
        api_key = await store.get(row.encrypted_api_key_ref)

    upstream_url = f"{row.base_url.rstrip('/')}/v1/messages"
    upstream_headers = {
        # Anthropic uses x-api-key (not Authorization). Matches the
        # existing /v1/messages route's header convention.
        "x-api-key": api_key,
        "anthropic-version": _ANTHROPIC_VERSION,
        "content-type": "application/json",
    }
    upstream: httpx.AsyncClient = request.app.state.upstream_client
    try:
        upstream_response = await upstream.post(
            upstream_url,
            json=payload,
            headers=upstream_headers,
            timeout=settings.upstream_timeout_sec,
            follow_redirects=False,
        )
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

    # Token usage via the existing Anthropic dialect adapter; carries
    # the cache_creation_input_tokens + cache_read_input_tokens extras
    # forward into llm_calls.provider_extras.
    usage = DIALECTS["anthropic"].extract_tokens(body)
    # Anthropic 200s always include a usage block; missing → upstream
    # contract violation. Same 502 rationale as the legacy /v1/messages
    # route so the worker's cost rollup never gets a silently-zero
    # row from a malformed upstream.
    if usage.input_tokens == 0 and usage.output_tokens == 0:
        raise HTTPException(
            status_code=502,
            detail=(
                "upstream returned 200 with missing/zero usage block; "
                "expected non-zero input_tokens+output_tokens"
            ),
        )

    cost_usd, rate_card_hash = await compute_facade_cost_usd(
        row,
        payload["model"],
        usage,
        rate_card_cache=request.app.state.rate_card_cache,
    )

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
        )

    return body
