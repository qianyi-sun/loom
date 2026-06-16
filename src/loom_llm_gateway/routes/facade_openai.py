"""POST /openai/v1/chat/completions — provider-connection facade
(cluster-deploy.md §Component map: `loom-llm-gateway`).

This is the route the in-sandbox agent SDK targets when it speaks the
OpenAI dialect. Unlike `/v1/chat/completions` (which routes by
`model="provider/name"` and uses gateway-resident provider keys),
this facade route resolves a `provider_connection_id` to the
operator's stored credential, decrypts it, and forwards verbatim to
the connection's `base_url`. The agent never sees the upstream key.

Wire shape (MVP):
- `Authorization: Bearer loom_step_<jwt>` — step-scoped JWT minted
  by control-plane (existing `verify_bearer_token` path).
- `x-loom-provider-connection-id: <uuid>` — required header.
  (Eventually folded into the JWT scope alongside trial_id/step_id;
  see cluster-deploy.md §Authentication. For now an explicit header
  keeps this PR focused on the facade plumbing.)
- Body: OpenAI-shape chat completion request, passed through.

Out of scope for this PR (explicit follow-ups):
- Streaming (`stream=true` returns 501 — see comment below).
- `/anthropic/v1/messages` + `/google/v1beta/...` facade variants
  (each provider's dialect needs its own request/response shape;
  this PR ships the OpenAI variant only).
- Egress proxy routing (the upstream POST goes direct from the
  gateway pod today; egress-proxy IP allowlisting is Phase 2.5+).
"""

from __future__ import annotations

from typing import Any
from uuid import UUID

import httpx
from fastapi import APIRouter, Header, HTTPException, Request
from sqlalchemy import select

from loom.db.schema import ProviderConnection
from loom.security.secret_store import LocalEncryptedSecretStore
from loom_llm_gateway.auth import verify_bearer_token
from loom_llm_gateway.dialect import TokenUsage
from loom_llm_gateway.llm_calls import record_call

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
# Lets downstream consumers (finalize projection, billing rollup)
# distinguish facade calls from the legacy model="provider/name"
# path without parsing the model string.
_FACADE_DIALECT = "openai-facade"


async def _resolve_connection(
    session: Any, connection_id: UUID, team_id: UUID,
) -> ProviderConnection:
    """Lookup by id, restrict to caller's team, exclude soft-deleted.
    404 (not 403) on cross-team to match loom_service's convention —
    existence shouldn't leak across teams.
    """
    row: ProviderConnection | None = (await session.execute(
        select(ProviderConnection).where(
            ProviderConnection.id == connection_id,
            ProviderConnection.deleted_at.is_(None),
        ),
    )).scalar_one_or_none()
    if row is None or row.team_id != team_id:
        raise HTTPException(
            status_code=404, detail="provider_connection not found",
        )
    return row


def _compute_cost_usd(
    row: ProviderConnection, input_tokens: int, output_tokens: int,
) -> float:
    """Cost compute for facade calls.

    - `operator-supplied`: per-1M token rates from `pricing_data`.
    - `tokens-only`: 0 (no pricing data is available — operators who
      want USD numbers set `operator-supplied`).
    - `rate-card`: 0 (the rate-card lookup path is bound to the legacy
      `provider/name` routing; wiring it into the facade is a separate
      concern. Operators today fall back to `operator-supplied` for
      facade-routed connections needing cost attribution).
    """
    if row.pricing_source != "operator-supplied":
        return 0.0
    data = row.pricing_data or {}
    try:
        in_per_1m = float(data.get("input_usd_per_1m", 0) or 0)
        out_per_1m = float(data.get("output_usd_per_1m", 0) or 0)
    except (TypeError, ValueError):
        return 0.0
    cost = (input_tokens / 1_000_000.0) * in_per_1m
    cost += (output_tokens / 1_000_000.0) * out_per_1m
    return cost


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

    if not x_loom_provider_connection_id:
        raise HTTPException(
            status_code=400,
            detail="x-loom-provider-connection-id header is required",
        )
    try:
        connection_id = UUID(x_loom_provider_connection_id)
    except ValueError as exc:
        raise HTTPException(
            status_code=400,
            detail=(
                "x-loom-provider-connection-id is not a valid UUID: "
                f"{exc}"
            ),
        ) from exc

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
        row = await _resolve_connection(session, connection_id, ctx.team_id)
        if row.provider_type not in _OPENAI_SHAPED_TYPES:
            raise HTTPException(
                status_code=400,
                detail=(
                    f"provider_connection.type={row.provider_type!r} is "
                    f"not served by /openai/v1/chat/completions; use the "
                    f"dialect-matched facade for this provider type."
                ),
            )
        store = LocalEncryptedSecretStore(session)
        api_key = await store.get(row.encrypted_api_key_ref)

    upstream_url = f"{row.base_url.rstrip('/')}/chat/completions"
    upstream_headers = {
        "Authorization": f"Bearer {api_key}",
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
        raise HTTPException(
            status_code=upstream_response.status_code,
            detail=(
                f"upstream returned {upstream_response.status_code}: "
                f"{upstream_response.text[:500]}"
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

    cost_usd = _compute_cost_usd(row, input_tokens, output_tokens)
    usage = TokenUsage(
        input_tokens=input_tokens,
        output_tokens=output_tokens,
        provider_extras={},
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
            rate_card_hash=f"facade:{row.pricing_source}",
        )

    return body
