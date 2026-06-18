"""Shared facade helpers used by `/openai/v1/...`, `/anthropic/v1/...`,
and `/google/v1beta/...` routes.

Each dialect-specific route is a thin module that:
1. Calls `verify_facade_auth` to validate the step-JWT.
2. Calls `parse_connection_id_header` to extract + UUID-parse the
   `x-loom-provider-connection-id` header.
3. Calls `resolve_facade_connection` to look up the connection and
   restrict by team + supported type.
4. Decrypts the api_key via SecretStore.
5. Builds the dialect-specific upstream request (URL + headers).
6. Forwards and audits via `record_call`.

The helpers below cover steps 1-3 + the common error paths so each
dialect route stays focused on the upstream contract.
"""

from __future__ import annotations

import re
from typing import Any
from uuid import UUID

from fastapi import HTTPException
from sqlalchemy import select

from loom.auth import AuthContext, verify_bearer_token
from loom.db.schema import ProviderConnection
from loom.models.types import ModelSpec
from loom_llm_gateway.dialect import TokenUsage
from loom_llm_gateway.errors import RateCardNotFoundError
from loom_llm_gateway.rate_card import (
    RateCardCache,
    compute_cost_usd,
    hash_table,
    lookup_entry,
)

_TYPE_TO_DEFAULT_RATE_CARD_PROVIDER = {
    "anthropic": "anthropic",
    "google": "google",
    "openai-compatible": "openai",
}

_BEARER_VALUE_RE = re.compile(
    r"(?i)(\bBearer\s+)([A-Za-z0-9._~+/=-]{4,})",
)


async def verify_facade_auth(
    session: Any, authorization: str | None, signing_key: str,
) -> AuthContext:
    """Step-scoped JWT auth path shared by all facade routes.

    Returns the validated AuthContext with non-None trial_id, step_id,
    and team_id. Raises 401 / 403 directly on auth failure.
    """
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
    return ctx


def parse_connection_id_header(raw: str | None) -> UUID:
    """Validate the `x-loom-provider-connection-id` header. Raises 400
    on missing or non-UUID values.

    Kept for callers that still want pure-header resolution. New code
    should prefer `resolve_provider_connection_id` which honors the
    JWT-scoped value when present (issue #72).
    """
    if not raw:
        raise HTTPException(
            status_code=400,
            detail="x-loom-provider-connection-id header is required",
        )
    try:
        return UUID(raw)
    except ValueError as exc:
        raise HTTPException(
            status_code=400,
            detail=(
                "x-loom-provider-connection-id is not a valid UUID: "
                f"{exc}"
            ),
        ) from exc


def resolve_provider_connection_id(
    ctx: AuthContext, header_value: str | None,
) -> UUID:
    """Resolve the connection_id from EITHER the step-JWT scope
    (``ctx.provider_connection_id``) OR the
    ``x-loom-provider-connection-id`` header.

    Precedence rules (issue #72):
    - **Both set + match** ⇒ use the value (canonical case during
      transition; sandbox SDK can send both safely).
    - **Both set + mismatch** ⇒ 400. The JWT scope is authoritative;
      the response message says so explicitly so operators know to
      drop the header or align it.
    - **JWT only** ⇒ use the JWT value (forward-compatible: post-
      transition path).
    - **Header only** ⇒ use the header value (legacy callers; will
      keep working until the header is sunset in a follow-up).
    - **Neither** ⇒ 400. The facade has nothing to route against.

    The JWT scope binds at mint time (CP looks it up from the trial
    row); the header is operator-supplied. When they agree, the
    request is unambiguous regardless of trust level.
    """
    header_uuid: UUID | None = None
    if header_value:
        try:
            header_uuid = UUID(header_value)
        except ValueError as exc:
            raise HTTPException(
                status_code=400,
                detail=(
                    "x-loom-provider-connection-id is not a valid "
                    f"UUID: {exc}"
                ),
            ) from exc

    jwt_uuid = ctx.provider_connection_id

    if jwt_uuid is not None and header_uuid is not None:
        if jwt_uuid != header_uuid:
            raise HTTPException(
                status_code=400,
                detail=(
                    "provider_connection_id mismatch: JWT scope says "
                    f"{jwt_uuid}, header says {header_uuid}. The JWT "
                    "scope is authoritative — drop the header or align "
                    "it to the JWT value."
                ),
            )
        return jwt_uuid
    if jwt_uuid is not None:
        return jwt_uuid
    if header_uuid is not None:
        return header_uuid
    raise HTTPException(
        status_code=400,
        detail=(
            "provider_connection_id is required: pass via the "
            "x-loom-provider-connection-id header or mint a step-JWT "
            "with provider_connection_id in scope (issue #72)."
        ),
    )


async def resolve_facade_connection(
    session: Any,
    connection_id: UUID,
    team_id: UUID,
    *,
    supported_types: frozenset[str],
    dialect_label: str,
) -> ProviderConnection:
    """Lookup, team-scope, soft-delete, and dialect-type checks all in
    one helper. 404 (not 403) on cross-team to match loom_service's
    existence-hiding convention; the 400 on type mismatch surfaces a
    diagnostic hint pointing the operator at the matched facade.

    Cross-team 404 fires BEFORE the dialect check so cross-team
    callers can't probe which provider_type a connection has.
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
    if row.provider_type not in supported_types:
        raise HTTPException(
            status_code=400,
            detail=(
                f"provider_connection.type={row.provider_type!r} is "
                f"not served by {dialect_label}; use the "
                f"dialect-matched facade for this provider type."
            ),
        )
    return row


async def compute_facade_cost_usd(
    row: ProviderConnection,
    model_name: str,
    usage: TokenUsage,
    *,
    rate_card_cache: RateCardCache | None,
) -> tuple[float, str]:
    """Compute facade call cost and the audit hash/marker.

    `operator-supplied` and `tokens-only` keep the existing marker
    contract. `rate-card` looks up the connection's explicit
    `rate_card_provider`, falling back to safe provider-type defaults
    where possible. Missing provider/model rows degrade to cost=0 with
    an explainable marker for downstream billing audits.
    """
    if row.pricing_source == "operator-supplied":
        data = row.pricing_data or {}
        try:
            in_per_1m = float(data.get("input_usd_per_1m", 0) or 0)
            out_per_1m = float(data.get("output_usd_per_1m", 0) or 0)
        except (TypeError, ValueError):
            return 0.0, "facade:operator-supplied"
        cost = (usage.input_tokens / 1_000_000.0) * in_per_1m
        cost += (usage.output_tokens / 1_000_000.0) * out_per_1m
        return cost, "facade:operator-supplied"

    if row.pricing_source != "rate-card":
        return 0.0, f"facade:{row.pricing_source}"

    lookup_provider = (
        row.rate_card_provider
        or _TYPE_TO_DEFAULT_RATE_CARD_PROVIDER.get(row.provider_type)
    )
    if lookup_provider is None or rate_card_cache is None:
        return 0.0, "facade:rate-card:missing"

    try:
        table = await rate_card_cache.get()
        entry = lookup_entry(
            table, ModelSpec(provider=lookup_provider, name=model_name),
        )
    except RateCardNotFoundError:
        return 0.0, "facade:rate-card:missing"

    cost = compute_cost_usd(
        entry,
        input_tokens=usage.input_tokens,
        output_tokens=usage.output_tokens,
        cached_input_tokens=usage.cached_input_tokens,
        cache_write_tokens=usage.cache_write_tokens,
    )
    return cost, hash_table(table)


def redact_api_key(text: str, api_key: str, *, limit: int = 500) -> str:
    """Scrub the decrypted api_key from an excerpt before it lands in
    a user-visible error string. Same 4-char minimum as
    `provider_connections_service._redact_secret` to avoid
    over-redaction on degenerate inputs (1-3 char secrets would
    obliterate common substrings).
    """
    excerpt = (text or "")[:limit]
    if api_key and len(api_key) >= 4:
        excerpt = excerpt.replace(api_key, "[REDACTED]")
    excerpt = _BEARER_VALUE_RE.sub(r"\1[REDACTED]", excerpt)
    return excerpt
