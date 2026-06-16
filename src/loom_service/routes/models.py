"""Model catalog — GET /api/v1/models.

Returns legacy rate-card model tuples plus BYO provider-connection
models visible to the caller's team. Default view is launch-safe:
agent-capable/recommended models only. `view=raw` keeps noisy provider
entries for debugging with classifier reasons.
"""

from __future__ import annotations

from typing import Any, Literal

from fastapi import APIRouter, Query
from sqlalchemy import select

from loom.db.schema import ProviderConnection, ProviderModelCache, RateCard
from loom_service.auth_guards import is_admin
from loom_service.dependencies import SessionAndCtx
from loom_service.provider_model_classifier import classify_model_id

router = APIRouter()


def _provider_namespace(row: ProviderConnection) -> str:
    if row.rate_card_provider:
        return row.rate_card_provider
    if row.provider_type == "openai-compatible":
        return "openai"
    return row.provider_type


def _byo_model_item(
    conn: ProviderConnection,
    cache: ProviderModelCache,
) -> dict[str, Any]:
    capabilities = dict(cache.capabilities or {})
    source = capabilities.get("source")
    source_str = source if isinstance(source, str) else "discovered"
    classification = classify_model_id(
        cache.model_id, source=source_str, family=cache.family,
    )
    if not cache.visible:
        visibility = "hidden"
        recommended = False
        hidden_reason = cache.hidden_reason
    else:
        visibility = classification.visibility
        recommended = classification.recommended
        hidden_reason = cache.hidden_reason or classification.reason
    return {
        "provider": _provider_namespace(conn),
        "name": cache.model_id,
        "provider_connection_id": str(conn.id),
        "provider_connection_name": conn.display_name,
        "provider_connection_type": conn.provider_type,
        "source": source_str,
        "family": cache.family,
        "context_length": cache.context_length,
        "capabilities": capabilities,
        "visible": cache.visible,
        "upstream_present": cache.upstream_present,
        "last_seen_at": cache.last_seen_at.isoformat(),
        "agent_capable": classification.agent_capable,
        "recommended": recommended,
        "visibility": visibility,
        "hidden_reason": hidden_reason,
    }


@router.get("/models")
async def list_models(
    sc: SessionAndCtx,
    view: Literal["default", "raw"] = Query(default="default"),
) -> dict[str, Any]:
    """Return launch model metadata for the authenticated caller."""
    s, ctx = sc
    rows = (await s.execute(select(RateCard.table))).scalars().all()
    seen: set[tuple[str, str]] = set()
    items: list[dict[str, Any]] = []
    for card in rows:
        entries = (card or {}).get("entries") or []
        for e in entries:
            provider = e.get("provider")
            name = e.get("model")
            if not isinstance(provider, str) or not isinstance(name, str):
                continue
            if (provider, name) in seen:
                continue
            seen.add((provider, name))
            items.append({
                "provider": provider,
                "name": name,
                "source": "rate-card",
                "agent_capable": True,
                "recommended": True,
                "visibility": "default",
                "hidden_reason": None,
            })

    stmt = (
        select(ProviderConnection, ProviderModelCache)
        .join(
            ProviderModelCache,
            ProviderModelCache.provider_connection_id == ProviderConnection.id,
        )
        .where(ProviderConnection.deleted_at.is_(None))
    )
    if not is_admin(ctx):
        stmt = stmt.where(ProviderConnection.team_id == ctx.team_id)
    byo_rows = (await s.execute(stmt)).all()
    for conn, cache in byo_rows:
        item = _byo_model_item(conn, cache)
        if view == "default" and not item["recommended"]:
            continue
        items.append(item)

    # Stable ordering: provider asc, name asc.
    items.sort(key=lambda x: (
        x.get("provider_connection_name") or "",
        x["provider"],
        x["name"],
    ))
    return {"items": items}
