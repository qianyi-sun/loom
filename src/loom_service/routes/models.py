"""Model catalog — GET /api/v1/models.

Returns the distinct (provider, name) pairs the rate_cards table
knows about. The LLM Gateway routes by these tuples, so they
double as the catalog of "models the Gateway is configured for."
The SPA uses the list to populate a model dropdown in
SubmitTrialModal + NewBatch.
"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter
from sqlalchemy import select

from loom.db.schema import RateCard
from loom_service.dependencies import SessionAndCtx

router = APIRouter()


@router.get("/models")
async def list_models(sc: SessionAndCtx) -> dict[str, Any]:
    """De-duplicates across all rate cards. A model that appears in
    multiple cards (older + newer pricing) collapses to one entry."""
    s, _ctx = sc
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
            items.append({"provider": provider, "name": name})
    # Stable ordering: provider asc, name asc.
    items.sort(key=lambda x: (x["provider"], x["name"]))
    return {"items": items}
