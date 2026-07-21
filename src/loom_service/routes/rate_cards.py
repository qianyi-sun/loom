"""Rate-card APIs (spec §5.6).

Reads use the shared database authority after Service authentication so
browser sessions do not need a second Gateway bearer credential. Mutations
remain thin proxies to the Gateway's `/admin/rate-cards` surface and require
the `admin:rate_cards` scope, since rate cards are global and changing one
affects every team's billing.
"""

from __future__ import annotations

from typing import Annotated, Any

from fastapi import APIRouter, Header, HTTPException, Request
from fastapi.responses import JSONResponse
from sqlalchemy import select

from loom.db.schema import RateCard
from loom_service.auth_guards import require_scope
from loom_service.dependencies import SessionAndCtx
from loom_service.forwarders import forward, propagate

router = APIRouter()


def _serialize_rate_card(row: RateCard) -> dict[str, Any]:
    return {
        "id": row.id,
        "captured_at": row.captured_at,
        "table": row.table,
    }


@router.get("/rate-cards")
async def list_rate_cards(
    sc: SessionAndCtx,
) -> dict[str, list[dict[str, Any]]]:
    # SessionAndCtx enforces human-or-admin; reads stay open to any
    # authenticated user.
    session, _ctx = sc
    rows = (
        await session.scalars(
            select(RateCard).order_by(
                RateCard.captured_at.desc(),
                RateCard.id.asc(),
            )
        )
    ).all()
    return {"items": [_serialize_rate_card(row) for row in rows]}


@router.get("/rate-cards/{rate_card_id}")
async def get_rate_card(
    sc: SessionAndCtx,
    rate_card_id: str,
) -> dict[str, Any]:
    session, _ctx = sc
    row = await session.get(RateCard, rate_card_id)
    if row is None:
        raise HTTPException(status_code=404, detail="rate card not found")
    return _serialize_rate_card(row)


@router.post("/rate-cards", status_code=201)
async def create_rate_card(
    request: Request,
    sc: SessionAndCtx,
    payload: dict[str, Any],
    authorization: Annotated[str | None, Header()] = None,
) -> JSONResponse:
    _session, ctx = sc
    require_scope(ctx, "admin:rate_cards")
    resp = await forward(
        request.app.state.gateway_client,
        method="POST",
        path="/admin/rate-cards",
        authorization=authorization,
        json_body=payload,
    )
    return propagate(resp)


@router.post("/rate-cards/sync/yibuapi", status_code=201)
async def sync_yibuapi_rate_card(
    request: Request,
    sc: SessionAndCtx,
    payload: dict[str, Any] | None = None,
    authorization: Annotated[str | None, Header()] = None,
) -> JSONResponse:
    _session, ctx = sc
    require_scope(ctx, "admin:rate_cards")
    resp = await forward(
        request.app.state.gateway_client,
        method="POST",
        path="/admin/rate-cards/sync/yibuapi",
        authorization=authorization,
        json_body=payload or {},
    )
    return propagate(resp)
