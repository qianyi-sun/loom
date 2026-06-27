"""Rate-card forwarders (spec §5.6).

Thin proxy to the Gateway's `/admin/rate-cards` surface (shipped in
Plan 4). Reads are open to any authenticated user — teams need to see
what their calls cost. Mutations (POST / PATCH / DELETE) still require
the `admin:rate_cards` scope, since rate cards are global and changing
one affects every team's billing.
"""

from __future__ import annotations

from typing import Annotated, Any

from fastapi import APIRouter, Header, Request
from fastapi.responses import JSONResponse

from loom_service.auth_guards import require_scope
from loom_service.dependencies import SessionAndCtx
from loom_service.forwarders import forward, propagate

router = APIRouter()


@router.get("/rate-cards")
async def list_rate_cards(
    request: Request,
    sc: SessionAndCtx,
    authorization: Annotated[str | None, Header()] = None,
) -> JSONResponse:
    # SessionAndCtx enforces human-or-admin; reads stay open to any
    # authenticated user.
    _ = sc
    resp = await forward(
        request.app.state.gateway_client,
        method="GET",
        path="/admin/rate-cards",
        authorization=authorization,
    )
    return propagate(resp)


@router.get("/rate-cards/{rate_card_id}")
async def get_rate_card(
    request: Request,
    sc: SessionAndCtx,
    rate_card_id: str,
    authorization: Annotated[str | None, Header()] = None,
) -> JSONResponse:
    _ = sc
    resp = await forward(
        request.app.state.gateway_client,
        method="GET",
        path=f"/admin/rate-cards/{rate_card_id}",
        authorization=authorization,
    )
    return propagate(resp)


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
