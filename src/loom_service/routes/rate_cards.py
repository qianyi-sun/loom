"""Rate-card forwarders (spec §5.6).

Thin proxy to the Gateway's `/admin/rate-cards` surface (shipped in
Plan 4). Every route requires the `admin:rate_cards` scope that Plan
17's migration 0005 granted to existing `admin:tokens` holders.
"""

from __future__ import annotations

from typing import Annotated, Any

from fastapi import APIRouter, Header, Request
from fastapi.responses import JSONResponse

from loom.auth import verify_bearer_token
from loom_service.auth_guards import require_human_or_admin, require_scope
from loom_service.forwarders import forward, propagate

router = APIRouter()


async def _check_admin_rate_cards(
    request: Request, authorization: str | None,
) -> None:
    async with request.app.state.session_factory() as s:
        ctx = await verify_bearer_token(s, authorization)
        ctx = require_human_or_admin(ctx)
        require_scope(ctx, "admin:rate_cards")


@router.get("/rate-cards")
async def list_rate_cards(
    request: Request,
    authorization: Annotated[str | None, Header()] = None,
) -> JSONResponse:
    await _check_admin_rate_cards(request, authorization)
    resp = await forward(
        request.app.state.gateway_client,
        method="GET", path="/admin/rate-cards",
        authorization=authorization,
    )
    return propagate(resp)


@router.get("/rate-cards/{rate_card_id}")
async def get_rate_card(
    request: Request,
    rate_card_id: str,
    authorization: Annotated[str | None, Header()] = None,
) -> JSONResponse:
    await _check_admin_rate_cards(request, authorization)
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
    payload: dict[str, Any],
    authorization: Annotated[str | None, Header()] = None,
) -> JSONResponse:
    await _check_admin_rate_cards(request, authorization)
    resp = await forward(
        request.app.state.gateway_client,
        method="POST", path="/admin/rate-cards",
        authorization=authorization, json_body=payload,
    )
    return propagate(resp)
