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

from loom.auth import verify_bearer_token
from loom_service.auth_guards import require_human_or_admin, require_scope
from loom_service.forwarders import forward, propagate

router = APIRouter()


async def _check_read(
    request: Request, authorization: str | None,
) -> None:
    """Any signed-in team/human/admin token can read rate cards.

    Costs are derived at query time from the rate card the call was
    priced against; teams need to see those rows to interpret their
    own bills and reproduce historical cost math. Mutations stay
    admin-only via `_check_admin_mutation` below.
    """
    async with request.app.state.session_factory() as s:
        ctx = await verify_bearer_token(s, authorization)
        require_human_or_admin(ctx)


async def _check_admin_mutation(
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
    await _check_read(request, authorization)
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
    await _check_read(request, authorization)
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
    await _check_admin_mutation(request, authorization)
    resp = await forward(
        request.app.state.gateway_client,
        method="POST", path="/admin/rate-cards",
        authorization=authorization, json_body=payload,
    )
    return propagate(resp)
