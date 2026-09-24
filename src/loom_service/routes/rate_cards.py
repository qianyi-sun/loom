"""Rate-card APIs (spec §5.6).

Reads use the shared database authority after Service authentication so
browser sessions do not need a second Gateway bearer credential. Mutations
remain thin proxies to the Gateway's `/admin/rate-cards` surface and require
the `admin:rate_cards` scope, since rate cards are global and changing one
affects every team's billing.
"""

from __future__ import annotations

# New catalogs are isolated from the legacy globally-readable rate_cards table.
from datetime import UTC, datetime
from typing import Annotated, Any, Literal
from uuid import UUID, uuid4

from fastapi import APIRouter, Header, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import or_, select

from loom.auth import is_admin
from loom.db.schema import PriceCatalog, RateCard
from loom.provider_pricing import ModelPrice, parse_price_import
from loom_service import wire_responses as wire
from loom_service.auth_guards import require_scope
from loom_service.dependencies import SessionAndCtx
from loom_service.forwarders import forward, propagate
from loom_service.price_catalogs import CatalogPrices, catalog_response, sync_catalog

router = APIRouter()


def _serialize_rate_card(row: RateCard) -> dict[str, Any]:
    return {
        "id": row.id,
        "captured_at": row.captured_at,
        "table": row.table,
    }


@router.get("/rate-cards", response_model=wire.GetRateCardsResponse, response_model_exclude_unset=True)
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




class CatalogResponse(CatalogPrices):
    id: str
    team_id: UUID | None
    name: str
    supplier_id: str | None
    source_url: str | None
    currency: Literal["USD"]
    unit: Literal["usd_per_1m_tokens"]
    revision: int
    source_metadata: dict[str, Any]
    updated_at: datetime | None
    checked_at: datetime | None
    sync_error: str | None
    stale: bool


class CatalogListResponse(BaseModel):
    items: list[CatalogResponse]


class CatalogImportResponse(BaseModel):
    summary: dict[str, list[str]]
    applied: bool
    catalog: CatalogResponse


class PriceImportResponse(BaseModel):
    prices: dict[str, ModelPrice]
    count: int


class CatalogCreate(CatalogPrices):
    name: str = Field(min_length=1, max_length=128)


class CatalogImport(BaseModel):
    model_config = ConfigDict(extra="forbid")
    content: str = Field(max_length=2_000_000)
    format: Literal["csv", "json"]
    expected_revision: int = Field(ge=0)
    apply: bool = False


async def _catalog(sc: SessionAndCtx, catalog_id: str, *, manage: bool = False) -> PriceCatalog:
    session, ctx = sc
    stmt = select(PriceCatalog).where(PriceCatalog.id == catalog_id)
    if manage:
        stmt = stmt.with_for_update()
    row = await session.scalar(stmt)
    if row is None or (row.team_id is not None and row.team_id != ctx.team_id and not is_admin(ctx)):
        raise HTTPException(404, "price catalog not found")
    if manage and not is_admin(ctx):
        if row.team_id is None:
            raise HTTPException(403, "platform administrator required")
        require_scope(ctx, "team:manage")
    return row


@router.get("/price-catalogs", response_model=CatalogListResponse)
async def list_price_catalogs(sc: SessionAndCtx) -> dict[str, Any]:
    session, ctx = sc
    stmt = select(PriceCatalog).where(or_(PriceCatalog.team_id.is_(None), PriceCatalog.team_id == ctx.team_id))
    rows = (await session.scalars(stmt.order_by(PriceCatalog.name))).all()
    return {"items": [catalog_response(row) for row in rows]}


@router.get("/price-catalogs/{catalog_id}", response_model=CatalogResponse)
async def get_price_catalog(sc: SessionAndCtx, catalog_id: str) -> dict[str, Any]:
    return catalog_response(await _catalog(sc, catalog_id))


@router.post("/price-catalogs", status_code=201, response_model=CatalogResponse)
async def create_price_catalog(sc: SessionAndCtx, payload: CatalogCreate) -> dict[str, Any]:
    session, ctx = sc
    if not is_admin(ctx):
        require_scope(ctx, "team:manage")
    if ctx.team_id is None:
        raise HTTPException(400, "a team context is required")
    row = PriceCatalog(id=f"team:{uuid4()}", team_id=ctx.team_id, name=payload.name,
                       prices={k: v.model_dump(mode="json") for k, v in payload.prices.items()},
                       aliases=payload.aliases, revision=1, updated_at=datetime.now(UTC))
    session.add(row)
    await session.commit()
    return catalog_response(row)


@router.post("/price-catalogs/{catalog_id}/import", response_model=CatalogImportResponse)
async def import_price_catalog(sc: SessionAndCtx, catalog_id: str, payload: CatalogImport) -> dict[str, Any]:
    session, _ctx = sc
    row = await _catalog(sc, catalog_id, manage=True)
    if row.supplier_id is not None:
        raise HTTPException(400, "supplier catalogs are synchronized; create a team catalog for negotiated prices")
    if row.revision != payload.expected_revision:
        raise HTTPException(409, "catalog changed; preview the import again")
    try:
        prices = parse_price_import(payload.content, payload.format)
    except (ValueError, TypeError) as exc:
        raise HTTPException(400, str(exc)) from exc
    values = {k: v.model_dump(mode="json") for k, v in prices.items()}
    # Removed targets and newly explicit model prices supersede old aliases.
    aliases = {k: v for k, v in row.aliases.items() if v in values and k not in values}
    summary = {
        "added": sorted(values.keys() - row.prices.keys()),
        "removed": sorted(row.prices.keys() - values.keys()),
        "changed": sorted(k for k in values.keys() & row.prices.keys() if values[k] != row.prices[k]),
    }
    if payload.apply:
        row.prices, row.aliases = values, aliases
        row.revision += 1
        row.updated_at = datetime.now(UTC)
        await session.commit()
    return {"summary": summary, "applied": payload.apply, "catalog": catalog_response(row)}


@router.post("/price-catalogs/{catalog_id}/sync", response_model=CatalogResponse)
async def sync_price_catalog(sc: SessionAndCtx, catalog_id: str) -> dict[str, Any]:
    session, _ctx = sc
    row = await _catalog(sc, catalog_id, manage=True)
    if row.supplier_id is None:
        raise HTTPException(400, "only supplier catalogs can synchronize")
    await sync_catalog(session, row)
    await session.commit()
    return catalog_response(row)


class PriceImportPreview(BaseModel):
    model_config = ConfigDict(extra="forbid")
    content: str = Field(max_length=2_000_000)
    format: Literal["csv", "json"]


@router.post("/price-imports/preview", response_model=PriceImportResponse)
async def preview_model_prices(sc: SessionAndCtx, payload: PriceImportPreview) -> dict[str, Any]:
    try:
        prices = parse_price_import(payload.content, payload.format)
    except (ValueError, TypeError) as exc:
        raise HTTPException(400, str(exc)) from exc
    return {"prices": {k: v.model_dump(mode="json") for k, v in prices.items()}, "count": len(prices)}
