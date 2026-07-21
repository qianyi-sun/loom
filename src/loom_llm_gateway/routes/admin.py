"""Admin endpoints — rate card upsert (more in Plan 5)."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import httpx
from fastapi import APIRouter, Header, HTTPException, Request
from pydantic import ValidationError
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert

from loom.auth import verify_bearer_token
from loom.db.schema import RateCard
from loom_llm_gateway.rate_card import RateCardTable, hash_table
from loom_llm_gateway.yibuapi_pricing import (
    DEFAULT_YIBUAPI_PRICING_URL,
    YIBUAPI_RATE_CARD_PROVIDER,
    build_yibuapi_rate_card,
)

router = APIRouter(prefix="/admin")


async def fetch_yibuapi_pricing_payload(source_url: str) -> dict[str, Any]:
    try:
        async with httpx.AsyncClient(timeout=20.0) as client:
            resp = await client.get(source_url)
            resp.raise_for_status()
            payload = resp.json()
    except httpx.HTTPError as exc:
        raise HTTPException(
            status_code=502,
            detail=f"could not fetch YibuAPI pricing from {source_url}: {exc}",
        ) from exc
    except ValueError as exc:
        raise HTTPException(
            status_code=502,
            detail=f"YibuAPI pricing response was not valid JSON: {exc}",
        ) from exc
    if not isinstance(payload, dict):
        raise HTTPException(
            status_code=502,
            detail="YibuAPI pricing response must be a JSON object",
        )
    return payload


async def _require_rate_card_admin(
    request: Request,
    authorization: str | None,
) -> None:
    async with request.app.state.session_factory() as session:
        ctx = await verify_bearer_token(
            session,
            authorization,
            admin_verifier=getattr(
                request.app.state,
                "admin_secret_verifier",
                None,
            ),
        )
    if ctx is None:
        raise HTTPException(status_code=401, detail="invalid token")
    if "admin:rate_cards" not in ctx.scopes:
        raise HTTPException(
            status_code=403,
            detail="missing scope admin:rate_cards",
        )


async def _require_rate_card_reader(
    request: Request,
    authorization: str | None,
) -> None:
    async with request.app.state.session_factory() as session:
        ctx = await verify_bearer_token(
            session,
            authorization,
            admin_verifier=getattr(
                request.app.state,
                "admin_secret_verifier",
                None,
            ),
        )
    if ctx is None:
        raise HTTPException(status_code=401, detail="invalid token")


def _serialize_rate_card(row: RateCard) -> dict[str, Any]:
    table = RateCardTable.model_validate(
        {
            **row.table,
            "id": row.id,
            "captured_at": row.captured_at,
        }
    )
    return {
        "id": row.id,
        "captured_at": row.captured_at,
        "table": row.table,
        "table_hash": hash_table(table),
    }


@router.get("/rate-cards")
async def list_rate_cards(
    request: Request,
    authorization: str | None = Header(default=None),
) -> dict[str, list[dict[str, Any]]]:
    await _require_rate_card_reader(request, authorization)
    async with request.app.state.session_factory() as session:
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
    request: Request,
    rate_card_id: str,
    authorization: str | None = Header(default=None),
) -> dict[str, Any]:
    await _require_rate_card_reader(request, authorization)
    async with request.app.state.session_factory() as session:
        row = await session.get(RateCard, rate_card_id)
    if row is None:
        raise HTTPException(status_code=404, detail="rate card not found")
    return _serialize_rate_card(row)


async def _store_rate_card(
    request: Request,
    *,
    payload: dict[str, Any],
    captured_at: datetime,
) -> str:
    card_id = payload.get("id")
    if not card_id or "entries" not in payload:
        raise HTTPException(status_code=400, detail="id + entries required")

    # Bug 2 fix: validate the payload BEFORE persisting. A bad payload would
    # otherwise land in the DB and break every subsequent chat request when
    # the rate-card cache tries to construct RateCardTable from it.
    try:
        RateCardTable.model_validate(
            {
                **payload,
                "id": card_id,
                "captured_at": captured_at,
            }
        )
    except ValidationError as exc:
        raise HTTPException(status_code=400, detail=exc.errors()) from exc

    async with request.app.state.session_factory() as session:
        stmt = (
            insert(RateCard)
            .values(
                id=card_id,
                captured_at=captured_at,
                table=payload,
            )
            .on_conflict_do_update(
                index_elements=["id"],
                set_={"table": payload, "captured_at": captured_at},
            )
        )
        await session.execute(stmt)
        await session.commit()
    request.app.state.rate_card_cache.invalidate()
    return str(card_id)


@router.post("/rate-cards", status_code=201)
async def upsert_rate_card(
    request: Request,
    payload: dict[str, Any],
    authorization: str | None = Header(default=None),
) -> dict[str, str]:
    await _require_rate_card_admin(request, authorization)
    now = datetime.now(UTC)
    card_id = await _store_rate_card(request, payload=payload, captured_at=now)
    return {"id": card_id}


@router.post("/rate-cards/sync/yibuapi", status_code=201)
async def sync_yibuapi_rate_card(
    request: Request,
    payload: dict[str, Any] | None = None,
    authorization: str | None = Header(default=None),
) -> dict[str, Any]:
    await _require_rate_card_admin(request, authorization)
    body = payload or {}
    source_url = str(body.get("source_url") or DEFAULT_YIBUAPI_PRICING_URL)
    group = str(body.get("group") or "default")
    provider = str(body.get("provider") or YIBUAPI_RATE_CARD_PROVIDER)
    now = datetime.now(UTC)
    fetcher = getattr(
        request.app.state,
        "yibuapi_pricing_fetcher",
        fetch_yibuapi_pricing_payload,
    )
    pricing_payload = await fetcher(source_url)
    card = build_yibuapi_rate_card(
        pricing_payload,
        source_url=source_url,
        fetched_at=now,
        provider=provider,
        group=group,
    )
    card_id = await _store_rate_card(request, payload=card, captured_at=now)
    return {
        "id": card_id,
        "source_url": card["source_url"],
        "pricing_version": card["pricing_version"],
        "entry_count": card["entry_count"],
        "skipped_model_count": card["skipped_model_count"],
    }
