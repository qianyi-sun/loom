"""Admin endpoints — rate card upsert (more in Plan 5)."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from fastapi import APIRouter, Header, HTTPException, Request
from pydantic import ValidationError
from sqlalchemy.dialects.postgresql import insert

from loom.db.schema import RateCard
from loom_llm_gateway.auth import verify_bearer_token
from loom_llm_gateway.rate_card import RateCardTable

router = APIRouter(prefix="/admin")


@router.post("/rate-cards", status_code=201)
async def upsert_rate_card(
    request: Request,
    payload: dict[str, Any],
    authorization: str | None = Header(default=None),
) -> dict[str, str]:
    async with request.app.state.session_factory() as session:
        ctx = await verify_bearer_token(session, authorization)
    if ctx is None:
        raise HTTPException(status_code=401, detail="invalid token")
    if "admin:rate_cards" not in ctx.scopes:
        raise HTTPException(status_code=403, detail="missing scope admin:rate_cards")

    card_id = payload.get("id")
    if not card_id or "entries" not in payload:
        raise HTTPException(status_code=400, detail="id + entries required")

    # Bug 2 fix: validate the payload BEFORE persisting. A bad payload would
    # otherwise land in the DB and break every subsequent chat request when
    # the rate-card cache tries to construct RateCardTable from it.
    now = datetime.now(UTC)
    try:
        RateCardTable.model_validate({
            "id": card_id,
            "captured_at": now,
            "entries": payload["entries"],
        })
    except ValidationError as exc:
        raise HTTPException(status_code=400, detail=exc.errors()) from exc

    async with request.app.state.session_factory() as session:
        stmt = insert(RateCard).values(
            id=card_id, captured_at=now, table=payload,
        ).on_conflict_do_update(
            index_elements=["id"],
            set_={"table": payload, "captured_at": now},
        )
        await session.execute(stmt)
        await session.commit()
    request.app.state.rate_card_cache.invalidate()
    return {"id": card_id}
