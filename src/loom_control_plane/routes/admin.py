"""Admin token endpoints."""

from __future__ import annotations

import hashlib
import re
import secrets
from datetime import UTC, datetime, timedelta
from typing import Any

from fastapi import APIRouter, Header, HTTPException, Request
from sqlalchemy import insert, text

from loom.auth import verify_bearer_token
from loom.db.schema import Token

router = APIRouter(prefix="/admin")

# Bug 3 fix: revoke prefix must be hex (the same charset `encode(.., 'hex')`
# emits) and at least 4 chars long, otherwise `%` or `` would revoke every
# token in the table.
_HEX_PREFIX_RE = re.compile(r"^[0-9a-f]{4,64}$")


@router.post("/worker-tokens", status_code=201)
async def issue_worker_token(
    request: Request,
    payload: dict[str, Any],
    authorization: str | None = Header(default=None),
) -> dict[str, str]:
    async with request.app.state.session_factory() as session:
        ctx = await verify_bearer_token(
            session,
            authorization,
            admin_verifier=getattr(
                request.app.state, "admin_secret_verifier", None,
            ),
        )
    if ctx is None or "admin:tokens" not in ctx.scopes:
        raise HTTPException(status_code=403, detail="missing scope admin:tokens")

    raw_bytes = secrets.token_bytes(32)
    raw = "loom_w_" + raw_bytes.hex()
    token_hash = hashlib.sha256(raw.encode()).digest()
    expires_at: datetime | None = None
    days = payload.get("expires_in_days")
    if days is not None:
        expires_at = datetime.now(UTC) + timedelta(days=int(days))

    async with request.app.state.session_factory() as session:
        await session.execute(insert(Token).values(
            token_hash=token_hash,
            type="worker",
            scopes=["worker:claim", "worker:report", "worker:index"],
            team_id=None,
            issued_at=datetime.now(UTC),
            expires_at=expires_at,
        ))
        await session.commit()

    return {
        "token": raw,
        "token_hash_prefix": token_hash.hex()[:8],
    }


@router.delete("/worker-tokens/{prefix}")
async def revoke_token(
    prefix: str,
    request: Request,
    authorization: str | None = Header(default=None),
) -> dict[str, str]:
    async with request.app.state.session_factory() as session:
        ctx = await verify_bearer_token(
            session,
            authorization,
            admin_verifier=getattr(
                request.app.state, "admin_secret_verifier", None,
            ),
        )
    if ctx is None or "admin:tokens" not in ctx.scopes:
        raise HTTPException(status_code=403, detail="missing scope")
    if not _HEX_PREFIX_RE.fullmatch(prefix):
        raise HTTPException(
            status_code=400,
            detail="prefix must be 4-64 hex characters",
        )
    async with request.app.state.session_factory() as session:
        await session.execute(
            text("""
                UPDATE tokens SET revoked_at = NOW()
                 WHERE encode(token_hash, 'hex') LIKE :prefix
            """),
            {"prefix": prefix + "%"},
        )
        await session.commit()
    return {"status": "revoked"}
