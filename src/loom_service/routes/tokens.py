"""GET/POST/DELETE /api/v1/tokens (spec §5.5).

Routes:
- GET    /tokens          — list tokens (team scopes to own team; admin sees all)
- POST   /tokens          — mint a new token (team callers limited to team-typed
                            tokens with scopes ⊆ {read:own, submit}; admin
                            may mint anything including cross-team)
- DELETE /tokens/{prefix} — revoke a token by its 8-char hex prefix; team
                            callers may only revoke tokens belonging to
                            their own team.

The hex-prefix lookup is O(N) over all tokens — fine for the v1 scale
(low thousands at most). A composite index would let us do an indexed
range query, but the prefix-as-identifier choice is per spec §5.5 to
keep the raw secret out of the response after revocation."""

from __future__ import annotations

import hashlib
import secrets
from datetime import UTC, datetime, timedelta
from typing import Annotated, Any
from uuid import UUID

from fastapi import APIRouter, Header, HTTPException, Request
from pydantic import BaseModel, Field
from sqlalchemy import insert, select, update

from loom.auth import verify_bearer_token
from loom.db.schema import Token
from loom_service.auth_guards import (
    is_admin,
    require_human_or_admin,
)

router = APIRouter()


class _CreateTokenReq(BaseModel):
    type: str = Field(pattern=r"^(team|admin)$")
    scopes: list[str] = Field(min_length=1)
    expires_in_days: int = Field(gt=0, le=365)
    team_id: UUID | None = None  # admin-only cross-team mint


# Scopes a non-admin team caller may grant via POST /tokens.
_TEAM_ALLOWED_SCOPES = frozenset({"read:own", "submit"})

# Every recognized scope across the whole product. Plan 20 will add
# `admin:rate_cards` (migration 0005 already grants it to existing
# `admin:tokens` holders). Plan 18 will add `read:trajectory`. Updates
# here are deliberate — an admin minting a token with a never-defined
# scope is almost certainly a typo, so the route rejects it instead of
# silently storing it.
_KNOWN_SCOPES = frozenset({
    "read:own",
    "submit",
    "admin:tokens",
    "admin:rate_cards",
    "worker:claim",
    "worker:report",
})


def _hash_prefix(token_hash: bytes) -> str:
    return token_hash.hex()[:8]


def _serialize(row: Token) -> dict[str, Any]:
    return {
        "token_hash_prefix": _hash_prefix(row.token_hash),
        "type": row.type,
        "scopes": list(row.scopes),
        "team_id": str(row.team_id) if row.team_id else None,
        "issued_at": row.issued_at.isoformat(),
        "expires_at": (
            row.expires_at.isoformat() if row.expires_at else None
        ),
        "revoked_at": (
            row.revoked_at.isoformat() if row.revoked_at else None
        ),
    }


@router.get("/tokens")
async def list_tokens(
    request: Request,
    authorization: Annotated[str | None, Header()] = None,
) -> dict[str, list[dict[str, Any]]]:
    async with request.app.state.session_factory() as s:
        ctx = await verify_bearer_token(s, authorization)
        ctx = require_human_or_admin(ctx)
        stmt = select(Token).order_by(Token.issued_at.desc())
        if not is_admin(ctx):
            stmt = stmt.where(Token.team_id == ctx.team_id)
        rows = (await s.execute(stmt)).scalars().all()
    return {"items": [_serialize(r) for r in rows]}


@router.post("/tokens", status_code=201)
async def create_token(
    request: Request,
    payload: _CreateTokenReq,
    authorization: Annotated[str | None, Header()] = None,
) -> dict[str, str]:
    async with request.app.state.session_factory() as s:
        ctx = await verify_bearer_token(s, authorization)
        ctx = require_human_or_admin(ctx)

        # Reject unrecognized scopes up front (audit M2). Catches
        # typos before they reach the DB; downstream guards only check
        # known scopes, so an unknown scope is dead weight at best,
        # confusing at worst.
        unknown = [sc for sc in payload.scopes if sc not in _KNOWN_SCOPES]
        if unknown:
            raise HTTPException(
                status_code=400,
                detail=f"unrecognized scopes: {sorted(unknown)}",
            )

        admin = is_admin(ctx)
        if not admin:
            if payload.type != "team":
                raise HTTPException(
                    status_code=403,
                    detail="only admins may mint admin tokens",
                )
            if payload.team_id is not None and payload.team_id != ctx.team_id:
                raise HTTPException(
                    status_code=403,
                    detail="cannot mint into another team",
                )
            for sc in payload.scopes:
                if sc not in _TEAM_ALLOWED_SCOPES:
                    raise HTTPException(
                        status_code=403,
                        detail=f"scope {sc!r} requires admin:tokens",
                    )

        # Admin tokens are global — accidentally pinning one to a team
        # would create a confused-deputy hazard at a later API revision,
        # so reject the input rather than silently dropping the field
        # (audit H2).
        if payload.type == "admin" and payload.team_id is not None:
            raise HTTPException(
                status_code=400,
                detail="admin tokens are global; team_id must be omitted",
            )

        target_team = (
            payload.team_id if payload.team_id else ctx.team_id
        )
        if payload.type == "team" and target_team is None:
            raise HTTPException(
                status_code=400, detail="team_id required for team token",
            )

        prefix_str = "team" if payload.type == "team" else "admin"
        raw = f"loom_{prefix_str}_{secrets.token_urlsafe(32)}"
        token_hash = hashlib.sha256(raw.encode()).digest()
        expires_at = datetime.now(UTC) + timedelta(
            days=payload.expires_in_days,
        )

        await s.execute(insert(Token).values(
            token_hash=token_hash,
            type=payload.type,
            scopes=list(payload.scopes),
            team_id=target_team if payload.type == "team" else None,
            issued_at=datetime.now(UTC),
            expires_at=expires_at,
        ))
        await s.commit()

    return {
        "token": raw,
        "token_hash_prefix": token_hash.hex()[:8],
        "expires_at": expires_at.isoformat(),
    }


@router.delete("/tokens/{prefix}", status_code=204)
async def revoke_token(
    request: Request,
    prefix: str,
    authorization: Annotated[str | None, Header()] = None,
) -> None:
    if len(prefix) != 8 or not all(c in "0123456789abcdef" for c in prefix):
        raise HTTPException(
            status_code=400, detail="prefix must be 8 lowercase hex chars",
        )
    async with request.app.state.session_factory() as s:
        ctx = await verify_bearer_token(s, authorization)
        ctx = require_human_or_admin(ctx)

        # Scope the lookup to what the caller is allowed to see: team
        # callers can only revoke their own team's tokens, so include
        # the team_id predicate BEFORE the prefix scan. This both
        # closes a non-deterministic cross-team-collision window
        # (audit H1) and avoids loading every row into Python
        # (audit M1). Admins still get the full table.
        stmt = select(Token).where(Token.revoked_at.is_(None))
        if not is_admin(ctx):
            stmt = stmt.where(Token.team_id == ctx.team_id)
        rows = (await s.execute(stmt)).scalars().all()
        matches = [
            r for r in rows if r.token_hash.hex().startswith(prefix)
        ]
        if not matches:
            raise HTTPException(status_code=404, detail="token not found")
        if len(matches) > 1:
            # 8-hex-char prefix collisions are rare but possible (32
            # bits, birthday ≈ 77k tokens). When they happen, refuse
            # rather than silently picking one — the caller can supply
            # more hex digits in a future API revision (currently
            # 8 is hard-coded by spec §5.5).
            raise HTTPException(
                status_code=409,
                detail=(
                    f"prefix {prefix!r} matches {len(matches)} tokens; "
                    f"ambiguous"
                ),
            )
        target = matches[0]
        if target.team_id is None and not is_admin(ctx):
            # Defense-in-depth: a non-admin shouldn't have reached the
            # prefix-scan over a global-admin token (team_id filter
            # above), but guard anyway in case the scope filter ever
            # changes shape.
            raise HTTPException(
                status_code=403, detail="admin scope required",
            )
        await s.execute(
            update(Token)
            .where(Token.token_hash == target.token_hash)
            .values(revoked_at=datetime.now(UTC)),
        )
        await s.commit()
