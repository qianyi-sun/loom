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
from typing import Any
from uuid import UUID

from fastapi import APIRouter, Header, HTTPException, Request
from pydantic import BaseModel, Field
from sqlalchemy import insert, select, update

from loom.db.schema import Token
from loom_service.admin_audit import require_admin_actor, write_admin_audit_event
from loom_service.auth_guards import (
    is_admin,
)
from loom_service.dependencies import SessionAndCtx

router = APIRouter()


class _CreateTokenReq(BaseModel):
    type: str = Field(pattern=r"^(team|admin)$")
    scopes: list[str] = Field(min_length=1)
    expires_in_days: int = Field(gt=0, le=365)
    team_id: UUID | None = None  # admin-only cross-team mint


# Scopes a non-admin team caller may grant via POST /tokens.
_TEAM_ALLOWED_SCOPES = frozenset({"read:own", "submit"})

# Every recognized scope across the whole product. Updates here are deliberate:
# a caller minting a token with a never-defined scope is almost certainly a
# typo, so the route rejects it instead of silently storing it. Admin scopes are
# listed only so legacy rows/migration paths remain explicit; new DB-backed
# tokens carrying admin authority are rejected below.
_KNOWN_SCOPES = frozenset({
    "read:own",
    "submit",
    "submit:batch",
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
    sc: SessionAndCtx,
) -> dict[str, list[dict[str, Any]]]:
    s, ctx = sc
    stmt = select(Token).order_by(Token.issued_at.desc())
    if not is_admin(ctx):
        stmt = stmt.where(Token.team_id == ctx.team_id)
    rows = (await s.execute(stmt)).scalars().all()
    return {"items": [_serialize(r) for r in rows]}


@router.post("/tokens", status_code=201)
async def create_token(
    request: Request,
    sc: SessionAndCtx,
    payload: _CreateTokenReq,
    x_loom_admin_actor: str | None = Header(default=None),
) -> dict[str, str]:
    s, ctx = sc

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

    if payload.type == "admin":
        raise HTTPException(
            status_code=400,
            detail=(
                "database-backed admin tokens are disabled; use the "
                "singleton admin secret file managed by `loom service "
                "init-admin` or `loom service rotate-admin`"
            ),
        )
    if any(scope.startswith("admin:") for scope in payload.scopes):
        raise HTTPException(
            status_code=400,
            detail=(
                "database-backed admin scopes are disabled; admin "
                "authority must come from the singleton admin secret file"
            ),
        )

    admin = is_admin(ctx)
    admin_actor = require_admin_actor(x_loom_admin_actor) if admin else None
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
        for scope in payload.scopes:
            if scope not in _TEAM_ALLOWED_SCOPES:
                raise HTTPException(
                    status_code=403,
                    detail=f"scope {scope!r} requires admin:tokens",
                )

    target_team = (
        payload.team_id if payload.team_id else ctx.team_id
    )
    if payload.type == "team" and target_team is None:
        raise HTTPException(
            status_code=400, detail="team_id required for team token",
        )

    raw = f"loom_team_{secrets.token_urlsafe(32)}"
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
    token_hash_prefix = token_hash.hex()[:8]
    if admin_actor is not None:
        await write_admin_audit_event(
            s,
            actor=admin_actor,
            action="token.create",
            target_type="token",
            target_id=token_hash_prefix,
            request=request,
            metadata={
                "token_hash_prefix": token_hash_prefix,
                "token_type": payload.type,
                "team_id": str(target_team) if target_team else None,
                "scopes": list(payload.scopes),
                "expires_at": expires_at.isoformat(),
            },
        )
    await s.commit()

    return {
        "token": raw,
        "token_hash_prefix": token_hash_prefix,
        "expires_at": expires_at.isoformat(),
    }


@router.delete("/tokens/{prefix}", status_code=204)
async def revoke_token(
    request: Request,
    sc: SessionAndCtx,
    prefix: str,
    x_loom_admin_actor: str | None = Header(default=None),
) -> None:
    if len(prefix) != 8 or not all(c in "0123456789abcdef" for c in prefix):
        raise HTTPException(
            status_code=400, detail="prefix must be 8 lowercase hex chars",
        )
    s, ctx = sc
    admin = is_admin(ctx)
    admin_actor = require_admin_actor(x_loom_admin_actor) if admin else None

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
    if admin_actor is not None:
        await write_admin_audit_event(
            s,
            actor=admin_actor,
            action="token.revoke",
            target_type="token",
            target_id=prefix,
            request=request,
            metadata={
                "token_hash_prefix": prefix,
                "token_type": target.type,
                "team_id": str(target.team_id) if target.team_id else None,
                "scopes": list(target.scopes),
            },
        )
    await s.commit()
