"""FastAPI `Depends()` callables for the common route preamble.

Before this module every endpoint repeated:

    async with request.app.state.session_factory() as s:
        ctx = await verify_bearer_token(s, authorization)
        require_human_or_admin(ctx)
        ...

across ~30 handlers. The `Authorization` header parameter and the
`session_factory` lookup both ran inline; any per-request hook
(audit logging, rate limiting, role differentiation) had to be
threaded through every site.

This module centralizes the three jobs:

- **`authed_session`** — open a session, verify the bearer token,
  enforce human-or-admin, yield `(session, auth_context)`. Async
  generator so the session stays open for the whole request.
- **`authed_admin`** — same, but additionally rejects non-admin
  tokens. Use on routes gated by `admin:*` scopes today.
- **`team_membership`** — given a `team_id` path param, enforces
  same-team-or-admin. Routes that pull a team-scoped resource use
  this as a second `Depends()` after `authed_session`.

The legacy helpers in `auth_guards.py` (`require_human_or_admin`,
`require_scope`, `require_team_or_admin`, `is_admin`) stay — they
encapsulate the business rules these deps wrap.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Annotated
from uuid import UUID

from fastapi import Depends, Header, HTTPException, Request
from sqlalchemy.ext.asyncio import AsyncSession

from loom.auth import AuthContext, verify_bearer_token
from loom_service.auth_guards import (
    is_admin,
    require_human_or_admin,
    require_team_or_admin,
)


async def authed_session(
    request: Request,
    authorization: Annotated[str | None, Header()] = None,
) -> AsyncIterator[tuple[AsyncSession, AuthContext]]:
    """Open a session, verify the bearer token, enforce human/admin.

    Yields the open session + the auth context so handlers can use
    both. The session closes on request teardown (FastAPI manages
    the generator).
    """
    async with request.app.state.session_factory() as session:
        ctx_optional = await verify_bearer_token(session, authorization)
        ctx = require_human_or_admin(ctx_optional)
        yield session, ctx


SessionAndCtx = Annotated[
    tuple[AsyncSession, AuthContext], Depends(authed_session),
]


async def authed_admin(
    sc: SessionAndCtx,
) -> tuple[AsyncSession, AuthContext]:
    """Same as `authed_session` but rejects non-admin tokens.

    Use on routes today gated inline by `is_admin(ctx)` or by an
    explicit `require_scope(ctx, "admin:*")`. Returns the same
    `(session, ctx)` tuple so the handler can keep querying.
    """
    _session, ctx = sc
    if not is_admin(ctx):
        raise HTTPException(status_code=403, detail="admin scope required")
    return sc


AdminSessionAndCtx = Annotated[
    tuple[AsyncSession, AuthContext], Depends(authed_admin),
]


def team_membership(team_id: UUID, sc: SessionAndCtx) -> AuthContext:
    """Enforce same-team-or-admin against a team_id path param.

    Pair with `authed_session` (FastAPI resolves both deps; this one
    reuses the session for its check via Depends parameter chain).
    Returns the validated context for the handler if it needs it.
    """
    _session, ctx = sc
    require_team_or_admin(ctx, team_id)
    return ctx

# modular-A/B/C/D shipped: benchmarks.json + Depends() + Materializer + `loom datasets`
