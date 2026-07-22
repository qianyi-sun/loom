"""Authorization guards for loom_service routes (spec §4, §8).

Three primitives the routes compose:

- `require_human_or_admin(ctx)` — reject anonymous, worker tokens, and
  step-session JWTs. Workers and the trial-runtime auth path are NOT
  meant to call the service layer.
- `require_scope(ctx, scope)` — fail-closed scope check; admin tokens
  satisfy any scope (wildcard).
- `require_team_or_admin(ctx, team_id)` — cross-team access requires
  admin scope; same-team is always allowed.
"""

from __future__ import annotations

from uuid import UUID

from fastapi import HTTPException

from loom.auth import AuthContext, is_platform_admin_role
from loom.submission_identity import require_submitting_user as _require_submitting_user


def require_human_or_admin(ctx: AuthContext | None) -> AuthContext:
    """Reject anonymous, worker tokens, and step JWT principals."""
    if ctx is None:
        raise HTTPException(
            status_code=401, detail="missing or invalid token",
        )
    if ctx.type == "worker":
        raise HTTPException(
            status_code=403,
            detail="worker tokens cannot use the service layer",
        )
    if ctx.type == "step_session":
        raise HTTPException(
            status_code=403,
            detail="step session tokens cannot use the service layer",
        )
    if ctx.type not in {"team", "admin", "user", "readonly_probe"}:
        raise HTTPException(
            status_code=403,
            detail=f"unsupported token type: {ctx.type}",
        )
    return ctx


def is_admin(ctx: AuthContext) -> bool:
    return is_platform_admin_role(ctx.role) or ctx.type == "admin" or any(
        s.startswith("admin:") for s in ctx.scopes
    )


def require_scope(ctx: AuthContext, scope: str) -> None:
    if is_admin(ctx):
        return
    if scope not in ctx.scopes:
        raise HTTPException(
            status_code=403,
            detail=f"missing required scope: {scope}",
        )


def require_team_or_admin(ctx: AuthContext, target_team_id: UUID) -> None:
    if is_admin(ctx):
        return
    if ctx.team_id != target_team_id:
        raise HTTPException(
            status_code=403,
            detail="cross-team access requires admin scope",
        )


def require_submitting_user(ctx: AuthContext) -> None:
    _require_submitting_user(ctx)
