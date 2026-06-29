"""Submission identity guards shared by service and control-plane submit paths."""

from __future__ import annotations

from fastapi import HTTPException

from loom.auth import AuthContext


def require_submitting_user(ctx: AuthContext) -> None:
    """Require a human user identity for user-facing work creation."""
    if ctx.user_id is not None:
        return
    if ctx.type == "team":
        raise HTTPException(
            status_code=403,
            detail=(
                "legacy team token cannot submit user-facing work; "
                "log in with username/password and create a user-owned API token"
            ),
        )
    raise HTTPException(
        status_code=403,
        detail=(
            "submissions require a browser session or user-owned API token; "
            "admin and internal service credentials cannot submit user-facing work"
        ),
    )
