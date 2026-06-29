"""loom_service auth guards (Plan 17 Task 3)."""

from __future__ import annotations

from uuid import UUID, uuid4

import pytest
from fastapi import HTTPException

from loom.auth import AuthContext, role_scopes
from loom_service.auth_guards import (
    is_admin,
    require_human_or_admin,
    require_scope,
    require_submitting_user,
    require_team_or_admin,
)


def _ctx(
    *,
    type_: str,
    scopes: list[str],
    team_id: UUID | None = None,
    user_id: UUID | None = None,
    role: str | None = None,
) -> AuthContext:
    return AuthContext(
        token_hash=b"\x00" * 32,
        type=type_,
        scopes=scopes,
        team_id=team_id,
        expires_at=None,
        user_id=user_id,
        role=role,
    )


def test_rejects_anonymous() -> None:
    with pytest.raises(HTTPException) as ei:
        require_human_or_admin(None)
    assert ei.value.status_code == 401


def test_rejects_worker() -> None:
    ctx = _ctx(type_="worker", scopes=["worker:claim"])
    with pytest.raises(HTTPException) as ei:
        require_human_or_admin(ctx)
    assert ei.value.status_code == 403
    assert "worker" in ei.value.detail


def test_rejects_step_session() -> None:
    ctx = _ctx(type_="step_session", scopes=["llm:invoke"])
    with pytest.raises(HTTPException) as ei:
        require_human_or_admin(ctx)
    assert ei.value.status_code == 403
    assert "step session" in ei.value.detail


def test_rejects_unsupported_type() -> None:
    ctx = _ctx(type_="bogus", scopes=[])
    with pytest.raises(HTTPException) as ei:
        require_human_or_admin(ctx)
    assert ei.value.status_code == 403


def test_allows_team() -> None:
    ctx = _ctx(type_="team", scopes=["read:own"], team_id=uuid4())
    assert require_human_or_admin(ctx) is ctx


def test_allows_user_session() -> None:
    ctx = _ctx(
        type_="user",
        scopes=role_scopes("viewer"),
        team_id=uuid4(),
        role="viewer",
    )
    assert require_human_or_admin(ctx) is ctx


def test_allows_admin() -> None:
    ctx = _ctx(type_="admin", scopes=["admin:tokens"])
    assert require_human_or_admin(ctx) is ctx


def test_is_admin_by_type() -> None:
    assert is_admin(_ctx(type_="admin", scopes=[]))


def test_is_admin_by_scope() -> None:
    assert is_admin(_ctx(type_="team", scopes=["admin:tokens"]))


def test_is_admin_false_for_plain_team() -> None:
    assert not is_admin(_ctx(type_="team", scopes=["read:own", "submit"]))


def test_role_scope_mapping() -> None:
    assert role_scopes("viewer") == ["read:own"]
    assert role_scopes("member") == ["read:own", "submit"]
    assert role_scopes("owner") == [
        "read:own",
        "submit",
        "tokens:manage",
        "providers:manage",
        "team:manage",
    ]
    assert role_scopes("platform_admin") == ["admin:platform"]


def test_is_admin_for_platform_admin_user_role() -> None:
    ctx = _ctx(
        type_="user",
        scopes=role_scopes("platform_admin"),
        role="platform_admin",
    )
    assert is_admin(ctx)


def test_is_admin_false_for_plain_user_role() -> None:
    ctx = _ctx(
        type_="user",
        scopes=role_scopes("owner"),
        team_id=uuid4(),
        role="owner",
    )
    assert not is_admin(ctx)


def test_require_scope_missing() -> None:
    ctx = _ctx(type_="team", scopes=["read:own"], team_id=uuid4())
    with pytest.raises(HTTPException) as ei:
        require_scope(ctx, "submit")
    assert ei.value.status_code == 403


def test_require_scope_satisfied() -> None:
    ctx = _ctx(type_="team", scopes=["read:own", "submit"], team_id=uuid4())
    require_scope(ctx, "submit")


def test_require_scope_admin_wildcard() -> None:
    """Admin tokens satisfy ANY scope check."""
    ctx = _ctx(type_="admin", scopes=["admin:tokens"])
    require_scope(ctx, "submit")
    require_scope(ctx, "anything")


def test_require_scope_platform_admin_user_wildcard() -> None:
    ctx = _ctx(
        type_="user",
        scopes=role_scopes("platform_admin"),
        role="platform_admin",
    )
    require_scope(ctx, "submit")
    require_scope(ctx, "admin:tokens")


def test_require_submitting_user_allows_browser_session() -> None:
    user_id = uuid4()
    ctx = _ctx(
        type_="user",
        scopes=role_scopes("member"),
        team_id=uuid4(),
        user_id=user_id,
        role="member",
    )
    require_submitting_user(ctx)


def test_require_submitting_user_allows_user_owned_api_token() -> None:
    ctx = _ctx(
        type_="team",
        scopes=["read:own", "submit"],
        team_id=uuid4(),
        user_id=uuid4(),
    )
    require_submitting_user(ctx)


def test_require_submitting_user_rejects_legacy_team_token() -> None:
    ctx = _ctx(type_="team", scopes=["read:own", "submit"], team_id=uuid4())
    with pytest.raises(HTTPException) as ei:
        require_submitting_user(ctx)
    assert ei.value.status_code == 403
    assert "legacy team token" in ei.value.detail
    assert "user-owned API token" in ei.value.detail


def test_require_submitting_user_rejects_admin_secret_token() -> None:
    ctx = _ctx(type_="admin", scopes=["admin:tokens"])
    with pytest.raises(HTTPException) as ei:
        require_submitting_user(ctx)
    assert ei.value.status_code == 403
    assert "user-owned API token" in ei.value.detail


def test_team_or_admin_other_team_forbidden() -> None:
    team_a, team_b = uuid4(), uuid4()
    ctx = _ctx(type_="team", scopes=["read:own"], team_id=team_a)
    with pytest.raises(HTTPException) as ei:
        require_team_or_admin(ctx, team_b)
    assert ei.value.status_code == 403


def test_team_or_admin_same_team_ok() -> None:
    team = uuid4()
    ctx = _ctx(type_="team", scopes=["read:own"], team_id=team)
    require_team_or_admin(ctx, team)


def test_team_or_admin_admin_wildcard() -> None:
    """Admin bypasses team-id matching."""
    team_a, team_b = uuid4(), uuid4()
    ctx = _ctx(type_="admin", scopes=["admin:tokens"], team_id=team_a)
    require_team_or_admin(ctx, team_b)


def test_team_or_admin_platform_admin_user_wildcard() -> None:
    team_a, team_b = uuid4(), uuid4()
    ctx = _ctx(
        type_="user",
        scopes=role_scopes("platform_admin"),
        team_id=team_a,
        role="platform_admin",
    )
    require_team_or_admin(ctx, team_b)
