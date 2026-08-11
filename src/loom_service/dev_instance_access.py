"""Build the minimal, hash-only owner bootstrap for a dev instance."""

from __future__ import annotations

from datetime import UTC, datetime
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from loom.auth import AuthContext
from loom.db.schema import (
    Team,
    TeamMembership,
    TeamQuota,
    Token,
    User,
    UserSession,
)
from loom.dev_instance_provisioner import (
    BearerAccessSnapshot,
    OwnerAccessSnapshot,
    SessionAccessSnapshot,
)
from loom.personal_dev_environment import PersonalDevAccessBinding

_DEFAULT_LICENSE_ALLOWLIST = ("MIT", "Apache-2.0", "BSD-3-Clause", "CC-BY-4.0")


class DevInstanceAccessError(RuntimeError):
    """The authenticated management identity cannot be bootstrapped safely."""


def access_binding_from_context(ctx: AuthContext) -> PersonalDevAccessBinding:
    """Reduce a verified request principal to the one credential it used."""
    if ctx.auth_kind == "session":
        if ctx.session_hash is None:
            raise DevInstanceAccessError("authenticated browser session is incomplete")
        return PersonalDevAccessBinding(
            auth_kind="session",
            credential_hash=ctx.session_hash,
        )
    if ctx.auth_kind != "bearer" or not ctx.token_hash:
        raise DevInstanceAccessError("authenticated bearer credential is incomplete")
    return PersonalDevAccessBinding(
        auth_kind="bearer",
        credential_hash=ctx.token_hash,
    )


async def load_owner_access_snapshot(
    session: AsyncSession,
    ctx: AuthContext,
    *,
    now: datetime | None = None,
) -> OwnerAccessSnapshot:
    """Load the caller's user/team plus only the credential used now.

    Raw credentials are unavailable by design. Mirroring the verified hashes
    lets the caller reuse its current bearer or session while password login
    also remains available on the isolated endpoint.
    """
    if ctx.user_id is None or ctx.team_id is None:
        raise DevInstanceAccessError("dev instance owner identity is incomplete")
    return await load_owner_access_snapshot_by_binding(
        session,
        owner_user_id=ctx.user_id,
        owner_team_id=ctx.team_id,
        binding=access_binding_from_context(ctx),
        now=now,
    )


async def load_owner_access_snapshot_by_binding(
    session: AsyncSession,
    *,
    owner_user_id: UUID,
    owner_team_id: UUID,
    binding: PersonalDevAccessBinding,
    now: datetime | None = None,
) -> OwnerAccessSnapshot:
    """Reload the exact attempt-bound credential and current owner policy.

    This is intentionally suitable for delayed background reconciliation: it
    does not depend on a request-local ``AuthContext`` and fails closed when
    the captured credential, owner, team, or membership is no longer active.
    """
    now = now or datetime.now(UTC)
    user = await session.get(User, owner_user_id)
    team = await session.get(Team, owner_team_id)
    membership = (
        await session.execute(
            select(TeamMembership).where(
                TeamMembership.user_id == owner_user_id,
                TeamMembership.team_id == owner_team_id,
            ),
        )
    ).scalar_one_or_none()
    if (
        user is None
        or team is None
        or membership is None
        or user.status != "active"
        or user.disabled_at is not None
        or team.disabled_at is not None
    ):
        raise DevInstanceAccessError("dev instance owner records are incomplete")
    quota = await session.get(TeamQuota, owner_team_id)

    bearer: BearerAccessSnapshot | None = None
    browser_session: SessionAccessSnapshot | None = None
    if binding.auth_kind == "session":
        source_session = await session.get(UserSession, binding.credential_hash)
        if (
            source_session is None
            or source_session.user_id != owner_user_id
            or source_session.current_team_id != owner_team_id
            or source_session.revoked_at is not None
            or source_session.expires_at <= now
        ):
            raise DevInstanceAccessError("authenticated browser session is unavailable")
        browser_session = SessionAccessSnapshot(
            session_hash=source_session.session_hash,
            csrf_hash=source_session.csrf_hash,
            issued_at=source_session.issued_at,
            expires_at=source_session.expires_at,
            last_seen_at=source_session.last_seen_at,
        )
    else:
        source_token = await session.get(Token, binding.credential_hash)
        if (
            source_token is None
            or source_token.created_by_user_id != owner_user_id
            or source_token.team_id != owner_team_id
            or source_token.type != "team"
            or source_token.revoked_at is not None
            or (source_token.expires_at is not None and source_token.expires_at <= now)
        ):
            raise DevInstanceAccessError("authenticated bearer credential is unavailable")
        bearer = BearerAccessSnapshot(
            token_hash=source_token.token_hash,
            name=source_token.name,
            type=source_token.type,
            scopes=tuple(source_token.scopes),
            issued_at=source_token.issued_at,
            expires_at=source_token.expires_at,
            created_by_actor=source_token.created_by_actor,
        )

    return OwnerAccessSnapshot(
        user_id=user.id,
        email=user.email,
        username=user.username,
        username_normalized=user.username_normalized,
        display_name=user.display_name,
        password_hash=user.password_hash,
        password_set_at=user.password_set_at,
        user_status=user.status,
        user_disabled_at=user.disabled_at,
        user_created_at=user.created_at,
        user_last_login_at=user.last_login_at,
        team_id=team.id,
        team_name=team.name,
        team_created_at=team.created_at,
        membership_role=membership.role,
        membership_created_at=membership.created_at,
        fair_share_weight=quota.fair_share_weight if quota is not None else 1.0,
        max_attempts_ceiling=quota.max_attempts_ceiling if quota is not None else 3,
        license_allowlist=(
            tuple(quota.license_allowlist) if quota is not None else _DEFAULT_LICENSE_ALLOWLIST
        ),
        taskset_max_count=quota.taskset_max_count if quota is not None else None,
        taskset_max_storage_bytes=(quota.taskset_max_storage_bytes if quota is not None else None),
        allow_private_endpoints=(quota.allow_private_endpoints if quota is not None else False),
        bearer=bearer,
        session=browser_session,
    )


__all__ = [
    "DevInstanceAccessError",
    "access_binding_from_context",
    "load_owner_access_snapshot",
    "load_owner_access_snapshot_by_binding",
]
