"""Provision a deployment-managed headless smoke-user credential.

The release-gate / operator trajectory smoke (``s13_smoke``,
``submit_mode="user-token"``) submits ``oracle × gb10-smoke`` via
``POST /api/v1/trials``. That route requires a *user-owned* API token:
``require_submitting_user`` rejects anything whose ``ctx.user_id`` is
``None`` (a bare admin or "legacy" team token cannot submit user-facing
work). A fresh or post-cutover database has no such principal, so the
headless smoke has nothing to submit with.

This module provisions one the way operational infra should be created —
service-account-owned, repo-driven, no human login and no personal
token: a dedicated non-human ``loom-smoke`` User + Team + owner
membership, then a freshly-minted user-owned token with ``submit`` scope.
The deploy runs this against the target DB and writes the returned raw
token into ``loom-secrets`` for the release-gate to read via
``smoke_api_token_source`` — see ``docs/runbooks/deploy-staging-k3s.md``.

Idempotent on the identity (user/team/membership are get-or-create);
each run mints a fresh token and, by default, revokes the smoke user's
prior tokens (rotation) so exactly one live credential exists.
"""
from __future__ import annotations

import hashlib
import secrets
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

from sqlalchemy import create_engine, insert, select, update
from sqlalchemy.orm import sessionmaker

from loom.db.schema import Team, TeamMembership, TeamQuota, Token, User
from loom_service.password_auth import normalize_username

_DEFAULT_USERNAME = "loom-smoke"
_DEFAULT_TEAM_NAME = "loom-smoke"
_DEFAULT_SCOPES: tuple[str, ...] = ("submit", "read:own")
_DEFAULT_TTL_DAYS = 90


@dataclass(frozen=True)
class SmokeCredential:
    """The result of provisioning the smoke-user credential."""

    raw_token: str
    token_hash_prefix: str
    user_id: UUID
    team_id: UUID
    username: str
    team_name: str
    expires_at: datetime | None
    rotated_prior: int  # count of the user's prior tokens revoked this run


def _raw_api_token() -> str:
    """Mint a raw user API token in the same shape the service issues.

    Mirrors ``loom_service.routes.tokens._raw_api_token`` so a
    provisioned smoke token is indistinguishable in form from one created
    through ``POST /api/v1/tokens``.
    """
    return f"loom_api_{secrets.token_urlsafe(32)}"


def ensure_smoke_user_credential(
    db_url: str,
    *,
    username: str = _DEFAULT_USERNAME,
    team_name: str = _DEFAULT_TEAM_NAME,
    scopes: Sequence[str] = _DEFAULT_SCOPES,
    ttl_days: int | None = _DEFAULT_TTL_DAYS,
    revoke_prior: bool = True,
) -> SmokeCredential:
    """Idempotently ensure the smoke user/team, then mint a fresh
    user-owned API token for headless trajectory-smoke submission.

    ``db_url`` is a SQLAlchemy URL for the target service database (the
    one that backs ``/api/v1/trials``). User, team, quota and membership
    are get-or-create; the token is always freshly minted so the caller
    has a raw value to store. With ``revoke_prior`` (default) the smoke
    user's other unrevoked tokens are marked revoked first, so exactly
    one live credential exists after each run.

    Returns a :class:`SmokeCredential`; only the caller ever sees the raw
    token — persist it straight into the secret store without logging it.
    """
    if not scopes:
        raise ValueError("scopes must be non-empty")
    if "submit" not in scopes:
        raise ValueError("smoke credential requires the 'submit' scope")
    if ttl_days is not None and ttl_days <= 0:
        raise ValueError("ttl_days must be positive when set")

    username_normalized = normalize_username(username)
    engine = create_engine(db_url)
    session_local = sessionmaker(engine)
    now = datetime.now(UTC)
    expires_at = now + timedelta(days=ttl_days) if ttl_days is not None else None

    try:
        with session_local.begin() as s:
            # 1. User — get-or-create by normalized username. The smoke
            # principal is non-human: active, never a platform admin, no
            # password (it authenticates only via the minted API token).
            user_id = s.execute(
                select(User.id).where(
                    User.username_normalized == username_normalized,
                ),
            ).scalar_one_or_none()
            if user_id is None:
                user_id = uuid4()
                s.execute(insert(User).values(
                    id=user_id,
                    username=username,
                    username_normalized=username_normalized,
                    status="active",
                    is_platform_admin=False,
                ))

            # 2. Team — get-or-create by name, with its quota row.
            team_id = s.execute(
                select(Team.id).where(Team.name == team_name),
            ).scalar_one_or_none()
            if team_id is None:
                team_id = uuid4()
                s.execute(insert(Team).values(id=team_id, name=team_name))
                s.execute(insert(TeamQuota).values(team_id=team_id))

            # 3. Membership — the smoke user owns its team (get-or-create).
            has_membership = s.execute(
                select(TeamMembership.user_id).where(
                    TeamMembership.team_id == team_id,
                    TeamMembership.user_id == user_id,
                ),
            ).scalar_one_or_none()
            if has_membership is None:
                s.execute(insert(TeamMembership).values(
                    team_id=team_id,
                    user_id=user_id,
                    role="owner",
                ))

            # 4. Rotation — revoke the smoke user's other live tokens so
            # exactly one credential is valid after provisioning.
            rotated_prior = 0
            if revoke_prior:
                prior = s.execute(
                    select(Token.token_hash).where(
                        Token.created_by_user_id == user_id,
                        Token.revoked_at.is_(None),
                    ),
                ).scalars().all()
                rotated_prior = len(prior)
                if prior:
                    s.execute(
                        update(Token)
                        .where(Token.token_hash.in_(prior))
                        .values(revoked_at=now),
                    )

            # 5. Mint the fresh user-owned submit token.
            raw = _raw_api_token()
            token_hash = hashlib.sha256(raw.encode()).digest()
            s.execute(insert(Token).values(
                token_hash=token_hash,
                name=f"{username} headless smoke",
                type="team",
                scopes=list(scopes),
                team_id=team_id,
                created_by_user_id=user_id,
                issued_at=now,
                expires_at=expires_at,
            ))
    finally:
        engine.dispose()

    return SmokeCredential(
        raw_token=raw,
        token_hash_prefix=token_hash.hex()[:8],
        user_id=user_id,
        team_id=team_id,
        username=username,
        team_name=team_name,
        expires_at=expires_at,
        rotated_prior=rotated_prior,
    )
