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


# ---------------------------------------------------------------------------
# Batch-runner control-plane token
# ---------------------------------------------------------------------------

_BATCH_RUNNER_TOKEN_NAME = "batch-runner (deploy-provisioned)"


@dataclass(frozen=True)
class BatchRunnerToken:
    """The result of provisioning the batch-runner CP token."""

    raw_token: str
    token_hash_prefix: str
    expires_at: datetime | None
    rotated_prior: int


def ensure_batch_runner_token(
    db_url: str,
    *,
    ttl_days: int | None = _DEFAULT_TTL_DAYS,
    revoke_prior: bool = True,
) -> BatchRunnerToken:
    """Mint the batch-runner control-plane token loom-service uses to fan
    batches out to ``POST /trials`` on the control-plane.

    Mirrors the control-plane's ``POST /admin/batch-runner-tokens``
    (``loom_control_plane.routes.admin.issue_batch_runner_token``): a
    non-user ``worker`` token scoped to ``submit:batch``. A fresh or
    post-cutover DB has no valid one, so ``LOOM_SVC_BATCH_RUNNER_CP_TOKEN``
    (from ``loom-secrets/batch-runner-cp-token``) 401s and batches never
    dispatch. The deploy runs this against the target DB, writes the raw
    token into ``loom-secrets``, and restarts loom-service to pick it up —
    see ``docs/runbooks/deploy-staging-k3s.md``.

    Idempotent by rotation: prior deploy-provisioned batch-runner tokens
    (tagged by name) are revoked so exactly one is live after each run.
    """
    if ttl_days is not None and ttl_days <= 0:
        raise ValueError("ttl_days must be positive when set")

    engine = create_engine(db_url)
    now = datetime.now(UTC)
    expires_at = now + timedelta(days=ttl_days) if ttl_days is not None else None

    try:
        with sessionmaker(engine).begin() as s:
            rotated_prior = 0
            if revoke_prior:
                prior = s.execute(
                    select(Token.token_hash).where(
                        Token.name == _BATCH_RUNNER_TOKEN_NAME,
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
            raw = f"loom_br_{secrets.token_urlsafe(32)}"
            token_hash = hashlib.sha256(raw.encode()).digest()
            s.execute(insert(Token).values(
                token_hash=token_hash,
                name=_BATCH_RUNNER_TOKEN_NAME,
                type="worker",
                scopes=["submit:batch"],
                team_id=None,
                issued_at=now,
                expires_at=expires_at,
            ))
    finally:
        engine.dispose()

    return BatchRunnerToken(
        raw_token=raw,
        token_hash_prefix=token_hash.hex()[:8],
        expires_at=expires_at,
        rotated_prior=rotated_prior,
    )


# ---------------------------------------------------------------------------
# Dev/smoke worker token
# ---------------------------------------------------------------------------

_DEV_WORKER_TOKEN_NAME = "dev worker (smoke-provisioned)"

# Fixed plaintext that MUST equal the `worker-token` value emitted by
# `loom cluster bootstrap-secrets --smoke-defaults` (loom_config.bootstrap
# _SMOKE_DEFAULTS). Because that same value lands in `loom-secrets/worker-token`
# which the loom-worker pods carry, seeding a DB token with this exact plaintext
# lets workers authenticate at `/workers/register` with NO token mint, secret
# patch, or restart. A drift guard test asserts the two stay equal.
_DEV_WORKER_TOKEN = "smoke-worker-token"

# Matches the control-plane's `POST /admin/worker-tokens` scopes
# (loom_control_plane.routes.admin.issue_worker_token).
_DEV_WORKER_TOKEN_SCOPES = ["worker:claim", "worker:report", "worker:index"]


@dataclass(frozen=True)
class DevWorkerToken:
    """Result of seeding the dev/smoke worker token."""

    token_hash_prefix: str
    created: bool


def ensure_dev_worker_token(
    db_url: str,
    *,
    raw_token: str = _DEV_WORKER_TOKEN,
) -> DevWorkerToken:
    """Idempotently seed a worker token whose plaintext matches the
    `--smoke-defaults` `worker-token`, so the in-cluster loom-worker pods
    authenticate at `/workers/register` with no mint/patch/restart.

    LOCAL/DEV ONLY. Uses a fixed, well-known throwaway plaintext (the same
    value `bootstrap-secrets --smoke-defaults` already writes into
    `loom-secrets`). Never run this against a real environment — it would
    install a guessable worker credential. Writes directly to the target DB
    (run after migrations); get-or-create by token hash, so re-running is a
    no-op.
    """
    engine = create_engine(db_url)
    now = datetime.now(UTC)
    token_hash = hashlib.sha256(raw_token.encode()).digest()
    try:
        with sessionmaker(engine).begin() as s:
            existing = s.execute(
                select(Token.token_hash).where(
                    Token.token_hash == token_hash,
                    Token.revoked_at.is_(None),
                ),
            ).scalars().first()
            created = existing is None
            if created:
                s.execute(insert(Token).values(
                    token_hash=token_hash,
                    name=_DEV_WORKER_TOKEN_NAME,
                    type="worker",
                    scopes=list(_DEV_WORKER_TOKEN_SCOPES),
                    team_id=None,
                    issued_at=now,
                    expires_at=None,
                ))
    finally:
        engine.dispose()

    return DevWorkerToken(
        token_hash_prefix=token_hash.hex()[:8],
        created=created,
    )
