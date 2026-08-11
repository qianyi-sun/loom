from __future__ import annotations

from datetime import UTC, datetime, timedelta
from uuid import UUID

import pytest

from loom.auth import AuthContext
from loom.db.schema import Team, TeamMembership, TeamQuota, Token, User, UserSession
from loom.personal_dev_environment import PersonalDevAccessBinding
from loom_service.dev_instance_access import (
    DevInstanceAccessError,
    access_binding_from_context,
    load_owner_access_snapshot,
    load_owner_access_snapshot_by_binding,
)

_NOW = datetime(2026, 8, 6, tzinfo=UTC)
_USER = UUID("00000000-0000-0000-0000-000000000001")
_TEAM = UUID("00000000-0000-0000-0000-000000000002")
_TOKEN_HASH = b"t" * 32
_SESSION_HASH = b"s" * 32


class _ScalarResult:
    def __init__(self, value):
        self.value = value

    def scalar_one_or_none(self):
        return self.value


class _Session:
    def __init__(self) -> None:
        self.user = User(
            id=_USER,
            email="alice@example.test",
            username="Alice",
            username_normalized="alice",
            display_name="Alice",
            password_hash="argon2-hash",
            password_set_at=_NOW,
            status="active",
            disabled_at=None,
            is_platform_admin=False,
            created_at=_NOW,
            last_login_at=_NOW,
        )
        self.team = Team(id=_TEAM, name="research", created_at=_NOW)
        self.membership = TeamMembership(
            team_id=_TEAM,
            user_id=_USER,
            role="owner",
            created_at=_NOW,
        )
        self.quota = TeamQuota(
            team_id=_TEAM,
            fair_share_weight=2.0,
            max_attempts_ceiling=5,
            in_flight_count=9,
            license_allowlist=["MIT"],
            taskset_max_count=10,
            taskset_max_storage_bytes=1000,
            allow_private_endpoints=True,
        )
        self.token = Token(
            token_hash=_TOKEN_HASH,
            name="dev-cli",
            type="team",
            scopes=["read:own", "submit"],
            team_id=_TEAM,
            created_by_user_id=_USER,
            created_by_actor="user:alice",
            issued_at=_NOW,
            expires_at=None,
            revoked_at=None,
        )
        self.browser_session = UserSession(
            session_hash=_SESSION_HASH,
            user_id=_USER,
            current_team_id=_TEAM,
            csrf_hash=b"c" * 32,
            issued_at=_NOW,
            expires_at=_NOW + timedelta(hours=1),
            revoked_at=None,
            last_seen_at=_NOW,
        )

    async def get(self, model, key):
        return {
            (User, _USER): self.user,
            (Team, _TEAM): self.team,
            (TeamQuota, _TEAM): self.quota,
            (Token, _TOKEN_HASH): self.token,
            (UserSession, _SESSION_HASH): self.browser_session,
        }.get((model, key))

    async def execute(self, _statement):
        return _ScalarResult(self.membership)


async def test_owner_bootstrap_copies_only_current_hash_and_resets_runtime_count() -> None:
    session = _Session()
    ctx = AuthContext(
        token_hash=_TOKEN_HASH,
        type="team",
        scopes=["read:own", "submit"],
        team_id=_TEAM,
        expires_at=None,
        user_id=_USER,
        auth_kind="bearer",
    )

    snapshot = await load_owner_access_snapshot(session, ctx)  # type: ignore[arg-type]

    assert snapshot.user_id == _USER
    assert snapshot.team_id == _TEAM
    assert snapshot.password_hash == "argon2-hash"
    assert snapshot.fair_share_weight == 2.0
    assert snapshot.bearer is not None
    assert snapshot.bearer.token_hash == _TOKEN_HASH
    assert snapshot.session is None


async def test_delayed_bootstrap_reloads_only_the_attempt_bound_credential() -> None:
    session = _Session()
    binding = PersonalDevAccessBinding(
        auth_kind="bearer",
        credential_hash=_TOKEN_HASH,
    )

    snapshot = await load_owner_access_snapshot_by_binding(
        session,  # type: ignore[arg-type]
        owner_user_id=_USER,
        owner_team_id=_TEAM,
        binding=binding,
        now=_NOW,
    )

    assert snapshot.bearer is not None
    assert snapshot.bearer.token_hash == _TOKEN_HASH
    assert snapshot.session is None

    session.token.revoked_at = _NOW
    with pytest.raises(DevInstanceAccessError, match="unavailable"):
        await load_owner_access_snapshot_by_binding(
            session,  # type: ignore[arg-type]
            owner_user_id=_USER,
            owner_team_id=_TEAM,
            binding=binding,
            now=_NOW,
        )


async def test_delayed_bootstrap_rejects_expired_session_and_identity_drift() -> None:
    session = _Session()
    binding = PersonalDevAccessBinding(
        auth_kind="session",
        credential_hash=_SESSION_HASH,
    )
    session.browser_session.expires_at = _NOW
    with pytest.raises(DevInstanceAccessError, match="unavailable"):
        await load_owner_access_snapshot_by_binding(
            session,  # type: ignore[arg-type]
            owner_user_id=_USER,
            owner_team_id=_TEAM,
            binding=binding,
            now=_NOW,
        )


def test_request_context_is_reduced_to_one_hash_only_binding() -> None:
    bearer = AuthContext(
        token_hash=_TOKEN_HASH,
        type="team",
        scopes=["submit"],
        team_id=_TEAM,
        expires_at=None,
        user_id=_USER,
    )
    browser = AuthContext(
        token_hash=b"",
        type="session",
        scopes=["submit"],
        team_id=_TEAM,
        expires_at=_NOW + timedelta(hours=1),
        user_id=_USER,
        session_hash=_SESSION_HASH,
        auth_kind="session",
    )

    assert access_binding_from_context(bearer) == PersonalDevAccessBinding(
        auth_kind="bearer",
        credential_hash=_TOKEN_HASH,
    )
    assert access_binding_from_context(browser) == PersonalDevAccessBinding(
        auth_kind="session",
        credential_hash=_SESSION_HASH,
    )
