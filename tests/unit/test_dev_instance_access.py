from __future__ import annotations

from datetime import UTC, datetime
from uuid import UUID

from loom.auth import AuthContext
from loom.db.schema import Team, TeamMembership, TeamQuota, Token, User
from loom_service.dev_instance_access import load_owner_access_snapshot

_NOW = datetime(2026, 8, 6, tzinfo=UTC)
_USER = UUID("00000000-0000-0000-0000-000000000001")
_TEAM = UUID("00000000-0000-0000-0000-000000000002")
_TOKEN_HASH = b"t" * 32


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

    async def get(self, model, key):
        return {
            (User, _USER): self.user,
            (Team, _TEAM): self.team,
            (TeamQuota, _TEAM): self.quota,
            (Token, _TOKEN_HASH): self.token,
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
