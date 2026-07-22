"""Bearer auth tests against a real Postgres via testcontainers.

Each test gets a fresh AsyncSession but shares the session-scoped container.
"""

from __future__ import annotations

import hashlib
from collections.abc import AsyncGenerator
from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

import pytest
from sqlalchemy import delete, insert, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from loom.db.schema import Team, Token, User
from loom_llm_gateway.auth import verify_bearer_token
from tests.integration.gateway_db import delete_teams_by_name_async


async def _insert_token(
    session: AsyncSession,
    *,
    raw: str,
    type_: str = "team",
    team_id: UUID | None = None,
    created_by_user_id: UUID | None = None,
    expires_in_sec: int = 3600,
    scopes: list[str] | None = None,
) -> None:
    h = hashlib.sha256(raw.encode()).digest()
    await session.execute(
        insert(Token).values(
            token_hash=h,
            type=type_,
            scopes=scopes or ["submit"],
            team_id=team_id,
            created_by_user_id=created_by_user_id,
            issued_at=datetime.now(UTC),
            expires_at=datetime.now(UTC) + timedelta(seconds=expires_in_sec),
        )
    )
    await session.commit()


@pytest.fixture
async def db_session(postgres_url: str) -> AsyncGenerator[AsyncSession, None]:
    engine = create_async_engine(postgres_url)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    async with session_factory() as session:
        try:
            yield session
        finally:
            # Per-test cleanup so tests don't see each other's tokens.
            await session.execute(delete(Token))
            await session.execute(
                delete(User).where(User.username_normalized.like("gateway-auth-%")),
            )
            await delete_teams_by_name_async(session, "t-%")
            await session.commit()
    await engine.dispose()


async def test_verify_valid_token(db_session: AsyncSession):
    team_id = uuid4()
    await db_session.execute(insert(Team).values(id=team_id, name=f"t-{team_id}"))
    raw = "loom_team_abc123"
    await _insert_token(db_session, raw=raw, type_="team", team_id=team_id)
    ctx = await verify_bearer_token(db_session, f"Bearer {raw}")
    assert ctx is not None
    assert ctx.team_id == team_id
    assert "submit" in ctx.scopes


async def test_verify_token_restores_created_by_user_id(db_session: AsyncSession):
    team_id = uuid4()
    user_id = uuid4()
    await db_session.execute(insert(Team).values(id=team_id, name=f"t-{team_id}"))
    await db_session.execute(
        insert(User).values(
            id=user_id,
            username="GatewayAuthUser",
            username_normalized="gateway-auth-user",
            status="active",
            is_platform_admin=False,
        )
    )
    raw = "loom_team_user_owned"
    await _insert_token(
        db_session,
        raw=raw,
        type_="team",
        team_id=team_id,
        created_by_user_id=user_id,
    )

    ctx = await verify_bearer_token(db_session, f"Bearer {raw}")

    assert ctx is not None
    assert ctx.team_id == team_id
    assert ctx.user_id == user_id


async def test_verify_token_debounces_last_seen_update(
    db_session: AsyncSession,
):
    raw = "loom_team_debounce"
    await _insert_token(db_session, raw=raw, type_="team")
    token_hash = hashlib.sha256(raw.encode()).digest()

    ctx = await verify_bearer_token(db_session, f"Bearer {raw}")
    assert ctx is not None
    first_seen = await db_session.scalar(
        select(Token.last_seen_at).where(Token.token_hash == token_hash),
    )
    assert first_seen is not None

    ctx = await verify_bearer_token(db_session, f"Bearer {raw}")
    assert ctx is not None
    second_seen = await db_session.scalar(
        select(Token.last_seen_at).where(Token.token_hash == token_hash),
    )

    assert second_seen == first_seen


async def test_readonly_probe_requires_explicit_safe_path_and_never_touches_usage(
    db_session: AsyncSession,
) -> None:
    team_id = uuid4()
    await db_session.execute(insert(Team).values(id=team_id, name=f"t-{team_id}"))
    raw = "loom_readonly_probe"
    await _insert_token(
        db_session,
        raw=raw,
        type_="readonly_probe",
        team_id=team_id,
        scopes=["read:own"],
    )
    token_hash = hashlib.sha256(raw.encode()).digest()

    assert await verify_bearer_token(db_session, f"Bearer {raw}") is None
    ctx = await verify_bearer_token(
        db_session,
        f"Bearer {raw}",
        allow_readonly_probe=True,
    )

    assert ctx is not None
    assert ctx.type == "readonly_probe"
    assert ctx.auth_kind == "readonly_probe"
    assert ctx.scopes == ["read:own"]
    assert (
        await db_session.scalar(
            select(Token.last_seen_at).where(Token.token_hash == token_hash),
        )
        is None
    )
    assert (
        await db_session.scalar(
            select(Token.last_used_at).where(Token.token_hash == token_hash),
        )
        is None
    )


@pytest.mark.parametrize(
    ("scopes", "created_by_user_id"),
    ((["read:own", "submit"], None), (["read:own"], UUID(int=1))),
)
async def test_readonly_probe_rejects_authority_drift(
    db_session: AsyncSession,
    scopes: list[str],
    created_by_user_id: UUID | None,
) -> None:
    team_id = uuid4()
    await db_session.execute(insert(Team).values(id=team_id, name=f"t-{team_id}"))
    if created_by_user_id is not None:
        await db_session.execute(
            insert(User).values(
                id=created_by_user_id,
                username=f"GatewayAuth{created_by_user_id.hex}",
                username_normalized=f"gateway-auth-{created_by_user_id.hex}",
                status="active",
                is_platform_admin=False,
            )
        )
    raw = f"loom_readonly_{uuid4().hex}"
    await _insert_token(
        db_session,
        raw=raw,
        type_="readonly_probe",
        team_id=team_id,
        created_by_user_id=created_by_user_id,
        scopes=scopes,
    )

    assert (
        await verify_bearer_token(
            db_session,
            f"Bearer {raw}",
            allow_readonly_probe=True,
        )
        is None
    )


async def test_family_orchestrator_is_default_deny_and_explicit_allow(
    db_session: AsyncSession,
) -> None:
    raw = "loom_family_orchestrator_exact"
    await _insert_token(
        db_session,
        raw=raw,
        type_="family_orchestrator",
        scopes=["family:evolve"],
        team_id=None,
    )

    assert await verify_bearer_token(db_session, f"Bearer {raw}") is None
    ctx = await verify_bearer_token(
        db_session,
        f"Bearer {raw}",
        allow_family_orchestrator=True,
    )

    assert ctx is not None
    assert ctx.type == "family_orchestrator"
    assert ctx.scopes == ["family:evolve"]
    assert ctx.team_id is None
    assert ctx.user_id is None


@pytest.mark.parametrize(
    ("scopes", "bind_team", "bind_user"),
    (
        (["family:evolve", "submit"], False, False),
        (["family:evolve"], True, False),
        (["family:evolve"], False, True),
    ),
)
async def test_family_orchestrator_rejects_authority_drift(
    db_session: AsyncSession,
    scopes: list[str],
    bind_team: bool,
    bind_user: bool,
) -> None:
    team_id = uuid4() if bind_team else None
    user_id = uuid4() if bind_user else None
    if team_id is not None:
        await db_session.execute(insert(Team).values(id=team_id, name=f"t-{team_id}"))
    if user_id is not None:
        await db_session.execute(
            insert(User).values(
                id=user_id,
                username=f"GatewayAuth{user_id.hex}",
                username_normalized=f"gateway-auth-{user_id.hex}",
                status="active",
                is_platform_admin=False,
            )
        )
    raw = f"loom_family_orchestrator_{uuid4().hex}"
    await _insert_token(
        db_session,
        raw=raw,
        type_="family_orchestrator",
        scopes=scopes,
        team_id=team_id,
        created_by_user_id=user_id,
    )

    assert (
        await verify_bearer_token(
            db_session,
            f"Bearer {raw}",
            allow_family_orchestrator=True,
        )
        is None
    )


async def test_missing_bearer_returns_none(db_session: AsyncSession):
    assert await verify_bearer_token(db_session, None) is None
    assert await verify_bearer_token(db_session, "") is None


async def test_non_bearer_header_returns_none(db_session: AsyncSession):
    """Other auth schemes (Basic, Digest) must be rejected, not crash."""
    assert await verify_bearer_token(db_session, "Basic foo:bar") is None


async def test_unknown_token_returns_none(db_session: AsyncSession):
    assert await verify_bearer_token(db_session, "Bearer unknown") is None


async def test_expired_token_rejected(db_session: AsyncSession):
    await _insert_token(db_session, raw="loom_team_exp", expires_in_sec=-1)
    assert await verify_bearer_token(db_session, "Bearer loom_team_exp") is None


async def test_revoked_token_rejected(db_session: AsyncSession):
    raw = "loom_team_rev"
    h = hashlib.sha256(raw.encode()).digest()
    await db_session.execute(
        insert(Token).values(
            token_hash=h,
            type="team",
            scopes=["submit"],
            team_id=None,
            issued_at=datetime.now(UTC),
            expires_at=datetime.now(UTC) + timedelta(hours=1),
            revoked_at=datetime.now(UTC),
        )
    )
    await db_session.commit()
    assert await verify_bearer_token(db_session, f"Bearer {raw}") is None
