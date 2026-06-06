"""Bearer auth tests against a real Postgres via testcontainers.

Each test gets a fresh AsyncSession but shares the session-scoped container.
"""

from __future__ import annotations

import hashlib
from collections.abc import AsyncGenerator
from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

import pytest
from sqlalchemy import delete, insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from loom.db.schema import Team, Token
from loom_llm_gateway.auth import verify_bearer_token


async def _insert_token(
    session: AsyncSession,
    *,
    raw: str,
    type_: str = "team",
    team_id: UUID | None = None,
    expires_in_sec: int = 3600,
) -> None:
    h = hashlib.sha256(raw.encode()).digest()
    await session.execute(insert(Token).values(
        token_hash=h, type=type_, scopes=["submit"], team_id=team_id,
        issued_at=datetime.now(UTC),
        expires_at=datetime.now(UTC) + timedelta(seconds=expires_in_sec),
    ))
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
            await session.execute(delete(Team))
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
    await db_session.execute(insert(Token).values(
        token_hash=h, type="team", scopes=["submit"], team_id=None,
        issued_at=datetime.now(UTC),
        expires_at=datetime.now(UTC) + timedelta(hours=1),
        revoked_at=datetime.now(UTC),
    ))
    await db_session.commit()
    assert await verify_bearer_token(db_session, f"Bearer {raw}") is None
