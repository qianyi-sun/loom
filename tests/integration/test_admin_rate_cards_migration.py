"""Migration 0005 grants `admin:rate_cards` to existing `admin:tokens`
holders and is idempotent (Plan 17 Task 5)."""

from __future__ import annotations

import hashlib
from collections.abc import Iterator
from datetime import UTC, datetime

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine, delete, insert, select
from sqlalchemy.orm import sessionmaker

from loom.db.schema import Token


def _alembic_cfg(postgres_url: str) -> Config:
    cfg = Config("migrations/alembic.ini")
    cfg.set_main_option("sqlalchemy.url", postgres_url)
    return cfg


@pytest.fixture
def seeded_tokens(postgres_url: str) -> Iterator[None]:
    """Roll back to revision 0004 (pre-Plan-17), seed tokens, yield.
    Cleanup runs `upgrade head` so subsequent tests in the session
    see the latest schema."""
    cfg = _alembic_cfg(postgres_url)
    command.downgrade(cfg, "0004")
    engine = create_engine(postgres_url)
    sl = sessionmaker(engine)
    with sl() as s:
        s.execute(insert(Token).values(
            token_hash=hashlib.sha256(b"a").digest(),
            type="admin", scopes=["admin:tokens"], team_id=None,
            issued_at=datetime.now(UTC),
        ))
        s.execute(insert(Token).values(
            token_hash=hashlib.sha256(b"b").digest(),
            type="admin",
            scopes=["admin:tokens", "admin:rate_cards"],
            team_id=None, issued_at=datetime.now(UTC),
        ))
        s.execute(insert(Token).values(
            token_hash=hashlib.sha256(b"c").digest(),
            type="team", scopes=["read:own"], team_id=None,
            issued_at=datetime.now(UTC),
        ))
        s.commit()
    try:
        yield
    finally:
        with sl() as s:
            s.execute(delete(Token))
            s.commit()
        engine.dispose()
        command.upgrade(cfg, "head")


def test_migration_grants_new_scope(
    postgres_url: str, seeded_tokens: None,
) -> None:
    cfg = _alembic_cfg(postgres_url)
    command.upgrade(cfg, "0005")
    engine = create_engine(postgres_url)
    sl = sessionmaker(engine)
    with sl() as s:
        rows = s.execute(select(Token)).scalars().all()
    by_hash = {r.token_hash: list(r.scopes) for r in rows}
    a = hashlib.sha256(b"a").digest()
    b = hashlib.sha256(b"b").digest()
    c = hashlib.sha256(b"c").digest()
    assert "admin:rate_cards" in by_hash[a]
    # Idempotent — token b already had the scope; not duplicated.
    assert by_hash[b].count("admin:rate_cards") == 1
    # Non-admin tokens untouched.
    assert "admin:rate_cards" not in by_hash[c]
    engine.dispose()


def test_migration_idempotent(
    postgres_url: str, seeded_tokens: None,
) -> None:
    cfg = _alembic_cfg(postgres_url)
    command.upgrade(cfg, "0005")
    # Manually re-run the data migration. Alembic refuses to call
    # upgrade twice on the same head, so invoke the SQL directly.
    engine = create_engine(postgres_url)
    with engine.begin() as conn:
        conn.exec_driver_sql(
            "UPDATE tokens SET scopes = array_append(scopes, 'admin:rate_cards') "
            "WHERE 'admin:tokens' = ANY(scopes) "
            "AND NOT ('admin:rate_cards' = ANY(scopes))",
        )
    sl = sessionmaker(engine)
    with sl() as s:
        rows = s.execute(select(Token)).scalars().all()
    for r in rows:
        if "admin:tokens" in r.scopes:
            assert r.scopes.count("admin:rate_cards") == 1
    engine.dispose()


def test_migration_downgrade_removes_scope(
    postgres_url: str, seeded_tokens: None,
) -> None:
    cfg = _alembic_cfg(postgres_url)
    command.upgrade(cfg, "0005")
    command.downgrade(cfg, "0004")
    engine = create_engine(postgres_url)
    sl = sessionmaker(engine)
    with sl() as s:
        rows = s.execute(select(Token)).scalars().all()
    for r in rows:
        assert "admin:rate_cards" not in r.scopes
    engine.dispose()
