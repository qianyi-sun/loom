"""Migration 0024 revokes legacy database-backed admin tokens."""

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
def seeded_admin_tokens(postgres_url: str) -> Iterator[None]:
    cfg = _alembic_cfg(postgres_url)
    command.downgrade(cfg, "0023")
    engine = create_engine(postgres_url)
    sl = sessionmaker(engine)
    with sl() as s:
        s.execute(insert(Token).values(
            token_hash=hashlib.sha256(b"legacy-admin").digest(),
            type="admin",
            scopes=["admin:tokens", "admin:rate_cards"],
            team_id=None,
            issued_at=datetime.now(UTC),
            expires_at=None,
            revoked_at=None,
        ))
        s.execute(insert(Token).values(
            token_hash=hashlib.sha256(b"team-token").digest(),
            type="team",
            scopes=["read:own"],
            team_id=None,
            issued_at=datetime.now(UTC),
            expires_at=None,
            revoked_at=None,
        ))
        s.execute(insert(Token).values(
            token_hash=hashlib.sha256(b"team-admin-scope").digest(),
            type="team",
            scopes=["read:own", "admin:tokens"],
            team_id=None,
            issued_at=datetime.now(UTC),
            expires_at=None,
            revoked_at=None,
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


def test_migration_revokes_legacy_db_admin_tokens(
    postgres_url: str,
    seeded_admin_tokens: None,
) -> None:
    cfg = _alembic_cfg(postgres_url)
    command.upgrade(cfg, "0024")

    engine = create_engine(postgres_url)
    sl = sessionmaker(engine)
    with sl() as s:
        rows = s.execute(select(Token)).scalars().all()
    by_hash = {row.token_hash: row for row in rows}

    assert by_hash[hashlib.sha256(b"legacy-admin").digest()].revoked_at is not None
    assert by_hash[hashlib.sha256(b"team-admin-scope").digest()].revoked_at is not None
    assert by_hash[hashlib.sha256(b"team-token").digest()].revoked_at is None
    engine.dispose()
