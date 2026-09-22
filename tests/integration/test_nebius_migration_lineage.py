"""Reject ambiguous isolated-branch revision numbers before changing a database."""

from __future__ import annotations

from collections.abc import Iterator

import pytest
from database.migrations.env import _assert_compatible_migration_lineage
from sqlalchemy import create_engine, text
from testcontainers.postgres import PostgresContainer


@pytest.fixture
def postgres_url() -> Iterator[str]:
    with PostgresContainer("postgres:16") as postgres:
        yield postgres.get_connection_url().replace(
            "postgresql+psycopg2://", "postgresql+psycopg://"
        )


@pytest.mark.parametrize("revision", ["0133", "0134", "0135", "0136"])
def test_isolated_nebius_revision_cannot_be_treated_as_dev(
    postgres_url: str, revision: str
) -> None:
    engine = create_engine(postgres_url)
    try:
        with engine.begin() as connection:
            connection.execute(text("CREATE TABLE alembic_version (version_num varchar(32))"))
            connection.execute(
                text("INSERT INTO alembic_version VALUES (:revision)"), {"revision": revision}
            )
        with engine.connect() as connection:
            with pytest.raises(RuntimeError, match="isolated Nebius migration lineage"):
                _assert_compatible_migration_lineage(connection)
            assert (
                connection.execute(text("SELECT version_num FROM alembic_version")).scalar()
                == revision
            )
        with engine.begin() as connection:
            connection.execute(text("CREATE TABLE gateway_dispatch_receipts (id uuid)"))
        with engine.connect() as connection:
            _assert_compatible_migration_lineage(connection)
    finally:
        with engine.begin() as connection:
            connection.execute(text("DROP TABLE IF EXISTS gateway_dispatch_receipts"))
            connection.execute(text("DROP TABLE IF EXISTS alembic_version"))
        engine.dispose()
