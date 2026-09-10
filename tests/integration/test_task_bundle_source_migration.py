"""Source recovery authority survives SQL mutation and migration rollback."""

import pytest
from alembic import command
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from loom.db.schema import Base
from tests.integration.test_task_bundle_source_journal import _receipts, _spec, _upload
from tests.integration.test_task_image_registry_credential_migration import _config

TABLES = (
    "task_bundle_sources",
    "task_bundle_source_incarnations",
    "task_bundle_source_writes",
    "task_bundle_source_versions",
    "task_bundle_source_references",
)


def test_empty_source_journal_roundtrip_and_orm_schema_parity(isolated_migration_postgres_url):
    config = _config(isolated_migration_postgres_url)
    engine = create_engine(isolated_migration_postgres_url)
    try:
        command.downgrade(config, "0136")
        assert not set(TABLES) & set(inspect(engine).get_table_names())
        command.upgrade(config, "head")
        inspector = inspect(engine)
        with engine.connect() as connection:
            assert connection.scalar(text("SELECT version_num FROM alembic_version")) == "0140"
            for table in TABLES:
                assert connection.scalar(text(f"SELECT count(*) FROM {table}")) == 0
                model = Base.metadata.tables[table]
                observed = inspector.get_columns(table)
                assert {col["name"] for col in observed} == set(model.columns.keys())
                assert {col["name"]: col["nullable"] for col in observed} == {
                    col.name: col.nullable for col in model.columns
                }
                assert set(inspector.get_pk_constraint(table)["constrained_columns"]) == {
                    col.name for col in model.primary_key
                }
                assert {item["name"] for item in inspector.get_check_constraints(table)} == {
                    item.name
                    for item in model.constraints
                    if item.__class__.__name__ == "CheckConstraint"
                }
    finally:
        engine.dispose()


async def test_populated_journal_cannot_lose_tombstones_or_immutable_facts(
    isolated_migration_postgres_url, tmp_path
):
    engine = create_async_engine(isolated_migration_postgres_url)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    try:
        spec = _spec(tmp_path)
        ticket = await _upload(factory, spec)
        await _receipts(factory, ticket)
        with pytest.raises(DBAPIError, match="recovery tombstones"):
            command.downgrade(_config(isolated_migration_postgres_url), "0136")
        async with factory() as session:
            assert await session.scalar(text("SELECT version_num FROM alembic_version")) == "0140"
        for table in reversed(TABLES[:-1]):
            for sql in (f"DELETE FROM {table}", f"TRUNCATE {table} CASCADE"):
                async with factory() as session:
                    with pytest.raises(DBAPIError, match=r"journal.*immutable"):
                        await session.execute(text(sql))
                    await session.rollback()
        for sql in (
            "UPDATE task_bundle_sources SET spec_json='{}'::jsonb",
            "UPDATE task_bundle_source_incarnations SET expires_at=expires_at+interval '1 hour'",
            "UPDATE task_bundle_source_writes SET content_sha256=repeat('e',64)",
            "UPDATE task_bundle_source_versions SET version_id='replacement'",
        ):
            async with factory() as session:
                with pytest.raises(DBAPIError, match=r"journal.*immutable"):
                    await session.execute(text(sql))
                await session.rollback()
    finally:
        await engine.dispose()
