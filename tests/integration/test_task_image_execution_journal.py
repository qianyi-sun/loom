"""Durable execution audit constraints, not signed issuance or live activation."""

from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine


async def test_execution_journal_has_durable_cross_revision_one_use_fence(
    isolated_migration_postgres_url,
):
    engine = create_async_engine(isolated_migration_postgres_url)
    try:
        async with engine.connect() as connection:
            tables = set((await connection.execute(text(
                "SELECT tablename FROM pg_tables WHERE schemaname='public' "
                "AND tablename IN ('task_image_execution_grants','task_image_execution_starts')"
            ))).scalars())
            assert tables == {"task_image_execution_grants", "task_image_execution_starts"}
            primary = await connection.scalar(text(
                "SELECT pg_get_constraintdef(oid) FROM pg_constraint "
                "WHERE conrelid='task_image_execution_starts'::regclass AND contype='p'"
            ))
            assert primary == "PRIMARY KEY (claim_id)"
            definitions = set((await connection.execute(text(
                "SELECT pg_get_constraintdef(oid) FROM pg_constraint "
                "WHERE conrelid='task_image_execution_starts'::regclass AND contype='f'"
            ))).scalars())
            assert any("(grant_id, revision, claim_id)" in item for item in definitions)
    finally:
        await engine.dispose()


async def test_execution_journal_truncation_is_forbidden_even_when_empty(
    isolated_migration_postgres_url,
):
    from sqlalchemy.exc import DBAPIError

    engine = create_async_engine(isolated_migration_postgres_url)
    try:
        for table in ("task_image_execution_starts", "task_image_execution_grants"):
            async with engine.connect() as connection:
                exists = await connection.scalar(text("SELECT to_regclass(:name)"), {"name": table})
                assert exists is not None, "execution journal missing"
            async with engine.connect() as connection:
                try:
                    await connection.execute(text(f"TRUNCATE TABLE {table} CASCADE"))
                except DBAPIError as error:
                    assert "execution journal is immutable" in str(error)
                else:
                    raise AssertionError("audit truncation was accepted")
    finally:
        await engine.dispose()
