import asyncio
from uuid import uuid4

import pytest
from alembic import command
from sqlalchemy import create_engine, text
from sqlalchemy.exc import DBAPIError

from tests.integration.test_task_image_registry_credential_migration import _config


def test_upgrade_preserves_legacy_ready_map_without_inventing_binding(
    isolated_migration_postgres_url,
):
    config = _config(isolated_migration_postgres_url)
    command.downgrade(config, "0134")
    engine = create_engine(isolated_migration_postgres_url)
    try:
        with engine.begin() as connection:
            connection.execute(
                text("""INSERT INTO task_image_materializations
                (id, materialization_key, task_id, task_checksum, cpu_arch, task_config,
                 state, registry_images, registry_image_history, ready_at)
                VALUES (:id, repeat('a', 64), 'legacy', repeat('b', 64), 'arm64', '{}',
                        'ready', '{"task":"legacy@sha256:abc"}',
                        '[{"registry_images":{"task":"old"}}]', now())"""),
                {"id": uuid4()},
            )
            before = connection.execute(
                text("""SELECT state, registry_images,
                registry_image_history, ready_at FROM task_image_materializations""")
            ).one()
        command.upgrade(config, "0135")
        with engine.connect() as connection:
            assert (
                connection.execute(
                    text("""SELECT state, registry_images,
                registry_image_history, ready_at FROM task_image_materializations""")
                ).one()
                == before
            )
            assert (
                connection.execute(
                    text("""SELECT ready_publication_operation_id
                FROM task_image_materializations""")
                ).scalar_one()
                is None
            )
        command.downgrade(config, "0134")
        with engine.connect() as connection:
            assert (
                connection.execute(
                    text("""SELECT state, registry_images,
                registry_image_history, ready_at FROM task_image_materializations""")
                ).one()
                == before
            )
    finally:
        engine.dispose()


@pytest.mark.parametrize(
    "table", ["task_image_materializations", "task_image_registry_credentials", "trials"]
)
async def test_upgrade_fails_fast_when_existing_authority_tables_are_busy(
    isolated_migration_postgres_url, table
):
    config = _config(isolated_migration_postgres_url)
    command.downgrade(config, "0134")
    engine = create_engine(isolated_migration_postgres_url)
    try:
        with engine.connect() as blocker:
            # Same table lock as an ordinary INSERT/UPDATE, even on empty tables.
            blocker.execute(text(f"LOCK TABLE {table} IN ROW EXCLUSIVE MODE"))
            blocker_pid = blocker.execute(text("SELECT pg_backend_pid()")).scalar_one()
            migration = asyncio.create_task(asyncio.to_thread(command.upgrade, config, "0135"))
            try:
                async with asyncio.timeout(5):
                    while not migration.done():
                        blocker.execute(text("SELECT pg_stat_clear_snapshot()"))
                        assert not blocker.execute(
                            text("""SELECT EXISTS (
                            SELECT 1 FROM pg_stat_activity
                            WHERE :blocker = ANY(pg_blocking_pids(pid)))"""),
                            {"blocker": blocker_pid},
                        ).scalar_one(), "upgrade blocks ordinary authority work"
                        await asyncio.sleep(0.01)
                with pytest.raises(DBAPIError) as rejected:
                    await migration
                assert rejected.value.orig.sqlstate == "55P03"
            finally:
                blocker.rollback()
                await asyncio.gather(migration, return_exceptions=True)
        with engine.connect() as connection:
            assert (
                connection.execute(text("SELECT version_num FROM alembic_version")).scalar_one()
                == "0134"
            )
    finally:
        engine.dispose()
