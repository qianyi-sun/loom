import asyncio
from uuid import uuid4

import pytest
from alembic import command
from sqlalchemy import create_engine, text
from sqlalchemy.exc import DBAPIError, IntegrityError

from tests.integration.test_task_image_registry_credential_migration import _config


def test_terminal_trial_guard_upgrade_preserves_rows_and_inactive_downgrade(
    isolated_migration_postgres_url,
):
    config = _config(isolated_migration_postgres_url)
    command.downgrade(config, "0134")
    engine = create_engine(isolated_migration_postgres_url)
    trial_id, team_id = uuid4(), uuid4()
    task_id = f"terminal-migration/{trial_id}"
    try:
        with engine.begin() as connection:
            connection.execute(
                text("INSERT INTO teams(id,name) VALUES (:id,:name)"),
                {"id": team_id, "name": str(team_id)},
            )
            connection.execute(
                text("INSERT INTO tasks(id,checksum,config) VALUES (:id,repeat('a',64),'{}')"),
                {"id": task_id},
            )
            connection.execute(
                text("""INSERT INTO trials
                (id,team_id,task_id,config,requires_caps,state,result,finished_at)
                VALUES (:id,:team,:task,'{}','{}','failed','{}',now())"""),
                {"id": trial_id, "team": team_id, "task": task_id},
            )
            before = connection.execute(
                text("SELECT to_jsonb(t) FROM trials t WHERE id=:id"), {"id": trial_id}
            ).scalar_one()
        command.upgrade(config, "0135")
        with engine.begin() as connection:
            assert (
                connection.execute(
                    text("SELECT to_jsonb(t) FROM trials t WHERE id=:id"), {"id": trial_id}
                ).scalar_one()
                == before
            )
            with pytest.raises(IntegrityError) as rejected:
                with connection.begin_nested():
                    connection.execute(
                        text("UPDATE trials SET state='queued' WHERE id=:id"), {"id": trial_id}
                    )
            assert rejected.value.orig.diag.constraint_name == "trials_terminal_state_monotonic"
            connection.execute(
                text("UPDATE trials SET failure_message='late metadata' WHERE id=:id"),
                {"id": trial_id},
            )
        command.downgrade(config, "0134")
        with engine.begin() as connection:
            assert (
                connection.execute(
                    text("SELECT state FROM trials WHERE id=:id"), {"id": trial_id}
                ).scalar_one()
                == "failed"
            )
            connection.execute(
                text("UPDATE trials SET state='queued' WHERE id=:id"), {"id": trial_id}
            )
        command.upgrade(config, "0135")
        with engine.connect() as connection:
            assert (
                connection.execute(
                    text("SELECT state FROM trials WHERE id=:id"), {"id": trial_id}
                ).scalar_one()
                == "queued"
            )
    finally:
        engine.dispose()


async def test_terminal_trial_guard_downgrade_fails_fast_on_busy_trials(isolated_migration_postgres_url):
    config = _config(isolated_migration_postgres_url)
    engine = create_engine(isolated_migration_postgres_url)
    try:
        with engine.connect() as blocker:
            blocker.execute(text("LOCK TABLE trials IN ROW EXCLUSIVE MODE"))
            blocker_pid = blocker.execute(text("SELECT pg_backend_pid()")).scalar_one()
            # Baseline0135 does not touch trials on downgrade, so it wrongly
            # succeeds. New preflight must reject, not wait on this writer.
            migration = asyncio.create_task(asyncio.to_thread(command.downgrade, config, "0134"))
            try:
                async with asyncio.timeout(5):
                    while not migration.done():
                        blocker.execute(text("SELECT pg_stat_clear_snapshot()"))
                        assert not blocker.execute(
                            text("""SELECT EXISTS (
                            SELECT 1 FROM pg_stat_activity
                            WHERE :blocker = ANY(pg_blocking_pids(pid)))"""),
                            {"blocker": blocker_pid},
                        ).scalar_one(), "downgrade blocks ordinary Trial work"
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
                == "0135"
            )
    finally:
        engine.dispose()
