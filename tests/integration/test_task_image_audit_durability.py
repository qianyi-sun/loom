"""Credential-first cleanup evidence survives ordinary SQL and rollback."""

import asyncio

import pytest
from alembic import command
from sqlalchemy import create_engine, select, text
from sqlalchemy.exc import DBAPIError

from loom.db.schema import TaskImagePublicationCandidate, TaskImageRegistryCredentialGeneration
from tests.integration.test_task_image_candidate_v2 import _prepared, _record
from tests.integration.test_task_image_publication_jobs import (
    registry_authority_session as registry_authority_session,
)
from tests.integration.test_task_image_registry_credential_migration import (
    _candidate_values,
    _config,
    _credential_values,
    _insert_attempt_prerequisites,
    _insert_candidate,
    _insert_credential,
    _insert_exchanged_projection,
)
from tests.integration.test_task_image_registry_credentials import (
    registry_issuer as registry_issuer,
)


@pytest.mark.parametrize("candidate", [False, True], ids=["first-credential", "candidate"])
async def test_issued_audit_cannot_be_changed_or_removed(
    registry_authority_session, registry_issuer, candidate
):
    async with registry_authority_session() as session:
        authorization, _, request, _ = await _prepared(session, registry_issuer)
        if candidate:
            await _record(session, authorization, request)
        else:
            assert await session.scalar(select(TaskImagePublicationCandidate)) is None
        await session.commit()
        table = (
            TaskImagePublicationCandidate.__tablename__
            if candidate
            else TaskImageRegistryCredentialGeneration.__tablename__
        )
        before = (await session.execute(text(f"SELECT * FROM {table}"))).one()
        await session.rollback()
        for sql in (
            f"UPDATE {table} SET response_sha256 = repeat('e', 64)",
            f"UPDATE {table} SET response_sha256 = response_sha256",
            f"DELETE FROM {table}",
            f"TRUNCATE {table} CASCADE",
        ):
            with pytest.raises(DBAPIError, match="task-image registry audit is immutable"):
                await session.execute(text(sql))
            await session.rollback()
            assert (await session.execute(text(f"SELECT * FROM {table}"))).one() == before
            await session.rollback()


def test_upgrade_protects_existing_audit_without_rewriting(isolated_migration_postgres_url):
    config = _config(isolated_migration_postgres_url)
    command.downgrade(config, "0128")
    engine = create_engine(isolated_migration_postgres_url)
    tables = ("task_image_registry_credentials", "task_image_publication_candidates")
    try:
        with engine.begin() as connection:
            _insert_exchanged_projection(connection, authority_version=2)
        command.upgrade(config, "0130")
        with engine.begin() as connection:
            _insert_attempt_prerequisites(connection)
        command.upgrade(config, "0134")
        with engine.begin() as connection:
            _insert_credential(connection, _credential_values())
            _insert_candidate(connection, _candidate_values())
            before = {
                table: connection.execute(text(f"SELECT * FROM {table}")).one() for table in tables
            }
        command.upgrade(config, "0135")
        with engine.connect() as connection:
            for table in tables:
                assert connection.execute(text(f"SELECT * FROM {table}")).one() == before[table]
        for table in tables:
            with pytest.raises(DBAPIError, match="task-image registry audit is immutable"):
                with engine.begin() as connection:
                    connection.execute(text(f"DELETE FROM {table}"))
    finally:
        engine.dispose()


async def test_downgrade_cannot_remove_first_credential_protection(
    isolated_migration_postgres_url, registry_authority_session, registry_issuer
):
    async with registry_authority_session() as session:
        await _prepared(session, registry_issuer)
        assert await session.scalar(select(TaskImagePublicationCandidate)) is None
        await session.commit()
        before = (
            await session.execute(text("SELECT * FROM task_image_registry_credentials"))
        ).one()
        await session.rollback()
        with pytest.raises(DBAPIError, match="publication authority cannot be discarded"):
            command.downgrade(_config(isolated_migration_postgres_url), "0134")
        assert await session.scalar(text("SELECT version_num FROM alembic_version")) == "0140"
        assert (
            await session.execute(text("SELECT * FROM task_image_registry_credentials"))
        ).one() == before
        await session.rollback()
        with pytest.raises(DBAPIError, match="task-image registry audit is immutable"):
            await session.execute(text("DELETE FROM task_image_registry_credentials"))
        await session.rollback()


async def test_busy_audit_refuses_downgrade_without_blocking_publication(
    isolated_migration_postgres_url, registry_authority_session, registry_issuer
):
    async with registry_authority_session() as session:
        await _prepared(session, registry_issuer)
        await session.commit()
        # Ordinary submission already reads/locks credentials before jobs.
        await session.execute(
            text("SELECT credential_id FROM task_image_registry_credentials FOR SHARE")
        )
        blocker_pid = await session.scalar(text("SELECT pg_backend_pid()"))
        task = asyncio.create_task(
            asyncio.to_thread(command.downgrade, _config(isolated_migration_postgres_url), "0134")
        )
        try:
            async with asyncio.timeout(5):
                while not task.done():
                    # Statistics snapshots are transaction-cached; observe the
                    # newly connected migration backend on each poll.
                    await session.execute(text("SELECT pg_stat_clear_snapshot()"))
                    waiting = await session.scalar(
                        text(
                            "SELECT EXISTS (SELECT 1 FROM pg_stat_activity "
                            "WHERE :blocker = ANY(pg_blocking_pids(pid)))"
                        ),
                        {"blocker": blocker_pid},
                    )
                    if waiting:
                        pytest.fail("downgrade waits while retaining locks needed by publication")
                    await asyncio.sleep(0.01)
            with pytest.raises(DBAPIError) as rejected:
                await task
            assert rejected.value.orig.sqlstate == "55P03"  # lock_not_available
            # Migration rollback released its earlier exclusive table locks.
            await session.execute(
                text("LOCK TABLE task_image_publication_jobs IN ROW SHARE MODE NOWAIT")
            )
            assert await session.scalar(text("SELECT version_num FROM alembic_version")) == "0140"
        finally:
            await session.rollback()
            await asyncio.gather(task, return_exceptions=True)
