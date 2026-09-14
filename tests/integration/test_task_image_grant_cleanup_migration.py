from __future__ import annotations

import asyncio
from datetime import timedelta

import pytest
from alembic import command
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError, IntegrityError
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from loom.db.schema import TaskImageBuildGrant
from loom_control_plane.task_image_build_grants import (
    begin_task_image_build_submission,
    issue_task_image_build_grant,
    reconcile_task_image_build_cleanup,
    reconcile_task_image_build_submission,
    record_task_image_build_release,
)
from tests.integration.test_task_image_build_grant_store import _NOW, _grant, _inventory
from tests.integration.test_task_image_registry_credential_migration import _config


async def test_cleanup_migration_preserves_released_grant_and_refuses_lossy_downgrade(
    isolated_migration_postgres_url: str,
) -> None:
    config = _config(isolated_migration_postgres_url)
    await asyncio.to_thread(command.downgrade, config, "0142")
    engine = create_async_engine(isolated_migration_postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    grant = _grant(expires_at=_NOW + timedelta(seconds=10))
    try:
        async with sessions() as session:
            await issue_task_image_build_grant(
                session, environment="staging", grant=grant, ambiguity_settle_seconds=1, now=_NOW
            )
            await begin_task_image_build_submission(session, grant_id=grant.grant_id, now=_NOW)
            await reconcile_task_image_build_submission(
                session,
                grant_id=grant.grant_id,
                inventory=_inventory(grant, job_id="12345", state="pending", held=True),
                now=_NOW,
            )
            await record_task_image_build_release(
                session, grant_id=grant.grant_id, job_id="12345", now=_NOW
            )
            await session.commit()
            before = (await session.execute(text("SELECT * FROM task_image_build_grants"))).one()
            with pytest.raises(IntegrityError, match="state_fields_check"):
                async with session.begin_nested():
                    await session.execute(
                        text("""UPDATE task_image_build_grants
                        SET state='revoked', revoked_at=grant_expires_at,
                        revoke_reason='grant_authority_expired'""")
                    )
            await session.rollback()
        await asyncio.to_thread(command.upgrade, config, "0143")
        async with sessions() as session:
            assert (
                await session.execute(text("SELECT * FROM task_image_build_grants"))
            ).one() == before
            await reconcile_task_image_build_cleanup(
                session,
                grant_id=grant.grant_id,
                inventory=_inventory(
                    grant,
                    job_id="12345",
                    state="running",
                    held=False,
                    observed_at=grant.authority.expires_at,
                ),
                now=grant.authority.expires_at,
            )
            await session.commit()
        with pytest.raises(DBAPIError, match="cannot downgrade 0143"):
            await asyncio.to_thread(command.downgrade, config, "0142")
        async with sessions() as session:
            assert await session.scalar(text("SELECT version_num FROM alembic_version")) == "0143"
            row = await session.get(TaskImageBuildGrant, grant.grant_id)
            assert row is not None and row.state == "revoked"
            assert row.bound_at == row.released_at == _NOW
            assert row.slurm_job_id == "12345"
    finally:
        await engine.dispose()


async def test_empty_cleanup_migration_downgrade_and_reupgrade(
    isolated_migration_postgres_url: str,
) -> None:
    config = _config(isolated_migration_postgres_url)
    await asyncio.to_thread(command.downgrade, config, "0142")
    await asyncio.to_thread(command.upgrade, config, "0143")
    engine = create_async_engine(isolated_migration_postgres_url)
    try:
        async with engine.connect() as connection:
            assert (
                await connection.scalar(text("SELECT version_num FROM alembic_version")) == "0143"
            )
            assert (
                await connection.scalar(text("SELECT count(*) FROM task_image_build_grants")) == 0
            )
    finally:
        await engine.dispose()
