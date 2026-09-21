from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime
from pathlib import Path
from uuid import UUID

import pytest
from alembic import command
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError, IntegrityError
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from loom.db.schema import TaskImageBuildGrant
from tests.integration.test_task_image_registry_credential_migration import _config


async def test_cleanup_migration_preserves_released_grant_and_refuses_lossy_downgrade(
    isolated_migration_postgres_url: str,
) -> None:
    config = _config(isolated_migration_postgres_url)
    await asyncio.to_thread(command.downgrade, config, "0142")
    engine = create_async_engine(isolated_migration_postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    fixture = Path(__file__).parents[1] / "fixtures/historical/released_task_image_build_grant.json"
    values = json.loads(fixture.read_text())
    values["id"] = UUID(values["id"])
    for field in (
        "grant_expires_at", "ambiguity_settle_until", "invocation_started_at",
        "bound_at", "released_at", "created_at", "updated_at",
    ):
        values[field] = datetime.fromisoformat(values[field])
    grant_id = values["id"]
    try:
        async with sessions() as session:
            session.add(TaskImageBuildGrant(**values))
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
            await session.execute(text("""UPDATE task_image_build_grants
                SET state='revoked', revoked_at=grant_expires_at,
                    revoke_reason='grant_authority_expired'"""))
            await session.commit()
        with pytest.raises(DBAPIError, match="cannot downgrade 0143"):
            await asyncio.to_thread(command.downgrade, config, "0142")
        async with sessions() as session:
            assert await session.scalar(text("SELECT version_num FROM alembic_version")) == "0143"
            row = await session.get(TaskImageBuildGrant, grant_id)
            assert row is not None and row.state == "revoked"
            assert row.bound_at == row.released_at == datetime(2026, 8, 22, 2, 0, tzinfo=UTC)
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
