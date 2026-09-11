from __future__ import annotations

import asyncio
import hashlib
from datetime import timedelta
from uuid import uuid4

import pytest
from alembic import command
from sqlalchemy import create_engine, inspect, select, text
from sqlalchemy.exc import DBAPIError, IntegrityError

from loom.db import schema
from loom_task_image_authority.retention_inventory import derive_attempt_repository_inventory
from tests.integration.test_task_image_publication_jobs import (
    registry_authority_session as registry_authority_session,
)
from tests.integration.test_task_image_registry_credential_migration import _config
from tests.integration.test_task_image_registry_credentials import NOW, _claimed_attempt


def _model():
    model = getattr(schema, "TaskImageAttemptRetention", None)
    assert model is not None, "durable attempt retirement record is missing"
    return model


async def _observation(session):
    model = _model()
    _, _, _, _, _, materialization, attempt = await _claimed_attempt(session)
    inventory = derive_attempt_repository_inventory(
        materialization=materialization,
        attempt=attempt,
        credentials=[],
        registry_origin="https://registry.example:5443",
    )
    row = model(
        attempt_id=attempt.id,
        observed_at=NOW + timedelta(days=1),
        unreferenced_since=NOW + timedelta(days=1),
    )
    session.add(row)
    await session.flush()
    return row, inventory


async def test_retirement_record_preserves_observation_then_irreversible_inventory(
    registry_authority_session,
):
    async with registry_authority_session() as session:
        row, inventory = await _observation(session)
        # Pin arrival clears only the observation; a later unreferenced interval
        # must start afresh rather than inherit the earlier grace clock.
        row.unreferenced_since = None
        row.observed_at = NOW + timedelta(days=2)
        await session.flush()
        row.unreferenced_since = NOW + timedelta(days=3)
        row.observed_at = NOW + timedelta(days=4)
        row.retired_at = row.observed_at
        row.canonical_inventory = inventory.canonical_bytes
        row.inventory_sha256 = hashlib.sha256(inventory.canonical_bytes).hexdigest()
        await session.commit()
        before = (await session.execute(text("SELECT * FROM task_image_attempt_retention"))).one()
        for mutation in (
            "retired_at=NULL, canonical_inventory=NULL, inventory_sha256=NULL",
            "observed_at=observed_at + interval '1 second', retired_at=retired_at + interval '1 second'",
            "unreferenced_since=unreferenced_since - interval '1 second'",
            "canonical_inventory=convert_to('{}','UTF8'), inventory_sha256=encode(sha256(convert_to('{}','UTF8')),'hex')",
            "attempt_id=gen_random_uuid()",
        ):
            with pytest.raises(IntegrityError):
                async with session.begin_nested():
                    await session.execute(
                        text(f"UPDATE task_image_attempt_retention SET {mutation}")
                    )
            assert (
                await session.execute(text("SELECT * FROM task_image_attempt_retention"))
            ).one() == before
        for sql in (
            "DELETE FROM task_image_attempt_retention",
            "TRUNCATE task_image_attempt_retention CASCADE",
        ):
            with pytest.raises(IntegrityError, match="retirement evidence is immutable"):
                async with session.begin_nested():
                    await session.execute(text(sql))
            assert (
                await session.execute(text("SELECT * FROM task_image_attempt_retention"))
            ).one() == before
        await session.execute(text("UPDATE task_image_attempt_retention SET retired_at=retired_at"))
        await session.commit()


async def test_retirement_record_rejects_invalid_shape_and_clock_regression(
    registry_authority_session,
):
    async with registry_authority_session() as session:
        row, _ = await _observation(session)
        await session.commit()
        before = (await session.execute(text("SELECT * FROM task_image_attempt_retention"))).one()
        for mutation in (
            "observed_at='infinity'",
            "observed_at='-infinity'",
            "observed_at=observed_at - interval '1 day', unreferenced_since=NULL",
            "unreferenced_since=observed_at + interval '1 second'",
            "attempt_id=gen_random_uuid()",
            "retired_at=observed_at",
            "canonical_inventory=convert_to('{}','UTF8')",
            "retired_at=observed_at, canonical_inventory=convert_to('{}','UTF8'), inventory_sha256=repeat('a',64)",
            "retired_at=observed_at - interval '1 second', canonical_inventory=convert_to('{}','UTF8'), inventory_sha256=encode(sha256(convert_to('{}','UTF8')),'hex')",
            "retired_at=observed_at, canonical_inventory=decode('','hex'), inventory_sha256=encode(sha256(decode('','hex')),'hex')",
        ):
            with pytest.raises(IntegrityError):
                async with session.begin_nested():
                    await session.execute(
                        text(f"UPDATE task_image_attempt_retention SET {mutation}")
                    )
            assert (
                await session.execute(text("SELECT * FROM task_image_attempt_retention"))
            ).one() == before
        with pytest.raises(IntegrityError):
            async with session.begin_nested():
                session.add(_model()(attempt_id=uuid4(), observed_at=NOW))
                await session.flush()
        assert await session.scalar(select(_model().attempt_id)) == row.attempt_id


def test_retirement_record_inactive_roundtrip_and_orm_parity(isolated_migration_postgres_url):
    model = _model()
    config = _config(isolated_migration_postgres_url)
    engine = create_engine(isolated_migration_postgres_url)
    try:
        assert {item["name"] for item in inspect(engine).get_columns(model.__tablename__)} == set(
            model.__table__.columns.keys()
        )
        fk = inspect(engine).get_foreign_keys(model.__tablename__)[0]
        assert fk["referred_table"] == "task_image_materialization_attempts"
        assert fk["options"]["ondelete"] == "RESTRICT"
        command.downgrade(config, "0134")
        assert model.__tablename__ not in inspect(engine).get_table_names()
        command.upgrade(config, "0135")
        assert model.__tablename__ in inspect(engine).get_table_names()
    finally:
        engine.dispose()


async def test_retired_attempt_blocks_inactive_downgrade(
    registry_authority_session, isolated_migration_postgres_url
):
    async with registry_authority_session() as session:
        row, inventory = await _observation(session)
        row.retired_at = row.observed_at
        row.canonical_inventory = inventory.canonical_bytes
        row.inventory_sha256 = hashlib.sha256(inventory.canonical_bytes).hexdigest()
        await session.commit()
    # No credentials, candidates, jobs or keys exist: the new retirement row
    # alone must prevent removal of its fence. This tests storage, not eligibility.
    with pytest.raises(DBAPIError, match="retirement authority cannot be discarded"):
        command.downgrade(_config(isolated_migration_postgres_url), "0134")
    async with registry_authority_session() as session:
        assert await session.scalar(text("SELECT version_num FROM alembic_version")) == "0142"
        assert await session.scalar(select(_model().retired_at)) is not None


@pytest.mark.parametrize(
    "table", ["task_image_materialization_attempts", "task_image_attempt_retention"]
)
async def test_retirement_downgrade_fails_fast_on_parent_and_record_writers(
    isolated_migration_postgres_url, table
):
    config = _config(isolated_migration_postgres_url)
    engine = create_engine(isolated_migration_postgres_url)
    try:
        with engine.connect() as blocker:
            blocker.execute(text(f"LOCK TABLE {table} IN ROW EXCLUSIVE MODE"))
            blocker_pid = blocker.execute(text("SELECT pg_backend_pid()")).scalar_one()
            migration = asyncio.create_task(asyncio.to_thread(command.downgrade, config, "0134"))
            try:
                async with asyncio.timeout(5):
                    while not migration.done():
                        blocker.execute(text("SELECT pg_stat_clear_snapshot()"))
                        assert not blocker.execute(
                            text(
                                "SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE :blocker = ANY(pg_blocking_pids(pid)))"
                            ),
                            {"blocker": blocker_pid},
                        ).scalar_one(), "downgrade waits on an ordinary authority writer"
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
                == "0142"
            )
    finally:
        engine.dispose()
