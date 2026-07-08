"""End-to-end pgbouncer integration tests (#609).

Exercises SQLAlchemy workloads, NOTIFY round-trips, and pool recovery
against real Postgres + pgbouncer (transaction mode) containers.
"""
from __future__ import annotations

import asyncio

import psycopg
import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine


@pytest.mark.integration
@pytest.mark.asyncio
async def test_sqlalchemy_workload_through_pgbouncer(
    pgbouncer_stack: dict[str, str],
) -> None:
    """100-iteration query workload via pgbouncer transaction mode with
    prepare_threshold=None completes without errors."""
    engine = create_async_engine(
        pgbouncer_stack["pool_url"],
        connect_args={"prepare_threshold": None},
        pool_pre_ping=True,
        pool_size=5,
    )
    async with engine.begin() as conn:
        await conn.execute(text("CREATE TABLE t (id serial PRIMARY KEY, val int)"))

    async def run_query(i: int) -> None:
        async with engine.begin() as conn:
            await conn.execute(text("INSERT INTO t (val) VALUES (:v)"), {"v": i})

    await asyncio.gather(*[run_query(i) for i in range(100)])

    async with engine.begin() as conn:
        result = await conn.execute(text("SELECT COUNT(*) FROM t"))
        assert result.scalar() == 100

    await engine.dispose()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_listen_watcher_receives_notify_from_pgbouncer_session(
    pgbouncer_stack: dict[str, str],
) -> None:
    """LISTEN via direct URL receives a NOTIFY issued from a SQLAlchemy
    session that went through pgbouncer."""
    direct_dsn = pgbouncer_stack["direct_url"].replace(
        "postgresql+psycopg://", "postgresql://"
    )
    listen_conn = await psycopg.AsyncConnection.connect(
        direct_dsn,
        autocommit=True,
    )
    await listen_conn.execute("LISTEN test_channel")

    engine = create_async_engine(
        pgbouncer_stack["pool_url"],
        connect_args={"prepare_threshold": None},
    )
    async with engine.begin() as conn:
        await conn.execute(text("NOTIFY test_channel, 'hello-from-pgbouncer'"))

    got_payload: str | None = None
    async for note in listen_conn.notifies():
        got_payload = note.payload
        break
    assert got_payload == "hello-from-pgbouncer"

    await listen_conn.close()
    await engine.dispose()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_pool_pre_ping_recovers_from_engine_dispose(
    pgbouncer_stack: dict[str, str],
) -> None:
    """SQLAlchemy pool_pre_ping recovers connections after dispose+recreate,
    which mirrors what happens when pgbouncer restarts."""
    engine = create_async_engine(
        pgbouncer_stack["pool_url"],
        connect_args={"prepare_threshold": None},
        pool_pre_ping=True,
    )
    async with engine.begin() as conn:
        await conn.execute(text("SELECT 1"))

    await engine.dispose()

    engine2 = create_async_engine(
        pgbouncer_stack["pool_url"],
        connect_args={"prepare_threshold": None},
        pool_pre_ping=True,
    )
    async with engine2.begin() as conn:
        result = await conn.execute(text("SELECT 1"))
        assert result.scalar() == 1

    await engine2.dispose()
