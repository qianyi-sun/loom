"""Ownership completion must account for role DDL submitted to other databases."""

import time
from concurrent.futures import ThreadPoolExecutor
from uuid import uuid4

import psycopg
import pytest
from psycopg import sql

from tests.integration.test_application_handoff_completion import _closed
from tests.integration.test_application_migrator_retirement_recovery import (
    transfer_postgres,  # noqa: F401
)
from tests.integration.test_application_ownership_transfer import (
    transfer_database,  # noqa: F401
    transfer_postgres_url,  # noqa: F401
)

pytestmark = pytest.mark.parametrize("transfer_postgres", [16, 17], indirect=True)


@pytest.mark.asyncio
@pytest.mark.parametrize("foreign_state", ["idle", "transaction"])
async def test_handoff_completion_refuses_surviving_foreign_database_client_work(
    transfer_database, foreign_state,  # noqa: F811
):
    from loom.application_handoff_completion import complete_application_handoff_database

    url, _, _ = transfer_database
    with _closed(transfer_database) as (peer, maintenance, guard, arguments):
        with psycopg.connect(url, dbname="postgres", autocommit=True) as old_manager:
            if foreign_state == "transaction":
                old_manager.execute("BEGIN")
                old_manager.execute("LOCK TABLE pg_catalog.pg_authid IN ROW EXCLUSIVE MODE")
            with pytest.raises(RuntimeError, match="client work"):
                complete_application_handoff_database(peer, maintenance=maintenance, **arguments)
            # Refusal must not signal any foreign backend or transfer ownership.
            assert old_manager.execute("SELECT 1").fetchone() == (1,)
            assert peer.execute("SELECT datdba,datallowconn FROM pg_database WHERE datname=current_database()").fetchone() == (arguments["target"].owner_oid, False)
            assert guard.info.backend_pid == arguments["coordination_guard"].backend.pid
            old_manager.execute("ROLLBACK")
        outcome = complete_application_handoff_database(peer, maintenance=maintenance, **arguments)
        assert outcome.target == arguments["target"]


@pytest.mark.asyncio
async def test_completion_refuses_unpublished_foreign_database_startup(transfer_database):  # noqa: F811
    from loom.application_handoff_completion import complete_application_handoff_database

    with _closed(transfer_database) as (peer, maintenance, _, arguments), ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(psycopg.connect, transfer_database[0], dbname="postgres", autocommit=True,
                             connect_timeout=10, options="-c post_auth_delay=3")
        try:
            deadline = time.monotonic() + 2
            while not maintenance.execute(
                "SELECT EXISTS (SELECT 1 FROM pg_locks WHERE locktype='object' "
                "AND classid='pg_database'::regclass AND mode='RowExclusiveLock')"
            ).fetchone()[0]:
                assert time.monotonic() < deadline, "startup lock was not observed"
                time.sleep(0.01)
            with pytest.raises(RuntimeError, match=r"client work.*startup"):
                complete_application_handoff_database(peer, maintenance=maintenance, **arguments)
        finally:
            future.result(timeout=10).close()
        complete_application_handoff_database(peer, maintenance=maintenance, **arguments)


@pytest.mark.asyncio
async def test_completion_refuses_prepared_work_in_other_database(transfer_database):  # noqa: F811
    from loom.application_handoff_completion import complete_application_handoff_database

    with _closed(transfer_database) as (peer, maintenance, _, arguments):
        gid = "retirement-" + uuid4().hex
        maintenance.execute("BEGIN")
        maintenance.execute("SELECT 1")
        maintenance.execute(sql.SQL("PREPARE TRANSACTION {}").format(sql.Literal(gid)))
        try:
            with pytest.raises(RuntimeError, match=r"client work.*prepared"):
                complete_application_handoff_database(peer, maintenance=maintenance, **arguments)
            assert maintenance.execute("SELECT count(*) FROM pg_prepared_xacts").fetchone() == (1,)
        finally:
            maintenance.execute(sql.SQL("ROLLBACK PREPARED {}").format(sql.Literal(gid)))
        complete_application_handoff_database(peer, maintenance=maintenance, **arguments)
