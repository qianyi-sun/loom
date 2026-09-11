"""Ownership completion must account for role DDL submitted to other databases."""

import psycopg
import pytest

from tests.integration.test_application_handoff_completion import _closed
from tests.integration.test_application_ownership_transfer import (
    transfer_database,  # noqa: F401
    transfer_postgres,  # noqa: F401
    transfer_postgres_url,  # noqa: F401
)


@pytest.mark.asyncio
@pytest.mark.parametrize("transfer_postgres", [16, 17], indirect=True)
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
