"""Prepared runtime work must survive a refused retirement unchanged."""

from uuid import uuid4

import pytest
from psycopg import sql

from loom.application_runtime_retirement import retire_application_runtime_sessions
from tests.integration.test_application_migrator_retirement_recovery import transfer_postgres  # noqa: F401
from tests.integration.test_application_ownership_transfer import (
    transfer_database,  # noqa: F401
    transfer_postgres_url,  # noqa: F401
)
from tests.integration.test_application_runtime_retirement import _close, _identity, _runtime, _seal

pytestmark = pytest.mark.parametrize("transfer_postgres", [16, 17], indirect=True)


def test_runtime_retirement_refuses_prepared_transaction(transfer_database):  # noqa: F811
    with _runtime(transfer_database) as (peer, maintenance, guard, active, arguments, options):
        gid = "runtime-cutover-" + uuid4().hex
        active.execute("BEGIN")
        active.execute("SELECT 1")
        active.execute(sql.SQL("PREPARE TRANSACTION {}").format(sql.Literal(gid)))
        try:
            _seal(peer, arguments)
            _close(maintenance, options)
            before = _identity(peer, options["target"])
            with pytest.raises(RuntimeError, match="prepared"):
                retire_application_runtime_sessions(maintenance, **options)
            assert active.execute("SELECT 1").fetchone() == (1,)
            assert peer.execute("SELECT gid FROM pg_prepared_xacts WHERE gid=%s", (gid,)).fetchone() == (gid,)
            assert _identity(peer, options["target"]) == before
            assert guard.execute("SELECT 1").fetchone() == (1,)
        finally:
            peer.execute(sql.SQL("ROLLBACK PREPARED {}").format(sql.Literal(gid)))
        retire_application_runtime_sessions(maintenance, **options)
