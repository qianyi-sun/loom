"""Retire only sealed application runtime sessions on real PostgreSQL."""

from contextlib import contextmanager

import psycopg
import pytest
from psycopg import sql

from loom.application_handoff_completion import complete_application_handoff_database
from loom.application_runtime_login import seal_application_runtime_for_cutover
from tests.integration.test_application_handoff_completion import _closed
from tests.integration.test_application_ownership_transfer import (
    transfer_database,  # noqa: F401
    transfer_postgres,  # noqa: F401
    transfer_postgres_url,  # noqa: F401
)

pytestmark = pytest.mark.parametrize("transfer_postgres", [16, 17], indirect=True)


@contextmanager
def _runtime(database_fixture):
    with _closed(database_fixture) as (peer, maintenance, guard, arguments):
        complete_application_handoff_database(peer, maintenance=maintenance, **arguments)
        target = arguments["target"]
        options = dict(target=target, coordination_guard=arguments["coordination_guard"],
            password=arguments["password"], provisioner_role=next(
                role for role, alias in arguments["role_bindings"].items() if alias == "provisioner"))
        with psycopg.connect(database_fixture[0], user=target.owner_role,
                             password=arguments["password"], autocommit=True) as active:
            yield peer, maintenance, guard, active, arguments, options


def _seal(peer, arguments):
    seal_application_runtime_for_cutover(peer,
        owner_role=arguments["target"].successor_role, role_bindings=arguments["role_bindings"],
        password=arguments["password"], target=arguments["target"],
        coordination_guard=arguments["coordination_guard"], schema_acl_profile="staging-readonly")


def _close(maintenance, options):
    maintenance.execute(sql.SQL("ALTER DATABASE {} ALLOW_CONNECTIONS false").format(
        sql.Identifier(options["target"].database)))


def _identity(peer, target):
    return (
        peer.execute("SELECT * FROM pg_authid WHERE oid=%s", (target.owner_oid,)).fetchall(),
        peer.execute("SELECT * FROM pg_shdepend WHERE refclassid='pg_authid'::regclass "
                     "AND refobjid=%s ORDER BY dbid,classid,objid,objsubid,deptype", (target.owner_oid,)).fetchall(),
    )


def test_runtime_retirement_requires_closed_admission_and_preserves_role(transfer_database):  # noqa: F811
    from loom.application_runtime_retirement import retire_application_runtime_sessions

    with _runtime(transfer_database) as (peer, maintenance, guard, active, arguments, options):
        _seal(peer, arguments)
        before = _identity(peer, options["target"])
        with pytest.raises(RuntimeError, match="closed admission"):
            retire_application_runtime_sessions(maintenance, **options)
        assert active.execute("SELECT 1").fetchone() == (1,)
        _close(maintenance, options)
        retire_application_runtime_sessions(maintenance, **options)
        with pytest.raises(psycopg.OperationalError):
            active.execute("SELECT 1")
        retire_application_runtime_sessions(maintenance, **options)
        assert _identity(peer, options["target"]) == before
        assert guard.execute("SELECT 1").fetchone() == (1,)
        assert peer.execute("SELECT 1").fetchone() == (1,)


@pytest.mark.parametrize("drift", ["password", "guard", "foreign"])
def test_runtime_retirement_refuses_drift_before_any_session_signal(transfer_database, drift):  # noqa: F811
    from loom.application_runtime_retirement import retire_application_runtime_sessions

    with _runtime(transfer_database) as (peer, maintenance, guard, active, arguments, options):
        foreign = None
        try:
            if drift == "foreign":
                foreign = psycopg.connect(transfer_database[0], dbname="postgres",
                    user=options["target"].owner_role, password=options["password"], autocommit=True)
            _seal(peer, arguments)
            _close(maintenance, options)
            before = _identity(peer, options["target"])
            if drift == "password":
                options["password"] = "wrong-password"
            elif drift == "guard":
                guard.execute("SELECT pg_advisory_unlock_all()")
            with pytest.raises(RuntimeError):
                retire_application_runtime_sessions(maintenance, **options)
            assert active.execute("SELECT 1").fetchone() == (1,)
            if foreign is not None:
                assert foreign.execute("SELECT 1").fetchone() == (1,)
            assert _identity(peer, options["target"]) == before
        finally:
            if foreign is not None:
                foreign.close()
