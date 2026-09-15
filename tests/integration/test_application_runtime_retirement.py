"""Retire only sealed application runtime sessions on real PostgreSQL."""

import time
from concurrent.futures import ThreadPoolExecutor
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
        peer.execute("SELECT to_jsonb(r) FROM pg_authid r WHERE oid=%s", (target.owner_oid,)).fetchall(),
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


@pytest.mark.parametrize("database", ["target", "postgres"])
def test_runtime_retirement_refuses_unpublished_startup(transfer_database, database):  # noqa: F811
    from loom.application_runtime_retirement import retire_application_runtime_sessions

    with _runtime(transfer_database) as (peer, maintenance, _, active, arguments, options), ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(psycopg.connect, transfer_database[0], autocommit=True,
            connect_timeout=10, dbname=options["target"].database if database == "target" else "postgres",
            options="-c post_auth_delay=3")
        try:
            deadline = time.monotonic() + 2
            while not maintenance.execute("SELECT EXISTS (SELECT 1 FROM pg_locks "
                "WHERE locktype='object' AND classid='pg_database'::regclass AND mode='RowExclusiveLock')").fetchone()[0]:
                assert time.monotonic() < deadline, "startup lock was not observed"
                time.sleep(0.01)
            _seal(peer, arguments)
            _close(maintenance, options)
            with pytest.raises(RuntimeError, match="startup"):
                retire_application_runtime_sessions(maintenance, **options)
            assert active.execute("SELECT 1").fetchone() == (1,)
        finally:
            future.result(timeout=10).close()
        retire_application_runtime_sessions(maintenance, **options)


def test_runtime_retirement_stops_signalling_after_guard_loss(transfer_database):  # noqa: F811
    from loom.application_runtime_retirement import retire_application_runtime_sessions

    with _runtime(transfer_database) as (peer, maintenance, guard, active, arguments, options):
        with psycopg.connect(transfer_database[0], user=options["target"].owner_role,
                            password=options["password"], autocommit=True) as second:
            _seal(peer, arguments)
            _close(maintenance, options)
            before = _identity(peer, options["target"])
            signalled = []

            class Interrupted:
                @property
                def info(self):
                    return maintenance.info

                def transaction(self):
                    return maintenance.transaction()

                def execute(self, query):
                    result = maintenance.execute(query)
                    rendered = query if isinstance(query, str) else query.as_string(maintenance)
                    if rendered.startswith("SELECT pg_catalog.pg_terminate_backend"):
                        signalled.append(rendered)
                        guard.execute("SELECT pg_advisory_unlock_all()")
                    return result

            with pytest.raises(RuntimeError, match="coordination guard"):
                retire_application_runtime_sessions(Interrupted(), **options)
            assert len(signalled) == 1
            ordered = sorted((active, second), key=lambda connection: connection.info.backend_pid)
            with pytest.raises(psycopg.OperationalError):
                ordered[0].execute("SELECT 1")
            assert ordered[1].execute("SELECT 1").fetchone() == (1,)
            assert _identity(peer, options["target"]) == before
            assert maintenance.execute("SELECT datallowconn FROM pg_database WHERE oid=%s",
                (options["target"].database_oid,)).fetchone() == (False,)


def test_installed_peer_transport_seals_and_retires_runtime(transfer_database, transfer_postgres):  # noqa: F811
    from loom.application_runtime_retirement import retire_application_runtime_sessions
    from tests.integration.test_protected_peer_database_connection import _peer

    with _runtime(transfer_database) as (peer, maintenance, guard, active, arguments, options):
        with _peer(transfer_postgres, database=options["target"].database) as installed_app:
            _seal(installed_app, arguments)
        before = _identity(peer, options["target"])
        _close(maintenance, options)
        with _peer(transfer_postgres, database="postgres") as installed_maintenance:
            retire_application_runtime_sessions(installed_maintenance, **options)
            retire_application_runtime_sessions(installed_maintenance, **options)
        with pytest.raises(psycopg.OperationalError):
            active.execute("SELECT 1")
        assert _identity(peer, options["target"]) == before
        assert guard.execute("SELECT 1").fetchone() == (1,)
