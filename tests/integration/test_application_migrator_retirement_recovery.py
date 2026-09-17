"""Real PostgreSQL startup and interrupted transient-authority retirement."""

import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from uuid import uuid4

import psycopg
import pytest
from psycopg import sql
from testcontainers.postgres import PostgresContainer

from loom.application_handoff_completion import complete_application_handoff_database
from loom.application_migrator_retirement import retire_application_migrator
from loom.application_schema_reference import application_schema_reference
from tests.integration.test_application_handoff_completion import _closed
from tests.integration.test_application_ownership_transfer import (
    transfer_database,  # noqa: F401
    transfer_postgres_url,  # noqa: F401
)

pytestmark = pytest.mark.parametrize("transfer_postgres", [16, 17], indirect=True)


@pytest.fixture(scope="module")
def transfer_postgres(request):
    image = application_schema_reference(postgres_major=request.param).postgres_image
    with PostgresContainer(image, driver="psycopg", password=uuid4().hex).with_command(
        "postgres -c max_prepared_transactions=10"
    ).with_bind_ports(5432, ("127.0.0.1", None)) as postgres:
        yield postgres


@contextmanager
def _migration(database_fixture):
    with _closed(database_fixture) as (peer, maintenance, guard, arguments):
        try:
            complete_application_handoff_database(peer, maintenance=maintenance, **arguments)
        except Exception as exc:
            activity = peer.execute("SELECT pid,usename,backend_type,state FROM pg_stat_activity WHERE datname=current_database()").fetchall()
            raise AssertionError(f"disposable migration setup was not quiescent: {activity!r}") from exc
        target = arguments["target"]
        name = "app_migrate_" + uuid4().hex
        peer.execute(sql.SQL("CREATE ROLE {} LOGIN NOINHERIT PASSWORD 'test-only'").format(sql.Identifier(name)))
        peer.execute(sql.SQL("GRANT {} TO {} WITH ADMIN FALSE, INHERIT FALSE, SET TRUE").format(sql.Identifier(target.successor_role), sql.Identifier(name)))
        peer.execute(sql.SQL("GRANT CONNECT ON DATABASE {} TO {}").format(sql.Identifier(target.database), sql.Identifier(name)))
        oid = peer.execute("SELECT oid::bigint FROM pg_roles WHERE rolname=%s", (name,)).fetchone()[0]
        options = dict(
            target=target, coordination_guard=arguments["coordination_guard"],
            migrator_role=name, migrator_oid=oid,
            provisioner_role=next(role for role, alias in arguments["role_bindings"].items() if alias == "provisioner"),
        )
        try:
            yield peer, maintenance, guard, options
        finally:
            # Fixture-owned cleanup only; never product DROP OWNED or adoption.
            if peer.execute("SELECT 1 FROM pg_roles WHERE rolname=%s", (name,)).fetchone():
                peer.execute(sql.SQL("DROP OWNED BY {}").format(sql.Identifier(name)))
                peer.execute(sql.SQL("DROP ROLE {}").format(sql.Identifier(name)))


def _seal(peer, maintenance, options):
    peer.execute(sql.SQL("ALTER ROLE {} NOLOGIN PASSWORD NULL").format(sql.Identifier(options["migrator_role"])))
    maintenance.execute(sql.SQL("ALTER DATABASE {} ALLOW_CONNECTIONS false").format(sql.Identifier(options["target"].database)))


@pytest.mark.parametrize("database", ["target", "postgres"])
def test_retirement_refuses_unpublished_startup_in_any_database(transfer_database, database):  # noqa: F811
    with _migration(transfer_database) as (peer, maintenance, guard, options), ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(
            psycopg.connect, transfer_database[0], autocommit=True, connect_timeout=10,
            dbname=options["target"].database if database == "target" else "postgres",
            options="-c post_auth_delay=3",
        )
        try:
            deadline = time.monotonic() + 2
            while not maintenance.execute(
                "SELECT EXISTS (SELECT 1 FROM pg_locks WHERE locktype='object' "
                "AND classid='pg_database'::regclass AND mode='RowExclusiveLock')"
            ).fetchone()[0]:
                assert time.monotonic() < deadline, "startup lock was not observed"
                time.sleep(0.01)
            _seal(peer, maintenance, options)
            with pytest.raises(RuntimeError, match="startup"):
                retire_application_migrator(maintenance, **options)
            assert peer.execute("SELECT oid::bigint FROM pg_roles WHERE rolname=%s", (options["migrator_role"],)).fetchone() == (options["migrator_oid"],)
        finally:
            future.result(timeout=10).close()
        retire_application_migrator(maintenance, **options)
        assert guard.execute("SELECT 1").fetchone() == (1,)


def test_retirement_refuses_real_prepared_work(transfer_database):  # noqa: F811
    with _migration(transfer_database) as (peer, maintenance, _, options):
        gid = "migrator-" + uuid4().hex
        with psycopg.connect(transfer_database[0], user=options["migrator_role"], password="test-only", autocommit=True) as migrator:
            migrator.execute("BEGIN")
            migrator.execute("SELECT 1")
            migrator.execute(sql.SQL("PREPARE TRANSACTION {}").format(sql.Literal(gid)))
        try:
            _seal(peer, maintenance, options)
            with pytest.raises(RuntimeError, match="prepared"):
                retire_application_migrator(maintenance, **options)
            assert peer.execute("SELECT gid FROM pg_prepared_xacts WHERE gid=%s", (gid,)).fetchone() == (gid,)
        finally:
            peer.execute(sql.SQL("ROLLBACK PREPARED {}").format(sql.Literal(gid)))
        retire_application_migrator(maintenance, **options)


@pytest.mark.parametrize("interruption", ["commit-ack", "guard-during-drop", "same-name-replacement", "owned-object"])
def test_retirement_reconciles_only_the_original_role(transfer_database, interruption):  # noqa: F811
    with _migration(transfer_database) as (peer, maintenance, guard, options):
        _seal(peer, maintenance, options)
        role = sql.Identifier(options["migrator_role"])
        if interruption == "same-name-replacement":
            peer.execute(sql.SQL("DROP OWNED BY {}; DROP ROLE {}; CREATE ROLE {} NOLOGIN NOINHERIT").format(role, role, role))
            with pytest.raises(RuntimeError, match="exact sealed role"):
                retire_application_migrator(maintenance, **options)
            assert peer.execute("SELECT oid::bigint FROM pg_roles WHERE rolname=%s", (options["migrator_role"],)).fetchone() != (options["migrator_oid"],)
            return
        if interruption == "owned-object":
            peer.execute(sql.SQL("CREATE TABLE public.transient_owned (id integer); ALTER TABLE public.transient_owned OWNER TO {}").format(role))
            with pytest.raises(RuntimeError, match="unexpected object"):
                retire_application_migrator(maintenance, **options)
            assert peer.execute("SELECT to_regclass('public.transient_owned') IS NOT NULL").fetchone() == (True,)
            return

        class Interrupted:
            @property
            def info(self):
                return maintenance.info

            def execute(self, query):
                result = maintenance.execute(query)
                rendered = query if isinstance(query, str) else query.as_string(maintenance)
                if interruption == "guard-during-drop" and rendered.startswith("DROP ROLE "):
                    guard.execute("SELECT pg_advisory_unlock_all()")
                return result

            @contextmanager
            def transaction(self):
                with maintenance.transaction():
                    yield
                if interruption == "commit-ack":
                    raise RuntimeError("commit acknowledgement lost")

        with pytest.raises(RuntimeError, match=r"acknowledgement|coordination guard"):
            retire_application_migrator(Interrupted(), **options)
        row = peer.execute("SELECT oid::bigint FROM pg_roles WHERE rolname=%s", (options["migrator_role"],)).fetchone()
        if interruption == "commit-ack":
            assert row is None
            retire_application_migrator(maintenance, **options)
        else:
            assert row == (options["migrator_oid"],)
        assert maintenance.execute("SELECT datallowconn FROM pg_database WHERE oid=%s", (options["target"].database_oid,)).fetchone() == (False,)
