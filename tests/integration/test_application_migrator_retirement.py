"""Retire transient application DDL authority on disposable PostgreSQL."""

from uuid import uuid4

import psycopg
import pytest
from psycopg import sql

from loom.application_handoff_completion import complete_application_handoff_database
from tests.integration.test_application_handoff_completion import _closed
from tests.integration.test_application_ownership_transfer import (
    transfer_database,  # noqa: F401
    transfer_postgres,  # noqa: F401
    transfer_postgres_url,  # noqa: F401
)

pytestmark = pytest.mark.parametrize("transfer_postgres", [16, 17], indirect=True)


def test_migrator_retirement_requires_closed_admission_and_retires_set_role_sessions(
    transfer_database,  # noqa: F811
):
    from loom.application_migrator_retirement import retire_application_migrator

    with _closed(transfer_database) as (peer, maintenance, guard, arguments):
        complete_application_handoff_database(peer, maintenance=maintenance, **arguments)
        target = arguments["target"]
        migrator = "app_migrate_" + uuid4().hex
        password = uuid4().hex
        peer.execute(sql.SQL("CREATE ROLE {} LOGIN NOINHERIT PASSWORD {}").format(sql.Identifier(migrator), sql.Literal(password)))
        peer.execute(sql.SQL("GRANT {} TO {} WITH ADMIN FALSE, INHERIT FALSE, SET TRUE").format(sql.Identifier(target.successor_role), sql.Identifier(migrator)))
        peer.execute(sql.SQL("GRANT CONNECT ON DATABASE {} TO {}").format(sql.Identifier(target.database), sql.Identifier(migrator)))
        oid = peer.execute("SELECT oid::bigint FROM pg_roles WHERE rolname=%s", (migrator,)).fetchone()[0]
        options = dict(target=target, coordination_guard=arguments["coordination_guard"], migrator_role=migrator, migrator_oid=oid)
        try:
            with psycopg.connect(transfer_database[0], user=migrator, password=password, autocommit=True) as active:
                active.execute(sql.SQL("SET ROLE {}").format(sql.Identifier(target.successor_role)))
                active.execute("CREATE TABLE public.migrator_probe (id integer)")
                # NOLOGIN + a revoked grant does not revoke an existing SET ROLE.
                peer.execute(sql.SQL("ALTER ROLE {} NOLOGIN PASSWORD NULL").format(sql.Identifier(migrator)))
                peer.execute(sql.SQL("REVOKE {} FROM {}").format(sql.Identifier(target.successor_role), sql.Identifier(migrator)))
                assert active.execute("SELECT current_user").fetchone() == (target.successor_role,)
                with pytest.raises(RuntimeError, match="admission.*closed"):
                    retire_application_migrator(maintenance, **options)
                assert active.execute("SELECT current_user").fetchone() == (target.successor_role,)
                maintenance.execute(sql.SQL("ALTER DATABASE {} ALLOW_CONNECTIONS false").format(sql.Identifier(target.database)))
                retire_application_migrator(maintenance, **options)
                with pytest.raises(psycopg.OperationalError):
                    active.execute("SELECT 1")
            assert peer.execute("SELECT oid FROM pg_roles WHERE rolname=%s", (migrator,)).fetchone() is None
            assert peer.execute("SELECT relowner::bigint FROM pg_class WHERE relname='migrator_probe'").fetchone() == (target.successor_oid,)
            assert guard.execute("SELECT 1").fetchone() == (1,)
            # Lost cleanup acknowledgement is safe to replay using the saved OID.
            retire_application_migrator(maintenance, **options)
        finally:
            peer.execute("DROP TABLE IF EXISTS public.migrator_probe")
            if peer.execute("SELECT 1 FROM pg_roles WHERE rolname=%s", (migrator,)).fetchone():
                peer.execute(sql.SQL("DROP OWNED BY {}").format(sql.Identifier(migrator)))
            peer.execute(sql.SQL("DROP ROLE IF EXISTS {}").format(sql.Identifier(migrator)))


@pytest.mark.parametrize("drift", ["login", "password", "oid", "guard", "owner", "foreign"])
def test_migrator_retirement_refuses_unsafe_authority(transfer_database, drift):  # noqa: F811
    from loom.application_migrator_retirement import retire_application_migrator

    with _closed(transfer_database) as (peer, maintenance, guard, arguments):
        complete_application_handoff_database(peer, maintenance=maintenance, **arguments)
        target = arguments["target"]
        migrator = "app_migrate_" + uuid4().hex
        peer.execute(sql.SQL("CREATE ROLE {} NOLOGIN NOINHERIT").format(sql.Identifier(migrator)))
        oid = peer.execute("SELECT oid::bigint FROM pg_roles WHERE rolname=%s", (migrator,)).fetchone()[0]
        options = dict(target=target, coordination_guard=arguments["coordination_guard"], migrator_role=migrator, migrator_oid=oid)
        foreign = None
        try:
            if drift == "foreign":
                peer.execute(sql.SQL("ALTER ROLE {} LOGIN PASSWORD 'test'").format(sql.Identifier(migrator)))
                foreign = psycopg.connect(transfer_database[0], dbname="postgres", user=migrator, password="test", autocommit=True)
                peer.execute(sql.SQL("ALTER ROLE {} NOLOGIN PASSWORD NULL").format(sql.Identifier(migrator)))
            maintenance.execute(sql.SQL("ALTER DATABASE {} ALLOW_CONNECTIONS false").format(sql.Identifier(target.database)))
            if drift == "login":
                peer.execute(sql.SQL("ALTER ROLE {} LOGIN").format(sql.Identifier(migrator)))
            elif drift == "password":
                peer.execute(sql.SQL("ALTER ROLE {} PASSWORD 'test'").format(sql.Identifier(migrator)))
            elif drift == "oid":
                options["migrator_oid"] = oid + 1
            elif drift == "guard":
                guard.execute("SELECT pg_advisory_unlock_all()")
            elif drift == "owner":
                peer.execute(sql.SQL("ALTER ROLE {} LOGIN").format(sql.Identifier(target.successor_role)))
            with pytest.raises(RuntimeError):
                retire_application_migrator(maintenance, **options)
            assert peer.execute("SELECT oid::bigint FROM pg_roles WHERE rolname=%s", (migrator,)).fetchone() == (oid,)
            if foreign is not None:
                assert foreign.execute("SELECT 1").fetchone() == (1,)
        finally:
            if foreign is not None:
                foreign.close()
            peer.execute(sql.SQL("ALTER ROLE {} NOLOGIN").format(sql.Identifier(target.successor_role)))
            peer.execute(sql.SQL("DROP ROLE {}").format(sql.Identifier(migrator)))
