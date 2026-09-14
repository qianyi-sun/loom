"""Bounded application DDL credentials on disposable PostgreSQL."""

from datetime import UTC, datetime, timedelta
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


@pytest.mark.asyncio
async def test_application_migrator_journals_oid_before_login_and_retires_surviving_owner_session(transfer_database):  # noqa: F811
    from loom.application_migrator_provision import (
        arm_application_migrator,
        create_application_migrator,
        seal_application_migrator,
    )
    from loom.application_migrator_retirement import retire_application_migrator

    with _closed(transfer_database) as (peer, maintenance, guard, args):
        complete_application_handoff_database(peer, maintenance=maintenance, **args)
        target = args["target"]
        name, password = "app_migrator_" + uuid4().hex, uuid4().hex
        authority = dict(target=target, coordination_guard=args["coordination_guard"],
            provisioner_role=next(n for n, a in args["role_bindings"].items() if a == "provisioner"))
        recorded = []
        def persist(identity):
            assert peer.execute("SELECT rolcanlogin,rolpassword FROM pg_authid WHERE oid=%s", (identity.role_oid,)).fetchone() == (False, None)
            recorded.append(identity)
        identity = create_application_migrator(peer, **authority, migrator_role=name, persist_identity=persist)
        assert recorded == [identity]
        expires = datetime.now(UTC) + timedelta(minutes=45)
        try:
            for _ in range(2):
                arm_application_migrator(peer, **authority, identity=identity, password=password, expires_at=expires)
            with psycopg.connect(transfer_database[0], user=name, password=password, autocommit=True) as active:
                with pytest.raises(psycopg.errors.InsufficientPrivilege):
                    active.execute("CREATE TABLE public.app_migrator_probe(id int)")
                active.execute(sql.SQL("SET ROLE {}").format(sql.Identifier(target.successor_role)))
                active.execute("CREATE TABLE public.app_migrator_probe(id int)")
                for _ in range(2):
                    seal_application_migrator(peer, **authority, identity=identity)
                assert active.execute("SELECT current_user").fetchone() == (target.successor_role,)
                maintenance.execute(sql.SQL("ALTER DATABASE {} ALLOW_CONNECTIONS false").format(sql.Identifier(target.database)))
                retire_application_migrator(maintenance, **authority, migrator_role=name, migrator_oid=identity.role_oid)
                with pytest.raises(psycopg.OperationalError):
                    active.execute("SELECT 1")
            assert peer.execute("SELECT relowner::bigint FROM pg_class WHERE relname='app_migrator_probe'").fetchone() == (target.successor_oid,)
            assert guard.execute("SELECT 1").fetchone() == (1,)
        finally:
            peer.execute(sql.SQL("DROP ROLE IF EXISTS {}").format(sql.Identifier(name)))


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["journal", "guard", "collision"])
async def test_migrator_creation_never_adopts_ambient_role_or_outlives_failed_journal(transfer_database, failure):  # noqa: F811
    from loom.application_migrator_provision import create_application_migrator

    with _closed(transfer_database) as (peer, maintenance, guard, args):
        complete_application_handoff_database(peer, maintenance=maintenance, **args)
        name = "app_migrator_" + uuid4().hex
        if failure == "collision":
            peer.execute(sql.SQL("CREATE ROLE {} NOLOGIN NOINHERIT").format(sql.Identifier(name)))
        recorded = []
        def persist(identity):
            recorded.append(identity)
            if failure == "journal":
                raise RuntimeError("journal publication failed")
            if failure == "guard":
                guard.execute("SELECT pg_advisory_unlock_all()")
        try:
            with pytest.raises(RuntimeError):
                create_application_migrator(peer, target=args["target"], coordination_guard=args["coordination_guard"],
                    provisioner_role=next(n for n, a in args["role_bindings"].items() if a == "provisioner"),
                    migrator_role=name, persist_identity=persist)
            assert bool(peer.execute("SELECT 1 FROM pg_roles WHERE rolname=%s", (name,)).fetchone()) == (failure == "collision")
            assert bool(recorded) == (failure != "collision")
        finally:
            peer.execute(sql.SQL("DROP ROLE IF EXISTS {}").format(sql.Identifier(name)))
