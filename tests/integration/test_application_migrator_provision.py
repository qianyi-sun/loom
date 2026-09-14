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
@pytest.mark.parametrize("drift", ["closed", "expired", "long-lease", "oid", "credential", "expiry", "membership", "role-setting"])
async def test_migrator_arming_refuses_changed_admission_or_saved_authority(transfer_database, drift):  # noqa: F811
    from dataclasses import replace

    from loom.application_migrator_provision import (
        arm_application_migrator,
        create_application_migrator,
    )

    with _closed(transfer_database) as (peer, maintenance, _guard, args):
        complete_application_handoff_database(peer, maintenance=maintenance, **args)
        target = args["target"]
        name, password = "app_migrator_" + uuid4().hex, uuid4().hex
        authority = dict(target=target, coordination_guard=args["coordination_guard"],
            provisioner_role=next(n for n, a in args["role_bindings"].items() if a == "provisioner"))
        identity = create_application_migrator(peer, **authority, migrator_role=name, persist_identity=lambda _: None)
        expiry = datetime.now(UTC) + timedelta(minutes=45)
        try:
            if drift in {"credential", "expiry"}:
                arm_application_migrator(peer, **authority, identity=identity, password=password, expires_at=expiry)
            if drift == "closed":
                maintenance.execute(sql.SQL("ALTER DATABASE {} ALLOW_CONNECTIONS false").format(sql.Identifier(target.database)))
            elif drift == "expired":
                expiry = datetime.now(UTC) - timedelta(seconds=1)
            elif drift == "long-lease":
                expiry = datetime.now(UTC) + timedelta(hours=2)
            elif drift == "oid":
                identity = replace(identity, role_oid=identity.role_oid + 1)
            elif drift == "credential":
                password += "changed"
            elif drift == "expiry":
                expiry += timedelta(seconds=1)
            elif drift == "membership":
                peer.execute(sql.SQL("GRANT pg_read_all_data TO {}").format(sql.Identifier(name)))
            elif drift == "role-setting":
                peer.execute(sql.SQL("ALTER ROLE {} SET role TO {}").format(sql.Identifier(name), sql.Identifier(target.successor_role)))
            with pytest.raises(RuntimeError):
                arm_application_migrator(peer, **authority, identity=identity, password=password, expires_at=expiry)
            assert peer.execute("SELECT rolcanlogin FROM pg_roles WHERE rolname=%s", (name,)).fetchone() == (drift in {"credential", "expiry"},)
        finally:
            peer.execute(sql.SQL("REVOKE ALL ON DATABASE {} FROM {}; DROP ROLE {}").format(
                sql.Identifier(target.database), sql.Identifier(name), sql.Identifier(name)))


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


@pytest.mark.asyncio
@pytest.mark.parametrize("transfer_database", ["baseline"], indirect=True)
async def test_actual_baseline_upgrade_preserves_separated_runtime_authority(transfer_database, monkeypatch):  # noqa: F811
    from pathlib import Path

    from alembic import command
    from alembic.config import Config
    from sqlalchemy.engine import make_url

    from loom.application_completed_authority import observe_completed_application_authority
    from loom.application_migrator_admission import (
        close_application_migrator_admission,
        reopen_application_migrator_admission,
    )
    from loom.application_migrator_provision import (
        arm_application_migrator,
        create_application_migrator,
        seal_application_migrator,
    )
    from loom.application_migrator_retirement import retire_application_migrator

    with _closed(transfer_database) as (peer, maintenance, _guard, args):
        args.update(schema_revision="0134/guard_0030", schema_acl_profile="cnpg-staging")
        complete_application_handoff_database(peer, maintenance=maintenance, **args)
        target = args["target"]
        authority = dict(target=target, coordination_guard=args["coordination_guard"],
            provisioner_role=next(n for n, a in args["role_bindings"].items() if a == "provisioner"))
        identity = create_application_migrator(peer, **authority, migrator_role="app_migrator_" + uuid4().hex,
            persist_identity=lambda _: None)
        password = uuid4().hex
        arm_application_migrator(peer, **authority, identity=identity, password=password,
            expires_at=datetime.now(UTC) + timedelta(minutes=45))
        root = Path(__file__).resolve().parents[2]
        config = Config(str(root / "migrations/alembic.ini"))
        config.set_main_option("script_location", str(root / "migrations"))
        url = make_url(transfer_database[0]).set(drivername="postgresql+psycopg", username=identity.role_name, password=password)
        config.set_main_option("sqlalchemy.url", url.render_as_string(hide_password=False).replace("%", "%%"))
        monkeypatch.setenv("LOOM_DB_OWNER_ROLE", target.successor_role)
        try:
            command.upgrade(config, "0142")
            assert peer.execute("SELECT version_num FROM public.alembic_version").fetchone() == ("0142",)
            observe_completed_application_authority(peer, target=target, runtime_password=args["password"], successor=identity)
        finally:
            seal_application_migrator(maintenance, **authority, identity=identity)
            close_application_migrator_admission(maintenance, **authority, identity=identity)
            retire_application_migrator(maintenance, **authority, migrator_role=identity.role_name, migrator_oid=identity.role_oid)
            reopen_application_migrator_admission(maintenance, **authority, identity=identity, runtime_password=args["password"])
        observe_completed_application_authority(peer, target=target, runtime_password=args["password"])
