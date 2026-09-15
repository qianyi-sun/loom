"""Committed login sealing is not session retirement or ownership transfer."""

import time
from concurrent.futures import ThreadPoolExecutor
from uuid import uuid4

import psycopg
import pytest
from psycopg import sql
from sqlalchemy.engine import make_url

from loom.application_login_sealing import seal_application_login
from tests.integration.test_application_ownership_transfer import (
    transfer_postgres,  # noqa: F401
    transfer_postgres_url,  # noqa: F401
)

pytestmark = pytest.mark.parametrize("transfer_postgres", [16, 17], indirect=True)


@pytest.fixture
def login_database(transfer_postgres_url):  # noqa: F811
    root = make_url(transfer_postgres_url).set(drivername="postgresql")
    suffix = uuid4().hex
    database, role, other = ("seal_db_" + suffix, "seal_app_" + suffix, "seal_other_" + suffix)
    password = uuid4().hex
    with psycopg.connect(
        root.render_as_string(hide_password=False), autocommit=True
    ) as maintenance:
        maintenance.execute(
            sql.SQL("CREATE ROLE {} LOGIN NOINHERIT PASSWORD {}").format(
                sql.Identifier(role), sql.Literal(password)
            )
        )
        maintenance.execute(sql.SQL("CREATE ROLE {} NOLOGIN").format(sql.Identifier(other)))
        maintenance.execute(
            sql.SQL("CREATE DATABASE {} OWNER {}").format(
                sql.Identifier(database), sql.Identifier(role)
            )
        )
        url = root.set(database=database).render_as_string(hide_password=False)
        client_url = root.set(database=database, username=role, password=password).render_as_string(
            hide_password=False
        )
        try:
            with psycopg.connect(url, autocommit=True) as admin:
                yield admin, database, role, other, client_url, str(root.username)
        finally:
            maintenance.execute(
                sql.SQL("DROP DATABASE {} WITH (FORCE)").format(sql.Identifier(database))
            )
            maintenance.execute(
                sql.SQL("DROP ROLE {}, {}").format(sql.Identifier(role), sql.Identifier(other))
            )


def _state(admin, role):
    return admin.execute(
        "SELECT oid, rolcanlogin, rolpassword, rolinherit FROM pg_catalog.pg_authid WHERE rolname=%s",
        (role,),
    ).fetchone()


def test_seal_commits_disables_new_login_and_preserves_existing_session(login_database):
    admin, database, role, _, client_url, provisioner = login_database
    with psycopg.connect(client_url, autocommit=True) as existing:
        before = _state(admin, role)
        for _ in range(2):
            seal_application_login(
                admin, database=database, role=role, provisioner_role=provisioner
            )
        assert _state(admin, role) == (before[0], False, None, False)
        with pytest.raises(psycopg.OperationalError):
            with psycopg.connect(client_url, connect_timeout=2):
                pass
        # LOGIN sealing must not be advertised as DDL/session retirement.
        existing.execute("CREATE TABLE public.still_owned(id integer)")
        existing.execute("INSERT INTO public.still_owned VALUES (42)")
        assert admin.execute("SELECT id FROM public.still_owned").fetchall() == [(42,)]


def test_seal_converges_membership_free_legacy_inherit_atomically_and_replays(login_database):
    admin, database, role, other, client_url, provisioner = login_database
    admin.execute(sql.SQL("ALTER ROLE {} INHERIT").format(sql.Identifier(role)))
    before, foreign_before = _state(admin, role), _state(admin, other)
    assert before[1] is True and before[3] is True
    with psycopg.connect(client_url, autocommit=True) as existing:
        for _ in range(2):
            seal_application_login(
                admin, database=database, role=role, provisioner_role=provisioner
            )
            assert _state(admin, role) == (before[0], False, None, False)
            assert _state(admin, other) == foreign_before
        with pytest.raises(psycopg.OperationalError):
            psycopg.connect(client_url, connect_timeout=2).close()
        # INHERIT convergence still does not retire an existing owner session.
        existing.execute("CREATE TABLE public.still_owned_after_convergence(id integer)")


@pytest.mark.parametrize("inherit", [False, True])
@pytest.mark.parametrize(
    "drift",
    [
        "incoming_membership",
        "outgoing_membership",
        "superuser",
        "foreign_schema",
        "wrong_database",
        "wrong_role",
    ],
)
def test_seal_refuses_scope_drift_without_modifying_credentials(login_database, drift, inherit):
    admin, database, role, other, _, provisioner = login_database
    if inherit:
        admin.execute(sql.SQL("ALTER ROLE {} INHERIT").format(sql.Identifier(role)))
    if drift == "incoming_membership":
        admin.execute(sql.SQL("GRANT {} TO {}").format(sql.Identifier(role), sql.Identifier(other)))
    elif drift == "outgoing_membership":
        admin.execute(sql.SQL("GRANT {} TO {}").format(sql.Identifier(other), sql.Identifier(role)))
    elif drift == "superuser":
        admin.execute(sql.SQL("ALTER ROLE {} SUPERUSER").format(sql.Identifier(role)))
    elif drift == "foreign_schema":
        admin.execute(
            sql.SQL("CREATE SCHEMA foreign_work AUTHORIZATION {}").format(sql.Identifier(role))
        )
    before = _state(admin, role)
    with pytest.raises(RuntimeError):
        seal_application_login(
            admin,
            database="wrong" if drift == "wrong_database" else database,
            role=other if drift == "wrong_role" else role,
            provisioner_role=provisioner,
        )
    assert _state(admin, role) == before


def test_seal_requires_own_fresh_administrator_transaction(login_database):
    admin, database, role, _, client_url, provisioner = login_database
    before = _state(admin, role)
    with admin.transaction(), pytest.raises(RuntimeError, match="idle"):
        seal_application_login(admin, database=database, role=role, provisioner_role=provisioner)
    with psycopg.connect(client_url, autocommit=True) as ordinary:
        with pytest.raises(RuntimeError, match="administrator"):
            seal_application_login(ordinary, database=database, role=role, provisioner_role=role)
    assert _state(admin, role) == before


@pytest.mark.parametrize("inherit", [False, True])
def test_seal_failure_after_alter_rolls_back_password_and_login(login_database, inherit):
    admin, database, role, _, _, provisioner = login_database
    if inherit:
        admin.execute(sql.SQL("ALTER ROLE {} INHERIT").format(sql.Identifier(role)))

    class FailedAfterAlter:
        @property
        def info(self):
            return admin.info

        def transaction(self):
            return admin.transaction()

        def execute(self, query):
            result = admin.execute(query)
            statement = query if isinstance(query, str) else query.as_string()
            if statement.startswith("ALTER ROLE"):
                raise RuntimeError("injected post-alter failure")
            return result

    before = _state(admin, role)
    with pytest.raises(RuntimeError, match="injected"):
        seal_application_login(
            FailedAfterAlter(), database=database, role=role, provisioner_role=provisioner
        )
    assert _state(admin, role) == before


def test_seal_preserves_unrelated_connected_role(login_database):
    admin, database, role, other, _, provisioner = login_database
    other_password = uuid4().hex
    admin.execute(
        sql.SQL("ALTER ROLE {} LOGIN PASSWORD {}").format(
            sql.Identifier(other), sql.Literal(other_password)
        )
    )
    # Build from psycopg's connection info without logging its password.
    with psycopg.connect(
        host=admin.info.host,
        port=admin.info.port,
        dbname=database,
        user=other,
        password=other_password,
        autocommit=True,
    ) as foreign:
        before = _state(admin, other)
        seal_application_login(admin, database=database, role=role, provisioner_role=provisioner)
        assert foreign.execute("SELECT 42").fetchone() == (42,)
        assert _state(admin, other) == before


@pytest.mark.parametrize("inherit", [False, True])
def test_seal_refuses_role_in_use_in_another_database_without_termination(login_database, inherit):
    admin, database, role, _, client_url, provisioner = login_database
    if inherit:
        admin.execute(sql.SQL("ALTER ROLE {} INHERIT").format(sql.Identifier(role)))
    with psycopg.connect(
        make_url(client_url).set(database="postgres").render_as_string(hide_password=False),
        autocommit=True,
    ) as foreign:
        before = _state(admin, role)
        with pytest.raises(RuntimeError, match="foreign sessions"):
            seal_application_login(
                admin, database=database, role=role, provisioner_role=provisioner
            )
        assert _state(admin, role) == before
        assert foreign.execute("SELECT 42").fetchone() == (42,)


@pytest.mark.parametrize("inherit", [False, True])
def test_seal_refuses_foreign_database_authority(login_database, inherit):
    admin, database, role, _, _, provisioner = login_database
    if inherit:
        admin.execute(sql.SQL("ALTER ROLE {} INHERIT").format(sql.Identifier(role)))
    foreign_database = "seal_foreign_" + uuid4().hex
    admin.execute(
        sql.SQL("CREATE DATABASE {} OWNER {}").format(
            sql.Identifier(foreign_database), sql.Identifier(role)
        )
    )
    try:
        before = _state(admin, role)
        with pytest.raises(RuntimeError, match="foreign role dependencies"):
            seal_application_login(
                admin, database=database, role=role, provisioner_role=provisioner
            )
        assert _state(admin, role) == before
        assert admin.execute(
            "SELECT pg_get_userbyid(datdba) FROM pg_database WHERE datname=%s", (foreign_database,)
        ).fetchone() == (role,)
    finally:
        admin.execute(sql.SQL("DROP DATABASE {}").format(sql.Identifier(foreign_database)))


def test_seal_requires_read_committed_and_restores_session_settings(login_database):
    admin, database, role, _, _, provisioner = login_database
    before = _state(admin, role)
    admin.execute("SET default_transaction_isolation='repeatable read'")
    with pytest.raises(RuntimeError, match="READ COMMITTED"):
        seal_application_login(admin, database=database, role=role, provisioner_role=provisioner)
    assert _state(admin, role) == before
    admin.execute("SET default_transaction_isolation='read committed'")
    admin.execute(
        "SET search_path=public,pg_catalog; SET lock_timeout='50ms'; SET statement_timeout='1s'"
    )
    seal_application_login(admin, database=database, role=role, provisioner_role=provisioner)
    assert admin.execute(
        "SELECT current_setting('search_path'),current_setting('lock_timeout'),current_setting('statement_timeout')"
    ).fetchone() == ("public, pg_catalog", "50ms", "1s")


def test_preseal_login_can_appear_after_committed_seal_and_empty_session_snapshot(login_database):
    admin, database, role, _, client_url, provisioner = login_database
    # PG16/17 take their startup database object lock after checking LOGIN, then
    # applies this startup delay before publishing pg_stat_activity.
    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(
            psycopg.connect,
            client_url,
            autocommit=True,
            connect_timeout=10,
            options="-c post_auth_delay=3",
        )
        try:
            deadline = time.monotonic() + 2
            while not admin.execute(
                "SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_locks WHERE locktype='object' "
                "AND classid='pg_catalog.pg_database'::regclass AND objid=(SELECT oid "
                "FROM pg_catalog.pg_database WHERE datname=%s) AND mode='RowExclusiveLock' "
                "AND granted AND pid<>pg_backend_pid())",
                (database,),
            ).fetchone()[0]:
                assert time.monotonic() < deadline, "startup never reached its database lock"
                time.sleep(0.01)
            assert not future.done()
            seal_application_login(
                admin, database=database, role=role, provisioner_role=provisioner
            )
            admin.execute("SELECT pg_catalog.pg_stat_clear_snapshot()")
            assert admin.execute(
                "SELECT count(*) FROM pg_catalog.pg_stat_activity WHERE usename=%s", (role,)
            ).fetchone() == (0,)
            assert not future.done()
            with future.result(timeout=10) as late:
                assert late.execute("SELECT 42").fetchone() == (42,)
                assert _state(admin, role)[1:3] == (False, None)
        finally:
            if not future.cancel():
                future.result(timeout=10).close()


@pytest.mark.parametrize("authority", ["CREATE", "USAGE WITH GRANT OPTION", "bridge-grant-option"])
def test_seal_refuses_excess_private_authority_without_clearing_credentials(
    login_database, authority
):
    admin, database, role, _, _, provisioner = login_database
    admin.execute("CREATE SCHEMA loom_capacity_guard")
    if authority == "bridge-grant-option":
        admin.execute(
            "CREATE FUNCTION loom_capacity_guard.close_protected_runtime_trial_claim(uuid,text,text,uuid,integer) RETURNS void LANGUAGE plpgsql AS $$BEGIN RETURN; END$$"
        )
        admin.execute(
            sql.SQL(
                "GRANT EXECUTE ON FUNCTION loom_capacity_guard.close_protected_runtime_trial_claim(uuid,text,text,uuid,integer) TO {} WITH GRANT OPTION"
            ).format(sql.Identifier(role))
        )
    else:
        privilege = "CREATE" if authority == "CREATE" else "USAGE"
        suffix = "" if authority == "CREATE" else " WITH GRANT OPTION"
        admin.execute(
            sql.SQL("GRANT " + privilege + " ON SCHEMA loom_capacity_guard TO {}" + suffix).format(
                sql.Identifier(role)
            )
        )
    before = _state(admin, role)
    with pytest.raises(RuntimeError, match="private authority"):
        seal_application_login(admin, database=database, role=role, provisioner_role=provisioner)
    assert _state(admin, role) == before


def test_seal_protects_search_path_before_any_name_comparison(login_database):
    admin, database, role, _, _, provisioner = login_database
    admin.execute(
        "CREATE FUNCTION public.hostile_name_equal(name,name) RETURNS boolean LANGUAGE plpgsql AS $$BEGIN RAISE EXCEPTION 'untrusted name operator executed'; END$$"
    )
    admin.execute(
        "CREATE OPERATOR public.= (LEFTARG=pg_catalog.name, RIGHTARG=pg_catalog.name, FUNCTION=public.hostile_name_equal)"
    )
    admin.execute("SET search_path=public,pg_catalog")
    seal_application_login(admin, database=database, role=role, provisioner_role=provisioner)
    admin.execute("SET search_path=pg_catalog,pg_temp")
    assert _state(admin, role)[1:3] == (False, None)
