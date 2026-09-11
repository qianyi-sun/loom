"""Transactional, exact definer handoff; not complete application owner sealing."""

from collections.abc import Iterator
from uuid import uuid4

import pytest
from sqlalchemy import Engine, create_engine, text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.pool import NullPool

from loom.trial_writer_trigger_authority import application_trigger_owner_handoff_ddl
from tests.integration.test_capacity_agent_store import _value

_PUBLIC = (
    "loom_drop_trial_writer_triggers",
    "loom_close_protected_runtime_trial_claim",
    "loom_transform_protected_runtime_trial_requeue",
)
_PRIVATE = (
    "loom_capacity_guard.close_protected_runtime_trial_claim(uuid,text,text,uuid,integer)",
    "loom_capacity_guard.transform_protected_runtime_trial_requeue"
    "(uuid,text,uuid,integer,uuid,integer,text,text,timestamp with time zone)",
)


@pytest.fixture
def handoff_roles(
    capacity_guard_database: dict[str, object],
) -> Iterator[tuple[Engine, str, str, str]]:
    database = capacity_guard_database
    engine = create_engine(_value(database, "admin_url"))
    previous, target = ("app_previous_" + uuid4().hex, "app_target_" + uuid4().hex)
    guard = _value(database, "owner_role")
    quote = engine.dialect.identifier_preparer.quote
    with engine.begin() as admin:
        original = admin.execute(text("SELECT current_user")).scalar_one()
        for role in (previous, target):
            admin.exec_driver_sql(
                f"CREATE ROLE {quote(role)} NOLOGIN NOINHERIT NOSUPERUSER NOCREATEDB "
                "NOCREATEROLE NOREPLICATION NOBYPASSRLS"
            )
        admin.exec_driver_sql(f"ALTER TABLE ONLY public.trials OWNER TO {quote(previous)}")
        for name in _PUBLIC:
            admin.exec_driver_sql(f"ALTER FUNCTION public.{name}() OWNER TO {quote(previous)}")
        for signature in _PRIVATE:
            admin.exec_driver_sql(f"GRANT EXECUTE ON FUNCTION {signature} TO {quote(previous)}")
        admin.exec_driver_sql(
            f"GRANT USAGE ON SCHEMA public, loom_capacity_guard TO {quote(previous)}"
        )
    try:
        yield engine, previous, target, guard
    finally:
        with engine.begin() as admin:
            admin.exec_driver_sql(f"ALTER TABLE ONLY public.trials OWNER TO {quote(original)}")
            for name in _PUBLIC:
                admin.exec_driver_sql(f"ALTER FUNCTION public.{name}() OWNER TO {quote(original)}")
                for role in (previous, target):
                    admin.exec_driver_sql(
                        f"REVOKE ALL ON FUNCTION public.{name}() FROM {quote(role)}"
                    )
            for role in (previous, target):
                for signature in _PRIVATE:
                    admin.exec_driver_sql(f"REVOKE ALL ON FUNCTION {signature} FROM {quote(role)}")
                admin.exec_driver_sql(f"REVOKE ALL ON public.trials FROM {quote(role)}")
                admin.exec_driver_sql(
                    f"REVOKE ALL ON SCHEMA public, loom_capacity_guard FROM {quote(role)}"
                )
                admin.exec_driver_sql(f"DROP ROLE {quote(role)}")
        engine.dispose()


def _handoff(roles: tuple[Engine, str, str, str]) -> None:
    engine, previous, target, guard = roles
    quote = engine.dialect.identifier_preparer.quote
    with engine.begin() as admin:
        admin.exec_driver_sql("SET LOCAL statement_timeout='5s'")
        admin.exec_driver_sql("LOCK TABLE ONLY public.trials IN ACCESS EXCLUSIVE MODE NOWAIT")
        admin.exec_driver_sql(f"ALTER TABLE ONLY public.trials OWNER TO {quote(target)}")
        admin.exec_driver_sql(
            application_trigger_owner_handoff_ddl(
                previous_owner=previous, application_owner=target, guard_owner=guard
            ).as_string(),
            execution_options={"no_parameters": True},
        )


def test_definer_handoff_moves_exact_owners_and_private_grants_with_replay(
    handoff_roles: tuple[Engine, str, str, str],
) -> None:
    engine, previous, target, _ = handoff_roles
    _handoff(handoff_roles)
    _handoff(handoff_roles)
    with engine.connect() as admin:
        for name in _PUBLIC:
            assert (
                admin.execute(
                    text(
                        "SELECT pg_get_userbyid(proowner) FROM pg_proc WHERE oid=to_regprocedure(:name)"
                    ),
                    {"name": f"public.{name}()"},
                ).scalar_one()
                == target
            )
        for signature in _PRIVATE:
            assert admin.execute(
                text(
                    "SELECT has_function_privilege(:previous, :signature, 'EXECUTE'), "
                    "has_function_privilege(:target, :signature, 'EXECUTE')"
                ),
                {"previous": previous, "target": target, "signature": signature},
            ).one() == (False, True)


@pytest.mark.parametrize(
    "drift",
    [
        "helper_body",
        "terminal_body",
        "requeue_body",
        "invoker",
        "search_path",
        "extra_grant",
        "target_login",
        "target_membership",
        "missing_guard_grant",
        "missing_previous_schema_usage",
        "target_bridge_grant_option",
    ],
)
def test_definer_handoff_refuses_drift_and_rolls_back_table_owner(
    handoff_roles: tuple[Engine, str, str, str],
    drift: str,
) -> None:
    engine, previous, target, guard = handoff_roles
    quote = engine.dialect.identifier_preparer.quote
    with engine.begin() as admin:
        if drift.endswith("_body"):
            name, result, body = {
                "helper_body": (_PUBLIC[0], "void", "RETURN;"),
                "terminal_body": (_PUBLIC[1], "trigger", "RETURN NEW;"),
                "requeue_body": (_PUBLIC[2], "trigger", "RETURN NEW;"),
            }[drift]
            admin.exec_driver_sql(
                f"CREATE OR REPLACE FUNCTION public.{name}() RETURNS {result} "
                f"LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$BEGIN {body} END$$"
            )
        elif drift == "invoker":
            admin.exec_driver_sql(f"ALTER FUNCTION public.{_PUBLIC[0]}() SECURITY INVOKER")
        elif drift == "search_path":
            admin.exec_driver_sql(f"ALTER FUNCTION public.{_PUBLIC[0]}() SET search_path=public")
        elif drift == "extra_grant":
            admin.exec_driver_sql(
                f"GRANT EXECUTE ON FUNCTION public.{_PUBLIC[0]}() TO {quote(target)}"
            )
        elif drift == "target_login":
            admin.exec_driver_sql(f"ALTER ROLE {quote(target)} LOGIN")
        elif drift == "missing_guard_grant":
            admin.exec_driver_sql(
                f"REVOKE EXECUTE ON FUNCTION public.{_PUBLIC[0]}() FROM {quote(guard)}"
            )
        elif drift == "missing_previous_schema_usage":
            admin.exec_driver_sql(
                f"REVOKE USAGE ON SCHEMA loom_capacity_guard FROM {quote(previous)}"
            )
        elif drift == "target_bridge_grant_option":
            admin.exec_driver_sql(
                f"GRANT EXECUTE ON FUNCTION {_PRIVATE[0]} TO {quote(target)} WITH GRANT OPTION"
            )
        else:
            admin.exec_driver_sql(f"GRANT {quote(previous)} TO {quote(target)}")
    with pytest.raises(DBAPIError) as refusal:
        _handoff(handoff_roles)
    assert refusal.value.orig.sqlstate in {"42501", "55000"}
    with engine.connect() as admin:
        assert (
            admin.execute(
                text(
                    "SELECT pg_get_userbyid(relowner) FROM pg_class WHERE oid='public.trials'::regclass"
                )
            ).scalar_one()
            == previous
        )
        for name in _PUBLIC:
            assert (
                admin.execute(
                    text(
                        "SELECT pg_get_userbyid(proowner) FROM pg_proc WHERE oid=to_regprocedure(:name)"
                    ),
                    {"name": f"public.{name}()"},
                ).scalar_one()
                == previous
            )


def test_definer_handoff_rejects_ordinary_caller(
    handoff_roles: tuple[Engine, str, str, str],
) -> None:
    engine, previous, target, guard = handoff_roles
    quote = engine.dialect.identifier_preparer.quote
    with pytest.raises(DBAPIError) as refusal:
        with engine.begin() as runtime:
            runtime.exec_driver_sql(f"SET LOCAL ROLE {quote(previous)}")
            runtime.exec_driver_sql(
                application_trigger_owner_handoff_ddl(
                    previous_owner=previous, application_owner=target, guard_owner=guard
                ).as_string(),
                execution_options={"no_parameters": True},
            )
    assert refusal.value.orig.sqlstate == "42501"


def test_definer_handoff_rejects_foreign_trigger_attachment_without_mutating_it(
    handoff_roles: tuple[Engine, str, str, str],
) -> None:
    engine, previous, _, _ = handoff_roles
    with engine.begin() as admin:
        admin.exec_driver_sql("CREATE TABLE public.foreign_trigger_source (id integer)")
        admin.exec_driver_sql(
            "CREATE TRIGGER foreign_terminal AFTER UPDATE ON public.foreign_trigger_source "
            "FOR EACH ROW EXECUTE FUNCTION public.loom_close_protected_runtime_trial_claim()"
        )
    try:
        with pytest.raises(DBAPIError) as refusal:
            _handoff(handoff_roles)
        assert refusal.value.orig.sqlstate == "55000"
        with engine.connect() as admin:
            assert (
                admin.execute(
                    text(
                        "SELECT count(*) FROM pg_trigger WHERE tgrelid='public.foreign_trigger_source'::regclass AND tgname='foreign_terminal'"
                    )
                ).scalar_one()
                == 1
            )
            assert (
                admin.execute(
                    text(
                        "SELECT pg_get_userbyid(relowner) FROM pg_class WHERE oid='public.trials'::regclass"
                    )
                ).scalar_one()
                == previous
            )
    finally:
        with engine.begin() as admin:
            admin.exec_driver_sql("DROP TABLE public.foreign_trigger_source")


@pytest.mark.parametrize("same_database", [True, False])
def test_definer_handoff_scopes_stale_set_role_sessions_to_its_database(
    handoff_roles: tuple[Engine, str, str, str],
    same_database: bool,
) -> None:
    engine, previous, _, _ = handoff_roles
    alias = "old_migrator_" + uuid4().hex
    password = uuid4().hex
    quote = engine.dialect.identifier_preparer.quote
    with engine.begin() as admin:
        admin.exec_driver_sql(f"CREATE ROLE {quote(alias)} LOGIN PASSWORD '{password}'")
        admin.exec_driver_sql(f"GRANT {quote(previous)} TO {quote(alias)}")
    stale_engine = create_engine(
        engine.url.set(
            username=alias,
            password=password,
            database=engine.url.database if same_database else "postgres",
        ),
        poolclass=NullPool,
    )
    try:
        with stale_engine.connect() as stale:
            stale.exec_driver_sql(f"SET ROLE {quote(previous)}")
            stale.commit()
            with engine.begin() as admin:
                admin.exec_driver_sql(f"REVOKE {quote(previous)} FROM {quote(alias)}")
                admin.exec_driver_sql(f"ALTER ROLE {quote(alias)} NOLOGIN PASSWORD NULL")
            assert stale.execute(text("SELECT current_user")).scalar_one() == previous
            stale.commit()
            if same_database:
                with pytest.raises(DBAPIError, match="quiescent legacy authority"):
                    _handoff(handoff_roles)
            else:
                _handoff(handoff_roles)
                assert stale.execute(text("SELECT current_user")).scalar_one() == previous
        # Ending the actual backend, not merely revoking its membership, makes
        # the protected substep eligible. Connections in other databases remain.
        _handoff(handoff_roles)
    finally:
        stale_engine.dispose()
        with engine.begin() as admin:
            admin.exec_driver_sql(f"DROP ROLE {quote(alias)}")


def test_definer_handoff_refuses_busy_catalog_and_releases_partial_locks(
    handoff_roles: tuple[Engine, str, str, str],
) -> None:
    engine, previous, _, _ = handoff_roles
    with engine.connect() as holder:
        holder.exec_driver_sql("ALTER FUNCTION public.loom_drop_trial_writer_triggers() COST 100")
        with pytest.raises(DBAPIError) as busy:
            _handoff(handoff_roles)
        assert busy.value.orig.sqlstate == "55P03"
        assert "lock timeout" in str(busy.value.orig)
        with engine.begin() as probe:
            probe.exec_driver_sql("SET LOCAL lock_timeout='100ms'")
            probe.exec_driver_sql(
                "SELECT singleton_id FROM loom_capacity_guard.trial_writer_fence FOR UPDATE NOWAIT"
            )
            probe.exec_driver_sql(
                "ALTER FUNCTION public.loom_close_protected_runtime_trial_claim() COST 100"
            )
            assert (
                probe.execute(
                    text(
                        "SELECT pg_get_userbyid(relowner) FROM pg_class WHERE oid='public.trials'::regclass"
                    )
                ).scalar_one()
                == previous
            )
        holder.rollback()
    _handoff(handoff_roles)
