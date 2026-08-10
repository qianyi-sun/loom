"""Database authority tests for the trusted read-only demand agent."""

from __future__ import annotations

import os
from pathlib import Path
from uuid import uuid4

import pytest
from alembic import command
from alembic.config import Config as AlembicConfig
from psycopg.errors import InsufficientPrivilege, ObjectNotInPrerequisiteState
from sqlalchemy import create_engine, text
from sqlalchemy.exc import DBAPIError


def _value(database: dict[str, object], key: str) -> str:
    value = database[key]
    assert isinstance(value, str)
    return value


def _guard_config(database: dict[str, object]) -> AlembicConfig:
    root = Path(__file__).resolve().parents[2]
    cfg = AlembicConfig(str(root / "capacity_guard_migrations" / "alembic.ini"))
    cfg.set_main_option("script_location", str(root / "capacity_guard_migrations"))
    os.environ["LOOM_CAPACITY_GUARD_DB_URL"] = _value(database, "migrator_url")
    os.environ["LOOM_CAPACITY_GUARD_OWNER_ROLE"] = _value(database, "owner_role")
    os.environ["LOOM_CAPACITY_GUARD_AGENT_ROLE"] = _value(database, "agent_role")
    return cfg


def test_agent_role_is_least_privileged_and_not_an_owner_member(
    capacity_guard_database: dict[str, object],
) -> None:
    engine = create_engine(_value(capacity_guard_database, "admin_url"))
    try:
        with engine.connect() as connection:
            role = (
                connection.execute(
                    text(
                        "SELECT rolcanlogin, rolinherit, rolsuper, rolcreatedb, "
                        "rolcreaterole, rolreplication, rolbypassrls, "
                        "pg_has_role(:agent, :owner, 'MEMBER') AS owner_member, "
                        "(SELECT count(*) FROM pg_auth_members AS m "
                        "JOIN pg_roles AS r ON r.oid = m.member "
                        "WHERE r.rolname = :agent) AS role_memberships "
                        "FROM pg_roles WHERE rolname = :agent"
                    ),
                    {
                        "agent": _value(capacity_guard_database, "agent_role"),
                        "owner": _value(capacity_guard_database, "owner_role"),
                    },
                )
                .mappings()
                .one()
            )
        assert dict(role) == {
            "rolcanlogin": True,
            "rolinherit": False,
            "rolsuper": False,
            "rolcreatedb": False,
            "rolcreaterole": False,
            "rolreplication": False,
            "rolbypassrls": False,
            "owner_member": False,
            "role_memberships": 0,
        }
    finally:
        engine.dispose()


def test_agent_can_execute_only_the_bounded_capture_function(
    capacity_guard_database: dict[str, object],
) -> None:
    admin = create_engine(_value(capacity_guard_database, "admin_url"))
    agent_role = _value(capacity_guard_database, "agent_role")
    try:
        with admin.connect() as connection:
            privileges = (
                connection.execute(
                    text(
                        "SELECT "
                        "has_schema_privilege(:agent, 'loom_capacity_guard', 'USAGE') "
                        "AS schema_usage, "
                        "has_table_privilege(:agent, "
                        "'loom_capacity_guard.agent_registrations', 'SELECT') AS table_select, "
                        "has_table_privilege(:agent, "
                        "'loom_capacity_guard.agent_reporter_state', 'UPDATE') AS table_update, "
                        "has_sequence_privilege(:agent, "
                        "'loom_capacity_guard.demand_observations_observation_id_seq', 'USAGE') "
                        "AS sequence_usage, "
                        "has_function_privilege(:agent, "
                        "'loom_capacity_guard.capture_demand_observation(uuid,bigint,integer)', "
                        "'EXECUTE') AS capture_execute, "
                        "has_function_privilege(:agent, "
                        "'loom_capacity_guard.reject_append_only_mutation()', 'EXECUTE') "
                        "AS trigger_execute"
                    ),
                    {"agent": agent_role},
                )
                .mappings()
                .one()
            )
        assert dict(privileges) == {
            "schema_usage": True,
            "table_select": False,
            "table_update": False,
            "sequence_usage": False,
            "capture_execute": True,
            "trigger_execute": False,
        }
    finally:
        admin.dispose()

    agent = create_engine(_value(capacity_guard_database, "agent_url"))
    try:
        with agent.connect() as connection:
            with pytest.raises(DBAPIError) as denied:
                connection.execute(
                    text("SELECT * FROM loom_capacity_guard.agent_registrations")
                )
            assert isinstance(denied.value.orig, InsufficientPrivilege)

        with agent.connect() as connection:
            with pytest.raises(DBAPIError, match="not registered") as unregistered:
                connection.execute(
                    text(
                        "SELECT loom_capacity_guard.capture_demand_observation("
                        ":agent_incarnation, 0, 100)"
                    ),
                    {"agent_incarnation": uuid4()},
                )
            assert isinstance(unregistered.value.orig, ObjectNotInPrerequisiteState)
    finally:
        agent.dispose()


def test_capture_function_is_fixed_search_path_security_definer(
    capacity_guard_database: dict[str, object],
) -> None:
    engine = create_engine(_value(capacity_guard_database, "admin_url"))
    try:
        with engine.connect() as connection:
            row = (
                connection.execute(
                    text(
                        "SELECT p.prosecdef, p.proconfig, pg_get_userbyid(p.proowner) AS owner "
                        "FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace "
                        "WHERE n.nspname = 'loom_capacity_guard' "
                        "AND p.proname = 'capture_demand_observation'"
                    )
                )
                .mappings()
                .one()
            )
        assert row["prosecdef"] is True
        assert row["proconfig"] == ["search_path=pg_catalog"]
        assert row["owner"] == _value(capacity_guard_database, "owner_role")
    finally:
        engine.dispose()


def test_guard_migration_rejects_missing_direct_demand_source_privilege(
    capacity_guard_database: dict[str, object],
) -> None:
    engine = create_engine(_value(capacity_guard_database, "admin_url"))
    owner = _value(capacity_guard_database, "owner_role")
    quoted_owner = engine.dialect.identifier_preparer.quote(owner)
    try:
        with engine.begin() as connection:
            connection.exec_driver_sql(
                f"REVOKE SELECT (submit_priority) ON TABLE public.trials FROM {quoted_owner}"
            )
        with pytest.raises(RuntimeError, match="submit_priority"):
            command.current(_guard_config(capacity_guard_database))
    finally:
        with engine.begin() as connection:
            connection.exec_driver_sql(
                f"GRANT SELECT (submit_priority) ON TABLE public.trials TO {quoted_owner}"
            )
        engine.dispose()


def test_guard_migration_rejects_agent_role_memberships(
    capacity_guard_database: dict[str, object],
) -> None:
    engine = create_engine(_value(capacity_guard_database, "admin_url"))
    extra_role = f"guard_agent_extra_test_{uuid4().hex[:12]}"
    quoted_extra = engine.dialect.identifier_preparer.quote(extra_role)
    quoted_agent = engine.dialect.identifier_preparer.quote(
        _value(capacity_guard_database, "agent_role")
    )
    try:
        with engine.begin() as connection:
            connection.exec_driver_sql(
                f"CREATE ROLE {quoted_extra} NOLOGIN NOSUPERUSER NOCREATEDB "
                "NOCREATEROLE NOINHERIT NOREPLICATION NOBYPASSRLS"
            )
            connection.exec_driver_sql(f"GRANT {quoted_extra} TO {quoted_agent}")
        with pytest.raises(RuntimeError, match="least-privileged"):
            command.current(_guard_config(capacity_guard_database))
    finally:
        with engine.begin() as connection:
            connection.exec_driver_sql(f"REVOKE {quoted_extra} FROM {quoted_agent}")
            connection.exec_driver_sql(f"DROP ROLE IF EXISTS {quoted_extra}")
        engine.dispose()
