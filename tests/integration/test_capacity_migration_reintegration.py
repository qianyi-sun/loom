"""Task 1 RED/GREEN contract for reintegrated executable-capacity migrations."""

from __future__ import annotations

import os
from collections.abc import Iterator, Sequence
from pathlib import Path
from uuid import uuid4

import pytest
from alembic import command
from alembic.config import Config as AlembicConfig
from alembic.script import ScriptDirectory
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import Connection, make_url

UPSTREAM_ALLOCATION_ROUTINES = (
    "capacity_allocation_binding_guard",
    "capacity_allocation_epoch_binding_guard",
    "capacity_executable_allocation_admission_guard",
    "capacity_executable_allocation_seal_guard",
)

UPSTREAM_ALLOCATION_TRIGGERS = (
    "capacity_allocation_binding_guard",
    "capacity_allocation_epoch_binding_guard",
    "capacity_executable_allocation_admission_guard",
    "capacity_executable_allocation_seal_guard",
)

UPSTREAM_ALLOCATION_EVENT_TRIGGERS = (
    "capacity_allocation_epoch_truncate_guard",
    "capacity_allocation_truncate_guard",
)

BRIDGE_COMPLETION_TABLES = {
    "capacity_executable_command_receipts",
    "capacity_executable_executor_states",
    "capacity_executable_intents",
    "capacity_executable_launch_rate_buckets",
}

BRIDGE_COMPLETION_COLUMNS = {
    "capacity_execution_epochs": {
        "drain_actor",
        "drain_idempotency_key",
        "drain_only_at",
        "drain_request_digest",
        "drain_request_payload",
        "retired_at",
        "retirement_actor",
        "retirement_idempotency_key",
        "retirement_request_digest",
        "retirement_request_payload",
    },
    "capacity_authority_state": {
        "execution_epoch",
        "execution_manifest_sha256",
        "execution_state",
        "executable_new_capacity_ceiling",
    },
}


def _capacity_config(url: str) -> AlembicConfig:
    root = Path(__file__).resolve().parents[2]
    cfg = AlembicConfig(str(root / "capacity_migrations" / "alembic.ini"))
    cfg.set_main_option("script_location", str(root / "capacity_migrations"))
    os.environ["LOOM_CAPACITY_DB_URL"] = url
    return cfg


def _capacity_config_without_database() -> AlembicConfig:
    root = Path(__file__).resolve().parents[2]
    cfg = AlembicConfig()
    cfg.set_main_option("script_location", str(root / "capacity_migrations"))
    return cfg


def _normalize_sql(value: str) -> str:
    return " ".join(value.split())


def _routine_definitions(connection: Connection, names: Sequence[str]) -> dict[str, str]:
    rows = connection.execute(
        text(
            """
            SELECT p.proname, pg_get_functiondef(p.oid) AS definition
            FROM pg_proc AS p
            JOIN pg_namespace AS n ON n.oid = p.pronamespace
            WHERE n.nspname = 'public' AND p.proname = ANY(:names)
            ORDER BY p.proname
            """
        ),
        {"names": list(names)},
    ).mappings()
    return {str(row["proname"]): _normalize_sql(str(row["definition"])) for row in rows}


def _trigger_definitions(connection: Connection, names: Sequence[str]) -> dict[str, str]:
    rows = connection.execute(
        text(
            """
            SELECT tgname, pg_get_triggerdef(oid, true) AS definition
            FROM pg_trigger
            WHERE NOT tgisinternal AND tgname = ANY(:names)
            ORDER BY tgname
            """
        ),
        {"names": list(names)},
    ).mappings()
    return {str(row["tgname"]): _normalize_sql(str(row["definition"])) for row in rows}


def _constraint_definitions(
    connection: Connection,
    table_names: Sequence[str],
) -> dict[tuple[str, str], str]:
    rows = connection.execute(
        text(
            """
            SELECT rel.relname AS table_name,
                   con.conname AS constraint_name,
                   pg_get_constraintdef(con.oid, true) AS definition
            FROM pg_constraint AS con
            JOIN pg_class AS rel ON rel.oid = con.conrelid
            JOIN pg_namespace AS nsp ON nsp.oid = rel.relnamespace
            WHERE nsp.nspname = 'public'
              AND rel.relname = ANY(:table_names)
            ORDER BY rel.relname, con.conname
            """
        ),
        {"table_names": list(table_names)},
    ).mappings()
    return {
        (str(row["table_name"]), str(row["constraint_name"])): _normalize_sql(
            str(row["definition"])
        )
        for row in rows
    }


def _routine_grants(connection: Connection, names: Sequence[str]) -> set[tuple[str, str, str]]:
    rows = connection.execute(
        text(
            """
            SELECT grantee, routine_name, privilege_type
            FROM information_schema.role_routine_grants
            WHERE routine_schema = 'public' AND routine_name = ANY(:names)
            ORDER BY grantee, routine_name, privilege_type
            """
        ),
        {"names": list(names)},
    ).mappings()
    return {
        (str(row["grantee"]), str(row["routine_name"]), str(row["privilege_type"])) for row in rows
    }


@pytest.fixture
def isolated_capacity_migration_url(postgres_url: str) -> Iterator[str]:
    source_url = make_url(postgres_url)
    database_name = f"loom_capacity_reintegration_{uuid4().hex}"
    admin_engine = create_engine(source_url.set(database="postgres"), isolation_level="AUTOCOMMIT")
    quoted = admin_engine.dialect.identifier_preparer.quote(database_name)
    try:
        with admin_engine.connect() as connection:
            connection.exec_driver_sql(f"CREATE DATABASE {quoted} TEMPLATE template0")
        yield source_url.set(database=database_name).render_as_string(hide_password=False)
    finally:
        with admin_engine.connect() as connection:
            connection.execute(
                text(
                    "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                    "WHERE datname = :database_name AND pid <> pg_backend_pid()"
                ),
                {"database_name": database_name},
            )
            connection.exec_driver_sql(f"DROP DATABASE IF EXISTS {quoted}")
        admin_engine.dispose()


def test_reintegrated_capacity_history_has_one_exact_head() -> None:
    script = ScriptDirectory.from_config(_capacity_config_without_database())
    assert script.get_heads() == ("capacity_0010",)
    assert tuple(
        revision.revision for revision in script.walk_revisions("capacity_0010", "capacity_0003")
    ) == (
        "capacity_0010",
        "capacity_0009",
        "capacity_0008",
        "capacity_0007",
        "capacity_0006",
        "capacity_0005",
        "capacity_0004",
    )
    revision = script.get_revision("capacity_0005")
    assert revision is not None
    assert revision.path.endswith("capacity_0005_executable_allocation.py")


def test_capacity_0006_adds_bridge_completion_without_replacing_upstream_guards(
    isolated_capacity_migration_url: str,
) -> None:
    cfg = _capacity_config(isolated_capacity_migration_url)
    command.upgrade(cfg, "capacity_0005")
    engine = create_engine(isolated_capacity_migration_url)
    try:
        with engine.connect() as connection:
            base_routines = _routine_definitions(connection, UPSTREAM_ALLOCATION_ROUTINES)
            base_triggers = _trigger_definitions(
                connection,
                (*UPSTREAM_ALLOCATION_TRIGGERS, *UPSTREAM_ALLOCATION_EVENT_TRIGGERS),
            )
            base_constraints = _constraint_definitions(
                connection,
                ("capacity_allocation_epochs", "capacity_allocations"),
            )
            base_grants = _routine_grants(connection, UPSTREAM_ALLOCATION_ROUTINES)
            inspector = inspect(connection)
            assert BRIDGE_COMPLETION_TABLES.isdisjoint(set(inspector.get_table_names()))

        command.upgrade(cfg, "capacity_0006")

        with engine.connect() as connection:
            inspector = inspect(connection)
            tables = set(inspector.get_table_names())
            assert BRIDGE_COMPLETION_TABLES <= tables
            for table_name, columns in BRIDGE_COMPLETION_COLUMNS.items():
                actual_columns = {column["name"] for column in inspector.get_columns(table_name)}
                assert columns <= actual_columns
            assert _routine_definitions(connection, UPSTREAM_ALLOCATION_ROUTINES) == base_routines
            assert (
                _trigger_definitions(
                    connection,
                    (*UPSTREAM_ALLOCATION_TRIGGERS, *UPSTREAM_ALLOCATION_EVENT_TRIGGERS),
                )
                == base_triggers
            )
            assert (
                _constraint_definitions(
                    connection,
                    ("capacity_allocation_epochs", "capacity_allocations"),
                )
                == base_constraints
            )
            assert _routine_grants(connection, UPSTREAM_ALLOCATION_ROUTINES) == base_grants
    finally:
        engine.dispose()


def test_reintegrated_capacity_round_trip_restores_exact_upstream_capacity_0005_surface(
    isolated_capacity_migration_url: str,
) -> None:
    cfg = _capacity_config(isolated_capacity_migration_url)
    command.upgrade(cfg, "capacity_0005")
    engine = create_engine(isolated_capacity_migration_url)
    try:
        with engine.connect() as connection:
            inspector = inspect(connection)
            upstream_tables = set(inspector.get_table_names())
            upstream_routines = _routine_definitions(connection, UPSTREAM_ALLOCATION_ROUTINES)
            upstream_triggers = _trigger_definitions(
                connection,
                (*UPSTREAM_ALLOCATION_TRIGGERS, *UPSTREAM_ALLOCATION_EVENT_TRIGGERS),
            )
            upstream_constraints = _constraint_definitions(
                connection,
                ("capacity_allocation_epochs", "capacity_allocations"),
            )
            upstream_grants = _routine_grants(connection, UPSTREAM_ALLOCATION_ROUTINES)

        command.upgrade(cfg, "capacity_0010")
        command.downgrade(cfg, "capacity_0005")

        with engine.connect() as connection:
            inspector = inspect(connection)
            assert set(inspector.get_table_names()) == upstream_tables
            assert BRIDGE_COMPLETION_TABLES.isdisjoint(upstream_tables)
            assert (
                _routine_definitions(connection, UPSTREAM_ALLOCATION_ROUTINES) == upstream_routines
            )
            assert (
                _trigger_definitions(
                    connection,
                    (*UPSTREAM_ALLOCATION_TRIGGERS, *UPSTREAM_ALLOCATION_EVENT_TRIGGERS),
                )
                == upstream_triggers
            )
            assert (
                _constraint_definitions(
                    connection,
                    ("capacity_allocation_epochs", "capacity_allocations"),
                )
                == upstream_constraints
            )
            assert _routine_grants(connection, UPSTREAM_ALLOCATION_ROUTINES) == upstream_grants

        command.upgrade(cfg, "capacity_0010")
        with engine.connect() as connection:
            version = connection.execute(
                text("SELECT version_num FROM alembic_version")
            ).scalar_one()
            assert version == "capacity_0010"
    finally:
        engine.dispose()
