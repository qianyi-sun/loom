"""Task 1 RED/GREEN contract for reintegrated executable-capacity migrations."""

from __future__ import annotations

import os
from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from pathlib import Path
from uuid import uuid4

import pytest
from alembic import command
from alembic.config import Config as AlembicConfig
from alembic.script import ScriptDirectory
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import Connection, make_url

from loom_capacity_manager.models import Base

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

UPSTREAM_QUEUE_GUARD_ROUTINES = (
    "capacity_executable_executor_state_guard",
    "capacity_executable_intent_guard",
    "capacity_executable_launch_rate_bucket_guard",
    "capacity_executable_protected_release_insert_guard",
    "capacity_executable_receipt_append_only_guard",
)

UPSTREAM_QUEUE_GUARD_TRIGGERS = (
    "capacity_executable_command_receipts_append_only_guard",
    "capacity_executable_command_receipts_truncate_guard",
    "capacity_executable_executor_state_mutation_guard",
    "capacity_executable_executor_state_truncate_guard",
    "capacity_executable_intent_mutation_guard",
    "capacity_executable_intent_truncate_guard",
    "capacity_executable_launch_rate_bucket_mutation_guard",
    "capacity_executable_launch_rate_bucket_truncate_guard",
    "capacity_executable_protected_release_insert_guard",
    "capacity_executable_protected_release_receipts_append_only_guar",
    "capacity_executable_protected_release_receipts_truncate_guard",
)

UPSTREAM_QUEUE_GUARD_TABLES = (
    "capacity_allocation_epochs",
    "capacity_executable_command_receipts",
    "capacity_executable_executor_states",
    "capacity_executable_intents",
    "capacity_executable_launch_rate_buckets",
    "capacity_executable_protected_release_receipts",
)

UPSTREAM_QUEUE_GUARD_SENTINEL_CONSTRAINTS = {
    ("capacity_allocation_epochs", "capacity_allocation_epoch_mode_check"): (
        "input_valid_until IS NOT NULL"
    ),
    (
        "capacity_executable_command_receipts",
        "capacity_executable_command_receipt_quantity_check",
    ): ("command_sequence > 0"),
    (
        "capacity_executable_launch_rate_buckets",
        "capacity_executable_launch_rate_bucket_quantity_check",
    ): "refill_remainder < 60",
    (
        "capacity_executable_protected_release_receipts",
        "capacity_executable_protected_release_receipt_epoch_check",
    ): "protected_registration_epoch > bootstrap_registration_epoch",
}

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

BRIDGE_COMPLETION_0007_COLUMNS = {
    "capacity_execution_epochs": {
        "drain_actor",
        "drain_idempotency_key",
        "drain_request_digest",
        "drain_request_payload",
        "retirement_actor",
        "retirement_idempotency_key",
        "retirement_request_digest",
        "retirement_request_payload",
    },
    "capacity_executable_executor_states": {
        "retirement_safe",
        "retirement_inventory_digest",
    },
}


@dataclass(frozen=True)
class CapacitySchemaSurface:
    routines: dict[str, str]
    triggers: dict[tuple[str, str], str]
    constraints: dict[tuple[str, str], str]
    grants: set[tuple[str, str, str]]
    database_columns: dict[str, dict[str, tuple[str, bool]]]
    orm_columns: dict[str, dict[str, tuple[str, bool]]]


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


def _routine_search_paths(
    connection: Connection,
    names: Sequence[str],
) -> dict[str, str | None]:
    rows = connection.execute(
        text(
            """
            SELECT p.proname, array_to_string(p.proconfig, ',') AS config
            FROM pg_proc AS p
            JOIN pg_namespace AS n ON n.oid = p.pronamespace
            WHERE n.nspname = 'public' AND p.proname = ANY(:names)
            ORDER BY p.proname
            """
        ),
        {"names": list(names)},
    ).mappings()
    return {
        str(row["proname"]): (None if row["config"] is None else str(row["config"])) for row in rows
    }


def _all_capacity_routine_definitions(connection: Connection) -> dict[str, str]:
    rows = connection.execute(
        text(
            """
            SELECT p.oid::regprocedure::text AS routine_identity,
                   pg_get_functiondef(p.oid) AS definition
            FROM pg_proc AS p
            JOIN pg_namespace AS n ON n.oid = p.pronamespace
            WHERE n.nspname = 'public'
              AND p.proname LIKE 'capacity_%'
            ORDER BY routine_identity
            """
        )
    ).mappings()
    return {str(row["routine_identity"]): _normalize_sql(str(row["definition"])) for row in rows}


def _all_capacity_trigger_definitions(connection: Connection) -> dict[tuple[str, str], str]:
    rows = connection.execute(
        text(
            """
            SELECT rel.relname AS table_name,
                   trg.tgname AS trigger_name,
                   pg_get_triggerdef(trg.oid, true) AS definition
            FROM pg_trigger AS trg
            JOIN pg_class AS rel ON rel.oid = trg.tgrelid
            JOIN pg_namespace AS nsp ON nsp.oid = rel.relnamespace
            WHERE NOT trg.tgisinternal
              AND nsp.nspname = 'public'
              AND rel.relname LIKE 'capacity_%'
            ORDER BY rel.relname, trg.tgname
            """
        )
    ).mappings()
    return {
        (str(row["table_name"]), str(row["trigger_name"])): _normalize_sql(str(row["definition"]))
        for row in rows
    }


def _all_capacity_constraint_definitions(connection: Connection) -> dict[tuple[str, str], str]:
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
              AND rel.relname LIKE 'capacity_%'
            ORDER BY rel.relname, con.conname
            """
        )
    ).mappings()
    return {
        (str(row["table_name"]), str(row["constraint_name"])): _normalize_sql(
            str(row["definition"])
        )
        for row in rows
    }


def _all_capacity_routine_grants(connection: Connection) -> set[tuple[str, str, str]]:
    rows = connection.execute(
        text(
            """
            SELECT grantee, routine_name, privilege_type
            FROM information_schema.role_routine_grants
            WHERE routine_schema = 'public'
              AND routine_name LIKE 'capacity_%'
            ORDER BY grantee, routine_name, privilege_type
            """
        )
    ).mappings()
    return {
        (str(row["grantee"]), str(row["routine_name"]), str(row["privilege_type"])) for row in rows
    }


def _column_signature(type_name: object, nullable: bool) -> tuple[str, bool]:
    return (str(type_name).upper(), bool(nullable))


def _database_columns(connection: Connection) -> dict[str, dict[str, tuple[str, bool]]]:
    inspector = inspect(connection)
    table_names = sorted(
        table_name
        for table_name in inspector.get_table_names()
        if table_name.startswith("capacity_")
    )
    return {
        table_name: {
            str(column["name"]): _column_signature(column["type"], bool(column["nullable"]))
            for column in inspector.get_columns(table_name)
        }
        for table_name in table_names
    }


def _project_orm_columns(
    database_columns: dict[str, dict[str, tuple[str, bool]]],
) -> dict[str, dict[str, tuple[str, bool]]]:
    projected: dict[str, dict[str, tuple[str, bool]]] = {}
    for table_name, columns in database_columns.items():
        table = Base.metadata.tables.get(table_name)
        assert table is not None, f"missing ORM table metadata for {table_name}"
        projected[table_name] = {
            column_name: _column_signature(
                table.columns[column_name].type, table.columns[column_name].nullable
            )
            for column_name in columns
        }
    return projected


def _schema_surface(connection: Connection) -> CapacitySchemaSurface:
    database_columns = _database_columns(connection)
    return CapacitySchemaSurface(
        routines=_all_capacity_routine_definitions(connection),
        triggers=_all_capacity_trigger_definitions(connection),
        constraints=_all_capacity_constraint_definitions(connection),
        grants=_all_capacity_routine_grants(connection),
        database_columns=database_columns,
        orm_columns=_project_orm_columns(database_columns),
    )


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
    assert tuple(script.get_heads()) == ("capacity_0013",)
    assert tuple(
        revision.revision for revision in script.walk_revisions("capacity_0004", "capacity_0013")
    ) == (
        "capacity_0013",
        "capacity_0012",
        "capacity_0011",
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
    official_queue = script.get_revision("capacity_0006")
    assert official_queue is not None
    assert official_queue.path.endswith("capacity_0006_executable_work_queue.py")
    official_bootstrap = script.get_revision("capacity_0007")
    assert official_bootstrap is not None
    assert official_bootstrap.path.endswith("capacity_0007_protected_bootstrap_handshake.py")
    bridge_completion = script.get_revision("capacity_0008")
    assert bridge_completion is not None
    assert bridge_completion.path.endswith("capacity_0008_executable_bridge_completion.py")


def test_capacity_0008_adds_bridge_completion_and_only_patches_accepted_release_guard(
    isolated_capacity_migration_url: str,
) -> None:
    cfg = _capacity_config(isolated_capacity_migration_url)
    command.upgrade(cfg, "capacity_0007")
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
            base_queue_routines = _routine_definitions(connection, UPSTREAM_QUEUE_GUARD_ROUTINES)
            assert set(base_queue_routines) == set(UPSTREAM_QUEUE_GUARD_ROUTINES)
            base_queue_triggers = _trigger_definitions(connection, UPSTREAM_QUEUE_GUARD_TRIGGERS)
            assert set(base_queue_triggers) == set(UPSTREAM_QUEUE_GUARD_TRIGGERS)
            base_queue_constraints = _constraint_definitions(
                connection, UPSTREAM_QUEUE_GUARD_TABLES
            )
            assert set(UPSTREAM_QUEUE_GUARD_SENTINEL_CONSTRAINTS) <= set(base_queue_constraints)
            for (
                constraint_name,
                expected_fragment,
            ) in UPSTREAM_QUEUE_GUARD_SENTINEL_CONSTRAINTS.items():
                assert expected_fragment in base_queue_constraints[constraint_name]
            base_queue_grants = _routine_grants(connection, UPSTREAM_QUEUE_GUARD_ROUTINES)
            base_queue_search_paths = _routine_search_paths(
                connection, UPSTREAM_QUEUE_GUARD_ROUTINES
            )
            assert base_queue_search_paths == {
                routine_name: "search_path=pg_catalog"
                for routine_name in UPSTREAM_QUEUE_GUARD_ROUTINES
            }
            inspector = inspect(connection)
            assert BRIDGE_COMPLETION_TABLES <= set(inspector.get_table_names())
            for table_name, columns in BRIDGE_COMPLETION_0007_COLUMNS.items():
                actual_columns = {column["name"] for column in inspector.get_columns(table_name)}
                assert columns.isdisjoint(actual_columns)

        command.upgrade(cfg, "capacity_0008")

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
            current_queue_routines = _routine_definitions(connection, UPSTREAM_QUEUE_GUARD_ROUTINES)
            assert set(current_queue_routines) == set(base_queue_routines)
            assert (
                current_queue_routines["capacity_executable_intent_guard"]
                != base_queue_routines["capacity_executable_intent_guard"]
            )
            assert (
                "(OLD.state = 'accepted' AND NEW.state IN "
                "('launch-ready','closing','released','quarantined')) OR"
                in current_queue_routines["capacity_executable_intent_guard"]
            )
            assert (
                "IF OLD.state = 'accepted' AND NEW.state = 'released' "
                "AND release_changed AND NEW.released_at IS NOT NULL "
                "AND NOT accepted_changed AND NOT bootstrap_changed "
                "AND NOT permit_changed AND NOT consumption_changed "
                "AND NOT inventory_changed THEN RETURN NEW; END IF;"
                in current_queue_routines["capacity_executable_intent_guard"]
            )
            for routine_name, definition in base_queue_routines.items():
                if routine_name == "capacity_executable_intent_guard":
                    continue
                assert current_queue_routines[routine_name] == definition
            assert (
                _trigger_definitions(connection, UPSTREAM_QUEUE_GUARD_TRIGGERS)
                == base_queue_triggers
            )
            current_queue_constraints = _constraint_definitions(
                connection, UPSTREAM_QUEUE_GUARD_TABLES
            )
            assert set(base_queue_constraints) <= set(current_queue_constraints)
            for constraint_name, definition in base_queue_constraints.items():
                assert current_queue_constraints[constraint_name] == definition
            assert _routine_grants(connection, UPSTREAM_QUEUE_GUARD_ROUTINES) == base_queue_grants
            assert (
                _routine_search_paths(connection, UPSTREAM_QUEUE_GUARD_ROUTINES)
                == base_queue_search_paths
            )
    finally:
        engine.dispose()


def test_reintegrated_capacity_round_trip_restores_exact_upstream_capacity_0005_surface(
    isolated_capacity_migration_url: str,
) -> None:
    cfg = _capacity_config(isolated_capacity_migration_url)
    engine = create_engine(isolated_capacity_migration_url)
    try:
        command.upgrade(cfg, "capacity_0005")
        with engine.connect() as connection:
            surface_0005 = _schema_surface(connection)
            assert surface_0005.database_columns == surface_0005.orm_columns

        command.upgrade(cfg, "capacity_0006")
        with engine.connect() as connection:
            surface_0006 = _schema_surface(connection)
            assert surface_0006 != surface_0005
            assert surface_0006.database_columns == surface_0006.orm_columns
            assert (
                "input_valid_until" in surface_0006.database_columns["capacity_allocation_epochs"]
            )
            assert (
                surface_0006.database_columns["capacity_allocation_epochs"]["input_valid_until"]
                == surface_0006.orm_columns["capacity_allocation_epochs"]["input_valid_until"]
            )
            assert "capacity_execution_epoch_transition_guard()" in surface_0006.routines
            assert "capacity_prepared_retirement_evidence_guard()" not in surface_0006.routines
            assert "capacity_executable_executor_states" in surface_0006.database_columns
            assert (
                "retirement_safe"
                not in surface_0006.database_columns["capacity_executable_executor_states"]
            )
            assert "capacity_executable_intent_observed_state_check" not in {
                constraint_name for _, constraint_name in surface_0006.constraints
            }

        command.upgrade(cfg, "capacity_0007")
        with engine.connect() as connection:
            surface_0007 = _schema_surface(connection)
            assert surface_0007 != surface_0006
            assert surface_0007.database_columns == surface_0007.orm_columns
            assert "capacity_executable_bootstrap_proposals" in surface_0007.database_columns
            assert "capacity_executable_bootstrap_acknowledgements" in surface_0007.database_columns
            assert (
                "inventory_confirmation_journal_digest"
                not in surface_0007.database_columns["capacity_executable_executor_states"]
            )
            assert (
                "retirement_safe"
                not in surface_0007.database_columns["capacity_executable_executor_states"]
            )
            assert (
                "capacity_executable_intents",
                "capacity_executable_intent_observed_state_check",
            ) not in (surface_0007.constraints)

        command.upgrade(cfg, "capacity_0008")
        with engine.connect() as connection:
            surface_0008 = _schema_surface(connection)
            assert surface_0008 != surface_0007
            assert surface_0008.database_columns == surface_0008.orm_columns
            assert (
                "retirement_safe"
                in surface_0008.database_columns["capacity_executable_executor_states"]
            )
            assert "capacity_executable_bootstrap_proposals" in surface_0008.database_columns
            assert "capacity_executable_bootstrap_acknowledgements" in surface_0008.database_columns

        command.downgrade(cfg, "capacity_0007")
        with engine.connect() as connection:
            roundtrip_0007_from_0008 = _schema_surface(connection)
            assert roundtrip_0007_from_0008 == surface_0007

        command.upgrade(cfg, "capacity_0008")
        with engine.connect() as connection:
            roundtrip_0008_from_0007 = _schema_surface(connection)
            assert roundtrip_0008_from_0007 == surface_0008

        command.upgrade(cfg, "capacity_0012")
        with engine.connect() as connection:
            surface_0012 = _schema_surface(connection)
            assert surface_0012 != surface_0008
            assert surface_0012.database_columns == surface_0012.orm_columns
            assert "capacity_prepared_retirement_evidence_guard()" in surface_0012.routines
            assert (
                "inventory_confirmation_journal_digest"
                in surface_0012.database_columns["capacity_executable_executor_states"]
            )
            assert (
                "capacity_executable_intents",
                "capacity_executable_intent_observed_state_check",
            ) in surface_0012.constraints

        command.downgrade(cfg, "capacity_0005")

        with engine.connect() as connection:
            roundtrip_0005 = _schema_surface(connection)
            assert roundtrip_0005 == surface_0005

        command.upgrade(cfg, "capacity_0006")
        with engine.connect() as connection:
            roundtrip_0006 = _schema_surface(connection)
            assert roundtrip_0006 == surface_0006

        command.upgrade(cfg, "capacity_0007")
        with engine.connect() as connection:
            roundtrip_0007 = _schema_surface(connection)
            assert roundtrip_0007 == surface_0007

        command.upgrade(cfg, "capacity_0012")
        with engine.connect() as connection:
            roundtrip_0012 = _schema_surface(connection)
            assert roundtrip_0012 == surface_0012
            assert roundtrip_0012.database_columns == roundtrip_0012.orm_columns
            version = connection.execute(
                text("SELECT version_num FROM alembic_version")
            ).scalar_one()
            assert version == "capacity_0012"
    finally:
        engine.dispose()
