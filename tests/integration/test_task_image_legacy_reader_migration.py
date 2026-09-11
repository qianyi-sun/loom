"""The installed protected claim function must keep both V1 readers closed."""

from pathlib import Path

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine, text

FUNCTION = "loom_capacity_guard.claim_staging_assigned_trial(uuid,text,jsonb)"


def _configuration(database, monkeypatch):
    root = Path(__file__).resolve().parents[2] / "capacity_guard_migrations"
    config = Config(str(root / "alembic.ini"))
    config.set_main_option("script_location", str(root))
    for option, key in (
        ("DB_URL", "migrator_url"),
        ("OWNER_ROLE", "owner_role"),
        ("AGENT_ROLE", "agent_role"),
        ("EXECUTOR_ROLE", "executor_role"),
        ("OBSERVER_ROLE", "observer_role"),
        ("RUNTIME_ROLE", "runtime_role"),
    ):
        monkeypatch.setenv("LOOM_CAPACITY_GUARD_" + option, database[key])
    return config


def _installed(connection):
    return dict(
        connection.execute(
            text(
                "SELECT pg_get_functiondef(oid) AS definition, proowner, proacl, prosecdef, proconfig "
                "FROM pg_proc WHERE oid = CAST(:function AS regprocedure)"
            ),
            {"function": FUNCTION},
        )
        .mappings()
        .one()
    )


def test_installed_protected_reader_fences_both_native_selection_boundaries(
    capacity_guard_database,
):
    engine = create_engine(capacity_guard_database["admin_url"])
    try:
        with engine.connect() as connection:
            function = (
                connection.execute(
                    text(
                        "SELECT pg_get_functiondef(oid) AS definition, "
                        "pg_get_userbyid(proowner) AS owner, prosecdef, proconfig, "
                        "has_column_privilege(proowner, 'public.task_image_materializations', "
                        "'ready_publication_operation_id', 'SELECT') AS can_read_native_identity "
                        "FROM pg_proc WHERE oid = CAST(:function AS regprocedure)"
                    ),
                    {"function": FUNCTION},
                )
                .mappings()
                .one()
            )
            assert (
                function["definition"].count(
                    "materialization.ready_publication_operation_id IS NULL"
                )
                == 2
            ), "candidate and locked V1 snapshot must each exclude native publication"
            assert function["can_read_native_identity"] is True
            assert function["owner"] == capacity_guard_database["owner_role"]
            assert function["prosecdef"] is True
            assert function["proconfig"] == ["search_path=pg_catalog"]
    finally:
        engine.dispose()


def test_schema_label_rollback_retains_reader_fence_and_reupgrade_is_exact(
    capacity_guard_database,
    monkeypatch,
):
    config = _configuration(capacity_guard_database, monkeypatch)
    engine = create_engine(capacity_guard_database["admin_url"])
    try:
        with engine.connect() as connection:
            before = _installed(connection)
        command.downgrade(config, "guard_0030")
        with engine.connect() as connection:
            assert _installed(connection) == before
            assert (
                connection.execute(
                    text(
                        "SELECT version_num FROM loom_capacity_guard.capacity_guard_alembic_version"
                    )
                ).scalar_one()
                == "guard_0030"
            )
        command.upgrade(config, "guard_0031")
        with engine.connect() as connection:
            assert _installed(connection) == before
    finally:
        engine.dispose()


@pytest.mark.parametrize("drift", ["permission", "partial", "security"])
def test_reader_migration_refuses_unadmitted_authority_without_partial_upgrade(
    capacity_guard_database,
    monkeypatch,
    drift,
):
    config = _configuration(capacity_guard_database, monkeypatch)
    command.downgrade(config, "guard_0030")
    engine = create_engine(capacity_guard_database["admin_url"])
    try:
        with engine.begin() as connection:
            if drift == "permission":
                owner = engine.dialect.identifier_preparer.quote(
                    capacity_guard_database["owner_role"]
                )
                connection.exec_driver_sql(
                    "REVOKE SELECT (ready_publication_operation_id) "
                    f"ON public.task_image_materializations FROM {owner}"
                )
            elif drift == "partial":
                definition = _installed(connection)["definition"]
                connection.execute(
                    text(
                        definition.replace(
                            "materialization.ready_publication_operation_id IS NULL",
                            "materialization.ready_publication_operation_id IS NOT NULL",
                            1,
                        )
                    )
                )
            else:
                connection.exec_driver_sql(f"ALTER FUNCTION {FUNCTION} SECURITY INVOKER")
            before = _installed(connection)
        with pytest.raises(RuntimeError, match="native reader fence"):
            command.upgrade(config, "guard_0031")
        with engine.connect() as connection:
            assert _installed(connection) == before
            assert (
                connection.execute(
                    text(
                        "SELECT version_num FROM loom_capacity_guard.capacity_guard_alembic_version"
                    )
                ).scalar_one()
                == "guard_0030"
            )
    finally:
        engine.dispose()
