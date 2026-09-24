"""The archival retry migration preserves the original mutation guards on downgrade."""
from alembic import command
from sqlalchemy import create_engine, inspect, text

from tests.integration.test_migration_service_execution_materialization import _config


def test_archival_retry_migration_round_trip(isolated_migration_postgres_url: str) -> None:
    config = _config(isolated_migration_postgres_url)
    engine = create_engine(isolated_migration_postgres_url)
    try:
        command.downgrade(config, "0156")
        with engine.connect() as connection:
            before = tuple(connection.scalar(text("SELECT pg_get_functiondef(to_regprocedure(:name))"),
                {"name": name + "()"}) for name in
                ("validate_execution_lease_mutation", "append_execution_lease_history"))
        command.upgrade(config, "0157")
        assert "materialization_recovery_requested_at" in {
            item["name"] for item in inspect(engine).get_columns("execution_leases")
        }
        command.downgrade(config, "0156")
        with engine.connect() as connection:
            after = tuple(connection.scalar(text("SELECT pg_get_functiondef(to_regprocedure(:name))"),
                {"name": name + "()"}) for name in
                ("validate_execution_lease_mutation", "append_execution_lease_history"))
        assert after == before
        command.upgrade(config, "head")
    finally:
        engine.dispose()
