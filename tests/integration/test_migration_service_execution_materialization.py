from __future__ import annotations

from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine, inspect, text


def _config(url: str) -> Config:
    config = Config("migrations/alembic.ini")
    config.set_main_option("sqlalchemy.url", url.replace("%", "%%"))
    return config


def test_service_execution_materialization_migration_round_trip(
    isolated_migration_postgres_url: str,
) -> None:
    config = _config(isolated_migration_postgres_url)
    command.downgrade(config, "0128")
    engine = create_engine(isolated_migration_postgres_url)
    try:
        before = {item["name"] for item in inspect(engine).get_columns("execution_leases")}
        assert "materialization_state" not in before

        command.upgrade(config, "0129")
        columns = {item["name"] for item in inspect(engine).get_columns("execution_leases")}
        assert {
            "materialization_state",
            "materialization_attempts",
            "materialization_next_attempt_at",
            "materialization_claim_id",
            "materialization_claim_expires_at",
            "materialization_started_at",
            "materialization_committed_at",
            "materialization_error_code",
            "materialization_error_message",
            "canonical_trajectory_sha256",
            "canonical_atif_sha256",
            "source_cleanup_state",
            "source_retain_until",
            "source_cleanup_attempts",
            "source_cleanup_claim_id",
            "source_cleanup_claim_expires_at",
            "source_cleanup_error_message",
        } <= columns
        indexes = {item["name"] for item in inspect(engine).get_indexes("execution_leases")}
        assert "execution_leases_materialization_queue_idx" in indexes
        assert "execution_leases_source_cleanup_queue_idx" in indexes
        with engine.connect() as connection:
            history_function = connection.scalar(
                text("SELECT pg_get_functiondef('append_execution_lease_history()'::regprocedure)")
            )
            mutation_function = connection.scalar(
                text("SELECT pg_get_functiondef('validate_execution_lease_mutation()'::regprocedure)")
            )
        assert history_function is not None
        assert "'materialization_state', new.materialization_state" in history_function.lower()
        assert mutation_function is not None
        assert "deleted execution lease is immutable outside materialization" in mutation_function
        assert "source cleanup attempts cannot decrease" in mutation_function
        assert "complete source cleanup state is immutable" in mutation_function

        command.downgrade(config, "0128")
        after = {item["name"] for item in inspect(engine).get_columns("execution_leases")}
        assert "materialization_state" not in after
        command.upgrade(config, "head")
    finally:
        engine.dispose()
