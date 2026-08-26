from __future__ import annotations

from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine, inspect, text


def _config(url: str) -> Config:
    config = Config("migrations/alembic.ini")
    config.set_main_option("sqlalchemy.url", url.replace("%", "%%"))
    return config


def test_service_execution_output_migration_round_trip(
    isolated_migration_postgres_url: str,
) -> None:
    config = _config(isolated_migration_postgres_url)
    command.downgrade(config, "0114")
    engine = create_engine(isolated_migration_postgres_url)
    try:
        assert "pod_ip" not in {
            item["name"] for item in inspect(engine).get_columns("execution_leases")
        }
        command.upgrade(config, "0115")
        lease_columns = {
            item["name"] for item in inspect(engine).get_columns("execution_leases")
        }
        upload_columns = {
            item["name"] for item in inspect(engine).get_columns("artifact_upload_sessions")
        }
        assert {
            "pod_ip",
            "output_commit_state",
            "output_upload_session_id",
            "output_generation",
            "output_manifest_sha256",
            "output_marker_sha256",
            "output_committed_at",
            "output_unavailable_reason",
        } <= lease_columns
        assert {
            "service_execution_lease_id",
            "service_execution_generation",
            "service_execution_role",
            "service_execution_runtime_contract_sha256",
            "service_execution_candidate_sha",
            "service_execution_task_revision_sha256",
            "service_execution_command_identity_sha256",
        } <= upload_columns
        with engine.connect() as connection:
            function = connection.scalar(
                text("SELECT pg_get_functiondef('append_execution_lease_history()'::regprocedure)")
            )
        assert function is not None
        assert "'pod_ip', NEW.pod_ip" in function
        assert "'output_commit_state', NEW.output_commit_state" in function

        command.downgrade(config, "0114")
        assert "pod_ip" not in {
            item["name"] for item in inspect(engine).get_columns("execution_leases")
        }
        command.upgrade(config, "head")
    finally:
        engine.dispose()
