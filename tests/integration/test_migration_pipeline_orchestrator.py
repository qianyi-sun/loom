from __future__ import annotations

from uuid import uuid4

from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine, inspect, text

ORCHESTRATOR_TABLES = {
    "pipeline_budget_ledgers",
    "pipeline_budget_reservations",
    "execution_attempt_provider_budgets",
    "pipeline_cancellation_outbox",
}


def _config(url: str) -> Config:
    config = Config("migrations/alembic.ini")
    config.set_main_option("sqlalchemy.url", url)
    return config


def test_0078_to_0079_round_trip_preserves_pipeline_run(
    isolated_migration_postgres_url: str,
) -> None:
    url = isolated_migration_postgres_url
    config = _config(url)
    command.downgrade(config, "0078")
    engine = create_engine(url)
    team_id = uuid4()
    run_id = uuid4()
    digest = "sha256:" + "a" * 64
    with engine.begin() as connection:
        connection.execute(
            text("INSERT INTO teams (id,name) VALUES (:id,:name)"),
            {"id": team_id, "name": f"pipeline-orchestrator-migration-{team_id}"},
        )
        connection.execute(
            text("""
                INSERT INTO pipeline_runs (
                    id,team_id,submission_policy,recipe_name,recipe_version,recipe_digest,
                    graph_spec_json,graph_spec_digest,parameters_json,parameters_digest,
                    resolved_inputs_json,budget_json,request_digest,idempotency_key
                ) VALUES (
                    :id,:team,'ordinary','migration-fixture',1,:digest,
                    '{}'::jsonb,:digest,'{}'::jsonb,:digest,'[]'::jsonb,'{}'::jsonb,
                    :digest,:key
                )
            """),
            {"id": run_id, "team": team_id, "digest": digest, "key": f"run-{run_id}"},
        )
    engine.dispose()

    command.upgrade(config, "0079")
    engine = create_engine(url)
    inspector = inspect(engine)
    assert ORCHESTRATOR_TABLES.issubset(set(inspector.get_table_names()))
    assert {"claimed_by", "lease_epoch", "lease_expires_at"}.issubset(
        {column["name"] for column in inspector.get_columns("pipeline_runs")}
    )
    assert {
        "worker_lease_epoch",
        "slurm_cluster_id",
        "slurm_cluster_config_sha256",
        "slurm_allocation_id",
    }.issubset(
        {
            column["name"]
            for column in inspector.get_columns("pipeline_acceptance_preflight_prerequisites")
        }
    )
    with engine.connect() as connection:
        assert (
            connection.execute(
                text("SELECT id FROM pipeline_runs WHERE id=:id"), {"id": run_id}
            ).scalar_one()
            == run_id
        )
    engine.dispose()

    command.downgrade(config, "0078")
    engine = create_engine(url)
    inspector = inspect(engine)
    assert ORCHESTRATOR_TABLES.isdisjoint(set(inspector.get_table_names()))
    assert {"claimed_by", "lease_epoch", "lease_expires_at"}.isdisjoint(
        {column["name"] for column in inspector.get_columns("pipeline_runs")}
    )
    with engine.connect() as connection:
        assert (
            connection.execute(
                text("SELECT id FROM pipeline_runs WHERE id=:id"), {"id": run_id}
            ).scalar_one()
            == run_id
        )
    engine.dispose()
