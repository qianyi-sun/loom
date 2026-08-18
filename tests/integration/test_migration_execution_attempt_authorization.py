"""Migration coverage for immutable execution authorization snapshots."""

from __future__ import annotations

import json
from uuid import uuid4

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.exc import DBAPIError


def _config(postgres_url: str) -> Config:
    config = Config("migrations/alembic.ini")
    config.set_main_option("sqlalchemy.url", postgres_url)
    return config


def test_0101_round_trips_empty_schema_and_refuses_lossy_downgrade(
    isolated_migration_postgres_url: str,
) -> None:
    config = _config(isolated_migration_postgres_url)
    command.downgrade(config, "0100")
    engine = create_engine(isolated_migration_postgres_url)
    try:
        command.upgrade(config, "0101")
        columns = {column["name"] for column in inspect(engine).get_columns("execution_attempts")}
        assert {
            "execution_authorization_json",
            "execution_authorization_bytes",
            "execution_authorization_digest",
        } <= columns
        command.downgrade(config, "0100")
        assert "execution_authorization_json" not in {
            column["name"] for column in inspect(engine).get_columns("execution_attempts")
        }
        command.upgrade(config, "0101")

        team_id, run_id, stage_id, attempt_id = (uuid4() for _ in range(4))
        digest = "sha256:" + "1" * 64
        authorization = {"schema_version": "test.authorization.v1"}
        authorization_bytes = b'{"schema_version":"test.authorization.v1"}\n'
        with engine.begin() as connection:
            connection.execute(
                text("INSERT INTO teams (id, name) VALUES (:id, :name)"),
                {"id": team_id, "name": f"attempt-authorization-{team_id}"},
            )
            connection.execute(
                text("""
                    INSERT INTO pipeline_runs (
                        id, team_id, submission_policy, recipe_name, recipe_version,
                        recipe_digest, graph_spec_json, graph_spec_digest,
                        parameters_json, parameters_digest, resolved_inputs_json,
                        budget_json, request_digest, idempotency_key, state
                    ) VALUES (
                        :id, :team, 'ordinary', 'fixture', 1, :digest,
                        '{}'::jsonb, :digest, '{}'::jsonb, :digest,
                        '[]'::jsonb, '{}'::jsonb, :digest, :key, 'running'
                    )
                """),
                {
                    "id": run_id,
                    "team": team_id,
                    "digest": digest,
                    "key": f"attempt-authorization-{run_id}",
                },
            )
            connection.execute(
                text("""
                    INSERT INTO pipeline_stage_runs (
                        id, pipeline_run_id, node_key, shard_key, node_kind, state,
                        resource_profile_json, resource_profile_digest, failure_policy
                    ) VALUES (
                        :id, :run, 'plan_batch', 'singleton', 'container', 'blocked',
                        '{}'::jsonb, :digest, 'fail_run'
                    )
                """),
                {"id": stage_id, "run": run_id, "digest": digest},
            )
            connection.execute(
                text("""
                    INSERT INTO execution_attempts (
                        id, stage_run_id, attempt_number, state,
                        execution_authorization_json,
                        execution_authorization_bytes,
                        execution_authorization_digest
                    ) VALUES (
                        :id, :stage, 1, 'fault_pending', CAST(:authorization AS jsonb),
                        :authorization_bytes, :digest
                    )
                """),
                {
                    "id": attempt_id,
                    "stage": stage_id,
                    "authorization": json.dumps(authorization),
                    "authorization_bytes": authorization_bytes,
                    "digest": digest,
                },
            )

        with pytest.raises(DBAPIError, match="cannot downgrade 0101"):
            command.downgrade(config, "0100")
        with engine.begin() as connection:
            connection.execute(
                text("DELETE FROM execution_attempts WHERE id=:id"), {"id": attempt_id}
            )
        command.downgrade(config, "0100")
        command.upgrade(config, "0101")
    finally:
        engine.dispose()
