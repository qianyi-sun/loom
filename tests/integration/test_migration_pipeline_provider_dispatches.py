"""Migration coverage for durable Pipeline provider dispatch identity."""

from __future__ import annotations

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


def test_0102_round_trips_empty_schema_and_refuses_lossy_downgrade(
    isolated_migration_postgres_url: str,
) -> None:
    config = _config(isolated_migration_postgres_url)
    command.downgrade(config, "0101")
    engine = create_engine(isolated_migration_postgres_url)
    try:
        command.upgrade(config, "0102")
        columns = {
            column["name"] for column in inspect(engine).get_columns("pipeline_provider_dispatches")
        }
        assert {
            "execution_attempt_id",
            "provider_request_id",
            "reservation_id",
            "request_digest",
            "state",
            "outcome",
            "upstream_attempt_count",
            "llm_call_id",
        } <= columns
        command.downgrade(config, "0101")
        assert "pipeline_provider_dispatches" not in inspect(engine).get_table_names()
        command.upgrade(config, "0102")

        team_id, run_id, stage_id, attempt_id = (uuid4() for _ in range(4))
        connection_id, reservation_id, dispatch_id, provider_request_id = (
            uuid4() for _ in range(4)
        )
        digest = "sha256:" + "1" * 64
        with engine.begin() as connection:
            connection.execute(
                text("INSERT INTO teams (id, name) VALUES (:id, :name)"),
                {"id": team_id, "name": f"provider-dispatch-{team_id}"},
            )
            connection.execute(
                text("""
                    INSERT INTO provider_connections (
                        id, team_id, provider_type, display_name, base_url,
                        upstream_host, encrypted_api_key_ref, created_by
                    ) VALUES (
                        :id, :team, 'openai-compatible', :name,
                        'https://provider.invalid/v1', 'provider.invalid',
                        'loom://test/provider', 'migration-test'
                    )
                """),
                {"id": connection_id, "team": team_id, "name": str(connection_id)},
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
                    "key": f"provider-dispatch-{run_id}",
                },
            )
            connection.execute(
                text("""
                    INSERT INTO pipeline_stage_runs (
                        id, pipeline_run_id, node_key, shard_key, node_kind, state,
                        resource_profile_json, resource_profile_digest, failure_policy
                    ) VALUES (
                        :id, :run, 'generate_card_00', 'singleton', 'container', 'blocked',
                        '{}'::jsonb, :digest, 'fail_run'
                    )
                """),
                {"id": stage_id, "run": run_id, "digest": digest},
            )
            connection.execute(
                text("""
                    INSERT INTO execution_attempts (
                        id, stage_run_id, attempt_number, state
                    ) VALUES (:id, :stage, 1, 'fault_pending')
                """),
                {"id": attempt_id, "stage": stage_id},
            )
            connection.execute(
                text("""
                    INSERT INTO pipeline_budget_ledgers (
                        pipeline_run_id, provider_limit_microusd, gpu_limit_seconds,
                        artifact_limit_bytes, stage_run_limit, attempt_limit,
                        wall_deadline_at
                    ) VALUES (:run, 100, 0, 0, 1, 1, now() + interval '1 hour')
                """),
                {"run": run_id},
            )
            connection.execute(
                text("""
                    INSERT INTO execution_attempt_provider_budgets (
                        attempt_id, binding_snapshot_sha256, request_limit,
                        cost_limit_microusd, per_call_timeout_seconds
                    ) VALUES (:attempt, :digest, 1, 100, 30)
                """),
                {"attempt": attempt_id, "digest": digest},
            )
            connection.execute(
                text("""
                    INSERT INTO pipeline_budget_reservations (
                        id, pipeline_run_id, execution_attempt_id, kind,
                        reservation_key, request_digest, reserved_amount
                    ) VALUES (
                        :id, :run, :attempt, 'provider', :key, :digest, 100
                    )
                """),
                {
                    "id": reservation_id,
                    "run": run_id,
                    "attempt": attempt_id,
                    "key": f"provider:{attempt_id}:{provider_request_id}",
                    "digest": digest,
                },
            )
            connection.execute(
                text("""
                    INSERT INTO pipeline_provider_dispatches (
                        id, execution_attempt_id, provider_request_id, reservation_id,
                        binding_snapshot_sha256, request_digest,
                        provider_connection_id, provider, model, wire_api,
                        reserved_cost_microusd
                    ) VALUES (
                        :id, :attempt, :request_id, :reservation, :digest, :digest,
                        :connection, 'openai', 'gpt-test', 'responses', 100
                    )
                """),
                {
                    "id": dispatch_id,
                    "attempt": attempt_id,
                    "request_id": provider_request_id,
                    "reservation": reservation_id,
                    "digest": digest,
                    "connection": connection_id,
                },
            )

        with pytest.raises(DBAPIError, match="cannot downgrade 0102"):
            command.downgrade(config, "0101")
        with engine.begin() as connection:
            connection.execute(
                text("DELETE FROM pipeline_provider_dispatches WHERE id=:id"),
                {"id": dispatch_id},
            )
        command.downgrade(config, "0101")
        command.upgrade(config, "0102")
    finally:
        engine.dispose()
