"""Migration coverage for durable fan-out item authority."""

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


def test_0100_refuses_legacy_or_lossy_fanout_item_authority_transition(
    isolated_migration_postgres_url: str,
) -> None:
    config = _config(isolated_migration_postgres_url)
    command.downgrade(config, "0099")
    engine = create_engine(isolated_migration_postgres_url)
    try:
        command.upgrade(config, "0100")
        assert "fanout_item_json" in {
            column["name"] for column in inspect(engine).get_columns("pipeline_stage_runs")
        }
        command.downgrade(config, "0099")
        assert "fanout_item_json" not in {
            column["name"] for column in inspect(engine).get_columns("pipeline_stage_runs")
        }

        team_id = uuid4()
        run_id = uuid4()
        expansion_id = uuid4()
        source_artifact_id = uuid4()
        child_id = uuid4()
        with engine.begin() as connection:
            connection.execute(
                text("INSERT INTO teams (id, name) VALUES (:id, :name)"),
                {"id": team_id, "name": f"fanout-authority-{team_id}"},
            )
            connection.execute(
                text("""
                    INSERT INTO pipeline_runs (
                        id, team_id, submission_policy,
                        recipe_name, recipe_version, recipe_digest,
                        graph_spec_json, graph_spec_digest, parameters_json,
                        parameters_digest, resolved_inputs_json, budget_json,
                        request_digest, idempotency_key, state
                    ) VALUES (
                        :id, :team, 'ordinary', 'fixture', 1, :digest,
                        '{}'::jsonb, :digest, '{}'::jsonb,
                        :digest, '[]'::jsonb, '{}'::jsonb,
                        :digest, 'migration-fixture', 'running'
                    )
                """),
                {"id": run_id, "team": team_id, "digest": "sha256:" + "1" * 64},
            )
            connection.execute(
                text("""
                    INSERT INTO artifacts (id, artifact_type, name, team_id, content_hash)
                    VALUES (:id, 'loom.fanout-manifest.v1', 'manifest', :team, :digest)
                """),
                {
                    "id": source_artifact_id,
                    "team": team_id,
                    "digest": "sha256:" + "2" * 64,
                },
            )
            connection.execute(
                text("""
                    INSERT INTO pipeline_fanout_expansions (
                        id, pipeline_run_id, node_key, source_kind, source_artifact_id,
                        source_manifest_digest, fanout_spec_digest, item_count
                    ) VALUES (
                        :id, :run, 'child', 'run_input', :artifact, :digest, :digest, 1
                    )
                """),
                {
                    "id": expansion_id,
                    "run": run_id,
                    "artifact": source_artifact_id,
                    "digest": "sha256:" + "2" * 64,
                },
            )
            connection.execute(
                text("""
                    INSERT INTO pipeline_stage_runs (
                        id, pipeline_run_id, node_key, shard_key, node_kind, state,
                        resource_profile_json, resource_profile_digest,
                        fanout_parameters_json, fanout_item_digest, fanout_expansion_id,
                        failure_policy
                    ) VALUES (
                        :id, :run, 'child', 'item-000', 'container', 'blocked',
                        '{}'::jsonb, :digest, '{}'::jsonb, :digest, :expansion, 'fail_run'
                    )
                """),
                {
                    "id": child_id,
                    "run": run_id,
                    "digest": "sha256:" + "3" * 64,
                    "expansion": expansion_id,
                },
            )

        with pytest.raises(DBAPIError, match="legacy fanout child"):
            command.upgrade(config, "0100")
        with engine.begin() as connection:
            connection.execute(text("DELETE FROM pipeline_stage_runs WHERE id=:id"), {"id": child_id})
            connection.execute(
                text("DELETE FROM pipeline_fanout_expansions WHERE id=:id"),
                {"id": expansion_id},
            )
        command.upgrade(config, "0100")
        with engine.begin() as connection:
            connection.execute(
                text("""
                    INSERT INTO pipeline_fanout_expansions (
                        id, pipeline_run_id, node_key, source_kind, source_artifact_id,
                        source_manifest_digest, fanout_spec_digest, item_count
                    ) VALUES (
                        :id, :run, 'child', 'run_input', :artifact, :digest, :digest, 1
                    )
                """),
                {
                    "id": expansion_id,
                    "run": run_id,
                    "artifact": source_artifact_id,
                    "digest": "sha256:" + "2" * 64,
                },
            )
            connection.execute(
                text("""
                    INSERT INTO pipeline_stage_runs (
                        id, pipeline_run_id, node_key, shard_key, node_kind, state,
                        resource_profile_json, resource_profile_digest,
                        fanout_parameters_json, fanout_item_json, fanout_item_digest,
                        fanout_expansion_id, failure_policy
                    ) VALUES (
                        :id, :run, 'child', 'item-000', 'container', 'blocked',
                        '{}'::jsonb, :digest, '{}'::jsonb,
                        CAST(:item AS jsonb),
                        :digest, :expansion, 'fail_run'
                    )
                """),
                {
                    "id": child_id,
                    "run": run_id,
                    "digest": "sha256:" + "3" * 64,
                    "expansion": expansion_id,
                    "item": json.dumps(
                        {
                            "shard_key": "item-000",
                            "parameters": {},
                            "artifact_bindings": [
                                {
                                    "artifact_id": str(source_artifact_id),
                                    "artifact_type": "loom.fanout-manifest.v1",
                                    "name": "manifest",
                                }
                            ],
                        }
                    ),
                },
            )
        with pytest.raises(DBAPIError, match="cannot downgrade 0100"):
            command.downgrade(config, "0099")
        with engine.begin() as connection:
            connection.execute(text("DELETE FROM pipeline_stage_runs WHERE id=:id"), {"id": child_id})
            connection.execute(
                text("DELETE FROM pipeline_fanout_expansions WHERE id=:id"),
                {"id": expansion_id},
            )
        command.downgrade(config, "0099")
        command.upgrade(config, "0100")
    finally:
        engine.dispose()
