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


def test_0105_round_trips_empty_schema_and_refuses_lossy_downgrade(
    isolated_migration_postgres_url: str,
) -> None:
    config = _config(isolated_migration_postgres_url)
    command.downgrade(config, "0104")
    engine = create_engine(isolated_migration_postgres_url)
    try:
        command.upgrade(config, "0105")
        tables = set(inspect(engine).get_table_names())
        assert {
            "terminalgen_corpus_aliases",
            "terminalgen_corpus_publications",
            "terminalgen_corpus_tasks",
            "terminalgen_corpus_versions",
        } <= tables
        command.downgrade(config, "0104")
        assert "terminalgen_corpus_versions" not in set(inspect(engine).get_table_names())
        command.upgrade(config, "0105")

        team_id, run_id, audit_id, authoring_id, runtime_id, version_id = (
            uuid4() for _ in range(6)
        )
        digest = "sha256:" + "1" * 64
        with engine.begin() as connection:
            connection.execute(
                text("INSERT INTO teams (id, name) VALUES (:id, :name)"),
                {"id": team_id, "name": f"terminalgen-publication-{team_id}"},
            )
            connection.execute(
                text("""
                    INSERT INTO pipeline_runs (
                        id, team_id, submission_policy, recipe_name, recipe_version,
                        recipe_digest, graph_spec_json, graph_spec_digest,
                        parameters_json, parameters_digest, resolved_inputs_json,
                        budget_json, request_digest, idempotency_key, state
                    ) VALUES (
                        :id, :team, 'ordinary', 'terminalgen-authoring', 1, :digest,
                        '{}'::jsonb, :digest, '{}'::jsonb, :digest,
                        '[]'::jsonb, '{}'::jsonb, :digest, :key, 'running'
                    )
                """),
                {
                    "id": run_id,
                    "team": team_id,
                    "digest": digest,
                    "key": f"terminalgen-publication-{run_id}",
                },
            )
            for artifact_id, artifact_type in (
                (audit_id, "terminalgen_final_audit.v1"),
                (authoring_id, "terminalgen_corpus.v1"),
                (runtime_id, "terminalgen_corpus.v1"),
            ):
                connection.execute(
                    text("""
                        INSERT INTO artifacts (
                            id, artifact_type, name, team_id, content_hash
                        ) VALUES (:id, :artifact_type, 'fixture', :team, :digest)
                    """),
                    {
                        "id": artifact_id,
                        "artifact_type": artifact_type,
                        "team": team_id,
                        "digest": digest,
                    },
                )
            connection.execute(
                text("""
                    INSERT INTO terminalgen_corpus_versions (
                        id, team_id, pipeline_run_id, corpus_id, corpus_version,
                        version_sha256, recipe_digest, plan_identity_sha256,
                        final_audit_artifact_id, authoring_corpus_artifact_id,
                        runtime_corpus_artifact_id, authoring_tree_sha256,
                        runtime_tree_sha256, task_count, taskset_smoke_task_count,
                        taskset_smoke_object_key, taskset_smoke_sha256,
                        taskset_smoke_size_bytes, taskset_manifest_object_key,
                        taskset_manifest_json, taskset_manifest_sha256
                    ) VALUES (
                        :id, :team, :run, 'fixture', 1, :digest, :digest, :digest,
                        :audit, :authoring, :runtime, :digest, :digest, 1, 1,
                        'fixture/smoke.tar', :digest, 1, 'fixture/manifest.yaml',
                        '{}'::jsonb, :digest
                    )
                """),
                {
                    "id": version_id,
                    "team": team_id,
                    "run": run_id,
                    "digest": digest,
                    "audit": audit_id,
                    "authoring": authoring_id,
                    "runtime": runtime_id,
                },
            )

        with pytest.raises(DBAPIError, match="cannot downgrade 0105"):
            command.downgrade(config, "0104")
        with engine.begin() as connection:
            connection.execute(text("DELETE FROM terminalgen_corpus_versions"))
            connection.execute(text("DELETE FROM artifacts WHERE team_id=:team"), {"team": team_id})
            connection.execute(text("DELETE FROM pipeline_runs WHERE id=:run"), {"run": run_id})
            connection.execute(text("DELETE FROM teams WHERE id=:team"), {"team": team_id})
        command.downgrade(config, "0104")
        command.upgrade(config, "0105")
    finally:
        engine.dispose()
