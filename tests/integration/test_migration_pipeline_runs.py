"""PostgreSQL migration coverage for the immutable Pipeline run schema."""

from __future__ import annotations

import json
from uuid import uuid4

from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine, inspect, text

PIPELINE_TABLES = {
    "execution_attempts",
    "pipeline_acceptance_preflight_prerequisites",
    "pipeline_events",
    "pipeline_fanout_expansions",
    "pipeline_runs",
    "pipeline_stage_dependencies",
    "pipeline_stage_runs",
    "pipeline_terminal_snapshots",
}
PIPELINE_ARTIFACT_COLUMNS = {
    "pipeline_run_id",
    "pipeline_stage_run_id",
    "execution_attempt_id",
    "producer_kind",
    "control_producer_kind",
    "control_producer_id",
}


def _alembic_cfg(postgres_url: str) -> Config:
    cfg = Config("migrations/alembic.ini")
    cfg.set_main_option("sqlalchemy.url", postgres_url)
    return cfg


def _seed_legacy_rows(postgres_url: str) -> dict[str, str]:
    ids = {
        "team": str(uuid4()),
        "batch": str(uuid4()),
        "trial": str(uuid4()),
        "parent_artifact": str(uuid4()),
        "child_artifact": str(uuid4()),
        "lineage": str(uuid4()),
    }
    engine = create_engine(postgres_url)
    with engine.begin() as conn:
        conn.execute(
            text("INSERT INTO teams (id, name) VALUES (:id, 'pipeline-migration-legacy')"),
            {"id": ids["team"]},
        )
        conn.execute(
            text(
                "INSERT INTO tasks (id, checksum, config) "
                "VALUES ('pipeline-migration-task', :checksum, '{}'::jsonb)"
            ),
            {"checksum": "1" * 64},
        )
        conn.execute(
            text(
                "INSERT INTO batches "
                "(id, team_id, name, task_filter, trial_config, created_by_token_prefix, "
                " expected_trial_count) "
                "VALUES (:id, :team_id, 'legacy-batch', '{}'::jsonb, '{}'::jsonb, "
                "        'legacy-token', 1)"
            ),
            {"id": ids["batch"], "team_id": ids["team"]},
        )
        conn.execute(
            text(
                "INSERT INTO trials "
                "(id, team_id, task_id, config, requires_caps, state, batch_id) "
                "VALUES (:id, :team_id, 'pipeline-migration-task', "
                "        CAST(:config AS jsonb), '{}'::jsonb, 'queued', :batch_id)"
            ),
            {
                "id": ids["trial"],
                "team_id": ids["team"],
                "batch_id": ids["batch"],
                "config": json.dumps({"legacy": True}),
            },
        )
        for artifact_id, name in (
            (ids["parent_artifact"], "legacy-input"),
            (ids["child_artifact"], "legacy-output"),
        ):
            conn.execute(
                text(
                    "INSERT INTO artifacts "
                    "(id, artifact_type, name, team_id, batch_id, trial_id, content_hash, metadata) "
                    "VALUES (:id, 'legacy.fixture.v1', :name, :team_id, :batch_id, :trial_id, "
                    "        :content_hash, CAST(:metadata AS jsonb))"
                ),
                {
                    "id": artifact_id,
                    "name": name,
                    "team_id": ids["team"],
                    "batch_id": ids["batch"],
                    "trial_id": ids["trial"],
                    "content_hash": f"sha256:{artifact_id.replace('-', '')}",
                    "metadata": json.dumps({"legacy": True}),
                },
            )
        conn.execute(
            text(
                "INSERT INTO artifact_lineage_edges "
                "(id, child_artifact_id, parent_artifact_id, relation, metadata) "
                "VALUES (:id, :child, :parent, 'derived_from', CAST(:metadata AS jsonb))"
            ),
            {
                "id": ids["lineage"],
                "child": ids["child_artifact"],
                "parent": ids["parent_artifact"],
                "metadata": json.dumps({"legacy": True}),
            },
        )
    engine.dispose()
    return ids


def _legacy_snapshot(postgres_url: str, ids: dict[str, str]) -> dict[str, tuple[object, ...]]:
    engine = create_engine(postgres_url)
    with engine.connect() as conn:
        snapshot = {
            "batch": tuple(
                conn.execute(
                    text(
                        "SELECT id::text, team_id::text, name, task_filter, trial_config, "
                        "       created_by_token_prefix, expected_trial_count "
                        "FROM batches WHERE id = :id"
                    ),
                    {"id": ids["batch"]},
                ).one()
            ),
            "trial": tuple(
                conn.execute(
                    text(
                        "SELECT id::text, team_id::text, task_id, config, requires_caps, state, "
                        "       batch_id::text "
                        "FROM trials WHERE id = :id"
                    ),
                    {"id": ids["trial"]},
                ).one()
            ),
            "parent_artifact": tuple(
                conn.execute(
                    text(
                        "SELECT id::text, artifact_type, name, team_id::text, batch_id::text, "
                        "       trial_id::text, content_hash, metadata "
                        "FROM artifacts WHERE id = :id"
                    ),
                    {"id": ids["parent_artifact"]},
                ).one()
            ),
            "child_artifact": tuple(
                conn.execute(
                    text(
                        "SELECT id::text, artifact_type, name, team_id::text, batch_id::text, "
                        "       trial_id::text, content_hash, metadata "
                        "FROM artifacts WHERE id = :id"
                    ),
                    {"id": ids["child_artifact"]},
                ).one()
            ),
            "lineage": tuple(
                conn.execute(
                    text(
                        "SELECT id::text, child_artifact_id::text, parent_artifact_id::text, "
                        "       relation, metadata "
                        "FROM artifact_lineage_edges WHERE id = :id"
                    ),
                    {"id": ids["lineage"]},
                ).one()
            ),
        }
    engine.dispose()
    return snapshot


def test_pipeline_migration_round_trip_preserves_legacy_rows(
    isolated_migration_postgres_url: str,
) -> None:
    cfg = _alembic_cfg(isolated_migration_postgres_url)
    command.downgrade(cfg, "0077")
    ids = _seed_legacy_rows(isolated_migration_postgres_url)
    before = _legacy_snapshot(isolated_migration_postgres_url, ids)

    command.upgrade(cfg, "0078")
    engine = create_engine(isolated_migration_postgres_url)
    inspector = inspect(engine)
    assert PIPELINE_TABLES.issubset(set(inspector.get_table_names()))
    assert PIPELINE_ARTIFACT_COLUMNS.issubset(
        {column["name"] for column in inspector.get_columns("artifacts")}
    )
    with engine.connect() as conn:
        new_values = conn.execute(
            text(
                "SELECT pipeline_run_id, pipeline_stage_run_id, execution_attempt_id, "
                "       producer_kind, control_producer_kind, control_producer_id "
                "FROM artifacts WHERE id IN (:parent_id, :child_id) ORDER BY id"
            ),
            {
                "parent_id": ids["parent_artifact"],
                "child_id": ids["child_artifact"],
            },
        ).all()
    assert new_values == [(None, None, None, None, None, None)] * 2
    assert _legacy_snapshot(isolated_migration_postgres_url, ids) == before
    engine.dispose()

    command.downgrade(cfg, "0077")
    engine = create_engine(isolated_migration_postgres_url)
    inspector = inspect(engine)
    assert PIPELINE_TABLES.isdisjoint(set(inspector.get_table_names()))
    assert PIPELINE_ARTIFACT_COLUMNS.isdisjoint(
        {column["name"] for column in inspector.get_columns("artifacts")}
    )
    engine.dispose()
    assert _legacy_snapshot(isolated_migration_postgres_url, ids) == before
