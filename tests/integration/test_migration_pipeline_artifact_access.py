"""Migration coverage for immutable Pipeline Artifact access classes."""

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


def test_0099_backfills_and_refuses_lossy_restricted_artifact_downgrade(
    isolated_migration_postgres_url: str,
) -> None:
    config = _config(isolated_migration_postgres_url)
    command.downgrade(config, "0098")
    team_id = uuid4()
    artifact_id = uuid4()
    engine = create_engine(isolated_migration_postgres_url)
    try:
        with engine.begin() as connection:
            connection.execute(
                text("INSERT INTO teams (id, name) VALUES (:id, :name)"),
                {"id": team_id, "name": f"artifact-access-{team_id}"},
            )
            connection.execute(
                text(
                    "INSERT INTO artifacts "
                    "(id, artifact_type, name, team_id, content_hash) "
                    "VALUES (:id, 'migration.fixture.v1', 'fixture', :team_id, :digest)"
                ),
                {
                    "id": artifact_id,
                    "team_id": team_id,
                    "digest": f"sha256:{'1' * 64}",
                },
            )

        command.upgrade(config, "0099")
        inspector = inspect(engine)
        assert "access_class" in {
            column["name"] for column in inspector.get_columns("artifacts")
        }
        assert "artifacts_team_access_class_idx" in {
            index["name"] for index in inspector.get_indexes("artifacts")
        }
        with engine.begin() as connection:
            assert connection.execute(
                text("SELECT access_class FROM artifacts WHERE id = :id"),
                {"id": artifact_id},
            ).scalar_one() == "team_runtime"
            connection.execute(
                text(
                    "UPDATE artifacts SET access_class = 'authoring_restricted' "
                    "WHERE id = :id"
                ),
                {"id": artifact_id},
            )

        with pytest.raises(DBAPIError, match="cannot downgrade 0099"):
            command.downgrade(config, "0098")

        with engine.begin() as connection:
            connection.execute(
                text("UPDATE artifacts SET access_class = 'team_runtime' WHERE id = :id"),
                {"id": artifact_id},
            )
        command.downgrade(config, "0098")
        assert "access_class" not in {
            column["name"] for column in inspect(engine).get_columns("artifacts")
        }
        command.upgrade(config, "0099")
    finally:
        engine.dispose()
