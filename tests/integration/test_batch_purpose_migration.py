"""Upgrade the deployed dev schema without changing existing batch identities."""

from uuid import uuid4

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.exc import IntegrityError


def test_0151_preserves_batches_and_rolling_upgrade_compatibility(
    isolated_migration_postgres_url: str,
) -> None:
    cfg = Config("migrations/alembic.ini")
    cfg.set_main_option("sqlalchemy.url", isolated_migration_postgres_url)
    command.downgrade(cfg, "0150")
    engine = create_engine(isolated_migration_postgres_url)
    team_id, batch_id = uuid4(), uuid4()
    try:
        with engine.begin() as connection:
            connection.execute(text("INSERT INTO teams (id,name) VALUES (:id,:name)"),
                               {"id": team_id, "name": f"migration-{team_id}"})
            connection.execute(text(
                "INSERT INTO batches (id,team_id,name,task_filter,trial_config,"
                "created_by_token_prefix,expected_trial_count) "
                "VALUES (:id,:team,'preserved','{}','{}','migration',0)"
            ), {"id": batch_id, "team": team_id})
        command.upgrade(cfg, "head")
        with engine.connect() as connection:
            row = connection.execute(text(
                "SELECT name,purpose FROM batches WHERE id=:id"
            ), {"id": batch_id}).one()
            assert tuple(row) == ("preserved", "trajectory_generation")
            assert connection.execute(text("SELECT version_num FROM alembic_version")).scalar_one() == "0151"
        column = next(row for row in inspect(engine).get_columns("batches") if row["name"] == "purpose")
        assert column["nullable"] is False
        assert "trajectory_generation" in column["default"]
        # The old service omits purpose until its replica has been replaced.
        with engine.begin() as connection:
            legacy_id = uuid4()
            connection.execute(text(
                "INSERT INTO batches (id,team_id,name,task_filter,trial_config,"
                "created_by_token_prefix,expected_trial_count) "
                "VALUES (:id,:team,'old-replica','{}','{}','migration',0)"
            ), {"id": legacy_id, "team": team_id})
            assert connection.execute(text(
                "SELECT purpose FROM batches WHERE id=:id"
            ), {"id": legacy_id}).scalar_one() == "trajectory_generation"
        for purpose in (None, "unknown"):
            with pytest.raises(IntegrityError):
                with engine.begin() as connection:
                    connection.execute(text("UPDATE batches SET purpose=:purpose WHERE id=:id"),
                                       {"purpose": purpose, "id": batch_id})
        with engine.begin() as connection:
            connection.execute(text("UPDATE batches SET purpose='evaluation' WHERE id=:id"),
                               {"id": batch_id})
        command.upgrade(cfg, "head")
        with engine.connect() as connection:
            assert connection.execute(text("SELECT purpose FROM batches WHERE id=:id"),
                                      {"id": batch_id}).scalar_one() == "evaluation"
    finally:
        command.upgrade(cfg, "head")
        engine.dispose()
