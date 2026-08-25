"""Migration coverage for fail-closed removal of a retired provider ledger."""

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


def test_0111_refuses_rows_then_drops_empty_ledger(
    isolated_migration_postgres_url: str,
) -> None:
    config = _config(isolated_migration_postgres_url)
    command.downgrade(config, "0110")
    engine = create_engine(isolated_migration_postgres_url)
    provider = "day" + "tona"
    table = provider + "_sandboxes"
    team_id, worker_id, trial_id = (uuid4() for _ in range(3))
    task_id = f"retired-provider/{uuid4()}"
    try:
        with engine.begin() as connection:
            connection.execute(
                text("INSERT INTO teams (id, name) VALUES (:id, :name)"),
                {"id": team_id, "name": f"retired-provider-{team_id}"},
            )
            connection.execute(
                text(
                    "INSERT INTO tasks (id, config, checksum) "
                    "VALUES (:id, '{}'::jsonb, :checksum)",
                ),
                {"id": task_id, "checksum": "e" * 64},
            )
            connection.execute(
                text(
                    """
                    INSERT INTO workers (
                        id, hostname, version, capabilities,
                        registered_at, last_seen_at, status
                    ) VALUES (
                        :id, :hostname, 'test', '[]'::jsonb,
                        now(), now(), 'active'
                    )
                    """,
                ),
                {"id": worker_id, "hostname": f"retired-provider-{worker_id}"},
            )
            connection.execute(
                text(
                    """
                    INSERT INTO trials (
                        id, team_id, task_id, config, requires_caps,
                        state, attempt_count
                    ) VALUES (
                        :id, :team_id, :task_id, '{}'::jsonb,
                        '{}'::jsonb, 'queued', 1
                    )
                    """,
                ),
                {
                    "id": trial_id,
                    "team_id": team_id,
                    "task_id": task_id,
                },
            )
            connection.execute(
                text(
                    f"""
                    INSERT INTO {table} (
                        trial_id, attempt_count, team_id, worker_id,
                        candidate_sha, provider_scope, artifact_ref,
                        sandbox_name, state, deadline_at
                    ) VALUES (
                        :trial_id, 1, :team_id, :worker_id,
                        :candidate_sha, :provider_scope, :artifact_ref,
                        :sandbox_name, 'reserved', now() + interval '1 hour'
                    )
                    """,
                ),
                {
                    "trial_id": trial_id,
                    "team_id": team_id,
                    "worker_id": worker_id,
                    "candidate_sha": "a" * 40,
                    "provider_scope": "b" * 64,
                    "artifact_ref": "registry.example/task@sha256:" + "c" * 64,
                    "sandbox_name": f"retired-provider-{trial_id.hex}",
                },
            )

        with pytest.raises(DBAPIError, match="cannot retire " + provider.title()):
            command.upgrade(config, "0111")
        assert table in inspect(engine).get_table_names()

        with engine.begin() as connection:
            connection.execute(text(f"DELETE FROM {table}"))
            connection.execute(text("DELETE FROM trials WHERE id=:id"), {"id": trial_id})
            connection.execute(text("DELETE FROM workers WHERE id=:id"), {"id": worker_id})
            connection.execute(text("DELETE FROM tasks WHERE id=:id"), {"id": task_id})
            connection.execute(text("DELETE FROM teams WHERE id=:id"), {"id": team_id})

        command.upgrade(config, "0111")
        assert table not in inspect(engine).get_table_names()
    finally:
        engine.dispose()
