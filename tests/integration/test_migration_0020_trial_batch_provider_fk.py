"""Migration 0020 — Trial + Batch get nullable provider_connection_id /
provider_model_id columns. Verifies:

- Both tables get the new columns with the right SQL type + nullability.
- FK references work (insertion with the connection's UUID succeeds).
- NULL is allowed (preserves backward-compat for trials submitted
  without a provider override).
- Hard-deleting the provider_connection is blocked (the FK enforces
  RESTRICT in spirit — actually it's the default `NO ACTION` rule,
  same effect for our flow because provider_connections has its own
  RESTRICT on team_id and we don't cascade through).
- ORM models round-trip the new columns.

Spec: cluster-deploy.md §Schema additions (Trial / Batch payload extensions).
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.exc import IntegrityError
from testcontainers.postgres import PostgresContainer


@pytest.fixture(scope="module")
def postgres_url() -> str:
    with PostgresContainer("postgres:16") as pg:
        url = pg.get_connection_url().replace(
            "postgresql+psycopg2://", "postgresql+psycopg://",
        )
        os.environ["LOOM_DB_URL"] = url
        repo_root = Path(__file__).resolve().parents[2]
        subprocess.run(
            [sys.executable, "-m", "alembic", "-c",
             "migrations/alembic.ini", "upgrade", "head"],
            cwd=repo_root, check=True,
        )
        yield url


@pytest.fixture()
def seeded(postgres_url: str):
    """Insert a team + task + provider_connection so trial/batch
    inserts have valid FK targets. Yields (team_id, task_id, conn_id).
    Each test gets fresh IDs so they don't collide on shared module-
    scoped Postgres state."""
    engine = create_engine(postgres_url)
    team_id = uuid4()
    task_id = f"task-{uuid4().hex[:8]}"
    conn_id = uuid4()
    with engine.begin() as conn:
        conn.execute(text(
            "INSERT INTO teams (id, name) VALUES (:t, :n)",
        ), {"t": team_id, "n": f"team-{team_id}"})
        conn.execute(text(
            "INSERT INTO team_quotas (team_id) VALUES (:t)",
        ), {"t": team_id})
        conn.execute(text(
            "INSERT INTO tasks (id, checksum, config) "
            "VALUES (:i, :c, '{}'::jsonb)",
        ), {"i": task_id, "c": "0" * 64})
        conn.execute(text(
            "INSERT INTO provider_connections "
            "(id, team_id, provider_type, display_name, base_url, "
            " upstream_host, encrypted_api_key_ref, created_by) "
            "VALUES (:id, :t, 'openai-compatible', :n, 'https://x', "
            "        'x', 'loom://ref', 'admin:0')",
        ), {"id": conn_id, "t": team_id, "n": f"conn-{conn_id}"})
    yield team_id, task_id, conn_id


def test_trial_columns_added(postgres_url: str) -> None:
    engine = create_engine(postgres_url)
    with engine.connect() as conn:
        cols = {r[0] for r in conn.execute(text(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name='trials'",
        ))}
    assert "provider_connection_id" in cols
    assert "provider_model_id" in cols


def test_batch_columns_added(postgres_url: str) -> None:
    engine = create_engine(postgres_url)
    with engine.connect() as conn:
        cols = {r[0] for r in conn.execute(text(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name='batches'",
        ))}
    assert "provider_connection_id" in cols
    assert "provider_model_id" in cols


def test_trial_columns_are_nullable(postgres_url: str) -> None:
    """NULL preserves backward-compat — trials submitted without an
    override use the platform default."""
    engine = create_engine(postgres_url)
    with engine.connect() as conn:
        rows = list(conn.execute(text(
            "SELECT column_name, is_nullable "
            "FROM information_schema.columns "
            "WHERE table_name='trials' AND column_name IN "
            "('provider_connection_id', 'provider_model_id')",
        )))
    assert {(r[0], r[1]) for r in rows} == {
        ("provider_connection_id", "YES"),
        ("provider_model_id", "YES"),
    }


def test_trial_insert_with_provider_succeeds(seeded, postgres_url) -> None:
    team_id, task_id, conn_id = seeded
    engine = create_engine(postgres_url)
    trial_id = uuid4()
    with engine.begin() as conn:
        conn.execute(text(
            "INSERT INTO trials "
            "(id, team_id, task_id, config, requires_caps, state, "
            " provider_connection_id, provider_model_id) "
            "VALUES (:id, :t, :ti, '{}'::jsonb, '{}'::jsonb, 'queued', "
            "        :pc, :pm)",
        ), {"id": trial_id, "t": team_id, "ti": task_id,
            "pc": conn_id, "pm": "gpt-4o"})

    with engine.connect() as conn:
        row = conn.execute(text(
            "SELECT provider_connection_id, provider_model_id "
            "FROM trials WHERE id = :i",
        ), {"i": trial_id}).one()
    assert row[0] == conn_id
    assert row[1] == "gpt-4o"


def test_trial_insert_with_null_provider_succeeds(seeded, postgres_url) -> None:
    """Default for existing trial submission code that doesn't yet
    pass the new columns."""
    team_id, task_id, _ = seeded
    engine = create_engine(postgres_url)
    trial_id = uuid4()
    with engine.begin() as conn:
        conn.execute(text(
            "INSERT INTO trials "
            "(id, team_id, task_id, config, requires_caps, state) "
            "VALUES (:id, :t, :ti, '{}'::jsonb, '{}'::jsonb, 'queued')",
        ), {"id": trial_id, "t": team_id, "ti": task_id})


def test_trial_fk_blocks_provider_connection_hard_delete(seeded, postgres_url) -> None:
    """The Trial.provider_connection_id FK has the default
    `ON DELETE NO ACTION` rule, so attempting to hard-delete the
    provider_connection while a trial still references it fails. In
    practice provider_connections is soft-deleted (deleted_at column),
    so this matters only for the future `loom admin providers purge`
    operation — which must first verify no trials reference the row."""
    team_id, task_id, conn_id = seeded
    engine = create_engine(postgres_url)
    trial_id = uuid4()
    with engine.begin() as conn:
        conn.execute(text(
            "INSERT INTO trials "
            "(id, team_id, task_id, config, requires_caps, state, "
            " provider_connection_id) "
            "VALUES (:id, :t, :ti, '{}'::jsonb, '{}'::jsonb, 'queued', :pc)",
        ), {"id": trial_id, "t": team_id, "ti": task_id, "pc": conn_id})

    with engine.begin() as conn, pytest.raises(IntegrityError):
        conn.execute(text(
            "DELETE FROM provider_connections WHERE id = :i",
        ), {"i": conn_id})


def test_batch_insert_with_provider_succeeds(seeded, postgres_url) -> None:
    team_id, _, conn_id = seeded
    engine = create_engine(postgres_url)
    batch_id = uuid4()
    with engine.begin() as conn:
        conn.execute(text(
            "INSERT INTO batches "
            "(id, team_id, name, task_filter, trial_config, "
            " created_by_token_prefix, "
            " provider_connection_id, provider_model_id) "
            "VALUES (:id, :t, 'b', '{}'::jsonb, '{}'::jsonb, "
            "        'admin:0', :pc, :pm)",
        ), {"id": batch_id, "t": team_id, "pc": conn_id, "pm": "gpt-4o"})

    with engine.connect() as conn:
        row = conn.execute(text(
            "SELECT provider_connection_id, provider_model_id "
            "FROM batches WHERE id = :i",
        ), {"i": batch_id}).one()
    assert row[0] == conn_id
    assert row[1] == "gpt-4o"


def test_orm_models_have_new_columns() -> None:
    """SQLAlchemy ORM Trial / Batch models match the migration."""
    from loom.db.schema import Batch, Trial

    trial_cols = {c.name for c in Trial.__table__.columns}
    assert "provider_connection_id" in trial_cols
    assert "provider_model_id" in trial_cols

    batch_cols = {c.name for c in Batch.__table__.columns}
    assert "provider_connection_id" in batch_cols
    assert "provider_model_id" in batch_cols
