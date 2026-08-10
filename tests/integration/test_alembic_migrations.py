"""Verify Alembic migrations apply cleanly and the in_flight_count trigger fires."""

from __future__ import annotations

import os
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, text
from testcontainers.postgres import PostgresContainer


@pytest.fixture(scope="module")
def postgres_url():
    with PostgresContainer("postgres:16") as pg:
        url = pg.get_connection_url().replace(
            "postgresql+psycopg2://", "postgresql+psycopg://",
        )
        os.environ["LOOM_DB_URL"] = url
        repo_root = Path(__file__).resolve().parents[2]
        # Use the venv's alembic via `python -m alembic` so PATH doesn't matter.
        subprocess.run(
            [sys.executable, "-m", "alembic", "-c", "migrations/alembic.ini", "upgrade", "head"],
            cwd=repo_root, check=True,
        )
        yield url


def test_all_tables_exist(postgres_url: str) -> None:
    engine = create_engine(postgres_url)
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT table_name
              FROM information_schema.tables
             WHERE table_schema = 'public'
        """))
        names = {row[0] for row in result}
    expected = {"teams", "team_quotas", "tasks", "agents", "workers",
                "trials", "tokens", "rate_cards", "llm_calls",
                "benchmarks", "pending_team_registrations",
                "slurm_worker_jobs", "gb10_worker_pool_desired_states",
                "gb10_worker_node_statuses",
                "worker_pool_autoscaler_policies",
                "dev_instances",
                "artifacts", "artifact_lineage_edges",
                "pipeline_runs", "pipeline_stage_runs",
                "pipeline_stage_dependencies", "pipeline_fanout_expansions",
                "execution_attempts", "pipeline_events",
                "pipeline_terminal_snapshots",
                "pipeline_acceptance_preflight_prerequisites",
                "pipeline_budget_ledgers", "pipeline_budget_reservations",
                "execution_attempt_provider_budgets", "pipeline_cancellation_outbox",
                "alembic_version"}
    assert expected.issubset(names)


def test_in_flight_count_trigger(postgres_url: str) -> None:
    engine = create_engine(postgres_url)
    team_id = uuid4()
    task_id = "demo"
    trial_id = uuid4()
    worker_id = uuid4()

    with engine.begin() as conn:
        conn.execute(text(
            "INSERT INTO teams (id, name) VALUES (:id, :name)"
        ), {"id": team_id, "name": "test"})
        conn.execute(text(
            "INSERT INTO team_quotas (team_id) VALUES (:tid)"
        ), {"tid": team_id})
        conn.execute(text(
            "INSERT INTO tasks (id, checksum, config) VALUES (:i, :c, '{}'::jsonb)"
        ), {"i": task_id, "c": "0" * 64})
        conn.execute(text(
            "INSERT INTO workers (id, hostname, version, capabilities, "
            "registered_at, last_seen_at, status) VALUES "
            "(:id, 'h', 'v', '[]'::jsonb, :now, :now, 'active')"
        ), {"id": worker_id, "now": datetime.now(UTC)})
        conn.execute(text(
            "INSERT INTO trials (id, team_id, task_id, config, requires_caps, state) "
            "VALUES (:id, :t, :ti, '{}'::jsonb, '{}'::jsonb, 'queued')"
        ), {"id": trial_id, "t": team_id, "ti": task_id})

    def in_flight() -> int:
        with engine.connect() as conn:
            return conn.execute(text(
                "SELECT in_flight_count FROM team_quotas WHERE team_id = :t"
            ), {"t": team_id}).scalar_one()

    assert in_flight() == 0

    # queued → claimed: +1
    with engine.begin() as conn:
        conn.execute(text(
            "UPDATE trials SET state='claimed', worker_id=:w WHERE id=:id"
        ), {"w": worker_id, "id": trial_id})
    assert in_flight() == 1

    # claimed → running: 0 (both active)
    with engine.begin() as conn:
        conn.execute(text("UPDATE trials SET state='running' WHERE id=:id"),
                     {"id": trial_id})
    assert in_flight() == 1

    # running → succeeded: -1
    with engine.begin() as conn:
        conn.execute(
            text("UPDATE trials SET state='succeeded', result='{}'::jsonb WHERE id=:id"),
            {"id": trial_id},
        )
    assert in_flight() == 0

    # Re-queue → +1 next time we go claimed
    with engine.begin() as conn:
        conn.execute(text("UPDATE trials SET state='queued' WHERE id=:id"),
                     {"id": trial_id})
        conn.execute(text("UPDATE trials SET state='claimed' WHERE id=:id"),
                     {"id": trial_id})
    assert in_flight() == 1
