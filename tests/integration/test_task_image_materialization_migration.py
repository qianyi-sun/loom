from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path
from uuid import uuid4

from sqlalchemy import create_engine, insert, select
from sqlalchemy.orm import sessionmaker
from testcontainers.postgres import PostgresContainer

from loom.db.schema import (
    Task,
    TaskImageMaterialization,
    Team,
    Trial,
    TrialTaskImageMaterialization,
)

REPO_ROOT = Path(__file__).resolve().parents[2]


def _alembic(database_url: str, command: str, revision: str) -> None:
    subprocess.run(
        [
            sys.executable,
            "-m",
            "alembic",
            "-c",
            "migrations/alembic.ini",
            command,
            revision,
        ],
        cwd=REPO_ROOT,
        env={**os.environ, "LOOM_DB_URL": database_url},
        check=True,
    )


def test_migration_backfills_unlinked_dockerfile_trials_and_round_trips() -> None:
    with PostgresContainer("postgres:16") as postgres:
        database_url = postgres.get_connection_url().replace(
            "postgresql+psycopg2://",
            "postgresql+psycopg://",
        )
        _alembic(database_url, "upgrade", "0097")
        engine = create_engine(database_url)
        sessions = sessionmaker(engine)
        team_id = uuid4()
        trial_id = uuid4()
        task_id = f"legacy-task-image/{uuid4()}"
        task_config = {
            "schema_version": "1",
            "task": {"id": task_id, "name": task_id},
            "environment": {
                "os": "linux",
                "cpu_arch": "any",
                "dockerfile": "environment/Dockerfile",
            },
            "agent": {"name": "oracle"},
            "verifier": {"name": "pytest"},
            "steps": [{"name": "main"}],
        }
        try:
            with sessions.begin() as session:
                session.execute(insert(Team).values(id=team_id, name=f"legacy-{team_id}"))
                session.execute(
                    insert(Task).values(
                        id=task_id,
                        checksum="sha256:" + "a" * 64,
                        config=task_config,
                    )
                )
                session.execute(
                    insert(Trial).values(
                        id=trial_id,
                        team_id=team_id,
                        task_id=task_id,
                        config={},
                        requires_caps={
                            "os": "linux",
                            "cpu_arch": "any",
                            "gpu_vendor": "none",
                            "network_policies": ["public"],
                        },
                        state="queued",
                    )
                )

            for _round in range(2):
                _alembic(database_url, "upgrade", "head")
                with sessions() as session:
                    materializations = session.scalars(
                        select(TaskImageMaterialization)
                        .where(TaskImageMaterialization.task_id == task_id)
                        .order_by(TaskImageMaterialization.cpu_arch)
                    ).all()
                    links = session.scalars(
                        select(TrialTaskImageMaterialization).where(
                            TrialTaskImageMaterialization.trial_id == trial_id
                        )
                    ).all()
                assert [row.cpu_arch for row in materializations] == ["arm64", "x86_64"]
                assert all(row.state == "queued" for row in materializations)
                assert {row.materialization_id for row in links} == {
                    row.id for row in materializations
                }
                _alembic(database_url, "downgrade", "0097")
        finally:
            engine.dispose()
