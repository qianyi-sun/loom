"""Migration 0051 — user TaskSet foundation schema (#242 sub-plan 1).

Verifies upgrade/downgrade and that native ``benchmarks`` rows are
unchanged by the migration.
"""

from __future__ import annotations

import os
import subprocess
import sys
from collections.abc import Iterator
from pathlib import Path
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, text
from testcontainers.postgres import PostgresContainer

REPO_ROOT = Path(__file__).resolve().parents[2]

_BENCHMARK_SEED = (
    {
        "id": "humaneval",
        "display_name": "HumanEval",
        "upstream_kind": "huggingface",
        "upstream_locator": "upstream/humaneval",
        "upstream_revision": "",
        "license_spdx": "MIT",
        "license_url": "https://example/humaneval",
        "splits": ["test"],
    },
    {
        "id": "mbpp",
        "display_name": "MBPP",
        "upstream_kind": "huggingface",
        "upstream_locator": "upstream/mbpp",
        "upstream_revision": "",
        "license_spdx": "CC-BY-4.0",
        "license_url": "https://example/mbpp",
        "splits": ["test"],
    },
)


def _alembic(url: str, *args: str) -> None:
    env = os.environ.copy()
    env["LOOM_DB_URL"] = url
    subprocess.run(
        [sys.executable, "-m", "alembic", "-c", "migrations/alembic.ini", *args],
        cwd=REPO_ROOT,
        env=env,
        check=True,
    )


def _seed_benchmarks(url: str) -> list[dict[str, object]]:
    engine = create_engine(url)
    with engine.begin() as conn:
        for row in _BENCHMARK_SEED:
            conn.execute(
                text(
                    "INSERT INTO benchmarks ("
                    "id, display_name, upstream_kind, upstream_locator, "
                    "upstream_revision, license_spdx, license_url, splits"
                    ") VALUES ("
                    ":id, :display_name, :upstream_kind, :upstream_locator, "
                    ":upstream_revision, :license_spdx, :license_url, :splits"
                    ")",
                ),
                {
                    **row,
                    "splits": row["splits"],
                },
            )
    engine.dispose()
    return [dict(row) for row in _BENCHMARK_SEED]


def _snapshot_benchmarks(url: str) -> list[dict[str, object]]:
    engine = create_engine(url)
    with engine.begin() as conn:
        rows = conn.execute(
            text(
                "SELECT id, display_name, upstream_kind, upstream_locator, "
                "upstream_revision, license_spdx, license_url, splits "
                "FROM benchmarks ORDER BY id",
            ),
        ).mappings().all()
    engine.dispose()
    return [dict(row) for row in rows]


@pytest.fixture(scope="module")
def postgres_url_at_0050() -> Iterator[str]:
    with PostgresContainer("postgres:16") as pg:
        url = pg.get_connection_url().replace(
            "postgresql+psycopg2://", "postgresql+psycopg://",
        )
        _alembic(url, "upgrade", "0050")
        yield url


def test_upgrade_creates_task_set_tables_preserves_benchmarks(
    postgres_url_at_0050: str,
) -> None:
    seeded = _seed_benchmarks(postgres_url_at_0050)
    before = _snapshot_benchmarks(postgres_url_at_0050)
    assert len(before) == len(seeded)

    _alembic(postgres_url_at_0050, "upgrade", "0051")

    engine = create_engine(postgres_url_at_0050)
    with engine.begin() as conn:
        names = {
            row[0]
            for row in conn.execute(
                text(
                    "SELECT table_name FROM information_schema.tables "
                    "WHERE table_schema = 'public'",
                ),
            )
        }
    engine.dispose()
    assert {"task_sets", "task_set_manifests"}.issubset(names)

    after = _snapshot_benchmarks(postgres_url_at_0050)
    assert after == before

    _alembic(postgres_url_at_0050, "downgrade", "0050")


def test_downgrade_drops_task_set_tables(postgres_url_at_0050: str) -> None:
    _alembic(postgres_url_at_0050, "upgrade", "0051")
    _alembic(postgres_url_at_0050, "downgrade", "0050")

    engine = create_engine(postgres_url_at_0050)
    with engine.begin() as conn:
        names = {
            row[0]
            for row in conn.execute(
                text(
                    "SELECT table_name FROM information_schema.tables "
                    "WHERE table_schema = 'public'",
                ),
            )
        }
    engine.dispose()
    assert "task_sets" not in names
    assert "task_set_manifests" not in names


def test_manifest_fk_cascade_on_task_set_delete(postgres_url_at_0050: str) -> None:
    _alembic(postgres_url_at_0050, "upgrade", "0051")
    team_id = str(uuid4())
    slug = "cascade-test"
    task_set_id = f"ts/{team_id}/{slug}"

    engine = create_engine(postgres_url_at_0050)
    with engine.begin() as conn:
        conn.execute(
            text("INSERT INTO teams (id, name) VALUES (:id, :name)"),
            {"id": team_id, "name": f"t-{team_id}"},
        )
        conn.execute(
            text(
                "INSERT INTO task_sets ("
                "id, owning_team_id, slug, display_name, status, intents, "
                "manifest_blob_uri"
                ") VALUES ("
                ":id, :team, :slug, 'Cascade Test', 'materializing', "
                "ARRAY['trajectory_generation']::text[], 's3://bucket/manifest.yaml'"
                ")",
            ),
            {"id": task_set_id, "team": team_id, "slug": slug},
        )
        conn.execute(
            text(
                "INSERT INTO task_set_manifests (task_set_id, schema_version, manifest) "
                "VALUES (:id, 1, '{}'::jsonb)",
            ),
            {"id": task_set_id},
        )
        conn.execute(
            text("DELETE FROM task_sets WHERE id = :id"),
            {"id": task_set_id},
        )
        remaining = conn.execute(
            text("SELECT count(*) FROM task_set_manifests WHERE task_set_id = :id"),
            {"id": task_set_id},
        ).scalar_one()
    engine.dispose()
    assert remaining == 0
    _alembic(postgres_url_at_0050, "downgrade", "0050")
