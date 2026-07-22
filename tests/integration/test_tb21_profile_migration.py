"""Migration coverage for immutable TB2 profile identities (#749)."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from uuid import UUID, uuid4

import pytest
from sqlalchemy import create_engine, text
from testcontainers.postgres import PostgresContainer

REPO_ROOT = Path(__file__).resolve().parents[2]
LEGACY_BENCHMARK_ID = "terminal-bench-2"
LEGACY_PROFILE_ID = "terminal-bench-2@tb2.0-91e10457"


def _alembic(url: str, *args: str) -> None:
    env = os.environ.copy()
    env["LOOM_DB_URL"] = url
    subprocess.run(
        [sys.executable, "-m", "alembic", "-c", "migrations/alembic.ini", *args],
        cwd=REPO_ROOT,
        env=env,
        check=True,
    )


@pytest.fixture()
def postgres_url_at_0061() -> Iterator[str]:
    with PostgresContainer("postgres:16") as pg:
        url = pg.get_connection_url().replace(
            "postgresql+psycopg2://", "postgresql+psycopg://",
        )
        _alembic(url, "upgrade", "0061")
        yield url


@dataclass(frozen=True)
class LegacyTaskAndTrial:
    task_id: str
    trial_id: UUID
    result: dict[str, object]


def _seed_legacy_benchmark(conn) -> None:  # type: ignore[no-untyped-def]
    conn.execute(
        text(
            "INSERT INTO benchmarks ("
            "id, display_name, upstream_kind, upstream_locator, "
            "upstream_revision, license_spdx, license_url, splits"
            ") VALUES ("
            ":id, :display_name, :upstream_kind, :upstream_locator, "
            ":upstream_revision, :license_spdx, :license_url, :splits"
            ") ON CONFLICT (id) DO NOTHING",
        ),
        {
            "id": LEGACY_BENCHMARK_ID,
            "display_name": "Terminal-Bench 2",
            "upstream_kind": "git",
            "upstream_locator": "laude-institute/terminal-bench",
            "upstream_revision": "91e10457",
            "license_spdx": "MIT",
            "license_url": "https://opensource.org/license/mit",
            "splits": ["test"],
        },
    )


def _seed_tb20_task_and_trial(url: str, task_name: str) -> LegacyTaskAndTrial:
    task_id = f"{LEGACY_BENCHMARK_ID}/{task_name}"
    trial_id = uuid4()
    team_id = uuid4()
    result: dict[str, object] = {
        "reward": 0,
        "verifier": {"summary": "legacy verifier result"},
    }
    engine = create_engine(url)
    with engine.begin() as conn:
        _seed_legacy_benchmark(conn)
        conn.execute(
            text("INSERT INTO teams (id, name) VALUES (:id, :name)"),
            {"id": team_id, "name": f"tb20-{team_id}"},
        )
        conn.execute(
            text(
                "INSERT INTO tasks ("
                "id, checksum, config, source, license, benchmark_id, tags"
                ") VALUES ("
                ":id, :checksum, CAST(:config AS jsonb), :source, :license, "
                ":benchmark_id, CAST(:tags AS jsonb)"
                ")",
            ),
            {
                "id": task_id,
                "checksum": "sha256:" + "a" * 64,
                "config": json.dumps({"task": {"id": task_id}}),
                "source": "s3://legacy/tb2/hello-world/",
                "license": "MIT",
                "benchmark_id": LEGACY_BENCHMARK_ID,
                "tags": json.dumps({"split": "test"}),
            },
        )
        conn.execute(
            text(
                "INSERT INTO trials ("
                "id, team_id, task_id, config, requires_caps, state, result"
                ") VALUES ("
                ":id, :team_id, :task_id, CAST(:config AS jsonb), "
                "CAST(:requires_caps AS jsonb), 'failed', CAST(:result AS jsonb)"
                ")",
            ),
            {
                "id": trial_id,
                "team_id": team_id,
                "task_id": task_id,
                "config": json.dumps({"agent": "legacy"}),
                "requires_caps": json.dumps({}),
                "result": json.dumps(result),
            },
        )
    engine.dispose()
    return LegacyTaskAndTrial(task_id=task_id, trial_id=trial_id, result=result)


def _seed_batch(url: str, task_filter: dict[str, object]) -> UUID:
    batch_id = uuid4()
    team_id = uuid4()
    engine = create_engine(url)
    with engine.begin() as conn:
        _seed_legacy_benchmark(conn)
        conn.execute(
            text("INSERT INTO teams (id, name) VALUES (:id, :name)"),
            {"id": team_id, "name": f"tb20-batch-{team_id}"},
        )
        conn.execute(
            text(
                "INSERT INTO batches ("
                "id, team_id, name, task_filter, trial_config, state, "
                "created_by_token_prefix, expected_trial_count"
                ") VALUES ("
                ":id, :team_id, 'Legacy TB2 batch', CAST(:task_filter AS jsonb), "
                "CAST(:trial_config AS jsonb), 'submitted', 'test', 0"
                ")",
            ),
            {
                "id": batch_id,
                "team_id": team_id,
                "task_filter": json.dumps(task_filter),
                "trial_config": json.dumps({}),
            },
        )
    engine.dispose()
    return batch_id


def _upgrade_to_0062(url: str) -> None:
    _alembic(url, "upgrade", "0062_tb21_profile_catalog")


def test_tb20_migration_preserves_task_and_trial(
    postgres_url_at_0061: str,
) -> None:
    legacy = _seed_tb20_task_and_trial(postgres_url_at_0061, "hello-world")

    _upgrade_to_0062(postgres_url_at_0061)

    engine = create_engine(postgres_url_at_0061)
    with engine.begin() as conn:
        task = conn.execute(
            text(
                "SELECT id, benchmark_id, source_provenance FROM tasks WHERE id = :id",
            ),
            {"id": legacy.task_id},
        ).mappings().one()
        trial = conn.execute(
            text("SELECT task_id, result FROM trials WHERE id = :id"),
            {"id": legacy.trial_id},
        ).mappings().one()
        profile = conn.execute(
            text(
                "SELECT display_name, execution_state, profile_provenance "
                "FROM benchmarks WHERE id = :id",
            ),
            {"id": LEGACY_PROFILE_ID},
        ).mappings().one()
        old_profile = conn.execute(
            text("SELECT 1 FROM benchmarks WHERE id = :id"),
            {"id": LEGACY_BENCHMARK_ID},
        ).scalar_one_or_none()
        aliases_table = conn.execute(
            text(
                "SELECT to_regclass('public.benchmark_aliases') "
                "AS table_name",
            ),
        ).scalar_one()
    engine.dispose()

    assert task["id"] == legacy.task_id
    assert task["benchmark_id"] == LEGACY_PROFILE_ID
    assert task["source_provenance"] == {}
    assert trial["task_id"] == legacy.task_id
    assert trial["result"] == legacy.result
    assert profile["display_name"] == "Terminal-Bench 2.0 (archived, 91e10457)"
    assert profile["execution_state"] == "historical"
    assert profile["profile_provenance"]["migration_revision"] == "0062_tb21_profile_catalog"
    assert old_profile is None
    assert aliases_table == "benchmark_aliases"


def test_old_tb20_batch_selectors_are_pinned(
    postgres_url_at_0061: str,
) -> None:
    singular_batch_id = _seed_batch(
        postgres_url_at_0061,
        {"benchmark_id": LEGACY_BENCHMARK_ID},
    )
    plural_batch_id = _seed_batch(
        postgres_url_at_0061,
        {"benchmark_ids": ["humaneval", LEGACY_BENCHMARK_ID]},
    )

    _upgrade_to_0062(postgres_url_at_0061)

    engine = create_engine(postgres_url_at_0061)
    with engine.begin() as conn:
        singular = conn.execute(
            text(
                "SELECT task_filter, resolved_task_ids FROM batches WHERE id = :id",
            ),
            {"id": singular_batch_id},
        ).mappings().one()
        plural = conn.execute(
            text(
                "SELECT task_filter, resolved_task_ids FROM batches WHERE id = :id",
            ),
            {"id": plural_batch_id},
        ).mappings().one()
    engine.dispose()

    assert singular["task_filter"] == {"benchmark_id": LEGACY_PROFILE_ID}
    assert singular["resolved_task_ids"] is None
    assert plural["task_filter"] == {
        "benchmark_ids": ["humaneval", LEGACY_PROFILE_ID],
    }
    assert plural["resolved_task_ids"] is None


def test_downgrade_restores_legacy_tb20_references(
    postgres_url_at_0061: str,
) -> None:
    legacy = _seed_tb20_task_and_trial(postgres_url_at_0061, "hello-world")
    batch_id = _seed_batch(
        postgres_url_at_0061,
        {"benchmark_id": LEGACY_BENCHMARK_ID},
    )

    _upgrade_to_0062(postgres_url_at_0061)
    _alembic(postgres_url_at_0061, "downgrade", "0061")

    engine = create_engine(postgres_url_at_0061)
    with engine.begin() as conn:
        task_benchmark_id = conn.execute(
            text("SELECT benchmark_id FROM tasks WHERE id = :id"),
            {"id": legacy.task_id},
        ).scalar_one()
        task_filter = conn.execute(
            text("SELECT task_filter FROM batches WHERE id = :id"),
            {"id": batch_id},
        ).scalar_one()
        legacy_profile = conn.execute(
            text("SELECT display_name FROM benchmarks WHERE id = :id"),
            {"id": LEGACY_BENCHMARK_ID},
        ).scalar_one()
        archived_profile = conn.execute(
            text("SELECT 1 FROM benchmarks WHERE id = :id"),
            {"id": LEGACY_PROFILE_ID},
        ).scalar_one_or_none()
    engine.dispose()

    assert task_benchmark_id == LEGACY_BENCHMARK_ID
    assert task_filter == {"benchmark_id": LEGACY_BENCHMARK_ID}
    assert legacy_profile == "Terminal-Bench 2"
    assert archived_profile is None
