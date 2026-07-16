"""Deterministic local family-runs seed contract (#696)."""

from __future__ import annotations

from uuid import UUID

import pytest
from scripts.seed_test_data import (
    _SLB_DEV_BASELINES,
    REPO_ROOT,
    _family_run_dev_task_rows,
    _seed_family_run_dev_tasks,
)
from sqlalchemy import create_engine, delete, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import sessionmaker

from loom.db.schema import Benchmark, Task
from loom.family_run.sequencers import RankingFileSequencer
from loom.models.task import TaskConfig
from loom.trajectory.storage import FakeObjectStore
from loom_worker.main_loop import _materialize_task_dir


@pytest.fixture
def seed_session(postgres_url: str):
    engine = create_engine(postgres_url)
    session_local = sessionmaker(engine)
    expected_benchmarks = {*_SLB_DEV_BASELINES, "skillflow-iterative"}
    task_ids = [row["id"] for row in _family_run_dev_task_rows()]
    with session_local() as session:
        session.execute(delete(Task).where(Task.id.in_(task_ids)))
        for benchmark_id in sorted(expected_benchmarks):
            session.execute(
                pg_insert(Benchmark)
                .values(
                    id=benchmark_id,
                    display_name=benchmark_id,
                    upstream_kind="git",
                    upstream_locator="dev-fixture",
                    upstream_revision="dev-fixture",
                    license_spdx="NOASSERTION",
                    license_url="",
                    splits=["test"],
                    series="skill",
                    imported_by="test_seed_family_run_dev_tasks",
                )
                .on_conflict_do_nothing(index_elements=[Benchmark.id])
            )
        session.commit()
    with session_local() as session:
        yield session
    with session_local() as session:
        session.execute(delete(Task).where(Task.id.in_(task_ids)))
        session.commit()
    engine.dispose()


def _snapshot(session) -> list[tuple]:  # type: ignore[no-untyped-def]
    task_ids = [row["id"] for row in _family_run_dev_task_rows()]
    rows = (
        session.execute(
            select(Task).where(Task.id.in_(task_ids)).order_by(Task.id),
        )
        .scalars()
        .all()
    )
    return [
        (row.id, row.benchmark_id, row.checksum, row.config, row.source, row.tags) for row in rows
    ]


def test_family_run_dev_seed_is_small_complete_and_idempotent(
    seed_session,
    capsys: pytest.CaptureFixture[str],
) -> None:
    assert _seed_family_run_dev_tasks(seed_session) == 28
    seed_session.commit()
    first = _snapshot(seed_session)

    assert len(first) == 28
    for benchmark_id in _SLB_DEV_BASELINES:
        assert sum(row[1] == benchmark_id for row in first) == 5
    skillflow = [row for row in first if row[1] == "skillflow-iterative"]
    assert len(skillflow) == 3
    assert {row[0].split("/", 1)[0] for row in skillflow} == {"skillflow-dev-family"}
    assert {row[5]["family_run_rank"] for row in skillflow} == {"0", "1", "2"}
    for _task_id, _benchmark_id, _checksum, config, source, _tags in first:
        TaskConfig.model_validate(config)
        assert source == "fixture://family-runs-dev/smoke"

    assert _seed_family_run_dev_tasks(seed_session) == 28
    seed_session.commit()
    assert _snapshot(seed_session) == first
    captured = capsys.readouterr()
    assert "dev-only-shared-jwt-key-do-not-use-in-prod" not in captured.out
    assert "dev-only-shared-jwt-key-do-not-use-in-prod" not in captured.err


def test_skillflow_dev_seed_consumes_checked_in_ranking(seed_session) -> None:
    _seed_family_run_dev_tasks(seed_session)
    seed_session.commit()
    tasks = (
        seed_session.execute(
            select(Task).where(Task.benchmark_id == "skillflow-iterative"),
        )
        .scalars()
        .all()
    )

    ordered = RankingFileSequencer().sequence(
        "skillflow-dev-family",
        list(reversed(tasks)),
        {"path": "worker-only/ALL_TASK_DIFFICULTY_RANKING.json"},
    )

    assert [task_id.rsplit("/", 1)[-1] for task_id in ordered] == [
        "task-c",
        "task-a",
        "task-b",
    ]


@pytest.mark.asyncio
async def test_family_run_dev_fixture_materializes_without_network() -> None:
    row = _family_run_dev_task_rows()[0]
    task_dir = await _materialize_task_dir(
        bundle=row,
        object_store=FakeObjectStore(),
        trial_id=UUID("00000000-0000-0000-0000-000000000001"),
        fixtures_root=REPO_ROOT / "tests" / "fixtures" / "tasks",
    )

    assert (task_dir / "task.toml").is_file()
    assert (task_dir / "solution" / "solve.sh").is_file()
    assert (task_dir / "tests" / "test_result.py").is_file()
