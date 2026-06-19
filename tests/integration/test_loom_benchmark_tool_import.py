"""End-to-end ingestion: `run_import` pulls a fixture'd HumanEval
slice, uploads bundles to the in-memory store, and upserts
benchmarks + tasks rows in Postgres (Plan 14 Task 6)."""

from __future__ import annotations

import json
from collections.abc import Iterator
from pathlib import Path

import pytest
from sqlalchemy import create_engine, delete, text
from sqlalchemy.orm import sessionmaker

from loom.db.schema import Benchmark
from loom.db.schema import Task as TaskRow
from loom.trajectory.storage import FakeObjectStore
from loom_benchmark_tool.import_cmd import run_import

_FIXTURE = (
    Path(__file__).resolve().parents[2]
    / "packages/loom-benchmarks/tests/fixtures/humaneval/sample.json"
)


@pytest.fixture
def _cleanup(postgres_url: str) -> Iterator[None]:
    """Drop benchmarks + tasks rows after each test so the upsert path
    is exercised cleanly and the session-scoped Postgres container
    doesn't accumulate state."""
    yield
    # postgres_url is already postgresql+psycopg:// (psycopg3); the
    # same URL works for both sync and async engines.
    engine = create_engine(postgres_url)
    session_local = sessionmaker(engine)
    with session_local() as s:
        s.execute(delete(TaskRow).where(TaskRow.benchmark_id == "humaneval"))
        s.execute(delete(Benchmark).where(Benchmark.id == "humaneval"))
        s.commit()
    engine.dispose()


async def test_import_humaneval_with_fixture(
    postgres_url: str,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    _cleanup: None,
) -> None:
    """The HF cache is bypassed entirely: monkeypatch list_instances to
    read the local fixture, and stub fetch_upstream so we don't hit the
    network. The conversion + upload + DB upsert path is the unit under
    test."""
    from loom_benchmarks.adapters import humaneval as hv
    from loom_benchmarks.base import BenchmarkInstance

    fixture_records = json.loads(_FIXTURE.read_text())

    def _fake_list(
        self: hv.HumanEvalAdapter, *, source_dir: Path, split: str,
    ) -> Iterator[BenchmarkInstance]:
        for r in fixture_records:
            yield BenchmarkInstance(
                instance_id=r["task_id"], split=split, raw=r,
            )

    monkeypatch.setattr(hv.HumanEvalAdapter, "list_instances", _fake_list)
    # fetch_upstream pinging HF would slow + flake the test; stub it.
    monkeypatch.setattr(
        "loom_benchmark_tool.import_cmd.fetch_upstream",
        lambda *a, **kw: tmp_path / "stub-source",
    )

    store = FakeObjectStore()
    bucket = "loom-benchmarks"

    stats = await run_import(
        benchmark="humaneval",
        db_url=postgres_url,
        object_store=store,
        bucket=bucket,
        cache_dir=tmp_path / "cache",
        limit=None,
        imported_by="ci",
    )

    assert stats["converted"] == len(fixture_records)
    assert stats["warnings"] == 0

    # DB state.
    # postgres_url is already postgresql+psycopg:// (psycopg3); the
    # same URL works for both sync and async engines.
    engine = create_engine(postgres_url)
    with engine.begin() as conn:
        n_tasks = conn.execute(text(
            "SELECT count(*) FROM tasks WHERE benchmark_id = 'humaneval'",
        )).scalar_one()
        bench_row = conn.execute(text(
            "SELECT license_spdx, upstream_kind, imported_by FROM benchmarks "
            "WHERE id='humaneval'",
        )).one()
        task_row = conn.execute(text(
            "SELECT license, source FROM tasks "
            "WHERE id='humaneval/HumanEval/0'",
        )).one()
    engine.dispose()

    assert n_tasks == len(fixture_records)
    assert bench_row.license_spdx == "MIT"
    assert bench_row.upstream_kind == "huggingface"
    assert bench_row.imported_by == "ci"
    assert task_row.license == "MIT"
    assert task_row.source == "s3://loom-benchmarks/humaneval/HumanEval/0/"

    # MinIO state: task.toml + solution + tests landed under the prefix.
    body = await store.get_object(
        bucket=bucket, key="humaneval/HumanEval/0/task.toml",
    )
    assert b"schema_version" in body
    body = await store.get_object(
        bucket=bucket, key="humaneval/HumanEval/0/solution/solution.py",
    )
    assert b"has_close_elements" in body


async def test_import_filters_specific_instance_ids(
    postgres_url: str,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    _cleanup: None,
) -> None:
    """Operators can import a deterministic smoke slice without relying
    on adapter iteration order. This is the same path first-wave benchmark
    onboarding uses for cheap Terminal-Bench-2 smoke tests."""
    from loom_benchmarks.adapters import humaneval as hv
    from loom_benchmarks.base import BenchmarkInstance

    fixture_records = json.loads(_FIXTURE.read_text())
    skipped_id = fixture_records[0]["task_id"]
    target_id = fixture_records[1]["task_id"]

    def _fake_list(
        self: hv.HumanEvalAdapter, *, source_dir: Path, split: str,
    ) -> Iterator[BenchmarkInstance]:
        for r in fixture_records:
            yield BenchmarkInstance(
                instance_id=r["task_id"], split=split, raw=r,
            )

    monkeypatch.setattr(hv.HumanEvalAdapter, "list_instances", _fake_list)
    monkeypatch.setattr(
        "loom_benchmark_tool.import_cmd.fetch_upstream",
        lambda *a, **kw: tmp_path / "stub-source",
    )

    store = FakeObjectStore()
    bucket = "loom-benchmarks"

    stats = await run_import(
        benchmark="humaneval",
        db_url=postgres_url,
        object_store=store,
        bucket=bucket,
        cache_dir=tmp_path / "cache",
        instance_ids={target_id},
        imported_by="ci",
    )

    assert stats["converted"] == 1
    assert stats["warnings"] == 0

    engine = create_engine(postgres_url)
    with engine.begin() as conn:
        task_ids = conn.execute(text(
            "SELECT id FROM tasks WHERE benchmark_id = :benchmark_id ORDER BY id",
        ), {"benchmark_id": "humaneval"}).scalars().all()
    engine.dispose()

    assert task_ids == [f"humaneval/{target_id}"]

    body = await store.get_object(
        bucket=bucket, key=f"humaneval/{target_id}/task.toml",
    )
    assert b"schema_version" in body
    with pytest.raises(KeyError):
        await store.get_object(
            bucket=bucket, key=f"humaneval/{skipped_id}/task.toml",
        )


async def test_import_idempotent(
    postgres_url: str,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    _cleanup: None,
) -> None:
    """Running import twice over the same fixture upserts; row count
    stays equal to the fixture size."""
    from loom_benchmarks.adapters import humaneval as hv
    from loom_benchmarks.base import BenchmarkInstance

    fixture_records = json.loads(_FIXTURE.read_text())

    def _fake_list(
        self: hv.HumanEvalAdapter, *, source_dir: Path, split: str,
    ) -> Iterator[BenchmarkInstance]:
        for r in fixture_records:
            yield BenchmarkInstance(
                instance_id=r["task_id"], split=split, raw=r,
            )

    monkeypatch.setattr(hv.HumanEvalAdapter, "list_instances", _fake_list)
    monkeypatch.setattr(
        "loom_benchmark_tool.import_cmd.fetch_upstream",
        lambda *a, **kw: tmp_path / "stub-source",
    )

    store = FakeObjectStore()
    bucket = "loom-benchmarks"

    for _ in range(2):
        await run_import(
            benchmark="humaneval",
            db_url=postgres_url,
            object_store=store,
            bucket=bucket,
            cache_dir=tmp_path / "cache",
        )

    # postgres_url is already postgresql+psycopg:// (psycopg3); the
    # same URL works for both sync and async engines.
    engine = create_engine(postgres_url)
    with engine.begin() as conn:
        n_tasks = conn.execute(text(
            "SELECT count(*) FROM tasks WHERE benchmark_id = 'humaneval'",
        )).scalar_one()
        n_bench = conn.execute(text(
            "SELECT count(*) FROM benchmarks WHERE id='humaneval'",
        )).scalar_one()
    engine.dispose()

    assert n_tasks == len(fixture_records)
    assert n_bench == 1
