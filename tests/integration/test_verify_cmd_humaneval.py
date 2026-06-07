"""verify subcommand end-to-end against a seeded HumanEval bundle
(Plan 16 Task 2).

Uses FakeObjectStore + monkey-patches oracle_runner to avoid the
docker-run round trip (covered separately by the system smoke test
in tests/system/test_benchmark_humaneval_smoke.py)."""

from __future__ import annotations

import json
from collections.abc import Iterator
from pathlib import Path

import pytest
from sqlalchemy import create_engine, delete

from loom.db.schema import Benchmark
from loom.db.schema import Task as TaskRow
from loom.trajectory.storage import FakeObjectStore
from loom_benchmark_tool.import_cmd import run_import
from loom_benchmark_tool.oracle_runner import OracleResult
from loom_benchmark_tool.verify_cmd import run_verify

_FIXTURE = (
    Path(__file__).resolve().parents[2]
    / "packages/loom-benchmarks/tests/fixtures/humaneval/sample.json"
)


@pytest.fixture
def _cleanup(postgres_url: str) -> Iterator[None]:
    yield
    engine = create_engine(postgres_url)
    with engine.begin() as conn:
        conn.execute(
            delete(TaskRow).where(TaskRow.benchmark_id == "humaneval"),
        )
        conn.execute(delete(Benchmark).where(Benchmark.id == "humaneval"))
    engine.dispose()


async def test_verify_passes_for_seeded_humaneval(
    postgres_url: str,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    _cleanup: None,
) -> None:
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

    # Stub the oracle: this integration test exercises the verify
    # pipeline (download → validate → tally), not the docker-run path.
    async def _fake_oracle(
        *, task_id: str, task_dir: Path, image: str,
    ) -> OracleResult:
        assert (task_dir / "task.toml").exists(), (
            "verify must download the task bundle before running oracle"
        )
        return OracleResult(
            task_id=task_id, passed=True, return_code=0,
            stdout_tail="ok", stderr_tail="",
        )

    monkeypatch.setattr(
        "loom_benchmark_tool.verify_cmd.run_oracle_for_task",
        _fake_oracle,
    )

    store = FakeObjectStore()
    bucket = "loom-benchmarks"

    await run_import(
        benchmark="humaneval",
        db_url=postgres_url,
        object_store=store,
        bucket=bucket,
        cache_dir=tmp_path / "cache",
        limit=None,
    )

    report = await run_verify(
        benchmark="humaneval", object_store=store,
        bucket=bucket, limit=10,
    )
    assert report["total"] == len(fixture_records)
    assert report["passed"] == len(fixture_records)
    assert report["failed"] == 0
    ids = {r["task_id"] for r in report["results"]}
    assert ids == {f"humaneval/HumanEval/{i}" for i in range(len(fixture_records))}


async def test_verify_empty_benchmark_returns_zero(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = FakeObjectStore()
    report = await run_verify(
        benchmark="nonexistent", object_store=store,
        bucket="loom-benchmarks", limit=10,
    )
    assert report == {"total": 0, "passed": 0, "failed": 0, "results": []}


async def test_verify_samples_when_limit_below_total(
    postgres_url: str,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    _cleanup: None,
) -> None:
    """limit=1 against 2 imported tasks samples deterministically."""
    from loom_benchmarks.adapters import humaneval as hv
    from loom_benchmarks.base import BenchmarkInstance

    fixture_records = json.loads(_FIXTURE.read_text())
    assert len(fixture_records) >= 2

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

    async def _fake_oracle(
        *, task_id: str, task_dir: Path, image: str,
    ) -> OracleResult:
        return OracleResult(
            task_id=task_id, passed=True, return_code=0,
            stdout_tail="", stderr_tail="",
        )

    monkeypatch.setattr(
        "loom_benchmark_tool.verify_cmd.run_oracle_for_task",
        _fake_oracle,
    )

    store = FakeObjectStore()
    await run_import(
        benchmark="humaneval",
        db_url=postgres_url,
        object_store=store,
        bucket="loom-benchmarks",
        cache_dir=tmp_path / "cache",
    )

    report = await run_verify(
        benchmark="humaneval", object_store=store,
        bucket="loom-benchmarks", limit=1, seed=42,
    )
    assert report["total"] == 1
    assert report["passed"] == 1
