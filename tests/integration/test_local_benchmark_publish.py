"""Integration coverage for user-owned local benchmark publishing (#275)."""
from __future__ import annotations

from pathlib import Path

import pytest
from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from loom.db.schema import Benchmark
from loom.db.schema import Task as TaskRow
from loom.trajectory.storage import FakeObjectStore
from loom_cli.local_benchmark_publish import (
    PUBLISH_IMPORTED_BY,
    S3_FOLDER_KIND,
    publish_local_benchmark,
)

_TASK_TOML = """\
schema_version = "1"

[task]
id = "{tid}"
name = "Sample task {tid}"

[environment]
os = "linux"
docker_image = "python:3.11-alpine"

[agent]
name = "oracle"

[verifier]
name = "pytest"

[[steps]]
name = "main"
"""


def _write_layout(root: Path) -> None:
    root.mkdir(parents=True)
    (root / "benchmark.toml").write_text(
        'schema_version = 1\n'
        'id = "team-evals"\n'
        'display_name = "Team evaluations"\n'
        'series = "internal"\n'
        'license_spdx = "MIT"\n',
    )
    alpha = root / "tasks" / "alpha"
    alpha.mkdir(parents=True)
    (alpha / "task.toml").write_text(_TASK_TOML.format(tid="alpha"))
    (alpha / "instruction.md").write_text("do alpha\n")


@pytest.mark.asyncio
async def test_publish_local_benchmark_uploads_and_registers(
    postgres_url: str, tmp_path: Path,
) -> None:
    root = tmp_path / "team-evals"
    _write_layout(root)
    store = FakeObjectStore()

    stats = await publish_local_benchmark(
        root,
        db_url=postgres_url,
        object_store=store,
        bucket="loom-benchmarks",
    )

    assert stats.benchmark_id == "team-evals"
    assert stats.task_count == 1
    assert stats.inserted == 1
    assert stats.updated == 0
    assert stats.unchanged == 0
    assert stats.uploaded_objects == 2
    assert stats.source_prefix == "s3://loom-benchmarks/team-evals/"
    assert ("loom-benchmarks", "team-evals/alpha/task.toml") in store.objects
    assert ("loom-benchmarks", "team-evals/alpha/instruction.md") in store.objects

    engine = create_async_engine(postgres_url)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with factory() as session:
            bench = (await session.execute(
                select(Benchmark).where(Benchmark.id == "team-evals"),
            )).scalar_one()
            assert bench.display_name == "Team evaluations"
            assert bench.series == "internal"
            assert bench.license_spdx == "MIT"
            assert bench.upstream_kind == S3_FOLDER_KIND
            assert bench.upstream_locator == "s3://loom-benchmarks/team-evals/"
            assert bench.imported_by == PUBLISH_IMPORTED_BY

            task = (await session.execute(
                select(TaskRow).where(TaskRow.id == "team-evals/alpha"),
            )).scalar_one()
            assert task.source == "s3://loom-benchmarks/team-evals/alpha/"
            assert task.license == "MIT"
            assert task.benchmark_id == "team-evals"
            assert task.config["task"]["id"] == "alpha"
            assert len(task.checksum) == 64
    finally:
        async with factory() as session:
            await session.execute(
                delete(TaskRow).where(TaskRow.benchmark_id == "team-evals"),
            )
            await session.execute(
                delete(Benchmark).where(Benchmark.id == "team-evals"),
            )
            await session.commit()
        await engine.dispose()
