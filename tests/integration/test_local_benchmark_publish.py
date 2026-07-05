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
from loom_cli.local_benchmark_validate import LocalBenchmarkValidationError

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


def _write_environment_path_mismatch_layout(root: Path) -> None:
    root.mkdir(parents=True)
    (root / "benchmark.toml").write_text(
        'schema_version = 1\n'
        'id = "source-useful-compat"\n'
        'display_name = "Source Useful compat fixture"\n'
        'series = "internal"\n'
        'license_spdx = "MIT"\n',
    )
    task = root / "tasks" / "app-path-missing"
    environment = task / "environment"
    environment.mkdir(parents=True)
    (task / "task.toml").write_text(
        'schema_version = "1"\n'
        '[task]\n'
        'id = "app-path-missing"\n'
        'name = "App path missing"\n'
        '[environment]\n'
        'os = "linux"\n'
        'dockerfile = "environment/Dockerfile"\n'
        '[agent]\n'
        'name = "oracle"\n'
        '[verifier]\n'
        'name = "pytest"\n'
        '[[steps]]\n'
        'name = "main"\n',
    )
    (task / "instruction.md").write_text("do task\n")
    (environment / "setup_repo.sh").write_text("#!/bin/sh\necho setup\n")
    (environment / "Dockerfile").write_text(
        "FROM debian:bookworm\n"
        "COPY . /app/\n"
        "RUN chmod +x /app/setup_repo.sh && /app/setup_repo.sh\n",
    )


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


@pytest.mark.asyncio
async def test_publish_local_benchmark_rejects_environment_path_mismatch_by_default(
    postgres_url: str, tmp_path: Path,
) -> None:
    root = tmp_path / "source-useful-compat"
    _write_environment_path_mismatch_layout(root)
    store = FakeObjectStore()

    with pytest.raises(LocalBenchmarkValidationError) as excinfo:
        await publish_local_benchmark(
            root,
            db_url=postgres_url,
            object_store=store,
            bucket="loom-benchmarks",
        )

    message = str(excinfo.value)
    assert "TASK_COMPAT_APP_PATH_MISSING" in message
    assert "environment/setup_repo.sh" in message
    assert store.objects == {}

    engine = create_async_engine(postgres_url)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with factory() as session:
            bench = (await session.execute(
                select(Benchmark).where(Benchmark.id == "source-useful-compat"),
            )).scalar_one_or_none()
            tasks = (await session.execute(
                select(TaskRow).where(
                    TaskRow.benchmark_id == "source-useful-compat",
                ),
            )).scalars().all()
            assert bench is None
            assert tasks == []
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_publish_local_explicit_flatten_override_records_evidence(
    postgres_url: str, tmp_path: Path,
) -> None:
    root = tmp_path / "source-useful-compat"
    _write_environment_path_mismatch_layout(root)
    store = FakeObjectStore()

    stats = await publish_local_benchmark(
        root,
        db_url=postgres_url,
        object_store=store,
        bucket="loom-benchmarks",
        compat_flatten_environment=True,
    )

    assert stats.benchmark_id == "source-useful-compat"
    assert stats.task_count == 1
    assert stats.compat_flattened_files == 2
    assert (
        "loom-benchmarks",
        "source-useful-compat/app-path-missing/setup_repo.sh",
    ) in store.objects
    assert (
        "loom-benchmarks",
        "source-useful-compat/app-path-missing/environment/setup_repo.sh",
    ) in store.objects

    engine = create_async_engine(postgres_url)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with factory() as session:
            task = (await session.execute(
                select(TaskRow).where(
                    TaskRow.id == "source-useful-compat/app-path-missing",
                ),
            )).scalar_one()
            assert task.source == (
                "s3://loom-benchmarks/source-useful-compat/app-path-missing/"
            )
    finally:
        async with factory() as session:
            await session.execute(
                delete(TaskRow).where(
                    TaskRow.benchmark_id == "source-useful-compat",
                ),
            )
            await session.execute(
                delete(Benchmark).where(Benchmark.id == "source-useful-compat"),
            )
            await session.commit()
        await engine.dispose()
