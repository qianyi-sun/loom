"""Integration coverage for user-owned local benchmark publishing (#275)."""

from __future__ import annotations

from pathlib import Path

import pytest
from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from loom.db.schema import Benchmark
from loom.db.schema import Task as TaskRow
from loom.models.task_checksum import task_checksum
from loom.trajectory.storage import (
    BUNDLE_FILE_METADATA_NAME,
    FakeObjectStore,
    bundle_file_metadata_sha256,
)
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
        "schema_version = 1\n"
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
        "schema_version = 1\n"
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
        "[task]\n"
        'id = "app-path-missing"\n'
        'name = "App path missing"\n'
        "[environment]\n"
        'os = "linux"\n'
        'dockerfile = "environment/Dockerfile"\n'
        "[agent]\n"
        'name = "oracle"\n'
        "[verifier]\n"
        'name = "pytest"\n'
        "[[steps]]\n"
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
    postgres_url: str,
    tmp_path: Path,
) -> None:
    root = tmp_path / "team-evals"
    _write_layout(root)
    store = FakeObjectStore()
    task_dir = root / "tasks" / "alpha"
    expected_checksum = task_checksum(task_dir)
    metadata_digest = bundle_file_metadata_sha256(task_dir).removeprefix("sha256:")
    revision_prefix = f"team-evals/alpha/.loom-revisions/{expected_checksum}/{metadata_digest}/"

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
    assert ("loom-benchmarks", f"{revision_prefix}task.toml") in store.objects
    assert ("loom-benchmarks", f"{revision_prefix}instruction.md") in store.objects

    engine = create_async_engine(postgres_url)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with factory() as session:
            bench = (
                await session.execute(
                    select(Benchmark).where(Benchmark.id == "team-evals"),
                )
            ).scalar_one()
            assert bench.display_name == "Team evaluations"
            assert bench.series == "internal"
            assert bench.license_spdx == "MIT"
            assert bench.upstream_kind == S3_FOLDER_KIND
            assert bench.upstream_locator == "s3://loom-benchmarks/team-evals/"
            assert bench.imported_by == PUBLISH_IMPORTED_BY

            task = (
                await session.execute(
                    select(TaskRow).where(TaskRow.id == "team-evals/alpha"),
                )
            ).scalar_one()
            assert task.source == f"s3://loom-benchmarks/{revision_prefix}"
            assert task.license == "MIT"
            assert task.benchmark_id == "team-evals"
            assert task.config["task"]["id"] == "alpha"
            assert task.checksum == expected_checksum
            assert task.source_provenance == {
                "bundle_file_metadata_sha256": f"sha256:{metadata_digest}",
            }
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
async def test_failed_database_commit_does_not_overwrite_live_task_bundle(
    postgres_url: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "team-evals"
    _write_layout(root)
    store = FakeObjectStore()

    await publish_local_benchmark(
        root,
        db_url=postgres_url,
        object_store=store,
        bucket="loom-benchmarks",
    )

    engine = create_async_engine(postgres_url)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with factory() as session:
            before = (
                await session.execute(
                    select(TaskRow).where(TaskRow.id == "team-evals/alpha"),
                )
            ).scalar_one()
            before_checksum = before.checksum
            before_source = before.source
        assert before_source is not None
        before_prefix = before_source.removeprefix("s3://loom-benchmarks/")
        before_key = f"{before_prefix}instruction.md"
        assert store.objects[("loom-benchmarks", before_key)] == b"do alpha\n"

        task_dir = root / "tasks" / "alpha"
        (task_dir / "instruction.md").write_text("do revised alpha\n")
        revised_checksum = task_checksum(task_dir)
        revised_metadata_digest = bundle_file_metadata_sha256(task_dir).removeprefix(
            "sha256:",
        )

        async def reject_commit(_session: AsyncSession) -> None:
            raise RuntimeError("injected database commit failure")

        with monkeypatch.context() as scoped:
            scoped.setattr(AsyncSession, "commit", reject_commit)
            with pytest.raises(RuntimeError, match="injected database commit failure"):
                await publish_local_benchmark(
                    root,
                    db_url=postgres_url,
                    object_store=store,
                    bucket="loom-benchmarks",
                )

        async with factory() as session:
            after = (
                await session.execute(
                    select(TaskRow).where(TaskRow.id == "team-evals/alpha"),
                )
            ).scalar_one()
            assert after.checksum == before_checksum
            assert after.source == before_source

        assert store.objects[("loom-benchmarks", before_key)] == b"do alpha\n"
        revised_key = (
            "team-evals/alpha/.loom-revisions/"
            f"{revised_checksum}/{revised_metadata_digest}/instruction.md"
        )
        assert store.objects[("loom-benchmarks", revised_key)] == b"do revised alpha\n"
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
async def test_mode_only_revision_does_not_overwrite_live_transport_metadata(
    postgres_url: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "team-evals"
    _write_layout(root)
    store = FakeObjectStore()

    await publish_local_benchmark(
        root,
        db_url=postgres_url,
        object_store=store,
        bucket="loom-benchmarks",
    )

    engine = create_async_engine(postgres_url)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with factory() as session:
            before = (
                await session.execute(
                    select(TaskRow).where(TaskRow.id == "team-evals/alpha"),
                )
            ).scalar_one()
            before_checksum = before.checksum
            before_source = before.source
        assert before_source is not None
        before_prefix = before_source.removeprefix("s3://loom-benchmarks/")
        before_metadata_key = f"{before_prefix}{BUNDLE_FILE_METADATA_NAME}"
        before_metadata = store.objects[("loom-benchmarks", before_metadata_key)]

        task_dir = root / "tasks" / "alpha"
        instruction = task_dir / "instruction.md"
        instruction.chmod(0o755)
        assert task_checksum(task_dir) == before_checksum
        revised_metadata_digest = bundle_file_metadata_sha256(task_dir).removeprefix(
            "sha256:",
        )

        async def reject_commit(_session: AsyncSession) -> None:
            raise RuntimeError("injected database commit failure")

        with monkeypatch.context() as scoped:
            scoped.setattr(AsyncSession, "commit", reject_commit)
            with pytest.raises(RuntimeError, match="injected database commit failure"):
                await publish_local_benchmark(
                    root,
                    db_url=postgres_url,
                    object_store=store,
                    bucket="loom-benchmarks",
                )

        async with factory() as session:
            after = (
                await session.execute(
                    select(TaskRow).where(TaskRow.id == "team-evals/alpha"),
                )
            ).scalar_one()
            assert after.checksum == before_checksum
            assert after.source == before_source

        assert store.objects[("loom-benchmarks", before_metadata_key)] == before_metadata
        revised_metadata_key = (
            "team-evals/alpha/.loom-revisions/"
            f"{before_checksum}/{revised_metadata_digest}/"
            f"{BUNDLE_FILE_METADATA_NAME}"
        )
        assert store.objects[("loom-benchmarks", revised_metadata_key)] != before_metadata
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
    postgres_url: str,
    tmp_path: Path,
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
            bench = (
                await session.execute(
                    select(Benchmark).where(Benchmark.id == "source-useful-compat"),
                )
            ).scalar_one_or_none()
            tasks = (
                (
                    await session.execute(
                        select(TaskRow).where(
                            TaskRow.benchmark_id == "source-useful-compat",
                        ),
                    )
                )
                .scalars()
                .all()
            )
            assert bench is None
            assert tasks == []
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_publish_local_explicit_flatten_override_records_evidence(
    postgres_url: str,
    tmp_path: Path,
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
    engine = create_async_engine(postgres_url)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with factory() as session:
            task = (
                await session.execute(
                    select(TaskRow).where(
                        TaskRow.id == "source-useful-compat/app-path-missing",
                    ),
                )
            ).scalar_one()
            source_prefix = task.source.removeprefix("s3://loom-benchmarks/")
            revision_prefix = (
                f"source-useful-compat/app-path-missing/.loom-revisions/{task.checksum}/"
            )
            assert source_prefix.startswith(revision_prefix)
            metadata_digest = source_prefix.removeprefix(revision_prefix).removesuffix(
                "/",
            )
            assert len(metadata_digest) == 64
            assert set(metadata_digest) <= set("0123456789abcdef")
            assert (
                "loom-benchmarks",
                f"{source_prefix}setup_repo.sh",
            ) in store.objects
            assert (
                "loom-benchmarks",
                f"{source_prefix}environment/setup_repo.sh",
            ) in store.objects
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
