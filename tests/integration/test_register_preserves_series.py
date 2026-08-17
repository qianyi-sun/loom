"""register_cmd's UPSERT preserves an existing `series` when the
manifest doesn't carry one.

Concrete scenario that motivated this: a v1 manifest (published before
PR-1) has no `series` key. The stub seed in `seed_test_data.py` first
backfills the right series straight off the adapter class
(`swe-bench`); then `_auto_register_benchmarks` calls `run_register`
which re-upserts from the manifest. If the UPSERT writes `series=None`
unconditionally it clobbers the just-set value and the SPA's grouped
Benchmarks page shows swe-bench under "Other"."""

from __future__ import annotations

import json
from collections.abc import AsyncIterator
from pathlib import Path
from unittest.mock import patch

import pytest
from sqlalchemy import create_engine, delete, insert, select
from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.orm import sessionmaker

from loom.db.schema import Benchmark, TaskImageMaterialization
from loom.db.schema import Task as TaskRow
from loom.models.task import TaskConfig
from loom_benchmark_tool.register_cmd import run_register


@pytest.fixture
async def db(postgres_url: str) -> AsyncIterator[AsyncSession]:
    sync_engine = create_engine(postgres_url)
    sl = sessionmaker(sync_engine)
    with sl() as s:
        s.execute(
            delete(TaskImageMaterialization).where(
                TaskImageMaterialization.task_id.like("fake-bench/%")
            )
        )
        s.execute(delete(TaskRow).where(TaskRow.benchmark_id == "fake-bench"))
        s.execute(delete(Benchmark).where(Benchmark.id == "fake-bench"))
        s.commit()
    sync_engine.dispose()

    engine = create_async_engine(postgres_url)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with factory() as s2:
            yield s2
    finally:
        await engine.dispose()
        sync_engine = create_engine(postgres_url)
        with sessionmaker(sync_engine)() as s:
            s.execute(
                delete(TaskImageMaterialization).where(
                    TaskImageMaterialization.task_id.like("fake-bench/%")
                )
            )
            s.execute(delete(TaskRow).where(TaskRow.benchmark_id == "fake-bench"))
            s.execute(delete(Benchmark).where(Benchmark.id == "fake-bench"))
            s.commit()
        sync_engine.dispose()


def _v1_manifest(tmp_path: Path) -> Path:
    """Pre-PR-1 manifest shape — no `series` field at top level, no
    `tags` on tasks."""
    p = tmp_path / "manifest.json"
    p.write_text(
        json.dumps(
            {
                "manifest_version": 1,
                "benchmark_id": "fake-bench",
                "display_name": "Fake (v1 manifest)",
                "upstream_kind": "huggingface",
                "upstream_locator": "fake-org/fake-bench",
                "upstream_revision": "main",
                "license_spdx": "MIT",
                "license_url": "",
                "splits": ["test"],
                "tasks": [],
            }
        )
    )
    return p


async def test_register_preserves_existing_series_on_v1_manifest(
    db: AsyncSession,
    tmp_path: Path,
    postgres_url: str,
) -> None:
    # Stub seed already populated the row with series='fake-series'.
    await db.execute(
        insert(Benchmark).values(
            id="fake-bench",
            display_name="Fake",
            upstream_kind="huggingface",
            upstream_locator="fake-org/fake-bench",
            upstream_revision="main",
            license_spdx="MIT",
            license_url="",
            splits=["test"],
            series="fake-series",
        )
    )
    await db.commit()

    # Now run register_cmd against a v1 manifest (no series).
    manifest_path = _v1_manifest(tmp_path)
    with patch(
        "loom_benchmark_tool.register_cmd.read_manifest_from_hf",
    ) as rmf:
        rmf.return_value = json.loads(manifest_path.read_text())
        await run_register(
            benchmark="fake-bench",
            hf_org="fake-org",
            hf_token=None,
            db_url=postgres_url,
            registered_by="test",
        )

    # Re-fetch row; series must NOT have been clobbered by the upsert.
    series = (
        await db.execute(
            select(Benchmark.series).where(Benchmark.id == "fake-bench"),
        )
    ).scalar_one()
    assert series == "fake-series"


async def test_register_writes_series_from_v2_manifest(
    db: AsyncSession,
    tmp_path: Path,
    postgres_url: str,
) -> None:
    """The complementary case: v2 manifests do carry `series`; the
    upsert must apply it (overwriting whatever was there before)."""
    await db.execute(
        insert(Benchmark).values(
            id="fake-bench",
            display_name="Fake",
            upstream_kind="huggingface",
            upstream_locator="fake-org/fake-bench",
            upstream_revision="main",
            license_spdx="MIT",
            license_url="",
            splits=["test"],
            series=None,
        )
    )
    await db.commit()
    manifest = {
        "manifest_version": 2,
        "benchmark_id": "fake-bench",
        "display_name": "Fake (v2 manifest)",
        "upstream_kind": "huggingface",
        "upstream_locator": "fake-org/fake-bench",
        "upstream_revision": "main",
        "license_spdx": "MIT",
        "license_url": "",
        "splits": ["test"],
        "series": "from-manifest",
        "tasks": [],
    }
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text(json.dumps(manifest))
    with patch(
        "loom_benchmark_tool.register_cmd.read_manifest_from_hf",
    ) as rmf:
        rmf.return_value = manifest
        await run_register(
            benchmark="fake-bench",
            hf_org="fake-org",
            hf_token=None,
            db_url=postgres_url,
            registered_by="test",
        )
    series = (
        await db.execute(
            select(Benchmark.series).where(Benchmark.id == "fake-bench"),
        )
    ).scalar_one()
    assert series == "from-manifest"


async def test_register_keeps_legacy_task_as_placeholder(
    db: AsyncSession,
    postgres_url: str,
) -> None:
    task_id = "fake-bench/task-001"
    manifest = {
        "manifest_version": 2,
        "benchmark_id": "fake-bench",
        "display_name": "Fake (legacy manifest)",
        "upstream_kind": "huggingface",
        "upstream_locator": "fake-org/fake-bench",
        "upstream_revision": "main",
        "license_spdx": "MIT",
        "license_url": "",
        "splits": ["test"],
        "series": "fake-series",
        "tasks": [
            {
                "task_id": task_id,
                "instance_id": "task-001",
                "hf_path": "task-001/",
                "checksum": "sha256:" + "1" * 64,
                "license_spdx": "MIT",
                "split": "test",
                "tags": {"difficulty": "legacy"},
            }
        ],
    }
    with patch(
        "loom_benchmark_tool.register_cmd.read_manifest_from_hf",
    ) as rmf:
        rmf.return_value = manifest
        result = await run_register(
            benchmark="fake-bench",
            hf_org="fake-org",
            hf_token=None,
            db_url=postgres_url,
            registered_by="test",
        )

    row = (
        await db.execute(
            select(TaskRow).where(TaskRow.id == task_id),
        )
    ).scalar_one()
    assert row.config == {}
    assert row.tags == {"difficulty": "legacy"}
    assert result["registered"] == 1
    assert result["legacy_placeholders"] == 1


def _valid_task_config(task_id: str) -> dict[str, object]:
    return {
        "schema_version": "1",
        "task": {"id": task_id, "name": "Fake task"},
        "environment": {"os": "linux", "docker_image": "python:3.12-slim"},
        "agent": {"name": "oracle"},
        "verifier": {"name": "pytest"},
        "steps": [{"name": "main"}],
    }


async def test_register_writes_valid_task_config_from_vnext_manifest(
    db: AsyncSession,
    postgres_url: str,
) -> None:
    task_id = "fake-bench/task-001"
    task_config = _valid_task_config(task_id)
    manifest = {
        "schema_version": 3,
        "benchmark_id": "fake-bench",
        "display_name": "Fake (vNext manifest)",
        "upstream_kind": "huggingface",
        "upstream_locator": "fake-org/fake-bench",
        "upstream_revision": "main",
        "license_spdx": "MIT",
        "license_url": "",
        "splits": ["test"],
        "series": "fake-series",
        "tasks": [
            {
                "task_id": task_id,
                "instance_id": "task-001",
                "hf_path": "task-001/",
                "checksum": "sha256:" + "1" * 64,
                "license_spdx": "MIT",
                "split": "test",
                "tags": {"difficulty": "smoke"},
                "task_config": task_config,
            }
        ],
    }
    with patch(
        "loom_benchmark_tool.register_cmd.read_manifest_from_hf",
    ) as rmf:
        rmf.return_value = manifest
        await run_register(
            benchmark="fake-bench",
            hf_org="fake-org",
            hf_token=None,
            db_url=postgres_url,
            registered_by="test",
        )

    row = (
        await db.execute(
            select(TaskRow).where(TaskRow.id == task_id),
        )
    ).scalar_one()
    assert row.config == task_config
    assert row.source == ("hf://fake-org/loom-benchmark-fake-bench@main/task-001/")
    assert row.tags == {"difficulty": "smoke"}
    TaskConfig.model_validate(row.config)


async def test_register_enqueues_dockerfile_task_for_each_native_architecture(
    db: AsyncSession,
    postgres_url: str,
) -> None:
    task_id = "fake-bench/dockerfile-task"
    task_config = _valid_task_config(task_id)
    task_config["environment"] = {
        "os": "linux",
        "cpu_arch": "any",
        "dockerfile": "environment/Dockerfile",
    }
    manifest = {
        "schema_version": 3,
        "benchmark_id": "fake-bench",
        "display_name": "Fake materialized benchmark",
        "upstream_kind": "huggingface",
        "upstream_locator": "fake-org/fake-bench",
        "upstream_revision": "main",
        "license_spdx": "MIT",
        "license_url": "",
        "splits": ["test"],
        "tasks": [
            {
                "task_id": task_id,
                "instance_id": "dockerfile-task",
                "hf_path": "dockerfile-task/",
                "checksum": "sha256:" + "4" * 64,
                "license_spdx": "MIT",
                "split": "test",
                "tags": {},
                "task_config": task_config,
            }
        ],
    }
    with patch("loom_benchmark_tool.register_cmd.read_manifest_from_hf") as read_manifest:
        read_manifest.return_value = manifest
        await run_register(
            benchmark="fake-bench",
            hf_org="fake-org",
            hf_token=None,
            db_url=postgres_url,
            registered_by="test",
        )

    rows = (
        (
            await db.execute(
                select(TaskImageMaterialization)
                .where(TaskImageMaterialization.task_id == task_id)
                .order_by(TaskImageMaterialization.cpu_arch.desc())
            )
        )
        .scalars()
        .all()
    )
    assert [row.cpu_arch for row in rows] == ["x86_64", "arm64"]
    assert all(row.state == "queued" for row in rows)
