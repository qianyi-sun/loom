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

from loom.db.schema import Benchmark
from loom_benchmark_tool.register_cmd import run_register


@pytest.fixture
async def db(postgres_url: str) -> AsyncIterator[AsyncSession]:
    sync_engine = create_engine(postgres_url)
    sl = sessionmaker(sync_engine)
    with sl() as s:
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
            s.execute(delete(Benchmark).where(Benchmark.id == "fake-bench"))
            s.commit()
        sync_engine.dispose()


def _v1_manifest(tmp_path: Path) -> Path:
    """Pre-PR-1 manifest shape — no `series` field at top level, no
    `tags` on tasks."""
    p = tmp_path / "manifest.json"
    p.write_text(json.dumps({
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
    }))
    return p


async def test_register_preserves_existing_series_on_v1_manifest(
    db: AsyncSession, tmp_path: Path, postgres_url: str,
) -> None:
    # Stub seed already populated the row with series='fake-series'.
    await db.execute(insert(Benchmark).values(
        id="fake-bench",
        display_name="Fake",
        upstream_kind="huggingface",
        upstream_locator="fake-org/fake-bench",
        upstream_revision="main",
        license_spdx="MIT",
        license_url="",
        splits=["test"],
        series="fake-series",
    ))
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
    series = (await db.execute(
        select(Benchmark.series).where(Benchmark.id == "fake-bench"),
    )).scalar_one()
    assert series == "fake-series"


async def test_register_writes_series_from_v2_manifest(
    db: AsyncSession, tmp_path: Path, postgres_url: str,
) -> None:
    """The complementary case: v2 manifests do carry `series`; the
    upsert must apply it (overwriting whatever was there before)."""
    await db.execute(insert(Benchmark).values(
        id="fake-bench",
        display_name="Fake",
        upstream_kind="huggingface",
        upstream_locator="fake-org/fake-bench",
        upstream_revision="main",
        license_spdx="MIT",
        license_url="",
        splits=["test"],
        series=None,
    ))
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
    series = (await db.execute(
        select(Benchmark.series).where(Benchmark.id == "fake-bench"),
    )).scalar_one()
    assert series == "from-manifest"
