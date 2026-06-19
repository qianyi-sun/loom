"""Publish validated local benchmark folders to object storage.

This is the production-oriented companion to ``validate-local`` and the
dev-only ``sync-config`` path: validate the same folder contract, upload each
task bundle under an ``s3://`` prefix the worker already knows how to
materialize, and upsert benchmark/task rows into the service database.
"""

from __future__ import annotations

import tomllib
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from loom.db.schema import Benchmark
from loom.db.schema import Task as TaskRow
from loom.models.task_checksum import task_checksum
from loom.trajectory.storage import ObjectStore
from loom_benchmark_tool.db_url import normalize_db_url
from loom_benchmark_tool.upload import upload_task_dir
from loom_cli.local_benchmark_validate import validate_local_benchmark

PUBLISH_IMPORTED_BY = "local-benchmark-publish"
S3_FOLDER_KIND = "s3-folder"


@dataclass(frozen=True)
class LocalBenchmarkPublishStats:
    benchmark_id: str
    task_count: int
    inserted: int
    updated: int
    unchanged: int
    uploaded_objects: int
    bucket: str
    source_prefix: str


async def publish_local_benchmark(
    root: Path,
    *,
    db_url: str,
    object_store: ObjectStore,
    bucket: str,
    benchmark_id: str | None = None,
    display_name: str | None = None,
    series: str | None = None,
    license_spdx: str | None = None,
    source_subdir: str | None = None,
    imported_by: str | None = None,
) -> LocalBenchmarkPublishStats:
    """Validate, upload, and register a user-owned local benchmark folder."""

    result = validate_local_benchmark(
        root,
        benchmark_id=benchmark_id,
        display_name=display_name,
        series=series,
        license_spdx=license_spdx,
        source_subdir=source_subdir,
    )
    entry = result.entry
    await object_store.ensure_bucket(bucket)

    engine = create_async_engine(normalize_db_url(db_url))
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    inserted = updated = unchanged = uploaded_objects = 0
    source_prefix = f"s3://{bucket}/{entry.id}/"
    try:
        async with session_factory() as session:
            await session.execute(
                pg_insert(Benchmark).values(
                    id=entry.id,
                    display_name=entry.display_name,
                    upstream_kind=S3_FOLDER_KIND,
                    upstream_locator=source_prefix,
                    upstream_revision="",
                    license_spdx=entry.license_spdx,
                    license_url="",
                    series=entry.series,
                    splits=[],
                    imported_by=imported_by or PUBLISH_IMPORTED_BY,
                ).on_conflict_do_update(
                    index_elements=["id"],
                    set_={
                        "display_name": entry.display_name,
                        "upstream_kind": S3_FOLDER_KIND,
                        "upstream_locator": source_prefix,
                        "upstream_revision": "",
                        "license_spdx": entry.license_spdx,
                        "license_url": "",
                        "series": entry.series,
                        "splits": [],
                        "imported_by": imported_by or PUBLISH_IMPORTED_BY,
                    },
                ),
            )

            for task_toml in result.task_tomls:
                bundle_dir = task_toml.parent
                rel = bundle_dir.relative_to(result.task_root)
                task_id = entry.id if rel == Path(".") else f"{entry.id}/{rel.as_posix()}"
                prefix = _task_prefix(entry.id, rel)
                source = f"s3://{bucket}/{prefix}"
                uploaded_objects += await upload_task_dir(
                    store=object_store,
                    bucket=bucket,
                    prefix=prefix,
                    task_dir=bundle_dir,
                )

                with task_toml.open("rb") as f:
                    raw_cfg: dict[str, Any] = tomllib.load(f)
                checksum = task_checksum(bundle_dir)
                existing = await _get_task(session, task_id)
                if existing is None:
                    inserted += 1
                elif (
                    existing.checksum != checksum
                    or existing.source != source
                    or existing.benchmark_id != entry.id
                    or existing.license != entry.license_spdx
                ):
                    updated += 1
                else:
                    unchanged += 1

                await session.execute(
                    pg_insert(TaskRow).values(
                        id=task_id,
                        checksum=checksum,
                        config=raw_cfg,
                        source=source,
                        license=entry.license_spdx,
                        benchmark_id=entry.id,
                    ).on_conflict_do_update(
                        index_elements=["id"],
                        set_={
                            "checksum": checksum,
                            "config": raw_cfg,
                            "source": source,
                            "license": entry.license_spdx,
                            "benchmark_id": entry.id,
                        },
                    ),
                )

            await session.commit()
    finally:
        await engine.dispose()

    return LocalBenchmarkPublishStats(
        benchmark_id=entry.id,
        task_count=result.task_count,
        inserted=inserted,
        updated=updated,
        unchanged=unchanged,
        uploaded_objects=uploaded_objects,
        bucket=bucket,
        source_prefix=source_prefix,
    )


def _task_prefix(benchmark_id: str, rel: Path) -> str:
    if rel == Path("."):
        return f"{benchmark_id}/"
    return f"{benchmark_id}/{rel.as_posix().strip('/')}/"


async def _get_task(session, task_id: str):  # type: ignore[no-untyped-def]
    return (await session.execute(
        select(TaskRow).where(TaskRow.id == task_id),
    )).scalar_one_or_none()
