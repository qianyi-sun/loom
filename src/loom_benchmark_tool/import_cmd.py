"""`python -m loom_benchmark_tool import <benchmark>` — end-to-end ingestion.

Steps per invocation:

1. Resolve the adapter from REGISTRY.
2. `fetch_upstream` populates a cached source dir.
3. Upsert the `benchmarks` row (id = adapter.name).
4. For each `BenchmarkInstance`:
   - convert into a fresh tempdir (`out_dir = tempdir / instance_slug`)
   - upload the tempdir contents to MinIO under
     `s3://{bucket}/{benchmark}/{instance_id}/`
   - upsert the corresponding `tasks` row with the converted
     `task_id`, content checksum, parsed `config`, `license`, and
     `benchmark_id`.

`tasks.source` is set to the s3:// prefix so the worker's Plan 13
`_materialize_task_dir` pulls the bundle on claim.
"""

from __future__ import annotations

import re
import tempfile
import tomllib
from pathlib import Path
from typing import Any

from loom_benchmarks.fetch import fetch_upstream
from loom_benchmarks.registry import REGISTRY
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from loom.db.schema import Benchmark
from loom.db.schema import Task as TaskRow
from loom.trajectory.storage import ObjectStore
from loom_benchmark_tool.upload import upload_task_dir

# Permissive but bounded: forward-slashes are fine (HumanEval IDs are
# `HumanEval/0`); reject anything that could traverse out of the
# benchmark namespace or smuggle in a control char / NUL / shell-special
# byte that would break the S3 prefix or TOML interpolation downstream.
_INSTANCE_ID_RE = re.compile(r"^[A-Za-z0-9._/+\-]+$")


def _validate_instance_id(instance_id: str) -> None:
    if not _INSTANCE_ID_RE.match(instance_id):
        raise ValueError(
            f"instance_id {instance_id!r} contains characters outside "
            f"[A-Za-z0-9._/+\\-]; reject to keep S3 prefix + task_id safe",
        )
    parts = instance_id.split("/")
    if "" in parts or ".." in parts or "." in parts:
        raise ValueError(
            f"instance_id {instance_id!r} contains empty / .. / . segments; reject",
        )


def _normalize_db_url(url: str) -> str:
    """Ensure the URL is the async psycopg variant SQLAlchemy expects."""
    if url.startswith("postgresql+"):
        return url
    return url.replace("postgresql://", "postgresql+psycopg://", 1)


async def run_import(
    *,
    benchmark: str,
    db_url: str,
    object_store: ObjectStore,
    bucket: str,
    cache_dir: Path,
    limit: int | None = None,
    imported_by: str | None = None,
    refresh: bool = False,
) -> dict[str, int]:
    adapter = REGISTRY[benchmark]
    cache_dir.mkdir(parents=True, exist_ok=True)
    source_dir = fetch_upstream(
        adapter.upstream_source, cache_root=cache_dir, refresh=refresh,
    )

    engine = create_async_engine(_normalize_db_url(db_url))
    session_factory = async_sessionmaker(engine, expire_on_commit=False)

    # Upsert benchmarks row first (A13.2: human-readable PK supports
    # ON CONFLICT (id) DO UPDATE).
    async with session_factory() as session:
        await session.execute(
            pg_insert(Benchmark).values(
                id=adapter.name,
                display_name=adapter.display_name,
                upstream_kind=adapter.upstream_source.kind,
                upstream_locator=adapter.upstream_source.locator,
                upstream_revision=adapter.upstream_source.revision or "",
                license_spdx=adapter.license_spdx,
                license_url=adapter.license_url,
                splits=list(adapter.splits),
                imported_by=imported_by,
            ).on_conflict_do_update(
                index_elements=["id"],
                set_={
                    "upstream_revision": adapter.upstream_source.revision or "",
                    "imported_by": imported_by,
                },
            ),
        )
        await session.commit()

    stats = {"converted": 0, "warnings": 0}

    for split in adapter.splits:
        count = 0
        for inst in adapter.list_instances(source_dir=source_dir, split=split):
            if limit is not None and count >= limit:
                break
            count += 1
            _validate_instance_id(inst.instance_id)
            with tempfile.TemporaryDirectory() as tmp:
                out_dir = Path(tmp) / inst.instance_id.replace("/", "__")
                out_dir.mkdir(parents=True, exist_ok=True)
                converted = adapter.convert_instance(inst, out_dir=out_dir)
                prefix = f"{adapter.name}/{inst.instance_id}/"
                await upload_task_dir(
                    store=object_store, bucket=bucket,
                    prefix=prefix, task_dir=out_dir,
                )

                cfg: dict[str, Any] = tomllib.loads(
                    (out_dir / "task.toml").read_text(),
                )
                source_uri = f"s3://{bucket}/{prefix}"
                async with session_factory() as session:
                    await session.execute(
                        pg_insert(TaskRow).values(
                            id=converted.task_id,
                            checksum=converted.checksum,
                            config=cfg,
                            source=source_uri,
                            license=converted.license_spdx,
                            benchmark_id=adapter.name,
                        ).on_conflict_do_update(
                            index_elements=["id"],
                            set_={
                                "checksum": converted.checksum,
                                "config": cfg,
                                "source": source_uri,
                                "license": converted.license_spdx,
                                "benchmark_id": adapter.name,
                            },
                        ),
                    )
                    await session.commit()
                stats["converted"] += 1
                stats["warnings"] += len(converted.warnings)

    await engine.dispose()
    return stats
