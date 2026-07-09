"""Publish validated local benchmark folders to object storage.

This is the production-oriented companion to ``validate-local`` and the
dev-only ``sync-config`` path: validate the same folder contract, upload each
task bundle under an ``s3://`` prefix the worker already knows how to
materialize, and upsert benchmark/task rows into the service database.
"""

from __future__ import annotations

import shutil
import tempfile
import tomllib
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from loom.db.schema import Benchmark
from loom.db.schema import Task as TaskRow
from loom.driver.task_image import (
    dockerfile_uses_runtime_arm64_fallback_base,
)
from loom.models.task_checksum import task_checksum
from loom.task_bundle_compat import (
    CompatibilitySeverity,
    collect_task_dir_compatibility_issues,
    format_compatibility_issues,
)
from loom.terminal_bench_normalize import (
    normalize_terminal_bench_task_toml,
)
from loom.trajectory.storage import ObjectStore
from loom_benchmark_tool.db_url import normalize_db_url
from loom_benchmark_tool.upload import upload_task_dir
from loom_cli.local_benchmark_validate import (
    LocalBenchmarkValidationError,
    validate_local_benchmark,
)

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
    compat_flattened_files: int
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
    compat_flatten_environment: bool = False,
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
    compat_flattened_files = 0
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

                # #369: bundles whose files live under `environment/` but
                # whose Dockerfile does `COPY . /app/` and references
                # `/app/<file>` need the tree flattened so the files land
                # at `/app/<file>` and not `/app/environment/<file>`. We
                # stage each bundle into a scratch dir, flatten there,
                # and upload from the scratch dir. The operator's
                # on-disk bundle stays untouched.
                with tempfile.TemporaryDirectory(
                    prefix="loom-publish-stage-",
                ) as stage_root:
                    staged = Path(stage_root) / "bundle"
                    shutil.copytree(bundle_dir, staged, symlinks=False)
                    if compat_flatten_environment:
                        compat_flattened_files += len(
                            _flatten_environment_subdir(staged),
                        )

                    compatibility_issues = [
                        issue
                        for issue in collect_task_dir_compatibility_issues(staged)
                        if issue.severity == CompatibilitySeverity.ERROR
                    ]
                    if compatibility_issues:
                        raise LocalBenchmarkValidationError(
                            "task bundle compatibility preflight failed for "
                            f"{task_id}:\n"
                            + format_compatibility_issues(compatibility_issues),
                        )

                    uploaded_objects += await upload_task_dir(
                        store=object_store,
                        bucket=bucket,
                        prefix=prefix,
                        task_dir=staged,
                    )
                    with (staged / task_toml.name).open("rb") as f:
                        raw_cfg: dict[str, Any] = tomllib.load(f)
                    # #341: normalize Terminal-Bench-shaped task.toml
                    # to a Loom TaskConfig dict before persisting. The
                    # S3-uploaded bundle preserves the operator's
                    # original files (plus the flattened compat copies
                    # from #369); the DB row's `config` JSONB carries
                    # the Loom-schema form so the worker validates.
                    raw_cfg = normalize_terminal_bench_task_toml(raw_cfg)
                    _promote_cpu_arch_if_runtime_fallback(raw_cfg, staged)
                    checksum = task_checksum(staged)
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
        compat_flattened_files=compat_flattened_files,
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


def _flatten_environment_subdir(bundle_dir: Path) -> list[str]:
    """Copy every file under ``bundle_dir/environment/`` up to
    ``bundle_dir/`` in place so a Dockerfile that does ``COPY . /app/``
    and expects ``/app/<file>`` finds the file even when the bundle
    stores it as ``environment/<file>``.

    Existing top-level files are preserved (name collisions skip the
    copy — the operator's intent at the root wins). The
    ``environment/`` tree itself is preserved after flattening so
    Dockerfiles that DO expect ``environment/<file>`` at the build
    context root still find it there. Returns the list of relative
    paths that were flattened (empty when no ``environment/`` subdir
    exists). #369.
    """
    env_dir = bundle_dir / "environment"
    if not env_dir.is_dir():
        return []
    flattened: list[str] = []
    for src in sorted(env_dir.rglob("*")):
        if not src.is_file() or src.is_symlink():
            continue
        rel = src.relative_to(env_dir)
        dst = bundle_dir / rel
        if dst.exists():
            continue  # top-level name wins on collision
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src, dst)
        flattened.append(rel.as_posix())
    return flattened


def _promote_cpu_arch_if_runtime_fallback(
    raw_cfg: dict[str, Any], bundle_dir: Path,
) -> None:
    """If the bundle's Dockerfile uses a base image the worker will
    substitute an arm64 build for at trial time, promote an unspecified
    ``environment.cpu_arch`` to ``"any"`` so the scheduler routes trials
    to arm64 pools too. Explicit user choices are respected. #342.
    """
    env = raw_cfg.get("environment")
    if not isinstance(env, dict):
        return
    if "cpu_arch" in env:
        return  # explicit choice — never override
    dockerfile_rel = env.get("dockerfile")
    if not isinstance(dockerfile_rel, str):
        return
    dockerfile_path = bundle_dir / dockerfile_rel
    if not dockerfile_path.is_file():
        return
    if dockerfile_uses_runtime_arm64_fallback_base(dockerfile_path):
        env["cpu_arch"] = "any"
