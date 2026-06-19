"""`python -m loom_benchmark_tool register <benchmark>` — per-deploy
counterpart to `publish`.

Reads the manifest from `{hf_org}/loom-benchmark-{benchmark}` on HF
Hub, upserts the Benchmark row, and upserts a Task row per entry with
`source = "hf://{repo_id}@{revision}/{hf_path}"`. No upstream fetch,
no conversion, no MinIO.

New manifests carry validated per-task `TaskConfig` payloads, so
registered rows are runnable immediately after this command commits.
Legacy manifests that lack `task_config` remain catalog placeholders:
they preserve metadata and source pointers but are not counted as
runnable until republished or backfilled.
"""

from __future__ import annotations

from typing import Any

from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from loom.db.schema import Benchmark
from loom.db.schema import Task as TaskRow
from loom.models.task import TaskConfig
from loom_benchmark_tool.db_url import normalize_db_url
from loom_benchmark_tool.manifest import (
    read_manifest_from_hf,
    repo_id_for,
)


def _hf_source_url(
    *,
    repo_id: str,
    revision: str,
    hf_path: str,
) -> str:
    """Canonical `hf://` source URL. The worker's hf:// dispatcher
    parses this exact shape; keep the format here in lockstep with
    `loom_worker.main_loop._materialize_hf_dir`."""
    return f"hf://{repo_id}@{revision}/{hf_path}"


def task_config_from_manifest_entry(entry: dict[str, Any]) -> dict[str, Any]:
    """Return validated TaskConfig payload for a manifest task entry.

    Legacy manifests do not carry task config data; those rows remain explicit
    non-runnable placeholders until republished or backfilled.
    """
    config = entry.get("task_config")
    if config is None:
        return {}
    if not isinstance(config, dict):
        raise TypeError("manifest task_config must be an object")
    parsed = TaskConfig.model_validate(config)
    expected_task_id = entry.get("task_id")
    if expected_task_id is not None and parsed.task.id != expected_task_id:
        raise ValueError(
            "manifest task_config.task.id does not match task_id "
            f"({parsed.task.id!r} != {expected_task_id!r})",
        )
    return config


async def run_register(
    *,
    benchmark: str,
    hf_org: str,
    hf_token: str | None,
    db_url: str,
    revision: str = "main",
    registered_by: str | None = None,
) -> dict[str, Any]:
    """Read manifest from HF, upsert Benchmark + Task rows. Returns
    `{"registered": N, "skipped": M, "repo_id": str, "revision": str}`.

    The manifest's `task_count` MUST match the length of `tasks`; the
    publish path guarantees this. We trust it here rather than re-walking
    the HF tree.
    """
    repo_id = repo_id_for(hf_org, benchmark)
    manifest = read_manifest_from_hf(
        hf_org=hf_org,
        benchmark=benchmark,
        hf_token=hf_token,
        revision=revision,
    )

    engine = create_async_engine(normalize_db_url(db_url))
    session_factory = async_sessionmaker(engine, expire_on_commit=False)

    registered = 0
    legacy_placeholders = 0
    skipped = 0
    try:
        async with session_factory() as session:
            # Upsert the Benchmark row from manifest metadata. Same
            # ON CONFLICT shape as import_cmd so re-registering doesn't
            # double-write.
            # PR-1: `series` is added to benchmarks; manifest v2 carries
            # it as a top-level field. v1 manifests don't include it, so
            # default to None — `register` stays back-compat with already-
            # published benchmarks.
            series = manifest.get("series")
            # ON CONFLICT update set. Critically `series` is included
            # only when the manifest actually carries one — v1 manifests
            # (pre-PR-1, e.g. the already-published swe-bench / osworld
            # / humaneval) have no `series` field, so `manifest.get`
            # returns None. If we wrote None into the SET clause we'd
            # clobber a correct value previously written by the stub
            # seed (which reads `series` straight off the adapter
            # class). Skipping the key preserves the existing column.
            update_set: dict[str, Any] = {
                "display_name": manifest["display_name"],
                "upstream_revision": manifest.get(
                    "upstream_revision",
                    "",
                ),
                "imported_by": (registered_by or "loom_benchmark_tool:register"),
            }
            if series is not None:
                update_set["series"] = series
            await session.execute(
                pg_insert(Benchmark)
                .values(
                    id=manifest["benchmark_id"],
                    display_name=manifest["display_name"],
                    upstream_kind=manifest["upstream_kind"],
                    upstream_locator=manifest["upstream_locator"],
                    upstream_revision=manifest.get(
                        "upstream_revision",
                        "",
                    ),
                    license_spdx=manifest["license_spdx"],
                    license_url=manifest.get("license_url", ""),
                    splits=manifest.get("splits", ["test"]),
                    series=series,
                    imported_by=registered_by or "loom_benchmark_tool:register",
                )
                .on_conflict_do_update(
                    index_elements=["id"],
                    set_=update_set,
                ),
            )
            await session.commit()

        # One row per task. We could bulk-insert but ~thousands of
        # rows per benchmark is well within single-statement reach for
        # postgres, and the per-row upsert lets a re-register pick up
        # checksum drift (publish bumped → task row's checksum updated).
        async with session_factory() as session:
            for t in manifest["tasks"]:
                source = _hf_source_url(
                    repo_id=repo_id,
                    revision=revision,
                    hf_path=t["hf_path"],
                )
                # PR-1: per-task tags from manifest v2. v1 manifests
                # omit `tags`; treat absent + {} identically.
                tags = t.get("tags") or {}
                config = task_config_from_manifest_entry(t)
                if not config:
                    legacy_placeholders += 1
                await session.execute(
                    pg_insert(TaskRow)
                    .values(
                        id=t["task_id"],
                        checksum=t["checksum"],
                        config=config,
                        source=source,
                        license=t.get(
                            "license_spdx",
                            manifest["license_spdx"],
                        ),
                        benchmark_id=manifest["benchmark_id"],
                        tags=tags,
                    )
                    .on_conflict_do_update(
                        index_elements=["id"],
                        set_={
                            "checksum": t["checksum"],
                            "source": source,
                            "license": t.get(
                                "license_spdx",
                                manifest["license_spdx"],
                            ),
                            "benchmark_id": manifest["benchmark_id"],
                            "tags": tags,
                            "config": config,
                        },
                    ),
                )
                registered += 1
            await session.commit()
    finally:
        await engine.dispose()

    return {
        "registered": registered,
        "legacy_placeholders": legacy_placeholders,
        "skipped": skipped,
        "repo_id": repo_id,
        "revision": revision,
    }
