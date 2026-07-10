"""`python -m loom_benchmark_tool register <benchmark>` — per-deploy
counterpart to `publish`.

Reads the manifest from `{hf_org}/loom-benchmark-{benchmark}` on HF
Hub, upserts the Benchmark row, and upserts a Task row per entry. By
default task rows point at `hf://{repo_id}@{revision}/{hf_path}` for
back-compat. In protected deployments, pass `mirror_to_object_store`
with an object store so the operator process downloads the exact HF
revision, mirrors bundles into internal S3/MinIO, and stores `s3://...`
runtime sources while retaining HF provenance in tags.

New manifests carry validated per-task `TaskConfig` payloads, so
registered rows are runnable immediately after this command commits.
Legacy manifests that lack `task_config` remain catalog placeholders:
they preserve metadata and source pointers but are not counted as
runnable until republished or backfilled.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

from loom_benchmarks.util import sha256_of_dir
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from loom.db.schema import Benchmark
from loom.db.schema import Task as TaskRow
from loom.models.task import TaskConfig
from loom.trajectory.storage import ObjectStore
from loom_benchmark_tool.db_url import normalize_db_url
from loom_benchmark_tool.dockerfile_safety import validate_task_dir_dockerfiles
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


@dataclass(frozen=True)
class MirrorResult:
    source: str
    uploaded: int
    skipped: int
    bytes_uploaded: int
    bytes_skipped: int


def _safe_key_part(value: str) -> str:
    return value.strip("/").replace("/", "__")


def _validate_relative_prefix(value: str, *, label: str) -> str:
    prefix = value.strip("/")
    if not prefix:
        raise ValueError(f"{label} must not be empty")
    parts = Path(prefix).parts
    if prefix.startswith("/") or ".." in parts:
        raise ValueError(f"{label} contains unsafe path segments: {value!r}")
    return prefix


def _bundle_checksum(bundle_dir: Path) -> str:
    return cast(str, sha256_of_dir(bundle_dir))


def _mirror_prefix(
    *,
    repo_id: str,
    revision: str,
    task_id: str,
    checksum: str,
    hf_path: str,
) -> str:
    benchmark_id = task_id.split("/", 1)[0]
    return (
        f"{_safe_key_part(benchmark_id)}/"
        f"{_safe_key_part(repo_id)}/"
        f"{_safe_key_part(revision)}/"
        f"{_validate_relative_prefix(hf_path, label='hf_path')}/"
        f"{_safe_key_part(checksum)}/"
    )


async def mirror_manifest_task_bundle(
    *,
    repo_id: str,
    revision: str,
    task_id: str,
    checksum: str,
    hf_path: str,
    snapshot_root: Path,
    object_store: ObjectStore,
    bucket: str,
) -> MirrorResult:
    """Mirror one manifest task bundle from a local HF snapshot into S3/MinIO.

    Re-running against the same snapshot is idempotent: existing byte-identical
    objects are skipped, while missing or changed objects are written.
    """
    relative_prefix = _validate_relative_prefix(hf_path, label="hf_path")
    bundle_dir = snapshot_root / relative_prefix
    if not bundle_dir.is_dir():
        raise FileNotFoundError(
            f"HF bundle path {relative_prefix!r} not found under {snapshot_root}",
        )
    actual_checksum = _bundle_checksum(bundle_dir)
    if actual_checksum != checksum:
        raise ValueError(
            "HF bundle checksum mismatch for "
            f"{task_id}: manifest={checksum} actual={actual_checksum}",
        )
    validate_task_dir_dockerfiles(bundle_dir)

    await object_store.ensure_bucket(bucket)
    target_prefix = _mirror_prefix(
        repo_id=repo_id,
        revision=revision,
        task_id=task_id,
        checksum=checksum,
        hf_path=hf_path,
    )
    uploaded = 0
    skipped = 0
    bytes_uploaded = 0
    bytes_skipped = 0
    for path in sorted(bundle_dir.rglob("*")):
        if path.is_dir():
            continue
        rel = path.relative_to(bundle_dir).as_posix()
        key = f"{target_prefix}{rel}"
        body = path.read_bytes()
        try:
            existing = await object_store.get_object(bucket=bucket, key=key)
        except Exception:
            existing = None
        if existing == body:
            skipped += 1
            bytes_skipped += len(body)
            continue
        await object_store.put_object(bucket=bucket, key=key, body=body)
        uploaded += 1
        bytes_uploaded += len(body)

    if uploaded + skipped == 0:
        raise FileNotFoundError(f"HF bundle path {relative_prefix!r} has no files")

    return MirrorResult(
        source=f"s3://{bucket}/{target_prefix}",
        uploaded=uploaded,
        skipped=skipped,
        bytes_uploaded=bytes_uploaded,
        bytes_skipped=bytes_skipped,
    )


async def _download_hf_bundle_snapshot(
    *,
    repo_id: str,
    revision: str,
    hf_token: str | None,
    tasks: list[dict[str, Any]],
    chunk_size: int | None = None,
    chunk_sleep_secs: float = 300.0,
) -> Path:
    """Snapshot-download the HF bundles for `tasks` into the shared HF cache.

    When `chunk_size` is set, `tasks` is split into batches and
    `snapshot_download` is called once per batch with `chunk_sleep_secs` seconds
    of sleep between batches. This keeps the total `resolve` API calls per
    5-minute window below HF's 5000/5min free-tier budget, at the cost of extra
    wall time. Because `snapshot_download` returns the same snapshot directory
    for a given `repo_id` + `revision`, files from every batch accumulate under
    the same returned path.
    """
    from huggingface_hub import snapshot_download

    patterns = [
        f"{_validate_relative_prefix(str(task['hf_path']), label='hf_path')}/*"
        for task in tasks
    ]

    if chunk_size is None or chunk_size <= 0 or len(patterns) <= chunk_size:
        single_shot = await asyncio.to_thread(
            snapshot_download,
            repo_id=repo_id,
            revision=revision,
            repo_type="dataset",
            allow_patterns=patterns,
            token=hf_token,
        )
        return Path(single_shot)

    snapshot_path: str | None = None
    total_batches = (len(patterns) + chunk_size - 1) // chunk_size
    for batch_idx in range(total_batches):
        start = batch_idx * chunk_size
        batch_patterns = patterns[start : start + chunk_size]
        print(
            f"mirror snapshot: batch {batch_idx + 1}/{total_batches} "
            f"({len(batch_patterns)} bundles)",
            flush=True,
        )
        snapshot_path = await asyncio.to_thread(
            snapshot_download,
            repo_id=repo_id,
            revision=revision,
            repo_type="dataset",
            allow_patterns=batch_patterns,
            token=hf_token,
        )
        if batch_idx < total_batches - 1 and chunk_sleep_secs > 0:
            print(
                f"mirror snapshot: sleeping {chunk_sleep_secs}s "
                "to stay under HF resolve budget",
                flush=True,
            )
            await asyncio.sleep(chunk_sleep_secs)
    assert snapshot_path is not None
    return Path(snapshot_path)


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
    mirror_to_object_store: bool = False,
    object_store: ObjectStore | None = None,
    bucket: str = "loom-benchmarks",
    chunk_size: int | None = None,
    chunk_sleep_secs: float = 300.0,
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
    manifest_tasks = list(manifest["tasks"])

    snapshot_root: Path | None = None
    mirrored = 0
    mirror_uploaded = 0
    mirror_skipped = 0
    mirror_bytes_uploaded = 0
    mirror_bytes_skipped = 0
    mirrored_at = datetime.now(UTC).isoformat()
    if mirror_to_object_store:
        if object_store is None:
            raise ValueError("mirror_to_object_store requires object_store")
        snapshot_root = await _download_hf_bundle_snapshot(
            repo_id=repo_id,
            revision=revision,
            hf_token=hf_token,
            tasks=manifest_tasks,
            chunk_size=chunk_size,
            chunk_sleep_secs=chunk_sleep_secs,
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
            for t in manifest_tasks:
                source = _hf_source_url(
                    repo_id=repo_id,
                    revision=revision,
                    hf_path=t["hf_path"],
                )
                # PR-1: per-task tags from manifest v2. v1 manifests
                # omit `tags`; treat absent + {} identically.
                tags = dict(t.get("tags") or {})
                config = task_config_from_manifest_entry(t)
                if not config:
                    legacy_placeholders += 1
                if mirror_to_object_store:
                    assert object_store is not None
                    assert snapshot_root is not None
                    mirror = await mirror_manifest_task_bundle(
                        repo_id=repo_id,
                        revision=revision,
                        task_id=t["task_id"],
                        checksum=t["checksum"],
                        hf_path=t["hf_path"],
                        snapshot_root=snapshot_root,
                        object_store=object_store,
                        bucket=bucket,
                    )
                    source = mirror.source
                    mirrored += 1
                    mirror_uploaded += mirror.uploaded
                    mirror_skipped += mirror.skipped
                    mirror_bytes_uploaded += mirror.bytes_uploaded
                    mirror_bytes_skipped += mirror.bytes_skipped
                    tags.update({
                        "hf_repo_id": repo_id,
                        "hf_revision": revision,
                        "hf_path": t["hf_path"],
                        "hf_checksum": t["checksum"],
                        "runtime_source_kind": "internal_object_store",
                        "runtime_source_mirrored_at": mirrored_at,
                    })
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
        "mirrored": mirrored,
        "mirror_uploaded": mirror_uploaded,
        "mirror_skipped": mirror_skipped,
        "mirror_bytes_uploaded": mirror_bytes_uploaded,
        "mirror_bytes_skipped": mirror_bytes_skipped,
        "repo_id": repo_id,
        "revision": revision,
    }
