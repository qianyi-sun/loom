"""Sync engine for `config/benchmarks.toml` (issue #234).

Walks the parsed `BenchmarksConfig` and UPSERTs rows into the
`benchmarks` + `tasks` tables. Idempotent: re-running against the same
TOML + same on-disk task bundles yields no row churn.

Topology note: this engine assumes the caller has access to BOTH the
DB session AND the on-disk `fixtures_root` directory tree. In dev
compose CP runs on the host so it satisfies both. In k8s, this is an
operator-run one-shot Job that mounts the same data PV as the worker.
The control-plane lifespan does NOT auto-sync — that would require
mounting the data PV into CP which violates the workload separation.
"""
from __future__ import annotations

import logging
import os
import tomllib
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from loom.db.schema import Benchmark
from loom.db.schema import Task as TaskRow
from loom.models.task import TaskConfig
from loom.models.task_checksum import task_checksum
from loom_cli.benchmarks_config import (
    BenchmarksConfig,
    LocalBenchmarkEntry,
    RemapBenchmarkEntry,
)

logger = logging.getLogger(__name__)

LOCAL_FOLDER_KIND = "local-folder"
SYNC_IMPORTED_BY = "benchmarks-toml-sync"

PlanAction = Literal["INSERT", "UPDATE", "SKIP", "ERROR"]


@dataclass(frozen=True)
class PlanRow:
    kind: Literal["local", "remap"]
    id: str
    action: PlanAction
    reason: str


@dataclass(frozen=True)
class TaskCounts:
    """Per-benchmark task-row diff outcome from one sync pass."""

    inserted: int = 0
    updated: int = 0
    unchanged: int = 0

    @property
    def total(self) -> int:
        return self.inserted + self.updated + self.unchanged


@dataclass
class SyncPlan:
    """Per-entry decisions + per-benchmark task diff counts."""

    rows: list[PlanRow] = field(default_factory=list)
    tasks: dict[str, TaskCounts] = field(default_factory=dict)

    def add(
        self,
        *,
        kind: Literal["local", "remap"],
        id: str,
        action: PlanAction,
        reason: str = "",
    ) -> None:
        self.rows.append(
            PlanRow(kind=kind, id=id, action=action, reason=reason),
        )


class SyncError(Exception):
    """Raised on a hard sync failure (TOML invalid, collision, bad task)."""


def preflight(
    cfg: BenchmarksConfig,
    *,
    registry_names: set[str],
) -> None:
    """Validate cross-cutting constraints before any DB work.

    Raises `SyncError` for any condition that should abort sync.
    """
    for local_entry in cfg.local:
        if local_entry.id in registry_names:
            raise SyncError(
                f"local benchmark id {local_entry.id!r} collides with a "
                "registered entry-point adapter; pick a different id "
                "to avoid shadowing the built-in",
            )
    for remap_entry in cfg.remap:
        if remap_entry.id in registry_names:
            raise SyncError(
                f"remap benchmark id {remap_entry.id!r} collides with a "
                "registered entry-point adapter; pick a different id",
            )
        if remap_entry.inherit not in registry_names:
            raise SyncError(
                f"remap {remap_entry.id!r} inherits from "
                f"{remap_entry.inherit!r} which is not in REGISTRY "
                f"(available: {sorted(registry_names)})",
            )


async def sync(
    cfg: BenchmarksConfig,
    *,
    fixtures_root: Path,
    session: AsyncSession,
    registry_names: set[str],
    dry_run: bool = False,
) -> SyncPlan:
    """Apply the TOML to the DB. Returns the plan."""
    preflight(cfg, registry_names=registry_names)
    plan = SyncPlan()

    for local_entry in cfg.local:
        await _sync_local(
            local_entry,
            fixtures_root=fixtures_root,
            session=session,
            plan=plan,
            dry_run=dry_run,
        )

    for remap_entry in cfg.remap:
        await _sync_remap(
            remap_entry,
            session=session,
            plan=plan,
            dry_run=dry_run,
        )

    return plan


async def _sync_local(
    entry: LocalBenchmarkEntry,
    *,
    fixtures_root: Path,
    session: AsyncSession,
    plan: SyncPlan,
    dry_run: bool,
) -> None:
    source_dir = fixtures_root / entry.id
    if not source_dir.is_dir():
        logger.warning(
            "benchmarks_sync_skip kind=local id=%s reason=missing_source_dir path=%s",
            entry.id, source_dir,
        )
        plan.add(
            kind="local", id=entry.id, action="SKIP",
            reason=f"source dir missing: {source_dir}",
        )
        return

    task_tomls = _walk_task_tomls(source_dir)
    if not task_tomls:
        logger.warning(
            "benchmarks_sync_skip kind=local id=%s reason=empty_source_dir path=%s",
            entry.id, source_dir,
        )
        plan.add(
            kind="local", id=entry.id, action="SKIP",
            reason=f"source dir empty: {source_dir}",
        )
        return

    desired = dict(
        id=entry.id,
        display_name=entry.display_name,
        series=entry.series,
        license_spdx=entry.license_spdx,
        license_url="",
        upstream_kind=LOCAL_FOLDER_KIND,
        upstream_locator=str(source_dir),
        upstream_revision="",
        splits=[],
        imported_by=SYNC_IMPORTED_BY,
    )
    existing = await _get_benchmark(session, entry.id)
    if existing is None:
        action: PlanAction = "INSERT"
        reason = "new"
    elif _benchmark_differs(existing, desired):
        action = "UPDATE"
        reason = "metadata changed"
    else:
        action = "SKIP"
        reason = "unchanged"

    plan.add(kind="local", id=entry.id, action=action, reason=reason)

    if not dry_run and action != "SKIP":
        await session.execute(
            pg_insert(Benchmark).values(**desired).on_conflict_do_update(
                index_elements=["id"],
                set_={k: v for k, v in desired.items() if k != "id"},
            ),
        )
        await session.commit()

    plan.tasks[entry.id] = await _sync_local_tasks(
        entry,
        source_dir=source_dir,
        task_tomls=task_tomls,
        session=session,
        dry_run=dry_run,
    )


async def _sync_local_tasks(
    entry: LocalBenchmarkEntry,
    *,
    source_dir: Path,
    task_tomls: list[Path],
    session: AsyncSession,
    dry_run: bool,
) -> TaskCounts:
    """SELECT-first then UPSERT only when checksum differs.

    Avoids O(tasks) writes on no-op re-syncs (e.g., the auto-sync
    hook on `loom service up`). One commit at the end of the loop —
    a 500-task benchmark sync used to issue 500 commits.
    """
    inserted = updated = unchanged = 0
    did_write = False
    for task_toml in task_tomls:
        bundle_dir = task_toml.parent
        rel = bundle_dir.relative_to(source_dir)
        task_id = (
            entry.id if rel == Path(".") else f"{entry.id}/{rel.as_posix()}"
        )

        try:
            with task_toml.open("rb") as f:
                raw_cfg: dict[str, Any] = tomllib.load(f)
            TaskConfig.model_validate(raw_cfg)
        except Exception as exc:
            raise SyncError(
                f"invalid task.toml at {task_toml}: {exc}. Fix the file "
                "or remove the bundle to unblock sync.",
            ) from exc

        checksum = task_checksum(bundle_dir)
        existing = await _get_task(session, task_id)
        if existing is None:
            inserted += 1
        elif (
            existing.checksum != checksum
            or existing.benchmark_id != entry.id
            or existing.license != entry.license_spdx
        ):
            updated += 1
        else:
            unchanged += 1
            continue  # no write needed

        if not dry_run:
            desired = dict(
                id=task_id,
                checksum=checksum,
                config=raw_cfg,
                source=f"fixture://{task_id}",
                license=entry.license_spdx,
                benchmark_id=entry.id,
            )
            await session.execute(
                pg_insert(TaskRow).values(**desired).on_conflict_do_update(
                    index_elements=["id"],
                    set_={
                        "checksum": checksum,
                        "config": raw_cfg,
                        "source": desired["source"],
                        "license": entry.license_spdx,
                        "benchmark_id": entry.id,
                    },
                ),
            )
            did_write = True
    if did_write:
        await session.commit()
    return TaskCounts(
        inserted=inserted, updated=updated, unchanged=unchanged,
    )


def _walk_task_tomls(source_dir: Path) -> list[Path]:
    """Find every `task.toml` under `source_dir`, no symlink-following.

    Returned paths are sorted for stable plan output.
    """
    found: list[Path] = []
    for dirpath, _dirnames, filenames in os.walk(
        source_dir, followlinks=False,
    ):
        if "task.toml" in filenames:
            found.append(Path(dirpath) / "task.toml")
    found.sort()
    return found


async def _sync_remap(
    entry: RemapBenchmarkEntry,
    *,
    session: AsyncSession,
    plan: SyncPlan,
    dry_run: bool,
) -> None:
    """PR-2 will populate this with adapter resolution + task import.

    PR-1 ships only the loader + preflight, so a remap entry that
    reaches sync is reported as SKIP (not-yet-implemented) without
    touching the DB.
    """
    plan.add(
        kind="remap", id=entry.id, action="SKIP",
        reason="remap support pending PR-2",
    )


async def _get_benchmark(
    session: AsyncSession, benchmark_id: str,
) -> Benchmark | None:
    result = await session.execute(
        select(Benchmark).where(Benchmark.id == benchmark_id),
    )
    return result.scalar_one_or_none()


async def _get_task(session: AsyncSession, task_id: str) -> TaskRow | None:
    result = await session.execute(
        select(TaskRow).where(TaskRow.id == task_id),
    )
    return result.scalar_one_or_none()


def _benchmark_differs(row: Benchmark, desired: dict[str, Any]) -> bool:
    """Compare the columns we manage. Ignore imported_at / generated."""
    for key in (
        "display_name",
        "series",
        "license_spdx",
        "license_url",
        "upstream_kind",
        "upstream_locator",
        "upstream_revision",
        "splits",
    ):
        if getattr(row, key) != desired[key]:
            return True
    return False


def render_plan_table(plan: SyncPlan) -> str:
    """Plain-text dry-run output (used by the CLI subcommand)."""
    if not plan.rows:
        return "(no entries)"
    header = ("KIND", "ID", "ACTION", "REASON")
    rows = [header] + [
        (r.kind, r.id, r.action, r.reason) for r in plan.rows
    ]
    widths = [max(len(str(r[i])) for r in rows) for i in range(4)]
    lines = []
    for r in rows:
        lines.append(
            "  ".join(str(r[i]).ljust(widths[i]) for i in range(4)).rstrip(),
        )
    return "\n".join(lines)
