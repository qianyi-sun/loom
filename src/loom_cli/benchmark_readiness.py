"""Benchmark readiness model shared by CLI and future catalog/API surfaces."""

from __future__ import annotations

import json
from collections import defaultdict
from dataclasses import asdict, dataclass
from typing import Any, Literal

from pydantic import ValidationError
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from loom.db.schema import Benchmark
from loom.db.schema import Task as TaskRow
from loom.models.task import TaskConfig
from loom_benchmark_tool.db_url import normalize_db_url

ReadinessState = Literal["adapter_available", "registered", "runnable", "blocked"]

KNOWN_MATERIALIZER_SCHEMES = frozenset({"fixture", "hf", "s3"})


@dataclass(frozen=True)
class BenchmarkAuditSource:
    id: str
    display_name: str
    series: str | None
    upstream_kind: str
    upstream_locator: str
    upstream_revision: str


@dataclass(frozen=True)
class TaskAuditSource:
    id: str
    config: dict[str, Any]
    source: str | None


@dataclass(frozen=True)
class BenchmarkReadinessItem:
    id: str
    display_name: str
    series: str | None
    adapter_status: str
    manifest_status: str
    raw_task_count: int
    valid_task_config_count: int
    invalid_task_config_count: int
    source_schemes: list[str]
    materializer_status: str
    smoke_status: str
    readiness_state: ReadinessState
    blocker_reason: str | None


def _source_scheme(source: str | None) -> str:
    if not source:
        return "none"
    if "://" not in source:
        return "path"
    return source.split("://", 1)[0]


def build_readiness_item(
    benchmark: BenchmarkAuditSource,
    *,
    tasks: list[TaskAuditSource],
    registry_names: set[str],
) -> BenchmarkReadinessItem:
    adapter_status = (
        "available" if benchmark.id in registry_names else "missing"
    )
    raw_count = len(tasks)
    valid_count = 0
    for task in tasks:
        try:
            TaskConfig.model_validate(task.config)
        except ValidationError:
            continue
        valid_count += 1
    invalid_count = raw_count - valid_count

    source_schemes = sorted({_source_scheme(task.source) for task in tasks})
    missing_materializer = bool(
        source_schemes
        and any(scheme not in KNOWN_MATERIALIZER_SCHEMES for scheme in source_schemes)
    )
    materializer_status = "missing" if missing_materializer else "available"
    manifest_status = "registered" if raw_count else "missing"

    blocker_reason: str | None = None
    readiness_state: ReadinessState
    if raw_count == 0:
        readiness_state = "blocked"
        blocker_reason = "manifest_missing"
    elif missing_materializer:
        readiness_state = "blocked"
        blocker_reason = "materializer_missing"
    elif valid_count == 0:
        readiness_state = "blocked"
        blocker_reason = "manifest_legacy_missing_task_config"
    elif invalid_count > 0:
        readiness_state = "blocked"
        blocker_reason = "task_config_invalid"
    else:
        readiness_state = "runnable"

    return BenchmarkReadinessItem(
        id=benchmark.id,
        display_name=benchmark.display_name,
        series=benchmark.series,
        adapter_status=adapter_status,
        manifest_status=manifest_status,
        raw_task_count=raw_count,
        valid_task_config_count=valid_count,
        invalid_task_config_count=invalid_count,
        source_schemes=source_schemes,
        materializer_status=materializer_status,
        smoke_status="unknown",
        readiness_state=readiness_state,
        blocker_reason=blocker_reason,
    )


def render_readiness_json(items: list[BenchmarkReadinessItem]) -> str:
    return json.dumps(
        {"count": len(items), "items": [asdict(item) for item in items]},
        indent=2,
        sort_keys=True,
    )


def render_readiness_table(items: list[BenchmarkReadinessItem]) -> str:
    id_w = max(12, max((len(item.id) for item in items), default=0))
    state_w = max(9, max((len(item.readiness_state) for item in items), default=0))
    blocker_w = max(
        7, max((len(item.blocker_reason or "-") for item in items), default=0)
    )
    header = (
        f"{'BENCHMARK':<{id_w}} {'READINESS':<{state_w}} "
        f"{'RAW':>5} {'VALID':>5} {'SCHEMES':<12} {'BLOCKER':<{blocker_w}}"
    )
    rows = [header]
    for item in items:
        rows.append(
            f"{item.id:<{id_w}} {item.readiness_state:<{state_w}} "
            f"{item.raw_task_count:>5} {item.valid_task_config_count:>5} "
            f"{','.join(item.source_schemes) or '-':<12} "
            f"{item.blocker_reason or '-':<{blocker_w}}"
        )
    return "\n".join(rows)


def _load_registry_names() -> set[str]:
    try:
        from loom_benchmarks.registry import REGISTRY
    except Exception:
        return set()
    return set(REGISTRY.keys())


async def run_readiness_audit(
    *,
    db_url: str,
    benchmark: str | None = None,
    registry_names: set[str] | None = None,
) -> list[BenchmarkReadinessItem]:
    engine = create_async_engine(normalize_db_url(db_url))
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with session_factory() as session:
            benchmark_stmt = select(Benchmark).order_by(Benchmark.id)
            if benchmark is not None:
                benchmark_stmt = benchmark_stmt.where(Benchmark.id == benchmark)
            benchmarks = list((await session.scalars(benchmark_stmt)).all())
            if not benchmarks:
                return []

            benchmark_ids = [row.id for row in benchmarks]
            task_rows = (
                await session.execute(
                    select(
                        TaskRow.benchmark_id,
                        TaskRow.id,
                        TaskRow.config,
                        TaskRow.source,
                    ).where(TaskRow.benchmark_id.in_(benchmark_ids)),
                )
            ).all()
    finally:
        await engine.dispose()

    tasks_by_benchmark: dict[str, list[TaskAuditSource]] = defaultdict(list)
    for benchmark_id, task_id, config, source in task_rows:
        if benchmark_id is None:
            continue
        tasks_by_benchmark[str(benchmark_id)].append(
            TaskAuditSource(
                id=str(task_id),
                config=dict(config),
                source=source,
            )
        )

    names = registry_names if registry_names is not None else _load_registry_names()
    return [
        build_readiness_item(
            BenchmarkAuditSource(
                id=row.id,
                display_name=row.display_name,
                series=row.series,
                upstream_kind=row.upstream_kind,
                upstream_locator=row.upstream_locator,
                upstream_revision=row.upstream_revision,
            ),
            tasks=tasks_by_benchmark.get(row.id, []),
            registry_names=names,
        )
        for row in benchmarks
    ]
