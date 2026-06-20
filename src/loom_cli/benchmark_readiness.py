"""Benchmark readiness audit command helpers."""

from __future__ import annotations

from collections import defaultdict

from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from loom.benchmark_readiness import (
    BenchmarkAuditSource,
    BenchmarkReadinessItem,
    TaskAuditSource,
    build_readiness_item,
    render_readiness_json,
    render_readiness_table,
)
from loom.db.schema import Benchmark
from loom.db.schema import Task as TaskRow
from loom_benchmark_tool.db_url import normalize_db_url


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


__all__ = [
    "BenchmarkAuditSource",
    "BenchmarkReadinessItem",
    "TaskAuditSource",
    "build_readiness_item",
    "render_readiness_json",
    "render_readiness_table",
    "run_readiness_audit",
]
