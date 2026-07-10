"""Resolve public benchmark selectors to immutable physical profiles."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass

from fastapi import HTTPException
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from loom.db.schema import Benchmark, BenchmarkAlias


@dataclass(frozen=True)
class ResolvedBenchmarkSelectors:
    """Physical benchmark profiles and their requested-selector audit trail."""

    physical_ids: tuple[str, ...]
    provenance: tuple[dict[str, str], ...]


async def resolve_benchmark_selectors(
    session: AsyncSession,
    selectors: Sequence[str],
    *,
    require_runnable: bool,
) -> ResolvedBenchmarkSelectors:
    """Resolve aliases and reject retired profiles for new execution.

    Unknown selectors deliberately remain unchanged so existing task-filter
    behavior still produces an empty selection rather than a new 404.  Legacy
    runners pass ``require_runnable=False`` to replay batches that predate the
    submission-time task snapshot.
    """
    requested = tuple(selectors)
    if not requested:
        return ResolvedBenchmarkSelectors(physical_ids=(), provenance=())

    aliases = {
        str(alias): str(benchmark_id)
        for alias, benchmark_id in (
            await session.execute(
                select(BenchmarkAlias.alias, BenchmarkAlias.benchmark_id).where(
                    BenchmarkAlias.alias.in_(requested),
                ),
            )
        ).all()
    }
    physical_ids = tuple(aliases.get(selector, selector) for selector in requested)
    profile_states = {
        str(benchmark_id): str(execution_state)
        for benchmark_id, execution_state in (
            await session.execute(
                select(Benchmark.id, Benchmark.execution_state).where(
                    Benchmark.id.in_(physical_ids),
                ),
            )
        ).all()
    }
    if require_runnable:
        retired = next(
            (
                physical_id
                for physical_id in physical_ids
                if profile_states.get(physical_id) == "historical"
            ),
            None,
        )
        if retired is not None:
            raise HTTPException(
                status_code=409,
                detail=(
                    "benchmark_retired: historical benchmark profile "
                    f"{retired!r} cannot be selected for a new submission"
                ),
            )

    return ResolvedBenchmarkSelectors(
        physical_ids=physical_ids,
        provenance=tuple(
            {
                "kind": "benchmark_profile_selection",
                "requested_selector": selector,
                "resolved_profile": physical_id,
            }
            for selector, physical_id in zip(requested, physical_ids, strict=True)
        ),
    )
