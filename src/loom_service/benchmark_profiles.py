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


def _benchmark_not_runnable_error(profile_id: str, execution_state: str) -> HTTPException:
    reason = (
        "benchmark_retired" if execution_state == "historical" else "benchmark_not_runnable"
    )
    lifecycle = "historical" if execution_state == "historical" else execution_state
    return HTTPException(
        status_code=409,
        detail={
            "reason": reason,
            "benchmark_profile": profile_id,
            "message": (
                f"{lifecycle} benchmark profile {profile_id!r} "
                "cannot be selected for a new submission"
            ),
        },
    )


async def reject_non_runnable_benchmark_profiles(
    session: AsyncSession,
    profile_ids: Sequence[str],
) -> None:
    """Reject every non-runnable profile at the execution boundary.

    Historical profiles remain readable through catalog endpoints, while
    pending audited profiles preserve their registration evidence until the
    activation transaction promotes them.  Neither state can be selected for
    a new submission, including by a direct physical-profile selector.
    """
    requested_profiles = tuple(dict.fromkeys(profile_ids))
    if not requested_profiles:
        return
    unavailable = (
        await session.execute(
            select(Benchmark.id, Benchmark.execution_state)
            .where(Benchmark.id.in_(requested_profiles))
            .where(Benchmark.execution_state != "runnable")
            .order_by(Benchmark.id.asc()),
        )
    ).first()
    if unavailable is not None:
        profile_id, execution_state = unavailable
        raise _benchmark_not_runnable_error(str(profile_id), str(execution_state))


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
    if require_runnable:
        await reject_non_runnable_benchmark_profiles(session, physical_ids)

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
