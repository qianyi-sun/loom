"""Shared task_filter resolution.

Hoisted from `routes/batches.py` so `POST /tasks/count` (the SPA's
real-count check on NewBatch — issue #28) can reuse the exact same
materialization the batch creator runs. No behavior change vs. the
previous in-batches.py implementation; just relocated.

Filter shape (`task_filter`):
- `license: str` — exact match on Task.license
- `task_ids: list[str]` — restrict to the given ids (also serves
  as the existence-check in subset_kind='explicit')
- `benchmark_id: str` — singular benchmark filter
- `benchmark_ids: list[str]` — multi-select; takes precedence over
  the singular form. Empty list ⇒ zero tasks (the SPA's group-select
  uses this convention)
- `tag_filters: dict[str, list[str]]` — JSONB containment match per
  key; AND across keys, OR within each key's values
- `subset_kind: "all" | "first_n" | "last_n" | "random_n" | "explicit"`
- `n: int` (required when subset_kind ∈ {first_n, last_n, random_n})
- `seed: int` (required when subset_kind = "random_n" so the same
  call reproduces the same sample)
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from fastapi import HTTPException
from pydantic import ValidationError
from sqlalchemy import cast, false, or_, select
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.asyncio import AsyncSession

from loom.db.schema import Task
from loom.license_policy import is_license_allowed_for_submit
from loom.models.task import TaskConfig
from loom_service.license_policy import sorted_license_block_reasons

# Recognized task_filter keys. Anything else is rejected so a typo
# (`liscense` instead of `license`) doesn't silently match nothing.
FILTER_KEYS: frozenset[str] = frozenset(
    {
        "license",
        "task_ids",
        "benchmark_id",
        "subset_kind",
        "n",
        "seed",
        "benchmark_ids",
        "tag_filters",
    }
)

SUBSET_KINDS: frozenset[str] = frozenset(
    {"all", "first_n", "last_n", "random_n", "explicit"},
)


@dataclass(frozen=True)
class TaskFilterResult:
    task_ids: list[str]
    license_blocked_count: int = 0
    license_blocked_reasons: tuple[str, ...] = ()


async def resolve_task_filter(
    session: AsyncSession,
    task_filter: Mapping[str, Any],
) -> list[str]:
    return (
        await resolve_task_filter_with_diagnostics(
            session,
            task_filter,
        )
    ).task_ids


async def resolve_task_filter_with_diagnostics(
    session: AsyncSession,
    task_filter: Mapping[str, Any],
    *,
    license_allowlist: tuple[str, ...] | None = None,
) -> TaskFilterResult:
    """Materialize a task_filter into a list of Task.id strings.

    Honors `subset_kind`:
    - `"all"` (default): every matching task.
    - `"first_n"`: first N task ids sorted ascending.
    - `"last_n"`: last N task ids sorted ascending.
    - `"random_n"`: N randomly-chosen task ids. Requires `seed` so
      the same call reproduces the same selection (Python's seeded
      `random.sample` on the materialized candidate list).
    - `"explicit"`: returns `task_ids` verbatim (after the existence
      check below).
    """
    unknown = set(task_filter.keys()) - FILTER_KEYS
    if unknown:
        raise HTTPException(
            status_code=400,
            detail=f"unknown task_filter keys: {sorted(unknown)}",
        )
    subset_kind = task_filter.get("subset_kind", "all")
    if subset_kind not in SUBSET_KINDS:
        raise HTTPException(
            status_code=400,
            detail=(f"unknown subset_kind {subset_kind!r}. valid: {sorted(SUBSET_KINDS)}"),
        )

    # Build the candidate query using the predicate keys. subset_kind
    # / n / seed are applied to the candidate set in Python.
    #
    # `benchmark_ids` (list) takes precedence over the singular
    # `benchmark_id` for multi-select / group-select UX. `tag_filters`
    # is a dict like `{"verified": ["true"]}` applied as a JSONB
    # containment predicate per key — `AND` across keys, `OR` within
    # each key's value list.
    stmt = select(Task.id, Task.config, Task.license, Task.tags).order_by(
        Task.id.asc(),
    )
    if "license" in task_filter:
        stmt = stmt.where(Task.license == task_filter["license"])
    if "task_ids" in task_filter:
        ids = [str(x) for x in task_filter["task_ids"]]
        stmt = stmt.where(Task.id.in_(ids))
    benchmark_ids_raw = task_filter.get("benchmark_ids")
    if benchmark_ids_raw is not None:
        if not isinstance(benchmark_ids_raw, (list, tuple)) or not all(
            isinstance(x, str) for x in benchmark_ids_raw
        ):
            raise HTTPException(
                status_code=400,
                detail="task_filter.benchmark_ids must be a list of strings",
            )
        if benchmark_ids_raw:
            stmt = stmt.where(Task.benchmark_id.in_(list(benchmark_ids_raw)))
        else:
            stmt = stmt.where(false())
    elif "benchmark_id" in task_filter:
        stmt = stmt.where(Task.benchmark_id == task_filter["benchmark_id"])
    tag_filters_raw = task_filter.get("tag_filters")
    if tag_filters_raw is not None:
        if not isinstance(tag_filters_raw, dict):
            raise HTTPException(
                status_code=400,
                detail="task_filter.tag_filters must be a JSON object",
            )
        for tag_key, tag_values in tag_filters_raw.items():
            if not isinstance(tag_key, str) or not tag_key:
                raise HTTPException(
                    status_code=400,
                    detail="tag_filters keys must be non-empty strings",
                )
            if not isinstance(tag_values, list) or not all(isinstance(v, str) for v in tag_values):
                raise HTTPException(
                    status_code=400,
                    detail=(f"tag_filters[{tag_key!r}] must be a list of strings"),
                )
            if not tag_values:
                continue
            # OR within a key: any of the listed values matches.
            value_clauses = [
                Task.tags.op("@>")(
                    cast({tag_key: v}, JSONB),
                )
                for v in tag_values
            ]
            stmt = stmt.where(or_(*value_clauses))
    candidate_rows = (await session.execute(stmt)).all()
    blocked_licenses: set[str] = set()
    candidates: list[str] = []
    for task_id, config, task_license, tags in candidate_rows:
        valid_for_license_policy = True
        if license_allowlist is not None:
            try:
                TaskConfig.model_validate(config)
            except ValidationError:
                valid_for_license_policy = False
        if (
            license_allowlist is not None
            and valid_for_license_policy
            and not is_license_allowed_for_submit(
                task_license=task_license,
                allowlist=license_allowlist,
                tags=tags,
            )
        ):
            blocked_licenses.add(str(task_license))
            continue
        candidates.append(str(task_id))
    license_blocked_count = len(candidate_rows) - len(candidates)
    license_blocked_reasons = sorted_license_block_reasons(blocked_licenses)

    if subset_kind == "all":
        return TaskFilterResult(
            task_ids=candidates,
            license_blocked_count=license_blocked_count,
            license_blocked_reasons=license_blocked_reasons,
        )
    if subset_kind == "explicit":
        # Explicit mode REQUIRES `task_ids` to be supplied AND the
        # existence check above runs via the `Task.id.in_(ids)`
        # predicate. The returned `candidates` is the intersection
        # of the supplied list and the live tasks table; any supplied
        # id not present in the table is silently dropped (the higher
        # route layer checks for empty-result).
        return TaskFilterResult(
            task_ids=candidates,
            license_blocked_count=license_blocked_count,
            license_blocked_reasons=license_blocked_reasons,
        )

    n_raw = task_filter.get("n")
    try:
        n = int(n_raw) if n_raw is not None else None
    except (TypeError, ValueError) as exc:
        raise HTTPException(
            status_code=400,
            detail="task_filter.n must be an integer",
        ) from exc
    if n is None or n < 1:
        raise HTTPException(
            status_code=400,
            detail=(f"task_filter.n is required and must be ≥ 1 when subset_kind={subset_kind!r}"),
        )

    if subset_kind == "first_n":
        selected = candidates[:n]
        return TaskFilterResult(
            task_ids=selected,
            license_blocked_count=license_blocked_count,
            license_blocked_reasons=license_blocked_reasons,
        )
    if subset_kind == "last_n":
        selected = candidates[-n:] if n <= len(candidates) else candidates
        return TaskFilterResult(
            task_ids=selected,
            license_blocked_count=license_blocked_count,
            license_blocked_reasons=license_blocked_reasons,
        )
    # random_n
    seed_raw = task_filter.get("seed")
    if seed_raw is None:
        raise HTTPException(
            status_code=400,
            detail="task_filter.seed is required when subset_kind='random_n'",
        )
    try:
        seed = int(seed_raw)
    except (TypeError, ValueError) as exc:
        raise HTTPException(
            status_code=400,
            detail="task_filter.seed must be an integer",
        ) from exc
    import random as _random

    rng = _random.Random(seed)
    if n >= len(candidates):
        selected = candidates
    else:
        selected = sorted(rng.sample(candidates, n))
    return TaskFilterResult(
        task_ids=selected,
        license_blocked_count=license_blocked_count,
        license_blocked_reasons=license_blocked_reasons,
    )
