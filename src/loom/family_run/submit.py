"""Batch-submit helper: materialise family state (#672).

Called by the service on POST /batches when the trial_config opts into
family-run mode. Groups tasks by ``family_key_extractor``, orders each
family by ``sequencer``, provisions per-family ``state_uri`` via the
resolved state backend, and returns the (family_key -> ordered task ids)
mapping so the fanout path can populate ``trials.family_key``.

This is a pure orchestration helper - no DB session dependency other
than an ``add()`` callback. Concrete session wiring lives in
``loom_service.routes.batches``.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol
from uuid import UUID

from loom.family_run.protocols import TaskLike
from loom.family_run.registry import resolve_plugin
from loom.family_run.spec import ResolvedFamilyRunSpec


class _Adder(Protocol):
    def __call__(self, row: Any) -> None: ...


@dataclass(frozen=True)
class FamilySeed:
    """One seeded family - carries the ordered task ids so the fanout
    caller can propagate ``family_key`` onto each trial.
    """

    family_key: str
    task_sequence: list[str]
    state_uri: str


async def seed_family_state(
    *,
    batch_id: UUID,
    tasks: list[TaskLike],
    resolved: ResolvedFamilyRunSpec,
    state_backend: Any,
) -> list[FamilySeed]:
    """Group ``tasks`` by extractor, order each family, provision URIs.

    Returns one :class:`FamilySeed` per family. The caller writes these
    into ``batch_family_state`` rows and stamps ``trials.family_key`` on
    each corresponding trial row at fanout time.
    """
    extractor = resolve_plugin("loom.family.keys", resolved.family_key_extractor)
    extractor.default_params = resolved.family_key_extractor.params
    sequencer = resolve_plugin("loom.family.sequencers", resolved.sequencer)
    sequencer.default_params = resolved.sequencer.params
    adapter = resolve_plugin("loom.family.adapters", resolved.adapter)
    adapter.default_params = resolved.adapter.params

    families: dict[str, list[TaskLike]] = {}
    for task in tasks:
        key = extractor.key_for(task)
        families.setdefault(key, []).append(task)

    seeds: list[FamilySeed] = []
    for family_key, family_tasks in families.items():
        empty_uri = await state_backend.initialize(
            batch_id=batch_id,
            family_key=family_key,
            params=resolved.state_backend.params,
        )
        seeded_uri = await adapter.initialize_state(
            family_key=family_key,
            spec=resolved,
            backend=state_backend,
            state_uri=empty_uri,
            params=resolved.adapter.params,
        )
        sequence = sequencer.sequence(
            family_key, family_tasks, resolved.sequencer.params,
        )
        seeds.append(FamilySeed(
            family_key=family_key,
            task_sequence=list(sequence),
            state_uri=seeded_uri,
        ))
    return seeds
