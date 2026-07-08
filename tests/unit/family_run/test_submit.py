"""Batch-submit helper: materialise family state."""

from __future__ import annotations

from dataclasses import dataclass
from uuid import uuid4

import pytest

from loom.family_run.spec import PluginRef, ResolvedFamilyRunSpec
from loom.family_run.state_backends import S3ArtifactsStateBackend
from loom.family_run.submit import seed_family_state
from loom.trajectory.storage import FakeObjectStore


@dataclass
class _Task:
    id: str
    tags: dict[str, str] | None = None


def _spec() -> ResolvedFamilyRunSpec:
    return ResolvedFamilyRunSpec(
        enabled=True,
        family_key_extractor=PluginRef(name="instance_id_prefix"),
        sequencer=PluginRef(name="alphabetical"),
        advance_predicate=PluginRef(name="always_on_terminal"),
        adapter=PluginRef(name="noop"),
        failure_policy=PluginRef(name="stall_family"),
        state_backend=PluginRef(name="s3_artifacts"),
    )


@pytest.mark.asyncio
async def test_seed_groups_tasks_by_extractor_and_orders_by_sequencer() -> None:
    store = FakeObjectStore()
    await store.ensure_bucket("artifacts")
    backend = S3ArtifactsStateBackend(store=store, bucket="artifacts")

    batch_id = uuid4()
    tasks = [
        _Task(id="skillflow/A/task-2"),
        _Task(id="skillflow/A/task-1"),
        _Task(id="slb/B/task-1"),
    ]
    seeds = await seed_family_state(
        batch_id=batch_id,
        tasks=tasks,
        resolved=_spec(),
        state_backend=backend,
    )

    by_key = {s.family_key: s for s in seeds}
    assert set(by_key) == {"skillflow", "slb"}
    assert by_key["skillflow"].task_sequence == [
        "skillflow/A/task-1",
        "skillflow/A/task-2",
    ]
    assert by_key["slb"].task_sequence == ["slb/B/task-1"]
    # Each family got a persisted state_uri under its own prefix.
    assert by_key["skillflow"].state_uri.startswith(
        f"s3://artifacts/family-state/{batch_id}/skillflow/",
    )
    assert by_key["slb"].state_uri.startswith(
        f"s3://artifacts/family-state/{batch_id}/slb/",
    )
