"""Unit tests for the loom_service family-run seed helper (#672 PR-3).

The helper wires the loom.family_run resolver + seeder into the batches
route without touching Postgres. Tests exercise the plumbing with an
in-memory session fake and a stub state backend so the logic can be
verified without standing up MinIO.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any
from uuid import UUID, uuid4

import pytest

from loom.family_run.spec import FamilyRunSpec, PluginRef
from loom_service.family_run_seed import prepare_family_run_state


class _FakeStateBackend:
    async def initialize(
        self,
        *,
        batch_id: UUID,
        family_key: str,
        params: dict[str, Any],
    ) -> str:
        return f"s3://fake/family-state/{batch_id}/{family_key}/state.tar.gz"


@dataclass
class _FakeSession:
    """Records execute() calls so tests can assert the insert shape."""

    inserted: list[dict[str, Any]] = field(default_factory=list)

    async def execute(self, stmt: Any, params: Any = None) -> Any:
        # The helper only executes a single insert(BatchFamilyState).
        if params is not None:
            if isinstance(params, list):
                self.inserted.extend(params)
            else:
                self.inserted.append(params)

        class _Result:
            def all(self) -> list:
                return []
        return _Result()


@dataclass
class _FakeTask:
    id: str
    tags: dict[str, str]


def _default_override() -> FamilyRunSpec:
    return FamilyRunSpec(
        enabled=True,
        family_key_extractor=PluginRef(name="instance_id_prefix", params={"depth": 1}),
        sequencer=PluginRef(name="submitted_order"),
        advance_predicate=PluginRef(name="always_on_terminal"),
        adapter=PluginRef(name="noop"),
        failure_policy=PluginRef(name="skip_and_advance"),
        state_backend=PluginRef(name="s3_artifacts"),
        mount_path="/root/.skills",
    )


@pytest.mark.asyncio
async def test_seed_helper_returns_none_when_family_run_disabled() -> None:
    """No opt-in from either layer → helper returns None and the batch
    stays in classic mode."""
    session = _FakeSession()
    result = await prepare_family_run_state(
        session=session,  # type: ignore[arg-type]
        batch_id=uuid4(),
        tasks=[_FakeTask(id="benchmarks/x/task-1-a", tags={})],
        catalog_default=None,
        override=None,
        state_backend=_FakeStateBackend(),
    )
    assert result is None
    assert session.inserted == []


@pytest.mark.asyncio
async def test_seed_helper_seeds_families_and_returns_mapping() -> None:
    """Full spec + two families → one batch_family_state row per family
    and the returned mapping covers every input task."""
    batch_id = uuid4()
    tasks = [
        _FakeTask(id="benchmarks/x/family-a/task-1", tags={}),
        _FakeTask(id="benchmarks/x/family-a/task-2", tags={}),
        _FakeTask(id="benchmarks/x/family-b/task-1", tags={}),
    ]
    session = _FakeSession()
    result = await prepare_family_run_state(
        session=session,  # type: ignore[arg-type]
        batch_id=batch_id,
        tasks=tasks,
        catalog_default=None,
        override=_default_override(),
        state_backend=_FakeStateBackend(),
    )
    assert result is not None
    assert result.resolved_spec.enabled is True
    # instance_id_prefix depth=1 groups by first path segment.
    assert set(result.task_id_to_family_key.values()) == {"benchmarks"}
    # Every input task is covered.
    assert set(result.task_id_to_family_key.keys()) == {t.id for t in tasks}
    # One row per family (single family with depth=1).
    assert len(session.inserted) == 1
    row = session.inserted[0]
    assert row["batch_id"] == batch_id
    assert row["state"] == "pending"
    assert row["current_index"] == 0
    assert row["state_uri"].startswith("s3://fake/family-state/")


@pytest.mark.asyncio
async def test_seed_helper_translates_partial_spec_error() -> None:
    """A partial spec (missing required roles) raises ValueError so the
    caller can surface a 400 to the client."""
    partial = FamilyRunSpec(enabled=True, adapter=PluginRef(name="noop"))
    with pytest.raises(ValueError, match="missing required role"):
        await prepare_family_run_state(
            session=_FakeSession(),  # type: ignore[arg-type]
            batch_id=uuid4(),
            tasks=[_FakeTask(id="benchmarks/x/task-1", tags={})],
            catalog_default=None,
            override=partial,
            state_backend=_FakeStateBackend(),
        )
