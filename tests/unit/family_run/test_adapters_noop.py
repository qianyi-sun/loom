"""Noop adapter - leaves state untouched."""

from __future__ import annotations

from dataclasses import dataclass, field
from uuid import uuid4

import pytest

from loom.family_run.adapters import NoopAdapter
from loom.family_run.spec import PluginRef, ResolvedFamilyRunSpec


@dataclass
class _FakeBackend:
    calls: list[str] = field(default_factory=list)

    async def initialize(self, *, batch_id, family_key, params):
        self.calls.append("initialize")
        return f"uri://{batch_id}/{family_key}"

    async def download(self, state_uri, dst, params):
        self.calls.append(f"download:{state_uri}")

    async def upload(self, state_uri, src, params):
        self.calls.append(f"upload:{state_uri}")
        return state_uri


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


@dataclass
class _Trial:
    id: object = field(default_factory=uuid4)
    task_id: str = "t/1"
    state: str = "succeeded"
    reward: float | None = 1.0
    attempt_count: int = 1


@dataclass
class _Family:
    batch_id: object = field(default_factory=uuid4)
    family_key: str = "fam"
    task_sequence: list[str] = field(default_factory=list)
    current_index: int = 0
    attempt_count: int = 0


@pytest.mark.asyncio
async def test_noop_initialize_returns_seed_uri_unchanged():
    backend = _FakeBackend()
    adapter = NoopAdapter()
    seeded = await adapter.initialize_state(
        family_key="fam",
        spec=_spec(),
        backend=backend,
        state_uri="uri://seed",
        params={},
    )
    assert seeded == "uri://seed"
    assert backend.calls == []  # noop touches nothing


@pytest.mark.asyncio
async def test_noop_evolve_returns_input_uri():
    backend = _FakeBackend()
    adapter = NoopAdapter()
    family = _Family(task_sequence=["t/1"])
    result = await adapter.evolve(
        trial=_Trial(),
        family=family,
        state_uri="uri://before",
        backend=backend,
        params={},
    )
    assert result == "uri://before"
    assert backend.calls == []
