"""Unit tests for the family-run orchestrator loop (#672 PR-2).

Exercises the success + all three failure-action paths against a
fake DB session that records SQL executes verbatim. The pure state
machine already has coverage in tests/unit/family_run/; here we prove
the orchestrator translates adapter results into the right SQL.
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import Any
from uuid import uuid4

import pytest

from loom.family_run.spec import FailureAction, PluginRef, ResolvedFamilyRunSpec
from loom_family_orchestrator.main_loop import OrchestratorContext, run_once


def _spec(
    *,
    adapter: str = "noop",
    failure_policy: str = "stall_family",
    adapter_params: dict[str, Any] | None = None,
    failure_params: dict[str, Any] | None = None,
) -> ResolvedFamilyRunSpec:
    return ResolvedFamilyRunSpec(
        enabled=True,
        family_key_extractor=PluginRef(name="instance_id_prefix"),
        sequencer=PluginRef(name="alphabetical"),
        advance_predicate=PluginRef(name="always_on_terminal"),
        adapter=PluginRef(name=adapter, params=adapter_params or {}),
        failure_policy=PluginRef(name=failure_policy, params=failure_params or {}),
        state_backend=PluginRef(name="s3_artifacts"),
    )


@dataclass
class _FakeResult:
    rows: list[dict[str, Any]]

    def mappings(self) -> _FakeResult:
        return self

    def one_or_none(self) -> dict[str, Any] | None:
        return self.rows[0] if self.rows else None


@dataclass
class _FakeSession:
    """Records execute() calls and returns programmed rows.

    ``responses`` is a list of (predicate, rows) pairs; the first
    predicate that matches the SQL string wins. Anything not matched
    returns an empty result.
    """

    responses: list[tuple[Any, list[dict[str, Any]]]] = field(default_factory=list)
    executed: list[tuple[str, dict[str, Any] | None]] = field(default_factory=list)
    committed: bool = False

    async def execute(self, sql: Any, params: dict[str, Any] | None = None) -> _FakeResult:
        sql_text = str(sql).strip()
        self.executed.append((sql_text, params))
        for predicate, rows in self.responses:
            if predicate in sql_text:
                return _FakeResult(rows=rows)
        return _FakeResult(rows=[])

    async def commit(self) -> None:
        self.committed = True


def _factory(session: _FakeSession) -> Any:
    @asynccontextmanager
    async def _make():
        yield session
    return _make


class _AdapterOK:
    def __init__(self, new_uri: str = "uri://after") -> None:
        self.new_uri = new_uri
        self.called = 0

    async def evolve(self, **kwargs: Any) -> str:
        self.called += 1
        return self.new_uri


class _AdapterBoom:
    async def evolve(self, **kwargs: Any) -> str:
        raise RuntimeError("adapter blew up")


class _StubBackend:
    def __init__(self) -> None:
        self.downloads: list[Any] = []
        self.uploads: list[Any] = []

    async def download(self, uri: str, dst: Any, params: Any) -> None:
        self.downloads.append(uri)

    async def upload(self, uri: str, src: Any, params: Any) -> str:
        self.uploads.append(src)
        return uri


def _ctx(
    session: _FakeSession,
    *,
    adapter: Any,
    backend: Any = None,
) -> OrchestratorContext:
    return OrchestratorContext(
        session_factory=_factory(session),
        gateway=object(),
        object_store=None,
        artifacts_bucket="artifacts",
        state_backend_factory=(lambda spec: backend or _StubBackend()),
        settings_default_model="anthropic/claude-sonnet-4-6",
        adapter_call_timeout_sec=10.0,
        poll_sec=0.01,
    )


# ─── success path ─────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_run_once_empty_queue_returns_false(monkeypatch):
    session = _FakeSession()
    ctx = _ctx(session, adapter=_AdapterOK())
    result = await run_once(ctx)
    assert result is False
    assert session.committed  # empty poll still commits to release lock


@pytest.mark.asyncio
async def test_run_once_success_bumps_index_to_pending(monkeypatch):
    from loom.family_run import registry
    batch_id = uuid4()
    trial_id = uuid4()
    adapter = _AdapterOK(new_uri="uri://v2")
    # Monkeypatch adapter resolution so the "noop"-named plugin
    # returns our capturing adapter instead of the real NoopAdapter.
    orig_resolve = registry.resolve_plugin

    def fake_resolve(group: str, ref: PluginRef) -> Any:
        if group == "loom.family.adapters":
            return adapter
        return orig_resolve(group, ref)

    monkeypatch.setattr(registry, "resolve_plugin", fake_resolve)
    monkeypatch.setattr(
        "loom_family_orchestrator.main_loop.resolve_plugin", fake_resolve,
    )

    session = _FakeSession(responses=[
        ("FROM batch_family_state", [{
            "batch_id": batch_id,
            "family_key": "fam",
            "task_sequence": ["a", "b", "c"],
            "current_index": 0,
            "state_uri": "uri://v1",
            "attempt_count": 0,
            "next_attempt_at": None,
        }]),
        ("FROM batches", [{
            "family_run_spec": _spec().model_dump(),
        }]),
        ("FROM trials", [{
            "id": trial_id,
            "task_id": "a",
            "state": "succeeded",
            "result": {"reward": 1.0},
            "attempt_count": 1,
            "trajectory_uri": None,
        }]),
    ])
    ctx = _ctx(session, adapter=adapter)
    result = await run_once(ctx)
    assert result is True
    assert adapter.called == 1
    # Verify the UPDATE with new_state=pending, new_current_index=1
    updates = [(s, p) for s, p in session.executed if "UPDATE batch_family_state" in s]
    assert updates, "expected an UPDATE batch_family_state"
    params = updates[-1][1] or {}
    assert params["new_state"] == "pending"
    assert params["new_current_index"] == 1
    assert params["new_state_uri"] == "uri://v2"


@pytest.mark.asyncio
async def test_run_once_success_end_of_sequence_marks_done(monkeypatch):
    from loom.family_run import registry
    batch_id = uuid4()
    trial_id = uuid4()
    adapter = _AdapterOK()
    monkeypatch.setattr(
        "loom_family_orchestrator.main_loop.resolve_plugin",
        lambda group, ref: adapter if group == "loom.family.adapters" else registry.resolve_plugin(group, ref),
    )

    session = _FakeSession(responses=[
        ("FROM batch_family_state", [{
            "batch_id": batch_id,
            "family_key": "fam",
            "task_sequence": ["a", "b"],
            "current_index": 1,  # last position
            "state_uri": "uri://v1",
            "attempt_count": 0,
            "next_attempt_at": None,
        }]),
        ("FROM batches", [{"family_run_spec": _spec().model_dump()}]),
        ("FROM trials", [{
            "id": trial_id,
            "task_id": "b",
            "state": "succeeded",
            "result": None,
            "attempt_count": 1,
            "trajectory_uri": None,
        }]),
    ])
    ctx = _ctx(session, adapter=adapter)
    await run_once(ctx)
    updates = [(s, p) for s, p in session.executed if "UPDATE batch_family_state" in s]
    assert updates[-1][1]["new_state"] == "done"
    assert updates[-1][1]["new_current_index"] == 2


# ─── failure paths ─────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_run_once_stall_family_retry_writes_backoff(monkeypatch):
    from loom.family_run import registry
    batch_id = uuid4()
    trial_id = uuid4()
    adapter = _AdapterBoom()

    def _fake(group: str, ref: PluginRef) -> Any:
        if group == "loom.family.adapters":
            return adapter
        return registry.resolve_plugin(group, ref)

    monkeypatch.setattr("loom_family_orchestrator.main_loop.resolve_plugin", _fake)

    spec = _spec(
        failure_policy="stall_family",
        failure_params={"max_retries": 3, "backoff_sec": 60.0},
    )
    session = _FakeSession(responses=[
        ("FROM batch_family_state", [{
            "batch_id": batch_id,
            "family_key": "fam",
            "task_sequence": ["a", "b"],
            "current_index": 0,
            "state_uri": "uri://v1",
            "attempt_count": 0,  # first failure -> retry with backoff
            "next_attempt_at": None,
        }]),
        ("FROM batches", [{"family_run_spec": spec.model_dump()}]),
        ("FROM trials", [{
            "id": trial_id, "task_id": "a", "state": "succeeded",
            "result": None, "attempt_count": 1, "trajectory_uri": None,
        }]),
    ])
    ctx = _ctx(session, adapter=adapter)
    await run_once(ctx)
    retry_updates = [
        (s, p) for s, p in session.executed
        if "UPDATE batch_family_state" in s and "next_attempt_at" in s and "attempt_count + 1" in s
    ]
    assert retry_updates
    params = retry_updates[-1][1] or {}
    assert params["next_attempt_at"] is not None
    assert "adapter blew up" in params["last_error"]


@pytest.mark.asyncio
async def test_run_once_stall_family_exhausted_stalls(monkeypatch):
    from loom.family_run import registry
    batch_id = uuid4()
    trial_id = uuid4()
    adapter = _AdapterBoom()
    monkeypatch.setattr(
        "loom_family_orchestrator.main_loop.resolve_plugin",
        lambda group, ref: adapter if group == "loom.family.adapters" else registry.resolve_plugin(group, ref),
    )
    spec = _spec(
        failure_policy="stall_family",
        failure_params={"max_retries": 2, "backoff_sec": 60.0},
    )
    session = _FakeSession(responses=[
        ("FROM batch_family_state", [{
            "batch_id": batch_id,
            "family_key": "fam",
            "task_sequence": ["a"],
            "current_index": 0,
            "state_uri": "uri://v1",
            "attempt_count": 2,  # already at max -> policy returns abort
            "next_attempt_at": None,
        }]),
        ("FROM batches", [{"family_run_spec": spec.model_dump()}]),
        ("FROM trials", [{
            "id": trial_id, "task_id": "a", "state": "succeeded",
            "result": None, "attempt_count": 1, "trajectory_uri": None,
        }]),
    ])
    ctx = _ctx(session, adapter=adapter)
    await run_once(ctx)
    stalled = [
        (s, p) for s, p in session.executed
        if "UPDATE batch_family_state" in s and "'stalled'" in s
    ]
    assert stalled, f"expected stalled UPDATE, got {session.executed}"


@pytest.mark.asyncio
async def test_run_once_skip_and_advance_bumps_index(monkeypatch):
    from loom.family_run import registry
    batch_id = uuid4()
    trial_id = uuid4()
    adapter = _AdapterBoom()
    monkeypatch.setattr(
        "loom_family_orchestrator.main_loop.resolve_plugin",
        lambda group, ref: adapter if group == "loom.family.adapters" else registry.resolve_plugin(group, ref),
    )
    spec = _spec(failure_policy="skip_and_advance")
    session = _FakeSession(responses=[
        ("FROM batch_family_state", [{
            "batch_id": batch_id,
            "family_key": "fam",
            "task_sequence": ["a", "b", "c"],
            "current_index": 1,
            "state_uri": "uri://v1",
            "attempt_count": 0,
            "next_attempt_at": None,
        }]),
        ("FROM batches", [{"family_run_spec": spec.model_dump()}]),
        ("FROM trials", [{
            "id": trial_id, "task_id": "b", "state": "succeeded",
            "result": None, "attempt_count": 1, "trajectory_uri": None,
        }]),
    ])
    ctx = _ctx(session, adapter=adapter)
    await run_once(ctx)
    # skip_and_advance uses the success SQL variant (state -> pending, index bumped).
    updates = [
        (s, p) for s, p in session.executed
        if "UPDATE batch_family_state" in s and "new_current_index" in s
    ]
    assert updates
    params = updates[-1][1] or {}
    assert params["new_state"] == "pending"
    assert params["new_current_index"] == 2


@pytest.mark.asyncio
async def test_run_once_abort_family_marks_aborted_and_cancels_queued(monkeypatch):
    from loom.family_run import registry
    batch_id = uuid4()
    trial_id = uuid4()
    adapter = _AdapterBoom()
    monkeypatch.setattr(
        "loom_family_orchestrator.main_loop.resolve_plugin",
        lambda group, ref: adapter if group == "loom.family.adapters" else registry.resolve_plugin(group, ref),
    )
    spec = _spec(failure_policy="abort_family")
    session = _FakeSession(responses=[
        ("FROM batch_family_state", [{
            "batch_id": batch_id,
            "family_key": "fam",
            "task_sequence": ["a", "b"],
            "current_index": 0,
            "state_uri": "uri://v1",
            "attempt_count": 0,
            "next_attempt_at": None,
        }]),
        ("FROM batches", [{"family_run_spec": spec.model_dump()}]),
        ("FROM trials", [{
            "id": trial_id, "task_id": "a", "state": "succeeded",
            "result": None, "attempt_count": 1, "trajectory_uri": None,
        }]),
    ])
    ctx = _ctx(session, adapter=adapter)
    await run_once(ctx)
    aborted = [
        (s, p) for s, p in session.executed
        if "UPDATE batch_family_state" in s and "'aborted'" in s
    ]
    cancelled = [
        (s, p) for s, p in session.executed
        if "UPDATE trials" in s and "'cancelled'" in s
    ]
    assert aborted
    assert cancelled


@pytest.mark.asyncio
async def test_run_once_missing_trial_stalls_family(monkeypatch):
    from loom.family_run import registry
    batch_id = uuid4()
    monkeypatch.setattr(
        "loom_family_orchestrator.main_loop.resolve_plugin",
        registry.resolve_plugin,
    )
    session = _FakeSession(responses=[
        ("FROM batch_family_state", [{
            "batch_id": batch_id,
            "family_key": "fam",
            "task_sequence": ["a"],
            "current_index": 0,
            "state_uri": "uri://v1",
            "attempt_count": 0,
            "next_attempt_at": None,
        }]),
        ("FROM batches", [{"family_run_spec": _spec().model_dump()}]),
        # no trials row
    ])
    ctx = _ctx(session, adapter=_AdapterOK())
    await run_once(ctx)
    stalled = [
        (s, p) for s, p in session.executed
        if "UPDATE batch_family_state" in s and "'stalled'" in s
    ]
    assert stalled


@pytest.mark.asyncio
async def test_run_once_missing_batch_spec_stalls(monkeypatch):
    from loom.family_run import registry
    batch_id = uuid4()
    monkeypatch.setattr(
        "loom_family_orchestrator.main_loop.resolve_plugin",
        registry.resolve_plugin,
    )
    session = _FakeSession(responses=[
        ("FROM batch_family_state", [{
            "batch_id": batch_id,
            "family_key": "fam",
            "task_sequence": ["a"],
            "current_index": 0,
            "state_uri": "uri://v1",
            "attempt_count": 0,
            "next_attempt_at": None,
        }]),
        ("FROM batches", [{"family_run_spec": None}]),
    ])
    ctx = _ctx(session, adapter=_AdapterOK())
    await run_once(ctx)
    stalled = [
        (s, p) for s, p in session.executed
        if "UPDATE batch_family_state" in s and "'stalled'" in s
    ]
    assert stalled


def test_failure_action_kind_dispatch_covers_all_paths():
    # Belt-and-braces guard that the FailureAction shape is exhaustive.
    kinds = {"retry_with_backoff", "skip_and_advance", "abort_family"}
    for k in kinds:
        FailureAction(kind=k, backoff_sec=1.0 if k == "retry_with_backoff" else None)
