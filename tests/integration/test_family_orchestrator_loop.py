"""Testcontainer integration test for the family-run orchestrator (#672 PR-2).

Seeds a minimal team + task + batch + trials + adapting family row,
runs one orchestrator iteration with a fake adapter, and asserts the
family transitions from ``adapting`` to ``pending`` with the current
index bumped.
"""

from __future__ import annotations

import json
from typing import Any
from uuid import UUID, uuid4

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from loom.family_run.spec import PluginRef, ResolvedFamilyRunSpec
from loom_family_orchestrator.main_loop import OrchestratorContext, run_once


class _CapturingAdapter:
    def __init__(self, new_uri: str) -> None:
        self.new_uri = new_uri
        self.calls: list[dict[str, Any]] = []

    async def evolve(self, **kwargs: Any) -> str:
        self.calls.append(kwargs)
        return self.new_uri


class _NullBackend:
    async def download(self, uri: str, dst: Any, params: Any) -> None:
        pass

    async def upload(self, uri: str, src: Any, params: Any) -> str:
        return uri


def _spec() -> ResolvedFamilyRunSpec:
    return ResolvedFamilyRunSpec(
        enabled=True,
        family_key_extractor=PluginRef(name="instance_id_prefix"),
        sequencer=PluginRef(name="alphabetical"),
        advance_predicate=PluginRef(name="always_on_terminal"),
        adapter=PluginRef(name="noop"),  # replaced by monkeypatch below
        failure_policy=PluginRef(name="stall_family"),
        state_backend=PluginRef(name="s3_artifacts"),
    )


async def _seed_minimal_batch(
    session: Any,
    *,
    team_id: UUID,
    batch_id: UUID,
    task_ids: list[str],
    family_key: str,
    trial_ids: list[UUID],
) -> None:
    """Seed team + tasks + batch + trials + family row via raw SQL.

    Kept minimal on purpose: only the columns the orchestrator's
    queries touch plus foreign-key constraints must be satisfied.
    """
    await session.execute(text("""
        INSERT INTO teams (id, name, slug)
        VALUES (:tid, 'test-team', 'test-team')
    """), {"tid": team_id})
    for tid in task_ids:
        await session.execute(text("""
            INSERT INTO tasks (id, config, source_checksum)
            VALUES (:tid, '{}'::jsonb, 'sha256:test')
        """), {"tid": tid})
    await session.execute(text("""
        INSERT INTO batches (
            id, team_id, name, task_filter, trial_config,
            state, created_by_token_prefix, family_run_spec
        )
        VALUES (
            :bid, :tid, 'test-batch', '{}'::jsonb, '{}'::jsonb,
            'running', 'test', (:spec)::jsonb
        )
    """), {
        "bid": batch_id,
        "tid": team_id,
        "spec": json.dumps(_spec().model_dump()),
    })
    for trial_id, task_id in zip(trial_ids, task_ids, strict=True):
        await session.execute(text("""
            INSERT INTO trials (
                id, team_id, task_id, config, requires_caps,
                state, submit_priority, batch_id, family_key,
                result, finished_at, attempt_count
            )
            VALUES (
                :tid, :team, :task, '{}'::jsonb, '{}'::jsonb,
                'succeeded', 100, :bid, :fam,
                :result, NOW(), 1
            )
        """), {
            "tid": trial_id,
            "team": team_id,
            "task": task_id,
            "bid": batch_id,
            "fam": family_key,
            "result": json.dumps({"reward": 1.0}),
        })


@pytest.mark.asyncio
async def test_orchestrator_iteration_transitions_adapting_to_pending(
    postgres_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    engine = create_async_engine(postgres_url)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)

    team_id = uuid4()
    batch_id = uuid4()
    family_key = "familyA"
    task_ids = [f"{family_key}/task1", f"{family_key}/task2"]
    trial_ids = [uuid4(), uuid4()]

    async with session_factory() as session:
        await _seed_minimal_batch(
            session,
            team_id=team_id,
            batch_id=batch_id,
            task_ids=task_ids,
            family_key=family_key,
            trial_ids=trial_ids,
        )
        await session.execute(text("""
            INSERT INTO batch_family_state (
                batch_id, family_key, task_sequence, current_index,
                state, state_uri, attempt_count
            )
            VALUES (
                :bid, :fam, :seq, 0, 'adapting', 'uri://v1', 0
            )
        """), {
            "bid": batch_id,
            "fam": family_key,
            "seq": task_ids,
        })
        await session.commit()

    adapter = _CapturingAdapter(new_uri="uri://v2")
    # Swap resolve_plugin so the "noop"-named adapter yields our capture,
    # and any state backend request returns a stub.
    from loom.family_run import registry as reg

    def _fake_resolve(group: str, ref: PluginRef) -> Any:
        if group == "loom.family.adapters":
            return adapter
        return reg.resolve_plugin(group, ref)

    monkeypatch.setattr(
        "loom_family_orchestrator.main_loop.resolve_plugin", _fake_resolve,
    )

    ctx = OrchestratorContext(
        session_factory=session_factory,
        gateway=object(),
        object_store=None,
        artifacts_bucket="artifacts",
        state_backend_factory=(lambda spec: _NullBackend()),
        settings_default_model="anthropic/claude-sonnet-4-6",
        adapter_call_timeout_sec=10.0,
        poll_sec=0.01,
    )

    picked = await run_once(ctx)
    assert picked is True
    assert adapter.calls, "adapter.evolve was not called"

    async with session_factory() as session:
        row = (await session.execute(text("""
            SELECT state, current_index, state_uri, last_error
              FROM batch_family_state
             WHERE batch_id = :bid AND family_key = :fam
        """), {"bid": batch_id, "fam": family_key})).mappings().one()
        assert row["state"] == "pending"
        assert row["current_index"] == 1
        assert row["state_uri"] == "uri://v2"
        assert row["last_error"] is None
    await engine.dispose()


@pytest.mark.asyncio
async def test_orchestrator_end_of_sequence_transitions_to_done(
    postgres_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    engine = create_async_engine(postgres_url)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)

    team_id = uuid4()
    batch_id = uuid4()
    family_key = "familyB"
    task_ids = [f"{family_key}/task1", f"{family_key}/task2"]
    trial_ids = [uuid4(), uuid4()]

    async with session_factory() as session:
        await _seed_minimal_batch(
            session,
            team_id=team_id,
            batch_id=batch_id,
            task_ids=task_ids,
            family_key=family_key,
            trial_ids=trial_ids,
        )
        await session.execute(text("""
            INSERT INTO batch_family_state (
                batch_id, family_key, task_sequence, current_index,
                state, state_uri, attempt_count
            )
            VALUES (
                :bid, :fam, :seq, 1, 'adapting', 'uri://v1', 0
            )
        """), {
            "bid": batch_id,
            "fam": family_key,
            "seq": task_ids,
        })
        await session.commit()

    adapter = _CapturingAdapter(new_uri="uri://final")
    from loom.family_run import registry as reg
    monkeypatch.setattr(
        "loom_family_orchestrator.main_loop.resolve_plugin",
        lambda group, ref: adapter if group == "loom.family.adapters" else reg.resolve_plugin(group, ref),
    )

    ctx = OrchestratorContext(
        session_factory=session_factory,
        gateway=object(),
        object_store=None,
        artifacts_bucket="artifacts",
        state_backend_factory=(lambda spec: _NullBackend()),
        settings_default_model="anthropic/claude-sonnet-4-6",
        adapter_call_timeout_sec=10.0,
        poll_sec=0.01,
    )

    picked = await run_once(ctx)
    assert picked is True

    async with session_factory() as session:
        row = (await session.execute(text("""
            SELECT state, current_index FROM batch_family_state
             WHERE batch_id = :bid AND family_key = :fam
        """), {"bid": batch_id, "fam": family_key})).mappings().one()
        assert row["state"] == "done"
        assert row["current_index"] == 2
    await engine.dispose()
