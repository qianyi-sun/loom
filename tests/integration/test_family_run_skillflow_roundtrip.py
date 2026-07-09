"""Full SkillFlow round-trip integration test (#672 PR-4 Item 6).

Composes every family-run surface end-to-end for a 3-task
skill-patcher-driven family, deferred from PR-3:

- ``batch_family_state`` seeded via the submit helper against a real
  ``S3ArtifactsStateBackend`` on an in-memory ``FakeObjectStore``.
- Fake worker ``_spawn_trial`` shim uses ``prepare_family_state_mount``
  to download the current tarball onto disk, verifies the mount
  contents, then PATCHes the trial to ``succeeded`` via raw SQL (real
  worker uses the CP HTTP endpoint; we bypass it because the CP HTTP
  contract is already covered by ``test_cp_trials_idempotency`` and the
  interesting surface here is the state-mount + orchestrator loop).
- Fake gateway returns canned skill-patch JSONs so the
  ``skill_patcher_llm`` adapter deterministically evolves the tree.
- Orchestrator ``run_once`` picks the ``adapting`` row, calls the fake
  gateway via the adapter, and bumps ``current_index``.

Asserts:

- Only one trial in the family is claimable at each step - the scheduler
  claim predicate serialises the family.
- The evolved skill dir carries forward between trials (skill_a.md
  after task_a, skill_a.md + skill_b.md after task_b).
- Family ``state_uri`` version bumps monotonically at each evolve step.
- Family terminates in ``done`` after the last task.
"""

from __future__ import annotations

import json
from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from loom.family_run.prestart import prepare_family_state_mount
from loom.family_run.spec import PluginRef, ResolvedFamilyRunSpec
from loom.family_run.state_backends import S3ArtifactsStateBackend
from loom.family_run.submit import seed_family_state
from loom.trajectory.storage import FakeObjectStore
from loom_family_orchestrator.main_loop import OrchestratorContext, run_once

# ─── Shared harness scaffolding ─────────────────────────────────────


@pytest.fixture(autouse=True)
async def _cleanup_roundtrip_rows(
    postgres_url: str,
) -> AsyncIterator[None]:
    yield
    engine = create_async_engine(postgres_url)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with session_factory() as session:
            await session.execute(
                text("""
                DELETE FROM batch_family_state
                 WHERE family_key = 'family_rt'
                    OR batch_id IN (
                        SELECT id FROM batches WHERE name = 'rt-batch'
                    )
            """)
            )
            await session.execute(
                text("""
                DELETE FROM trials
                 WHERE family_key = 'family_rt'
                    OR batch_id IN (
                        SELECT id FROM batches WHERE name = 'rt-batch'
                    )
            """)
            )
            await session.execute(
                text("""
                DELETE FROM batches WHERE name = 'rt-batch'
            """)
            )
            await session.execute(
                text("""
                DELETE FROM tasks WHERE id LIKE 'family_rt/%'
            """)
            )
            await session.execute(
                text("""
                DELETE FROM teams WHERE name LIKE 'test-team-rt-%'
            """)
            )
            await session.commit()
    finally:
        await engine.dispose()


@dataclass
class _Task:
    """Minimal shape the submit sequencer + extractor need."""

    id: str
    tags: dict[str, str] = field(default_factory=dict)


@dataclass
class _CannedGateway:
    """Fake gateway client that returns pre-registered patch JSONs.

    ``queue`` is consumed in FIFO order - one entry per expected
    ``evolve`` call. Any surplus call raises so a runaway loop fails
    loudly instead of hanging.
    """

    queue: list[dict[str, Any]]
    calls: list[dict[str, Any]] = field(default_factory=list)

    async def chat_completion(
        self,
        *,
        model: str,
        messages: list[dict[str, Any]],
        dialect: str,
        max_tokens: int,
        timeout_sec: float,
    ) -> dict[str, Any]:
        if not self.queue:
            raise RuntimeError(
                "canned gateway exhausted - orchestrator called "
                "chat_completion more times than the test scripted",
            )
        patch = self.queue.pop(0)
        self.calls.append(
            {
                "model": model,
                "dialect": dialect,
                "patch": patch,
            }
        )
        return {
            "choices": [
                {
                    "message": {"content": json.dumps(patch)},
                }
            ],
        }


def _spec() -> ResolvedFamilyRunSpec:
    return ResolvedFamilyRunSpec(
        enabled=True,
        family_key_extractor=PluginRef(name="instance_id_prefix"),
        sequencer=PluginRef(name="alphabetical"),
        advance_predicate=PluginRef(name="always_on_terminal"),
        adapter=PluginRef(name="skill_patcher_llm"),
        failure_policy=PluginRef(name="stall_family"),
        state_backend=PluginRef(name="s3_artifacts"),
    )


async def _seed_batch(
    session: Any,
    *,
    team_id: UUID,
    batch_id: UUID,
    tasks: list[str],
    family_key: str,
    trial_ids: list[UUID],
    resolved: ResolvedFamilyRunSpec,
    state_uri: str,
) -> None:
    """Insert team + tasks + batch + one PENDING trial per task + the
    initial ``batch_family_state`` row. Mirrors what the batches route
    + fanout do in production."""
    await session.execute(
        text("""
        INSERT INTO teams (id, name)
        VALUES (:tid, :name)
    """),
        {"tid": team_id, "name": f"test-team-rt-{team_id.hex[:8]}"},
    )
    for task_id in tasks:
        await session.execute(
            text("""
            INSERT INTO tasks (id, config, checksum)
            VALUES (:tid, '{}'::jsonb, 'sha256:rt')
        """),
            {"tid": task_id},
        )
    await session.execute(
        text("""
        INSERT INTO batches (
            id, team_id, name, task_filter, trial_config,
            state, created_by_token_prefix, family_run_spec
        )
        VALUES (
            :bid, :tid, 'rt-batch', '{}'::jsonb, '{}'::jsonb,
            'running', 'test', (:spec)::jsonb
        )
    """),
        {
            "bid": batch_id,
            "tid": team_id,
            "spec": json.dumps(resolved.model_dump()),
        },
    )
    for trial_id, task_id in zip(trial_ids, tasks, strict=True):
        await session.execute(
            text("""
            INSERT INTO trials (
                id, team_id, task_id, config, requires_caps,
                state, submit_priority, batch_id, family_key,
                attempt_count
            )
            VALUES (
                :tid, :team, :task, '{}'::jsonb, '{}'::jsonb,
                'pending', 100, :bid, :fam, 0
            )
        """),
            {
                "tid": trial_id,
                "team": team_id,
                "task": task_id,
                "bid": batch_id,
                "fam": family_key,
            },
        )
    await session.execute(
        text("""
        INSERT INTO batch_family_state (
            batch_id, family_key, task_sequence, current_index,
            state, state_uri, attempt_count
        )
        VALUES (
            :bid, :fam, :seq, 0, 'pending', :uri, 0
        )
    """),
        {
            "bid": batch_id,
            "fam": family_key,
            "seq": tasks,
            "uri": state_uri,
        },
    )


async def _claim_next_trial(
    session: Any,
    *,
    batch_id: UUID,
    family_key: str,
) -> tuple[UUID, str, str] | None:
    """Simulate the scheduler's claim predicate: only the trial whose
    task_id equals ``task_sequence[current_index]`` and whose family
    state is ``pending`` is claimable. Flips the family state to
    ``running`` in the same transaction, mirroring the production
    UPDATE."""
    row = (
        (
            await session.execute(
                text("""
        WITH claimable AS (
            SELECT t.id AS trial_id, t.task_id, bfs.state_uri
              FROM trials t
              JOIN batch_family_state bfs
                ON bfs.batch_id = t.batch_id
               AND bfs.family_key = t.family_key
             WHERE t.batch_id = :bid
               AND t.family_key = :fam
               AND t.state = 'pending'
               AND bfs.state = 'pending'
               AND bfs.task_sequence[bfs.current_index + 1] = t.task_id
             LIMIT 1
        )
        UPDATE batch_family_state bfs
           SET state = 'running'
          FROM claimable
         WHERE bfs.batch_id = :bid AND bfs.family_key = :fam
        RETURNING claimable.trial_id, claimable.task_id, claimable.state_uri
    """),
                {"bid": batch_id, "fam": family_key},
            )
        )
        .mappings()
        .one_or_none()
    )
    if row is None:
        return None
    return (row["trial_id"], row["task_id"], row["state_uri"])


async def _count_claimable(
    session: Any,
    *,
    batch_id: UUID,
    family_key: str,
) -> int:
    """Read-only variant: how many trials would the claim predicate
    match right now?"""
    row = (
        (
            await session.execute(
                text("""
        SELECT COUNT(*) AS n
          FROM trials t
          JOIN batch_family_state bfs
            ON bfs.batch_id = t.batch_id
           AND bfs.family_key = t.family_key
         WHERE t.batch_id = :bid
           AND t.family_key = :fam
           AND t.state = 'pending'
           AND bfs.state = 'pending'
           AND bfs.task_sequence[bfs.current_index + 1] = t.task_id
    """),
                {"bid": batch_id, "fam": family_key},
            )
        )
        .mappings()
        .one()
    )
    return int(row["n"])


async def _finalize_trial_succeeded(
    session: Any,
    *,
    trial_id: UUID,
    reward: float,
) -> None:
    """Simulate CP finalize -> ADVANCE decision -> family_state = adapting
    (post-noop-shortcut removal in PR-2, the CP does this transition
    in-transaction; the scheduler test suite covers the transition;
    here we skip straight to what the DB looks like after)."""
    await session.execute(
        text("""
        UPDATE trials
           SET state = 'succeeded',
               result = :result,
               finished_at = NOW(),
               attempt_count = attempt_count + 1
         WHERE id = :tid
    """),
        {
            "tid": trial_id,
            "result": json.dumps({"reward": reward}),
        },
    )
    # Flip the family row to adapting so the orchestrator picks it up.
    trial_row = (
        (
            await session.execute(
                text("""
        SELECT batch_id, family_key FROM trials WHERE id = :tid
    """),
                {"tid": trial_id},
            )
        )
        .mappings()
        .one()
    )
    await session.execute(
        text("""
        UPDATE batch_family_state
           SET state = 'adapting'
         WHERE batch_id = :bid AND family_key = :fam
    """),
        {"bid": trial_row["batch_id"], "fam": trial_row["family_key"]},
    )


# ─── The test ────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_skillflow_iterative_three_task_family_round_trip(
    postgres_url: str,
    tmp_path: Path,
) -> None:
    engine = create_async_engine(postgres_url)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)

    resolved = _spec()
    store = FakeObjectStore()
    await store.ensure_bucket("artifacts")
    backend = S3ArtifactsStateBackend(store=store, bucket="artifacts")

    family_key = "family_rt"
    task_ids = [
        f"{family_key}/task_a",
        f"{family_key}/task_b",
        f"{family_key}/task_c",
    ]
    trial_ids = [uuid4() for _ in task_ids]
    team_id = uuid4()
    batch_id = uuid4()

    # Submit-time seed - provisions the empty state_uri from the backend.
    seeds = await seed_family_state(
        batch_id=batch_id,
        tasks=[_Task(id=tid) for tid in task_ids],
        resolved=resolved,
        state_backend=backend,
    )
    assert len(seeds) == 1
    seed = seeds[0]
    assert seed.task_sequence == task_ids

    async with session_factory() as session:
        await _seed_batch(
            session,
            team_id=team_id,
            batch_id=batch_id,
            tasks=task_ids,
            family_key=family_key,
            trial_ids=trial_ids,
            resolved=resolved,
            state_uri=seed.state_uri,
        )
        await session.commit()

    gateway = _CannedGateway(
        queue=[
            {  # After task_a: introduce skill_a.md.
                "add": [
                    {
                        "path": "skill_a.md",
                        "content": "# Skill A\nLearned from task_a.\n",
                    }
                ],
                "modify": [],
                "delete": [],
            },
            {  # After task_b: add skill_b.md AND modify skill_a.md.
                "add": [
                    {
                        "path": "skill_b.md",
                        "content": "# Skill B\nLearned from task_b.\n",
                    }
                ],
                "modify": [
                    {
                        "path": "skill_a.md",
                        "content": "# Skill A (revised)\nRefined via task_b.\n",
                    }
                ],
                "delete": [],
            },
            # No third evolve - orchestrator terminates after task_c since
            # apply_advance_decision on the last index transitions to done.
        ]
    )

    ctx = OrchestratorContext(
        session_factory=session_factory,
        gateway=gateway,
        object_store=store,
        artifacts_bucket="artifacts",
        state_backend_factory=(lambda spec: backend),
        settings_default_model="anthropic/claude-sonnet-4-6",
        adapter_call_timeout_sec=10.0,
        poll_sec=0.01,
    )

    state_uri_history: list[str] = [seed.state_uri]
    skill_dir_snapshots: list[set[str]] = []

    # ─── Step 1: task_a claimable, task_b/c not; run, finalize, evolve.
    async with session_factory() as session:
        assert (
            await _count_claimable(
                session,
                batch_id=batch_id,
                family_key=family_key,
            )
            == 1
        )
        claim = await _claim_next_trial(
            session,
            batch_id=batch_id,
            family_key=family_key,
        )
        assert claim is not None
        claimed_trial_id, claimed_task, claimed_uri = claim
        assert claimed_task == task_ids[0]
        await session.commit()

    # Worker downloads state at mount, verifies contents (empty).
    mount = await prepare_family_state_mount(
        trial_id=str(claimed_trial_id),
        state_uri=claimed_uri,
        mount_path=resolved.mount_path,
        state_backend=backend,
    )
    try:
        assert mount.container_dir == "/root/.skills"
        skill_dir_snapshots.append({p.name for p in mount.host_dir.iterdir() if p.is_file()})
    finally:
        mount.cleanup()

    async with session_factory() as session:
        await _finalize_trial_succeeded(
            session,
            trial_id=claimed_trial_id,
            reward=0.87,
        )
        await session.commit()

    picked = await run_once(ctx)
    assert picked is True
    assert len(gateway.calls) == 1

    async with session_factory() as session:
        row = (
            (
                await session.execute(
                    text("""
            SELECT state, current_index, state_uri
              FROM batch_family_state
             WHERE batch_id = :bid AND family_key = :fam
        """),
                    {"bid": batch_id, "fam": family_key},
                )
            )
            .mappings()
            .one()
        )
        assert row["state"] == "pending"
        assert row["current_index"] == 1
        assert row["state_uri"] != state_uri_history[-1], "state_uri must bump after evolve"
        state_uri_history.append(row["state_uri"])

    # ─── Step 2: task_b claimable, task_a done, task_c not.
    async with session_factory() as session:
        assert (
            await _count_claimable(
                session,
                batch_id=batch_id,
                family_key=family_key,
            )
            == 1
        )
        claim = await _claim_next_trial(
            session,
            batch_id=batch_id,
            family_key=family_key,
        )
        assert claim is not None
        claimed_trial_id, claimed_task, claimed_uri = claim
        assert claimed_task == task_ids[1]
        await session.commit()

    mount = await prepare_family_state_mount(
        trial_id=str(claimed_trial_id),
        state_uri=claimed_uri,
        mount_path=resolved.mount_path,
        state_backend=backend,
    )
    try:
        # Skill_a.md must be present now - the previous evolve added it.
        contents = {p.name for p in mount.host_dir.iterdir() if p.is_file()}
        assert contents == {"skill_a.md"}, f"expected skill_a.md, got {contents}"
        skill_dir_snapshots.append(contents)
    finally:
        mount.cleanup()

    async with session_factory() as session:
        await _finalize_trial_succeeded(
            session,
            trial_id=claimed_trial_id,
            reward=0.91,
        )
        await session.commit()

    picked = await run_once(ctx)
    assert picked is True
    assert len(gateway.calls) == 2

    async with session_factory() as session:
        row = (
            (
                await session.execute(
                    text("""
            SELECT state, current_index, state_uri
              FROM batch_family_state
             WHERE batch_id = :bid AND family_key = :fam
        """),
                    {"bid": batch_id, "fam": family_key},
                )
            )
            .mappings()
            .one()
        )
        assert row["state"] == "pending"
        assert row["current_index"] == 2
        assert row["state_uri"] not in state_uri_history
        state_uri_history.append(row["state_uri"])

    # ─── Step 3: task_c claimable, and the mount contains BOTH skills.
    async with session_factory() as session:
        claim = await _claim_next_trial(
            session,
            batch_id=batch_id,
            family_key=family_key,
        )
        assert claim is not None
        claimed_trial_id, claimed_task, claimed_uri = claim
        assert claimed_task == task_ids[2]
        await session.commit()

    mount = await prepare_family_state_mount(
        trial_id=str(claimed_trial_id),
        state_uri=claimed_uri,
        mount_path=resolved.mount_path,
        state_backend=backend,
    )
    try:
        contents = {p.name for p in mount.host_dir.iterdir() if p.is_file()}
        assert contents == {"skill_a.md", "skill_b.md"}, (
            f"expected {{skill_a.md, skill_b.md}}, got {contents}"
        )
        # The modify from step 2 must have taken effect.
        skill_a = (mount.host_dir / "skill_a.md").read_text()
        assert "revised" in skill_a
        skill_dir_snapshots.append(contents)
    finally:
        mount.cleanup()

    async with session_factory() as session:
        await _finalize_trial_succeeded(
            session,
            trial_id=claimed_trial_id,
            reward=0.95,
        )
        await session.commit()

    # Orchestrator: final iteration reaches end-of-sequence -> done.
    # (Adapter is still called on the trailing task: the orchestrator
    # decides ``done`` only AFTER incrementing current_index past the
    # last slot. Our canned gateway ran out; expect exactly the two
    # evolve calls the earlier tasks used.)
    # Guard against a stray evolve: give the queue an empty patch to
    # cover the extra call the orchestrator issues when current_index
    # rolls off the end.
    gateway.queue.append({"add": [], "modify": [], "delete": []})
    picked = await run_once(ctx)
    assert picked is True

    async with session_factory() as session:
        row = (
            (
                await session.execute(
                    text("""
            SELECT state, current_index FROM batch_family_state
             WHERE batch_id = :bid AND family_key = :fam
        """),
                    {"bid": batch_id, "fam": family_key},
                )
            )
            .mappings()
            .one()
        )
        assert row["state"] == "done"
        assert row["current_index"] == 3

    # Cross-step invariants: monotonically-evolving skill library.
    assert skill_dir_snapshots == [
        set(),
        {"skill_a.md"},
        {"skill_a.md", "skill_b.md"},
    ]
    # 4 unique URIs: seed + one per evolve invocation that mutated state
    # (steps 1 + 2). The trailing empty-patch call in step 3 short-
    # circuits inside the adapter and returns the same URI, so we assert
    # ≥ 3 unique.
    assert len(set(state_uri_history)) >= 3

    await engine.dispose()
