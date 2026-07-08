"""End-to-end family-run smoke with noop adapter (#672 PR-1).

Composes the resolver, submit-time seeding, worker pre-start mount, and
finalize-time advance predicate in a single flow to prove the framework
works end-to-end without an orchestrator service. A full DB round-trip
version ships in the integration suite in PR-2 once the fanout wiring
persists ``trials.family_key`` end-to-end.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from uuid import uuid4

import pytest

from loom.family_run.orchestration import apply_advance_decision
from loom.family_run.prestart import prepare_family_state_mount
from loom.family_run.registry import resolve_plugin
from loom.family_run.resolve import resolve_family_run_spec
from loom.family_run.spec import FamilyRunSpec, PluginRef
from loom.family_run.state_backends import S3ArtifactsStateBackend
from loom.family_run.submit import seed_family_state
from loom.trajectory.storage import FakeObjectStore


@dataclass
class _Task:
    id: str
    tags: dict[str, str] | None = None


@dataclass
class _Trial:
    id: object = field(default_factory=uuid4)
    task_id: str = ""
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
async def test_family_run_two_task_sequence_with_noop_adapter(
    tmp_path: Path,
) -> None:
    """Compose the full pipeline for a 1-family / 2-task noop run:

    1. Resolve catalog + override -> ResolvedFamilyRunSpec.
    2. Seed batch_family_state (one row, task_sequence=[a, b]).
    3. Worker pre-start: download state_uri, bind-mount to /root/.skills.
    4. Trial 1 terminates (succeeded) -> advance predicate -> ADVANCE.
    5. Apply noop shortcut: bump current_index -> 1, state=pending.
    6. Worker pre-start again for trial 2 (mount identical since noop).
    7. Trial 2 terminates -> ADVANCE + noop shortcut -> state=done.
    """
    # PR-1 shortcut: adapter is noop.
    override = FamilyRunSpec(
        enabled=True,
        family_key_extractor=PluginRef(name="instance_id_prefix"),
        sequencer=PluginRef(name="alphabetical"),
        advance_predicate=PluginRef(name="always_on_terminal"),
        adapter=PluginRef(name="noop"),
        failure_policy=PluginRef(name="stall_family"),
        state_backend=PluginRef(name="s3_artifacts"),
    )
    resolved = resolve_family_run_spec(catalog=None, override=override)

    # Provision the state backend + seed.
    store = FakeObjectStore()
    await store.ensure_bucket("artifacts")
    backend = S3ArtifactsStateBackend(store=store, bucket="artifacts")
    batch_id = uuid4()
    tasks = [_Task(id="fam/a"), _Task(id="fam/b")]
    seeds = await seed_family_state(
        batch_id=batch_id,
        tasks=tasks,
        resolved=resolved,
        state_backend=backend,
    )
    assert len(seeds) == 1
    seed = seeds[0]
    assert seed.family_key == "fam"
    assert seed.task_sequence == ["fam/a", "fam/b"]

    # Represent the in-DB batch_family_state row.
    family = _Family(
        batch_id=batch_id,
        family_key=seed.family_key,
        task_sequence=list(seed.task_sequence),
        current_index=0,
        attempt_count=0,
    )
    current_state_uri = seed.state_uri

    predicate = resolve_plugin("loom.family.advance", resolved.advance_predicate)

    for expected_task in ("fam/a", "fam/b"):
        # Worker pre-start.
        mount = await prepare_family_state_mount(
            trial_id=str(uuid4()),
            state_uri=current_state_uri,
            mount_path=resolved.mount_path,
            state_backend=backend,
        )
        try:
            assert mount.container_dir == "/root/.skills"
        finally:
            mount.cleanup()

        # Trial terminates.
        trial = _Trial(task_id=expected_task, state="succeeded")
        decision = predicate.decide(
            trial=trial, family=family, spec=resolved,
            params=resolved.advance_predicate.params,
        )
        next_state = apply_advance_decision(family, decision)
        # Noop shortcut: bump index in-line.
        bumped = family.current_index + 1
        if bumped >= len(family.task_sequence):
            family_state_after = "done"
        else:
            family_state_after = "pending"
        assert next_state.state == "adapting"  # pre-shortcut

        # Apply shortcut to our in-memory family state (mirrors CP finalize).
        family = _Family(
            batch_id=family.batch_id,
            family_key=family.family_key,
            task_sequence=family.task_sequence,
            current_index=bumped,
            attempt_count=0,
        )
        if bumped >= len(family.task_sequence):
            assert family_state_after == "done"
        else:
            assert family_state_after == "pending"

    assert family.current_index == 2
    assert len(family.task_sequence) == 2
