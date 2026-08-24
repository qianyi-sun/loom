import hashlib
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

import pytest

from loom.models.trajectory import StepStartEvent, TrialEndEvent, TrialStartEvent
from loom.trajectory.storage import FakeObjectStore
from loom.trajectory.writer import TrajectoryWriter
from loom.trial.finalize import finalize_trajectory, finalize_trajectory_with_metadata


@pytest.fixture
def store() -> FakeObjectStore:
    return FakeObjectStore()


def _ev(seq: int, trial_uuid: UUID, **kwargs: Any) -> dict[str, Any]:
    return {
        "emitted_at": datetime.now(UTC),
        "trial_id": trial_uuid,
        "step_id": "main",
        "seq": seq,
        **kwargs,
    }


async def test_finalize_uploads_atif(tmp_path: Path, store: FakeObjectStore):
    local = tmp_path / "events.jsonl"
    writer = TrajectoryWriter(
        local_path=local, store=store,
        bucket="trajectories", key="team/trial-1/events.jsonl",
        min_part_bytes=0,
    )
    trial_uuid = uuid4()
    async with writer:
        await writer.append(TrialStartEvent(
            **_ev(0, trial_uuid), task_id="t", agent_name="oracle",
            agent_mode="out-of-box",
        ))
        await writer.append(StepStartEvent(
            **_ev(1, trial_uuid), instruction_excerpt="x",
        ))
        await writer.append(TrialEndEvent(
            **_ev(2, trial_uuid), final_state="succeeded",
        ))

    atif_uri = await finalize_trajectory(
        local_path=local, store=store,
        team_id="team", trial_id="trial-1",
        task_id="t", agent_name="oracle", agent_version="1.0",
    )

    assert atif_uri == "s3://trajectories/team/trial-1/atif.json"
    assert ("trajectories", "team/trial-1/atif.json") in store.objects


async def test_finalize_uses_custom_bucket(tmp_path: Path, store: FakeObjectStore):
    local = tmp_path / "events.jsonl"
    writer = TrajectoryWriter(
        local_path=local, store=store,
        bucket="x", key="x", min_part_bytes=0,
    )
    trial_uuid = uuid4()
    async with writer:
        await writer.append(TrialStartEvent(
            **_ev(0, trial_uuid), task_id="t",
            agent_name="oracle", agent_mode="out-of-box",
        ))
        await writer.append(TrialEndEvent(
            **_ev(1, trial_uuid), final_state="succeeded",
        ))
    uri = await finalize_trajectory(
        local_path=local, store=store,
        team_id="team", trial_id="trial-9",
        task_id="t", agent_name="oracle", agent_version="1.0",
        bucket="custom-bucket",
    )
    assert uri == "s3://custom-bucket/team/trial-9/atif.json"


async def test_finalize_returns_exact_object_metadata(
    tmp_path: Path,
    store: FakeObjectStore,
) -> None:
    local = tmp_path / "events.jsonl"
    writer = TrajectoryWriter(
        local_path=local, store=store,
        bucket="x", key="x", min_part_bytes=0,
    )
    trial_uuid = uuid4()
    async with writer:
        await writer.append(TrialStartEvent(
            **_ev(0, trial_uuid), task_id="t",
            agent_name="oracle", agent_mode="out-of-box",
        ))
        await writer.append(TrialEndEvent(
            **_ev(1, trial_uuid), final_state="succeeded",
        ))

    finalized = await finalize_trajectory_with_metadata(
        local_path=local, store=store,
        team_id="team", trial_id="trial-10",
        task_id="t", agent_name="oracle", agent_version="1.0",
    )
    body = store.objects[("trajectories", "team/trial-10/atif.json")]
    assert finalized.uri == "s3://trajectories/team/trial-10/atif.json"
    assert finalized.size_bytes == len(body)
    assert finalized.sha256 == hashlib.sha256(body).hexdigest()


async def test_finalize_attempt_uploads_immutable_atif_identity(
    tmp_path: Path,
    store: FakeObjectStore,
) -> None:
    local = tmp_path / "attempt-2-events.jsonl"
    writer = TrajectoryWriter(
        local_path=local,
        store=store,
        bucket="x",
        key="x",
        min_part_bytes=0,
    )
    trial_uuid = uuid4()
    async with writer:
        await writer.append(TrialStartEvent(
            **_ev(0, trial_uuid),
            task_id="t",
            agent_name="oracle",
            agent_mode="out-of-box",
        ))
        await writer.append(TrialEndEvent(
            **_ev(1, trial_uuid),
            final_state="succeeded",
        ))

    finalized = await finalize_trajectory_with_metadata(
        local_path=local,
        store=store,
        team_id="team",
        trial_id="trial-11",
        task_id="t",
        agent_name="oracle",
        agent_version="1.0",
        attempt_count=2,
    )

    key = "team/trial-11/attempts/2/atif.json"
    assert finalized.uri == f"s3://trajectories/{key}"
    assert ("trajectories", key) in store.objects
