"""Finalize trajectory — project events to ATIF v1.7 and upload as JSON.

Spec §3.7. Pure transform — re-runnable when ATIF schema bumps.
"""

from __future__ import annotations

from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path

from loom.trajectory.atif import project_to_atif
from loom.trajectory.reader import TrajectoryReader
from loom.trajectory.storage import ObjectStore


@dataclass(frozen=True, slots=True)
class FinalizedTrajectory:
    uri: str
    sha256: str
    size_bytes: int


async def finalize_trajectory_with_metadata(
    *,
    local_path: Path,
    store: ObjectStore,
    team_id: str,
    trial_id: str,
    task_id: str,
    agent_name: str,
    agent_version: str,
    bucket: str = "trajectories",
) -> FinalizedTrajectory:
    """Project, upload, and return exact immutable-object evidence."""
    reader = TrajectoryReader(local_path)
    atif = project_to_atif(
        reader.iter_all(),
        task_id=task_id,
        agent_name=agent_name,
        agent_version=agent_version,
    )
    key = f"{team_id}/{trial_id}/atif.json"
    body = atif.model_dump_json(indent=2).encode("utf-8")
    uri = await store.put_object(bucket=bucket, key=key, body=body)
    return FinalizedTrajectory(
        uri=uri,
        sha256=sha256(body).hexdigest(),
        size_bytes=len(body),
    )


async def finalize_trajectory(
    *,
    local_path: Path,
    store: ObjectStore,
    team_id: str,
    trial_id: str,
    task_id: str,
    agent_name: str,
    agent_version: str,
    bucket: str = "trajectories",
) -> str:
    """Project events to ATIF v1.7 and upload. Returns the ATIF object URI."""
    finalized = await finalize_trajectory_with_metadata(
        local_path=local_path,
        store=store,
        team_id=team_id,
        trial_id=trial_id,
        task_id=task_id,
        agent_name=agent_name,
        agent_version=agent_version,
        bucket=bucket,
    )
    return finalized.uri
