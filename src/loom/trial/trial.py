"""Trial composition + run() body (spec §2.5 + §3.3).

Task 16 lands TrialContext; Task 18 adds the Trial class + run() body that
binds the whole pipeline (env start → step loop → finalize → result).
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from uuid import UUID

from loom.agent.base import AgentRuntime
from loom.driver.base import Driver
from loom.models.task import TaskConfig
from loom.models.trial import TrialConfig
from loom.trajectory.storage import ObjectStore
from loom.verifier.base import Verifier


@dataclass
class TrialContext:
    """Everything a Trial needs to run. Constructed by the worker; the Trial
    itself just executes against this bundle."""

    trial_id: UUID
    team_id: UUID
    task_config: TaskConfig
    task_checksum: str
    task_dir: Path
    trial_config: TrialConfig
    driver: Driver
    agent: AgentRuntime
    verifier: Verifier
    object_store: ObjectStore
    local_trajectory_path: Path
    trajectory_bucket: str = "trajectories"
    artifacts_bucket: str = "artifacts"

    @property
    def task_id(self) -> str:
        return self.task_config.task.id

    @property
    def trajectory_key(self) -> str:
        return f"{self.team_id}/{self.trial_id}/events.jsonl"

    @property
    def trajectory_uri(self) -> str:
        return f"s3://{self.trajectory_bucket}/{self.trajectory_key}"
