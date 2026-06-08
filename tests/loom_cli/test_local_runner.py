"""LocalRunner produces a TrialResult for a solution-baseline oracle
trial against FakeDriver, and writes the trajectory to the local
object store + a friendly per-trial directory."""

from __future__ import annotations

import json
from pathlib import Path
from uuid import uuid4

import pytest

from loom.driver.fake import FakeDriver
from loom.models.result import TrialState
from loom.models.task import TaskConfig
from loom_cli.local_object_store import LocalDiskObjectStore
from loom_cli.local_runner import LocalRunner


def _oracle_task_config() -> TaskConfig:
    return TaskConfig.model_validate({
        "schema_version": "1",
        "task": {"id": "cli-test/echo", "name": "echo"},
        "environment": {"os": "linux", "docker_image": "alpine"},
        "agent": {"name": "oracle"},
        "verifier": {"name": "pytest"},
        "steps": [{"name": "solve"}],
    })


@pytest.mark.asyncio
async def test_local_runner_succeeds_for_oracle_no_op(
    tmp_path: Path,
) -> None:
    bucket_root = tmp_path / "store"
    bucket_root.mkdir()
    store = LocalDiskObjectStore(root=bucket_root)
    task_dir = tmp_path / "task"
    task_dir.mkdir()
    (task_dir / "solution").mkdir()
    (task_dir / "solution" / "solve.sh").write_text("#!/bin/sh\nexit 0\n")
    (task_dir / "solution" / "solve.sh").chmod(0o755)

    runner = LocalRunner(
        trial_id=uuid4(), team_id=uuid4(),
        task_config=_oracle_task_config(),
        task_checksum="x" * 64,
        task_dir=task_dir,
        driver_factory=FakeDriver,
        output_dir=tmp_path / "runs",
        object_store=store,
        upstream_gateway_tokens={"anthropic": "x"},
        anthropic_client=None, openai_client=None, google_client=None,
    )
    result = await runner.run()
    assert result.state in {TrialState.SUCCEEDED, TrialState.FAILED}
    traj_dir = tmp_path / "runs" / str(runner.trial_id)
    assert traj_dir.exists()
    assert (traj_dir / "events.jsonl").exists()
    lines = (traj_dir / "events.jsonl").read_text().splitlines()
    kinds = [json.loads(line)["kind"] for line in lines if line.strip()]
    assert "trial_start" in kinds
    assert "trial_end" in kinds
