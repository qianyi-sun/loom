"""Checkpoint bridge unit tests (#744)."""

from __future__ import annotations

import json
from pathlib import Path
from uuid import uuid4

import pytest
from pydantic import TypeAdapter

from loom.agent.terminus2.checkpoint_bridge import HarborCheckpointBridge
from loom.models.trajectory import EventKind, TrajectoryEvent
from loom.models.types import ModelSpec
from loom.trajectory.storage import FakeObjectStore
from loom.trajectory.writer import TrajectoryWriter

_adapter: TypeAdapter[TrajectoryEvent] = TypeAdapter(TrajectoryEvent)


@pytest.mark.asyncio
async def test_bridge_syncs_agent_steps(tmp_path: Path) -> None:
    store = FakeObjectStore()
    local = tmp_path / "events.jsonl"
    writer = TrajectoryWriter(
        local_path=local,
        store=store,
        bucket="trajectories",
        key="t/events.jsonl",
        flush_event_count=1000,
        flush_bytes=10_000_000,
        flush_sec=3600,
        min_part_bytes=0,
    )
    bridge = HarborCheckpointBridge(
        trajectory=writer,
        trial_id=uuid4(),
        step_id="agent",
        model=ModelSpec(provider="openai", name="gpt-4"),
    )
    traj = tmp_path / "trajectory.json"
    traj.write_text(
        json.dumps(
            {
                "steps": [
                    {
                        "step_id": 2,
                        "source": "agent",
                        "message": "done",
                        "metrics": {
                            "input_tokens": 10,
                            "output_tokens": 5,
                            "cost_usd": 0.01,
                        },
                        "tool_calls": [
                            {
                                "function_name": "bash_command",
                                "tool_call_id": "c1",
                                "arguments": {
                                    "keystrokes": "ls\n",
                                    "duration": 0.5,
                                },
                            },
                            {"function_name": "mark_task_complete"},
                        ],
                        "observation": {
                            "results": [{"content": "file.txt\n$ "}],
                        },
                    },
                ],
            },
        ),
        encoding="utf-8",
    )

    async with writer:
        await bridge.emit_provenance()
        synced = await bridge.sync_trajectory_file(traj)
        await bridge.emit_artifact_refs(tmp_path)

    assert synced == 1
    kinds = []
    for line in local.read_text().strip().splitlines():
        event = _adapter.validate_json(line)
        kinds.append(event.kind)

    assert EventKind.TERMINUS2_RUNTIME_PROVENANCE in kinds
    assert EventKind.LLM_CALL in kinds
    assert EventKind.TERMINUS2_TURN in kinds
    assert EventKind.TERMINUS2_COMMAND in kinds
    assert EventKind.TERMINUS2_TERMINAL_OBSERVATION in kinds
    assert EventKind.TERMINUS2_ARTIFACT_REF in kinds


@pytest.mark.asyncio
async def test_bridge_skips_non_agent_steps(tmp_path: Path) -> None:
    writer = TrajectoryWriter(
        local_path=tmp_path / "events.jsonl",
        store=FakeObjectStore(),
        bucket="trajectories",
        key="t/events.jsonl",
        flush_event_count=1000,
        flush_bytes=10_000_000,
        flush_sec=3600,
        min_part_bytes=0,
    )
    bridge = HarborCheckpointBridge(
        trajectory=writer,
        trial_id=uuid4(),
        step_id="agent",
        model=ModelSpec(provider="openai", name="gpt-4"),
    )
    traj = tmp_path / "trajectory.json"
    traj.write_text(
        json.dumps({"steps": [{"step_id": 1, "source": "user"}]}),
        encoding="utf-8",
    )
    async with writer:
        assert await bridge.sync_trajectory_file(traj) == 0
