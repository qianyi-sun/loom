"""MiniSweAgentAdapter contract: build_invocation + JSONL capture."""

from __future__ import annotations

from pathlib import PurePosixPath
from uuid import uuid4

from loom_launcher import get_adapter
from loom_launcher.adapter import ModelSpec


def test_build_invocation_argv() -> None:
    adapter = get_adapter("mini-swe-agent")
    assert adapter is not None
    env: dict[str, str] = {}
    argv = adapter.build_invocation(
        instruction="solve fizzbuzz",
        workdir=PurePosixPath("/workspace"),
        model=ModelSpec(provider="openai", name="gpt-5"),
        env=env,
    )
    assert argv == [
        "mini-swe-agent",
        "--model", "openai/gpt-5",
        "--workdir", "/workspace",
        "--output", "jsonl",
        "--task", "solve fizzbuzz",
    ]


async def test_capture_via_stdout_jsonl(make_handle) -> None:
    adapter = get_adapter("mini-swe-agent")
    assert adapter is not None
    handle = make_handle(stdout_chunks=[
        b'{"step": 1, "kind": "plan", "text": "investigate"}\n',
        b'{"step": 2, "kind": "act", "tool": "Bash"}\n',
    ])
    events = [
        e.model_dump()
        async for e in adapter.capture_events(
            exec_handle=handle, step_id="main", trial_id=uuid4(),
        )
    ]
    assert events == [
        {"step": 1, "kind": "plan", "text": "investigate"},
        {"step": 2, "kind": "act", "tool": "Bash"},
    ]
