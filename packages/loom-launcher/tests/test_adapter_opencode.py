"""OpencodeAdapter contract: build_invocation + stream_stdout_jsonl capture."""

from __future__ import annotations

from pathlib import PurePosixPath
from uuid import uuid4

from loom_launcher import get_adapter
from loom_launcher.adapter import ModelSpec


def test_build_invocation_argv() -> None:
    adapter = get_adapter("opencode")
    assert adapter is not None
    env: dict[str, str] = {}
    argv = adapter.build_invocation(
        instruction="ping",
        workdir=PurePosixPath("/workspace"),
        model=ModelSpec(provider="openai", name="gpt-5"),
        env=env,
    )
    assert argv == [
        "opencode", "run",
        "--model", "openai/gpt-5",
        "--output", "json",
        "--cwd", "/workspace",
        "ping",
    ]


async def test_capture_via_stdout_jsonl(make_handle) -> None:
    adapter = get_adapter("opencode")
    assert adapter is not None
    handle = make_handle(stdout_chunks=[
        b'{"kind": "thought", "text": "exploring"}\n',
        b'{"kind": "tool_use", "name": "Read"}\n',
    ])
    events = [
        e.model_dump()
        async for e in adapter.capture_events(
            exec_handle=handle, step_id="main", trial_id=uuid4(),
        )
    ]
    assert events == [
        {"kind": "thought", "text": "exploring"},
        {"kind": "tool_use", "name": "Read"},
    ]
