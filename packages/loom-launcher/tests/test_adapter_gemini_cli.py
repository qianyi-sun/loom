"""GeminiCliAdapter contract: build_invocation + JSON-output capture."""

from __future__ import annotations

from pathlib import PurePosixPath
from uuid import uuid4

from loom_launcher import get_adapter
from loom_launcher.adapter import ModelSpec


def test_build_invocation_argv() -> None:
    adapter = get_adapter("gemini-cli")
    assert adapter is not None
    env: dict[str, str] = {}
    argv = adapter.build_invocation(
        instruction="explain this codebase",
        workdir=PurePosixPath("/workspace"),
        model=ModelSpec(provider="google", name="gemini-2.5-pro"),
        env=env,
    )
    assert argv == [
        "gemini",
        "--model", "google/gemini-2.5-pro",
        "--output", "json",
        "--prompt", "explain this codebase",
    ]


async def test_capture_via_stdout_jsonl(make_handle) -> None:
    adapter = get_adapter("gemini-cli")
    assert adapter is not None
    handle = make_handle(stdout_chunks=[
        b'{"role": "model", "text": "this is a Python project"}\n',
        b'{"role": "model", "tool_call": {"name": "search"}}\n',
    ])
    events = [
        e.model_dump()
        async for e in adapter.capture_events(
            exec_handle=handle, step_id="main", trial_id=uuid4(),
        )
    ]
    assert events == [
        {"role": "model", "text": "this is a Python project"},
        {"role": "model", "tool_call": {"name": "search"}},
    ]
