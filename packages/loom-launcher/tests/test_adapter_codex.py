"""CodexAdapter contract: build_invocation + tail_pty capture."""

from __future__ import annotations

from pathlib import PurePosixPath
from uuid import uuid4

from loom_launcher import get_adapter
from loom_launcher.adapter import ModelSpec


def test_build_invocation_argv() -> None:
    adapter = get_adapter("codex")
    assert adapter is not None
    env: dict[str, str] = {}
    argv = adapter.build_invocation(
        instruction="solve fizzbuzz",
        workdir=PurePosixPath("/workspace"),
        model=ModelSpec(provider="openai", name="gpt-5"),
        env=env,
    )
    assert argv == ["codex", "solve fizzbuzz"]
    # No telemetry env mutations expected.
    assert env == {}


async def test_capture_via_pty(make_handle) -> None:
    adapter = get_adapter("codex")
    assert adapter is not None
    handle = make_handle(stdout_chunks=[
        b"\x1b[31mthinking...\x1b[0m\n",
        b"done!\n",
    ])
    events = [
        e.model_dump()
        async for e in adapter.capture_events(
            exec_handle=handle, step_id="main", trial_id=uuid4(),
        )
    ]
    assert events == [
        {"line": "thinking...", "kind": "tty_thought"},
        {"line": "done!", "kind": "tty_thought"},
    ]
