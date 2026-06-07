"""KimiCliAdapter contract: best-effort argv + tail_pty capture."""

from __future__ import annotations

from pathlib import PurePosixPath
from uuid import uuid4

from loom_launcher import get_adapter
from loom_launcher.adapter import ModelSpec


def test_build_invocation_argv() -> None:
    adapter = get_adapter("kimi-cli")
    assert adapter is not None
    env: dict[str, str] = {}
    argv = adapter.build_invocation(
        instruction="hello kimi",
        workdir=PurePosixPath("/workspace"),
        model=ModelSpec(provider="openai", name="kimi-k2"),
        env=env,
    )
    assert argv == ["kimi", "hello kimi"]


async def test_capture_via_pty(make_handle) -> None:
    adapter = get_adapter("kimi-cli")
    assert adapter is not None
    handle = make_handle(stdout_chunks=[
        b"kimi: hello!\n",
        b"\x1b[33mkimi: more\x1b[0m\n",
    ])
    events = [
        e.model_dump()
        async for e in adapter.capture_events(
            exec_handle=handle, step_id="main", trial_id=uuid4(),
        )
    ]
    assert events == [
        {"line": "kimi: hello!", "kind": "tty_thought"},
        {"line": "kimi: more", "kind": "tty_thought"},
    ]
