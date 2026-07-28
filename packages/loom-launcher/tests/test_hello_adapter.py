"""HelloAdapter end-to-end: build_invocation + capture_events round-trip."""

from __future__ import annotations

from pathlib import PurePosixPath
from uuid import uuid4

from loom_launcher import get_adapter
from loom_launcher.adapter import ModelSpec


async def test_hello_adapter_capture_returns_one_event(make_handle) -> None:
    adapter = get_adapter("hello")
    assert adapter is not None
    assert adapter.needs_model is False
    assert adapter.catalog_visibility == "internal"

    env: dict[str, str] = {}
    argv = adapter.build_invocation(
        instruction="ping",
        workdir=PurePosixPath("/workspace"),
        model=ModelSpec(provider="openai", name="gpt-5"),
        env=env,
    )
    assert argv == ["echo", '{"line": "hello from ping"}']

    handle = make_handle(stdout_chunks=[b'{"line": "hello from ping"}\n'])
    events = [
        e.model_dump()
        async for e in adapter.capture_events(
            exec_handle=handle, step_id="main", trial_id=uuid4(),
        )
    ]
    assert events == [{"line": "hello from ping"}]
