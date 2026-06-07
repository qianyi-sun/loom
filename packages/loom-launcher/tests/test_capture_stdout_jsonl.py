"""stream_stdout_jsonl contract tests."""

from __future__ import annotations

import json

from loom_launcher.capture import stream_stdout_jsonl


async def test_one_event_per_line(make_handle) -> None:
    handle = make_handle(stdout_chunks=[
        b'{"kind": "thought", "text": "first"}\n',
        b'{"kind": "tool_use", "name": "Bash"}\n',
    ])
    events = [e async for e in stream_stdout_jsonl(handle)]
    assert len(events) == 2
    assert events[0].model_dump() == {"kind": "thought", "text": "first"}
    assert events[1].model_dump() == {"kind": "tool_use", "name": "Bash"}


async def test_chunks_split_mid_line(make_handle) -> None:
    """The stream parser must reassemble when a chunk boundary falls
    inside a JSON object — common when the streaming source flushes
    every N bytes."""
    line = b'{"kind": "thought", "text": "split across chunks"}\n'
    mid = len(line) // 2
    handle = make_handle(stdout_chunks=[line[:mid], line[mid:]])
    events = [e async for e in stream_stdout_jsonl(handle)]
    assert len(events) == 1
    assert events[0].model_dump()["text"] == "split across chunks"


async def test_malformed_lines_skipped_by_default(make_handle) -> None:
    handle = make_handle(stdout_chunks=[
        b'{"valid": true}\n',
        b'not json at all\n',
        b'{"also_valid": true}\n',
    ])
    events = [e async for e in stream_stdout_jsonl(handle)]
    assert len(events) == 2
    assert events[0].model_dump() == {"valid": True}
    assert events[1].model_dump() == {"also_valid": True}


async def test_malformed_raises_when_skip_disabled(make_handle) -> None:
    handle = make_handle(stdout_chunks=[
        b'{"valid": true}\n',
        b'not json at all\n',
    ])
    import pytest
    with pytest.raises(json.JSONDecodeError):
        _ = [e async for e in stream_stdout_jsonl(handle, skip_malformed=False)]


async def test_trailing_no_newline_is_flushed(make_handle) -> None:
    """A process that exits without printing a final newline still emits
    its last event (real CLIs sometimes forget the trailing \\n)."""
    handle = make_handle(stdout_chunks=[
        b'{"first": 1}\n',
        b'{"final": 2}',   # no \n at end
    ])
    events = [e async for e in stream_stdout_jsonl(handle)]
    assert len(events) == 2
    assert events[1].model_dump() == {"final": 2}


async def test_empty_lines_skipped(make_handle) -> None:
    handle = make_handle(stdout_chunks=[
        b'\n\n{"k": 1}\n\n\n{"k": 2}\n',
    ])
    events = [e async for e in stream_stdout_jsonl(handle)]
    assert len(events) == 2
