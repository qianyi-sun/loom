"""tail_pty contract tests.

tail_pty is the lossy capture path for TUI-only agents (codex, qwen-cli,
kimi-cli at v1). It strips ANSI escapes and yields one event per
non-empty line. With a prompt pattern it filters to only matching lines.
"""

from __future__ import annotations

import re

from loom_launcher.capture import tail_pty


async def test_yields_one_event_per_line(make_handle) -> None:
    handle = make_handle(stdout_chunks=[
        b"first line\n",
        b"second line\n",
    ])
    events = [
        e.model_dump() async for e in tail_pty(handle)
    ]
    assert len(events) == 2
    assert events[0]["line"] == "first line"
    assert events[0]["kind"] == "tty_thought"
    assert events[1]["line"] == "second line"


async def test_strips_ansi_csi_escapes(make_handle) -> None:
    """Real TUI streams interleave cursor moves + color codes. We strip
    them before yielding so dashboards don't have to."""
    # \x1b[31m = red, \x1b[0m = reset, \x1b[2J = clear screen
    handle = make_handle(stdout_chunks=[
        b"\x1b[31mhello\x1b[0m world\n",
        b"\x1b[2Jclean\n",
    ])
    events = [
        e.model_dump() async for e in tail_pty(handle)
    ]
    assert events[0]["line"] == "hello world"
    assert events[1]["line"] == "clean"


async def test_prompt_pattern_filters_to_matches(make_handle) -> None:
    """When a pattern is supplied, only matching lines yield events."""
    handle = make_handle(stdout_chunks=[
        b"noise: skipping\n",
        b"AGENT: thinking about it\n",
        b"more noise\n",
        b"AGENT: i have an answer\n",
    ])
    pattern = re.compile(rb"^AGENT:")
    events = [
        e.model_dump() async for e in tail_pty(handle, prompt_pattern=pattern)
    ]
    assert len(events) == 2
    assert events[0]["line"] == "AGENT: thinking about it"
    assert events[1]["line"] == "AGENT: i have an answer"


async def test_empty_lines_skipped(make_handle) -> None:
    handle = make_handle(stdout_chunks=[
        b"\n\nline\n\n",
        b"\x1b[0m\n",   # ANSI-only line becomes empty after strip
    ])
    events = [
        e.model_dump() async for e in tail_pty(handle)
    ]
    assert len(events) == 1
    assert events[0]["line"] == "line"


async def test_chunks_split_mid_line(make_handle) -> None:
    """Same chunk-boundary robustness as stream_stdout_jsonl."""
    handle = make_handle(stdout_chunks=[
        b"this is ",
        b"one line\n",
    ])
    events = [
        e.model_dump() async for e in tail_pty(handle)
    ]
    assert len(events) == 1
    assert events[0]["line"] == "this is one line"
