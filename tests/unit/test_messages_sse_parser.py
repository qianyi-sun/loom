"""Unit coverage for the SSE-parser + usage-extractor helpers in
`loom_llm_gateway.routes.messages`. Streaming Anthropic responses
arrive as arbitrary byte chunks; the parser must reconstruct event
boundaries and surface the cumulative token totals needed for cost
attribution regardless of how the chunks split."""

from __future__ import annotations

import json

from loom_llm_gateway.routes.messages import (
    _extract_stream_usage,
    _parse_sse_blocks,
)


def _b(event: str, data: dict) -> bytes:
    return f"event: {event}\ndata: {json.dumps(data)}\n\n".encode()


def test_parse_sse_blocks_handles_complete_buffer() -> None:
    buf = _b("foo", {"a": 1}) + _b("bar", {"b": 2})
    events, tail = _parse_sse_blocks(buf)
    assert events == [("foo", {"a": 1}), ("bar", {"b": 2})]
    assert tail == b""


def test_parse_sse_blocks_keeps_unfinished_tail() -> None:
    buf = _b("foo", {"a": 1}) + b"event: bar\ndata: {\"b\""
    events, tail = _parse_sse_blocks(buf)
    assert events == [("foo", {"a": 1})]
    assert tail == b'event: bar\ndata: {"b"'


def test_parse_sse_blocks_skips_malformed_json() -> None:
    buf = b"event: x\ndata: not-json\n\n" + _b("y", {"ok": True})
    events, _ = _parse_sse_blocks(buf)
    assert events == [("y", {"ok": True})]


def test_extract_stream_usage_pulls_input_and_cache_from_message_start() -> None:
    accum: dict[str, int] = {"input_tokens": 0, "output_tokens": 0}
    _extract_stream_usage(
        "message_start",
        {
            "type": "message_start",
            "message": {
                "usage": {
                    "input_tokens": 1000,
                    "cache_creation_input_tokens": 50,
                    "cache_read_input_tokens": 250,
                    "output_tokens": 1,
                },
            },
        },
        accum,
    )
    assert accum == {
        "input_tokens": 1000,
        "output_tokens": 1,
        "cache_creation_input_tokens": 50,
        "cache_read_input_tokens": 250,
    }


def test_extract_stream_usage_overwrites_output_from_message_delta() -> None:
    accum: dict[str, int] = {"input_tokens": 100, "output_tokens": 1}
    _extract_stream_usage(
        "message_delta",
        {
            "type": "message_delta",
            "delta": {"stop_reason": "end_turn"},
            "usage": {"output_tokens": 250},
        },
        accum,
    )
    assert accum["output_tokens"] == 250
    assert accum["input_tokens"] == 100


def test_extract_stream_usage_ignores_intermediate_event_types() -> None:
    accum: dict[str, int] = {"input_tokens": 5, "output_tokens": 5}
    snapshot = dict(accum)
    for event in (
        "content_block_start",
        "content_block_delta",
        "content_block_stop",
        "message_stop",
        "ping",
    ):
        _extract_stream_usage(event, {"type": event}, accum)
    assert accum == snapshot


def test_parse_sse_blocks_resumes_across_chunk_boundary() -> None:
    """Simulate the real streaming path: bytes arrive in arbitrary
    chunks, and the parser is called incrementally per chunk."""
    full = _b("message_start", {
        "message": {"usage": {"input_tokens": 10, "output_tokens": 1}},
    }) + _b("message_delta", {"usage": {"output_tokens": 42}})

    buffer = b""
    seen: list[tuple[str, dict]] = []
    # Split at every byte to stress-test boundary handling.
    for i in range(len(full)):
        buffer += full[i:i + 1]
        events, buffer = _parse_sse_blocks(buffer)
        seen.extend(events)
    assert buffer == b""
    assert [name for name, _ in seen] == ["message_start", "message_delta"]
