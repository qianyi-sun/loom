"""#5 Slice 1 helpers — fast unit tests that don't need a MinIO
testcontainer. The integration-test layer in
`tests/integration/test_service_trajectory.py` covers the route
wiring end-to-end; here we pin the pure helpers so a future
refactor can't break the SSE wire format or the seq-cursor read
semantics without a fast local signal."""

from __future__ import annotations

import json

from botocore.exceptions import ClientError

from loom_service.routes.trajectory import (
    _read_events_after_seq,
    _sse_format,
)


class _FakeBody:
    def __init__(self, payload: bytes) -> None:
        self._payload = payload

    def iter_lines(self):  # type: ignore[no-untyped-def]
        return iter(self._payload.splitlines())

    def close(self) -> None:
        pass


class _FakeClient:
    def __init__(self, payload: bytes | None) -> None:
        self._payload = payload

    def get_object(self, **_kw):  # type: ignore[no-untyped-def]
        if self._payload is None:
            raise ClientError(
                {"Error": {"Code": "NoSuchKey"}},
                "GetObject",
            )
        return {"Body": _FakeBody(self._payload)}


def _jsonl(events: list[dict]) -> bytes:
    return ("\n".join(json.dumps(e) for e in events) + "\n").encode()


def test_sse_format_with_id_and_event_kind() -> None:
    """Both `id:` and `event:` fields are emitted; data is JSON-encoded
    on a single line. Pin the on-wire shape against accidental
    formatting drift."""
    out = _sse_format("complete", {"last_seq": 4}, event_id="4")
    assert out == b'id: 4\nevent: complete\ndata: {"last_seq":4}\n\n'


def test_sse_format_default_event_is_message() -> None:
    """No `event:` line means the SSE client interprets it as the
    default `message` event — matches browser EventSource semantics."""
    out = _sse_format(None, {"seq": 1, "kind": "step_start"}, event_id="1")
    assert b"event:" not in out
    assert out == b'id: 1\ndata: {"seq":1,"kind":"step_start"}\n\n'


def test_sse_format_no_id_omits_id_line() -> None:
    out = _sse_format(None, {"k": "v"})
    assert out == b'data: {"k":"v"}\n\n'


def test_read_events_after_seq_returns_all_with_default_cursor() -> None:
    """`after_seq=-1` (sentinel for "from the beginning") returns
    every event in seq order."""
    events = [{"seq": i, "kind": f"k{i}"} for i in range(5)]
    client = _FakeClient(_jsonl(events))
    out = _read_events_after_seq(
        client, bucket="x", key="y", after_seq=-1, limit=100,
    )
    assert [e["seq"] for e in out] == [0, 1, 2, 3, 4]


def test_read_events_after_seq_strictly_skips_already_seen() -> None:
    """`after_seq=N` returns events with seq > N (strict inequality)."""
    events = [{"seq": i, "kind": f"k{i}"} for i in range(5)]
    client = _FakeClient(_jsonl(events))
    out = _read_events_after_seq(
        client, bucket="x", key="y", after_seq=2, limit=100,
    )
    assert [e["seq"] for e in out] == [3, 4]


def test_read_events_after_seq_respects_limit() -> None:
    events = [{"seq": i, "kind": f"k{i}"} for i in range(10)]
    client = _FakeClient(_jsonl(events))
    out = _read_events_after_seq(
        client, bucket="x", key="y", after_seq=-1, limit=3,
    )
    assert [e["seq"] for e in out] == [0, 1, 2]


def test_read_events_after_seq_drops_events_without_int_seq() -> None:
    """Legacy/test events that lack a numeric `seq` can't be resumed
    against; they're filtered out rather than confusing the cursor."""
    payload = _jsonl([
        {"seq": 0, "kind": "ok"},
        {"kind": "no_seq_at_all"},
        {"seq": "1", "kind": "string_seq_invalid"},
        {"seq": 2, "kind": "ok"},
    ])
    client = _FakeClient(payload)
    out = _read_events_after_seq(
        client, bucket="x", key="y", after_seq=-1, limit=100,
    )
    assert [e["seq"] for e in out] == [0, 2]


def test_read_events_after_seq_tolerates_blank_lines() -> None:
    payload = b'{"seq":0,"kind":"a"}\n\n{"seq":1,"kind":"b"}\n'
    client = _FakeClient(payload)
    out = _read_events_after_seq(
        client, bucket="x", key="y", after_seq=-1, limit=100,
    )
    assert [e["seq"] for e in out] == [0, 1]


def test_read_events_after_seq_tolerates_truncated_final_line() -> None:
    """Finalize crashes can leave a partial JSON line at the end; the
    reader skips it instead of erroring (matches the legacy
    list_events behavior in trajectory.py)."""
    payload = b'{"seq":0,"kind":"a"}\n{"seq":1,"kind":"b"}\n{"seq":2,"kin'
    client = _FakeClient(payload)
    out = _read_events_after_seq(
        client, bucket="x", key="y", after_seq=-1, limit=100,
    )
    assert [e["seq"] for e in out] == [0, 1]


def test_read_events_after_seq_missing_object_returns_empty() -> None:
    """Trial existed but the trajectory object was never written
    (queued / just-claimed / crashed pre-first-event). Empty result
    matches the legacy /trajectory behavior — the SPA should render
    "no events yet", not a 404."""
    client = _FakeClient(None)
    out = _read_events_after_seq(
        client, bucket="x", key="y", after_seq=-1, limit=100,
    )
    assert out == []


def test_read_events_after_seq_sorts_by_seq() -> None:
    """If the underlying JSONL is out of order (e.g. parallel-writer
    bug), the response is sorted so cursor semantics still work."""
    payload = _jsonl([
        {"seq": 2, "kind": "c"},
        {"seq": 0, "kind": "a"},
        {"seq": 1, "kind": "b"},
    ])
    client = _FakeClient(payload)
    out = _read_events_after_seq(
        client, bucket="x", key="y", after_seq=-1, limit=100,
    )
    assert [e["seq"] for e in out] == [0, 1, 2]
