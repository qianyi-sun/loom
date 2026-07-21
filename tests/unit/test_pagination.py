"""Cursor pagination util (Plan 18 Task 1)."""

from __future__ import annotations

import base64
import json
from datetime import UTC, datetime, timedelta, timezone
from uuid import uuid4

import pytest

from loom_service.pagination import Cursor, decode_cursor, encode_cursor


def _raw_cursor(timestamp: str, cursor_id: str) -> str:
    body = json.dumps({"t": timestamp, "i": cursor_id}).encode()
    return base64.urlsafe_b64encode(body).decode().rstrip("=")


def test_round_trip() -> None:
    t = datetime(2026, 6, 6, 12, 0, 0, tzinfo=UTC)
    i = uuid4()
    enc = encode_cursor(Cursor(submitted_at=t, id=i))
    out = decode_cursor(enc)
    assert out.submitted_at == t
    assert out.id == i


def test_round_trip_with_microseconds() -> None:
    t = datetime(2026, 6, 6, 12, 0, 0, 123456, tzinfo=UTC)
    i = uuid4()
    enc = encode_cursor(Cursor(submitted_at=t, id=i))
    out = decode_cursor(enc)
    assert out.submitted_at == t
    assert out.id == i


def test_decode_invalid_b64_raises() -> None:
    with pytest.raises(ValueError, match="invalid cursor"):
        decode_cursor("!!!not-valid-b64!!!")


def test_decode_truncated_raises() -> None:
    with pytest.raises(ValueError, match="invalid cursor"):
        decode_cursor("abc")


def test_decode_wrong_shape_raises() -> None:
    """JSON that doesn't have the expected `t`/`i` keys is rejected."""
    body = base64.urlsafe_b64encode(b'{"other": "stuff"}').decode().rstrip("=")
    with pytest.raises(ValueError, match="invalid cursor"):
        decode_cursor(body)


def test_decode_rejects_naive_timestamp_with_valid_uuid() -> None:
    body = _raw_cursor("2026-06-06T12:00:00", str(uuid4()))

    with pytest.raises(ValueError, match="timezone offset"):
        decode_cursor(body)


def test_decode_normalizes_timezone_offset_to_utc() -> None:
    offset = timezone(timedelta(hours=5, minutes=30))
    timestamp = datetime(2026, 6, 6, 17, 30, tzinfo=offset)

    decoded = decode_cursor(_raw_cursor(timestamp.isoformat(), str(uuid4())))

    assert decoded.submitted_at == datetime(2026, 6, 6, 12, 0, tzinfo=UTC)
    assert decoded.submitted_at.tzinfo is UTC


def test_encode_rejects_naive_timestamp() -> None:
    with pytest.raises(ValueError, match="timezone offset"):
        encode_cursor(Cursor(submitted_at=datetime(2026, 6, 6), id=uuid4()))


def test_encoded_is_decodable_by_hand() -> None:
    """Cursors are not sealed — deliberate so a developer can debug."""
    t = datetime(2026, 6, 6, tzinfo=UTC)
    i = uuid4()
    enc = encode_cursor(Cursor(submitted_at=t, id=i))
    padded = enc + "=" * (-len(enc) % 4)
    decoded = json.loads(base64.urlsafe_b64decode(padded.encode()).decode())
    assert decoded["t"] == t.isoformat()
    assert decoded["i"] == str(i)
