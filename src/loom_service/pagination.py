"""Cursor pagination utility for trials list (spec §3).

The cursor encodes `(submitted_at, id)` as base64(json). Deliberately
NOT a sealed JWT: cursors are debug-friendly (a developer can decode
one by hand) and don't need integrity protection — they're returned
to the same caller who sent them, only carry public DB-sort keys,
and the queries downstream re-filter by team scope.

Timestamp values must be offset-aware. Encoding and decoding normalize them
to UTC so comparisons never depend on a service pod's local timezone.
"""

from __future__ import annotations

import base64
import binascii
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from uuid import UUID


@dataclass(frozen=True)
class Cursor:
    submitted_at: datetime
    id: UUID


def _aware_utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("cursor timestamp must include a timezone offset")
    return value.astimezone(UTC)


def encode_cursor(c: Cursor) -> str:
    body = json.dumps({"t": _aware_utc(c.submitted_at).isoformat(), "i": str(c.id)})
    return base64.urlsafe_b64encode(body.encode()).decode().rstrip("=")


def decode_cursor(s: str) -> Cursor:
    try:
        # Re-pad — we strip `=` on encode for compact URLs.
        padded = s + "=" * (-len(s) % 4)
        raw = base64.urlsafe_b64decode(padded.encode()).decode()
        obj = json.loads(raw)
        submitted_at = _aware_utc(datetime.fromisoformat(obj["t"]))
        return Cursor(submitted_at=submitted_at, id=UUID(obj["i"]))
    except (
        binascii.Error,
        json.JSONDecodeError,
        OverflowError,
        ValueError,
        UnicodeDecodeError,
        KeyError,
        TypeError,
    ) as exc:
        raise ValueError(f"invalid cursor: {exc}") from exc
