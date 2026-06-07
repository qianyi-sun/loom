"""Cursor pagination utility for trials list (spec §3).

The cursor encodes `(submitted_at, id)` as base64(json). Deliberately
NOT a sealed JWT: cursors are debug-friendly (a developer can decode
one by hand) and don't need integrity protection — they're returned
to the same caller who sent them, only carry public DB-sort keys,
and the queries downstream re-filter by team scope.
"""

from __future__ import annotations

import base64
import binascii
import json
from dataclasses import dataclass
from datetime import datetime
from uuid import UUID


@dataclass(frozen=True)
class Cursor:
    submitted_at: datetime
    id: UUID


def encode_cursor(c: Cursor) -> str:
    body = json.dumps({"t": c.submitted_at.isoformat(), "i": str(c.id)})
    return base64.urlsafe_b64encode(body.encode()).decode().rstrip("=")


def decode_cursor(s: str) -> Cursor:
    try:
        # Re-pad — we strip `=` on encode for compact URLs.
        padded = s + "=" * (-len(s) % 4)
        raw = base64.urlsafe_b64decode(padded.encode()).decode()
        obj = json.loads(raw)
        return Cursor(
            submitted_at=datetime.fromisoformat(obj["t"]),
            id=UUID(obj["i"]),
        )
    except (
        binascii.Error,
        json.JSONDecodeError,
        ValueError,
        UnicodeDecodeError,
        KeyError,
        TypeError,
    ) as exc:
        raise ValueError(f"invalid cursor: {exc}") from exc
