"""Unit tests for `_facade_common.py` — the helpers shared by every
facade route. Route-level integration tests cover end-to-end
behavior; this file pins the pure-function pieces."""

from __future__ import annotations

import pytest
from fastapi import HTTPException

from loom_llm_gateway.routes._facade_common import (
    parse_connection_id_header,
    redact_api_key,
)

# ──────────────────────────────────────────────────────────────────────
# parse_connection_id_header
# ──────────────────────────────────────────────────────────────────────


def test_parse_connection_id_happy() -> None:
    out = parse_connection_id_header("00000000-0000-0000-0000-000000000001")
    assert str(out) == "00000000-0000-0000-0000-000000000001"


def test_parse_connection_id_missing_raises_400() -> None:
    with pytest.raises(HTTPException) as exc:
        parse_connection_id_header(None)
    assert exc.value.status_code == 400
    assert "x-loom-provider-connection-id" in exc.value.detail


def test_parse_connection_id_empty_string_raises_400() -> None:
    with pytest.raises(HTTPException) as exc:
        parse_connection_id_header("")
    assert exc.value.status_code == 400


def test_parse_connection_id_malformed_raises_400() -> None:
    with pytest.raises(HTTPException) as exc:
        parse_connection_id_header("not-a-uuid")
    assert exc.value.status_code == 400
    assert "not a valid UUID" in exc.value.detail


# ──────────────────────────────────────────────────────────────────────
# redact_api_key
# ──────────────────────────────────────────────────────────────────────


def test_redact_api_key_strips_known_secret() -> None:
    out = redact_api_key("Bearer sk-LIVE-XYZ; valid", "sk-LIVE-XYZ")
    assert "sk-LIVE-XYZ" not in out
    assert "[REDACTED]" in out


def test_redact_api_key_truncates_to_limit() -> None:
    """500-char default truncation matches the facade routes' behavior."""
    long_body = "x" * 2000
    out = redact_api_key(long_body, "fake-key")
    assert len(out) == 500


def test_redact_api_key_custom_limit() -> None:
    out = redact_api_key("x" * 100, "k", limit=10)
    assert len(out) == 10


def test_redact_api_key_4_char_minimum() -> None:
    """Same guard as `provider_connections_service._redact_secret`:
    keys shorter than 4 chars don't trigger redaction to avoid
    over-redacting common short substrings."""
    assert redact_api_key("hello world", "") == "hello world"
    assert redact_api_key("hello world", "ab") == "hello world"
    assert redact_api_key("hello world", "abc") == "hello world"
    assert redact_api_key("hello world", "world") == "hello [REDACTED]"


def test_redact_api_key_empty_input() -> None:
    assert redact_api_key("", "sk-XYZ-key") == ""
    # Also tolerate None-ish (the helper passes through `text or ""`)
    assert redact_api_key(None, "sk-XYZ-key") == ""  # type: ignore[arg-type]
