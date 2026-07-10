"""Redaction tests for Terminus-2 observations (#744)."""

from __future__ import annotations

from loom.security.redaction import redact_text


def test_redact_text_strips_openai_key() -> None:
    raw = "key=sk-abcdefghijklmnopqrstuvwxyz123456"
    out = redact_text(raw)
    assert "sk-abc" not in out
    assert "[REDACTED" in out
