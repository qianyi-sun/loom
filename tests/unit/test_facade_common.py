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


def test_redact_api_key_strips_bearer_header_value_without_known_secret() -> None:
    out = redact_api_key(
        "received_args.response_object.headers={Authorization: Bearer sk-LIVE-XYZ}",
        "",
    )

    assert "sk-LIVE-XYZ" not in out
    assert "Bearer [REDACTED]" in out


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


# ──────────────────────────────────────────────────────────────────────
# resolve_provider_connection_id — issue #72 dual-source logic
# ──────────────────────────────────────────────────────────────────────


from datetime import UTC, datetime  # noqa: E402
from uuid import UUID, uuid4  # noqa: E402

from loom.auth import AuthContext  # noqa: E402
from loom_llm_gateway.routes._facade_common import (  # noqa: E402
    resolve_provider_connection_id,
)


def _step_ctx(provider_connection_id: UUID | None) -> AuthContext:
    """Helper: build a step-scoped AuthContext for tests."""
    return AuthContext(
        token_hash=b"",
        type="step_session",
        scopes=["llm:call"],
        team_id=uuid4(),
        expires_at=datetime.now(UTC),
        trial_id=uuid4(),
        step_id="step-1",
        provider_connection_id=provider_connection_id,
    )


def test_resolve_jwt_only() -> None:
    """No header, JWT-only path (post-transition shape)."""
    conn = uuid4()
    assert (
        resolve_provider_connection_id(_step_ctx(conn), None)
        == conn
    )


def test_resolve_header_only() -> None:
    """Legacy path: JWT scope has no connection_id, header is used."""
    conn = uuid4()
    assert (
        resolve_provider_connection_id(_step_ctx(None), str(conn))
        == conn
    )


def test_resolve_both_match() -> None:
    """Canonical transition case: sandbox sends both, they agree."""
    conn = uuid4()
    assert (
        resolve_provider_connection_id(_step_ctx(conn), str(conn))
        == conn
    )


def test_resolve_both_mismatch_400_with_authoritative_message() -> None:
    """Mismatch ⇒ 400. The message MUST say the JWT is authoritative
    so operators know which one to align."""
    jwt_conn = uuid4()
    header_conn = uuid4()
    with pytest.raises(HTTPException) as exc:
        resolve_provider_connection_id(
            _step_ctx(jwt_conn), str(header_conn),
        )
    assert exc.value.status_code == 400
    assert "JWT scope is authoritative" in exc.value.detail
    assert str(jwt_conn) in exc.value.detail
    assert str(header_conn) in exc.value.detail


def test_resolve_neither_400() -> None:
    """Neither source set ⇒ 400 with a hint pointing at both options."""
    with pytest.raises(HTTPException) as exc:
        resolve_provider_connection_id(_step_ctx(None), None)
    assert exc.value.status_code == 400
    assert "x-loom-provider-connection-id" in exc.value.detail
    assert "step-JWT" in exc.value.detail


def test_resolve_malformed_header_400() -> None:
    """A non-UUID header value 400s with a clear parse error."""
    with pytest.raises(HTTPException) as exc:
        resolve_provider_connection_id(_step_ctx(None), "not-a-uuid")
    assert exc.value.status_code == 400
    assert "not a valid UUID" in exc.value.detail


def test_resolve_empty_string_header_treated_as_absent() -> None:
    """Empty header value should fall through to the JWT path (or
    400 if JWT also empty), not 400 on parse."""
    conn = uuid4()
    # JWT has it, header is empty ⇒ JWT wins.
    assert (
        resolve_provider_connection_id(_step_ctx(conn), "")
        == conn
    )
    # Both empty/None ⇒ 400.
    with pytest.raises(HTTPException):
        resolve_provider_connection_id(_step_ctx(None), "")
