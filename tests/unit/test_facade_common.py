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


def test_redact_api_key_strips_signed_urls_and_internal_endpoints() -> None:
    out = redact_api_key(
        "provider failed via http://loom-llm-gateway:9100/v1/chat; "
        "artifact=https://minio.internal:9000/artifacts/a/b?"
        "X-Amz-Signature=abcdef&X-Amz-Credential=minio",
        "",
    )

    assert "loom-llm-gateway" not in out
    assert "minio.internal" not in out
    assert "X-Amz-Signature=abcdef" not in out
    assert "provider failed via" in out


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
    resolve_optional_provider_connection_id,
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


def _platform_bound_step_ctx() -> AuthContext:
    return AuthContext(
        token_hash=b"",
        type="step_session",
        scopes=["llm:call"],
        team_id=uuid4(),
        expires_at=datetime.now(UTC),
        trial_id=uuid4(),
        step_id="family_evolver",
        provider_connection_id=None,
        provider_connection_id_bound=True,
    )


def test_resolve_jwt_only() -> None:
    """No header, JWT-only path (post-transition shape)."""
    conn = uuid4()
    assert resolve_provider_connection_id(_step_ctx(conn), None) == conn


def test_resolve_header_only() -> None:
    """Legacy path: JWT scope has no connection_id, header is used."""
    conn = uuid4()
    assert resolve_provider_connection_id(_step_ctx(None), str(conn)) == conn


def test_resolve_both_match() -> None:
    """Canonical transition case: sandbox sends both, they agree."""
    conn = uuid4()
    assert resolve_provider_connection_id(_step_ctx(conn), str(conn)) == conn


def test_resolve_both_mismatch_400_with_authoritative_message() -> None:
    """Mismatch ⇒ 400. The message MUST say the JWT is authoritative
    so operators know which one to align."""
    jwt_conn = uuid4()
    header_conn = uuid4()
    with pytest.raises(HTTPException) as exc:
        resolve_provider_connection_id(
            _step_ctx(jwt_conn),
            str(header_conn),
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
    assert resolve_provider_connection_id(_step_ctx(conn), "") == conn
    # Both empty/None ⇒ 400.
    with pytest.raises(HTTPException):
        resolve_provider_connection_id(_step_ctx(None), "")


def test_resolve_optional_neither_returns_none() -> None:
    assert (
        resolve_optional_provider_connection_id(
            _step_ctx(None),
            header_value=None,
            body_value=None,
        )
        is None
    )


@pytest.mark.parametrize("source", ["header", "body"])
def test_resolve_optional_platform_bound_jwt_rejects_provider_override(
    source: str,
) -> None:
    conn = str(uuid4())
    with pytest.raises(HTTPException) as exc:
        resolve_optional_provider_connection_id(
            _platform_bound_step_ctx(),
            header_value=conn if source == "header" else None,
            body_value=conn if source == "body" else None,
        )
    assert exc.value.status_code == 400
    assert "JWT scope says platform" in exc.value.detail
    assert "JWT scope is authoritative" in exc.value.detail


def test_resolve_optional_body_only() -> None:
    conn = uuid4()
    assert (
        resolve_optional_provider_connection_id(
            _step_ctx(None),
            header_value=None,
            body_value=str(conn),
        )
        == conn
    )


def test_resolve_optional_header_and_body_match() -> None:
    conn = uuid4()
    assert (
        resolve_optional_provider_connection_id(
            _step_ctx(None),
            header_value=str(conn),
            body_value=str(conn),
        )
        == conn
    )


def test_resolve_optional_header_and_body_mismatch() -> None:
    with pytest.raises(HTTPException) as exc:
        resolve_optional_provider_connection_id(
            _step_ctx(None),
            header_value=str(uuid4()),
            body_value=str(uuid4()),
        )
    assert exc.value.status_code == 400
    assert "body says" in exc.value.detail
    assert "header says" in exc.value.detail


def test_resolve_optional_jwt_rejects_body_mismatch() -> None:
    jwt_conn = uuid4()
    with pytest.raises(HTTPException) as exc:
        resolve_optional_provider_connection_id(
            _step_ctx(jwt_conn),
            header_value=str(jwt_conn),
            body_value=str(uuid4()),
        )
    assert exc.value.status_code == 400
    assert "JWT scope is authoritative" in exc.value.detail
    assert "body says" in exc.value.detail


def test_resolve_optional_malformed_body_400() -> None:
    with pytest.raises(HTTPException) as exc:
        resolve_optional_provider_connection_id(
            _step_ctx(None),
            header_value=None,
            body_value="not-a-uuid",
        )
    assert exc.value.status_code == 400
    assert "loom.provider_connection_id is not a valid UUID" in exc.value.detail


# ──────────────────────────────────────────────────────────────────────
# decrypt_facade_api_key — #423 controlled-error contract
# ──────────────────────────────────────────────────────────────────────


from types import SimpleNamespace  # noqa: E402
from typing import Any  # noqa: E402

from loom.security.secret_store import (  # noqa: E402
    DecryptError,
    InvalidRefError,
    SecretNotFoundError,
)
from loom_llm_gateway.routes._facade_common import (  # noqa: E402
    decrypt_facade_api_key,
)


def _row_with_ref(ref: str) -> SimpleNamespace:
    """Minimal ProviderConnection stand-in. decrypt_facade_api_key only
    reads .id and .encrypted_api_key_ref on the row — duck-typed."""
    return SimpleNamespace(id=uuid4(), encrypted_api_key_ref=ref)


class _StubStore:
    """Replaces LocalEncryptedSecretStore in the helper for these
    tests so we can drive each error class without standing up a real
    session/DB. Patched via monkeypatch in the test below."""

    def __init__(self, exc: Exception | None = None, value: str = "sk-x"):
        self._exc = exc
        self._value = value

    def __call__(self, _session: Any) -> _StubStore:
        return self

    async def get(self, _ref: str) -> str:
        if self._exc is not None:
            raise self._exc
        return self._value


async def test_decrypt_facade_api_key_happy_returns_decrypted_key(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    stub = _StubStore(value="sk-decrypted")
    monkeypatch.setattr(
        "loom_llm_gateway.routes._facade_common.LocalEncryptedSecretStore",
        stub,
    )
    out = await decrypt_facade_api_key(
        object(),
        _row_with_ref("loom://team:abc/" + str(uuid4())),
    )
    assert out == "sk-decrypted"


async def test_decrypt_facade_api_key_malformed_ref_returns_controlled_502(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Legacy env:X refs were the #423 trigger. They surface as
    InvalidRefError; facade must return 502 with an actionable detail,
    not bubble a 500."""
    stub = _StubStore(
        exc=InvalidRefError("ref missing scheme separator: 'env:LEGACY'"),
    )
    monkeypatch.setattr(
        "loom_llm_gateway.routes._facade_common.LocalEncryptedSecretStore",
        stub,
    )
    with pytest.raises(HTTPException) as exc:
        await decrypt_facade_api_key(
            object(),
            _row_with_ref("env:LEGACY"),
        )
    assert exc.value.status_code == 502
    assert "malformed_ref" in exc.value.detail
    assert "rotate-key" in exc.value.detail
    assert "env:LEGACY" not in exc.value.detail
    assert "ref missing scheme separator" not in exc.value.detail
    assert exc.value.__cause__ is None


async def test_decrypt_facade_api_key_missing_secret_returns_controlled_502(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    raw_ref = "loom://team:secret/00000000-0000-0000-0000-000000000001"
    stub = _StubStore(exc=SecretNotFoundError(f"no secret for ref {raw_ref!r}"))
    monkeypatch.setattr(
        "loom_llm_gateway.routes._facade_common.LocalEncryptedSecretStore",
        stub,
    )
    with pytest.raises(HTTPException) as exc:
        await decrypt_facade_api_key(
            object(),
            _row_with_ref("loom://team:abc/" + str(uuid4())),
        )
    assert exc.value.status_code == 502
    assert "missing_secret" in exc.value.detail
    assert raw_ref not in exc.value.detail
    assert "no secret for ref" not in exc.value.detail
    assert exc.value.__cause__ is None


async def test_decrypt_facade_api_key_decrypt_failed_returns_controlled_502(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    raw_ref = "loom://team:secret/00000000-0000-0000-0000-000000000002"
    stub = _StubStore(exc=DecryptError(f"AEAD verification failed for {raw_ref!r}"))
    monkeypatch.setattr(
        "loom_llm_gateway.routes._facade_common.LocalEncryptedSecretStore",
        stub,
    )
    with pytest.raises(HTTPException) as exc:
        await decrypt_facade_api_key(
            object(),
            _row_with_ref("loom://team:abc/" + str(uuid4())),
        )
    assert exc.value.status_code == 502
    assert "decrypt_failed" in exc.value.detail
    assert "master key" in exc.value.detail
    assert raw_ref not in exc.value.detail
    assert "AEAD verification failed" not in exc.value.detail
    assert exc.value.__cause__ is None
