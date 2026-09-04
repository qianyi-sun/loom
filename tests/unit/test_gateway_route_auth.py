from __future__ import annotations

import logging
from datetime import UTC, datetime
from uuid import uuid4

import pytest
from fastapi import HTTPException

from loom.auth import AuthContext, BearerValidationReason, BearerValidationResult
from loom_llm_gateway.routes import _auth


class _UnusedSession:
    pass


@pytest.mark.parametrize(
    "reason",
    ["missing", "malformed", "invalid_signature", "expired"],
)
async def test_invalid_bearer_reasons_share_one_public_401_contract(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    reason: BearerValidationReason,
) -> None:
    raw_token = "loom_step_must-never-appear-in-logs"

    async def _validate(*args: object, **kwargs: object) -> BearerValidationResult:
        return BearerValidationResult(context=None, reason=reason)

    monkeypatch.setattr(_auth, "validate_bearer_token", _validate)
    with caplog.at_level(logging.INFO), pytest.raises(HTTPException) as exc:
        await _auth.require_llm_call_bearer(
            _UnusedSession(),
            f"Bearer {raw_token}",
            signing_key="unused",
        )

    assert exc.value.status_code == 401
    assert exc.value.detail == {"code": "invalid_or_expired_bearer"}
    assert exc.value.headers == {"WWW-Authenticate": 'Bearer error="invalid_token"'}
    assert raw_token not in caplog.text
    assert caplog.records[-1].bearer_validation_reason == reason  # type: ignore[attr-defined]


async def test_valid_bearer_without_llm_scope_returns_structured_403(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    context = AuthContext(
        token_hash=b"",
        type="team",
        scopes=["submit"],
        team_id=uuid4(),
        expires_at=datetime.now(UTC),
    )

    async def _validate(*args: object, **kwargs: object) -> BearerValidationResult:
        return BearerValidationResult(context=context, reason="valid")

    monkeypatch.setattr(_auth, "validate_bearer_token", _validate)
    with pytest.raises(HTTPException) as exc:
        await _auth.require_llm_call_bearer(
            _UnusedSession(),
            "Bearer opaque",
            signing_key="unused",
        )

    assert exc.value.status_code == 403
    assert exc.value.detail == {
        "code": "missing_scope",
        "required_scope": "llm:call",
    }


async def test_valid_llm_bearer_returns_context(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    context = AuthContext(
        token_hash=b"",
        type="step_session",
        scopes=["llm:call"],
        team_id=uuid4(),
        expires_at=datetime.now(UTC),
    )

    async def _validate(*args: object, **kwargs: object) -> BearerValidationResult:
        return BearerValidationResult(context=context, reason="valid")

    monkeypatch.setattr(_auth, "validate_bearer_token", _validate)

    assert (
        await _auth.require_llm_call_bearer(
            _UnusedSession(),
            "Bearer opaque",
            signing_key="unused",
        )
        is context
    )
