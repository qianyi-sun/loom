from __future__ import annotations

import logging
from datetime import UTC, datetime
from types import SimpleNamespace
from uuid import uuid4

import pytest
from fastapi import HTTPException
from starlette.requests import Request

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
    from loom_llm_gateway.metrics import AUTH_REJECTIONS_TOTAL

    before = AUTH_REJECTIONS_TOTAL.labels(reason="missing_scope")._value.get()  # type: ignore[attr-defined]
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
    after = AUTH_REJECTIONS_TOTAL.labels(reason="missing_scope")._value.get()  # type: ignore[attr-defined]
    assert after == before + 1


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


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "reason",
    ["missing", "malformed", "invalid_signature", "expired"],
)
async def test_bearer_rejection_metric_uses_only_bounded_reason_labels(
    monkeypatch: pytest.MonkeyPatch,
    reason: BearerValidationReason,
) -> None:
    from loom_llm_gateway.metrics import AUTH_REJECTIONS_TOTAL

    async def _validate(*args: object, **kwargs: object) -> BearerValidationResult:
        return BearerValidationResult(context=None, reason=reason)

    monkeypatch.setattr(_auth, "validate_bearer_token", _validate)
    before = AUTH_REJECTIONS_TOTAL.labels(reason=reason)._value.get()  # type: ignore[attr-defined]
    with pytest.raises(HTTPException):
        await _auth.require_llm_call_bearer(
            _UnusedSession(),
            "Bearer never-exported",
            signing_key="unused",
        )
    after = AUTH_REJECTIONS_TOTAL.labels(reason=reason)._value.get()  # type: ignore[attr-defined]
    assert after == before + 1


async def test_expired_signed_attempt_deadline_returns_stable_504(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    context = AuthContext(
        token_hash=b"",
        type="step_session",
        scopes=["llm:call"],
        team_id=uuid4(),
        expires_at=datetime.now(UTC),
        attempt_deadline_wall_clock=datetime.now(UTC),
    )

    async def _validate(*args: object, **kwargs: object) -> BearerValidationResult:
        return BearerValidationResult(context=context, reason="valid")

    monkeypatch.setattr(_auth, "validate_bearer_token", _validate)
    request = Request({"type": "http", "method": "POST", "path": "/"})
    with pytest.raises(HTTPException) as exc:
        await _auth.require_llm_call_bearer(
            _UnusedSession(),
            "Bearer never-exported",
            signing_key="unused",
            request=request,
        )
    assert exc.value.status_code == 504
    assert exc.value.detail == {
        "code": "agent_timeout",
        "reason": "attempt_deadline_reached",
    }


@pytest.mark.parametrize(
    ("compat_seconds", "expected_status", "metric_outcome"),
    [(86400.0, None, "accepted"), (0.0, 401, "rejected")],
)
async def test_legacy_deadline_compatibility_is_bounded_and_can_fail_closed(
    monkeypatch: pytest.MonkeyPatch,
    compat_seconds: float,
    expected_status: int | None,
    metric_outcome: str,
) -> None:
    from loom_llm_gateway.metrics import LEGACY_ATTEMPT_DEADLINE_TOKEN_TOTAL

    context = AuthContext(
        token_hash=b"",
        type="step_session",
        scopes=["llm:call"],
        team_id=uuid4(),
        expires_at=datetime.now(UTC),
        attempt_deadline_wall_clock=None,
    )

    async def _validate(*args: object, **kwargs: object) -> BearerValidationResult:
        return BearerValidationResult(context=context, reason="valid")

    monkeypatch.setattr(_auth, "validate_bearer_token", _validate)
    app = SimpleNamespace(
        state=SimpleNamespace(
            settings=SimpleNamespace(
                legacy_attempt_deadline_compat_sec=compat_seconds,
            )
        )
    )
    request = Request(
        {"type": "http", "method": "POST", "path": "/", "app": app}
    )
    before = LEGACY_ATTEMPT_DEADLINE_TOKEN_TOTAL.labels(
        outcome=metric_outcome
    )._value.get()  # type: ignore[attr-defined]

    if expected_status is None:
        assert (
            await _auth.require_llm_call_bearer(
                _UnusedSession(),
                "Bearer never-exported",
                signing_key="unused",
                request=request,
            )
            is context
        )
    else:
        with pytest.raises(HTTPException) as exc:
            await _auth.require_llm_call_bearer(
                _UnusedSession(),
                "Bearer never-exported",
                signing_key="unused",
                request=request,
            )
        assert exc.value.status_code == expected_status
        assert exc.value.detail == {"code": "invalid_or_expired_bearer"}

    after = LEGACY_ATTEMPT_DEADLINE_TOKEN_TOTAL.labels(
        outcome=metric_outcome
    )._value.get()  # type: ignore[attr-defined]
    assert after == before + 1
