from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import uuid4

import httpx
import pytest
from starlette.requests import Request

from loom.auth import AuthContext
from loom_llm_gateway.attempt_deadline import (
    AttemptDeadlineReachedError,
    GatewayAttemptDeadline,
    bind_request_attempt_deadline,
    request_attempt_deadline,
)
from loom_llm_gateway.retry import send_with_retry


class _Clock:
    def __init__(self, value: float = 0.0) -> None:
        self.value = value

    def __call__(self) -> float:
        return self.value


class _RetrySettings:
    llm_retry_max_attempts = 3
    llm_retry_base_backoff_sec = 5.0
    llm_retry_jitter_sec = 0.0
    llm_retry_max_backoff_sec = 5.0
    llm_retry_budget_sec = 30.0


def _request() -> Request:
    return Request({"type": "http", "method": "POST", "path": "/"})


def _context(deadline: datetime | None) -> AuthContext:
    return AuthContext(
        token_hash=b"",
        type="step_session",
        scopes=["llm:call"],
        team_id=uuid4(),
        expires_at=None,
        attempt_deadline_wall_clock=deadline,
    )


def test_wall_deadline_is_translated_once_without_cleanup_reserve() -> None:
    wall_now = datetime(2026, 9, 4, 12, tzinfo=UTC)
    clock = _Clock(100.0)
    request = _request()
    deadline = bind_request_attempt_deadline(
        request,
        _context(wall_now + timedelta(seconds=12)),
        wall_now=wall_now,
        monotonic_now=clock(),
        clock=clock,
    )

    assert deadline is not None
    assert deadline.monotonic_cutoff == 112.0
    clock.value = 102.0
    assert request_attempt_deadline(request) is deadline
    assert deadline.remaining() == 10.0


@pytest.mark.parametrize(
    ("configured", "expected"),
    [(30.0, 3.5), (1.25, 1.25)],
)
def test_httpx_timeout_caps_every_phase_to_remaining(
    configured: float,
    expected: float,
) -> None:
    clock = _Clock(5.0)
    deadline = GatewayAttemptDeadline(8.5, clock=clock)

    timeout = deadline.httpx_timeout(configured)

    assert timeout.connect == expected
    assert timeout.read == expected
    assert timeout.write == expected
    assert timeout.pool == expected


@pytest.mark.asyncio
async def test_expired_deadline_dispatches_zero_times() -> None:
    clock = _Clock(10.0)
    deadline = GatewayAttemptDeadline(10.0, clock=clock)
    calls = 0

    async def send() -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(200)

    with pytest.raises(AttemptDeadlineReachedError):
        await send_with_retry(
            send,
            settings=_RetrySettings(),  # type: ignore[arg-type]
            dialect="test",
            deadline=deadline,
        )
    assert calls == 0


@pytest.mark.asyncio
async def test_provider_error_cannot_win_deadline_race() -> None:
    clock = _Clock()
    deadline = GatewayAttemptDeadline(1.0, clock=clock)

    async def fail_after_deadline() -> Any:
        clock.value = 1.0
        raise httpx.ConnectError("provider error")

    with pytest.raises(AttemptDeadlineReachedError):
        await deadline.run(fail_after_deadline)


@pytest.mark.asyncio
async def test_retry_backoff_never_crosses_deadline() -> None:
    clock = _Clock()
    deadline = GatewayAttemptDeadline(3.0, clock=clock)
    calls = 0
    sleeps: list[float] = []

    async def send() -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(
            503,
            request=httpx.Request("POST", "https://provider.invalid"),
        )

    async def sleep(delay: float) -> None:
        sleeps.append(delay)
        clock.value += delay

    with pytest.raises(AttemptDeadlineReachedError):
        await send_with_retry(
            send,
            settings=_RetrySettings(),  # type: ignore[arg-type]
            dialect="test",
            deadline=deadline,
            sleep=sleep,
            now=clock,
        )
    assert calls == 1
    assert sleeps == []


@pytest.mark.asyncio
async def test_stream_item_completing_at_cutoff_is_not_yielded() -> None:
    from loom_llm_gateway.metrics import ATTEMPT_DEADLINE_REACHED_TOTAL

    clock = _Clock()
    deadline = GatewayAttemptDeadline(2.0, clock=clock)
    before = ATTEMPT_DEADLINE_REACHED_TOTAL._value.get()  # type: ignore[attr-defined]

    async def source() -> Any:
        clock.value = 2.0
        yield b"too-late"

    iterator = source()
    with pytest.raises(AttemptDeadlineReachedError):
        await deadline.anext(iterator)
    after = ATTEMPT_DEADLINE_REACHED_TOTAL._value.get()  # type: ignore[attr-defined]
    assert after == before + 1


@pytest.mark.asyncio
async def test_legacy_request_without_deadline_keeps_retry_contract() -> None:
    calls = 0

    async def send() -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(200)

    outcome = await send_with_retry(
        send,
        settings=_RetrySettings(),  # type: ignore[arg-type]
        dialect="legacy",
    )
    assert outcome.response.status_code == 200
    assert calls == 1
