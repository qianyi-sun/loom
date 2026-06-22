"""`loom_llm_gateway.retry.send_with_retry` + `loom.errors.is_retryable`
(#298 Slice A).

`send_with_retry` is the per-request loop that wraps every upstream
LLM call in `loom_llm_gateway/routes/*`. Tests cover:

- Classification: retryable vs non-retryable statuses + transport errors.
- Success on first attempt: no extra waits, RETRY_ATTEMPTS observed at 1.
- Success after N retries: returns the final 200; waits in between.
- Exhaustion: returns the last 5xx response after max_attempts.
- Non-retryable status (401): returns immediately, no retries.
- Budget exceeded: stops before max_attempts when wall-clock budget runs out.
- Transport error: retries httpx.TimeoutException; raises after exhaustion.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import httpx
import pytest

from loom.errors import is_retryable
from loom_llm_gateway.retry import send_with_retry


@dataclass
class _StubSettings:
    """Mirror of the codegen'd GatewaySettings llm_retry_* fields."""

    llm_retry_max_attempts: int = 3
    llm_retry_base_backoff_sec: float = 0.01
    llm_retry_jitter_sec: float = 0.0
    llm_retry_max_backoff_sec: float = 1.0
    llm_retry_budget_sec: float = 30.0


def _resp(status: int, body: dict[str, Any] | None = None) -> httpx.Response:
    return httpx.Response(
        status, request=httpx.Request("POST", "http://upstream"),
        json=body or {},
    )


# ─── is_retryable classification ─────────────────────────────────────


@pytest.mark.parametrize("status", [502, 503, 504, 408, 429, 500])
def test_is_retryable_retryable_statuses(status: int) -> None:
    err = httpx.HTTPStatusError(
        "x", request=httpx.Request("POST", "http://u"),
        response=_resp(status),
    )
    assert is_retryable(err) is True


@pytest.mark.parametrize("status", [400, 401, 403, 404, 422])
def test_is_retryable_deterministic_4xx_not_retryable(status: int) -> None:
    err = httpx.HTTPStatusError(
        "x", request=httpx.Request("POST", "http://u"),
        response=_resp(status),
    )
    assert is_retryable(err) is False


def test_is_retryable_2xx_not_retryable() -> None:
    """is_retryable is for *exceptions*; a successful response never
    becomes an HTTPStatusError. But guard anyway."""
    err = httpx.HTTPStatusError(
        "x", request=httpx.Request("POST", "http://u"),
        response=_resp(200),
    )
    assert is_retryable(err) is False


@pytest.mark.parametrize("exc_cls", [
    httpx.ConnectTimeout,
    httpx.ReadTimeout,
    httpx.PoolTimeout,
    httpx.ConnectError,
    httpx.ReadError,
    httpx.RemoteProtocolError,
])
def test_is_retryable_transport_errors(exc_cls: type) -> None:
    exc = exc_cls("transient transport blip")
    assert is_retryable(exc) is True


def test_is_retryable_unrelated_exception_false() -> None:
    assert is_retryable(ValueError("not a network thing")) is False


# ─── send_with_retry ────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_send_with_retry_success_first_attempt() -> None:
    """No retries needed → returns response, no sleeps."""
    sleeps: list[float] = []
    settings = _StubSettings()
    call_count = 0

    async def send() -> httpx.Response:
        nonlocal call_count
        call_count += 1
        return _resp(200)

    async def sleep(_s: float) -> None:
        sleeps.append(_s)

    resp = await send_with_retry(
        send, settings=settings, dialect="test", sleep=sleep,
    )
    assert resp.status_code == 200
    assert call_count == 1
    assert sleeps == []


@pytest.mark.asyncio
async def test_send_with_retry_retries_5xx_then_succeeds() -> None:
    """Three 503s then a 200 → returns 200 on 4th attempt."""
    settings = _StubSettings(llm_retry_max_attempts=4)
    statuses_to_return = [503, 503, 503, 200]
    sleeps: list[float] = []

    async def send() -> httpx.Response:
        return _resp(statuses_to_return.pop(0))

    async def sleep(s: float) -> None:
        sleeps.append(s)

    resp = await send_with_retry(
        send, settings=settings, dialect="test", sleep=sleep,
    )
    assert resp.status_code == 200
    # 3 retries → 3 sleeps
    assert len(sleeps) == 3


@pytest.mark.asyncio
async def test_send_with_retry_exhausts_returns_last_5xx() -> None:
    """Persistent 503 → returns 503 after max_attempts (caller raises)."""
    settings = _StubSettings(llm_retry_max_attempts=3)
    attempts = 0

    async def send() -> httpx.Response:
        nonlocal attempts
        attempts += 1
        return _resp(503)

    async def sleep(_s: float) -> None:
        pass

    resp = await send_with_retry(
        send, settings=settings, dialect="test", sleep=sleep,
    )
    assert resp.status_code == 503
    assert attempts == 3


@pytest.mark.asyncio
async def test_send_with_retry_non_retryable_4xx_immediate() -> None:
    """401 → returns immediately, no retries."""
    settings = _StubSettings()
    attempts = 0
    sleeps: list[float] = []

    async def send() -> httpx.Response:
        nonlocal attempts
        attempts += 1
        return _resp(401)

    async def sleep(s: float) -> None:
        sleeps.append(s)

    resp = await send_with_retry(
        send, settings=settings, dialect="test", sleep=sleep,
    )
    assert resp.status_code == 401
    assert attempts == 1
    assert sleeps == []


@pytest.mark.asyncio
async def test_send_with_retry_transport_exhaust_raises() -> None:
    """Persistent ConnectTimeout → raises after max_attempts."""
    settings = _StubSettings(llm_retry_max_attempts=2)
    attempts = 0

    async def send() -> httpx.Response:
        nonlocal attempts
        attempts += 1
        raise httpx.ConnectTimeout("nope")

    async def sleep(_s: float) -> None:
        pass

    with pytest.raises(httpx.ConnectTimeout):
        await send_with_retry(
            send, settings=settings, dialect="test", sleep=sleep,
        )
    assert attempts == 2


@pytest.mark.asyncio
async def test_send_with_retry_non_retryable_exception_immediate() -> None:
    """ValueError → propagates immediately, no retries."""
    settings = _StubSettings()
    attempts = 0

    async def send() -> httpx.Response:
        nonlocal attempts
        attempts += 1
        raise httpx.HTTPError("non-retryable subclass missing?")  # actually retryable type root

    async def sleep(_s: float) -> None:
        pass

    # httpx.HTTPError is the base — not in our retryable set, not classified
    # as retryable by is_retryable. Verify it propagates.
    with pytest.raises(httpx.HTTPError):
        await send_with_retry(
            send, settings=settings, dialect="test", sleep=sleep,
        )
    assert attempts == 1


@pytest.mark.asyncio
async def test_send_with_retry_budget_exceeded_stops_early() -> None:
    """Budget tight → stops before max_attempts."""
    # 3 attempts allowed, but budget = 0.5s. Backoff at attempt 2 = base
    # (0.5s), so we'd hit budget on the second wait.
    settings = _StubSettings(
        llm_retry_max_attempts=5,
        llm_retry_base_backoff_sec=0.5,
        llm_retry_jitter_sec=0.0,
        llm_retry_max_backoff_sec=10.0,
        llm_retry_budget_sec=0.4,
    )
    attempts = 0
    sleeps: list[float] = []
    clock = [0.0]

    async def send() -> httpx.Response:
        nonlocal attempts
        attempts += 1
        return _resp(503)

    async def sleep(s: float) -> None:
        sleeps.append(s)
        clock[0] += s

    def now() -> float:
        return clock[0]

    resp = await send_with_retry(
        send, settings=settings, dialect="test", sleep=sleep, now=now,
    )
    assert resp.status_code == 503
    # Attempt 1 happens; first backoff (0.5s) would exceed 0.4s budget
    # → exit before sleeping. Should have made exactly 1 attempt.
    assert attempts == 1
    assert sleeps == []


@pytest.mark.asyncio
async def test_send_with_retry_single_attempt_setting_disables_retry() -> None:
    """max_attempts=1 → never retries even on 503."""
    settings = _StubSettings(llm_retry_max_attempts=1)
    attempts = 0
    sleeps: list[float] = []

    async def send() -> httpx.Response:
        nonlocal attempts
        attempts += 1
        return _resp(503)

    async def sleep(s: float) -> None:
        sleeps.append(s)

    resp = await send_with_retry(
        send, settings=settings, dialect="test", sleep=sleep,
    )
    assert resp.status_code == 503
    assert attempts == 1
    assert sleeps == []


@pytest.mark.asyncio
async def test_send_with_retry_504_increments_ambiguous_counter(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """504 retries bump the ambiguous-504 counter (per §D-idempotency)."""
    from loom_llm_gateway import metrics

    initial = metrics.RETRY_AMBIGUOUS_504_TOTAL.labels(dialect="test")._value.get()  # type: ignore[attr-defined]

    settings = _StubSettings(llm_retry_max_attempts=3)
    statuses = [504, 504, 200]

    async def send() -> httpx.Response:
        return _resp(statuses.pop(0))

    async def sleep(_s: float) -> None:
        pass

    resp = await send_with_retry(
        send, settings=settings, dialect="test", sleep=sleep,
    )
    assert resp.status_code == 200
    # Two 504s were retried (the third response was 200 — not counted)
    final = metrics.RETRY_AMBIGUOUS_504_TOTAL.labels(dialect="test")._value.get()  # type: ignore[attr-defined]
    assert final - initial == 2.0
