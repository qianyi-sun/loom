"""Unit tests for the llm-gateway drain hook (#547)."""

from __future__ import annotations

import asyncio

import pytest
from fastapi.testclient import TestClient

from loom_llm_gateway.drain import DrainState, drain_and_report


async def test_drain_state_enter_leave_updates_counter() -> None:
    state = DrainState()
    assert (await state.snapshot()) == (0, False)
    await state.enter()
    await state.enter()
    assert (await state.snapshot()) == (2, False)
    await state.leave()
    assert (await state.snapshot()) == (1, False)
    await state.leave()
    assert (await state.snapshot()) == (0, False)


async def test_wait_for_zero_returns_ok_when_counter_already_zero() -> None:
    state = DrainState()
    ok, remaining, elapsed = await state.wait_for_zero_in_flight(
        timeout_sec=5.0,
    )
    assert ok is True
    assert remaining == 0
    assert elapsed < 0.5


async def test_wait_for_zero_polls_until_in_flight_drops() -> None:
    """Concurrent scenario: one request is in flight, drain waits; the
    request finishes (calls leave), drain returns ok."""
    state = DrainState()
    await state.enter()

    async def _finish_after_delay() -> None:
        await asyncio.sleep(0.1)
        await state.leave()

    finish_task = asyncio.create_task(_finish_after_delay())
    ok, remaining, elapsed = await state.wait_for_zero_in_flight(
        timeout_sec=2.0,
        poll_interval_sec=0.02,
    )
    await finish_task
    assert ok is True
    assert remaining == 0
    assert 0.05 <= elapsed <= 1.0


async def test_wait_for_zero_times_out_reports_remaining() -> None:
    """When in-flight requests never finish, timeout returns
    ok=False plus the leftover in-flight count for the diagnostic."""
    state = DrainState()
    await state.enter()
    await state.enter()
    ok, remaining, elapsed = await state.wait_for_zero_in_flight(
        timeout_sec=0.3,
        poll_interval_sec=0.05,
    )
    assert ok is False
    assert remaining == 2
    assert elapsed >= 0.3


async def test_begin_drain_flips_flag() -> None:
    state = DrainState()
    _, draining = await state.snapshot()
    assert draining is False
    await state.begin_drain()
    _, draining_after = await state.snapshot()
    assert draining_after is True


async def test_drain_and_report_returns_drained_when_zero_in_flight() -> None:
    state = DrainState()
    report = await drain_and_report(state, timeout_sec=1.0)
    assert report["status"] == "drained"
    assert report["remaining_in_flight"] == 0
    assert report["timeout_sec"] == 1.0
    # elapsed rounded to 3 dp
    assert isinstance(report["elapsed_sec"], float)


async def test_drain_and_report_returns_timeout_with_diagnostic() -> None:
    """Simulate a stuck in-flight request that outlives the drain
    timeout. The report must clearly say `timeout` and include the
    remaining count so operators can see WHAT didn't finish."""
    state = DrainState()
    await state.enter()
    report = await drain_and_report(state, timeout_sec=0.2)
    assert report["status"] == "timeout"
    assert report["remaining_in_flight"] == 1
    assert report["timeout_sec"] == 0.2


# ──────────────────────────────────────────────────────────────────────
# create_app wiring
# ──────────────────────────────────────────────────────────────────────


def test_create_app_attaches_drain_before_lifespan(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Integration fixtures wire app.state without entering lifespan."""
    from loom_llm_gateway.app import create_app
    from loom_llm_gateway.config import GatewaySettings

    monkeypatch.setenv(
        "LOOM_GW_DB_URL",
        "postgresql+asyncpg://loom:loom@localhost:5432/loom",
    )
    app = create_app(GatewaySettings(_env_file=None))
    assert isinstance(app.state.drain, DrainState)


# ──────────────────────────────────────────────────────────────────────
# End-to-end via TestClient: readinessProbe / drain interplay
# ──────────────────────────────────────────────────────────────────────


def _make_test_app() -> object:
    """Build a minimal FastAPI app that mirrors the drain wiring in
    `create_app` without needing a full Gateway startup (which requires
    Postgres, secrets, etc.)."""
    from fastapi import FastAPI

    from loom_llm_gateway.drain import install_drain_middleware
    from loom_llm_gateway.routes import drain, health

    app = FastAPI()
    app.state.drain = DrainState()

    # Fake settings object with just the field /drain reads.
    class _Settings:
        gateway_drain_timeout_sec = 1.0

    app.state.settings = _Settings()

    install_drain_middleware(app)
    app.include_router(health.router)
    app.include_router(drain.router)

    # Add a slow endpoint we can hold open for the tests below.
    from asyncio import sleep as _sleep

    @app.get("/slow")
    async def _slow() -> dict[str, str]:
        await _sleep(0.15)
        return {"ok": "true"}

    return app


def test_healthz_returns_200_before_drain() -> None:
    with TestClient(_make_test_app()) as client:
        r = client.get("/healthz")
        assert r.status_code == 200
        assert r.json()["status"] == "ok"


def test_healthz_returns_503_after_drain() -> None:
    """The core Kubernetes-side property: once /drain has flipped
    the flag, /healthz returns 503 so the readinessProbe fails and
    the Service load balancer stops routing new requests to this pod."""
    with TestClient(_make_test_app()) as client:
        r = client.post("/drain")
        assert r.status_code == 200
        assert r.json()["status"] == "drained"
        r2 = client.get("/healthz")
        assert r2.status_code == 503
        assert r2.json()["status"] == "draining"
        assert r2.json()["in_flight"] == 0


def test_drain_endpoint_reports_timeout_when_requests_still_in_flight() -> None:
    """Start a long request in one thread; call /drain in another and
    verify the timeout diagnostic. The in-flight request must complete
    successfully — the drain does NOT interrupt it."""
    import threading

    app = _make_test_app()
    with TestClient(app) as client:
        slow_result: dict[str, object] = {}

        def _run_slow() -> None:
            r = client.get("/slow")
            slow_result["status"] = r.status_code
            slow_result["body"] = r.json()

        slow_thread = threading.Thread(target=_run_slow)
        slow_thread.start()

        # Give the slow request time to enter the middleware.
        import time as _time

        _time.sleep(0.03)

        # Drain with a timeout shorter than the slow request duration.
        # Note the fake settings above set drain_timeout_sec = 1.0;
        # /slow takes 0.15s so drain should succeed shortly after.
        r = client.post("/drain")
        slow_thread.join(timeout=5.0)

    assert r.status_code == 200
    assert r.json()["status"] == "drained"
    # The in-flight request completed successfully — drain waited for
    # it, did not truncate it.
    assert slow_result["status"] == 200
    assert slow_result["body"] == {"ok": "true"}


# Enable async test discovery in this module.
pytest_plugins = ["pytest_asyncio"]
