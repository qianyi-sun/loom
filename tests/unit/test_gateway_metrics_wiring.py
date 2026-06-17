"""Unit tests for #81 slice B-2 — gateway metrics wired into the
facade routes via record_call + /metrics endpoint."""

from __future__ import annotations

import pytest
from prometheus_client import REGISTRY


@pytest.fixture(autouse=True)
def _isolate_env_from_dotenv(monkeypatch: pytest.MonkeyPatch) -> None:
    """Strip dev `.env` LOOM_* keys that aren't gateway fields."""
    for k in (
        "LOOM_WORKER_TOKEN", "LOOM_TEAM_TOKEN", "LOOM_ADMIN_TOKEN",
    ):
        monkeypatch.delenv(k, raising=False)


def test_metrics_endpoint_mounted_on_gateway_app() -> None:
    from loom_llm_gateway.app import create_app
    from loom_llm_gateway.config import GatewaySettings

    settings = GatewaySettings(
        _env_file=None,  # type: ignore[call-arg]
        db_url="postgresql+asyncpg://x@y/z",
        step_jwt_signing_key="x" * 32,
    )
    app = create_app(settings)
    mounts = [
        r for r in app.routes
        if hasattr(r, "path") and r.path.startswith("/metrics")
    ]
    assert mounts, "no /metrics route on gateway app"


def test_gateway_metric_objects_registered() -> None:
    from loom_llm_gateway import metrics  # noqa: F401

    names = {m.name for m in REGISTRY.collect()}
    expected = {
        "loom_gateway_llm_calls",      # Counter — exposed with _total suffix
        "loom_gateway_llm_call_latency_sec",
        "loom_gateway_cost_usd",       # Counter — exposed with _total
        "loom_gateway_provider_validation",
    }
    for stem in expected:
        assert stem in names or f"{stem}_total" in names or any(
            n.startswith(stem) for n in names
        ), f"metric stem {stem!r} missing"


def test_record_call_increments_llm_calls_total() -> None:
    """record_call() with a successful call must bump
    loom_gateway_llm_calls_total{provider, dialect, result=\"ok\"}."""
    import asyncio
    from unittest.mock import AsyncMock
    from uuid import uuid4

    from loom_llm_gateway.dialect import TokenUsage
    from loom_llm_gateway.llm_calls import record_call
    from loom_llm_gateway.metrics import LLM_CALLS_TOTAL

    before = LLM_CALLS_TOTAL.labels(
        provider="openai", dialect="openai-facade", result="ok",
    )._value.get()  # type: ignore[attr-defined]

    fake_session = AsyncMock()
    asyncio.run(record_call(
        fake_session,
        team_id=uuid4(),
        trial_id=uuid4(),
        step_id="step-1",
        dialect="openai-facade",
        model="gpt-4o-mini",
        usage=TokenUsage(input_tokens=10, output_tokens=20, provider_extras={}),
        cost_usd=0.0,
        rate_card_hash="rate-card:test",
        provider="openai",
    ))

    after = LLM_CALLS_TOTAL.labels(
        provider="openai", dialect="openai-facade", result="ok",
    )._value.get()  # type: ignore[attr-defined]
    assert after == before + 1


def test_record_call_skips_cost_when_zero() -> None:
    """COST_USD_TOTAL only increments for positive costs; zero-cost
    calls (rate-card-missing / operator-supplied=0) don't pollute
    the counter."""
    import asyncio
    from unittest.mock import AsyncMock
    from uuid import uuid4

    from loom_llm_gateway.dialect import TokenUsage
    from loom_llm_gateway.llm_calls import record_call
    from loom_llm_gateway.metrics import COST_USD_TOTAL

    team_id = uuid4()
    before = COST_USD_TOTAL.labels(
        team_id=str(team_id), provider="openai",
    )._value.get()  # type: ignore[attr-defined]

    fake_session = AsyncMock()
    asyncio.run(record_call(
        fake_session,
        team_id=team_id,
        trial_id=uuid4(),
        step_id="step",
        dialect="openai-facade",
        model="m",
        usage=TokenUsage(input_tokens=0, output_tokens=0, provider_extras={}),
        cost_usd=0.0,
        rate_card_hash="x",
        provider="openai",
    ))

    after = COST_USD_TOTAL.labels(
        team_id=str(team_id), provider="openai",
    )._value.get()  # type: ignore[attr-defined]
    assert after == before


def test_record_call_increments_cost_when_positive() -> None:
    import asyncio
    from unittest.mock import AsyncMock
    from uuid import uuid4

    from loom_llm_gateway.dialect import TokenUsage
    from loom_llm_gateway.llm_calls import record_call
    from loom_llm_gateway.metrics import COST_USD_TOTAL

    team_id = uuid4()
    fake_session = AsyncMock()
    asyncio.run(record_call(
        fake_session,
        team_id=team_id,
        trial_id=uuid4(),
        step_id="step",
        dialect="openai-facade",
        model="m",
        usage=TokenUsage(input_tokens=10, output_tokens=20, provider_extras={}),
        cost_usd=0.012345,
        rate_card_hash="x",
        provider="openai",
    ))

    after = COST_USD_TOTAL.labels(
        team_id=str(team_id), provider="openai",
    )._value.get()  # type: ignore[attr-defined]
    assert after >= 0.012345
