from typing import Any
from unittest.mock import AsyncMock, MagicMock

from loom_drivers.daytona.images import WarmPool


async def test_warm_pool_disabled_short_circuits() -> None:
    sdk = MagicMock()
    sdk.create = AsyncMock(side_effect=AssertionError("should not be called"))
    pool = WarmPool(sdk=sdk, image="python:3.12-slim", size=0)
    assert pool.enabled is False
    await pool.prefill()
    assert pool.available_count == 0


async def test_warm_pool_prefills_to_capacity() -> None:
    sdk = MagicMock()
    created: list[Any] = []

    async def _create(params: Any) -> Any:
        sb = MagicMock(id=f"sb-{len(created)}")
        created.append(sb)
        return sb

    sdk.create = _create
    pool = WarmPool(sdk=sdk, image="python:3.12-slim", size=2)
    await pool.prefill()
    assert pool.available_count == 2


async def test_warm_pool_acquire_returns_prewarmed() -> None:
    sdk = MagicMock()

    async def _create(params: Any) -> Any:
        return MagicMock(id="sb-warm")

    sdk.create = _create
    pool = WarmPool(sdk=sdk, image="python:3.12-slim", size=1)
    await pool.prefill()
    sb = await pool.acquire(timeout_sec=1.0)
    assert sb.id == "sb-warm"
    assert pool.available_count == 0


async def test_warm_pool_acquire_falls_back_when_empty() -> None:
    sdk = MagicMock()
    n = 0

    async def _create(params: Any) -> Any:
        nonlocal n
        n += 1
        return MagicMock(id=f"sb-{n}")

    sdk.create = _create
    pool = WarmPool(sdk=sdk, image="python:3.12-slim", size=0)
    sb = await pool.acquire(timeout_sec=1.0)
    assert sb.id == "sb-1"
