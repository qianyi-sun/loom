import asyncio
import time
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from loom_drivers.daytona.config import DaytonaConfig
from loom_drivers.daytona.driver import DaytonaDriver
from loom_drivers.daytona.registry import LiveSandboxRegistry


@pytest.fixture
def cfg(monkeypatch: pytest.MonkeyPatch) -> DaytonaConfig:
    monkeypatch.setenv("DAYTONA_API_KEY", "k")
    return DaytonaConfig.from_env()


async def test_cancel_path_completes_within_30s(
    cfg: DaytonaConfig, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Simulate cancel: task issues Driver.stop() while delete on Daytona's
    side takes 25s. The driver must return within 30s (with margin)."""
    sdk = MagicMock()
    sdk.close = AsyncMock()

    async def _slow_delete(sb: Any, timeout: float = 60) -> None:
        await asyncio.sleep(25.0)

    sdk.delete = _slow_delete

    sb = MagicMock(id="sb-cancel")
    sb.update_network_settings = AsyncMock()
    sdk.create = AsyncMock(return_value=sb)
    monkeypatch.setattr(
        "loom_drivers.daytona.client._build_async_daytona", lambda c: sdk,
    )

    drv = DaytonaDriver(image="img", config=cfg)
    await drv.start()
    t0 = time.monotonic()
    await drv.stop(delete=True)
    elapsed = time.monotonic() - t0
    assert elapsed < 30.0, f"stop() blocked for {elapsed:.1f}s, > 30s budget"


async def test_registry_cleanup_under_30s_with_many_sandboxes() -> None:
    """20 sandboxes × parallel delete must finish under 30s budget."""
    reg = LiveSandboxRegistry()
    sdk = MagicMock()

    async def _delete(sb: Any, timeout: float = 60) -> None:
        await asyncio.sleep(0.5)

    sdk.delete = _delete

    for i in range(20):
        reg.register(sdk, MagicMock(id=f"sb-{i}"))

    t0 = time.monotonic()
    deleted = await reg.cleanup(budget_sec=30.0)
    elapsed = time.monotonic() - t0
    assert deleted == 20, f"only {deleted}/20 deleted in {elapsed:.1f}s"
    assert elapsed < 30.0
