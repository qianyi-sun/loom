import asyncio
from typing import Any
from unittest.mock import AsyncMock, MagicMock

from loom_drivers.daytona.registry import LiveSandboxRegistry, run_cleanup_sync


async def test_register_unregister_roundtrip() -> None:
    reg = LiveSandboxRegistry()
    sdk = MagicMock()
    sdk.delete = AsyncMock()
    sb = MagicMock(id="sb-1")
    reg.register(sdk, sb)
    assert reg.count() == 1
    reg.unregister(sb)
    assert reg.count() == 0
    sdk.delete.assert_not_awaited()


async def test_cleanup_deletes_registered_within_budget() -> None:
    reg = LiveSandboxRegistry()
    sdk = MagicMock()
    sdk.delete = AsyncMock()
    sbs = [MagicMock(id=f"sb-{i}") for i in range(3)]
    for sb in sbs:
        reg.register(sdk, sb)
    deleted = await reg.cleanup(budget_sec=5.0)
    assert deleted == 3
    assert reg.count() == 0
    assert sdk.delete.await_count == 3


async def test_cleanup_honours_budget_on_slow_delete() -> None:
    reg = LiveSandboxRegistry()
    sdk = MagicMock()

    async def _slow_delete(sb: Any, timeout: float = 60) -> None:
        await asyncio.sleep(0.5)

    sdk.delete = _slow_delete
    for i in range(20):
        reg.register(sdk, MagicMock(id=f"sb-{i}"))
    deleted = await reg.cleanup(budget_sec=0.6)
    assert deleted < 20
    assert reg.count() == 20 - deleted


def test_run_cleanup_sync_executes_in_new_loop() -> None:
    reg = LiveSandboxRegistry()
    sdk = MagicMock()
    sdk.delete = AsyncMock()
    reg.register(sdk, MagicMock(id="sb-x"))
    deleted = run_cleanup_sync(reg, budget_sec=2.0)
    assert deleted == 1
