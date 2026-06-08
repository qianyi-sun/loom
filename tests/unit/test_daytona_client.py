from unittest.mock import AsyncMock, MagicMock

import pytest

from loom_drivers.daytona.client import DaytonaClient
from loom_drivers.daytona.config import DaytonaConfig


@pytest.fixture
def cfg(monkeypatch: pytest.MonkeyPatch) -> DaytonaConfig:
    monkeypatch.setenv("DAYTONA_API_KEY", "k")
    return DaytonaConfig.from_env()


async def test_open_close_lifecycle(
    cfg: DaytonaConfig, monkeypatch: pytest.MonkeyPatch,
) -> None:
    mock_sdk = MagicMock()
    mock_sdk.close = AsyncMock()
    monkeypatch.setattr(
        "loom_drivers.daytona.client._build_async_daytona",
        lambda c: mock_sdk,
    )
    client = DaytonaClient(cfg)
    await client.open()
    assert client.is_open
    await client.close()
    assert not client.is_open
    mock_sdk.close.assert_awaited_once()


async def test_close_is_idempotent(
    cfg: DaytonaConfig, monkeypatch: pytest.MonkeyPatch,
) -> None:
    mock_sdk = MagicMock()
    mock_sdk.close = AsyncMock()
    monkeypatch.setattr(
        "loom_drivers.daytona.client._build_async_daytona", lambda c: mock_sdk,
    )
    client = DaytonaClient(cfg)
    await client.open()
    await client.close()
    await client.close()
    mock_sdk.close.assert_awaited_once()


async def test_use_before_open_raises(cfg: DaytonaConfig) -> None:
    client = DaytonaClient(cfg)
    with pytest.raises(RuntimeError, match="not open"):
        _ = client.sdk
