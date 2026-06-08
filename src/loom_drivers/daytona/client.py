"""Thin lifecycle wrapper around AsyncDaytona.

The driver layer holds a DaytonaClient, not the raw SDK, so we can:
- mock the seam in unit tests (monkeypatch _build_async_daytona)
- centralise retry policy for transient SDK errors
- enforce closed-after-stop semantics
"""

from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable
from typing import Any, TypeVar

from tenacity import (
    AsyncRetrying,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from loom_drivers.daytona.config import DaytonaConfig

logger = logging.getLogger(__name__)

_T = TypeVar("_T")


def _build_async_daytona(cfg: DaytonaConfig) -> Any:
    """Construct the upstream AsyncDaytona. Isolated for monkeypatching."""
    from daytona import AsyncDaytona
    return AsyncDaytona(cfg.to_sdk_config())


class DaytonaClient:
    def __init__(self, cfg: DaytonaConfig) -> None:
        self._cfg = cfg
        self._sdk: Any | None = None

    @property
    def is_open(self) -> bool:
        return self._sdk is not None

    @property
    def sdk(self) -> Any:
        if self._sdk is None:
            raise RuntimeError("DaytonaClient is not open")
        return self._sdk

    async def open(self) -> None:
        if self._sdk is not None:
            return
        self._sdk = _build_async_daytona(self._cfg)

    async def close(self) -> None:
        if self._sdk is None:
            return
        sdk = self._sdk
        self._sdk = None
        try:
            await sdk.close()
        except Exception:
            logger.warning(
                "DaytonaClient.close failed; sandbox may leak", exc_info=True,
            )

    async def with_retry(
        self, op: Callable[[], Awaitable[_T]], *, attempts: int = 3,
    ) -> _T:
        """Run an async callable with exponential backoff on transient errors."""
        from daytona import DaytonaError

        async for attempt in AsyncRetrying(
            stop=stop_after_attempt(attempts),
            wait=wait_exponential(multiplier=0.5, min=0.5, max=4.0),
            retry=retry_if_exception_type(DaytonaError),
            reraise=True,
        ):
            with attempt:
                return await op()
        raise RuntimeError("unreachable")
