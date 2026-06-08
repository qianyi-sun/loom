"""Optional warm-pool of pre-created Daytona sandboxes for fast cold starts.

When `size > 0` the pool pre-creates that many sandboxes at driver start;
`acquire()` returns a pre-warmed one if available, otherwise creates on
demand. Pre-warmed sandboxes are NOT returned to the pool on release
(single-use; we don't trust intra-sandbox state cleanup).
"""

from __future__ import annotations

import asyncio
from typing import Any


class WarmPool:
    def __init__(self, *, sdk: Any, image: str, size: int) -> None:
        self._sdk = sdk
        self._image = image
        self._size = size
        self._queue: asyncio.Queue[Any] = asyncio.Queue(maxsize=max(size, 1))

    @property
    def enabled(self) -> bool:
        return self._size > 0

    @property
    def available_count(self) -> int:
        return self._queue.qsize()

    async def prefill(self) -> None:
        if not self.enabled:
            return
        from daytona import CreateSandboxFromImageParams
        for _ in range(self._size):
            sb = await self._sdk.create(
                CreateSandboxFromImageParams(image=self._image),
            )
            await self._queue.put(sb)

    async def acquire(self, *, timeout_sec: float = 30.0) -> Any:
        if self.enabled:
            try:
                return await asyncio.wait_for(
                    self._queue.get(), timeout=timeout_sec,
                )
            except TimeoutError:
                pass
        from daytona import CreateSandboxFromImageParams
        return await self._sdk.create(
            CreateSandboxFromImageParams(image=self._image),
        )

    async def drain(self) -> list[Any]:
        out: list[Any] = []
        while not self._queue.empty():
            out.append(self._queue.get_nowait())
        return out
