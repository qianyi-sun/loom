"""Per-process image handle cache.

Modal hashes image definitions server-side, but ``from_registry()`` still
does an RPC. Caching the returned ``modal.Image`` handle inside the worker
process eliminates the roundtrip for repeat starts of the same image
(common for benchmark runs that fire N trials against one agent image).
"""

from __future__ import annotations

import asyncio
from typing import Any

from loom_drivers.modal.client import ModalClient

_ImageKey = tuple[str, tuple[str, ...]]


def _key(base: str, pip_packages: list[str] | None) -> _ImageKey:
    pkgs = tuple(sorted(pip_packages)) if pip_packages else ()
    return (base, pkgs)


class ModalImageCache:
    """In-process image handle cache.

    Concurrency: a per-key ``asyncio.Lock`` collapses concurrent ``get()``
    calls for the same key into a single build. The lock is dropped from
    ``_locks`` once the image is cached, so ``_locks`` only ever holds
    entries for in-flight builds — bounded by concurrency, not by the
    total number of distinct images seen.
    """

    def __init__(self, client: ModalClient) -> None:
        self._client = client
        self._entries: dict[_ImageKey, Any] = {}
        self._locks: dict[_ImageKey, asyncio.Lock] = {}
        self._dict_lock = asyncio.Lock()

    async def _lock_for(self, key: _ImageKey) -> asyncio.Lock:
        async with self._dict_lock:
            lk = self._locks.get(key)
            if lk is None:
                lk = asyncio.Lock()
                self._locks[key] = lk
            return lk

    async def get(
        self, *, base: str, pip_packages: list[str] | None = None,
    ) -> Any:
        key = _key(base, pip_packages)
        cached = self._entries.get(key)
        if cached is not None:
            return cached
        lock = await self._lock_for(key)
        async with lock:
            cached = self._entries.get(key)
            if cached is not None:
                return cached
            img = await self._client.build_image(
                base=base, pip_packages=pip_packages,
            )
            self._entries[key] = img
        # Once the entry exists, future callers hit the fast-path
        # `_entries.get()` and never touch the per-key lock. Drop it so
        # `_locks` doesn't accumulate one entry per distinct image
        # fingerprint seen over the process lifetime.
        async with self._dict_lock:
            self._locks.pop(key, None)
        return img
