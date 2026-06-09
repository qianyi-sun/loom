"""ModalImageCache — in-process image handle cache."""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock


async def test_image_cache_reuses_for_same_key() -> None:
    from loom_drivers.modal.images import ModalImageCache

    client = MagicMock()
    img = MagicMock(name="image_v1")
    client.build_image = AsyncMock(return_value=img)

    cache = ModalImageCache(client)
    a = await cache.get(base="python:3.12-slim", pip_packages=["requests"])
    b = await cache.get(base="python:3.12-slim", pip_packages=["requests"])
    assert a is b is img
    client.build_image.assert_awaited_once()


async def test_image_cache_different_keys_separate_builds() -> None:
    from loom_drivers.modal.images import ModalImageCache

    client = MagicMock()
    img1 = MagicMock(name="i1")
    img2 = MagicMock(name="i2")
    client.build_image = AsyncMock(side_effect=[img1, img2])

    cache = ModalImageCache(client)
    a = await cache.get(base="python:3.12-slim", pip_packages=["a"])
    b = await cache.get(base="python:3.12-slim", pip_packages=["b"])
    assert a is img1
    assert b is img2
    assert client.build_image.await_count == 2


async def test_image_cache_concurrent_same_key_single_build() -> None:
    """Two concurrent gets for the same key yield ONE build."""
    from loom_drivers.modal.images import ModalImageCache

    client = MagicMock()
    img = MagicMock(name="i1")

    async def slow_build(**_: object) -> object:
        await asyncio.sleep(0.05)
        return img

    client.build_image = AsyncMock(side_effect=slow_build)

    cache = ModalImageCache(client)
    a, b = await asyncio.gather(
        cache.get(base="python:3.12-slim", pip_packages=None),
        cache.get(base="python:3.12-slim", pip_packages=None),
    )
    assert a is b is img
    assert client.build_image.await_count == 1


async def test_image_cache_pip_packages_order_independent() -> None:
    """['a','b'] and ['b','a'] cache to the SAME entry."""
    from loom_drivers.modal.images import ModalImageCache

    client = MagicMock()
    img = MagicMock()
    client.build_image = AsyncMock(return_value=img)

    cache = ModalImageCache(client)
    await cache.get(base="python:3.12-slim", pip_packages=["a", "b"])
    await cache.get(base="python:3.12-slim", pip_packages=["b", "a"])
    client.build_image.assert_awaited_once()
