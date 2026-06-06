"""_phase_network — async context manager that temporarily switches the
driver's network policy for the duration of a phase and restores the
baseline on exit (spec §3.4).

asyncio.shield on the restore so cancellation during the phase still
restores the baseline rather than leaving the container in an unknown
network state.
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import AsyncIterator

from loom.driver.base import Driver
from loom.models.networking import NetworkPolicy


@contextlib.asynccontextmanager
async def phase_network(
    driver: Driver, *, baseline: NetworkPolicy, phase: NetworkPolicy,
) -> AsyncIterator[None]:
    if phase == baseline:
        yield
        return
    await driver.set_network_policy(phase)
    try:
        yield
    finally:
        await asyncio.shield(driver.set_network_policy(baseline))
