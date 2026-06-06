"""Crash detector — reclaims trials whose worker stopped heartbeating
(spec §3.10). Task 13 fills in the actual reclaim SQL; this stub keeps
the app factory wired and the lifespan happy.
"""

from __future__ import annotations

import asyncio
from typing import Any


async def run_crash_detector_loop(
    *,
    session_factory: Any,
    expiry_sec: int,
    interval_sec: int,
) -> None:
    """Background coroutine. Sleeps until cancelled; Task 13 replaces the
    body with the real reclaim sweep."""
    while True:
        await asyncio.sleep(interval_sec)
