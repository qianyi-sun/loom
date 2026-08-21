"""Shared provider backpressure and durable cleanup for a Daytona controller."""

from __future__ import annotations

import asyncio
import hashlib
import time
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any
from uuid import UUID

from loom_drivers.daytona.client import DaytonaClient
from loom_drivers.daytona.config import DaytonaConfig


def provider_scope(config: DaytonaConfig) -> str:
    """Credential-bound account/target fingerprint used to fence cleanup."""

    credential = config.api_key or config.jwt_token
    if credential is None:
        raise ValueError("Daytona provider scope requires a credential")
    material = "\0".join((config.api_url or "default", config.target or "default", credential))
    return hashlib.sha256(material.encode("utf-8")).hexdigest()


@dataclass
class DaytonaApiGate:
    max_concurrent: int
    min_interval_sec: float
    _semaphore: asyncio.Semaphore = field(init=False, repr=False)
    _spacing_lock: asyncio.Lock = field(init=False, repr=False)
    _last_started_at: float = field(default=0.0, init=False, repr=False)

    def __post_init__(self) -> None:
        if self.max_concurrent <= 0:
            raise ValueError("Daytona API concurrency must be positive")
        if self.min_interval_sec < 0:
            raise ValueError("Daytona API minimum interval must be non-negative")
        self._semaphore = asyncio.Semaphore(self.max_concurrent)
        self._spacing_lock = asyncio.Lock()

    @asynccontextmanager
    async def slot(self) -> AsyncIterator[None]:
        async with self._semaphore:
            async with self._spacing_lock:
                now = time.monotonic()
                delay = self.min_interval_sec - (now - self._last_started_at)
                if delay > 0:
                    await asyncio.sleep(delay)
                self._last_started_at = time.monotonic()
            yield


async def reconcile_one(
    *,
    cp_client: Any,
    worker_id: UUID,
    config: DaytonaConfig,
    gate: DaytonaApiGate,
) -> bool:
    """Delete one cleanup-eligible sandbox claimed from the durable ledger."""

    record = await cp_client.claim_daytona_cleanup(
        worker_id=worker_id,
        provider_scope=provider_scope(config),
    )
    if record is None:
        return False
    ledger_id = UUID(str(record["id"]))
    sandbox_ref = record.get("sandbox_id") or record["sandbox_name"]
    deleted = False
    error: str | None = None
    client = DaytonaClient(config)
    await client.open()
    try:
        try:
            from daytona import DaytonaError

            async with gate.slot():
                try:
                    sandbox = await client.sdk.get(str(sandbox_ref))
                except DaytonaError as exc:
                    if exc.status_code == 404:
                        deleted = True
                    else:
                        raise
            if not deleted:
                async with gate.slot():
                    await asyncio.wait_for(
                        client.sdk.delete(sandbox, timeout=config.delete_timeout_sec),
                        timeout=config.delete_timeout_sec + 5.0,
                    )
                deleted = True
        except Exception as exc:  # provider failures are journaled for retry
            error = f"{type(exc).__name__}: {exc}"
    finally:
        await client.close()
    await cp_client.report_daytona_deleted(
        worker_id=worker_id,
        ledger_id=ledger_id,
        deleted=deleted,
        stopped_at=datetime.now(tz=UTC).isoformat(),
        error=error,
    )
    return True
