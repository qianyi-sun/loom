from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import uuid4

import pytest

from loom_pipeline_orchestrator.main_loop import OrchestratorContext, run_once
from loom_pipeline_orchestrator.repository import RunLease


class FakeRepository:
    def __init__(self, count: int = 1) -> None:
        self.leases = [
            RunLease(uuid4(), "controller-a", 1, datetime.now(UTC) + timedelta(seconds=60))
            for _ in range(count)
        ]
        self.lease = self.leases[0]
        self.released: list[RunLease] = []

    async def claim_runs(self, **_kwargs: Any) -> list[RunLease]:
        return self.leases

    async def renew(self, lease: RunLease) -> RunLease:
        return lease

    async def release(self, lease: RunLease) -> None:
        self.released.append(lease)


@pytest.mark.asyncio
async def test_iteration_reconciles_and_releases_owned_lease() -> None:
    repository = FakeRepository()
    reconciled: list[RunLease] = []

    async def reconcile(lease: RunLease) -> None:
        reconciled.append(lease)

    context = OrchestratorContext(
        repository=repository,  # type: ignore[arg-type]
        controller_id="controller-a",
        reconcile=reconcile,
    )
    assert await run_once(context) == 1
    assert reconciled == [repository.lease]
    assert repository.released == [repository.lease]


@pytest.mark.asyncio
async def test_claimed_batch_reconciles_concurrently_before_leases_age() -> None:
    repository = FakeRepository(count=2)
    both_started = asyncio.Event()
    started: set[object] = set()

    async def reconcile(lease: RunLease) -> None:
        started.add(lease.pipeline_run_id)
        if len(started) == 2:
            both_started.set()
        await asyncio.wait_for(both_started.wait(), timeout=1)

    context = OrchestratorContext(
        repository=repository,  # type: ignore[arg-type]
        controller_id="controller-a",
        reconcile=reconcile,
    )
    assert await run_once(context) == 2
    assert started == {lease.pipeline_run_id for lease in repository.leases}
    assert set(repository.released) == set(repository.leases)
