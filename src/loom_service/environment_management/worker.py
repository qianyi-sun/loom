"""Restartable environment executor. No provider call holds a DB transaction."""

from __future__ import annotations

import asyncio
from uuid import UUID

from loom_service.environment_management.provider import (
    EnvironmentProvider,
    ProviderBlockedError,
    ProviderRetryError,
    ProviderWaitingError,
)
from loom_service.environment_management.registry import (
    EnvironmentRegistry,
    ManagementError,
    OperationLease,
)


class EnvironmentWorker:
    def __init__(
        self, registry: EnvironmentRegistry, provider: EnvironmentProvider, *,
        lease_seconds: int = 60, step_timeout: float = 45, max_attempts: int = 20,
        readiness_poll_seconds: float = 2, readiness_timeout: float = 900,
    ):
        if not 3 <= lease_seconds <= 300 or not 0 < step_timeout <= 300 or not 1 <= max_attempts <= 100:
            raise ValueError("invalid environment worker limits")
        if not 0 < readiness_poll_seconds <= 30 or not 0 < readiness_timeout <= 1800:
            raise ValueError("invalid readiness limits")
        self.registry = registry
        self.provider = provider
        self.lease_seconds = lease_seconds
        self.step_timeout = step_timeout
        self.max_attempts = max_attempts
        self.readiness_poll_seconds = readiness_poll_seconds
        self.readiness_timeout = readiness_timeout

    async def _heartbeat(self, lease: OperationLease) -> None:
        while True:
            await asyncio.sleep(self.lease_seconds / 3)
            await self.registry.renew(lease, lease_seconds=self.lease_seconds)

    async def _advance(self, lease: OperationLease) -> None:
        while (step := await self.registry.next_step(lease)) is not None:
            deadline = asyncio.get_running_loop().time() + self.readiness_timeout
            while True:
                context = await self.registry.provisioning_context(lease)
                try:
                    async with asyncio.timeout(self.step_timeout):
                        identity = await self.provider.apply(context, step)
                    break
                except ProviderWaitingError:
                    if asyncio.get_running_loop().time() >= deadline:
                        raise ProviderBlockedError("environment_readiness_timeout") from None
                    await asyncio.sleep(self.readiness_poll_seconds)
            await self.registry.confirm_step(lease, step.key, provider_identity=identity)
        await self.registry.complete(lease)

    async def reconcile_once(self, operation_id: UUID) -> None:
        lease = await self.registry.claim(operation_id, lease_seconds=self.lease_seconds)
        if lease is None:
            return
        heartbeat = asyncio.create_task(self._heartbeat(lease))
        work = asyncio.create_task(self._advance(lease))
        try:
            done, _ = await asyncio.wait({heartbeat, work}, return_when=asyncio.FIRST_COMPLETED)
            # A failed heartbeat cancels in-flight work. Cancellation cannot undo
            # a sent provider request: its immutable intent stays charged/durable.
            for task in done:
                task.result()
        except ManagementError:
            # A successor owns the lease; no stale error or success may overwrite it.
            return
        except (ProviderRetryError, TimeoutError) as exc:
            code = exc.code if isinstance(exc, ProviderRetryError) else "provider_timeout"
            await self._failure(lease, code, retry=lease.runner_epoch < self.max_attempts)
        except ProviderBlockedError as exc:
            await self._failure(lease, exc.code, retry=False)
        except Exception:
            await self._failure(lease, "provider_internal_error", retry=False)
        finally:
            heartbeat.cancel()
            work.cancel()
            await asyncio.gather(heartbeat, work, return_exceptions=True)

    async def _failure(self, lease: OperationLease, code: str, *, retry: bool) -> None:
        try:
            await self.registry.finish_attempt(lease, error_code=code, retry=retry)
        except ManagementError:
            pass  # Lease loss fences error reporting too.

    async def run(self, *, concurrency: int = 4, poll_seconds: float = 5) -> None:
        if not 1 <= concurrency <= 16 or not 1 <= poll_seconds <= 60:
            raise ValueError("invalid environment worker loop limits")
        while True:
            operations = await self.registry.runnable_operations(limit=concurrency)
            async with asyncio.TaskGroup() as group:
                for identity in operations:
                    group.create_task(self.reconcile_once(identity))
            await asyncio.sleep(poll_seconds)
