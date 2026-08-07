"""In-process executor for durably claimed dev-instance lifecycle operations."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable, Coroutine
from functools import partial
from typing import Any
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from loom.dev_instance_provisioner import (
    DevInstanceProvisioner,
    DevInstanceRecord,
    InstanceStore,
    OwnerAccessSnapshot,
)
from loom.dev_instance_store import SqlAlchemyDevInstanceStore

logger = logging.getLogger(__name__)

ProvisionerFactory = Callable[[InstanceStore], DevInstanceProvisioner]


class DevInstanceLifecycleRunner:
    """Run fenced lifecycle effects after an API request returns ``202``.

    The registry claim is committed by the request first. Each operation then
    gets a new database session, so request teardown cannot invalidate it.
    Re-submitting a provisioning/deleting row after a service restart resumes
    the same operation; operation-id de-duplication prevents duplicate local
    runners in one process.
    """

    def __init__(
        self,
        *,
        session_factory: async_sessionmaker[AsyncSession],
        provisioner_factory: ProvisionerFactory,
    ) -> None:
        self._session_factory = session_factory
        self._provisioner_factory = provisioner_factory
        self._tasks: dict[UUID, asyncio.Task[None]] = {}
        self._closing = False

    def submit_create(
        self,
        record: DevInstanceRecord,
        access: OwnerAccessSnapshot,
    ) -> bool:
        return self._submit(
            record,
            self._run_create(record, access),
            action="create",
        )

    def submit_destroy(self, record: DevInstanceRecord) -> bool:
        return self._submit(
            record,
            self._run_destroy(record),
            action="destroy",
        )

    def _submit(
        self,
        record: DevInstanceRecord,
        operation: Coroutine[Any, Any, None],
        *,
        action: str,
    ) -> bool:
        if self._closing:
            operation.close()
            return False
        if record.operation_id in self._tasks:
            operation.close()
            return False
        task = asyncio.create_task(
            operation,
            name=f"loom-dev-instance-{action}-{record.name}-{record.operation_epoch}",
        )
        self._tasks[record.operation_id] = task
        task.add_done_callback(partial(self._done, record.operation_id))
        return True

    def _done(self, operation_id: UUID, task: asyncio.Task[None]) -> None:
        self._tasks.pop(operation_id, None)
        if task.cancelled():
            return
        error = task.exception()
        if error is not None:
            logger.error(
                "dev_instance_lifecycle_failed",
                extra={"error_type": type(error).__name__},
            )

    async def _run_create(
        self,
        record: DevInstanceRecord,
        access: OwnerAccessSnapshot,
    ) -> None:
        async with self._session_factory() as session:
            provisioner = self._provisioner_factory(SqlAlchemyDevInstanceStore(session))
            await provisioner.converge_create(record, access=access)

    async def _run_destroy(self, record: DevInstanceRecord) -> None:
        async with self._session_factory() as session:
            provisioner = self._provisioner_factory(SqlAlchemyDevInstanceStore(session))
            await provisioner.converge_destroy(record)

    async def close(self, *, grace_seconds: float = 30.0) -> None:
        """Stop admission, give active work a grace window, then cancel it."""
        self._closing = True
        tasks = tuple(self._tasks.values())
        if not tasks:
            return
        _done, pending = await asyncio.wait(tasks, timeout=grace_seconds)
        for task in pending:
            task.cancel()
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)


__all__ = ["DevInstanceLifecycleRunner"]
