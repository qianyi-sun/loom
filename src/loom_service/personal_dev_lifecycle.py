"""Service-owned background loop for durable personal-dev reconciliation."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from loom.dev_instance_provisioner import OwnerAccessSnapshot
from loom.personal_dev_environment import (
    PersonalDevLifecycleLimits,
    PersonalDevReconciliationClaim,
)
from loom.personal_dev_environment_store import SqlAlchemyPersonalDevEnvironmentAuthority
from loom.personal_dev_reconciler import (
    PersonalDevEnvironmentReconciler,
    PersonalDevPreparationExecutor,
)
from loom_service.dev_instance_access import load_owner_access_snapshot_by_binding

logger = logging.getLogger(__name__)


class SessionPersonalDevReconciliationAuthority:
    """Give every durable transition a short independent DB session."""

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        *,
        limits: PersonalDevLifecycleLimits,
    ) -> None:
        self._session_factory = session_factory
        self._limits = limits

    async def _call(self, method: str, **kwargs: Any) -> Any:
        async with self._session_factory() as session:
            authority = SqlAlchemyPersonalDevEnvironmentAuthority(
                session,
                limits=self._limits,
            )
            return await getattr(authority, method)(**kwargs)

    async def claim_next_reconciliation(self, **kwargs: Any) -> Any:
        return await self._call("claim_next_reconciliation", **kwargs)

    async def begin_activation(self, **kwargs: Any) -> Any:
        return await self._call("begin_activation", **kwargs)

    async def heartbeat_reconciliation(self, **kwargs: Any) -> Any:
        return await self._call("heartbeat_reconciliation", **kwargs)

    async def fail_pre_activation(self, **kwargs: Any) -> Any:
        return await self._call("fail_pre_activation", **kwargs)

    async def complete_activation(self, **kwargs: Any) -> Any:
        return await self._call("complete_activation", **kwargs)


def personal_dev_access_loader(
    session_factory: async_sessionmaker[AsyncSession],
) -> Callable[[PersonalDevReconciliationClaim], Awaitable[OwnerAccessSnapshot]]:
    async def load(claim: PersonalDevReconciliationClaim) -> OwnerAccessSnapshot:
        async with session_factory() as session:
            return await load_owner_access_snapshot_by_binding(
                session,
                owner_user_id=claim.operation.owner_user_id,
                owner_team_id=claim.operation.owner_team_id,
                binding=claim.attempt.access_binding,
            )

    return load


async def personal_dev_reconcile_run_loop(
    *,
    session_factory: async_sessionmaker[AsyncSession],
    executor: PersonalDevPreparationExecutor,
    limits: PersonalDevLifecycleLimits,
    reconciler_id: str,
    lease_seconds: int,
    poll_interval_seconds: float,
) -> None:
    if poll_interval_seconds <= 0:
        raise ValueError("personal-dev reconcile poll interval must be positive")
    authority = SessionPersonalDevReconciliationAuthority(
        session_factory,
        limits=limits,
    )
    reconciler = PersonalDevEnvironmentReconciler(
        authority=authority,
        executor=executor,
        access_loader=personal_dev_access_loader(session_factory),
        reconciler_id=reconciler_id,
        lease_seconds=lease_seconds,
    )
    while True:
        try:
            progressed = await reconciler.reconcile_once(now=datetime.now(UTC))
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.error(
                "personal_dev_reconcile_iteration_failed",
                extra={"error_type": type(exc).__name__},
            )
            progressed = False
        if not progressed:
            await asyncio.sleep(poll_interval_seconds)


__all__ = [
    "SessionPersonalDevReconciliationAuthority",
    "personal_dev_access_loader",
    "personal_dev_reconcile_run_loop",
]
