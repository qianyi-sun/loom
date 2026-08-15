"""Replay-safe publication loop for executable protected-release outbox events."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from typing import Protocol
from uuid import UUID, uuid5

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from loom_capacity_agent.admission import (
    ProtectedReleasePublicationCheckpointV2,
    PublishableExecutableProtectedReleaseV2,
)
from loom_capacity_agent.client import ExecutableProtectedReleasePublishReceiptV2
from loom_capacity_agent.contracts import AgentRegistrationV1, ReporterConfigurationV1
from loom_capacity_agent.store import (
    CapacityAgentStoreError,
    acknowledge_executable_protected_release_publication,
    read_next_executable_protected_release,
)
from loom_capacity_manager.executable_contracts import canonical_executable_digest

logger = logging.getLogger(__name__)

_RELEASE_PUBLICATION_NAMESPACE = UUID("37629c7f-d49d-483d-98ca-01fa2280f2c1")


class ExecutableProtectedReleasePublisher(Protocol):
    async def publish_executable_protected_release(
        self,
        publication: PublishableExecutableProtectedReleaseV2,
        *,
        idempotency_key: UUID,
    ) -> ExecutableProtectedReleasePublishReceiptV2: ...


ReadNextExecutableProtectedRelease = Callable[
    ...,
    Awaitable[PublishableExecutableProtectedReleaseV2 | None],
]
AcknowledgeExecutableProtectedRelease = Callable[
    ...,
    Awaitable[ProtectedReleasePublicationCheckpointV2],
]


def stable_release_publication_key(publication: PublishableExecutableProtectedReleaseV2) -> UUID:
    """Derive one deterministic idempotency key for a stable outbox publication."""

    if not isinstance(publication, PublishableExecutableProtectedReleaseV2):
        raise TypeError("protected release publication must be a schema-v2 executable report")
    if canonical_executable_digest(publication.release) != publication.publication_digest:
        raise ValueError("protected release publication digest changed")
    binding = publication.release.binding
    return uuid5(
        _RELEASE_PUBLICATION_NAMESPACE,
        (
            f"{publication.event_id}:"
            f"{publication.publication_digest}:"
            f"{binding.subject_id}:"
            f"{binding.subject_incarnation}"
        ),
    )


class ExecutableProtectedReleaseReporterRuntime:
    """Publish and acknowledge one executable protected-release event at a time."""

    def __init__(
        self,
        *,
        configuration: ReporterConfigurationV1,
        session_factory: async_sessionmaker[AsyncSession],
        publisher: ExecutableProtectedReleasePublisher,
        read_next: ReadNextExecutableProtectedRelease = read_next_executable_protected_release,
        acknowledge: AcknowledgeExecutableProtectedRelease = (
            acknowledge_executable_protected_release_publication
        ),
    ) -> None:
        self._registration = AgentRegistrationV1.model_validate(
            {field: getattr(configuration, field) for field in AgentRegistrationV1.model_fields}
        )
        self._session_factory = session_factory
        self._publisher = publisher
        self._read_next = read_next
        self._acknowledge = acknowledge
        self._initialized = False
        self.ready = False

    async def initialize(self) -> None:
        self._initialized = True
        self.ready = False

    async def run_once(self) -> None:
        if not self._initialized:
            raise CapacityAgentStoreError("capacity agent is not initialized")
        try:
            async with self._session_factory() as session, session.begin():
                publication = await self._read_next(
                    session,
                    registration=self._registration,
                )
            if publication is None:
                self.ready = True
                return
            receipt = await self._publisher.publish_executable_protected_release(
                publication,
                idempotency_key=stable_release_publication_key(publication),
            )
            async with self._session_factory() as session, session.begin():
                await self._acknowledge(
                    session,
                    registration=self._registration,
                    publication=publication,
                    manager_acknowledgement_digest=receipt.receipt_digest,
                )
        except BaseException:
            self.ready = False
            raise
        self.ready = True

    async def run_forever(self, *, poll_interval_seconds: float) -> None:
        if not 0 < poll_interval_seconds <= 300:
            raise ValueError("capacity agent poll interval must be between 0 and 300 seconds")
        await self.initialize()
        while True:
            try:
                await self.run_once()
            except asyncio.CancelledError:
                raise
            except CapacityAgentStoreError as exc:
                self.ready = False
                logger.error(
                    "capacity_agent_executable_release_store_error",
                    extra={"error_type": type(exc).__name__},
                )
                await asyncio.sleep(poll_interval_seconds)
                await self.initialize()
                continue
            except Exception as exc:
                self.ready = False
                logger.error(
                    "capacity_agent_executable_release_iteration_failed",
                    extra={"error_type": type(exc).__name__},
                )
            await asyncio.sleep(poll_interval_seconds)


__all__ = [
    "ExecutableProtectedReleasePublisher",
    "ExecutableProtectedReleaseReporterRuntime",
    "stable_release_publication_key",
]
