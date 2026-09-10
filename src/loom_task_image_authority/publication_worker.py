"""Bounded inactive verification service, composed only by trusted application code.

The dedicated signer transport is available for explicit trusted composition,
but there is no production signer/distribution composition or runtime default.
Callers submit fixed operation IDs; they never select dependencies or destinations.
Each admitted operation has one work task and one independently transacted renewal.
"""

from __future__ import annotations

import asyncio
import math
from collections.abc import Callable
from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol
from uuid import UUID, uuid4

from sqlalchemy.exc import DBAPIError, DisconnectionError
from sqlalchemy.exc import TimeoutError as DatabasePoolTimeoutError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from loom_task_image_authority.oci_verification import (
    OCIDescriptor,
    OCIVerificationError,
    OCIVerificationLimits,
    VerifiedOCIGraph,
    verify_oci_graph,
)
from loom_task_image_authority.publication_completion import (
    complete_publication_job,
    read_publication_signing_state,
    replay_completed_publication,
)
from loom_task_image_authority.publication_contracts import PublicationUnsignedInput
from loom_task_image_authority.publication_jobs import (
    PublicationFailureCode,
    PublicationJob,
    PublicationJobAuthorizationError,
    PublicationJobConflictError,
    PublicationJobOwnershipError,
)
from loom_task_image_authority.publication_receipts import PublicationReceipt
from loom_task_image_authority.publication_signing import (
    DistributedKeysetSnapshot,
    PublicationKeyRecord,
    PublicationSigner,
    PublicationState,
    request_publication_signature,
)
from loom_task_image_authority.publication_store import (
    Clock,
    _now,
    _uuid,
    claim_publication_job,
    expire_publication_job,
    fail_publication_job,
    release_publication_job,
    renew_publication_job,
)
from loom_task_image_authority.registry_reader import (
    HTTPSRegistryReader,
    RegistryReaderLimits,
    RegistryReadError,
)
from loom_task_image_authority.registry_token import DistributionRegistryTokenIssuer


class PublicationDistribution(Protocol):
    """Trusted authenticated evidence adapter; a snapshot cannot self-refresh."""

    async def snapshot(
        self, *, state: PublicationState, key: PublicationKeyRecord
    ) -> DistributedKeysetSnapshot: ...


def _retryable(error: BaseException) -> bool:
    # Signer transports use explicit connection/timeout errors. Other signer
    # failures (including malformed evidence) never become catch-all retries.
    if isinstance(
        error,
        (
            asyncio.CancelledError,
            TimeoutError,
            ConnectionError,
            DisconnectionError,
            DatabasePoolTimeoutError,
        ),
    ):
        return True
    if isinstance(error, RegistryReadError):
        return error.retryable
    if isinstance(error, DBAPIError):
        state = getattr(error.orig, "sqlstate", None) or getattr(error.orig, "pgcode", None)
        return error.connection_invalidated or (
            isinstance(state, str)
            and (
                state.startswith("08")
                or state in {"40001", "40P01", "55P03", "57014", "57P01", "57P02", "57P03"}
            )
        )
    return False


@dataclass(frozen=True)
class PublicationWorkerLimits:
    maximum_jobs: int = 2
    lease_seconds: float = 60
    renewal_interval_seconds: float = 15
    retry_delay_seconds: float = 5
    signer_timeout_seconds: float = 5
    database_timeout_seconds: float = 10

    def __post_init__(self) -> None:
        if type(self.maximum_jobs) is not int or not 1 <= self.maximum_jobs <= 32:
            raise ValueError("publication job concurrency outside bounds")
        for name, maximum in (
            ("lease_seconds", 300),
            ("renewal_interval_seconds", 150),
            ("retry_delay_seconds", 300),
            ("signer_timeout_seconds", 10),
            ("database_timeout_seconds", 30),
        ):
            value = getattr(self, name)
            if (
                type(value) not in (int, float)
                or not math.isfinite(value)
                or not 0.001 <= value <= maximum
            ):
                raise ValueError("publication worker duration outside bounds")
        if self.renewal_interval_seconds >= self.lease_seconds / 2:
            raise ValueError("publication renewal requires lease headroom")


def _unsigned(job: PublicationJob, index: int, graph: VerifiedOCIGraph) -> PublicationUnsignedInput:
    component = job.snapshot.components[index]
    values = job.snapshot.model_dump(
        mode="json", by_alias=True, exclude={"components", "builder_id"}
    )

    def descriptor(value: OCIDescriptor) -> dict[str, str | int]:
        return dict(media_type=value.media_type, digest=value.digest, size=value.size)

    values.update(
        schema="loom.task-image-publication/v1",
        component=component.candidate.component,
        repository=component.candidate.repository,
        root=descriptor(graph.root),
        manifest=descriptor(graph.manifest),
        config=descriptor(graph.config),
        layers=[descriptor(layer) for layer in graph.layers],
        observed_base_digests=component.candidate.base_resolution.observed_base_digests,
    )
    return PublicationUnsignedInput.model_validate(values)


_WORKER_LIMITS = PublicationWorkerLimits()
_READER_LIMITS = RegistryReaderLimits()
_GRAPH_LIMITS = OCIVerificationLimits()


class PublicationWorker:
    """One service-level admission bound; no polling loop or unbounded fan-out.

    Dependencies below are trusted composition, never request payload factories.
    Signer context managers/transports must join and close cancelled I/O. Registry
    sockets are owned directly here by exact-repository HTTPSRegistryReader.
    """

    def __init__(
        self,
        *,
        sessions: async_sessionmaker[AsyncSession],
        token_issuer: DistributionRegistryTokenIssuer,
        ca_file: Path,
        key_id: str,
        signer_factory: Callable[[], AbstractAsyncContextManager[PublicationSigner]],
        distribution: PublicationDistribution,
        clock: Clock,
        limits: PublicationWorkerLimits = _WORKER_LIMITS,
        reader_limits: RegistryReaderLimits = _READER_LIMITS,
        graph_limits: OCIVerificationLimits = _GRAPH_LIMITS,
    ) -> None:
        if type(token_issuer) is not DistributionRegistryTokenIssuer or not isinstance(
            ca_file, Path
        ):
            raise TypeError("publication requires fixed registry issuer and CA")
        if type(limits) is not PublicationWorkerLimits:
            raise TypeError("publication requires worker limits")
        self._sessions, self._issuer, self._ca_file = sessions, token_issuer, ca_file
        self._key_id, self._signer_factory, self._distribution = (
            key_id,
            signer_factory,
            distribution,
        )
        self._clock, self._limits = clock, limits
        self._reader_limits, self._graph_limits = reader_limits, graph_limits
        self._admission = asyncio.Semaphore(limits.maximum_jobs)

    async def run(self, operation_id: UUID) -> PublicationReceipt:
        """Return exact completion or raise, retaining cancellation and safe retry state."""
        _uuid(operation_id)
        # Admission precedes claim, reader creation and every independently owned task.
        async with self._admission:
            owner = uuid4()
            try:
                async with asyncio.timeout(self._limits.database_timeout_seconds):
                    async with self._sessions.begin() as session:
                        job = await claim_publication_job(
                            session,
                            operation_id=operation_id,
                            owner_id=owner,
                            clock=self._clock,
                            lease_seconds=self._limits.lease_seconds,
                        )
            except PublicationJobOwnershipError:
                await self._expire(operation_id)
                receipt = await self._replay(operation_id)
                if receipt is not None:
                    return receipt
                raise

            try:
                checked_at = _now(self._clock)
                # Claim computes expiry before committing. A slow commit must
                # not grant fresh network work an already consumed lease.
                if job.lease is None or checked_at >= job.lease.expires_at:
                    raise PublicationJobOwnershipError("publication worker fence lost")
                remaining = (job.deadline - checked_at).total_seconds()
                async with asyncio.timeout(max(0, remaining)):
                    return await self._supervise(job, owner)
            except BaseException as error:
                # _supervise joins BOTH children before any job-only cleanup. In
                # particular, no heartbeat conflict alone is completion evidence.
                if isinstance(error, Exception):
                    try:
                        receipt = await self._replay(operation_id)
                        if receipt is not None:
                            if receipt.snapshot_sha256 != job.snapshot_sha256:
                                raise PublicationJobConflictError("publication operation changed")
                            return receipt
                    except Exception as replay_error:
                        error.add_note(
                            f"Completion confirmation failed: {type(replay_error).__name__}"
                        )
                try:
                    await self._settle(job, owner, error)
                except Exception as settle_error:
                    error.add_note(f"Publication cleanup failed: {type(settle_error).__name__}")
                raise

    async def _expire(self, operation_id: UUID) -> None:
        async with asyncio.timeout(self._limits.database_timeout_seconds):
            async with self._sessions.begin() as session:
                try:
                    await expire_publication_job(
                        session, operation_id=operation_id, clock=self._clock
                    )
                except PublicationJobOwnershipError:
                    pass

    async def _replay(self, operation_id: UUID) -> PublicationReceipt | None:
        async with asyncio.timeout(self._limits.database_timeout_seconds):
            async with self._sessions.begin() as session:
                try:
                    return await replay_completed_publication(session, operation_id=operation_id)
                except PublicationJobConflictError:
                    return None

    async def _supervise(self, job: PublicationJob, owner: UUID) -> PublicationReceipt:
        work = asyncio.create_task(self._work(job, owner), name="publication-work")
        renewal = asyncio.create_task(self._renew(job, owner), name="publication-renewal")
        try:
            done, _ = await asyncio.wait((work, renewal), return_when=asyncio.FIRST_COMPLETED)
            # Committed exact proof wins a simultaneous late heartbeat conflict.
            if work in done:
                return await work
            await renewal
            raise PublicationJobOwnershipError("publication renewal ended")
        finally:
            for task in (work, renewal):
                if not task.done():
                    task.cancel()
            await asyncio.gather(work, renewal, return_exceptions=True)

    async def _renew(self, job: PublicationJob, owner: UUID) -> None:
        assert job.lease is not None
        expires_at = job.lease.expires_at
        while True:
            remaining = (expires_at - _now(self._clock)).total_seconds()
            if remaining <= 0:
                raise PublicationJobOwnershipError("publication worker fence lost")
            # Commit latency may leave less than a normal interval. Recover
            # that live lease promptly; bound BOTH sleep and the whole renewal
            # transaction by its last committed expiry, including commit.
            async with asyncio.timeout(remaining):
                await asyncio.sleep(min(self._limits.renewal_interval_seconds, remaining / 2))
                async with asyncio.timeout(self._limits.database_timeout_seconds):
                    async with self._sessions.begin() as session:
                        renewed = await renew_publication_job(
                            session,
                            operation_id=UUID(job.operation_id),
                            owner_id=owner,
                            generation=job.worker_generation,
                            clock=self._clock,
                            lease_seconds=self._limits.lease_seconds,
                        )
            assert renewed.lease is not None
            expires_at = renewed.lease.expires_at

    async def _work(self, job: PublicationJob, owner: UUID) -> PublicationReceipt:
        if job.snapshot.registry_origin != self._issuer.registry_origin:
            raise PublicationJobAuthorizationError("publication registry origin changed")
        async with asyncio.timeout(self._limits.database_timeout_seconds):
            async with self._sessions.begin() as session:
                state, key = await read_publication_signing_state(session, key_id=self._key_id)
        # No session survives into registry, distribution or signer network I/O.
        distribution = await self._distribution.snapshot(state=state, key=key)
        publications = []
        async with self._signer_factory() as signer:
            for index, component in enumerate(job.snapshot.components):
                root = component.root
                async with HTTPSRegistryReader(
                    repository=component.candidate.repository,
                    token_issuer=self._issuer,
                    ca_file=self._ca_file,
                    limits=self._reader_limits,
                ) as reader:
                    graph = await verify_oci_graph(
                        reader,
                        OCIDescriptor(root.media_type, root.digest, root.size),
                        job.snapshot.platform,
                        limits=self._graph_limits,
                    )
                publications.append(
                    await request_publication_signature(
                        signer,
                        _unsigned(job, index, graph),
                        key=key,
                        state=state,
                        distribution=distribution,
                        clock=lambda: _now(self._clock).replace(microsecond=0),
                        timeout_seconds=self._limits.signer_timeout_seconds,
                    )
                )
        async with asyncio.timeout(self._limits.database_timeout_seconds):
            async with self._sessions.begin() as session:
                # Completion admits live artifacts under the shared parent fence.
                # Own its fresh retirement snapshot before any application query,
                # independently of the supplied factory's default isolation.
                await session.connection(execution_options={"isolation_level": "READ COMMITTED"})
                return await complete_publication_job(
                    session,
                    job=job,
                    owner_id=owner,
                    generation=job.worker_generation,
                    publications=tuple(publications),
                    distribution=distribution,
                    clock=self._clock,
                )

    async def _settle(self, job: PublicationJob, owner: UUID, error: BaseException) -> None:
        if _now(self._clock) >= job.deadline:
            await self._expire(UUID(job.operation_id))
            return
        retryable = _retryable(error)
        code: PublicationFailureCode = "verification_failed"
        if isinstance(error, (OCIVerificationError, ValueError)):
            code = "integrity"
        elif isinstance(error, (PublicationJobAuthorizationError, PublicationJobConflictError)):
            code = "authority_lost"
        async with asyncio.timeout(self._limits.database_timeout_seconds):
            async with self._sessions.begin() as session:
                try:
                    if retryable:
                        await release_publication_job(
                            session,
                            operation_id=UUID(job.operation_id),
                            owner_id=owner,
                            generation=job.worker_generation,
                            clock=self._clock,
                            retry_delay_seconds=self._limits.retry_delay_seconds,
                        )
                    else:
                        await fail_publication_job(
                            session,
                            operation_id=UUID(job.operation_id),
                            owner_id=owner,
                            generation=job.worker_generation,
                            clock=self._clock,
                            failure_code=code,
                        )
                except PublicationJobOwnershipError:
                    # Cleanup itself may have waited past the immutable deadline.
                    # A live successor or completed job still cannot be changed.
                    try:
                        await expire_publication_job(
                            session, operation_id=UUID(job.operation_id), clock=self._clock
                        )
                    except PublicationJobOwnershipError:
                        pass
