"""Scoped authenticated IO owner; never passed across the mapped runtime edge."""

from __future__ import annotations

import asyncio
import socket
from collections.abc import AsyncIterator, Iterator
from contextlib import asynccontextmanager, contextmanager
from typing import Protocol

from loom_capacity_agent.build_admission import (
    BuildArtifactV1,
    BuildClaimExchangeV1,
    BuildClaimRequestV1,
    BuildOutcomeReceiptV1,
    BuildOutcomeRequestV1,
    BuildSourceContextV1,
)
from loom_capacity_agent.build_artifact_stream import BuildArtifactUploadReceiptV1
from loom_capacity_executor.native_authority_bridge import (
    NativeExecutionAuthorityClient,
    serve_native_execution_authority,
)
from loom_capacity_executor.native_build_source import NativeStagedBuildSource
from loom_capacity_manager.contracts import canonical_digest


class NativeAllocatedIOClient(NativeExecutionAuthorityClient, Protocol):
    async def upload_artifact(self, claim: BuildClaimRequestV1, *, worker_credential: str,
        artifact: BuildArtifactV1, chunks: AsyncIterator[bytes],
    ) -> BuildArtifactUploadReceiptV1: ...

    async def record_outcome(self, request: BuildOutcomeRequestV1, *, worker_credential: str) -> BuildOutcomeReceiptV1: ...


class NativeAllocatedIO:
    """Claim/source-bound operations with no launch, retry or release authority.

    The object stays in the original trusted IO process. Its source path remains
    descriptor-scoped and cannot be passed to a child. Consumers must settle the
    rootless launcher and keep artifact-receiver scope alive during upload.
    """

    def __init__(self, *, claim: BuildClaimRequestV1, source: NativeStagedBuildSource,
        client: NativeAllocatedIOClient, worker_credential: str,
    ) -> None:
        packet = BuildClaimExchangeV1.model_validate_json(BuildClaimExchangeV1(
            claim=claim, worker_credential=worker_credential).model_dump_json())
        context = BuildSourceContextV1.model_validate_json(source.context.model_dump_json())
        if (context.claim_digest != canonical_digest(packet.claim) or context.request_id != packet.claim.request_id
            or packet.claim.binding.pool_id != ("gb10" if context.platform == "linux/arm64" else "oldlab")):
            raise ValueError("native allocated IO source identity changed")
        self._claim = packet.claim
        self._source = NativeStagedBuildSource(context, source.archive)
        self._client = client
        self._credential = packet.worker_credential
        self._closed = False
        self._operations: set[asyncio.Task[object]] = set()

    @property
    def claim(self) -> BuildClaimRequestV1:
        return self._claim

    @property
    def source(self) -> NativeStagedBuildSource:
        return self._source

    @contextmanager
    def _operation(self) -> Iterator[str]:
        if self._closed:
            raise RuntimeError("native allocated IO scope is closed")
        task = asyncio.current_task()
        if task is None or task in self._operations:
            raise RuntimeError("native allocated IO requires an owned operation task")
        self._operations.add(task)
        try:
            yield self._credential
        finally:
            self._operations.discard(task)

    async def serve_authority(self, channel: socket.socket) -> None:
        """Channel ownership remains with the outer process; no cached permits."""
        with self._operation() as credential:
            await serve_native_execution_authority(channel, claim=self.claim,
                source_binding_sha256=self.source.context.source_binding_sha256,
                worker_credential=credential, client=self._client)

    async def upload_artifact(self, artifact: BuildArtifactV1, *, chunks: AsyncIterator[bytes]) -> BuildArtifactUploadReceiptV1:
        """One exact upload; even a lost reply does not imply another attempt."""
        with self._operation() as credential:
            artifact = BuildArtifactV1.model_validate_json(artifact.model_dump_json())
            receipt = await self._client.upload_artifact(self.claim, worker_credential=credential, artifact=artifact, chunks=chunks)
            if not isinstance(receipt, BuildArtifactUploadReceiptV1):
                raise ValueError("native allocated upload receipt is not typed")
            receipt = BuildArtifactUploadReceiptV1.model_validate_json(receipt.model_dump_json())
            if receipt.claim_digest != canonical_digest(self.claim) or receipt.artifact != artifact:
                raise ValueError("native allocated upload receipt identity changed")
            return receipt

    async def record_outcome(self, request: BuildOutcomeRequestV1) -> BuildOutcomeReceiptV1:
        """Historical outcome only. Caller retains exact operation on uncertainty."""
        with self._operation() as credential:
            request = BuildOutcomeRequestV1.model_validate_json(request.model_dump_json())
            if request.claim != self.claim:
                raise ValueError("native allocated outcome claim changed")
            receipt = await self._client.record_outcome(request, worker_credential=credential)
            if not isinstance(receipt, BuildOutcomeReceiptV1):
                raise ValueError("native allocated outcome receipt is not typed")
            receipt = BuildOutcomeReceiptV1.model_validate_json(receipt.model_dump_json())
            if receipt.request != request or receipt.request_digest != canonical_digest(request):
                raise ValueError("native allocated outcome receipt identity changed")
            return receipt

    async def _close(self) -> None:
        self._closed = True
        pending = tuple(self._operations)
        for task in pending:
            task.cancel()
        try:
            if pending:
                await asyncio.gather(*pending, return_exceptions=True)
        finally:
            self._credential = ""  # Drop our reference; this is not memory zeroization.


@asynccontextmanager
async def scoped_native_allocated_io(*, claim: BuildClaimRequestV1, source: NativeStagedBuildSource,
    client: NativeAllocatedIOClient, worker_credential: str,
) -> AsyncIterator[NativeAllocatedIO]:
    owner = NativeAllocatedIO(claim=claim, source=source, client=client, worker_credential=worker_credential)
    try:
        yield owner
    finally:
        # Settle even repeated cancellation before the surrounding source scope
        # removes files. Never infer child containment from cancelling IO tasks.
        closing = asyncio.create_task(owner._close())
        interrupted = False
        while not closing.done():
            try:
                await asyncio.shield(closing)
            except asyncio.CancelledError:
                interrupted = True
        closing.result()
        if interrupted:
            raise asyncio.CancelledError
