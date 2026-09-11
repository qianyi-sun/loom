"""Bounded, claim-scoped artifact return without transferable storage authority."""

from __future__ import annotations

import asyncio
import hashlib
from collections.abc import AsyncIterator
from dataclasses import dataclass
from typing import Any

from botocore.exceptions import BotoCoreError, ClientError
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from loom_capacity_agent.build_admission import (
    BuildArtifactV1,
    BuildClaimExchangeV1,
    BuildClaimRequestV1,
    BuildSourceContextV1,
    native_build_artifact_key,
)
from loom_capacity_build_guard.execution_store import BuildGuardExecutionStore
from loom_capacity_build_guard.source_access import BuildClaimSourceV1
from loom_capacity_executor.native_build_source import _settled_io
from loom_capacity_manager.contracts import canonical_digest

MAX_ARTIFACT_CHUNK_BYTES = 1024 * 1024
_PART_BYTES = 8 * 1024 * 1024
_SLOTS = 2


@dataclass(slots=True)
class _Upload:
    bucket: str
    key: str
    upload_id: str | None = None


class BuildArtifactWriter:
    """Return unverified archive facts only, never publication or build success.

All multipart bytes stay behind current worker/claim fences. A lost fence after
completion leaves an unaccepted object for authoritative GC; it cannot justify
deleting a concurrent exact replay's object or reporting artifact-ready.
"""

    def __init__(self, *, session_factory: async_sessionmaker[AsyncSession], object_store: Any,
        max_artifact_bytes: int,
    ) -> None:
        if type(max_artifact_bytes) is not int or not 1 <= max_artifact_bytes <= 16 * 1024**3:
            raise ValueError("native artifact byte limit is invalid")
        self._sessions = session_factory
        self._objects = object_store
        self._max_bytes = max_artifact_bytes
        self._slots = asyncio.Semaphore(_SLOTS)
        self._closed = False
        self._close_lock = asyncio.Lock()

    async def aclose(self) -> None:
        self._closed = True
        async with self._close_lock:
            acquired = 0
            try:
                for _ in range(_SLOTS):
                    await self._slots.acquire()
                    acquired += 1
            finally:
                for _ in range(acquired):
                    self._slots.release()

    async def _authorize(self, claim: BuildClaimRequestV1, credential: str) -> BuildClaimSourceV1:
        async with self._sessions.begin() as session:
            await session.execute(text("SET LOCAL statement_timeout='10000ms'"))
            await session.execute(text("SET LOCAL lock_timeout='5000ms'"))
            source = await BuildGuardExecutionStore(session, binding=claim.binding).authorize_source(
                claim, worker_credential=credential)
        return source

    async def _current(self, envelope: BuildClaimExchangeV1, expected: BuildClaimSourceV1 | None = None) -> BuildClaimSourceV1:
        if self._closed:
            raise ValueError("native artifact writer is closed")
        source = await self._authorize(envelope.claim, envelope.worker_credential)
        if (source.claim != envelope.claim or source.claim_digest != canonical_digest(envelope.claim)
            or (expected is not None and source.model_copy(update={"lease_not_after": expected.lease_not_after}) != expected)):
            raise ValueError("native artifact authority changed")
        return source

    async def _context(self, envelope: BuildClaimExchangeV1) -> BuildSourceContextV1:
        async with self._sessions.begin() as session:
            await session.execute(text("SET LOCAL statement_timeout='10000ms'"))
            await session.execute(text("SET LOCAL lock_timeout='5000ms'"))
            context = await BuildGuardExecutionStore(session, binding=envelope.claim.binding).read_source_context(
                envelope.claim, worker_credential=envelope.worker_credential)
        return context

    def _create(self, upload: _Upload, metadata: dict[str, str]) -> None:
        reply = self._objects.create_multipart_upload(Bucket=upload.bucket, Key=upload.key,
            ContentType="application/vnd.loom.personal-dev-build.v1+tar", Metadata=metadata)
        # Set state in the IO thread before returning. Cancellation can arrive
        # after S3 creates the upload but before its awaiter receives the result.
        upload_id = reply.get("UploadId")
        if not isinstance(upload_id, str) or not upload_id or len(upload_id) > 4096:
            raise ValueError("native artifact multipart identity is invalid")
        upload.upload_id = upload_id

    def _abort(self, upload: _Upload) -> None:
        if upload.upload_id is None:
            return
        try:
            self._objects.abort_multipart_upload(Bucket=upload.bucket, Key=upload.key, UploadId=upload.upload_id)
        except ClientError as exc:
            if exc.response.get("Error", {}).get("Code") != "NoSuchUpload":
                raise RuntimeError("native artifact multipart cleanup was not acknowledged") from None

    def _existing(self, upload: _Upload, artifact: BuildArtifactV1, metadata: dict[str, str]) -> None:
        head = self._objects.head_object(Bucket=upload.bucket, Key=upload.key)
        if (type(head.get("ContentLength")) is not int or head["ContentLength"] != artifact.archive_size_bytes
            or head.get("Metadata") != metadata):
            raise ValueError("native artifact immutable destination differs")

    async def write(self, claim: BuildClaimRequestV1, *, worker_credential: str,
        artifact: BuildArtifactV1, chunks: AsyncIterator[bytes],
    ) -> BuildArtifactV1:
        if self._closed:
            raise ValueError("native artifact writer is closed")
        try:
            envelope = BuildClaimExchangeV1.model_validate_json(BuildClaimExchangeV1(
                claim=claim, worker_credential=worker_credential).model_dump_json())
            artifact = BuildArtifactV1.model_validate_json(artifact.model_dump_json())
        except ValueError:
            raise ValueError("native artifact envelope is invalid") from None
        if artifact.archive_size_bytes > self._max_bytes:
            raise ValueError("native artifact exceeds byte limit")
        async with asyncio.timeout(1800), self._slots:
            source = await self._current(envelope)
            context = await self._context(envelope)
            if (context.claim_digest != source.claim_digest or context.request_id != envelope.claim.request_id
                or context.source_binding_sha256 != source.source_binding_sha256
                or context.archive_sha256 != source.archive_sha256 or context.archive_size_bytes != source.archive_size_bytes
                or context.platform != ("linux/arm64" if claim.binding.pool_id == "gb10" else "linux/amd64")):
                raise ValueError("native artifact source context changed")
            upload = _Upload(bucket=source.object_bucket, key=native_build_artifact_key(envelope.claim, artifact))
            metadata = {"claim-sha256": source.claim_digest, "artifact-sha256": artifact.archive_sha256,
                "attestation-scope": "personal-dev-only", "build-attempt-id": str(context.attempt_id),
                "build-lease-epoch": str(context.lease_epoch), "candidate-sha256": context.candidate_sha,
                "platform": context.platform}
            completed = False
            try:
                await _settled_io(self._create, upload, metadata)
                parts: list[dict[str, Any]] = []
                pending = bytearray()
                digest, observed = hashlib.sha256(), 0

                async def part(data: bytes) -> None:
                    await self._current(envelope, source)
                    number = len(parts) + 1
                    reply = await _settled_io(self._objects.upload_part, Bucket=upload.bucket, Key=upload.key,
                        UploadId=upload.upload_id, PartNumber=number, Body=data)
                    etag = reply.get("ETag")
                    if not isinstance(etag, str) or not etag or len(etag) > 1024:
                        raise ValueError("native artifact part receipt is invalid")
                    parts.append({"PartNumber": number, "ETag": etag})

                while True:
                    try:
                        async with asyncio.timeout(30):
                            chunk = await anext(chunks)
                    except StopAsyncIteration:
                        break
                    if self._closed:
                        raise ValueError("native artifact writer is closed")
                    if not isinstance(chunk, bytes) or not 1 <= len(chunk) <= MAX_ARTIFACT_CHUNK_BYTES:
                        raise ValueError("native artifact stream chunk is invalid")
                    observed += len(chunk)
                    if observed > artifact.archive_size_bytes:
                        raise ValueError("native artifact exceeds declared length")
                    digest.update(chunk)
                    pending.extend(chunk)
                    if len(pending) >= _PART_BYTES:
                        await part(bytes(pending[:_PART_BYTES]))
                        del pending[:_PART_BYTES]
                if observed != artifact.archive_size_bytes or digest.hexdigest() != artifact.archive_sha256:
                    raise ValueError("native artifact complete length or digest changed")
                if pending:
                    await part(bytes(pending))
                await self._current(envelope, source)
                try:
                    await _settled_io(self._objects.complete_multipart_upload, Bucket=upload.bucket, Key=upload.key,
                        UploadId=upload.upload_id, MultipartUpload={"Parts": parts}, IfNoneMatch="*")
                    completed = True
                except (BotoCoreError, ClientError, TimeoutError):
                    # Exact content has already been streamed and verified. A
                    # precondition failure or lost reply may have an exact object;
                    # no inference is permitted from absence/different metadata.
                    await _settled_io(self._existing, upload, artifact, metadata)
                await self._current(envelope, source)
                return artifact
            except (BotoCoreError, ClientError):
                raise RuntimeError("native artifact object IO failed") from None
            finally:
                if not completed:
                    await _settled_io(self._abort, upload)
