"""Versioned bounded artifact framing; credentials stay inside the TLS body."""

from __future__ import annotations

from collections.abc import AsyncIterator

from loom_capacity_agent.build_admission import BuildArtifactV1, BuildClaimExchangeV1
from loom_capacity_manager.contracts import Digest, StrictV1Model, canonical_bytes

ARTIFACT_STREAM_CONTENT_TYPE = "application/vnd.loom.native-artifact-stream.v1"
MAX_ARTIFACT_STREAM_CHUNK_BYTES = 1024 * 1024
_MAGIC = b"LOOMART1"
_MAX_HEADER_BYTES = 64 * 1024


class BuildArtifactUploadV1(BuildClaimExchangeV1):
    artifact: BuildArtifactV1


class BuildArtifactUploadReceiptV1(StrictV1Model):
    """Transport acknowledgment only; outcome and verified publication are separate."""

    claim_digest: Digest
    artifact: BuildArtifactV1


async def encode_artifact_stream(envelope: BuildArtifactUploadV1, chunks: AsyncIterator[bytes]) -> AsyncIterator[bytes]:
    try:
        envelope = BuildArtifactUploadV1.model_validate_json(envelope.model_dump_json())
    except ValueError:
        raise ValueError("native artifact stream envelope is invalid") from None
    header = canonical_bytes(envelope)
    if len(header) > _MAX_HEADER_BYTES:
        raise ValueError("native artifact stream header exceeds byte bound")
    yield _MAGIC + len(header).to_bytes(4, "big") + header
    async for chunk in chunks:
        if not isinstance(chunk, bytes) or not 1 <= len(chunk) <= MAX_ARTIFACT_STREAM_CHUNK_BYTES:
            raise ValueError("native artifact stream chunk is invalid")
        yield chunk


async def decode_artifact_stream(chunks: AsyncIterator[bytes]) -> tuple[BuildArtifactUploadV1, AsyncIterator[bytes]]:
    """Read at most one bounded chunk beyond the header; leave body streaming.

    The caller owns the deadline and must authenticate the returned envelope
    before consuming the remaining body. Transport receive events need not match
    sender yields: split them without copying an entire event into the header.
    ASGI's final empty chunk is harmless.
    """
    pending = bytearray()

    async def logical_chunks() -> AsyncIterator[bytes]:
        async for transport_chunk in chunks:
            if not isinstance(transport_chunk, bytes):
                raise ValueError("native artifact stream chunk is invalid")
            view = memoryview(transport_chunk)
            for offset in range(0, len(view), MAX_ARTIFACT_STREAM_CHUNK_BYTES):
                yield bytes(view[offset:offset + MAX_ARTIFACT_STREAM_CHUNK_BYTES])

    incoming = logical_chunks()

    async def read() -> bytes:
        try:
            chunk = await anext(incoming)
        except StopAsyncIteration:
            raise ValueError("native artifact stream header is truncated") from None
        if not isinstance(chunk, bytes) or len(chunk) > MAX_ARTIFACT_STREAM_CHUNK_BYTES:
            raise ValueError("native artifact stream chunk is invalid")
        return chunk

    while len(pending) < 12:
        pending.extend(await read())
    size = int.from_bytes(pending[8:12], "big")
    if pending[:8] != _MAGIC or not 1 <= size <= _MAX_HEADER_BYTES:
        raise ValueError("native artifact stream header is invalid")
    while len(pending) < 12 + size:
        pending.extend(await read())
    header = bytes(pending[12:12 + size])
    try:
        envelope = BuildArtifactUploadV1.model_validate_json(header)
    except ValueError:
        raise ValueError("native artifact stream envelope is invalid") from None
    if canonical_bytes(envelope) != header:
        raise ValueError("native artifact stream envelope is not canonical")
    del pending[:12 + size]

    async def body() -> AsyncIterator[bytes]:
        if pending:
            yield bytes(pending)
            pending.clear()
        async for chunk in incoming:
            if not isinstance(chunk, bytes) or len(chunk) > MAX_ARTIFACT_STREAM_CHUNK_BYTES:
                raise ValueError("native artifact stream chunk is invalid")
            if chunk:
                yield chunk

    return envelope, body()
