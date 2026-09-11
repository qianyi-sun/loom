"""Management-mediated bounded source IO with fresh authority on both sides."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from loom_capacity_agent.build_admission import BuildClaimExchangeV1, BuildClaimRequestV1
from loom_capacity_build_guard.execution_store import BuildGuardExecutionStore
from loom_capacity_build_guard.source_access import BuildClaimSourceV1

MAX_SOURCE_CHUNK_BYTES = 1024 * 1024


@dataclass(frozen=True, slots=True)
class BuildSourceChunk:
    source: BuildClaimSourceV1
    offset: int
    data: bytes


class BuildSourceReader:
    """No presigned URLs, object-store credentials or SQL locks cross the IO boundary.

    This reads into trusted management memory. Callers must not send any bytes
    until it returns, nor treat a returned chunk as permission to run a build.
    The runtime must verify the complete archive before extraction/execution.
    """

    def __init__(self, *, session_factory: async_sessionmaker[AsyncSession], object_store: Any) -> None:
        self._sessions = session_factory
        self._objects = object_store
        self._slots = asyncio.Semaphore(8)
        self._closed = False
        self._close_lock = asyncio.Lock()

    async def aclose(self) -> None:
        """Stop admission and drain SQL/IO before the owned clients are closed."""
        self._closed = True
        async with self._close_lock:
            acquired = 0
            try:
                for _ in range(8):
                    await self._slots.acquire()
                    acquired += 1
            finally:
                for _ in range(acquired):
                    self._slots.release()

    async def _authorize(self, claim: BuildClaimRequestV1, credential: str) -> BuildClaimSourceV1:
        async with self._sessions.begin() as session:
            await session.execute(text("SET LOCAL statement_timeout='10000ms'"))
            await session.execute(text("SET LOCAL lock_timeout='5000ms'"))
            result = await BuildGuardExecutionStore(session, binding=claim.binding).authorize_source(
                claim, worker_credential=credential)
        return result

    def _read_range(self, source: BuildClaimSourceV1, offset: int, count: int) -> bytes:
        last = offset + count - 1
        response = self._objects.get_object(Bucket=source.object_bucket, Key=source.object_key,
            Range=f"bytes={offset}-{last}")
        body = response["Body"]
        try:
            if (type(response.get("ContentLength")) is not int or response["ContentLength"] != count
                or response.get("ContentRange") != f"bytes {offset}-{last}/{source.archive_size_bytes}"):
                raise ValueError("native source object range changed")
            data = body.read(count + 1)
            if not isinstance(data, bytes) or len(data) != count:
                raise ValueError("native source object body changed")
            return data
        finally:
            body.close()

    def _release_finished_io(self, task: asyncio.Task[bytes]) -> None:
        # A disconnected caller cannot free a slot while boto3 still owns its
        # thread/stream. Consume its eventual exception without logging source.
        if not task.cancelled():
            task.exception()
        self._slots.release()

    async def read(self, claim: BuildClaimRequestV1, *, worker_credential: str, offset: int, length: int) -> BuildSourceChunk:
        if self._closed:
            raise ValueError("native source reader is closed")
        if (type(offset) is not int or offset < 0 or type(length) is not int
            or not 1 <= length <= MAX_SOURCE_CHUNK_BYTES):
            raise ValueError("native source range is invalid")
        envelope = BuildClaimExchangeV1.model_validate_json(BuildClaimExchangeV1(
            claim=claim, worker_credential=worker_credential).model_dump_json())
        io_task: asyncio.Task[bytes] | None = None
        async with asyncio.timeout(30):
            await self._slots.acquire()
            try:
                if self._closed:
                    raise ValueError("native source reader is closed")
                source = await self._authorize(envelope.claim, envelope.worker_credential)
                if offset >= source.archive_size_bytes:
                    raise ValueError("native source range is past archive end")
                count = min(length, source.archive_size_bytes - offset)
                io_task = asyncio.create_task(asyncio.to_thread(self._read_range, source, offset, count))
                data = await asyncio.shield(io_task)
                current = await self._authorize(envelope.claim, envelope.worker_credential)
                # A successful heartbeat can advance the same attempt's lease.
                # No other source/claim field may change between these checks.
                if current.model_copy(update={"lease_not_after": source.lease_not_after}) != source:
                    raise ValueError("native source authority changed during read")
                return BuildSourceChunk(source=current, offset=offset, data=data)
            finally:
                if io_task is not None and not io_task.done():
                    io_task.add_done_callback(self._release_finished_io)
                else:
                    self._slots.release()
