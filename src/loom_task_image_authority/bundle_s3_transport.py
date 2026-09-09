"""Authority-only TLS listing/manifest reads with owned cancellation and bounds."""

from __future__ import annotations

import asyncio
import math
import ssl
from dataclasses import dataclass, fields
from pathlib import Path
from types import TracebackType
from typing import Self
from urllib.parse import urlsplit

import h11

from loom.task_image_bundle_manifest import (
    MAX_CONTENT_MANIFEST_BYTES,
    task_image_bundle_manifest_key,
)
from loom_task_image_authority.bundle_capability import (
    MAX_TASK_IMAGE_BUNDLE_URL_BYTES,
    TaskImageBundleCapabilityError,
    _bucket,
)
from loom_task_image_authority.bundle_s3_listing import MAX_S3_LIST_PAGE_BYTES
from loom_task_image_authority.config import _validate_https_origin


@dataclass(frozen=True, slots=True)
class S3ListingReadLimits:
    connect_timeout_seconds: float = 5.0
    idle_timeout_seconds: float = 5.0
    total_timeout_seconds: float = 30.0
    maximum_header_bytes: int = 32 * 1024
    maximum_body_bytes: int = MAX_S3_LIST_PAGE_BYTES
    maximum_wire_bytes: int = 8 * 1024 * 1024
    maximum_concurrent_reads: int = 4

    def __post_init__(self) -> None:
        ceilings = (30.0, 30.0, 120.0, 64 * 1024, MAX_S3_LIST_PAGE_BYTES, 8 * 1024 * 1024, 32)
        for item, ceiling in zip(fields(self), ceilings, strict=True):
            value = getattr(self, item.name)
            if (
                isinstance(item.default, float) and (type(value) is not float or not 0.0 < value <= ceiling)
            ) or (
                isinstance(item.default, int) and (type(value) is not int or not 0 < value <= ceiling)
            ):
                raise ValueError("S3 listing limits must be positive and bounded")


_DEFAULT_LIMITS = S3ListingReadLimits()


class HTTPSBundleListingReader:
    """Read signed listings and exact digest-key manifests; never redirect.

    Caller owns signing, absolute authorization and cross-page inventory budgets.
    Each request owns one connection, aborted on every exit (no pool or lingering
    TLS close handshake). The caller's monotonic deadline includes queue time;
    this reader adds finite per-read, connect and idle ceilings, without retries.
    """

    def __init__(
        self, *, origin: str, bucket: str, ca_file: Path,
        limits: S3ListingReadLimits = _DEFAULT_LIMITS,
    ) -> None:
        _validate_https_origin(origin, label="S3 listing origin")
        _bucket(bucket)
        if not isinstance(ca_file, Path) or type(limits) is not S3ListingReadLimits:
            raise ValueError("S3 listing CA and limits are required")
        limits.__post_init__()
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        context.minimum_version = ssl.TLSVersion.TLSv1_2
        context.verify_mode = ssl.CERT_REQUIRED
        context.check_hostname = True
        try:
            context.load_verify_locations(cafile=str(ca_file))
        except (OSError, ssl.SSLError):
            raise TaskImageBundleCapabilityError("S3 listing CA is invalid") from None
        parsed = urlsplit(origin)
        assert parsed.hostname is not None
        self._origin = origin
        self._bucket = bucket
        self._hostname = parsed.hostname
        self._host = parsed.netloc.encode("ascii")
        self._port = parsed.port or 443
        self._context = context
        self._limits = limits
        self._semaphore = asyncio.Semaphore(limits.maximum_concurrent_reads)
        self._requests: set[asyncio.Task[bytes]] = set()
        self._close_task: asyncio.Task[None] | None = None

    async def __aenter__(self) -> Self:
        if self._close_task is not None:
            raise TaskImageBundleCapabilityError("S3 listing reader is closed")
        return self

    async def __aexit__(
        self, exc_type: type[BaseException] | None,
        exc_value: BaseException | None, traceback: TracebackType | None,
    ) -> None:
        await self.aclose()

    async def aclose(self) -> None:
        if self._close_task is None:
            self._close_task = asyncio.create_task(self._close())
        await asyncio.shield(self._close_task)

    async def _close(self) -> None:
        requests = tuple(self._requests)
        for request in requests:
            request.cancel()
        await asyncio.gather(*requests, return_exceptions=True)

    async def fetch(self, url: str, *, deadline: float) -> bytes:
        return await self._fetch_scoped(
            url, deadline=deadline, expected_path=f"/{self._bucket}",
            maximum_body_bytes=self._limits.maximum_body_bytes, accept=b"application/xml",
        )

    async def fetch_manifest(self, url: str, *, expected_sha256: str, deadline: float) -> bytes:
        """Transport only: the backend must verify registered canonical bytes.

        No arbitrary object key or capability-selected origin/CA is accepted.
        Both entrypoints share one concurrency ceiling and shutdown owner.
        """
        try:
            key = task_image_bundle_manifest_key(expected_sha256)
        except ValueError:
            raise TaskImageBundleCapabilityError("S3 manifest target is invalid") from None
        return await self._fetch_scoped(
            url, deadline=deadline, expected_path=f"/{self._bucket}/{key}",
            maximum_body_bytes=min(self._limits.maximum_body_bytes, MAX_CONTENT_MANIFEST_BYTES),
            accept=b"application/json",
        )

    async def _fetch_scoped(
        self, url: str, *, deadline: float, expected_path: str,
        maximum_body_bytes: int, accept: bytes,
    ) -> bytes:
        try:
            if (
                self._close_task is not None or type(url) is not str
                or not 0 < len(url) <= MAX_TASK_IMAGE_BUNDLE_URL_BYTES
                or not url.isascii() or any(ord(char) <= 32 or ord(char) == 127 for char in url)
                or type(deadline) is not float or not math.isfinite(deadline)
            ):
                raise ValueError("invalid request")
            parsed = urlsplit(url)
            if (
                f"{parsed.scheme}://{parsed.netloc}" != self._origin
                or parsed.path != expected_path or not parsed.query or parsed.fragment
            ):
                raise ValueError("unbound target")
            now = asyncio.get_running_loop().time()
            if deadline <= now:
                raise ValueError("expired deadline")
        except (ValueError, TypeError, AttributeError):
            raise TaskImageBundleCapabilityError("S3 listing request is unavailable or invalid") from None
        task = asyncio.create_task(self._fetch(
            parsed.path + "?" + parsed.query,
            min(deadline, now + self._limits.total_timeout_seconds),
            maximum_body_bytes=maximum_body_bytes, accept=accept,
        ))
        self._requests.add(task)
        task.add_done_callback(self._requests.discard)
        return await task

    async def _connect(self) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
        task = asyncio.create_task(asyncio.open_connection(
            self._hostname, self._port, ssl=self._context, server_hostname=self._hostname,
            limit=self._limits.maximum_header_bytes,
            ssl_handshake_timeout=self._limits.connect_timeout_seconds,
            ssl_shutdown_timeout=self._limits.connect_timeout_seconds,
        ))
        transferred = False

        def dispose(done: asyncio.Task[tuple[asyncio.StreamReader, asyncio.StreamWriter]]) -> None:
            if not done.cancelled() and done.exception() is None:
                done.result()[1].transport.abort()

        try:
            async with asyncio.timeout(self._limits.connect_timeout_seconds):
                result = await asyncio.shield(task)
            transferred = True
            return result
        finally:
            if not transferred:
                # Callback remains responsible even if cleanup is cancelled again
                # exactly as open_connection hands back its transport.
                task.add_done_callback(dispose)
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)

    def _check_response(self, event: h11.Response, *, maximum_body_bytes: int) -> None:
        headers = tuple(event.headers.raw_items())
        if event.status_code != 200 or 2 + sum(len(k) + len(v) + 4 for k, v in headers) > self._limits.maximum_header_bytes:
            raise ValueError("invalid response")
        for name in (b"content-encoding", b"content-length"):
            values = [v for k, v in headers if k.lower() == name]
            if len(values) > 1:
                raise ValueError("duplicate response metadata")
            if not values:
                continue
            if name == b"content-encoding" and values[0].lower() != b"identity":
                raise ValueError("compressed response")
            if name == b"content-length" and int(values[0]) > maximum_body_bytes:
                raise ValueError("oversized response")

    async def _fetch(self, target: str, deadline: float, *, maximum_body_bytes: int, accept: bytes) -> bytes:
        writer: asyncio.StreamWriter | None = None
        try:
            async with asyncio.timeout_at(deadline), self._semaphore:
                stream, writer = await self._connect()
                protocol = h11.Connection(h11.CLIENT, max_incomplete_event_size=self._limits.maximum_header_bytes)
                request = h11.Request(method=b"GET", target=target.encode("ascii"), headers=[
                    (b"Host", self._host), (b"Accept", accept),
                    (b"Accept-Encoding", b"identity"), (b"Connection", b"close"),
                ])
                writer.write((protocol.send(request) or b"") + (protocol.send(h11.EndOfMessage()) or b""))
                async with asyncio.timeout(self._limits.connect_timeout_seconds):
                    await writer.drain()
                body = bytearray()
                raw_head = bytearray()
                head_complete = False
                response_seen = False
                wire_bytes = 0
                while True:
                    if asyncio.get_running_loop().time() >= deadline:
                        raise TimeoutError
                    event = protocol.next_event()
                    if event is h11.NEED_DATA:
                        async with asyncio.timeout(self._limits.idle_timeout_seconds):
                            chunk = await stream.read(min(16384, self._limits.maximum_header_bytes + 1))
                        wire_bytes += len(chunk)
                        if wire_bytes > self._limits.maximum_wire_bytes:
                            raise ValueError("wire limit")
                        if not head_complete:
                            raw_head.extend(chunk)
                            head_end = raw_head.find(b"\r\n\r\n")
                            head_bytes = head_end + 4 if head_end >= 0 else len(raw_head)
                            if head_bytes > self._limits.maximum_header_bytes:
                                raise ValueError("raw header limit")
                            if head_end >= 0:
                                head_complete = True
                                raw_head.clear()
                        protocol.receive_data(chunk)
                    elif isinstance(event, h11.Response):
                        if response_seen or not head_complete:
                            raise ValueError("duplicate response")
                        self._check_response(event, maximum_body_bytes=maximum_body_bytes)
                        response_seen = True
                    elif isinstance(event, h11.Data):
                        if not response_seen or len(body) + len(event.data) > maximum_body_bytes:
                            raise ValueError("body limit")
                        body.extend(event.data)
                    elif isinstance(event, h11.EndOfMessage):
                        if not response_seen or event.headers or not body:
                            raise ValueError("invalid response end")
                        return bytes(body)
                    else:
                        # No informational responses, trailers or early EOF; no
                        # hidden retry, redirect, decompression or credential log.
                        raise ValueError("unsupported response event")
        except (TimeoutError, OSError, ValueError, h11.ProtocolError):
            raise TaskImageBundleCapabilityError("S3 listing transport failed or exceeded limits") from None
        finally:
            if writer is not None:
                writer.transport.abort()
