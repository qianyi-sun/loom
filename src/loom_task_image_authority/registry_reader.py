"""Repository-bound HTTPS OCI reader with a bounded public-h11 parser.

Every admitted digest GET owns one HTTP/1.1 TLS connection. The public h11
event stream exposes informational responses and trailers instead of silently
discarding them. Its incomplete-event limit bounds response heads, chunk
framing and trailers before parsing; this reader separately bounds every raw
receive and the complete parsed header bytes.
"""

from __future__ import annotations

import asyncio
import ssl
from collections.abc import AsyncGenerator, Awaitable, Iterable
from dataclasses import dataclass, fields
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import TracebackType
from typing import Literal, Self, TypeVar
from urllib.parse import urlsplit
from uuid import uuid4

import h11

from loom_task_image_authority.config import (
    TaskImageAuthorityConfigurationError,
    TaskImageAuthoritySettings,
)
from loom_task_image_authority.oci_verification import OCIDescriptor
from loom_task_image_authority.registry_token import (
    DistributionRegistryTokenIssuer,
    _validate_repository,
    load_distribution_registry_token_issuer,
)

_TOKEN_LIFETIME = timedelta(seconds=45)
_USER_AGENT = b"loom-task-image-verifier/1"
_T = TypeVar("_T")


class _TotalTimeoutError(TimeoutError):
    pass


class _TransportTimeoutError(TimeoutError):
    pass


async def _bounded_await(
    awaitable: Awaitable[_T],
    *,
    deadline: float,
    operation_timeout: float | None = None,
) -> _T:
    remaining = deadline - asyncio.get_running_loop().time()
    total_is_tighter = operation_timeout is None or remaining <= operation_timeout
    timeout = remaining if operation_timeout is None else min(remaining, operation_timeout)
    try:
        async with asyncio.timeout(max(0.0, timeout)):
            return await awaitable
    except TimeoutError:
        if total_is_tighter:
            raise _TotalTimeoutError from None
        raise _TransportTimeoutError from None


class RegistryReadError(RuntimeError):
    """A safe transport failure without response bytes or bearer credentials."""

    def __init__(self, message: str, *, retryable: bool) -> None:
        super().__init__(message)
        self.retryable = retryable


@dataclass(frozen=True, slots=True)
class RegistryReaderLimits:
    connect_timeout_seconds: float = 5.0
    idle_timeout_seconds: float = 10.0
    total_timeout_seconds: float = 120.0
    maximum_response_header_bytes: int = 32 * 1024
    maximum_chunk_bytes: int = 1024 * 1024
    maximum_manifest_bytes: int = 4 * 1024**2
    maximum_response_bytes: int = 100 * 1024**3
    maximum_concurrent_reads: int = 4

    def __post_init__(self) -> None:
        ceilings: dict[str, float | int] = {
            "connect_timeout_seconds": 30.0,
            "idle_timeout_seconds": 60.0,
            "total_timeout_seconds": 600.0,
            "maximum_response_header_bytes": 64 * 1024,
            "maximum_chunk_bytes": 1024 * 1024,
            "maximum_manifest_bytes": 4 * 1024**2,
            "maximum_response_bytes": 100 * 1024**3,
            "maximum_concurrent_reads": 32,
        }
        for item in fields(self):
            value = getattr(self, item.name)
            ceiling = ceilings[item.name]
            if (
                isinstance(item.default, float)
                and (type(value) is not float or not 0.0 < value <= ceiling)
            ) or (
                isinstance(item.default, int)
                and (type(value) is not int or not 0 < value <= ceiling)
            ):
                raise ValueError("registry reader limits must be positive and bounded")


_DEFAULT_LIMITS = RegistryReaderLimits()


def _header_bytes(headers: Iterable[tuple[bytes, bytes]]) -> int:
    return 2 + sum(len(name) + 2 + len(value) + 2 for name, value in headers)


def _one_header(
    headers: tuple[tuple[bytes, bytes], ...],
    name: bytes,
) -> bytes | None:
    values = [value for candidate, value in headers if candidate.lower() == name]
    if len(values) > 1:
        raise RegistryReadError("registry response has duplicate metadata", retryable=False)
    return values[0] if values else None


class HTTPSRegistryReader:
    """Read only exact digests from one validated Distribution repository."""

    def __init__(
        self,
        *,
        repository: str,
        token_issuer: DistributionRegistryTokenIssuer,
        ca_file: Path,
        limits: RegistryReaderLimits = _DEFAULT_LIMITS,
    ) -> None:
        self._repository = _validate_repository(repository)
        if type(token_issuer) is not DistributionRegistryTokenIssuer:
            raise TypeError("fixed registry token issuer is required")
        if not isinstance(ca_file, Path):
            raise TypeError("registry CA file path is required")
        if type(limits) is not RegistryReaderLimits:
            raise TypeError("registry reader limits are required")
        self._token_issuer = token_issuer
        self._limits = limits
        parsed = urlsplit(token_issuer.registry_origin)
        assert parsed.hostname is not None
        self._hostname = parsed.hostname
        self._port = parsed.port or 443
        self._host_header = parsed.netloc.encode("ascii")
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        context.minimum_version = ssl.TLSVersion.TLSv1_2
        context.verify_mode = ssl.CERT_REQUIRED
        context.check_hostname = True
        try:
            context.load_verify_locations(cafile=str(ca_file))
        except (OSError, ssl.SSLError) as exc:
            raise TaskImageAuthorityConfigurationError(
                "registry CA file is invalid"
            ) from exc
        self._ssl_context = context
        self._semaphore = asyncio.Semaphore(limits.maximum_concurrent_reads)
        self._admissions: set[asyncio.Task[bool]] = set()
        self._writers: set[asyncio.StreamWriter] = set()
        self._closed = False

    async def __aenter__(self) -> Self:
        if self._closed:
            raise RuntimeError("registry reader is closed")
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        await self.aclose()

    async def aclose(self) -> None:
        if self._closed:
            return
        self._closed = True
        admissions = tuple(self._admissions)
        for admission in admissions:
            admission.cancel()
        if admissions:
            await asyncio.gather(*admissions, return_exceptions=True)
        writers = tuple(self._writers)
        for writer in writers:
            writer.close()
        if writers:
            await asyncio.gather(
                *(self._finish_close(writer) for writer in writers),
                return_exceptions=True,
            )

    async def _finish_close(self, writer: asyncio.StreamWriter) -> None:
        try:
            await asyncio.wait_for(
                writer.wait_closed(),
                timeout=self._limits.connect_timeout_seconds,
            )
        except (ConnectionError, OSError, TimeoutError):
            writer.transport.abort()

    async def _admit(self, deadline: float) -> None:
        if self._closed:
            raise RuntimeError("registry reader is closed")
        admission = asyncio.create_task(self._semaphore.acquire())
        self._admissions.add(admission)
        try:
            try:
                await _bounded_await(admission, deadline=deadline)
            except asyncio.CancelledError:
                current = asyncio.current_task()
                if self._closed and current is not None and not current.cancelling():
                    raise RuntimeError("registry reader is closed") from None
                raise
        finally:
            self._admissions.discard(admission)
        if self._closed:
            self._semaphore.release()
            raise RuntimeError("registry reader is closed")

    async def _connect(
        self,
        deadline: float,
    ) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
        return await _bounded_await(
            asyncio.open_connection(
                self._hostname,
                self._port,
                ssl=self._ssl_context,
                server_hostname=self._hostname,
                ssl_handshake_timeout=self._limits.connect_timeout_seconds,
                ssl_shutdown_timeout=self._limits.connect_timeout_seconds,
            ),
            deadline=deadline,
            operation_timeout=self._limits.connect_timeout_seconds,
        )

    def _request_bytes(
        self,
        connection: h11.Connection,
        path: str,
        descriptor: OCIDescriptor,
        token: str,
    ) -> bytes:
        try:
            request = h11.Request(
                method=b"GET",
                target=path.encode("ascii"),
                headers=[
                    (b"Host", self._host_header),
                    (b"User-Agent", _USER_AGENT),
                    (b"Accept", descriptor.media_type.encode("ascii")),
                    (b"Accept-Encoding", b"identity"),
                    (b"Authorization", b"Bearer " + token.encode("ascii")),
                    (b"Connection", b"close"),
                ],
            )
            return (connection.send(request) or b"") + (
                connection.send(h11.EndOfMessage()) or b""
            )
        except (UnicodeError, h11.LocalProtocolError):
            raise RegistryReadError("invalid registry request metadata", retryable=False) from None

    def _validate_response(
        self,
        response: h11.Response,
        kind: Literal["manifest", "blob"],
        descriptor: OCIDescriptor,
    ) -> int:
        headers = tuple(response.headers.raw_items())
        header_bytes = _header_bytes(headers)
        if header_bytes > self._limits.maximum_response_header_bytes:
            raise RegistryReadError("registry response header limit exceeded", retryable=False)
        if response.status_code != 200:
            retryable = response.status_code in {401, 408, 425, 429} or 500 <= response.status_code
            raise RegistryReadError("registry rejected digest GET", retryable=retryable)
        content_encoding = _one_header(headers, b"content-encoding")
        if content_encoding is not None and content_encoding.lower().strip() != b"identity":
            raise RegistryReadError("registry response encoding is unsupported", retryable=False)
        content_length = _one_header(headers, b"content-length")
        if content_length is not None and content_length != str(descriptor.size).encode("ascii"):
            raise RegistryReadError("registry response size mismatch", retryable=False)
        content_digest = _one_header(headers, b"docker-content-digest")
        if content_digest is not None and content_digest != descriptor.digest.encode("ascii"):
            raise RegistryReadError("registry response digest header mismatch", retryable=False)
        content_type = _one_header(headers, b"content-type")
        expected_content_type = (
            descriptor.media_type.encode("ascii")
            if kind == "manifest"
            else b"application/octet-stream"
        )
        if content_type is not None and content_type != expected_content_type:
            raise RegistryReadError("registry response media type mismatch", retryable=False)
        return header_bytes

    async def read(
        self,
        kind: Literal["manifest", "blob"],
        descriptor: OCIDescriptor,
    ) -> AsyncGenerator[bytes, None]:
        if self._closed:
            raise RuntimeError("registry reader is closed")
        if kind not in {"manifest", "blob"}:
            raise ValueError("registry object kind is invalid")
        if type(descriptor) is not OCIDescriptor:
            raise TypeError("exact OCI descriptor is required")
        if descriptor.size > self._limits.maximum_response_bytes:
            raise RegistryReadError("registry response size limit exceeded", retryable=False)
        if kind == "manifest" and descriptor.size > self._limits.maximum_manifest_bytes:
            raise RegistryReadError("registry manifest size limit exceeded", retryable=False)

        collection = "manifests" if kind == "manifest" else "blobs"
        path = f"/v2/{self._repository}/{collection}/{descriptor.digest}"
        deadline = asyncio.get_running_loop().time() + self._limits.total_timeout_seconds
        writer: asyncio.StreamWriter | None = None
        acquired = False
        response_seen = False
        pre_response_input_bytes = 0
        try:
            await self._admit(deadline)
            acquired = True
            now = datetime.now(UTC).replace(microsecond=0)
            issued = self._token_issuer.issue_pull(
                credential_id=uuid4(),
                repository=self._repository,
                issued_at=now,
                expires_at=now + _TOKEN_LIFETIME,
            )
            protocol = h11.Connection(
                h11.CLIENT,
                max_incomplete_event_size=self._limits.maximum_response_header_bytes,
            )
            request_bytes = self._request_bytes(protocol, path, descriptor, issued.token)
            stream, writer = await self._connect(deadline)
            self._writers.add(writer)
            if self._closed:
                raise RuntimeError("registry reader is closed")
            writer.write(request_bytes)
            await _bounded_await(
                writer.drain(),
                deadline=deadline,
                operation_timeout=self._limits.connect_timeout_seconds,
            )

            observed = 0
            parsed_header_bytes = 0
            in_transfer_chunk = False
            transfer_chunk_bytes = 0
            receive_bytes = min(
                self._limits.maximum_chunk_bytes + 1,
                self._limits.maximum_response_header_bytes + 1,
            )
            while True:
                if self._closed:
                    raise RuntimeError("registry reader is closed")
                event = protocol.next_event()
                if event is h11.NEED_DATA:
                    payload = await _bounded_await(
                        stream.read(receive_bytes),
                        deadline=deadline,
                        operation_timeout=self._limits.idle_timeout_seconds,
                    )
                    if not response_seen:
                        pre_response_input_bytes += len(payload)
                    protocol.receive_data(payload)
                    continue
                if isinstance(event, h11.InformationalResponse):
                    headers = tuple(event.headers.raw_items())
                    if _header_bytes(headers) > self._limits.maximum_response_header_bytes:
                        raise RegistryReadError(
                            "registry response header limit exceeded",
                            retryable=False,
                        )
                    raise RegistryReadError(
                        "registry informational response is unsupported",
                        retryable=False,
                    )
                if isinstance(event, h11.Response):
                    if response_seen:
                        raise RegistryReadError("invalid registry response", retryable=False)
                    response_seen = True
                    parsed_header_bytes = self._validate_response(event, kind, descriptor)
                    continue
                if isinstance(event, h11.Data):
                    if not response_seen:
                        raise RegistryReadError("invalid registry response", retryable=False)
                    chunk = bytes(event.data)
                    if event.chunk_start:
                        in_transfer_chunk = True
                        transfer_chunk_bytes = 0
                    if in_transfer_chunk:
                        transfer_chunk_bytes += len(chunk)
                        if transfer_chunk_bytes > self._limits.maximum_chunk_bytes:
                            raise RegistryReadError(
                                "registry response chunk limit exceeded",
                                retryable=False,
                            )
                        if event.chunk_end:
                            in_transfer_chunk = False
                    elif len(chunk) > self._limits.maximum_chunk_bytes:
                        raise RegistryReadError(
                            "registry response chunk limit exceeded",
                            retryable=False,
                        )
                    observed += len(chunk)
                    if (
                        observed > descriptor.size
                        or observed > self._limits.maximum_response_bytes
                    ):
                        raise RegistryReadError(
                            "registry response size limit exceeded",
                            retryable=False,
                        )
                    if chunk:
                        if self._closed:
                            raise RuntimeError("registry reader is closed")
                        yield chunk
                    continue
                if isinstance(event, h11.EndOfMessage):
                    trailers = tuple(event.headers.raw_items())
                    if parsed_header_bytes + _header_bytes(trailers) - 2 > (
                        self._limits.maximum_response_header_bytes
                    ):
                        raise RegistryReadError(
                            "registry response header limit exceeded",
                            retryable=False,
                        )
                    if trailers:
                        raise RegistryReadError(
                            "registry response trailers are unsupported",
                            retryable=False,
                        )
                    if not response_seen or observed != descriptor.size:
                        raise RegistryReadError(
                            "registry response size mismatch",
                            retryable=False,
                        )
                    return
                if isinstance(event, h11.ConnectionClosed):
                    raise RegistryReadError("registry response ended early", retryable=True)
                raise RegistryReadError("invalid registry response", retryable=False)
        except _TotalTimeoutError:
            raise RegistryReadError("registry total timeout", retryable=True) from None
        except _TransportTimeoutError:
            raise RegistryReadError("registry transport timeout", retryable=True) from None
        except h11.RemoteProtocolError:
            if (
                not response_seen
                and pre_response_input_bytes
                > self._limits.maximum_response_header_bytes
            ):
                raise RegistryReadError(
                    "registry response header limit exceeded",
                    retryable=False,
                ) from None
            raise RegistryReadError("registry protocol failure", retryable=False) from None
        except (ConnectionError, OSError, ssl.SSLError):
            if self._closed:
                raise RuntimeError("registry reader is closed") from None
            raise RegistryReadError("registry transport failure", retryable=True) from None
        finally:
            try:
                if writer is not None:
                    self._writers.discard(writer)
                    writer.close()
                    await self._finish_close(writer)
            finally:
                if acquired:
                    self._semaphore.release()


def load_https_registry_reader(
    settings: TaskImageAuthoritySettings,
    repository: str,
) -> HTTPSRegistryReader:
    """Load an optional reader only when its fixed CA and signer are configured."""

    if type(settings) is not TaskImageAuthoritySettings:
        raise TypeError("task-image authority settings are required")
    if settings.registry_reader_ca_file is None:
        raise TaskImageAuthorityConfigurationError(
            "registry reader configuration is unavailable"
        )
    return HTTPSRegistryReader(
        repository=repository,
        token_issuer=load_distribution_registry_token_issuer(settings),
        ca_file=settings.registry_reader_ca_file,
        limits=RegistryReaderLimits(
            connect_timeout_seconds=settings.registry_connect_timeout_seconds,
            idle_timeout_seconds=settings.registry_idle_timeout_seconds,
            total_timeout_seconds=settings.registry_total_timeout_seconds,
            maximum_response_header_bytes=settings.registry_maximum_response_header_bytes,
            maximum_chunk_bytes=settings.registry_maximum_chunk_bytes,
            maximum_manifest_bytes=settings.registry_maximum_manifest_bytes,
            maximum_response_bytes=settings.registry_maximum_response_bytes,
            maximum_concurrent_reads=settings.registry_read_concurrency_limit,
        ),
    )


__all__ = [
    "HTTPSRegistryReader",
    "RegistryReadError",
    "RegistryReaderLimits",
    "load_https_registry_reader",
]
