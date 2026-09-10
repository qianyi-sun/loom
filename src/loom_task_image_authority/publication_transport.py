"""Fixed-operation mutual-TLS publication signer client; no signing private key.

Explicit trusted composition supplies a dedicated CA and client TLS identity.
This is transport, not proof of distribution, signature validity or readiness:
the existing publication verifier and fenced final transaction remain mandatory.
No production runtime composes this client until those other gates are met.
"""

from __future__ import annotations

import asyncio
import ssl
from dataclasses import dataclass, fields
from pathlib import Path
from types import TracebackType
from typing import Self
from urllib.parse import urlsplit

import h11

from loom_task_image_authority.config import _validate_https_origin
from loom_task_image_authority.publication_contracts import (
    MAX_SIGNER_REPLY_BYTES,
    decode_unsigned_input,
)


@dataclass(frozen=True, slots=True)
class PublicationSignerLimits:
    connect_timeout_seconds: float = 3.0
    idle_timeout_seconds: float = 3.0
    total_timeout_seconds: float = 5.0
    maximum_header_bytes: int = 32 * 1024
    maximum_wire_bytes: int = 1024 * 1024
    maximum_concurrent_requests: int = 2

    def __post_init__(self) -> None:
        ceilings = (10.0, 10.0, 10.0, 64 * 1024, 1024 * 1024, 32)
        for item, ceiling in zip(fields(self), ceilings, strict=True):
            value = getattr(self, item.name)
            if (
                isinstance(item.default, float)
                and (type(value) is not float or not 0.0 < value <= ceiling)
            ) or (
                isinstance(item.default, int)
                and (type(value) is not int or not 0 < value <= ceiling)
            ):
                raise ValueError("publication signer limits must be positive and bounded")


_DEFAULT_LIMITS = PublicationSignerLimits()


def _check_raw_framing(raw_head: bytes) -> None:
    # h11 coalesces identical Content-Length fields/comma lists, and permits
    # Transfer-Encoding to override Content-Length. Reject that ambiguity before
    # parser normalization; one short-lived signing reply needs no folded fields.
    unfolded = raw_head.replace(b"\r\n", b"")
    if b"\r" in unfolded or b"\n" in unfolded:
        raise ValueError("bare newline in response framing")
    framing: dict[bytes, bytes] = {}
    for line in raw_head.split(b"\r\n")[1:]:
        if not line or line[:1] in {b" ", b"\t"}:
            raise ValueError("invalid raw response header")
        name, separator, value = line.partition(b":")
        if not separator:
            raise ValueError("invalid raw response header")
        name, value = name.lower(), value.strip(b" \t")
        if name in {b"content-length", b"transfer-encoding"}:
            if name in framing:
                raise ValueError("duplicate response framing")
            framing[name] = value
    length = framing.get(b"content-length")
    if length is not None and (not length.isdigit() or b"transfer-encoding" in framing):
        raise ValueError("ambiguous response framing")


class HTTPSPublicationSigner:
    """One bounded POST per call; never retries, redirects or uses proxy settings.

    Queue time counts toward the total deadline. Each operation owns one socket,
    aborted on every exit. Close cancels and joins admitted and queued operations;
    cancellation during a connect handoff retains a socket disposal owner.
    """

    def __init__(
        self, *, origin: str, ca_file: Path, client_cert_file: Path,
        client_key_file: Path, limits: PublicationSignerLimits = _DEFAULT_LIMITS,
    ) -> None:
        _validate_https_origin(origin, label="publication signer origin")
        if (
            any(not isinstance(path, Path) for path in (ca_file, client_cert_file, client_key_file))
            or type(limits) is not PublicationSignerLimits
        ):
            raise ValueError("publication signer TLS identity and limits are required")
        limits.__post_init__()
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        context.minimum_version = ssl.TLSVersion.TLSv1_3
        context.verify_mode = ssl.CERT_REQUIRED
        context.check_hostname = True
        try:
            context.load_verify_locations(cafile=str(ca_file))
            # Never prompt for a key password on an unattended service startup.
            context.load_cert_chain(client_cert_file, client_key_file, password=lambda: b"")
        except (OSError, ssl.SSLError, ValueError):
            raise ValueError("publication signer TLS identity is invalid") from None
        parsed = urlsplit(origin)
        assert parsed.hostname is not None
        self._hostname, self._port = parsed.hostname, parsed.port or 443
        self._host = parsed.netloc.encode("ascii")
        self._context, self._limits = context, limits
        self._semaphore = asyncio.Semaphore(limits.maximum_concurrent_requests)
        self._requests: set[asyncio.Task[bytes]] = set()
        self._close_task: asyncio.Task[None] | None = None

    async def __aenter__(self) -> Self:
        if self._close_task is not None:
            raise ValueError("publication signer is closed")
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

    async def sign_publication(
        self, canonical_unsigned_input: bytes, *, maximum_reply_bytes: int,
    ) -> bytes:
        if (
            self._close_task is not None
            or type(maximum_reply_bytes) is not int
            or not 0 < maximum_reply_bytes <= MAX_SIGNER_REPLY_BYTES
        ):
            raise ValueError("publication signer request is unavailable or invalid")
        decode_unsigned_input(canonical_unsigned_input)
        deadline = asyncio.get_running_loop().time() + self._limits.total_timeout_seconds
        request = asyncio.create_task(self._sign(canonical_unsigned_input, maximum_reply_bytes, deadline))
        self._requests.add(request)
        request.add_done_callback(self._requests.discard)
        return await request

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
                task.add_done_callback(dispose)
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)

    def _check_response(self, event: h11.Response, maximum_reply_bytes: int) -> None:
        headers = tuple(event.headers.raw_items())
        if 2 + sum(len(k) + len(v) + 4 for k, v in headers) > self._limits.maximum_header_bytes:
            raise ValueError("response header ceiling")
        if event.status_code in {429, 502, 503, 504}:
            raise ConnectionError("publication signer temporarily unavailable")
        if event.status_code != 200:
            raise ValueError("response status")
        for name in (b"content-type", b"content-encoding", b"content-length"):
            values = [v for k, v in headers if k.lower() == name]
            if len(values) > 1:
                raise ValueError("duplicate response metadata")
            if name == b"content-type" and values != [b"application/json"]:
                raise ValueError("response content type")
            if not values:
                continue
            if name == b"content-encoding" and values[0].lower() != b"identity":
                raise ValueError("compressed response")
            if name == b"content-length" and int(values[0]) > maximum_reply_bytes:
                raise ValueError("response body ceiling")

    async def _sign(self, unsigned: bytes, maximum_reply_bytes: int, deadline: float) -> bytes:
        writer: asyncio.StreamWriter | None = None
        try:
            async with asyncio.timeout_at(deadline), self._semaphore:
                stream, writer = await self._connect()
                protocol = h11.Connection(h11.CLIENT, max_incomplete_event_size=self._limits.maximum_header_bytes)
                request = h11.Request(method=b"POST", target=b"/v1/publications/sign", headers=[
                    (b"Host", self._host), (b"Content-Type", b"application/json"),
                    (b"Accept", b"application/json"), (b"Accept-Encoding", b"identity"),
                    (b"Content-Length", str(len(unsigned)).encode("ascii")),
                    (b"Connection", b"close"),
                ])
                for outgoing in (request, h11.Data(data=unsigned), h11.EndOfMessage()):
                    writer.write(protocol.send(outgoing) or b"")
                async with asyncio.timeout(self._limits.connect_timeout_seconds):
                    await writer.drain()
                body, raw_head = bytearray(), bytearray()
                head_complete = response_seen = False
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
                            raise ValueError("wire ceiling")
                        if not head_complete:
                            raw_head.extend(chunk)
                            end = raw_head.find(b"\r\n\r\n")
                            if (end + 4 if end >= 0 else len(raw_head)) > self._limits.maximum_header_bytes:
                                raise ValueError("raw response header ceiling")
                            if end >= 0:
                                _check_raw_framing(bytes(raw_head[:end]))
                                head_complete = True
                                raw_head.clear()
                        protocol.receive_data(chunk)
                    elif isinstance(event, h11.Response):
                        if response_seen or not head_complete:
                            raise ValueError("duplicate response")
                        self._check_response(event, maximum_reply_bytes)
                        response_seen = True
                    elif isinstance(event, h11.Data):
                        if not response_seen or len(body) + len(event.data) > maximum_reply_bytes:
                            raise ValueError("body ceiling")
                        body.extend(event.data)
                    elif isinstance(event, h11.EndOfMessage):
                        if not response_seen or event.headers or not body:
                            raise ValueError("invalid response end")
                        return bytes(body)
                    else:
                        raise ValueError("unsupported response event")
        except ssl.SSLError:
            raise ValueError("publication signer TLS authentication failed") from None
        except TimeoutError:
            raise TimeoutError("publication signer deadline exceeded") from None
        except OSError:
            raise ConnectionError("publication signer connection unavailable") from None
        except (ValueError, h11.ProtocolError):
            raise ValueError("publication signer response invalid or exceeded limits") from None
        finally:
            if writer is not None:
                writer.transport.abort()
