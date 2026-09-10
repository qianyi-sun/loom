"""Owned TLS listener with bounded pre-handshake admission and fixed operations.

No forwarded identity, plaintext policy dispatch or arbitrary signing endpoint.
One accepted socket owns one TLS handshake and at most one operation. The socket
deadline includes handshake, request reads, policy queuing, signing and response.
"""

from __future__ import annotations

import asyncio
import hashlib
import math
import re
import socket
import ssl
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

import h11
from sqlalchemy.exc import SQLAlchemyError

from loom_task_image_authority.keyset_signing_request import decode_keyset_signing_request
from loom_task_image_authority.publication_contracts import (
    MAX_PUBLICATION_BYTES,
    MAX_SIGNER_REPLY_BYTES,
    decode_unsigned_input,
)
from loom_task_image_authority.publication_keyset import MAX_KEYSET_BYTES, MAX_KEYSET_ENVELOPE_BYTES


class Operations(Protocol):
    async def sign_keyset(self, canonical_request: bytes) -> bytes: ...

    async def sign_publication(self, canonical_unsigned_input: bytes) -> bytes: ...


@dataclass(frozen=True)
class SignerServerLimits:
    maximum_connections: int = 16
    maximum_operations: int = 2
    maximum_header_bytes: int = 16 * 1024
    handshake_seconds: float = 3.0
    idle_seconds: float = 3.0
    total_seconds: float = 10.0

    def __post_init__(self) -> None:
        for value, ceiling in (
            (self.maximum_connections, 128), (self.maximum_operations, 32), (self.maximum_header_bytes, 64 * 1024),
        ):
            if type(value) is not int or not 1 <= value <= ceiling:
                raise ValueError("invalid signer connection/header bound")
        for seconds in (self.handshake_seconds, self.idle_seconds, self.total_seconds):
            if type(seconds) is not float or not math.isfinite(seconds) or not 0.1 < seconds <= 10:
                raise ValueError("invalid signer deadline")


_DEFAULT_LIMITS = SignerServerLimits()


def _tls(ca: Path, certificate: Path, key: Path) -> ssl.SSLContext:
    if any(not isinstance(path, Path) for path in (ca, certificate, key)):
        raise ValueError("explicit signer TLS identity required")
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.minimum_version = ssl.TLSVersion.TLSv1_3
    context.verify_mode = ssl.CERT_REQUIRED
    try:
        context.load_verify_locations(cafile=str(ca))
        context.load_cert_chain(certificate, key, password=lambda: b"")
    except (OSError, ValueError, ssl.SSLError):
        raise ValueError("invalid signer TLS identity") from None
    return context


def _headers(raw: bytes) -> dict[bytes, bytes]:
    unfolded = raw.replace(b"\r\n", b"")
    if b"\r" in unfolded or b"\n" in unfolded:
        raise ValueError("bare newline in request framing")
    values: dict[bytes, bytes] = {}
    for line in raw[:-4].split(b"\r\n")[1:]:
        name, separator, value = line.partition(b":")
        if not separator or not re.fullmatch(rb"[!#$%&'*+.^_`|~0-9A-Za-z-]+", name):
            raise ValueError("invalid header framing")
        name, value = name.lower(), value.strip(b" \t")
        if name in values:
            raise ValueError("duplicate header")
        if name in {b"transfer-encoding", b"te", b"expect", b"upgrade", b"proxy-connection", b"forwarded"} or name.startswith((b"x-forwarded-", b"proxy-")):
            raise ValueError("unsupported proxy, upgrade or transfer header")
        values[name] = value
    if (
        values.get(b"content-type") != b"application/json"
        or values.get(b"content-encoding", b"identity") != b"identity"
        or not re.fullmatch(rb"0|[1-9][0-9]{0,6}", values.get(b"content-length", b""))
    ):
        raise ValueError("invalid request metadata")
    return values


class SignerServer:
    """Trusted explicit composition; constructing a server does not activate it."""

    def __init__(
        self, operations: Operations, *, ca_file: Path, certificate_file: Path,
        private_key_file: Path, peer_operations: Mapping[str, frozenset[str]],
        limits: SignerServerLimits = _DEFAULT_LIMITS,
    ) -> None:
        if type(limits) is not SignerServerLimits:
            raise ValueError("invalid signer limits")
        limits.__post_init__()
        if not 1 <= len(peer_operations) <= 128 or any(
            type(pin) is not str or not re.fullmatch(r"[0-9a-f]{64}", pin)
            or type(allowed) is not frozenset or not allowed or not allowed <= {"keyset", "publication"}
            for pin, allowed in peer_operations.items()
        ):
            raise ValueError("explicit peer certificate operation pins required")
        self._context = _tls(ca_file, certificate_file, private_key_file)
        self._operations, self._peers, self._limits = operations, dict(peer_operations), limits
        self._connections = asyncio.Semaphore(limits.maximum_connections)
        self._policy_slots = asyncio.Semaphore(limits.maximum_operations)
        self._listener: socket.socket | None = None
        self._accept_task: asyncio.Task[None] | None = None
        self._close_task: asyncio.Task[None] | None = None
        self._tasks: set[asyncio.Task[None]] = set()
        self._port: int | None = None

    @property
    def port(self) -> int:
        if self._port is None:
            raise ValueError("signer listener has not started")
        return self._port

    @property
    def active_connections(self) -> int:
        return len(self._tasks)

    async def start(self, *, host: str, port: int) -> None:
        if self._listener is not None or self._close_task is not None:
            raise ValueError("signer cannot be started twice")
        if type(port) is not int or not 0 <= port <= 65535:
            raise ValueError("invalid signer listener port")
        # A numeric bind address avoids ambient DNS resolution during startup.
        try:
            socket.inet_pton(socket.AF_INET, host)
        except (OSError, TypeError):
            raise ValueError("signer bind address must be explicit IPv4") from None
        listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            listener.setblocking(False)
            listener.bind((host, port))
            listener.listen(self._limits.maximum_connections)
        except BaseException:
            listener.close()
            raise
        self._listener, self._port = listener, listener.getsockname()[1]
        self._accept_task = asyncio.create_task(self._accept())

    async def aclose(self) -> None:
        if self._close_task is None:
            self._close_task = asyncio.create_task(self._close())
        await asyncio.shield(self._close_task)

    async def _close(self) -> None:
        if self._accept_task is not None:
            self._accept_task.cancel()
            await asyncio.gather(self._accept_task, return_exceptions=True)
        if self._listener is not None:
            self._listener.close()
        pending = tuple(self._tasks)
        for task in pending:
            task.cancel()
        await asyncio.gather(*pending, return_exceptions=True)

    async def _accept(self) -> None:
        assert self._listener is not None
        loop = asyncio.get_running_loop()
        while True:
            # Reserve capacity BEFORE accepting or allocating a TLS transport.
            # Excess TCP connections remain in the finite OS listen backlog.
            await self._connections.acquire()
            accepted: socket.socket | None = None
            transferred = False
            try:
                accepted, _ = await loop.sock_accept(self._listener)
                deadline = loop.time() + self._limits.total_seconds
                task = asyncio.create_task(self._serve(accepted, deadline))
                self._tasks.add(task)
                assert accepted is not None
                owned_socket: socket.socket = accepted
                # The callback owns cleanup even if close cancels the task
                # before its coroutine executes its first instruction.
                def finished(done: asyncio.Task[None], owned: socket.socket = owned_socket) -> None:
                    owned.close()
                    self._connections.release()
                    self._tasks.discard(done)
                    if not done.cancelled():
                        done.exception()
                task.add_done_callback(finished)
                transferred = True
            finally:
                if not transferred:
                    if accepted is not None:
                        accepted.close()
                    self._connections.release()

    async def _serve(self, accepted: socket.socket, deadline: float) -> None:
        writer: asyncio.StreamWriter | None = None
        try:
            # Reserve a bounded response budget inside the connection deadline.
            async with asyncio.timeout_at(deadline - 0.1):
                loop = asyncio.get_running_loop()
                reader = asyncio.StreamReader(limit=self._limits.maximum_header_bytes)
                protocol = asyncio.StreamReaderProtocol(reader)
                transport, _ = await loop.connect_accepted_socket(
                    lambda: protocol, accepted, ssl=self._context,
                    ssl_handshake_timeout=self._limits.handshake_seconds,
                    ssl_shutdown_timeout=self._limits.handshake_seconds,
                )
                writer = asyncio.StreamWriter(transport, protocol, reader, loop)
                tls = writer.get_extra_info("ssl_object")
                peer = tls.getpeercert(binary_form=True) if tls is not None else None
                if not peer:
                    raise PermissionError
                allowed = self._peers.get(hashlib.sha256(peer).hexdigest(), frozenset())
                if not allowed:
                    raise PermissionError
                body = await self._dispatch(reader, allowed)
                await self._reply(writer, 200, body)
        except asyncio.CancelledError:
            raise
        except PermissionError:
            await self._refuse(writer, 403, deadline)
        except (TimeoutError, ConnectionError, SQLAlchemyError):
            await self._refuse(writer, 503, deadline)
        except (ValueError, h11.ProtocolError, asyncio.IncompleteReadError, asyncio.LimitOverrunError):
            await self._refuse(writer, 400, deadline)
        except (OSError, ssl.SSLError):
            pass
        except Exception:
            # No exception payload (potential key/provider/DB details) on wire.
            await self._refuse(writer, 503, deadline)
        finally:
            if writer is not None:
                writer.transport.abort()

    async def _dispatch(self, reader: asyncio.StreamReader, allowed: frozenset[str]) -> bytes:
        async with asyncio.timeout(self._limits.idle_seconds):
            raw = await reader.readuntil(b"\r\n\r\n")
        if len(raw) > self._limits.maximum_header_bytes:
            raise ValueError("header ceiling")
        headers = _headers(raw)
        protocol = h11.Connection(h11.SERVER, max_incomplete_event_size=self._limits.maximum_header_bytes)
        protocol.receive_data(raw)
        event = protocol.next_event()
        if not isinstance(event, h11.Request) or event.method != b"POST" or event.http_version != b"1.1":
            raise ValueError("unsupported method or framing")
        operation = {b"/v1/keysets/sign": "keyset", b"/v1/publications/sign": "publication"}.get(event.target)
        if operation is None:
            raise ValueError("unsupported signer operation")
        if operation not in allowed:
            raise PermissionError
        length = int(headers[b"content-length"])
        maximum = MAX_KEYSET_BYTES if operation == "keyset" else MAX_PUBLICATION_BYTES
        if not 0 < length <= maximum:
            raise ValueError("request body ceiling")
        async with asyncio.timeout(self._limits.idle_seconds):
            body = await reader.readexactly(length)
        protocol.receive_data(body)
        received = bytearray()
        while True:
            event = protocol.next_event()
            if isinstance(event, h11.Data):
                received.extend(event.data)
            elif isinstance(event, h11.EndOfMessage):
                if event.headers or bytes(received) != body:
                    raise ValueError("ambiguous request end")
                break
            else:
                raise ValueError("incomplete request")
        if operation == "keyset":
            decode_keyset_signing_request(body)
        else:
            decode_unsigned_input(body)
        async with self._policy_slots:
            reply = await (self._operations.sign_keyset(body) if operation == "keyset" else self._operations.sign_publication(body))
        reply_limit = MAX_KEYSET_ENVELOPE_BYTES if operation == "keyset" else MAX_SIGNER_REPLY_BYTES
        if type(reply) is not bytes or not 0 < len(reply) <= reply_limit:
            raise ValueError("signer reply ceiling")
        return reply

    async def _refuse(self, writer: asyncio.StreamWriter | None, status: int, deadline: float) -> None:
        if writer is not None:
            try:
                async with asyncio.timeout_at(deadline):
                    await self._reply(writer, status, b'{"error":"signing_refused"}')
            except (OSError, TimeoutError):
                pass

    async def _reply(self, writer: asyncio.StreamWriter, status: int, body: bytes) -> None:
        reason = {200: "OK", 400: "Bad Request", 403: "Forbidden", 503: "Service Unavailable"}[status]
        writer.write(
            f"HTTP/1.1 {status} {reason}\r\nContent-Type: application/json\r\nContent-Length: {len(body)}\r\nConnection: close\r\n\r\n".encode() + body,
        )
        await writer.drain()
