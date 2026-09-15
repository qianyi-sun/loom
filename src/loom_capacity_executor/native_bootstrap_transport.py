"""Dedicated peer-pinned TLS delivery, with no remote command or shared storage.

One TLS 1.3 connection carries one length-prefixed canonical document and one
bounded reply. TLS EOF is not request framing; a fixed receiver-process adapter
closes its own stdin pipe. Both endpoints require an already-hardened dedicated
process before loading keys or accepting secret input. Installation and runtime
composition remain protected operator responsibilities, never request fields.
"""

from __future__ import annotations

import asyncio
import ctypes
import hashlib
import ipaddress
import math
import re
import resource
import socket
import ssl
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Literal, Protocol
from uuid import UUID

from loom_capacity_executor.native_bootstrap_delivery import (
    _MAX_DELIVERY_BYTES,
    _MAX_QUERY_BYTES,
    _MAX_RECEIPT_BYTES,
    BootstrapDeliveryError,
    NativeBootstrapDeliveryReceiptV1,
    _canonical,
    _decode,
    expected_native_delivery_receipt,
    parse_native_delivery_query,
    parse_native_delivery_receipt,
)
from loom_capacity_executor.pinned_admission_transport import (
    PinnedAdmissionFileV1,
    _read_pinned,
    _sealed_pem,
)
from loom_capacity_manager.executable_contracts import ExecutableIntentBindingV2

_ALPN = "loom-native-bootstrap/1"
_IDENTIFIER = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}", re.ASCII)
_DIGEST = re.compile(r"[0-9a-f]{64}", re.ASCII)
_FAILURE = "native bootstrap transport unavailable or refused"


def _identifier(value: str) -> None:
    if type(value) is not str or _IDENTIFIER.fullmatch(value) is None:
        raise ValueError("invalid native bootstrap transport identity")


def _expiry(value: datetime) -> None:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("native bootstrap transport expiry must be aware")


def _check_deadline(deadline: float) -> None:
    if asyncio.get_running_loop().time() >= deadline:
        raise BootstrapDeliveryError(_FAILURE)


def _assert_private_process() -> None:
    # This check does not silently change a general application's process-wide
    # settings. The dedicated entrypoint hardens before constructing transport.
    libc = ctypes.CDLL(None, use_errno=True)
    if resource.getrlimit(resource.RLIMIT_CORE) != (0, 0) or libc.prctl(3, 0, 0, 0, 0) != 0:
        raise BootstrapDeliveryError("native bootstrap transport process is not hardened")


@dataclass(frozen=True)
class NativeBootstrapTLSIdentity:
    ca: PinnedAdmissionFileV1
    certificate: PinnedAdmissionFileV1
    private_key: PinnedAdmissionFileV1


@dataclass(frozen=True)
class NativeBootstrapTransportLimits:
    maximum_connections: int = 8
    maximum_operations: int = 2
    handshake_seconds: float = 3.0
    total_seconds: float = 20.0

    def __post_init__(self) -> None:
        for value, maximum in ((self.maximum_connections, 32), (self.maximum_operations, 8)):
            if type(value) is not int or not 1 <= value <= maximum:
                raise ValueError("native bootstrap concurrency bound is invalid")
        for seconds, ceiling in ((self.handshake_seconds, 5), (self.total_seconds, 30)):
            if type(seconds) is not float or not math.isfinite(seconds) or not 0.05 <= seconds <= ceiling:
                raise ValueError("native bootstrap time bound is invalid")


@dataclass(frozen=True)
class NativeBootstrapPeer:
    pool_id: str
    executor_id: str
    executor_incarnation: UUID
    operations: frozenset[Literal["deliver", "status"]]
    expires_at: datetime

    def __post_init__(self) -> None:
        _identifier(self.pool_id)
        _identifier(self.executor_id)
        _expiry(self.expires_at)
        if (type(self.executor_incarnation) is not UUID or self.executor_incarnation.int == 0
            or type(self.operations) is not frozenset or not self.operations
            or not self.operations <= {"deliver", "status"}):
            raise ValueError("native bootstrap peer authority is invalid")


@dataclass(frozen=True)
class NativeBootstrapRoute:
    address: str
    port: int
    hostname: str
    target_node: str
    pool_id: str
    trusted_release_sha256: str
    server_certificate_sha256: str
    expires_at: datetime

    def __post_init__(self) -> None:
        ipaddress.ip_address(self.address)
        if type(self.port) is not int or not 1 <= self.port <= 65535:
            raise ValueError("native bootstrap route port is invalid")
        _identifier(self.hostname)
        _identifier(self.target_node)
        _identifier(self.pool_id)
        _expiry(self.expires_at)
        if any(type(value) is not str or not _DIGEST.fullmatch(value)
            for value in (self.trusted_release_sha256, self.server_certificate_sha256)):
            raise ValueError("native bootstrap route digest is invalid")


_DEFAULT_LIMITS = NativeBootstrapTransportLimits()


def _tls_context(identity: NativeBootstrapTLSIdentity, *, server: bool) -> ssl.SSLContext:
    _assert_private_process()
    if type(identity) is not NativeBootstrapTLSIdentity:
        raise BootstrapDeliveryError(_FAILURE)
    try:
        ca, cert, key = (PinnedAdmissionFileV1.model_validate_json(value.model_dump_json())
            for value in (identity.ca, identity.certificate, identity.private_key))
        ca_wire = _read_pinned(ca, maximum=1024 * 1024)
        cert_wire = _read_pinned(cert, maximum=1024 * 1024)
        key_wire = _read_pinned(key, maximum=1024 * 1024)
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER if server else ssl.PROTOCOL_TLS_CLIENT)
        context.minimum_version = context.maximum_version = ssl.TLSVersion.TLSv1_3
        context.verify_mode = ssl.CERT_REQUIRED
        context.check_hostname = not server
        context.options |= ssl.OP_NO_TICKET
        if server:
            context.num_tickets = 0
        context.set_alpn_protocols([_ALPN])
        context.load_verify_locations(cadata=ca_wire.decode("ascii"))
        with _sealed_pem(cert_wire) as certificate, _sealed_pem(key_wire) as private_key:
            context.load_cert_chain(certificate, private_key, password=lambda: b"")
        return context
    except (ValueError, OSError, AttributeError):
        raise BootstrapDeliveryError(_FAILURE) from None


def _peer_digest(writer: asyncio.StreamWriter) -> str:
    tls = writer.get_extra_info("ssl_object")
    if tls is None or tls.version() != "TLSv1.3" or tls.selected_alpn_protocol() != _ALPN:
        raise BootstrapDeliveryError(_FAILURE)
    certificate = tls.getpeercert(binary_form=True)
    if not certificate:
        raise BootstrapDeliveryError(_FAILURE)
    return hashlib.sha256(certificate).hexdigest()


def _request(raw: bytes, operation: bytes) -> tuple[ExecutableIntentBindingV2, NativeBootstrapDeliveryReceiptV1]:
    if operation == b"D":
        value = _decode(raw)
        return value.physical.binding, expected_native_delivery_receipt(raw)
    if operation == b"S":
        query = parse_native_delivery_query(raw)
        return query.physical.binding, query.expected
    raise BootstrapDeliveryError(_FAILURE)


def _scope(binding: ExecutableIntentBindingV2, *, node: str, pool: str, release: str) -> None:
    if (binding.node_ids != (node,) or binding.pool_id != pool
        or binding.execution.trusted_fleet_release_sha256 != release):
        raise BootstrapDeliveryError(_FAILURE)


class NativeBootstrapTLSClient:
    def __init__(self, *, route: NativeBootstrapRoute, identity: NativeBootstrapTLSIdentity,
        limits: NativeBootstrapTransportLimits = _DEFAULT_LIMITS) -> None:
        if type(route) is not NativeBootstrapRoute or type(limits) is not NativeBootstrapTransportLimits:
            raise BootstrapDeliveryError(_FAILURE)
        route.__post_init__()
        limits.__post_init__()
        self._context = _tls_context(identity, server=False)
        self._route, self._limits = route, limits
        self._slots = asyncio.Semaphore(limits.maximum_operations)
        self._tasks: set[asyncio.Task[NativeBootstrapDeliveryReceiptV1 | None]] = set()
        self._close_task: asyncio.Task[None] | None = None

    async def aclose(self) -> None:
        if self._close_task is None:
            self._close_task = asyncio.create_task(self._close())
        await asyncio.shield(self._close_task)

    async def _close(self) -> None:
        tasks = tuple(self._tasks)
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

    async def deliver(self, raw: bytes) -> NativeBootstrapDeliveryReceiptV1:
        receipt = await self._call(raw, b"D")
        if receipt is None:
            raise BootstrapDeliveryError(_FAILURE)
        return receipt

    async def observe_receipt(self, raw: bytes) -> NativeBootstrapDeliveryReceiptV1 | None:
        return await self._call(raw, b"S")

    async def _call(self, raw: bytes, operation: bytes) -> NativeBootstrapDeliveryReceiptV1 | None:
        deadline = asyncio.get_running_loop().time() + self._limits.total_seconds
        _assert_private_process()
        if self._close_task is not None or len(self._tasks) >= self._limits.maximum_connections:
            raise BootstrapDeliveryError(_FAILURE)
        binding, expected = _request(raw, operation)
        route = self._route
        _scope(binding, node=route.target_node, pool=route.pool_id, release=route.trusted_release_sha256)
        _check_deadline(deadline)
        task = asyncio.create_task(self._exchange(raw, operation, expected, deadline))
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)
        return await task

    async def _connect(self) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
        route = self._route
        task = asyncio.create_task(asyncio.open_connection(route.address, route.port,
            ssl=self._context, server_hostname=route.hostname, limit=8192,
            ssl_handshake_timeout=self._limits.handshake_seconds,
            ssl_shutdown_timeout=self._limits.handshake_seconds))
        transferred = False

        def dispose(done: asyncio.Task[tuple[asyncio.StreamReader, asyncio.StreamWriter]]) -> None:
            if not done.cancelled() and done.exception() is None:
                done.result()[1].transport.abort()

        try:
            async with asyncio.timeout(self._limits.handshake_seconds):
                result = await asyncio.shield(task)
            transferred = True
            return result
        finally:
            if not transferred:
                task.add_done_callback(dispose)
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)

    async def _exchange(self, raw: bytes, operation: bytes,
        expected: NativeBootstrapDeliveryReceiptV1, deadline: float) -> NativeBootstrapDeliveryReceiptV1 | None:
        writer: asyncio.StreamWriter | None = None
        try:
            async with asyncio.timeout_at(deadline), self._slots:
                _check_deadline(deadline)
                if datetime.now(UTC) >= self._route.expires_at:
                    raise BootstrapDeliveryError(_FAILURE)
                reader, writer = await self._connect()
                if (_peer_digest(writer) != self._route.server_certificate_sha256
                    or datetime.now(UTC) >= self._route.expires_at):
                    raise BootstrapDeliveryError(_FAILURE)
                _check_deadline(deadline)
                # No application bytes cross the socket until CA, hostname,
                # ALPN, exact leaf pin and configured route lifetime all pass.
                writer.write(b"LNB1" + operation + len(raw).to_bytes(4, "big") + raw)
                await writer.drain()
                header = await reader.readexactly(9)
                _check_deadline(deadline)
                status, size = header[4:5], int.from_bytes(header[5:], "big")
                if header[:4] != b"LNR1" or datetime.now(UTC) >= self._route.expires_at:
                    raise BootstrapDeliveryError(_FAILURE)
                if status == b"U" and size == 0 and operation == b"S":
                    return None
                if status != b"R" or not 0 < size <= _MAX_RECEIPT_BYTES:
                    raise BootstrapDeliveryError(_FAILURE)
                receipt = parse_native_delivery_receipt(await reader.readexactly(size))
                _check_deadline(deadline)
                if receipt != expected or datetime.now(UTC) >= self._route.expires_at:
                    raise BootstrapDeliveryError(_FAILURE)
                return receipt
        except asyncio.CancelledError:
            raise
        except Exception:
            raise BootstrapDeliveryError(_FAILURE) from None
        finally:
            if writer is not None:
                writer.transport.abort()


class _Operations(Protocol):
    async def receive(self, raw: bytes) -> NativeBootstrapDeliveryReceiptV1: ...
    async def observe_receipt(self, raw: bytes) -> NativeBootstrapDeliveryReceiptV1 | None: ...


class NativeBootstrapTLSServer:
    def __init__(self, operations: _Operations, *, identity: NativeBootstrapTLSIdentity,
        target_node: str, pool_id: str, trusted_release_sha256: str,
        peers: Mapping[str, NativeBootstrapPeer], limits: NativeBootstrapTransportLimits = _DEFAULT_LIMITS) -> None:
        _identifier(target_node)
        _identifier(pool_id)
        if not _DIGEST.fullmatch(trusted_release_sha256) or type(limits) is not NativeBootstrapTransportLimits:
            raise BootstrapDeliveryError(_FAILURE)
        limits.__post_init__()
        if not 1 <= len(peers) <= 32:
            raise BootstrapDeliveryError(_FAILURE)
        for digest, peer in peers.items():
            if not _DIGEST.fullmatch(digest) or type(peer) is not NativeBootstrapPeer:
                raise BootstrapDeliveryError(_FAILURE)
            peer.__post_init__()
        self._context = _tls_context(identity, server=True)
        self._operations, self._peers, self._limits = operations, dict(peers), limits
        self._node, self._pool, self._release = target_node, pool_id, trusted_release_sha256
        self._connections = asyncio.Semaphore(limits.maximum_connections)
        self._operation_slots = asyncio.Semaphore(limits.maximum_operations)
        self._listener: socket.socket | None = None
        self._accept_task: asyncio.Task[None] | None = None
        self._close_task: asyncio.Task[None] | None = None
        self._tasks: set[asyncio.Task[None]] = set()

    @property
    def port(self) -> int:
        if self._listener is None:
            raise BootstrapDeliveryError(_FAILURE)
        return int(self._listener.getsockname()[1])

    @property
    def active_connections(self) -> int:
        return len(self._tasks)

    async def start(self, *, host: str, port: int) -> None:
        _assert_private_process()
        if (self._listener is not None or self._close_task is not None
            or type(port) is not int or not 0 <= port <= 65535):
            raise BootstrapDeliveryError(_FAILURE)
        address = ipaddress.ip_address(host)
        listener = socket.socket(socket.AF_INET if address.version == 4 else socket.AF_INET6, socket.SOCK_STREAM)
        try:
            listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            listener.setblocking(False)
            listener.bind((host, port))
            listener.listen(self._limits.maximum_connections)
        except BaseException:
            listener.close()
            raise
        self._listener = listener
        self._accept_task = asyncio.create_task(self._accept())
        self._accept_task.add_done_callback(lambda done: None if done.cancelled() else done.exception())

    async def wait(self) -> None:
        """Let the fixed supervisor observe listener failure without owning it."""
        if self._accept_task is None:
            raise BootstrapDeliveryError(_FAILURE)
        await asyncio.shield(self._accept_task)

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
        tasks = tuple(self._tasks)
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

    async def _accept(self) -> None:
        try:
            await self._accept_connections()
        except asyncio.CancelledError:
            raise
        except Exception:
            raise BootstrapDeliveryError(_FAILURE) from None
        finally:
            if self._listener is not None:
                self._listener.close()

    async def _accept_connections(self) -> None:
        assert self._listener is not None
        loop = asyncio.get_running_loop()
        while True:
            await self._connections.acquire()
            accepted: socket.socket | None = None
            transferred = False
            try:
                accepted, _ = await loop.sock_accept(self._listener)
                task = asyncio.create_task(self._serve(accepted, loop.time() + self._limits.total_seconds))
                self._tasks.add(task)
                owned_socket: socket.socket = accepted

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
            async with asyncio.timeout_at(deadline):
                loop = asyncio.get_running_loop()
                reader = asyncio.StreamReader(limit=8192)
                protocol = asyncio.StreamReaderProtocol(reader)
                transport, _ = await loop.connect_accepted_socket(lambda: protocol, accepted,
                    ssl=self._context, ssl_handshake_timeout=self._limits.handshake_seconds,
                    ssl_shutdown_timeout=self._limits.handshake_seconds)
                writer = asyncio.StreamWriter(transport, protocol, reader, loop)
                peer = self._peers.get(_peer_digest(writer))
                if peer is None or datetime.now(UTC) >= peer.expires_at:
                    raise BootstrapDeliveryError(_FAILURE)
                header = await reader.readexactly(9)
                operation, size = header[4:5], int.from_bytes(header[5:], "big")
                name = {b"D": "deliver", b"S": "status"}.get(operation)
                maximum = _MAX_DELIVERY_BYTES if operation == b"D" else _MAX_QUERY_BYTES
                if header[:4] != b"LNB1" or name not in peer.operations or not 0 < size <= maximum:
                    raise BootstrapDeliveryError(_FAILURE)
                raw = await reader.readexactly(size)
                binding, expected = _request(raw, operation)
                _scope(binding, node=self._node, pool=self._pool, release=self._release)
                if (binding.pool_id != peer.pool_id or binding.executor_id != peer.executor_id
                    or binding.executor_incarnation != peer.executor_incarnation):
                    raise BootstrapDeliveryError(_FAILURE)
                async with self._operation_slots:
                    _check_deadline(deadline)
                    if datetime.now(UTC) >= peer.expires_at:
                        raise BootstrapDeliveryError(_FAILURE)
                    result = await (self._operations.receive(raw) if operation == b"D" else self._operations.observe_receipt(raw))
                _check_deadline(deadline)
                if datetime.now(UTC) >= peer.expires_at:
                    raise BootstrapDeliveryError(_FAILURE)
                if result is None:
                    if operation != b"S":
                        raise BootstrapDeliveryError(_FAILURE)
                    reply = b"LNR1U\0\0\0\0"
                else:
                    payload = _canonical(result)
                    if parse_native_delivery_receipt(payload) != expected:
                        raise BootstrapDeliveryError(_FAILURE)
                    reply = b"LNR1R" + len(payload).to_bytes(4, "big") + payload
                _check_deadline(deadline)
                writer.write(reply)
                await writer.drain()
        except asyncio.CancelledError:
            raise
        except BaseException:
            # The connection task must not leak adapter details via its exception
            # handler, even for SystemExit/control exceptions. No automatic retry.
            if writer is not None:
                try:
                    async with asyncio.timeout_at(deadline):
                        writer.write(b"LNR1F\0\0\0\0")
                        await writer.drain()
                except BaseException:
                    pass
        finally:
            if writer is not None:
                writer.transport.abort()
