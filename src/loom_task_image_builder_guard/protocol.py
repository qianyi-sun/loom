"""Bounded local seqpacket and sealed-descriptor protocol."""

from __future__ import annotations

import array
import ctypes
import fcntl
import json
import os
import socket
import stat
import struct
from dataclasses import dataclass
from typing import Literal
from uuid import UUID

from loom_task_image_builder_guard.errors import GuardError

LOCAL_SCHEMA = "loom.task-image-builder-guard-local/v1"
ADD_MEMFD_SEALS = 1033
GET_MEMFD_SEALS = 1034
REQUIRED_MEMFD_SEALS = 0x0001 | 0x0002 | 0x0004 | 0x0008
_MFD_CLOEXEC = 0x0001
_MFD_ALLOW_SEALING = 0x0002
_DIGEST_LENGTH = 64
_MAX_JSON_FIELDS = 5
_PEER_CREDENTIALS = struct.Struct("3i")


@dataclass(frozen=True, slots=True)
class LocalRequest:
    operation: Literal["project", "exchange", "ack"]
    grant_id: UUID | None = None
    exchange_id: UUID | None = None
    proof_sha256: str | None = None
    response_id: UUID | None = None


@dataclass(frozen=True, slots=True)
class PeerCredentials:
    pid: int
    uid: int
    gid: int

    def __post_init__(self) -> None:
        if (
            type(self.pid) is not int
            or self.pid <= 0
            or type(self.uid) is not int
            or self.uid < 0
            or type(self.gid) is not int
            or self.gid < 0
        ):
            raise GuardError("local_peer_credentials_invalid")


def _pairs(pairs: list[tuple[str, object]]) -> dict[str, object]:
    if len(pairs) > _MAX_JSON_FIELDS:
        raise ValueError("too many fields")
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate key")
        result[key] = value
    return result


def _uuid(value: object) -> UUID:
    if not isinstance(value, str):
        raise ValueError("invalid UUID")
    parsed = UUID(value)
    if parsed.int == 0 or str(parsed) != value:
        raise ValueError("invalid UUID")
    return parsed


def _digest(value: object) -> str:
    if (
        not isinstance(value, str)
        or len(value) != _DIGEST_LENGTH
        or value == "0" * _DIGEST_LENGTH
        or any(character not in "0123456789abcdef" for character in value)
    ):
        raise ValueError("invalid digest")
    return value


def parse_local_request(payload: bytes) -> LocalRequest:
    """Parse one exact nonsecret local request without reflecting its body."""

    try:
        if not payload or len(payload) > 64 * 1024:
            raise ValueError("invalid size")
        document = json.loads(payload, object_pairs_hook=_pairs)
        if not isinstance(document, dict) or document.get("schema") != LOCAL_SCHEMA:
            raise ValueError("invalid schema")
        operation = document.get("operation")
        if operation == "project" and set(document) == {"schema", "operation", "grant_id"}:
            return LocalRequest(operation="project", grant_id=_uuid(document["grant_id"]))
        if operation == "exchange" and set(document) == {
            "schema",
            "operation",
            "grant_id",
            "exchange_id",
            "proof_sha256",
        }:
            return LocalRequest(
                operation="exchange",
                grant_id=_uuid(document["grant_id"]),
                exchange_id=_uuid(document["exchange_id"]),
                proof_sha256=_digest(document["proof_sha256"]),
            )
        if operation == "ack" and set(document) == {"schema", "operation", "response_id"}:
            return LocalRequest(operation="ack", response_id=_uuid(document["response_id"]))
    except (
        UnicodeDecodeError,
        json.JSONDecodeError,
        RecursionError,
        TypeError,
        ValueError,
    ):
        pass
    raise GuardError("local_request_invalid")


def _validate_sealed_descriptor(descriptor: int) -> os.stat_result:
    try:
        metadata = os.fstat(descriptor)
        seals = fcntl.fcntl(descriptor, GET_MEMFD_SEALS)
    except OSError as exc:
        raise GuardError("memfd_invalid") from exc
    if not stat.S_ISREG(metadata.st_mode) or metadata.st_nlink != 0:
        raise GuardError("memfd_invalid")
    if seals != REQUIRED_MEMFD_SEALS:
        raise GuardError("memfd_seals_invalid")
    return metadata


def _memfd_create(name: str, flags: int) -> int:
    native = getattr(os, "memfd_create", None)
    if native is not None:
        return int(native(name, flags))
    libc = ctypes.CDLL(None, use_errno=True)
    create = libc.memfd_create
    create.argtypes = [ctypes.c_char_p, ctypes.c_uint]
    create.restype = ctypes.c_int
    descriptor = int(create(name.encode("ascii"), flags))
    if descriptor < 0:
        error = ctypes.get_errno()
        raise OSError(error, os.strerror(error))
    return descriptor


def create_sealed_memfd(name: str, payload: bytes, *, maximum: int) -> int:
    """Create one anonymous immutable descriptor containing a bounded payload."""

    if (
        not isinstance(name, str)
        or not name
        or len(name) > 128
        or not isinstance(payload, bytes)
        or not payload
        or type(maximum) is not int
        or maximum <= 0
        or len(payload) > maximum
    ):
        raise GuardError("memfd_payload_invalid")
    descriptor: int | None = None
    complete = False
    try:
        descriptor = _memfd_create(name, _MFD_CLOEXEC | _MFD_ALLOW_SEALING)
        view = memoryview(payload)
        while view:
            written = os.write(descriptor, view)
            if written <= 0:
                raise GuardError("memfd_write_failed")
            view = view[written:]
        os.fsync(descriptor)
        fcntl.fcntl(descriptor, ADD_MEMFD_SEALS, REQUIRED_MEMFD_SEALS)
        metadata = _validate_sealed_descriptor(descriptor)
        if metadata.st_size != len(payload):
            raise GuardError("memfd_payload_invalid")
        complete = True
        return descriptor
    except GuardError:
        raise
    except OSError as exc:
        raise GuardError("memfd_write_failed") from exc
    finally:
        if descriptor is not None and not complete:
            os.close(descriptor)


def read_sealed_memfd(descriptor: int, *, maximum: int) -> bytes:
    """Read an immutable secret-bearing payload without changing its file offset."""

    if type(descriptor) is not int or descriptor < 0 or type(maximum) is not int or maximum <= 0:
        raise GuardError("memfd_invalid")
    metadata = _validate_sealed_descriptor(descriptor)
    if metadata.st_size <= 0 or metadata.st_size > maximum:
        raise GuardError("memfd_payload_invalid")
    try:
        payload = os.pread(descriptor, metadata.st_size + 1, 0)
        final = os.fstat(descriptor)
    except OSError as exc:
        raise GuardError("memfd_read_failed") from exc
    if (
        len(payload) != metadata.st_size
        or (final.st_dev, final.st_ino, final.st_size, final.st_mode, final.st_nlink)
        != (
            metadata.st_dev,
            metadata.st_ino,
            metadata.st_size,
            metadata.st_mode,
            metadata.st_nlink,
        )
        or fcntl.fcntl(descriptor, GET_MEMFD_SEALS) != REQUIRED_MEMFD_SEALS
    ):
        raise GuardError("memfd_changed")
    return payload


def _require_seqpacket(connection: socket.socket) -> None:
    if (
        connection.family != socket.AF_UNIX
        or connection.getsockopt(socket.SOL_SOCKET, socket.SO_TYPE) != socket.SOCK_SEQPACKET
    ):
        raise GuardError("local_socket_invalid")


def send_packet(
    connection: socket.socket,
    payload: bytes,
    *,
    descriptor: int | None = None,
) -> None:
    """Send one complete packet with zero or one already sealed memfd."""

    _require_seqpacket(connection)
    if not isinstance(payload, bytes) or not payload:
        raise GuardError("local_packet_invalid")
    ancillary: list[tuple[int, int, bytes | array.array[int]]] = [
        (
            socket.SOL_SOCKET,
            socket.SCM_CREDENTIALS,
            _PEER_CREDENTIALS.pack(os.getpid(), os.geteuid(), os.getegid()),
        )
    ]
    if descriptor is not None:
        _validate_sealed_descriptor(descriptor)
        ancillary.append((socket.SOL_SOCKET, socket.SCM_RIGHTS, array.array("i", [descriptor])))
    try:
        written = connection.sendmsg([payload], ancillary)
    except OSError as exc:
        raise GuardError("local_send_failed") from exc
    if written != len(payload):
        raise GuardError("local_send_failed")


def _receive_request(
    connection: socket.socket,
    *,
    maximum: int,
) -> tuple[bytes, int | None, PeerCredentials | None]:
    _require_seqpacket(connection)
    if type(maximum) is not int or maximum <= 0 or maximum > 64 * 1024:
        raise GuardError("local_packet_invalid")
    item_size = array.array("i").itemsize
    received: list[int] = []
    credentials: PeerCredentials | None = None
    try:
        payload, ancillary, flags, _address = connection.recvmsg(
            maximum,
            socket.CMSG_SPACE(item_size * 2)
            + socket.CMSG_SPACE(_PEER_CREDENTIALS.size),
            socket.MSG_CMSG_CLOEXEC,
        )
        for level, kind, data in ancillary:
            if level != socket.SOL_SOCKET:
                raise GuardError("local_descriptor_invalid")
            if kind == socket.SCM_RIGHTS:
                values = array.array("i")
                values.frombytes(data[: len(data) - (len(data) % item_size)])
                received.extend(values.tolist())
            elif kind == socket.SCM_CREDENTIALS:
                if credentials is not None or len(data) != _PEER_CREDENTIALS.size:
                    raise GuardError("local_peer_credentials_invalid")
                credentials = PeerCredentials(*_PEER_CREDENTIALS.unpack(data))
            else:
                raise GuardError("local_descriptor_invalid")
        for received_fd in received:
            descriptor_flags = fcntl.fcntl(received_fd, fcntl.F_GETFD)
            if not descriptor_flags & fcntl.FD_CLOEXEC:
                raise GuardError("local_descriptor_invalid")
        if flags & (socket.MSG_TRUNC | socket.MSG_CTRUNC) or not payload:
            raise GuardError("local_packet_invalid")
        if len(received) > 1:
            raise GuardError("local_descriptor_invalid")
        result_fd = received.pop() if received else None
        return payload, result_fd, credentials
    except GuardError:
        raise
    except OSError as exc:
        raise GuardError("local_receive_failed") from exc
    finally:
        for received_fd in received:
            try:
                os.close(received_fd)
            except OSError:
                pass


def _close_received_rights(ancillary: tuple[tuple[int, int, bytes], ...]) -> None:
    item_size = array.array("i").itemsize
    for level, kind, data in ancillary:
        if level != socket.SOL_SOCKET or kind != socket.SCM_RIGHTS:
            continue
        values = array.array("i")
        values.frombytes(data[: len(data) - (len(data) % item_size)])
        for descriptor in values:
            try:
                os.close(descriptor)
            except OSError:
                pass


@dataclass(slots=True)
class AuthenticatedPacket:
    """Own a raw credentialed packet until peer pidfd capture is complete."""

    payload: bytes
    ancillary: tuple[tuple[int, int, bytes], ...]
    flags: int
    credentials: PeerCredentials
    _finished: bool = False

    def finish(self) -> tuple[bytes, int | None]:
        if self._finished:
            raise GuardError("local_packet_invalid")
        self._finished = True
        item_size = array.array("i").itemsize
        received: list[int] = []
        observed_credentials: PeerCredentials | None = None
        try:
            for level, kind, data in self.ancillary:
                if level != socket.SOL_SOCKET:
                    raise GuardError("local_descriptor_invalid")
                if kind == socket.SCM_RIGHTS:
                    values = array.array("i")
                    values.frombytes(data[: len(data) - (len(data) % item_size)])
                    received.extend(values.tolist())
                elif kind == socket.SCM_CREDENTIALS:
                    if (
                        observed_credentials is not None
                        or len(data) != _PEER_CREDENTIALS.size
                    ):
                        raise GuardError("local_peer_credentials_invalid")
                    observed_credentials = PeerCredentials(
                        *_PEER_CREDENTIALS.unpack(data)
                    )
                else:
                    raise GuardError("local_descriptor_invalid")
            for descriptor in received:
                descriptor_flags = fcntl.fcntl(descriptor, fcntl.F_GETFD)
                if not descriptor_flags & fcntl.FD_CLOEXEC:
                    raise GuardError("local_descriptor_invalid")
            if (
                self.flags & (socket.MSG_TRUNC | socket.MSG_CTRUNC)
                or not self.payload
            ):
                raise GuardError("local_packet_invalid")
            if len(received) > 1:
                raise GuardError("local_descriptor_invalid")
            if observed_credentials != self.credentials:
                raise GuardError("local_peer_credentials_invalid")
            result_fd = received.pop() if received else None
            return self.payload, result_fd
        except GuardError:
            raise
        except OSError as exc:
            raise GuardError("local_receive_failed") from exc
        finally:
            for descriptor in received:
                try:
                    os.close(descriptor)
                except OSError:
                    pass

    def close(self) -> None:
        if self._finished:
            return
        self._finished = True
        _close_received_rights(self.ancillary)


def receive_authenticated_packet(
    connection: socket.socket,
    *,
    maximum: int,
) -> AuthenticatedPacket:
    """Receive raw packet authority while deferring descriptor validation."""

    _require_seqpacket(connection)
    if type(maximum) is not int or maximum <= 0 or maximum > 64 * 1024:
        raise GuardError("local_packet_invalid")
    try:
        connection.setsockopt(socket.SOL_SOCKET, socket.SO_PASSCRED, 1)
    except OSError as exc:
        raise GuardError("local_peer_credentials_invalid") from exc
    try:
        item_size = array.array("i").itemsize
        payload, raw_ancillary, flags, _address = connection.recvmsg(
            maximum,
            socket.CMSG_SPACE(item_size * 2)
            + socket.CMSG_SPACE(_PEER_CREDENTIALS.size),
            socket.MSG_CMSG_CLOEXEC,
        )
    except OSError as exc:
        raise GuardError("local_receive_failed") from exc
    ancillary = tuple(raw_ancillary)
    credentials: PeerCredentials | None = None
    try:
        for level, kind, data in ancillary:
            if level == socket.SOL_SOCKET and kind == socket.SCM_CREDENTIALS:
                if credentials is not None or len(data) != _PEER_CREDENTIALS.size:
                    raise GuardError("local_peer_credentials_invalid")
                credentials = PeerCredentials(*_PEER_CREDENTIALS.unpack(data))
        if credentials is None:
            raise GuardError("local_peer_credentials_invalid")
        return AuthenticatedPacket(payload, ancillary, flags, credentials)
    except GuardError:
        _close_received_rights(ancillary)
        raise


def receive_request(connection: socket.socket, *, maximum: int) -> tuple[bytes, int | None]:
    """Receive one complete packet and take ownership of at most one FD."""

    payload, descriptor, _credentials = _receive_request(connection, maximum=maximum)
    return payload, descriptor


def receive_authenticated_request(
    connection: socket.socket,
    *,
    maximum: int,
) -> tuple[bytes, int | None, PeerCredentials]:
    """Receive one packet and bind it to the kernel's per-message credentials."""

    packet = receive_authenticated_packet(connection, maximum=maximum)
    try:
        payload, descriptor = packet.finish()
        return payload, descriptor, packet.credentials
    finally:
        packet.close()


def require_ack(
    connection: socket.socket,
    *,
    response_id: UUID,
    timeout_seconds: int,
    maximum: int,
    expected_credentials: PeerCredentials,
) -> None:
    """Require a descriptor-free acknowledgement for one exact response."""

    prior_timeout = connection.gettimeout()
    try:
        connection.settimeout(timeout_seconds)
        payload, descriptor, credentials = receive_authenticated_request(
            connection,
            maximum=maximum,
        )
        if descriptor is not None:
            os.close(descriptor)
            raise GuardError("local_ack_invalid")
        if credentials != expected_credentials:
            raise GuardError("local_peer_credentials_invalid")
        request = parse_local_request(payload)
        if request.operation != "ack" or request.response_id != response_id:
            raise GuardError("local_ack_invalid")
    except TimeoutError as exc:
        raise GuardError("local_ack_timeout") from exc
    finally:
        connection.settimeout(prior_timeout)


__all__ = [
    "GET_MEMFD_SEALS",
    "LOCAL_SCHEMA",
    "REQUIRED_MEMFD_SEALS",
    "AuthenticatedPacket",
    "LocalRequest",
    "PeerCredentials",
    "create_sealed_memfd",
    "parse_local_request",
    "read_sealed_memfd",
    "receive_authenticated_packet",
    "receive_authenticated_request",
    "receive_request",
    "require_ack",
    "send_packet",
]
