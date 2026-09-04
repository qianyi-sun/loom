from __future__ import annotations

import array
import ctypes
import errno
import json
import os
import socket
import stat
import struct
from pathlib import Path
from types import SimpleNamespace
from uuid import UUID

import pytest

from loom_task_image_builder_guard import protocol as protocol_module
from loom_task_image_builder_guard.errors import GuardError
from loom_task_image_builder_guard.protocol import (
    GET_MEMFD_SEALS,
    REQUIRED_MEMFD_SEALS,
    LocalRequest,
    PeerCredentials,
    create_sealed_memfd,
    parse_local_request,
    read_sealed_memfd,
    receive_authenticated_packet,
    receive_authenticated_request,
    receive_request,
    require_ack,
    send_packet,
)

GRANT = UUID("11111111-1111-4111-8111-111111111111")
EXCHANGE = UUID("22222222-2222-4222-8222-222222222222")
RESPONSE = UUID("33333333-3333-4333-8333-333333333333")
MATERIALIZATION = UUID("44444444-4444-4444-8444-444444444444")
ATTEMPT = UUID("55555555-5555-4555-8555-555555555555")
OPERATION = UUID("66666666-6666-4666-8666-666666666666")
DIGEST = "a" * 64
CURRENT_CREDENTIALS = PeerCredentials(os.getpid(), os.geteuid(), os.getegid())


def _wire(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode("ascii")


def _unsealed_memfd(name: str) -> int:
    libc = ctypes.CDLL(None, use_errno=True)
    create = libc.memfd_create
    create.argtypes = [ctypes.c_char_p, ctypes.c_uint]
    create.restype = ctypes.c_int
    descriptor = int(create(name.encode("ascii"), 0x0001))
    if descriptor < 0:
        raise OSError(ctypes.get_errno(), "memfd_create")
    return descriptor


def test_project_request_contains_only_nonsecret_grant_authority() -> None:
    request = parse_local_request(
        _wire(
            {
                "schema": "loom.task-image-builder-guard-local/v1",
                "operation": "project",
                "grant_id": str(GRANT),
            }
        )
    )

    assert request == LocalRequest(operation="project", grant_id=GRANT)


def test_exchange_request_binds_exact_replay_fields() -> None:
    request = parse_local_request(
        _wire(
            {
                "schema": "loom.task-image-builder-guard-local/v1",
                "operation": "exchange",
                "grant_id": str(GRANT),
                "exchange_id": str(EXCHANGE),
                "proof_sha256": DIGEST,
            }
        )
    )

    assert request.exchange_id == EXCHANGE
    assert request.proof_sha256 == DIGEST


@pytest.mark.parametrize(
    ("document", "expected"),
    [
        (
            {
                "schema": "loom.task-image-builder-guard-local/v1",
                "operation": "renew",
                "grant_id": str(GRANT),
                "operation_id": str(OPERATION),
            },
            LocalRequest(operation="renew", grant_id=GRANT, operation_id=OPERATION),
        ),
        (
            {
                "schema": "loom.task-image-builder-guard-local/v1",
                "operation": "claim",
                "grant_id": str(GRANT),
                "operation_id": str(OPERATION),
            },
            LocalRequest(operation="claim", grant_id=GRANT, operation_id=OPERATION),
        ),
        *(
            (
                {
                    "schema": "loom.task-image-builder-guard-local/v1",
                    "operation": operation,
                    "grant_id": str(GRANT),
                    "operation_id": str(OPERATION),
                    "materialization_id": str(MATERIALIZATION),
                    "attempt_id": str(ATTEMPT),
                    "lease_epoch": 3,
                },
                LocalRequest(
                    operation=operation,
                    grant_id=GRANT,
                    operation_id=OPERATION,
                    materialization_id=MATERIALIZATION,
                    attempt_id=ATTEMPT,
                    lease_epoch=3,
                ),
            )
            for operation in ("start", "heartbeat", "bundle", "release")
        ),
        (
            {
                "schema": "loom.task-image-builder-guard-local/v1",
                "operation": "fail",
                "grant_id": str(GRANT),
                "operation_id": str(OPERATION),
                "materialization_id": str(MATERIALIZATION),
                "attempt_id": str(ATTEMPT),
                "lease_epoch": 3,
                "failure_kind": "containment",
            },
            LocalRequest(
                operation="fail",
                grant_id=GRANT,
                operation_id=OPERATION,
                materialization_id=MATERIALIZATION,
                attempt_id=ATTEMPT,
                lease_epoch=3,
                failure_kind="containment",
            ),
        ),
        (
            {
                "schema": "loom.task-image-builder-guard-local/v1",
                "operation": "finish",
                "grant_id": str(GRANT),
                "operation_id": str(OPERATION),
                "cleanup": {
                    "descendant_processes": 0,
                    "mounts": 0,
                    "sockets": 0,
                    "open_files": 0,
                },
            },
            LocalRequest(
                operation="finish",
                grant_id=GRANT,
                operation_id=OPERATION,
                cleanup={
                    "descendant_processes": 0,
                    "mounts": 0,
                    "sockets": 0,
                    "open_files": 0,
                },
            ),
        ),
    ],
)
def test_session_and_lease_operations_have_exact_nonsecret_fields(
    document: dict[str, object],
    expected: LocalRequest,
) -> None:
    assert parse_local_request(_wire(document)) == expected


@pytest.mark.parametrize(
    "operation",
    ("renew", "claim", "start", "heartbeat", "bundle", "release", "fail", "finish"),
)
def test_local_operations_reject_secret_or_caller_selected_authority(operation: str) -> None:
    document: dict[str, object] = {
        "schema": "loom.task-image-builder-guard-local/v1",
        "operation": operation,
        "grant_id": str(GRANT),
        "operation_id": str(OPERATION),
        "session_token": "loom_tibs_" + "S" * 64,
    }

    with pytest.raises(GuardError) as caught:
        parse_local_request(_wire(document))

    assert caught.value.code == "local_request_invalid"
    assert "loom_tibs_" not in str(caught.value)


@pytest.mark.parametrize(
    "document",
    [
        {
            "schema": "loom.task-image-builder-guard-local/v1",
            "operation": "project",
            "grant_id": str(GRANT),
            "bootstrap_token": "loom_tibp_do-not-serialize-secrets",
        },
        {
            "schema": "loom.task-image-builder-guard-local/v1",
            "operation": "exchange",
            "grant_id": str(GRANT),
            "exchange_id": str(EXCHANGE),
        },
        {
            "schema": "loom.task-image-builder-guard-local/v1",
            "operation": "project",
            "grant_id": "00000000-0000-0000-0000-000000000000",
        },
    ],
)
def test_rejects_broadened_or_incomplete_local_request(document: object) -> None:
    with pytest.raises(GuardError) as caught:
        parse_local_request(_wire(document))

    assert caught.value.code == "local_request_invalid"
    assert "loom_tibp_" not in str(caught.value)


def test_rejects_deeply_nested_local_request_with_a_typed_error() -> None:
    payload = b"[" * 2000 + b"0" + b"]" * 2000

    with pytest.raises(GuardError) as caught:
        parse_local_request(payload)

    assert caught.value.code == "local_request_invalid"


def test_sealed_memfd_has_every_required_seal_and_round_trips() -> None:
    descriptor = create_sealed_memfd("loom-guard-projection", b'{"token":"redacted"}', maximum=64)
    try:
        import fcntl

        assert fcntl.fcntl(descriptor, GET_MEMFD_SEALS) == REQUIRED_MEMFD_SEALS
        assert read_sealed_memfd(descriptor, maximum=64) == b'{"token":"redacted"}'
        with pytest.raises(OSError):
            os.write(descriptor, b"changed")
    finally:
        os.close(descriptor)


def test_rejects_unsealed_or_oversized_descriptor() -> None:
    unsealed = _unsealed_memfd("unsealed")
    os.write(unsealed, b"secret")
    try:
        with pytest.raises(GuardError) as caught:
            read_sealed_memfd(unsealed, maximum=64)
        assert caught.value.code == "memfd_seals_invalid"
    finally:
        os.close(unsealed)

    with pytest.raises(GuardError) as caught:
        create_sealed_memfd("too-large", b"12345", maximum=4)
    assert caught.value.code == "memfd_payload_invalid"


def test_memfd_creation_closes_descriptor_when_final_validation_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    created: list[int] = []
    real_create = protocol_module._memfd_create

    def capture(name: str, flags: int) -> int:
        descriptor = real_create(name, flags)
        created.append(descriptor)
        return descriptor

    monkeypatch.setattr(protocol_module, "_memfd_create", capture)
    monkeypatch.setattr(
        protocol_module,
        "_validate_sealed_descriptor",
        lambda descriptor: SimpleNamespace(st_size=1),
    )

    with pytest.raises(GuardError) as caught:
        create_sealed_memfd("bad-final-size", b"payload", maximum=64)

    assert caught.value.code == "memfd_payload_invalid"
    assert len(created) == 1
    with pytest.raises(OSError) as closed:
        os.fstat(created[0])
    assert closed.value.errno == errno.EBADF


def test_packet_transfers_one_owned_sealed_descriptor() -> None:
    sender, receiver = socket.socketpair(socket.AF_UNIX, socket.SOCK_SEQPACKET)
    descriptor = create_sealed_memfd("exchange", b"payload", maximum=64)
    try:
        send_packet(sender, b"request", descriptor=descriptor)
        packet, received = receive_request(receiver, maximum=64)
        try:
            assert packet == b"request"
            assert received is not None
            assert received != descriptor
            assert read_sealed_memfd(received, maximum=64) == b"payload"
        finally:
            if received is not None:
                os.close(received)
    finally:
        os.close(descriptor)
        sender.close()
        receiver.close()


def test_projected_packet_transfers_exactly_three_ordered_capabilities(
    tmp_path: Path,
) -> None:
    sender, receiver = socket.socketpair(socket.AF_UNIX, socket.SOCK_SEQPACKET)
    bootstrap = create_sealed_memfd("bootstrap", b"secret", maximum=64)
    workspace = os.open(tmp_path, os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC)
    cgroup_path = tmp_path / "build-egress"
    cgroup_path.mkdir()
    cgroup = os.open(cgroup_path, os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC)
    received: list[int] = []
    try:
        send_packet(
            sender,
            b"projected",
            descriptors=(bootstrap, workspace, cgroup),
        )
        payload, ancillary, flags, _address = receiver.recvmsg(
            64,
            socket.CMSG_SPACE(array.array("i").itemsize * 3)
            + socket.CMSG_SPACE(struct.calcsize("3i")),
            socket.MSG_CMSG_CLOEXEC,
        )
        assert payload == b"projected"
        assert flags & (socket.MSG_TRUNC | socket.MSG_CTRUNC) == 0
        for level, kind, data in ancillary:
            if level == socket.SOL_SOCKET and kind == socket.SCM_RIGHTS:
                rights = array.array("i")
                rights.frombytes(data[: len(data) - len(data) % rights.itemsize])
                received.extend(rights)
        assert len(received) == 3
        assert read_sealed_memfd(received[0], maximum=64) == b"secret"
        assert stat.S_ISDIR(os.fstat(received[1]).st_mode)
        assert stat.S_ISDIR(os.fstat(received[2]).st_mode)
        assert (os.fstat(received[1]).st_dev, os.fstat(received[1]).st_ino) == (
            os.fstat(workspace).st_dev,
            os.fstat(workspace).st_ino,
        )
        assert (os.fstat(received[2]).st_dev, os.fstat(received[2]).st_ino) == (
            os.fstat(cgroup).st_dev,
            os.fstat(cgroup).st_ino,
        )
    finally:
        for descriptor in [*received, bootstrap, workspace, cgroup]:
            os.close(descriptor)
        sender.close()
        receiver.close()


def test_projected_packet_rejects_changed_descriptor_order(tmp_path: Path) -> None:
    sender, receiver = socket.socketpair(socket.AF_UNIX, socket.SOCK_SEQPACKET)
    bootstrap = create_sealed_memfd("bootstrap", b"secret", maximum=64)
    directory = os.open(tmp_path, os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC)
    try:
        with pytest.raises(GuardError) as caught:
            send_packet(sender, b"projected", descriptors=(directory, bootstrap, directory))
        assert caught.value.code == "local_descriptor_invalid"
    finally:
        os.close(bootstrap)
        os.close(directory)
        sender.close()
        receiver.close()


def test_authenticated_receive_identifies_the_process_using_an_inherited_socket() -> None:
    sender, receiver = socket.socketpair(socket.AF_UNIX, socket.SOCK_SEQPACKET)
    receiver.setsockopt(socket.SOL_SOCKET, socket.SO_PASSCRED, 1)
    connected_pid = struct.unpack(
        "3i",
        receiver.getsockopt(socket.SOL_SOCKET, socket.SO_PEERCRED, 12),
    )[0]
    child = os.fork()
    if child == 0:
        receiver.close()
        try:
            sender.send(b"delegated-request")
        finally:
            sender.close()
        os._exit(0)
    sender.close()
    try:
        packet, descriptor, credentials = receive_authenticated_request(
            receiver,
            maximum=64,
        )
    finally:
        receiver.close()
        _waited, status = os.waitpid(child, 0)

    assert os.waitstatus_to_exitcode(status) == 0
    assert packet == b"delegated-request"
    assert descriptor is None
    assert connected_pid == os.getpid()
    assert credentials == PeerCredentials(child, os.geteuid(), os.getegid())


def test_unfinished_authenticated_packet_closes_every_received_descriptor() -> None:
    sender, receiver = socket.socketpair(socket.AF_UNIX, socket.SOCK_SEQPACKET)
    descriptor = create_sealed_memfd("unfinished", b"secret", maximum=64)
    received: list[int] = []
    try:
        send_packet(sender, b"request", descriptor=descriptor)
        packet = receive_authenticated_packet(receiver, maximum=64)
        item_size = array.array("i").itemsize
        for level, kind, data in packet.ancillary:
            if level == socket.SOL_SOCKET and kind == socket.SCM_RIGHTS:
                values = array.array("i")
                values.frombytes(data[: len(data) - (len(data) % item_size)])
                received.extend(values)

        packet.close()

        assert len(received) == 1
        with pytest.raises(OSError) as closed:
            os.fstat(received[0])
        assert closed.value.errno == errno.EBADF
    finally:
        os.close(descriptor)
        sender.close()
        receiver.close()


def test_receive_rejects_multiple_descriptors_and_closes_them() -> None:
    sender, receiver = socket.socketpair(socket.AF_UNIX, socket.SOCK_SEQPACKET)
    first = create_sealed_memfd("first", b"a", maximum=8)
    second = create_sealed_memfd("second", b"b", maximum=8)
    try:
        rights = array.array("i", [first, second])
        sender.sendmsg([b"bad"], [(socket.SOL_SOCKET, socket.SCM_RIGHTS, rights)])
        with pytest.raises(GuardError) as caught:
            receive_request(receiver, maximum=64)
        assert caught.value.code == "local_descriptor_invalid"
    finally:
        os.close(first)
        os.close(second)
        sender.close()
        receiver.close()


def test_receive_rejects_truncated_seqpacket() -> None:
    sender, receiver = socket.socketpair(socket.AF_UNIX, socket.SOCK_SEQPACKET)
    try:
        sender.send(b"x" * 65)
        with pytest.raises(GuardError) as caught:
            receive_request(receiver, maximum=64)
        assert caught.value.code == "local_packet_invalid"
    finally:
        sender.close()
        receiver.close()


def test_ack_must_bind_the_exact_response_uuid() -> None:
    sender, receiver = socket.socketpair(socket.AF_UNIX, socket.SOCK_SEQPACKET)
    receiver.setsockopt(socket.SOL_SOCKET, socket.SO_PASSCRED, 1)
    try:
        sender.send(
            _wire(
                {
                    "schema": "loom.task-image-builder-guard-local/v1",
                    "operation": "ack",
                    "response_id": str(RESPONSE),
                }
            )
        )
        require_ack(
            receiver,
            response_id=RESPONSE,
            timeout_seconds=1,
            maximum=1024,
            expected_credentials=CURRENT_CREDENTIALS,
        )

        sender.send(
            _wire(
                {
                    "schema": "loom.task-image-builder-guard-local/v1",
                    "operation": "ack",
                    "response_id": str(EXCHANGE),
                }
            )
        )
        with pytest.raises(GuardError) as caught:
            require_ack(
                receiver,
                response_id=RESPONSE,
                timeout_seconds=1,
                maximum=1024,
                expected_credentials=CURRENT_CREDENTIALS,
            )
        assert caught.value.code == "local_ack_invalid"
    finally:
        sender.close()
        receiver.close()


def test_ack_rejects_a_process_using_an_inherited_socket() -> None:
    sender, receiver = socket.socketpair(socket.AF_UNIX, socket.SOCK_SEQPACKET)
    receiver.setsockopt(socket.SOL_SOCKET, socket.SO_PASSCRED, 1)
    child = os.fork()
    if child == 0:
        receiver.close()
        try:
            sender.send(
                _wire(
                    {
                        "schema": "loom.task-image-builder-guard-local/v1",
                        "operation": "ack",
                        "response_id": str(RESPONSE),
                    }
                )
            )
        finally:
            sender.close()
        os._exit(0)
    sender.close()
    try:
        with pytest.raises(GuardError) as caught:
            require_ack(
                receiver,
                response_id=RESPONSE,
                timeout_seconds=1,
                maximum=1024,
                expected_credentials=PeerCredentials(
                    os.getpid(),
                    os.geteuid(),
                    os.getegid(),
                ),
            )
    finally:
        receiver.close()
        _waited, status = os.waitpid(child, 0)

    assert os.waitstatus_to_exitcode(status) == 0
    assert caught.value.code == "local_peer_credentials_invalid"
