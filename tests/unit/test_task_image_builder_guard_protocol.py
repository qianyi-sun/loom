from __future__ import annotations

import array
import ctypes
import errno
import json
import os
import socket
from types import SimpleNamespace
from uuid import UUID

import pytest

from loom_task_image_builder_guard import protocol as protocol_module
from loom_task_image_builder_guard.errors import GuardError
from loom_task_image_builder_guard.protocol import (
    GET_MEMFD_SEALS,
    REQUIRED_MEMFD_SEALS,
    LocalRequest,
    create_sealed_memfd,
    parse_local_request,
    read_sealed_memfd,
    receive_request,
    require_ack,
    send_packet,
)

GRANT = UUID("11111111-1111-4111-8111-111111111111")
EXCHANGE = UUID("22222222-2222-4222-8222-222222222222")
RESPONSE = UUID("33333333-3333-4333-8333-333333333333")
DIGEST = "a" * 64


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
        require_ack(receiver, response_id=RESPONSE, timeout_seconds=1, maximum=1024)

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
            require_ack(receiver, response_id=RESPONSE, timeout_seconds=1, maximum=1024)
        assert caught.value.code == "local_ack_invalid"
    finally:
        sender.close()
        receiver.close()
