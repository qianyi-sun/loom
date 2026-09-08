from __future__ import annotations

import json
import os
import socket
from pathlib import Path
from threading import Event, Thread
from uuid import UUID

from loom_task_image_builder_guard.protocol import (
    LOCAL_SCHEMA,
    create_sealed_memfd,
    read_sealed_memfd,
    receive_request,
    send_packet,
)
from tests.unit.test_task_image_builder_guard_service import (
    ATTEMPT,
    BOOTSTRAP,
    CANDIDATE,
    CREDENTIAL,
    GRANT,
    LEASE_OPERATION,
    MATERIALIZATION,
    _establish_session,
    _json,
    _receive_projected_secret,
    _service,
)


def test_real_unix_seqpacket_projection_keeps_secrets_descriptor_only(
    tmp_path: Path,
) -> None:
    ready = Event()
    service, ledger, _peer, _slurm, _events = _service(tmp_path, ready=ready.set)
    failure: list[BaseException] = []

    def run() -> None:
        try:
            service.start()
        except BaseException as exc:
            failure.append(exc)

    thread = Thread(target=run)
    thread.start()
    assert ready.wait(timeout=3)
    assert service.config.protocol.socket_path.exists()

    with socket.socket(socket.AF_UNIX, socket.SOCK_SEQPACKET) as client:
        client.connect(str(service.config.protocol.socket_path))
        send_packet(
            client,
            _json(
                {
                    "schema": LOCAL_SCHEMA,
                    "operation": "project",
                    "grant_id": str(GRANT),
                }
            ),
        )
        response_payload, descriptor = _receive_projected_secret(client)
        assert descriptor is not None
        try:
            receipt = json.loads(read_sealed_memfd(descriptor, maximum=65536))
        finally:
            os.close(descriptor)
        response = json.loads(response_payload)
        assert receipt["bootstrap_token"] == BOOTSTRAP
        assert BOOTSTRAP.encode("ascii") not in response_payload
        send_packet(
            client,
            _json(
                {
                    "schema": LOCAL_SCHEMA,
                    "operation": "ack",
                    "response_id": response["response_id"],
                }
            ),
        )

    service.stop()
    thread.join(timeout=3)
    service.close()

    assert not thread.is_alive()
    assert failure == []
    assert not service.config.protocol.socket_path.exists()
    assert BOOTSTRAP.encode("ascii") not in ledger.get(GRANT).raw  # type: ignore[union-attr]
    ledger.close()


def test_real_unix_seqpacket_registry_publication_keeps_authority_inert(
    tmp_path: Path,
) -> None:
    ready = Event()
    service, ledger, _peer, _slurm, _events = _service(tmp_path, ready=ready.set)
    current_wire = _establish_session(service, ledger)
    service._uuid = iter(
        (
            UUID("abababab-abab-4bab-8bab-abababababab"),
            UUID("acacacac-acac-4cac-8cac-acacacacacac"),
        )
    ).__next__
    ledger_before = ledger.get(GRANT).raw  # type: ignore[union-attr]
    failure: list[BaseException] = []

    def run() -> None:
        try:
            service.start()
        except BaseException as exc:
            failure.append(exc)

    thread = Thread(target=run)
    thread.start()
    assert ready.wait(timeout=3)
    requests = (
        {
            "schema": LOCAL_SCHEMA,
            "operation": "registry-credential",
            "grant_id": str(GRANT),
            "operation_id": str(LEASE_OPERATION),
            "materialization_id": str(MATERIALIZATION),
            "attempt_id": str(ATTEMPT),
            "lease_epoch": 1,
            "component": "task",
            "predecessor_credential_id": None,
            "predecessor_generation": None,
        },
        {
            "schema": LOCAL_SCHEMA,
            "operation": "publication-candidate",
            "grant_id": str(GRANT),
            "operation_id": str(LEASE_OPERATION),
            "materialization_id": str(MATERIALIZATION),
            "attempt_id": str(ATTEMPT),
            "lease_epoch": 1,
            "credential_id": str(CREDENTIAL),
            "credential_generation": 1,
            "component": "task",
            "manifest_digest": "sha256:" + "1" * 64,
            "manifest_size": 512,
            "oci_file_sha256": "2" * 64,
            "oci_file_size": 4096,
            "platform": "linux/arm64",
        },
    )
    try:
        for request in requests:
            current_fd = create_sealed_memfd("current-session", current_wire, maximum=65536)
            try:
                with socket.socket(socket.AF_UNIX, socket.SOCK_SEQPACKET) as client:
                    client.connect(str(service.config.protocol.socket_path))
                    send_packet(client, _json(request), descriptor=current_fd)
                    response_payload, descriptor = receive_request(
                        client,
                        maximum=4096,
                    )
                    response = json.loads(response_payload)
                    if request["operation"] == "registry-credential":
                        assert descriptor is not None
                        try:
                            secret = read_sealed_memfd(descriptor, maximum=65536)
                        finally:
                            os.close(descriptor)
                        assert b"sentinel-private-registry-token" in secret
                        assert b"sentinel-private-registry-token" not in response_payload
                    else:
                        assert descriptor is None
                        assert response["candidate_id"] == str(CANDIDATE)
                    send_packet(
                        client,
                        _json(
                            {
                                "schema": LOCAL_SCHEMA,
                                "operation": "ack",
                                "response_id": response["response_id"],
                            }
                        ),
                    )
            finally:
                os.close(current_fd)
    finally:
        service.stop()
        thread.join(timeout=3)
        service.close()

    assert not thread.is_alive()
    assert failure == []
    assert ledger.get(GRANT).raw == ledger_before  # type: ignore[union-attr]
    assert b"sentinel-private-registry-token" not in ledger_before
    ledger.close()
