from __future__ import annotations

import json
import os
import socket
import time
from pathlib import Path
from threading import Thread

from loom_task_image_builder_guard.protocol import (
    LOCAL_SCHEMA,
    read_sealed_memfd,
    send_packet,
)
from tests.unit.test_task_image_builder_guard_service import (
    BOOTSTRAP,
    GRANT,
    _json,
    _receive_projected_secret,
    _service,
)


def test_real_unix_seqpacket_projection_keeps_secrets_descriptor_only(
    tmp_path: Path,
) -> None:
    service, ledger, _peer, _slurm, _events = _service(tmp_path)
    failure: list[BaseException] = []

    def run() -> None:
        try:
            service.start()
        except BaseException as exc:
            failure.append(exc)

    thread = Thread(target=run)
    thread.start()
    deadline = time.monotonic() + 3
    while not service.config.protocol.socket_path.exists() and time.monotonic() < deadline:
        time.sleep(0.01)
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
