"""Credential-free local artifact transfer is exact and scoped, not publication."""

import asyncio
import hashlib
import json
import socket
from contextlib import suppress
from importlib import import_module

import pytest


async def test_real_local_transfer_stages_exact_bytes_and_cleans_spool(tmp_path):
    module = import_module("loom_capacity_executor.native_artifact_transfer")
    payload = b"artifact-bytes" * 170000
    archive = tmp_path / "artifact.tar"
    archive.write_bytes(payload)
    workspace = tmp_path / "io"
    workspace.mkdir(mode=0o700)
    sender, receiver = socket.socketpair()
    task = asyncio.create_task(module.send_native_artifact(sender, archive=archive,
        claim_digest="a" * 64, source_binding_sha256="b" * 64, max_artifact_bytes=4 * 1024**2))
    try:
        async with module.receive_native_artifact(receiver, workspace=workspace,
            claim_digest="a" * 64, source_binding_sha256="b" * 64, max_artifact_bytes=4 * 1024**2) as received:
            assert received.archive.read_bytes() == payload
            assert received.artifact.archive_size_bytes == len(payload)
            assert received.artifact.archive_sha256 == hashlib.sha256(payload).hexdigest()
            assert received.archive.stat().st_mode & 0o777 == 0o400
            assert not receiver.get_inheritable() and not sender.get_inheritable()
            assert await task == received.artifact
        assert list(workspace.iterdir()) == []
    finally:
        task.cancel()
        with suppress(asyncio.CancelledError):
            await task
        sender.close()
        receiver.close()


@pytest.mark.parametrize("boundary", ["fragmented", "claim", "source", "digest", "size", "extra", "truncated",
    "header-limit", "noncanonical", "credential", "descriptor", "cancelled"])
async def test_received_stream_rejects_bad_identity_or_bytes_and_removes_partial_files(tmp_path, boundary):
    import array
    import os

    module = import_module("loom_capacity_executor.native_artifact_transfer")
    payload = b"test-artifact"
    header = {"schema_version": 1, "claim_digest": "c" * 64 if boundary == "claim" else "a" * 64,
        "source_binding_sha256": "c" * 64 if boundary == "source" else "b" * 64,
        "artifact": {"schema_version": 1, "archive_sha256": "d" * 64 if boundary == "digest" else hashlib.sha256(payload).hexdigest(),
            "archive_size_bytes": 1025 if boundary == "size" else len(payload)}}
    if boundary == "credential":
        header["worker_credential"] = "x" * 43
    wire = json.dumps(header, sort_keys=True, separators=(",", ":")).encode()
    if boundary == "noncanonical":
        wire += b" "
    prefix = b"LOOMNAT1" + (4097 if boundary == "header-limit" else len(wire)).to_bytes(4, "big")
    data = prefix + wire + (payload[:-1] if boundary == "truncated" else payload)
    if boundary == "extra":
        data += b"!"
    workspace = tmp_path / "io"
    workspace.mkdir(mode=0o700)
    sender, receiver = socket.socketpair()
    sender.setblocking(False)

    async def send():
        if boundary == "descriptor":
            fd = os.open("/dev/null", os.O_RDONLY)
            try:
                sender.sendmsg([data], [(socket.SOL_SOCKET, socket.SCM_RIGHTS, array.array("i", [fd]))])
            finally:
                os.close(fd)
        else:
            fragments = [data[index:index + 7] for index in range(0, len(data), 7)] if boundary == "fragmented" else [data]
            for fragment in fragments:
                await asyncio.get_running_loop().sock_sendall(sender, fragment)
        if boundary != "cancelled":
            sender.shutdown(socket.SHUT_WR)

    async def receive():
        async with module.receive_native_artifact(receiver, workspace=workspace, claim_digest="a" * 64,
            source_binding_sha256="b" * 64, max_artifact_bytes=1024) as received:
            assert boundary == "fragmented"
            assert received.archive.read_bytes() == payload

    try:
        await send()
        task = asyncio.create_task(receive())
        if boundary == "cancelled":
            # Full payload but no EOF: reception must not yield an artifact.
            await asyncio.sleep(0)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
        elif boundary == "fragmented":
            await task
        else:
            with pytest.raises((ValueError, OSError)):
                await task
        assert list(workspace.iterdir()) == []
    finally:
        sender.close()
        receiver.close()
