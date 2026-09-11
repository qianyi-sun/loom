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


@pytest.mark.parametrize("boundary", ["symlink", "fifo", "oversize", "empty", "limit-bool", "timeout-bool", "wrong-channel"])
async def test_export_rejects_invalid_archive_or_configuration_before_sending(tmp_path, boundary):
    import os

    module = import_module("loom_capacity_executor.native_artifact_transfer")
    path = tmp_path / "artifact"
    path.write_bytes(b"artifact" if boundary != "empty" else b"")
    if boundary == "symlink":
        link = tmp_path / "link"
        link.symlink_to(path)
        path = link
    elif boundary == "fifo":
        path = tmp_path / "fifo"
        os.mkfifo(path)
    sender, receiver = socket.socketpair(type=socket.SOCK_SEQPACKET if boundary == "wrong-channel" else socket.SOCK_STREAM)
    receiver.setblocking(False)
    try:
        with pytest.raises((OSError, ValueError)):
            await module.send_native_artifact(sender, archive=path, claim_digest="a" * 64,
                source_binding_sha256="b" * 64, max_artifact_bytes=True if boundary == "limit-bool" else 1 if boundary == "oversize" else 1024,
                timeout_seconds=True if boundary == "timeout-bool" else 10)
        with pytest.raises(BlockingIOError):
            receiver.recv(1)
    finally:
        sender.close()
        receiver.close()


async def test_receive_total_timeout_removes_partial_spool(tmp_path):
    module = import_module("loom_capacity_executor.native_artifact_transfer")
    workspace = tmp_path / "io"
    workspace.mkdir(mode=0o700)
    sender, receiver = socket.socketpair()
    try:
        sender.send(b"LOOM")  # Stalled before a complete header.
        with pytest.raises(TimeoutError):
            async with module.receive_native_artifact(receiver, workspace=workspace,
                claim_digest="a" * 64, source_binding_sha256="b" * 64,
                max_artifact_bytes=1024, timeout_seconds=1):
                pytest.fail("incomplete stream yielded an artifact")
        assert list(workspace.iterdir()) == []
    finally:
        sender.close()
        receiver.close()


async def test_receive_spool_stays_anchored_when_workspace_is_renamed(tmp_path):
    module = import_module("loom_capacity_executor.native_artifact_transfer")
    archive = tmp_path / "artifact"
    archive.write_bytes(b"private-artifact")
    workspace = tmp_path / "io"
    workspace.mkdir(mode=0o700)
    moved = tmp_path / "moved-io"
    sender, receiver = socket.socketpair()
    task = asyncio.create_task(module.send_native_artifact(sender, archive=archive, claim_digest="a" * 64,
        source_binding_sha256="b" * 64, max_artifact_bytes=1024))
    try:
        async with module.receive_native_artifact(receiver, workspace=workspace, claim_digest="a" * 64,
            source_binding_sha256="b" * 64, max_artifact_bytes=1024) as received:
            workspace.rename(moved)
            workspace.mkdir(mode=0o700)
            (workspace / "foreign").write_bytes(b"preserve")
            assert received.archive.read_bytes() == b"private-artifact"
        await task
        assert list(moved.iterdir()) == []
        assert (workspace / "foreign").read_bytes() == b"preserve"
    finally:
        task.cancel()
        with suppress(asyncio.CancelledError):
            await task
        sender.close()
        receiver.close()


async def test_receiver_cancellation_settles_actual_write_before_directory_cleanup(tmp_path, monkeypatch):
    import threading

    module = import_module("loom_capacity_executor.native_artifact_transfer")
    archive = tmp_path / "artifact"
    archive.write_bytes(b"private-artifact")
    workspace = tmp_path / "io"
    workspace.mkdir(mode=0o700)
    entered, release = asyncio.Event(), threading.Event()
    loop = asyncio.get_running_loop()
    original = module._write_all

    def blocked(descriptor, data):
        loop.call_soon_threadsafe(entered.set)
        assert release.wait(5)
        original(descriptor, data)

    monkeypatch.setattr(module, "_write_all", blocked)
    sender, receiver = socket.socketpair()
    sending = asyncio.create_task(module.send_native_artifact(sender, archive=archive, claim_digest="a" * 64,
        source_binding_sha256="b" * 64, max_artifact_bytes=1024))

    async def consume():
        async with module.receive_native_artifact(receiver, workspace=workspace, claim_digest="a" * 64,
            source_binding_sha256="b" * 64, max_artifact_bytes=1024):
            pytest.fail("cancelled write yielded an artifact")

    receiving = asyncio.create_task(consume())
    try:
        async with asyncio.timeout(2):
            await entered.wait()
        receiving.cancel()
        await asyncio.sleep(0)
        assert not receiving.done() and len(list(workspace.iterdir())) == 1
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await receiving
        await sending
        assert list(workspace.iterdir()) == []
    finally:
        release.set()
        for task in (sending, receiving):
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task
        sender.close()
        receiver.close()


async def test_sender_rejects_archive_mutation_between_digest_and_transfer(tmp_path, monkeypatch):
    module = import_module("loom_capacity_executor.native_artifact_transfer")
    archive = tmp_path / "artifact"
    archive.write_bytes(b"original")
    sender, receiver = socket.socketpair()
    loop = asyncio.get_running_loop()
    original = loop.sock_sendall
    calls = []

    async def mutate_after_header(channel, data):
        await original(channel, data)
        calls.append(len(data))
        if len(calls) == 1:
            archive.write_bytes(b"changed!")

    monkeypatch.setattr(loop, "sock_sendall", mutate_after_header)
    try:
        with pytest.raises(ValueError, match="changed during export"):
            await module.send_native_artifact(sender, archive=archive, claim_digest="a" * 64,
                source_binding_sha256="b" * 64, max_artifact_bytes=1024)
        assert len(calls) == 2
    finally:
        sender.close()
        receiver.close()


async def test_sender_total_timeout_stops_blocked_peer(tmp_path):
    module = import_module("loom_capacity_executor.native_artifact_transfer")
    archive = tmp_path / "artifact"
    archive.write_bytes(b"a" * 1024**2)
    sender, receiver = socket.socketpair()
    sender.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 1024)
    try:
        with pytest.raises(TimeoutError):
            await module.send_native_artifact(sender, archive=archive, claim_digest="a" * 64,
                source_binding_sha256="b" * 64, max_artifact_bytes=1024**2, timeout_seconds=1)
    finally:
        sender.close()
        receiver.close()
