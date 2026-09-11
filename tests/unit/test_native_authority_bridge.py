"""The IO helper forwards exact authority, never manufactures a live permit."""

import asyncio
import importlib
import socket
from contextlib import suppress
from uuid import uuid4

import pytest

from loom_capacity_executor.native_supervisor import NativeAuthorityRequest
from loom_capacity_manager.contracts import canonical_bytes
from tests.unit.test_native_execution_deadline import receipt
from tests.unit.test_native_execution_permit import execution_request


@pytest.mark.parametrize("pool", ["oldlab", "gb10"])
async def test_authority_bridge_forwards_exact_initial_and_renewal_without_credentials(pool):
    module = importlib.import_module("loom_capacity_executor.native_authority_bridge")
    request = execution_request(pool)
    calls = []
    expected = []

    class Client:
        async def authorize_execution(self, value, *, worker_credential):
            calls.append((value, worker_credential))
            return receipt(value)

    monitor, helper = socket.socketpair(socket.AF_UNIX, socket.SOCK_SEQPACKET)
    monitor.setblocking(False)
    task = asyncio.create_task(module.serve_native_execution_authority(helper,
        claim=request.claim, source_binding_sha256=request.source_binding_sha256,
        worker_credential="x" * 43, client=Client()))
    try:
        for _ in range(2):
            request = request.model_copy(update={"challenge": uuid4()})
            expected.append((request, "x" * 43))
            monitor.send(canonical_bytes(NativeAuthorityRequest(request=request)))
            async with asyncio.timeout(2):
                wire = await asyncio.get_running_loop().sock_recv(monitor, 65536)
            assert b"x" * 43 not in wire and b"worker_credential" not in wire
            assert module.NativeAuthorityPermission.model_validate_json(wire).permit == receipt(request)
        assert calls == expected
        monitor.close()
        async with asyncio.timeout(2):
            await task
    finally:
        task.cancel()
        with suppress(asyncio.CancelledError):
            await task
        monitor.close()
        helper.close()


@pytest.mark.parametrize("boundary", ["claim", "source", "noncanonical", "oversize", "malformed",
    "descriptor", "http", "wrong-permit", "invalid-permit", "untyped-permit"])
async def test_authority_bridge_fails_closed_without_retry_or_secret_echo(boundary):
    import array
    import os

    module = importlib.import_module("loom_capacity_executor.native_authority_bridge")
    request = execution_request()
    calls = []

    class Client:
        async def authorize_execution(self, value, *, worker_credential):
            calls.append(value)
            if boundary == "http":
                raise OSError("x" * 43)
            if boundary == "wrong-permit":
                return receipt(value.model_copy(update={"challenge": uuid4()}))
            if boundary == "invalid-permit":
                return receipt(value).model_copy(update={"request_digest": "f" * 64})
            if boundary == "untyped-permit":
                return receipt(value).model_dump()
            return receipt(value)

    sent = request
    if boundary == "claim":
        sent = sent.model_copy(update={"claim": execution_request().claim})
    elif boundary == "source":
        sent = sent.model_copy(update={"source_binding_sha256": "f" * 64})
    wire = canonical_bytes(NativeAuthorityRequest(request=sent))
    if boundary == "noncanonical":
        wire += b" "
    elif boundary == "oversize":
        wire = b"x" * 65537
    elif boundary == "malformed":
        wire = b'{"worker_credential":"' + b"x" * 43 + b'"}'
    monitor, helper = socket.socketpair(socket.AF_UNIX, socket.SOCK_SEQPACKET)
    monitor.setblocking(False)
    task = asyncio.create_task(module.serve_native_execution_authority(helper,
        claim=request.claim, source_binding_sha256=request.source_binding_sha256,
        worker_credential="x" * 43, client=Client()))
    try:
        if boundary == "descriptor":
            descriptor = os.open("/dev/null", os.O_RDONLY)
            try:
                monitor.sendmsg([wire], [(socket.SOL_SOCKET, socket.SCM_RIGHTS, array.array("i", [descriptor]))])
            finally:
                os.close(descriptor)
        else:
            monitor.send(wire)
        async with asyncio.timeout(2):
            response = await asyncio.get_running_loop().sock_recv(monitor, 65536)
            await task
        assert response == canonical_bytes(module.NativeAuthorityStop(kind="renewal-failed"))
        assert len(calls) == int(boundary in {"http", "wrong-permit", "invalid-permit", "untyped-permit"})
    finally:
        task.cancel()
        with suppress(asyncio.CancelledError):
            await task
        monitor.close()
        helper.close()


async def test_authority_bridge_cancellation_settles_inflight_client_and_sends_stop():
    module = importlib.import_module("loom_capacity_executor.native_authority_bridge")
    request = execution_request()
    reached, settled = asyncio.Event(), asyncio.Event()

    class Client:
        async def authorize_execution(self, value, *, worker_credential):
            reached.set()
            try:
                await asyncio.Future()
            finally:
                settled.set()

    monitor, helper = socket.socketpair(socket.AF_UNIX, socket.SOCK_SEQPACKET)
    monitor.setblocking(False)
    task = asyncio.create_task(module.serve_native_execution_authority(helper,
        claim=request.claim, source_binding_sha256=request.source_binding_sha256,
        worker_credential="x" * 43, client=Client()))
    try:
        monitor.send(canonical_bytes(NativeAuthorityRequest(request=request)))
        async with asyncio.timeout(2):
            await reached.wait()
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
            assert settled.is_set()
            response = await asyncio.get_running_loop().sock_recv(monitor, 65536)
        assert response == canonical_bytes(module.NativeAuthorityStop(kind="renewal-failed"))
    finally:
        task.cancel()
        with suppress(asyncio.CancelledError):
            await task
        monitor.close()
        helper.close()


@pytest.mark.parametrize("boundary", ["idle-cancel", "backpressure"])
async def test_authority_bridge_idle_cancellation_and_backpressure_are_bounded(monkeypatch, boundary):
    module = importlib.import_module("loom_capacity_executor.native_authority_bridge")
    request = execution_request()
    calls = []
    reading = asyncio.Event()
    loop = asyncio.get_running_loop()
    original = loop.add_reader
    removed = []
    remove_reader = loop.remove_reader

    def add_reader(*args):
        original(*args)
        reading.set()

    def remove(descriptor):
        removed.append(descriptor)
        return remove_reader(descriptor)

    monkeypatch.setattr(loop, "add_reader", add_reader)
    monkeypatch.setattr(loop, "remove_reader", remove)

    class Client:
        async def authorize_execution(self, value, *, worker_credential):
            calls.append(value)
            return receipt(value)

    monitor, helper = socket.socketpair(socket.AF_UNIX, socket.SOCK_SEQPACKET)
    monitor.setblocking(False)
    helper.setblocking(False)
    if boundary == "backpressure":
        helper.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 4096)
        for _ in range(1024):
            try:
                helper.send(b"test-buffer-fill" * 100)
            except BlockingIOError:
                break
        else:
            pytest.fail("test did not saturate the bounded socket")
        monitor.send(canonical_bytes(NativeAuthorityRequest(request=request)))
    task = asyncio.create_task(module.serve_native_execution_authority(helper,
        claim=request.claim, source_binding_sha256=request.source_binding_sha256,
        worker_credential="x" * 43, client=Client()))
    try:
        async with asyncio.timeout(2):
            if boundary == "idle-cancel":
                await reading.wait()
                task.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await task
                assert helper.fileno() in removed
                response = await loop.sock_recv(monitor, 65536)
                assert response == canonical_bytes(module.NativeAuthorityStop(kind="renewal-failed"))
            else:
                await task
        assert len(calls) == int(boundary == "backpressure")
    finally:
        task.cancel()
        with suppress(asyncio.CancelledError):
            await task
        monitor.close()
        helper.close()
