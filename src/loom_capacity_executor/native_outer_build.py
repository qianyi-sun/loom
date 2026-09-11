"""Join trusted outer IO to fixed native runtime; no installation/release power."""

from __future__ import annotations

import asyncio
import hashlib
import os
import socket
import stat
import sys
from collections.abc import AsyncGenerator
from contextlib import AsyncExitStack, aclosing, suppress
from pathlib import Path
from typing import TypeVar
from uuid import NAMESPACE_URL, uuid5

from loom_capacity_agent.build_admission import BuildOutcomeReceiptV1, BuildOutcomeRequestV1
from loom_capacity_executor.native_allocated_io import NativeAllocatedIO
from loom_capacity_executor.native_artifact_transfer import (
    NativeReceivedArtifact,
    receive_native_artifact,
)
from loom_capacity_executor.native_authority_bridge import _readable
from loom_capacity_executor.native_build_source import _settled_io
from loom_capacity_executor.native_rootless_runtime import (
    NativeRootlessResultV1,
    NativeRootlessSpecV1,
    read_native_rootless_spec,
)
from loom_capacity_executor.native_runtime_input import prepare_native_runtime_input
from loom_capacity_manager.contracts import canonical_bytes

_T = TypeVar("_T")


async def _join(task: asyncio.Task[_T]) -> _T:
    """Preserve cancellation without abandoning task-owned cleanup."""
    interrupted = False
    while not task.done():
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError:
            interrupted = True
    result = task.result()
    if interrupted:
        raise asyncio.CancelledError
    return result


async def _stop_process(process: asyncio.subprocess.Process) -> None:
    if process.returncode is None:
        with suppress(ProcessLookupError):
            process.kill()
    async with asyncio.timeout(10):
        # A rejected result can leave StreamReader paused under backpressure.
        # Reaping alone then waits for pipe disconnection forever, even after
        # SIGKILL. The result reader has settled before cleanup owns this pipe.
        async def discard_stdout() -> None:
            if process.stdout is not None:
                while await process.stdout.read(65536):
                    pass

        async with asyncio.TaskGroup() as group:
            group.create_task(discard_stdout())
            group.create_task(process.wait())


async def _cancel_and_settle(task: asyncio.Task[_T]) -> None:
    # Do not inject another cancellation into an operation's async finally.
    if not task.done() and task.cancelling() == 0:
        task.cancel()
    await asyncio.gather(task, return_exceptions=True)


async def _owned_operation(task: asyncio.Task[_T]) -> _T:
    """One operation, with single cancellation and fully settled client cleanup."""
    try:
        return await asyncio.shield(task)
    except asyncio.CancelledError:
        await _join(asyncio.create_task(_cancel_and_settle(task)))
        raise


async def _spawn(spec_path: Path, digest: str, authority_fd: int, artifact_fd: int) -> asyncio.subprocess.Process:
    starting = asyncio.create_task(asyncio.create_subprocess_exec(sys.executable, "-I", "-m",
        "loom_capacity_executor.native_rootless_runtime", "launch", "--spec", str(spec_path),
        "--spec-sha256", digest, "--expected-parent", str(os.getpid()), "--authority-fd", str(authority_fd),
        "--artifact-fd", str(artifact_fd), pass_fds=(authority_fd, artifact_fd),
        stdin=asyncio.subprocess.DEVNULL, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL,
        limit=4097, env={"PATH": "/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin", "LANG": "C.UTF-8"}))
    try:
        return await asyncio.shield(starting)
    except asyncio.CancelledError:
        async def settle_spawn() -> None:
            try:
                process = await starting
            except Exception:
                return  # Failed creation has no returned process handle.
            await _stop_process(process)
        await _join(asyncio.create_task(settle_spawn()))
        raise


async def _result(process: asyncio.subprocess.Process, spec: NativeRootlessSpecV1) -> NativeRootlessResultV1:
    if process.stdout is None:
        raise ValueError("native rootless result pipe is absent")
    wire = bytearray()
    while True:
        part = await process.stdout.read(4097 - len(wire))
        if not part:
            break
        wire.extend(part)
        if len(wire) > 4096:
            raise ValueError("native rootless result exceeds byte bound")
    result = NativeRootlessResultV1.model_validate_json(bytes(wire))
    if (canonical_bytes(result) + b"\n" != bytes(wire)
        or result.claim_digest != spec.context.claim_digest
        or result.source_binding_sha256 != spec.context.source_binding_sha256):
        raise ValueError("native rootless result identity changed")
    if await process.wait() != 0:
        raise RuntimeError("native rootless process failed")
    return result


async def _receive(stack: AsyncExitStack, channel: socket.socket, spec: NativeRootlessSpecV1,
    workspace: Path, timeout_seconds: int,
) -> NativeReceivedArtifact | None:
    channel.setblocking(False)
    while True:
        try:
            first, ancillary, flags, _address = channel.recvmsg(1, 0, socket.MSG_PEEK | socket.MSG_CMSG_CLOEXEC)
        except BlockingIOError:
            await _readable(channel)
            continue
        if ancillary or flags & (socket.MSG_CTRUNC | socket.MSG_TRUNC):
            raise ValueError("native artifact carried unexpected descriptors")
        if not first:
            return None  # Valid only when matching completed result also has no artifact.
        return await stack.enter_async_context(receive_native_artifact(channel, workspace=workspace,
            claim_digest=spec.context.claim_digest, source_binding_sha256=spec.context.source_binding_sha256,
            max_artifact_bytes=spec.max_artifact_bytes, timeout_seconds=min(timeout_seconds, 1800)))


class _UploadBody:
    def __init__(self, received: NativeReceivedArtifact) -> None:
        self.received = received
        self.complete = False

    async def chunks(self) -> AsyncGenerator[bytes, None]:
        descriptor = os.open(self.received.archive, os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW | os.O_NONBLOCK)
        try:
            metadata = os.fstat(descriptor)
            expected = self.received.artifact
            if (not stat.S_ISREG(metadata.st_mode) or metadata.st_uid != os.geteuid()
                or stat.S_IMODE(metadata.st_mode) != 0o400 or metadata.st_nlink != 1
                or metadata.st_size != expected.archive_size_bytes):
                raise ValueError("native upload spool identity changed")
            size, digest = 0, hashlib.sha256()
            while part := await _settled_io(os.read, descriptor, 1024**2):
                size += len(part)
                if size > expected.archive_size_bytes:
                    raise ValueError("native upload spool grew")
                digest.update(part)
                yield part
            if size != expected.archive_size_bytes or digest.hexdigest() != expected.archive_sha256:
                raise ValueError("native upload spool bytes changed")
            self.complete = True
        finally:
            os.close(descriptor)


async def run_native_outer_build(owner: NativeAllocatedIO, *, spec_path: Path, expected_sha256: str,
    artifact_workspace: Path, timeout_seconds: int,
) -> BuildOutcomeReceiptV1:
    """Caller retains allocated IO and preverified material/one-shot scope.

    Requires already provisioned mapped rootfs/output/runtime material; this is
    not an installer. No credential/client/source proc-FD crosses to the child.
    Uncertain cleanup or ambiguous writes fail to management recovery, not release.
    """
    if type(timeout_seconds) is not int or timeout_seconds <= 0:
        raise ValueError("native outer build timeout must be positive")
    spec = read_native_rootless_spec(spec_path, expected_sha256=expected_sha256)
    if spec.claim != owner.claim or spec.context != owner.source.context:
        raise ValueError("native outer build source identity changed")
    authority, mapped_authority = socket.socketpair(socket.AF_UNIX, socket.SOCK_SEQPACKET)
    artifact, mapped_artifact = socket.socketpair(socket.AF_UNIX, socket.SOCK_STREAM)
    process = None
    authority_task = None
    async with AsyncExitStack() as stack:
        try:
            async with asyncio.timeout(timeout_seconds):
                await prepare_native_runtime_input(owner.source, workspace=Path(spec.workspace),
                    max_artifact_bytes=spec.max_artifact_bytes, max_image_archive_bytes=spec.max_image_archive_bytes)
                process = await _spawn(spec_path, expected_sha256, mapped_authority.fileno(), mapped_artifact.fileno())
                mapped_authority.close()
                mapped_artifact.close()
                authority_task = asyncio.create_task(owner.serve_authority(authority))
                # Both directions progress under backpressure. TaskGroup cancels
                # the receiver immediately if result capture fails its bound.
                async with asyncio.TaskGroup() as group:
                    result_task = group.create_task(_result(process, spec))
                    receive_task = group.create_task(_receive(stack, artifact, spec, artifact_workspace, timeout_seconds))
                result, received = result_task.result(), receive_task.result()
                await _join(asyncio.create_task(_cancel_and_settle(authority_task)))
                if not result.broker_reaped or not result.cleanup_confirmed:
                    raise RuntimeError("native runtime cleanup is uncertain")
                if ((received is None) != (result.artifact is None)
                    or result.client_succeeded != (received is not None)
                    or (received is not None and received.artifact != result.artifact)):
                    raise ValueError("native rootless result and artifact differ")
                if received is not None:
                    body = _UploadBody(received)
                    async with aclosing(body.chunks()) as chunks:
                        await _owned_operation(asyncio.create_task(owner.upload_artifact(received.artifact, chunks=chunks)))
                    if not body.complete:
                        raise ValueError("native upload completed without consuming exact bytes")
                outcome = BuildOutcomeRequestV1(claim=owner.claim,
                    operation_id=uuid5(NAMESPACE_URL, f"loom:native-build-outcome:v1:{spec.context.claim_digest}"),
                    result="artifact-ready" if received is not None else "failed",
                    artifact=received.artifact if received is not None else None)
                return await _owned_operation(asyncio.create_task(owner.record_outcome(outcome)))
        finally:
            async def cleanup() -> None:
                try:
                    if process is not None:
                        await _stop_process(process)
                finally:
                    if authority_task is not None:
                        await _cancel_and_settle(authority_task)
                    for channel in (authority, mapped_authority, artifact, mapped_artifact):
                        channel.close()
            await _join(asyncio.create_task(cleanup()))
