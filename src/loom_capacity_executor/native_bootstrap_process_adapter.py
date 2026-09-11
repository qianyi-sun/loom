"""Fixed receiver child composition with retained spawn and cleanup ownership.

The trusted TLS supervisor constructs this only from immutable local policy.
Request bytes cannot choose argv, environment, configuration or interpreter.
An overdue or cancelled operation is unknown, not rollback: historical lookup
reconciles publication, and a still-unreaped child retains its cleanup slot.
"""

from __future__ import annotations

import asyncio
import math
import os
from dataclasses import dataclass
from typing import Literal

from loom_capacity_executor.native_bootstrap_delivery import (
    _MAX_RECEIPT_BYTES,
    BootstrapDeliveryError,
    NativeBootstrapDeliveryReceiptV1,
    parse_native_delivery_receipt,
)
from loom_capacity_executor.native_bootstrap_transport import (
    _assert_private_process,
    _check_deadline,
    _request,
)
from loom_capacity_executor.slurm_contracts import SlurmFileIdentityV2
from loom_capacity_executor.trusted_launcher import (
    TrustedCandidateExecutableV2,
    _open_verified_candidate,
)

_FAILURE = "native bootstrap receiver process unavailable or refused"
_ReceiverIO = list[asyncio.Task[bytes | int | None]]
_ReceiverSpawn = asyncio.Task[asyncio.subprocess.Process]


@dataclass(frozen=True)
class NativeBootstrapReceiverProcessPolicy:
    interpreter: TrustedCandidateExecutableV2
    configuration: SlurmFileIdentityV2
    timeout_seconds: float = 15.0
    cleanup_wait_seconds: float = 1.0
    maximum_processes: int = 2

    def __post_init__(self) -> None:
        TrustedCandidateExecutableV2.model_validate_json(self.interpreter.model_dump_json())
        SlurmFileIdentityV2.model_validate_json(self.configuration.model_dump_json())
        for value, ceiling in ((self.timeout_seconds, 20), (self.cleanup_wait_seconds, 2)):
            if type(value) is not float or not math.isfinite(value) or not 0.05 <= value <= ceiling:
                raise ValueError("native receiver process time bound is invalid")
        if type(self.maximum_processes) is not int or not 1 <= self.maximum_processes <= 8:
            raise ValueError("native receiver process concurrency bound is invalid")


async def _read_bounded(reader: asyncio.StreamReader, maximum: int) -> bytes:
    result = bytearray()
    while True:
        chunk = await reader.read(min(16384, maximum + 1 - len(result)))
        if not chunk:
            return bytes(result)
        result.extend(chunk)
        if len(result) > maximum:
            raise BootstrapDeliveryError(_FAILURE)


async def _write_input(writer: asyncio.StreamWriter, raw: bytes) -> None:
    try:
        for offset in range(0, len(raw), 16384):
            writer.write(raw[offset:offset + 16384])
            await writer.drain()
    finally:
        writer.close()
    await writer.wait_closed()


async def _discard(reader: asyncio.StreamReader | None) -> None:
    if reader is None:
        return
    while await reader.read(16384):
        # Reaping must remain cooperative even if a broken trusted helper left
        # another writer. No discarded byte is retained or included in errors.
        await asyncio.sleep(0)


class NativeBootstrapProcessAdapter:
    """One sealed interpreter, fixed module/config, and bounded owned children.

    The protected installer must keep the interpreter's original runtime prefix
    and installed module tree immutable too. Sealing the ELF does not seal its
    shared libraries, site-packages or configuration directory. This adapter
    neither admits a cgroup nor installs that runtime.
    """

    def __init__(self, policy: NativeBootstrapReceiverProcessPolicy) -> None:
        _assert_private_process()
        if type(policy) is not NativeBootstrapReceiverProcessPolicy:
            raise BootstrapDeliveryError(_FAILURE)
        policy.__post_init__()
        # Verify/copy before serving any request; per-request filesystem work
        # belongs to the bounded child, not to the supervisor's event loop.
        self._descriptor = _open_verified_candidate(policy.interpreter)
        self._policy = policy
        self._requests: set[asyncio.Task[bytes]] = set()
        self._cleanups: dict[asyncio.Task[None], tuple[_ReceiverSpawn, _ReceiverIO]] = {}
        self._cleanup_failed = False
        self._close_task: asyncio.Task[None] | None = None

    @property
    def active_operations(self) -> int:
        # Double-count an operation transitioning to cleanup conservatively;
        # neither its spawn nor its unreaped child may free admission capacity.
        return len(self._requests) + len(self._cleanups)

    async def aclose(self) -> None:
        if (self._close_task is None or (self._close_task.done()
            and not self._close_task.cancelled() and self._close_task.exception() is not None)):
            self._close_task = asyncio.create_task(self._close())
        await asyncio.shield(self._close_task)

    async def _close(self) -> None:
        requests = tuple(self._requests)
        for task in requests:
            task.cancel()
        await asyncio.gather(*requests, return_exceptions=True)
        # A previous cleanup failure keeps the original Process handle and
        # descriptor charged. An explicit close retries those exact owners;
        # it never silently frees capacity or busy-loops on a failed task.
        for cleanup_task, (spawn, io) in tuple(self._cleanups.items()):
            if cleanup_task.done():
                if cleanup_task.cancelled() or cleanup_task.exception() is not None:
                    self._start_cleanup(spawn, io)
                self._cleanups.pop(cleanup_task, None)
        await asyncio.gather(*tuple(self._cleanups), return_exceptions=True)
        if self._cleanups:
            raise BootstrapDeliveryError(_FAILURE)
        if self._descriptor >= 0:
            os.close(self._descriptor)
            self._descriptor = -1

    async def receive(self, raw: bytes) -> NativeBootstrapDeliveryReceiptV1:
        receipt = await self._call(raw, "deliver")
        if receipt is None:
            raise BootstrapDeliveryError(_FAILURE)
        return receipt

    async def observe_receipt(self, raw: bytes) -> NativeBootstrapDeliveryReceiptV1 | None:
        return await self._call(raw, "status")

    async def _call(self, raw: bytes, operation: Literal["deliver", "status"]) -> NativeBootstrapDeliveryReceiptV1 | None:
        deadline = asyncio.get_running_loop().time() + self._policy.timeout_seconds
        _assert_private_process()
        if (self._close_task is not None or self._cleanup_failed
            or self.active_operations >= self._policy.maximum_processes):
            raise BootstrapDeliveryError(_FAILURE)
        _binding, expected = _request(raw, b"D" if operation == "deliver" else b"S")
        task = asyncio.create_task(self._run(raw, operation, deadline))
        self._requests.add(task)

        def finished(done: asyncio.Task[bytes]) -> None:
            self._requests.discard(done)
            if not done.cancelled():
                done.exception()

        task.add_done_callback(finished)
        try:
            # Retain a task owner if a kernel-blocked child cannot be reaped in
            # this response window. The supervisor may join it again via close.
            async with asyncio.timeout_at(deadline + self._policy.cleanup_wait_seconds):
                stdout = await asyncio.shield(task)
            _check_deadline(deadline)
            if stdout == b"null\n" and operation == "status":
                return None
            if not stdout.endswith(b"\n"):
                raise BootstrapDeliveryError(_FAILURE)
            receipt = parse_native_delivery_receipt(stdout[:-1])
            _check_deadline(deadline)
            if receipt != expected:
                raise BootstrapDeliveryError(_FAILURE)
            return receipt
        except asyncio.CancelledError:
            task.cancel()
            raise
        except Exception:
            task.cancel()
            raise BootstrapDeliveryError(_FAILURE) from None

    async def _run(self, raw: bytes, operation: Literal["deliver", "status"], deadline: float) -> bytes:
        spawn: _ReceiverSpawn | None = None
        io: _ReceiverIO = []
        try:
            async with asyncio.timeout_at(deadline):
                _check_deadline(deadline)
                config = self._policy.configuration
                spawn = asyncio.create_task(asyncio.create_subprocess_exec(
                    self._policy.interpreter.path, "-I", "-B", "-m", "loom_capacity_executor.native_bootstrap_receiver",
                    "--configuration", config.path, "--configuration-sha256", config.sha256,
                    "--configuration-owner-uid", str(config.owner_uid), "--operation", operation,
                    executable=f"/proc/self/fd/{self._descriptor}", pass_fds=(self._descriptor,),
                    env={}, start_new_session=True, stdin=asyncio.subprocess.PIPE,
                    stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE, limit=8192))
                # Never cancel this handoff. Cleanup must receive the Process
                # handle even if cancellation happens after fork but before return.
                process = await asyncio.shield(spawn)
                assert process.stdin is not None and process.stdout is not None and process.stderr is not None
                stdout = asyncio.create_task(_read_bounded(process.stdout, _MAX_RECEIPT_BYTES + 1))
                stderr = asyncio.create_task(_read_bounded(process.stderr, 4096))
                written = asyncio.create_task(_write_input(process.stdin, raw))
                exited = asyncio.create_task(process.wait())
                io.extend((stdout, stderr, written, exited))
                await asyncio.gather(*io)
                _check_deadline(deadline)
                if exited.result() != 0 or stderr.result():
                    raise BootstrapDeliveryError(_FAILURE)
                return stdout.result()
        finally:
            if spawn is not None:
                cleanup = self._start_cleanup(spawn, io)
                await asyncio.shield(cleanup)

    def _start_cleanup(self, spawn: _ReceiverSpawn, io: _ReceiverIO) -> asyncio.Task[None]:
        cleanup = asyncio.create_task(self._cleanup(spawn, io))
        self._cleanups[cleanup] = (spawn, io)

        def finished(done: asyncio.Task[None]) -> None:
            if done.cancelled() or done.exception() is not None:
                self._cleanup_failed = True
            else:
                self._cleanups.pop(done, None)

        cleanup.add_done_callback(finished)
        return cleanup

    async def _cleanup(self, spawn: _ReceiverSpawn, io: _ReceiverIO) -> None:
        for task in io:
            task.cancel()
        await asyncio.gather(*io, return_exceptions=True)
        try:
            process = await asyncio.shield(spawn)
        except Exception:
            return
        if process.returncode is None:
            try:
                # Use the owned subprocess handle, not a recycled PID/process
                # group. The fixed receiver has no child-execution operation.
                process.kill()
            except ProcessLookupError:
                pass
        if process.stdin is not None and not process.stdin.transport.is_closing():
            process.stdin.transport.abort()
        # Drain remaining pipe bytes without retaining them; waiting with paused
        # full stdout/stderr transports can otherwise deadlock even after kill.
        results = await asyncio.gather(
            _discard(process.stdout), _discard(process.stderr), process.wait(),
            return_exceptions=True,
        )
        if any(isinstance(result, BaseException) for result in results) or process.returncode is None:
            raise BootstrapDeliveryError(_FAILURE)
        if process.stdin is not None:
            try:
                await process.stdin.wait_closed()
            except (BrokenPipeError, ConnectionResetError):
                # The child is reaped and both output pipes reached EOF. A
                # broken input is expected after killing a stalled receiver.
                pass
