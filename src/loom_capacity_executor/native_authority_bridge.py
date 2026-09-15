"""Authenticated authority IO, to run outside the independent native monitor.

The caller owns the consumed handoff, pinned client, staged source lifetime and
separate parent-bound helper process. This adapter neither launches that process
nor certifies Slurm containment. Silence still expires in the monitor even when
this helper or its HTTP operation is stuck.
"""

from __future__ import annotations

import asyncio
import re
import socket
from contextlib import suppress
from typing import Protocol

from loom_capacity_agent.build_admission import (
    BuildClaimExchangeV1,
    BuildClaimRequestV1,
    BuildExecutionPermitV1,
    BuildExecutionRequestV1,
)
from loom_capacity_executor.native_supervisor import (
    NativeAuthorityPermission,
    NativeAuthorityRequest,
    NativeAuthorityStop,
    _configure,
    _receive,
    _send,
)


class NativeExecutionAuthorityClient(Protocol):
    async def authorize_execution(self, request: BuildExecutionRequestV1, *,
        worker_credential: str,
    ) -> BuildExecutionPermitV1: ...


async def _readable(channel: socket.socket) -> None:
    loop = asyncio.get_running_loop()
    ready: asyncio.Future[None] = loop.create_future()
    descriptor = channel.fileno()

    def notify() -> None:
        if not ready.done():
            ready.set_result(None)

    loop.add_reader(descriptor, notify)
    try:
        await ready
    finally:
        loop.remove_reader(descriptor)


def _stop(channel: socket.socket) -> None:
    # No retries or sensitive error detail. A full/closed channel is handled by
    # the monitor's independent deadline, not a blocking send in this helper.
    with suppress(OSError, ValueError):
        _send(channel, NativeAuthorityStop(kind="renewal-failed"))


async def serve_native_execution_authority(channel: socket.socket, *, claim: BuildClaimRequestV1,
    source_binding_sha256: str, worker_credential: str, client: NativeExecutionAuthorityClient,
) -> None:
    """Forward only this consumed claim/source; caller retains socket ownership.

    Every initial/renewal request reaches existing authenticated authority once.
    No synthetic permissions, cached receipts, clock reset or network retry is
    allowed here. On failure emit a finite stop frame and return; cancellation
    propagates after cancellation of the awaited client operation.
    """
    try:
        envelope = BuildClaimExchangeV1.model_validate_json(BuildClaimExchangeV1(
            claim=claim, worker_credential=worker_credential).model_dump_json())
        if not isinstance(source_binding_sha256, str) or re.fullmatch(r"[0-9a-f]{64}", source_binding_sha256) is None:
            raise ValueError("invalid source binding")
    except ValueError:
        raise ValueError("native authority helper identity is invalid") from None
    _configure(channel)
    try:
        while True:
            try:
                message = _receive(channel, {"authorize": NativeAuthorityRequest})
            except BlockingIOError:
                await _readable(channel)
                continue
            if (not isinstance(message, NativeAuthorityRequest)
                or message.request.claim != envelope.claim
                or message.request.source_binding_sha256 != source_binding_sha256):
                raise ValueError("native authority request identity changed")
            permit = await client.authorize_execution(message.request, worker_credential=envelope.worker_credential)
            if not isinstance(permit, BuildExecutionPermitV1):
                raise ValueError("native authority receipt is not typed")
            permit = BuildExecutionPermitV1.model_validate_json(permit.model_dump_json())
            if permit.request != message.request:
                raise ValueError("native authority receipt request changed")
            _send(channel, NativeAuthorityPermission(permit=permit))
    except asyncio.CancelledError:
        _stop(channel)
        raise
    except Exception:
        _stop(channel)
