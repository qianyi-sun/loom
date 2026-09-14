"""Pre-material recovery acknowledgment on the existing private authority channel."""

from __future__ import annotations

import asyncio
import os
import select
import socket
from collections.abc import Awaitable, Callable
from contextlib import ExitStack
from dataclasses import asdict
from pathlib import Path
from typing import TYPE_CHECKING, Literal

from loom_capacity_agent.build_admission import BuildClaimRequestV1
from loom_capacity_agent.native_recovery import (
    NativeInstalledAttemptV2,
    NativeRecoveryMappingRange,
    NativeRecoveryPreparationV1,
)
from loom_capacity_agent.native_recovery_publication import (
    NativeRecoveryPublicationV1,
    NativeRecoveryReceiptV1,
)
from loom_capacity_executor.native_authority_bridge import _readable
from loom_capacity_executor.native_identity_mapping import observe_native_mapped_identity
from loom_capacity_executor.native_oci_material import _open_directory
from loom_capacity_executor.native_supervisor import _configure, _receive, _send
from loom_capacity_manager.contracts import Digest, StrictV1Model, canonical_digest

if TYPE_CHECKING:
    from loom_capacity_executor.native_rootless_runtime import NativeRootlessSpecV3


class NativeRecoveryFinalize(StrictV1Model):
    kind: Literal["recovery-finalize"] = "recovery-finalize"
    request: NativeRecoveryPublicationV1


class NativeRecoveryAcknowledgment(StrictV1Model):
    kind: Literal["recovery-committed"] = "recovery-committed"
    request_digest: Digest


def acknowledge_mapped_recovery(channel: socket.socket, *, spec: NativeRootlessSpecV3,
    runtime_spec_sha256: str,
) -> str:
    """Observe before material creation; fail without exact committed acknowledgment."""
    preparation = spec.recovery_preparation
    with ExitStack() as stack:
        descriptor = _open_directory(Path(preparation.locator.directory), stack)
        metadata = os.fstat(descriptor)
        if ((metadata.st_dev, metadata.st_ino) != (preparation.locator.device, preparation.locator.inode)
            or metadata.st_mode & 0o7777 != 0o700):
            raise ValueError("native mapped recovery attempt identity changed")
        actual = observe_native_mapped_identity()
        record = NativeInstalledAttemptV2(preparation=preparation, runtime_spec_sha256=runtime_spec_sha256,
            uid_map=tuple(NativeRecoveryMappingRange(**asdict(item)) for item in actual.uid_ranges),
            gid_map=tuple(NativeRecoveryMappingRange(**asdict(item)) for item in actual.gid_ranges))
        request = NativeRecoveryPublicationV1(claim=spec.claim, record=record)
        _configure(channel)
        _send(channel, NativeRecoveryFinalize(request=request))
        if not select.select([channel], [], [], 30)[0]:
            raise TimeoutError("native mapped recovery acknowledgment expired")
        acknowledgment = _receive(channel, {"recovery-committed": NativeRecoveryAcknowledgment})
        if not isinstance(acknowledgment, NativeRecoveryAcknowledgment) or acknowledgment.request_digest != canonical_digest(request):
            raise ValueError("native mapped recovery acknowledgment changed")
        visible = os.fstat(_open_directory(Path(preparation.locator.directory), stack))
        if (visible.st_dev, visible.st_ino, visible.st_mode) != (metadata.st_dev, metadata.st_ino, metadata.st_mode):
            raise ValueError("native mapped recovery attempt changed during publication")
        return acknowledgment.request_digest


async def commit_mapped_recovery(channel: socket.socket, *, claim: BuildClaimRequestV1,
    preparation: NativeRecoveryPreparationV1, runtime_spec_sha256: str,
    publish: Callable[[NativeRecoveryPublicationV1], Awaitable[NativeRecoveryReceiptV1]],
) -> str:
    """Original-UID IO only; publish once and acknowledge only validated receipt."""
    _configure(channel)
    async with asyncio.timeout(30):
        while True:
            try:
                message = _receive(channel, {"recovery-finalize": NativeRecoveryFinalize})
                break
            except BlockingIOError:
                await _readable(channel)
        if (not isinstance(message, NativeRecoveryFinalize) or message.request.claim != claim
            or not isinstance(message.request.record, NativeInstalledAttemptV2)
            or message.request.record.preparation != preparation
            or message.request.record.runtime_spec_sha256 != runtime_spec_sha256):
            raise ValueError("native recovery finalization changed before publication")
        receipt = await publish(message.request)
        receipt = NativeRecoveryReceiptV1.model_validate_json(receipt.model_dump_json())
        if receipt.request != message.request:
            raise ValueError("native recovery finalization receipt changed")
        _send(channel, NativeRecoveryAcknowledgment(request_digest=receipt.request_digest))
        return receipt.request_digest
