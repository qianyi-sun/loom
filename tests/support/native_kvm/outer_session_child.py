"""Transport-only child double; real KVM execution is tested separately."""

import asyncio
import hashlib
import os
import socket
import sys
from pathlib import Path
from uuid import uuid4

from loom_capacity_agent.build_admission import BuildArtifactV1, BuildExecutionRequestV1
from loom_capacity_executor.native_artifact_transfer import send_native_artifact
from loom_capacity_executor.native_rootless_runtime import (
    NativeRootlessResultV1,
    read_native_rootless_spec,
)
from loom_capacity_manager.contracts import canonical_bytes
from loom_capacity_executor.native_supervisor import NativeAuthorityRequest


async def main():
    spec = read_native_rootless_spec(Path(sys.argv[1]), expected_sha256=sys.argv[2])
    mode = sys.argv[5]
    with socket.socket(fileno=int(sys.argv[3])) as authority, socket.socket(fileno=int(sys.argv[4])) as artifact:
        assert authority.family == artifact.family == socket.AF_UNIX
        if mode in {"oversize", "stdout-flood"}:
            os.write(1, b"x" * (1024**2 if mode == "stdout-flood" else 4097))
            await asyncio.Future()
        if mode == "cancel":
            artifact.send(b"LOOMNAT1")
            Path(sys.argv[6]).write_text("started")
            await asyncio.Future()
        ready = None
        if mode == "authority-cleanup-cancel":
            authority.send(canonical_bytes(NativeAuthorityRequest(request=BuildExecutionRequestV1(claim=spec.claim,
                source_binding_sha256=spec.context.source_binding_sha256, challenge=uuid4()))))
        if mode not in {"failed", "wrong-claim", "malformed"}:
            ready = await send_native_artifact(artifact, archive=Path(sys.argv[6]),
                claim_digest=spec.context.claim_digest, source_binding_sha256=spec.context.source_binding_sha256,
                max_artifact_bytes=spec.max_artifact_bytes, timeout_seconds=5)
        else:
            artifact.shutdown(socket.SHUT_WR)
        if mode == "artifact-mismatch":
            ready = BuildArtifactV1(archive_sha256=hashlib.sha256(b"wrong").hexdigest(), archive_size_bytes=5)
        result = NativeRootlessResultV1(claim_digest="f" * 64 if mode == "wrong-claim" else spec.context.claim_digest,
            source_binding_sha256=spec.context.source_binding_sha256,
            client_succeeded=mode != "failed", broker_reaped=True, cleanup_confirmed=mode != "uncertain", artifact=ready)
        os.write(1, b"bad\n" if mode == "malformed" else canonical_bytes(result) + b"\n")


asyncio.run(main())
