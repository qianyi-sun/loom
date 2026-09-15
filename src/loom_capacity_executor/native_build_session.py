"""Trusted mapped runtime session; no credentials, publication or release power.

The caller supplies verified material and a private allocation-owned workspace,
retains staged source lifetime, and owns the outer IO/RootlessKit death chain.
This session only composes local execution, exact cleanup and artifact checking.
"""

from __future__ import annotations

import os
import select
import socket
import stat
import subprocess
import sys
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path

from loom.personal_dev_builder_artifact import (
    PersonalDevBuildArtifactBinding,
    VerifiedPersonalDevBuildArtifact,
    verify_bound_personal_dev_build_artifact,
)
from loom_capacity_agent.build_admission import BuildClaimRequestV1, BuildSourceContextV1
from loom_capacity_executor.native_parent_death import bind_native_parent_death
from loom_capacity_executor.native_runsc import NativeRunscLayout
from loom_capacity_executor.native_runtime_broker import NativeBrokerReady
from loom_capacity_executor.native_runtime_cleanup import (
    NativeRuntimeCleanupResult,
    reconcile_native_runtime_cleanup,
)
from loom_capacity_executor.native_sandbox_contract import render_native_sandbox_contract
from loom_capacity_executor.native_supervisor import (
    NativeSupervisionResult,
    _configure,
    _receive,
    supervise_native_execution,
)
from loom_capacity_manager.contracts import canonical_digest


@dataclass(frozen=True, slots=True)
class NativeBuildSessionResult:
    supervision: NativeSupervisionResult
    cleanup: NativeRuntimeCleanupResult
    artifact: VerifiedPersonalDevBuildArtifact | None


@contextmanager
def _broker_session(layout: NativeRunscLayout) -> Iterator[tuple[socket.socket, subprocess.Popen[bytes]]]:
    channel, child_channel = socket.socketpair(socket.AF_UNIX, socket.SOCK_SEQPACKET)
    broker = None
    try:
        _configure(channel)
        broker = subprocess.Popen([sys.executable, "-m", "loom_capacity_executor.native_runtime_broker",
            *layout.arguments(), "--expected-parent", str(os.getpid()), "--control-fd", str(child_channel.fileno())],
            pass_fds=(child_channel.fileno(),), close_fds=True, stdin=subprocess.DEVNULL)
        child_channel.close()
        if not select.select([channel], [], [], 10)[0]:
            raise RuntimeError("native broker did not become ready")
        ready = _receive(channel, {"broker-ready": NativeBrokerReady})
        if (not isinstance(ready, NativeBrokerReady)
            or (ready.pid, ready.parent_pid, ready.claim_digest) != (broker.pid, os.getpid(), layout.claim_digest)
            or broker.poll() is not None):
            raise ValueError("native broker readiness identity changed")
        yield channel, broker
    finally:
        try:
            if broker is not None:
                if broker.poll() is None:
                    broker.kill()
                broker.wait(timeout=5)
        finally:
            channel.close()
            child_channel.close()


def execute_native_build_session(*, claim: BuildClaimRequestV1, context: BuildSourceContextV1,
    layout: NativeRunscLayout, workspace: Path, authority: socket.socket, expected_parent_pid: int,
    max_artifact_bytes: int, max_image_archive_bytes: int,
) -> NativeBuildSessionResult:
    """No artifact is opened before successful execution and confirmed cleanup.

    Run only in the trusted mapped reader, never inside feature code. The caller
    has verified layout/rootfs/workspace material and enforces one-shot use. The
    authority channel is precreated and credential-free; its IO peer remains
    outside the runtime namespace. Failures confer no release authority.
    """
    bind_native_parent_death(expected_parent_pid)
    claim = BuildClaimRequestV1.model_validate_json(claim.model_dump_json())
    context = BuildSourceContextV1.model_validate_json(context.model_dump_json())
    layout.__post_init__()
    if (context.claim_digest != canonical_digest(claim) or layout.claim_digest != context.claim_digest
        or context.request_id != claim.request_id
        or claim.binding.pool_id != ("gb10" if context.platform == "linux/arm64" else "oldlab")):
        raise ValueError("native build session identity changed")
    render_native_sandbox_contract(context, max_artifact_bytes=max_artifact_bytes,
        max_image_archive_bytes=max_image_archive_bytes)
    if not workspace.is_absolute() or workspace == Path("/") or ".." in workspace.parts:
        raise ValueError("native build workspace must be absolute and private")
    metadata = workspace.lstat()
    if (not stat.S_ISDIR(metadata.st_mode) or metadata.st_uid != os.geteuid()
        or stat.S_IMODE(metadata.st_mode) != 0o700):
        raise ValueError("native build workspace must be private and owner-controlled")
    with _broker_session(layout) as (channel, broker):
        supervision = supervise_native_execution(claim, source_binding_sha256=context.source_binding_sha256,
            authority=authority, broker_channel=channel, broker_process=broker)
    cleanup = reconcile_native_runtime_cleanup(layout, broker_process=broker)
    if not supervision.client_succeeded or not supervision.broker_reaped or not cleanup.confirmed:
        return NativeBuildSessionResult(supervision, cleanup, None)
    binding = PersonalDevBuildArtifactBinding(candidate_sha=context.candidate_sha,
        source_sha256=context.source_sha256, archive_sha256=context.archive_sha256,
        build_contract_sha256=context.build_contract_sha256, attempt_id=context.attempt_id,
        lease_epoch=context.lease_epoch, platform=context.platform)
    verified = workspace / "verified"
    verified.mkdir(mode=0o700)
    artifact = verify_bound_personal_dev_build_artifact(workspace / "output/build/artifacts.tar", binding,
        output_directory=verified, max_artifact_bytes=max_artifact_bytes,
        max_image_archive_bytes=max_image_archive_bytes)
    return NativeBuildSessionResult(supervision, cleanup, artifact)
