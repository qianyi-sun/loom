"""Bounded native control loop, independent of broker spawn and authority IO.

The caller must precreate verified, kernel-parent-bound helpers under the exact
allocation and existing one-shot launcher fence. This loop neither installs
runtime material nor proves that killing the broker contains all its children.
Runtime reconciliation and manager physical-release proof remain mandatory.
"""

from __future__ import annotations

import json
import selectors
import socket
import subprocess
import time
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Annotated, Literal

from pydantic import Field

from loom_capacity_agent.build_admission import (
    BuildClaimRequestV1,
    BuildExecutionPermitV1,
    BuildExecutionRequestV1,
)
from loom_capacity_executor.native_execution_deadline import NativeExecutionDeadline
from loom_capacity_manager.contracts import StrictV1Model, canonical_bytes

_MAX_FRAME = 64 * 1024


class NativeAuthorityRequest(StrictV1Model):
    kind: Literal["authorize"] = "authorize"
    request: BuildExecutionRequestV1


class NativeAuthorityPermission(StrictV1Model):
    kind: Literal["permit"] = "permit"
    permit: BuildExecutionPermitV1


class NativeAuthorityStop(StrictV1Model):
    kind: Literal["cancelled", "renewal-failed"]


class NativeBrokerStart(StrictV1Model):
    kind: Literal["start"] = "start"
    role: Literal["pause", "buildkit", "client"]
    deadline_boottime_ns: Annotated[int, Field(gt=0)]


class NativeBrokerEvent(StrictV1Model):
    kind: Literal["pause-ready", "buildkit-ready", "client-succeeded", "client-failed", "failed"]


@dataclass(frozen=True, slots=True)
class NativeSupervisionResult:
    """Client observation only; never artifact-ready or physical release proof."""

    client_succeeded: bool
    reason: str
    broker_reaped: bool


_AUTHORITY_MODELS: Mapping[str, type[StrictV1Model]] = {
    "permit": NativeAuthorityPermission, "cancelled": NativeAuthorityStop, "renewal-failed": NativeAuthorityStop}
_BROKER_MODELS: Mapping[str, type[StrictV1Model]] = {
    kind: NativeBrokerEvent for kind in ("pause-ready", "buildkit-ready", "client-succeeded", "client-failed", "failed")}


def _configure(channel: socket.socket) -> None:
    if channel.family != socket.AF_UNIX or channel.getsockopt(socket.SOL_SOCKET, socket.SO_TYPE) != socket.SOCK_SEQPACKET:
        raise ValueError("native supervision requires local sequenced packets")
    channel.setblocking(False)
    channel.set_inheritable(False)


def _send(channel: socket.socket, message: StrictV1Model) -> None:
    wire = canonical_bytes(message)
    if not 1 <= len(wire) <= _MAX_FRAME or channel.send(wire) != len(wire):
        raise ValueError("native supervision packet exceeds its bound")


def _receive(channel: socket.socket, models: Mapping[str, type[StrictV1Model]]) -> StrictV1Model:
    # No descriptor transfer is permitted. A zero ancillary buffer makes Linux
    # close excess SCM_RIGHTS FDs rather than installing them in this monitor.
    wire, ancillary, flags, _address = channel.recvmsg(_MAX_FRAME, 0, socket.MSG_CMSG_CLOEXEC)
    if not wire or ancillary or flags & (socket.MSG_TRUNC | socket.MSG_CTRUNC):
        raise ValueError("native supervision channel closed or packet changed")
    try:
        document = json.loads(wire)
        if not isinstance(document, dict) or not isinstance(document.get("kind"), str):
            raise ValueError("native supervision message kind is invalid")
        message = models[document["kind"]].model_validate_json(wire)
    except (KeyError, ValueError, RecursionError):
        raise ValueError("native supervision message is invalid") from None
    if canonical_bytes(message) != wire:
        raise ValueError("native supervision message is not canonical")
    return message


def _drain_authority(channel: socket.socket, guard: NativeExecutionDeadline) -> bool:
    """Observe queued revocation before progress without an unbounded drain.

    There is at most one outstanding request. After its permit, another frame
    can only stop execution: a second permit is unsolicited and fails closed.
    Thus two bounded reads resolve queued cancellation without starving expiry.
    """
    accepted = False
    for _ in range(2):
        guard.poll_deadline()
        try:
            message = _receive(channel, _AUTHORITY_MODELS)
        except BlockingIOError:
            return accepted
        if isinstance(message, NativeAuthorityStop):
            guard.stop(message.kind)
            return accepted
        if not isinstance(message, NativeAuthorityPermission):
            raise ValueError("native authority returned an invalid message")
        guard.accept(message.permit)
        accepted = True
    raise ValueError("native authority exceeded its response budget")


def supervise_native_execution(
    claim: BuildClaimRequestV1, *, source_binding_sha256: str, authority: socket.socket,
    broker_channel: socket.socket, broker_process: subprocess.Popen[bytes],
) -> NativeSupervisionResult:
    """Own the broker's lifetime while preserving the caller's IO channel.

    Helpers must already be started and verified before entering this loop.
    No subprocess starts, network calls, file reads/writes, runtime-state reads,
    or arbitrary callbacks run while permission is live. Only bounded local
    packets, BOOTTIME checks and nonblocking child observation are performed.
    Always kill/reap the exact broker child on exit; caller owns socket closure,
    IO helper teardown and separate runtime/artifact reconciliation afterward.
    """
    guard: NativeExecutionDeadline | None = None
    reason = "protocol-error"
    succeeded = False
    reaped = False
    try:
        _configure(authority)
        _configure(broker_channel)
        guard = NativeExecutionDeadline(claim, source_binding_sha256=source_binding_sha256)
        with selectors.DefaultSelector() as selector:
            selector.register(authority, selectors.EVENT_READ, "authority")
            selector.register(broker_channel, selectors.EVENT_READ, "broker")
            _send(authority, NativeAuthorityRequest(request=guard.begin_request()))
            pending = True
            phase = "await-permit"
            renew_at = 0

            def read_authority(latch: NativeExecutionDeadline) -> None:
                nonlocal pending, renew_at
                if _drain_authority(authority, latch) and latch.stopped_reason is None:
                    pending = False
                    until = latch.require_live()
                    observed = time.clock_gettime_ns(time.CLOCK_BOOTTIME)
                    renew_at = observed + max(1, (until - observed) // 2)

            while True:
                deadline = guard.poll_deadline()
                if broker_process.poll() is not None:
                    reason = "broker-failed"
                    break
                now = time.clock_gettime_ns(time.CLOCK_BOOTTIME)
                if not pending and now >= renew_at:
                    _send(authority, NativeAuthorityRequest(request=guard.begin_request()))
                    pending = True
                wake_at = deadline if pending else min(deadline, renew_at)
                events = selector.select(min(0.05, max(0, (wake_at - now) / 1_000_000_000)))
                # Drain the bounded authority exchange even when the selector
                # only observed the broker. A permit followed by cancellation
                # must not allow a start or a simultaneous success to win.
                read_authority(guard)
                if guard.stopped_reason is not None:
                    reason = guard.stopped_reason
                    break
                if phase == "await-permit" and not pending:
                    _send(broker_channel, NativeBrokerStart(role="pause", deadline_boottime_ns=guard.require_live()))
                    phase = "pause"
                for key, _mask in events:
                    if key.data != "broker":
                        continue
                    guard.poll_deadline()
                    event = _receive(broker_channel, _BROKER_MODELS)
                    if not isinstance(event, NativeBrokerEvent):
                        raise ValueError("native broker returned an invalid message")
                    read_authority(guard)
                    if guard.stopped_reason is not None:
                        reason = guard.stopped_reason
                        break
                    guard.require_live()
                    if event.kind == "failed":
                        reason = "broker-failed"
                        guard.stop("cleanup")
                        break
                    if phase == "pause" and event.kind == "pause-ready":
                        _send(broker_channel, NativeBrokerStart(role="buildkit", deadline_boottime_ns=guard.require_live()))
                        phase = "buildkit"
                    elif phase == "buildkit" and event.kind == "buildkit-ready":
                        _send(broker_channel, NativeBrokerStart(role="client", deadline_boottime_ns=guard.require_live()))
                        phase = "client"
                    elif phase == "client" and event.kind in {"client-succeeded", "client-failed"}:
                        succeeded = event.kind == "client-succeeded"
                        reason = "completed" if succeeded else "client-failed"
                        guard.stop("completed" if succeeded else "cleanup")
                        break
                    else:
                        raise ValueError("native broker lifecycle order changed")
                if guard.stopped_reason is not None:
                    break
    except (OSError, ValueError, RuntimeError):
        reason = (guard.stopped_reason if guard is not None else None) or "protocol-error"
        succeeded = False
    finally:
        if guard is not None:
            guard.stop("cleanup")
        # Popen owns this direct child and its unreaped PID; no arbitrary
        # numeric PID lookup, unrelated process or manual Slurm mutation.
        try:
            if broker_process.poll() is None:
                broker_process.kill()
            broker_process.wait(timeout=5)
            reaped = True
        except (OSError, subprocess.TimeoutExpired):
            reason = "cleanup-uncertain"
            succeeded = False
    return NativeSupervisionResult(succeeded, reason, reaped)
