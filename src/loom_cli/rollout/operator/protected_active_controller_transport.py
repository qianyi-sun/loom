"""Bounded command-channel adapters for active controller operations."""

from __future__ import annotations

import hashlib
import re
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from typing import Protocol

from .protected_active_controller import (
    ActiveControllerEvidence,
    ActiveControllerRequest,
)
from .protected_controller_prerequisite_component import capacity_executor_image_digest
from .protected_controller_prerequisite_transport import (
    FixedOldlabControllerPrerequisiteInvoker,
)

_POOL_IDS = frozenset({"gb10", "oldlab"})
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_MAX_WIRE_BYTES = 2 * 1024 * 1024
_OLDLAB_DOCKER = "/usr/bin/docker"
_INSTALLER = "/opt/loom-capacity-executor-release/payload/installer/install_capacity_executor.py"
_ACTIVE_OPERATIONS = frozenset(
    {
        "observe-active",
        "converge-active-files",
        "enable-active-timer",
    }
)


class ActiveControllerCommandResult(Protocol):
    @property
    def returncode(self) -> int: ...

    @property
    def stdout(self) -> bytes | str: ...

    @property
    def stderr(self) -> bytes | str: ...


ActiveControllerInvoker = Callable[[str, bytes], ActiveControllerCommandResult]
OldlabActiveControllerRunner = Callable[
    [Sequence[str], str],
    ActiveControllerCommandResult,
]


class GB10ActiveControllerChannel(Protocol):
    @property
    def controller_prerequisite_authority_sha256(self) -> str: ...

    def invoke_active_controller(
        self,
        operation: str,
        payload: bytes,
    ) -> ActiveControllerCommandResult: ...


@dataclass(frozen=True, slots=True)
class FixedActiveControllerTransport:
    """Expose only active-file publication and timer enable operations on one channel."""

    pool_id: str
    authority_sha256: str
    invoke: ActiveControllerInvoker

    def __post_init__(self) -> None:
        if (
            self.pool_id not in _POOL_IDS
            or _SHA256_RE.fullmatch(self.authority_sha256) is None
            or not callable(self.invoke)
        ):
            raise ValueError("active controller transport authority is invalid")

    def observe(self, request: ActiveControllerRequest) -> ActiveControllerEvidence | None:
        return self._operation("observe-active", request, allow_absent=True)

    def converge_files(self, request: ActiveControllerRequest) -> ActiveControllerEvidence:
        evidence = self._operation("converge-active-files", request)
        assert evidence is not None
        return evidence

    def enable_timer(self, request: ActiveControllerRequest) -> ActiveControllerEvidence:
        evidence = self._operation("enable-active-timer", request)
        assert evidence is not None
        return evidence

    def _operation(
        self,
        operation: str,
        request: ActiveControllerRequest,
        *,
        allow_absent: bool = False,
    ) -> ActiveControllerEvidence | None:
        self._validate_request(request)
        result = self.invoke(operation, request.to_bytes())
        stdout = _bytes(result.stdout)
        stderr = _bytes(result.stderr)
        if (
            type(result.returncode) is not int
            or result.returncode != 0
            or stderr
            or not 0 < len(stdout) <= _MAX_WIRE_BYTES
        ):
            raise RuntimeError("active controller operation failed safely")
        if stdout == b"null\n":
            if allow_absent:
                return None
            raise RuntimeError("active controller operation failed safely")
        try:
            evidence = ActiveControllerEvidence.from_bytes(stdout)
        except ValueError as exc:
            raise RuntimeError("active controller operation failed safely") from exc
        if (
            evidence.operation_id != request.operation_id
            or dict(evidence.file_sha256)
            != {
                path: hashlib.sha256(payload).hexdigest() for path, payload in request.files.items()
            }
            or evidence.pool_id != self.pool_id
            or evidence.transport_authority_sha256 != self.authority_sha256
            or evidence.request_sha256 != request.request_sha256
            or not _evidence_matches_operation(evidence, operation)
        ):
            raise RuntimeError("active controller operation failed safely")
        return evidence

    def _validate_request(self, request: ActiveControllerRequest) -> None:
        if (
            not isinstance(request, ActiveControllerRequest)
            or request.pool_id != self.pool_id
            or request.transport_authority_sha256 != self.authority_sha256
        ):
            raise ValueError("active controller transport binding is invalid")


@dataclass(frozen=True, slots=True)
class FixedGB10ActiveControllerTransport:
    """Revalidate the fixed GB10 forced-SSH channel before every operation."""

    controller: GB10ActiveControllerChannel

    def __post_init__(self) -> None:
        if not callable(getattr(self.controller, "invoke_active_controller", None)):
            raise ValueError("GB10 active controller channel is invalid")
        _require_authority_sha256(self.authority_sha256)

    @property
    def authority_sha256(self) -> str:
        return _require_authority_sha256(self.controller.controller_prerequisite_authority_sha256)

    def observe(self, request: ActiveControllerRequest) -> ActiveControllerEvidence | None:
        return self._transport().observe(request)

    def converge_files(self, request: ActiveControllerRequest) -> ActiveControllerEvidence:
        return self._transport().converge_files(request)

    def enable_timer(self, request: ActiveControllerRequest) -> ActiveControllerEvidence:
        return self._transport().enable_timer(request)

    def _transport(self) -> FixedActiveControllerTransport:
        return FixedActiveControllerTransport(
            pool_id="gb10",
            authority_sha256=self.authority_sha256,
            invoke=self.controller.invoke_active_controller,
        )


@dataclass(frozen=True, slots=True)
class FixedOldlabActiveControllerInvoker:
    """Run active operations in OLDLAB1's host namespaces using the bound image."""

    run: OldlabActiveControllerRunner
    image: str
    runtime_image: str = field(init=False)

    def __post_init__(self) -> None:
        try:
            capacity_executor_image_digest(self.image)
            prerequisite_invoker = FixedOldlabControllerPrerequisiteInvoker(
                run=self.run,
                image=self.image,
            )
        except ValueError as exc:
            raise ValueError("OLDLAB active controller channel is invalid") from exc
        if not callable(self.run):
            raise ValueError("OLDLAB active controller channel is invalid")
        object.__setattr__(self, "runtime_image", prerequisite_invoker.runtime_image)

    @property
    def authority_sha256(self) -> str:
        return FixedOldlabControllerPrerequisiteInvoker(
            run=self.run,
            image=self.image,
        ).authority_sha256

    def __call__(self, operation: str, payload: bytes) -> ActiveControllerCommandResult:
        if operation not in _ACTIVE_OPERATIONS:
            raise ValueError("OLDLAB active controller operation is invalid")
        try:
            request = ActiveControllerRequest.from_bytes(payload)
            input_payload = payload.decode("ascii")
        except (UnicodeDecodeError, ValueError) as exc:
            raise ValueError("OLDLAB active controller request is invalid") from exc
        if (
            request.pool_id != "oldlab"
            or request.transport_authority_sha256 != self.authority_sha256
            or request.prepared.prerequisite.image != self.image
        ):
            raise ValueError("OLDLAB active controller request is invalid")
        argv = (
            _OLDLAB_DOCKER,
            "run",
            "--rm",
            "--interactive",
            "--user",
            "0:0",
            "--privileged",
            "--pid=host",
            "--network=none",
            "--read-only",
            "--tmpfs",
            "/tmp:rw,nosuid,nodev,noexec,size=64m,mode=0700",
            "--mount",
            "type=bind,src=/,dst=/host,bind-propagation=rslave",
            "--entrypoint",
            "/usr/local/bin/python",
            self.runtime_image,
            _INSTALLER,
            "--host-root",
            "/host",
            "--operation",
            operation,
        )
        return self.run(argv, input_payload)


def build_fixed_oldlab_active_controller_transport(
    *,
    run: OldlabActiveControllerRunner,
    image: str,
) -> FixedActiveControllerTransport:
    invoker = FixedOldlabActiveControllerInvoker(run=run, image=image)
    return FixedActiveControllerTransport(
        pool_id="oldlab",
        authority_sha256=invoker.authority_sha256,
        invoke=invoker,
    )


def build_fixed_gb10_active_controller_transport(
    *,
    controller: GB10ActiveControllerChannel,
) -> FixedGB10ActiveControllerTransport:
    return FixedGB10ActiveControllerTransport(controller=controller)


def _bytes(value: bytes | str) -> bytes:
    if isinstance(value, bytes):
        return value
    if isinstance(value, str):
        try:
            return value.encode("ascii")
        except UnicodeEncodeError as exc:
            raise RuntimeError("active controller operation failed safely") from exc
    raise RuntimeError("active controller operation failed safely")


def _require_authority_sha256(value: object) -> str:
    if not isinstance(value, str) or _SHA256_RE.fullmatch(value) is None:
        raise ValueError("active controller channel authority is invalid")
    return value


def _evidence_matches_operation(evidence: ActiveControllerEvidence, operation: str) -> bool:
    return (
        operation == "observe-active"
        or (operation == "converge-active-files" and evidence.state == "staged")
        or (operation == "enable-active-timer" and evidence.state == "active")
    )


__all__ = [
    "ActiveControllerCommandResult",
    "ActiveControllerInvoker",
    "FixedActiveControllerTransport",
    "FixedGB10ActiveControllerTransport",
    "FixedOldlabActiveControllerInvoker",
    "GB10ActiveControllerChannel",
    "OldlabActiveControllerRunner",
    "build_fixed_gb10_active_controller_transport",
    "build_fixed_oldlab_active_controller_transport",
]
