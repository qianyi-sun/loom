"""Bounded command-channel adapter for inert controller prerequisites."""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from typing import Protocol

from .protected_controller_discovery import (
    ControllerDiscoveryEvidence,
    ControllerDiscoveryRequest,
)
from .protected_controller_prerequisite_component import (
    ControllerPrerequisiteEvidence,
    ControllerPrerequisiteRequest,
    capacity_executor_image_digest,
)

_POOL_IDS = frozenset({"gb10", "oldlab"})
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_MAX_WIRE_BYTES = 2 * 1024 * 1024
_OLDLAB_DOCKER = "/usr/bin/docker"
_INSTALLER = "/opt/loom-capacity-executor-release/payload/installer/install_capacity_executor.py"
_OLDLAB_LOGICAL_IMAGE_PREFIX = "192.168.50.13:5000/loom-capacity-executor@sha256:"
_OLDLAB_RUNTIME_IMAGE_PREFIX = "localhost:5000/loom-capacity-executor@sha256:"


class ControllerCommandResult(Protocol):
    @property
    def returncode(self) -> int: ...

    @property
    def stdout(self) -> bytes | str: ...

    @property
    def stderr(self) -> bytes | str: ...


ControllerPrerequisiteInvoker = Callable[[str, bytes], ControllerCommandResult]
OldlabControllerRunner = Callable[[Sequence[str], str], ControllerCommandResult]


class GB10ControllerPrerequisiteChannel(Protocol):
    @property
    def controller_prerequisite_authority_sha256(self) -> str: ...

    def invoke_controller_prerequisite(
        self,
        operation: str,
        payload: bytes,
    ) -> ControllerCommandResult: ...


@dataclass(frozen=True, slots=True)
class FixedControllerPrerequisiteTransport:
    """Expose only observe/converge over one pre-authenticated controller channel."""

    pool_id: str
    authority_sha256: str
    invoke: ControllerPrerequisiteInvoker

    def __post_init__(self) -> None:
        if (
            self.pool_id not in _POOL_IDS
            or _SHA256_RE.fullmatch(self.authority_sha256) is None
            or not callable(self.invoke)
        ):
            raise ValueError("controller prerequisite transport authority is invalid")

    def observe(
        self,
        request: ControllerPrerequisiteRequest,
    ) -> ControllerPrerequisiteEvidence | None:
        self._validate_request(request)
        payload = self._invoke("observe-prerequisite", request.to_bytes())
        if payload == b"null\n":
            return None
        return self._decode_evidence(payload)

    def discover(self, request: ControllerDiscoveryRequest) -> ControllerDiscoveryEvidence:
        if (
            not isinstance(request, ControllerDiscoveryRequest)
            or request.pool_id != self.pool_id
            or request.transport_authority_sha256 != self.authority_sha256
        ):
            raise ValueError("controller discovery transport binding is invalid")
        payload = self._invoke("discover-controller", request.to_bytes())
        try:
            evidence = ControllerDiscoveryEvidence.from_bytes(payload)
        except ValueError as exc:
            raise RuntimeError("controller discovery operation failed safely") from exc
        if (
            evidence.pool_id != self.pool_id
            or evidence.transport_authority_sha256 != self.authority_sha256
        ):
            raise RuntimeError("controller discovery operation failed safely")
        return evidence

    def converge(
        self,
        request: ControllerPrerequisiteRequest,
    ) -> ControllerPrerequisiteEvidence:
        self._validate_request(request)
        payload = self._invoke("converge-prerequisite", request.to_bytes())
        if payload == b"null\n":
            raise RuntimeError("controller prerequisite operation failed safely")
        return self._decode_evidence(payload)

    def _validate_request(self, request: ControllerPrerequisiteRequest) -> None:
        if (
            not isinstance(request, ControllerPrerequisiteRequest)
            or request.pool_id != self.pool_id
            or request.transport_authority_sha256 != self.authority_sha256
        ):
            raise ValueError("controller prerequisite transport binding is invalid")

    def _invoke(self, operation: str, payload: bytes) -> bytes:
        result = self.invoke(operation, payload)
        stdout = _bytes(result.stdout)
        stderr = _bytes(result.stderr)
        if (
            type(result.returncode) is not int
            or result.returncode != 0
            or stderr
            or not 0 < len(stdout) <= _MAX_WIRE_BYTES
        ):
            raise RuntimeError("controller prerequisite operation failed safely")
        return stdout

    def _decode_evidence(self, payload: bytes) -> ControllerPrerequisiteEvidence:
        try:
            evidence = ControllerPrerequisiteEvidence.from_bytes(payload)
        except ValueError as exc:
            raise RuntimeError("controller prerequisite operation failed safely") from exc
        if (
            evidence.pool_id != self.pool_id
            or evidence.transport_authority_sha256 != self.authority_sha256
        ):
            raise RuntimeError("controller prerequisite operation failed safely")
        return evidence


@dataclass(frozen=True, slots=True)
class FixedGB10ControllerPrerequisiteTransport:
    """Revalidate the fixed GB10 forced-SSH authority for every operation."""

    controller: GB10ControllerPrerequisiteChannel

    def __post_init__(self) -> None:
        if not callable(getattr(self.controller, "invoke_controller_prerequisite", None)):
            raise ValueError("GB10 controller prerequisite channel is invalid")
        _require_authority_sha256(self.authority_sha256)

    @property
    def authority_sha256(self) -> str:
        return _require_authority_sha256(self.controller.controller_prerequisite_authority_sha256)

    def observe(
        self,
        request: ControllerPrerequisiteRequest,
    ) -> ControllerPrerequisiteEvidence | None:
        return self._transport().observe(request)

    def discover(self, request: ControllerDiscoveryRequest) -> ControllerDiscoveryEvidence:
        return self._transport().discover(request)

    def converge(
        self,
        request: ControllerPrerequisiteRequest,
    ) -> ControllerPrerequisiteEvidence:
        return self._transport().converge(request)

    def _transport(self) -> FixedControllerPrerequisiteTransport:
        return FixedControllerPrerequisiteTransport(
            pool_id="gb10",
            authority_sha256=self.authority_sha256,
            invoke=self.controller.invoke_controller_prerequisite,
        )


@dataclass(frozen=True, slots=True)
class FixedOldlabControllerPrerequisiteInvoker:
    """Run the exact release-contained installer in OLDLAB1's host namespaces."""

    run: OldlabControllerRunner
    image: str
    runtime_image: str = field(init=False)

    def __post_init__(self) -> None:
        try:
            runtime_image = oldlab_runtime_executor_image(self.image)
        except ValueError as exc:
            raise ValueError("OLDLAB controller prerequisite channel is invalid") from exc
        if not callable(self.run):
            raise ValueError("OLDLAB controller prerequisite channel is invalid")
        object.__setattr__(self, "runtime_image", runtime_image)

    @property
    def authority_sha256(self) -> str:
        value = {
            "channel": "docker-host-pid-v1",
            "controller_hostname": "TRT-EAI-OLDLAB-1",
            "docker": _OLDLAB_DOCKER,
            "installer": _INSTALLER,
            "image": self.image,
            "pool_id": "oldlab",
            "runtime_image": self.runtime_image,
            "schema_version": 2,
        }
        return hashlib.sha256(
            json.dumps(value, sort_keys=True, separators=(",", ":")).encode("ascii")
        ).hexdigest()

    def __call__(self, operation: str, payload: bytes) -> ControllerCommandResult:
        if operation not in {
            "discover-controller",
            "observe-prerequisite",
            "converge-prerequisite",
        }:
            raise ValueError("OLDLAB controller prerequisite operation is invalid")
        try:
            request: ControllerDiscoveryRequest | ControllerPrerequisiteRequest
            if operation == "discover-controller":
                request = ControllerDiscoveryRequest.from_bytes(payload)
            else:
                request = ControllerPrerequisiteRequest.from_bytes(payload)
            input_payload = payload.decode("ascii")
        except (UnicodeDecodeError, ValueError) as exc:
            raise ValueError("OLDLAB controller prerequisite request is invalid") from exc
        if (
            request.pool_id != "oldlab"
            or request.transport_authority_sha256 != self.authority_sha256
            or (isinstance(request, ControllerPrerequisiteRequest) and request.image != self.image)
        ):
            raise ValueError("OLDLAB controller prerequisite request is invalid")
        runtime_arguments = (
            ("--runtime-image", self.runtime_image)
            if isinstance(request, ControllerPrerequisiteRequest)
            else ()
        )
        argv = (
            _OLDLAB_DOCKER,
            "run",
            "--rm",
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
            *runtime_arguments,
            "--operation",
            operation,
        )
        return self.run(argv, input_payload)


def build_fixed_oldlab_controller_prerequisite_transport(
    *,
    run: OldlabControllerRunner,
    image: str,
) -> FixedControllerPrerequisiteTransport:
    invoker = FixedOldlabControllerPrerequisiteInvoker(run=run, image=image)
    return FixedControllerPrerequisiteTransport(
        pool_id="oldlab",
        authority_sha256=invoker.authority_sha256,
        invoke=invoker,
    )


def build_fixed_gb10_controller_prerequisite_transport(
    *,
    controller: GB10ControllerPrerequisiteChannel,
) -> FixedGB10ControllerPrerequisiteTransport:
    return FixedGB10ControllerPrerequisiteTransport(controller=controller)


def _require_authority_sha256(value: object) -> str:
    if not isinstance(value, str) or _SHA256_RE.fullmatch(value) is None:
        raise ValueError("GB10 controller prerequisite channel is invalid")
    return value


def oldlab_runtime_executor_image(image: object) -> str:
    """Map the staging logical executor identity to OLDLAB1's host-local mirror."""

    digest = capacity_executor_image_digest(image)
    if image != f"{_OLDLAB_LOGICAL_IMAGE_PREFIX}{digest}":
        raise ValueError("OLDLAB logical executor image is invalid")
    runtime_image = f"{_OLDLAB_RUNTIME_IMAGE_PREFIX}{digest}"
    if oldlab_runtime_executor_image_digest(runtime_image) != digest:  # pragma: no cover
        raise ValueError("OLDLAB runtime executor image is invalid")
    return runtime_image


def oldlab_runtime_executor_image_digest(image: object) -> str:
    """Validate one exact OLDLAB1 host-local executor image reference."""

    digest = capacity_executor_image_digest(image)
    if image != f"{_OLDLAB_RUNTIME_IMAGE_PREFIX}{digest}":
        raise ValueError("OLDLAB runtime executor image is invalid")
    return digest


def _bytes(value: bytes | str) -> bytes:
    if isinstance(value, bytes):
        return value
    if isinstance(value, str):
        try:
            return value.encode("ascii")
        except UnicodeEncodeError as exc:
            raise RuntimeError("controller prerequisite operation failed safely") from exc
    raise RuntimeError("controller prerequisite operation failed safely")


__all__ = [
    "ControllerCommandResult",
    "ControllerPrerequisiteInvoker",
    "FixedControllerPrerequisiteTransport",
    "FixedGB10ControllerPrerequisiteTransport",
    "FixedOldlabControllerPrerequisiteInvoker",
    "GB10ControllerPrerequisiteChannel",
    "OldlabControllerRunner",
    "build_fixed_gb10_controller_prerequisite_transport",
    "build_fixed_oldlab_controller_prerequisite_transport",
    "oldlab_runtime_executor_image",
    "oldlab_runtime_executor_image_digest",
]
