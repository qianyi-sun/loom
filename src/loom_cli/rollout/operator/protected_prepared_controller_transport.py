"""Bounded command-channel adapters for prepared controller operations."""

from __future__ import annotations

import re
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import Protocol

from .protected_capacity_execution_preparation_component import (
    PreparedControllerEvidence,
    PreparedControllerRequest,
)
from .protected_controller_prerequisite_component import capacity_executor_image_digest
from .protected_controller_prerequisite_transport import (
    FixedOldlabControllerPrerequisiteInvoker,
)

_POOL_IDS = frozenset({"gb10", "oldlab"})
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_MAX_WIRE_BYTES = 2 * 1024 * 1024
_PREPARED_TIMER = "loom-capacity-pool-executor-prepared.timer"
_OLDLAB_DOCKER = "/usr/bin/docker"
_INSTALLER = "/opt/loom-capacity-executor-release/payload/installer/install_capacity_executor.py"
_PREPARED_OPERATIONS = frozenset(
    {
        "observe-prepared",
        "converge-prepared-files",
        "enable-prepared-timer",
        "run-prepared-tick",
        "disable-prepared-timer",
    }
)


class PreparedControllerCommandResult(Protocol):
    @property
    def returncode(self) -> int: ...

    @property
    def stdout(self) -> bytes | str: ...

    @property
    def stderr(self) -> bytes | str: ...


PreparedControllerInvoker = Callable[[str, bytes], PreparedControllerCommandResult]
OldlabPreparedControllerRunner = Callable[
    [Sequence[str], str],
    PreparedControllerCommandResult,
]


class GB10PreparedControllerChannel(Protocol):
    @property
    def controller_prerequisite_authority_sha256(self) -> str: ...

    def invoke_prepared_controller(
        self,
        operation: str,
        payload: bytes,
    ) -> PreparedControllerCommandResult: ...


@dataclass(frozen=True, slots=True)
class FixedPreparedControllerTransport:
    """Expose only prepared-file, timer, and tick operations on one channel."""

    pool_id: str
    authority_sha256: str
    invoke: PreparedControllerInvoker

    def __post_init__(self) -> None:
        if (
            self.pool_id not in _POOL_IDS
            or _SHA256_RE.fullmatch(self.authority_sha256) is None
            or not callable(self.invoke)
        ):
            raise ValueError("prepared controller transport authority is invalid")

    def observe(self, request: PreparedControllerRequest) -> PreparedControllerEvidence | None:
        return self._operation("observe-prepared", request, allow_absent=True)

    def converge_files(self, request: PreparedControllerRequest) -> PreparedControllerEvidence:
        evidence = self._operation("converge-prepared-files", request)
        assert evidence is not None
        return evidence

    def enable_timer(self, request: PreparedControllerRequest) -> PreparedControllerEvidence:
        evidence = self._operation("enable-prepared-timer", request)
        assert evidence is not None
        return evidence

    def run_tick(self, request: PreparedControllerRequest) -> PreparedControllerEvidence:
        evidence = self._operation("run-prepared-tick", request)
        assert evidence is not None
        return evidence

    def disable_timer(
        self,
        request: PreparedControllerRequest,
    ) -> PreparedControllerEvidence | None:
        return self._operation("disable-prepared-timer", request, allow_absent=True)

    def _operation(
        self,
        operation: str,
        request: PreparedControllerRequest,
        *,
        allow_absent: bool = False,
    ) -> PreparedControllerEvidence | None:
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
            raise RuntimeError("prepared controller operation failed safely")
        if stdout == b"null\n":
            if allow_absent:
                return None
            raise RuntimeError("prepared controller operation failed safely")
        try:
            evidence = PreparedControllerEvidence.from_bytes(stdout)
        except ValueError as exc:
            raise RuntimeError("prepared controller operation failed safely") from exc
        if (
            evidence.pool_id != self.pool_id
            or evidence.transport_authority_sha256 != self.authority_sha256
            or evidence.request_sha256 != request.request_sha256
            or not _evidence_matches_operation(evidence, operation)
        ):
            raise RuntimeError("prepared controller operation failed safely")
        return evidence

    def _validate_request(self, request: PreparedControllerRequest) -> None:
        if (
            not isinstance(request, PreparedControllerRequest)
            or request.pool_id != self.pool_id
            or request.transport_authority_sha256 != self.authority_sha256
        ):
            raise ValueError("prepared controller transport binding is invalid")


@dataclass(frozen=True, slots=True)
class FixedGB10PreparedControllerTransport:
    """Revalidate the fixed GB10 forced-SSH channel before every operation."""

    controller: GB10PreparedControllerChannel

    def __post_init__(self) -> None:
        if not callable(getattr(self.controller, "invoke_prepared_controller", None)):
            raise ValueError("GB10 prepared controller channel is invalid")
        _require_authority_sha256(self.authority_sha256)

    @property
    def authority_sha256(self) -> str:
        return _require_authority_sha256(self.controller.controller_prerequisite_authority_sha256)

    def observe(self, request: PreparedControllerRequest) -> PreparedControllerEvidence | None:
        return self._transport().observe(request)

    def converge_files(self, request: PreparedControllerRequest) -> PreparedControllerEvidence:
        return self._transport().converge_files(request)

    def enable_timer(self, request: PreparedControllerRequest) -> PreparedControllerEvidence:
        return self._transport().enable_timer(request)

    def run_tick(self, request: PreparedControllerRequest) -> PreparedControllerEvidence:
        return self._transport().run_tick(request)

    def disable_timer(
        self,
        request: PreparedControllerRequest,
    ) -> PreparedControllerEvidence | None:
        return self._transport().disable_timer(request)

    def _transport(self) -> FixedPreparedControllerTransport:
        return FixedPreparedControllerTransport(
            pool_id="gb10",
            authority_sha256=self.authority_sha256,
            invoke=self.controller.invoke_prepared_controller,
        )


@dataclass(frozen=True, slots=True)
class FixedOldlabPreparedControllerInvoker:
    """Run prepared operations in OLDLAB1's host namespaces using the bound image."""

    run: OldlabPreparedControllerRunner
    image: str

    def __post_init__(self) -> None:
        try:
            capacity_executor_image_digest(self.image)
        except ValueError as exc:
            raise ValueError("OLDLAB prepared controller channel is invalid") from exc
        if not callable(self.run):
            raise ValueError("OLDLAB prepared controller channel is invalid")

    @property
    def authority_sha256(self) -> str:
        return FixedOldlabControllerPrerequisiteInvoker(
            run=self.run,
            image=self.image,
        ).authority_sha256

    def __call__(self, operation: str, payload: bytes) -> PreparedControllerCommandResult:
        if operation not in _PREPARED_OPERATIONS:
            raise ValueError("OLDLAB prepared controller operation is invalid")
        try:
            request = PreparedControllerRequest.from_bytes(payload)
            input_payload = payload.decode("ascii")
        except (UnicodeDecodeError, ValueError) as exc:
            raise ValueError("OLDLAB prepared controller request is invalid") from exc
        if (
            request.pool_id != "oldlab"
            or request.transport_authority_sha256 != self.authority_sha256
            or request.prerequisite.image != self.image
        ):
            raise ValueError("OLDLAB prepared controller request is invalid")
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
            self.image,
            _INSTALLER,
            "--host-root",
            "/host",
            "--operation",
            operation,
        )
        return self.run(argv, input_payload)


def build_fixed_oldlab_prepared_controller_transport(
    *,
    run: OldlabPreparedControllerRunner,
    image: str,
) -> FixedPreparedControllerTransport:
    invoker = FixedOldlabPreparedControllerInvoker(run=run, image=image)
    return FixedPreparedControllerTransport(
        pool_id="oldlab",
        authority_sha256=invoker.authority_sha256,
        invoke=invoker,
    )


def build_fixed_gb10_prepared_controller_transport(
    *,
    controller: GB10PreparedControllerChannel,
) -> FixedGB10PreparedControllerTransport:
    return FixedGB10PreparedControllerTransport(controller=controller)


def _bytes(value: bytes | str) -> bytes:
    if isinstance(value, bytes):
        return value
    if isinstance(value, str):
        try:
            return value.encode("ascii")
        except UnicodeEncodeError as exc:
            raise RuntimeError("prepared controller operation failed safely") from exc
    raise RuntimeError("prepared controller operation failed safely")


def _require_authority_sha256(value: object) -> str:
    if not isinstance(value, str) or _SHA256_RE.fullmatch(value) is None:
        raise ValueError("prepared controller channel authority is invalid")
    return value


def _evidence_matches_operation(
    evidence: PreparedControllerEvidence,
    operation: str,
) -> bool:
    if operation == "observe-prepared":
        return True
    timer_state = (
        evidence.unit_active_state[_PREPARED_TIMER],
        evidence.unit_file_state[_PREPARED_TIMER],
    )
    if operation in {"converge-prepared-files", "disable-prepared-timer"}:
        return timer_state == ("inactive", "disabled") and not evidence.successful_tick
    if operation == "enable-prepared-timer":
        return timer_state == ("active", "enabled")
    if operation == "run-prepared-tick":
        return timer_state == ("active", "enabled") and evidence.successful_tick
    return False


__all__ = [
    "FixedGB10PreparedControllerTransport",
    "FixedOldlabPreparedControllerInvoker",
    "FixedPreparedControllerTransport",
    "GB10PreparedControllerChannel",
    "OldlabPreparedControllerRunner",
    "PreparedControllerCommandResult",
    "PreparedControllerInvoker",
    "build_fixed_gb10_prepared_controller_transport",
    "build_fixed_oldlab_prepared_controller_transport",
]
