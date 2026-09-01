"""Constrained systemd user-manager boundary for detached rollout attempts."""

from __future__ import annotations

import hashlib
import json
import os
import re
import secrets
import shlex
import subprocess
import time
from collections.abc import Callable, Generator, Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Literal, Protocol, cast

from ..systemd_readiness import parse_systemctl_properties
from .config import OperatorConfig
from .model import validate_safe_identifier
from .policy import sanitized_child_environment
from .staging_mutation_guard import (
    MUTATION_GUARD_NORMAL_RELEASE_BOUND_SECONDS,
    MUTATION_GUARD_RUNTIME_SECONDS,
    MUTATION_GUARD_SYSTEMD_COMMAND_TIMEOUT_SECONDS,
    MutationGuardError,
    MutationGuardEvidence,
    guard_evidence_path,
    read_mutation_guard_evidence,
    validate_mutation_guard_generation,
)


class CommandResult(Protocol):
    """Captured result returned by the injected command runner."""

    @property
    def returncode(self) -> int: ...

    @property
    def stdout(self) -> str: ...

    @property
    def stderr(self) -> str: ...


CommandRunner = Callable[[list[str]], CommandResult]


class JournalLineStream(Protocol):
    """Closeable line iterator owned by one follow-mode journal command."""

    def __iter__(self) -> Iterator[str]: ...

    def __next__(self) -> str: ...

    def close(self) -> None: ...


JournalStreamRunner = Callable[[list[str]], JournalLineStream]


class UnitLaunchError(RuntimeError):
    """Raised when an approved transient unit cannot be launched safely."""


class SystemdQueryError(RuntimeError):
    """Raised when an exact unit status cannot be obtained safely."""


class SystemdOperationError(RuntimeError):
    """Raised when an approved unit control operation fails safely."""


ActiveState = Literal[
    "activating",
    "active",
    "reloading",
    "deactivating",
    "inactive",
    "failed",
]
_ACTIVE_STATES = frozenset(
    {"activating", "active", "reloading", "deactivating", "inactive", "failed"}
)
_STATUS_TOKEN_RE = re.compile(r"^[a-z0-9-]+$")
_RESULT_TOKEN_RE = re.compile(r"^[a-z0-9-]*$")
_SHA_RE = re.compile(r"^[0-9a-f]{40}$")
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_TRANSIENT_UNIT_RE = re.compile(
    r"(?:loom-staging-backup-[a-z0-9][a-z0-9-]{7,79}"
    r"|loom-staging-mutation-guard-[a-z0-9][a-z0-9-]{7,79}"
    r"|loom-staging-rollout-[a-z0-9][a-z0-9-]{7,79}-[1-9][0-9]*"
    r"|loom-preflight-lifecycle-[0-9a-f]{16})[.]service\Z"
)
_MUTATION_GUARD_READINESS_TIMEOUT_SECONDS = 1_500
_MUTATION_GUARD_STOP_WAIT_SECONDS = 60
MUTATION_GUARD_STOP_POST_BOUND_SECONDS = 3 * MUTATION_GUARD_SYSTEMD_COMMAND_TIMEOUT_SECONDS
MUTATION_GUARD_SERVICE_STOP_TIMEOUT_SECONDS = MUTATION_GUARD_NORMAL_RELEASE_BOUND_SECONDS + 1
MUTATION_GUARD_CLIENT_OPERATION_TIMEOUT_SECONDS = (
    MUTATION_GUARD_SERVICE_STOP_TIMEOUT_SECONDS + MUTATION_GUARD_STOP_POST_BOUND_SECONDS + 1
)
_SHOW_PROPERTIES = (
    "LoadState",
    "ActiveState",
    "SubState",
    "Result",
    "ExecMainStatus",
    "MainPID",
    "ExecMainStartTimestamp",
    "ExecMainExitTimestamp",
    "ExecMainStartTimestampMonotonic",
    "ExecMainExitTimestampMonotonic",
)


def _new_mutation_guard_generation() -> str:
    return secrets.token_hex(16)


def _guard_candidate_identity(
    candidate_sha: str | None,
    candidate_tree: str | None,
    *,
    error_type: type[UnitLaunchError] | type[SystemdOperationError],
) -> tuple[str, str] | None:
    if candidate_sha is None and candidate_tree is None:
        return None
    if (
        not isinstance(candidate_sha, str)
        or _SHA_RE.fullmatch(candidate_sha) is None
        or not isinstance(candidate_tree, str)
        or _SHA_RE.fullmatch(candidate_tree) is None
    ):
        raise error_type("mutation guard candidate identity is invalid")
    return candidate_sha, candidate_tree


def _same_mutation_guard_acquisition(
    ready: MutationGuardEvidence,
    released: MutationGuardEvidence,
) -> bool:
    return (
        ready.request_id,
        ready.candidate_sha,
        ready.candidate_tree,
        ready.generation,
        ready.mutation_epoch,
        ready.guard_pid,
        ready.database_backend_pid,
        ready.deadline_unix_seconds,
        ready.cronjob_uid,
        ready.suspended_resource_version,
    ) == (
        released.request_id,
        released.candidate_sha,
        released.candidate_tree,
        released.generation,
        released.mutation_epoch,
        released.guard_pid,
        released.database_backend_pid,
        released.deadline_unix_seconds,
        released.cronjob_uid,
        released.suspended_resource_version,
    )


@dataclass(frozen=True, slots=True)
class SystemdUnitStatus:
    """Strict, allowlisted status for one known rollout attempt unit."""

    unit_name: str
    active_state: ActiveState
    sub_state: str
    result: str
    exec_main_status: int
    main_pid: int
    exec_main_start_timestamp: str | None
    exec_main_exit_timestamp: str | None
    exec_main_start_timestamp_monotonic: int
    exec_main_exit_timestamp_monotonic: int

    @property
    def is_running(self) -> bool:
        return self.active_state in {"activating", "active", "reloading", "deactivating"}

    @property
    def is_cleanly_inactive(self) -> bool:
        return (
            self.active_state == "inactive"
            and self.sub_state == "dead"
            and self.result == "success"
            and self.exec_main_status == 0
            and self.main_pid == 0
            and 0
            < self.exec_main_start_timestamp_monotonic
            <= self.exec_main_exit_timestamp_monotonic
        )


@dataclass(frozen=True, slots=True)
class SystemdLaunchCancelEvidence:
    """Bounded evidence from one exact pre-request transient unit round trip."""

    ready: bool
    launched: bool
    cancelled: bool
    unit_absent: bool
    launch_latency_ms: int
    cancel_latency_ms: int
    latency_budget_ms: int
    evidence_digest: str


def transient_service_argv(
    *,
    unit_name: str,
    working_directory: Path,
    command: tuple[str, ...],
) -> list[str]:
    """Build the shared transient-unit prefix used by preflight and launch."""
    if (
        _TRANSIENT_UNIT_RE.fullmatch(unit_name) is None
        or not working_directory.is_absolute()
        or ".." in working_directory.parts
        or not command
        or not command[0].startswith("/")
        or any(not item or "\x00" in item for item in command)
    ):
        raise UnitLaunchError("transient service authority is invalid")
    return [
        "systemd-run",
        "--user",
        "--collect",
        "--service-type=exec",
        "--unit",
        unit_name,
        "--property",
        "UMask=0077",
        "--property",
        f"WorkingDirectory={working_directory}",
        *command,
    ]


def probe_transient_launch_cancel(
    run: CommandRunner,
    *,
    candidate_sha: str,
    working_directory: Path,
    latency_budget_ms: int = 10_000,
    monotonic: Callable[[], float] = time.monotonic,
) -> SystemdLaunchCancelEvidence:
    """Launch and cancel one isolated unit before any rollout request exists."""
    if (
        _SHA_RE.fullmatch(candidate_sha) is None
        or not working_directory.is_absolute()
        or ".." in working_directory.parts
        or not 1_000 <= latency_budget_ms <= 60_000
    ):
        raise ValueError("transient launch/cancel probe authority is invalid")
    identity = hashlib.sha256(f"{candidate_sha}\0{working_directory}".encode()).hexdigest()[:16]
    unit_name = f"loom-preflight-lifecycle-{identity}.service"
    launched = False
    launch_attempted = False
    cancelled = False
    unit_absent = False
    launch_latency_ms = 0
    cancel_latency_ms = 0

    def command_output_is_safe(result: CommandResult) -> bool:
        return (
            isinstance(result.returncode, int)
            and isinstance(result.stdout, str)
            and isinstance(result.stderr, str)
            and "\x00" not in result.stdout
            and "\x00" not in result.stderr
            and len(result.stdout.encode("utf-8")) <= 1024 * 1024
            and len(result.stderr.encode("utf-8")) <= 1024 * 1024
        )

    def load_state() -> str | None:
        result = run(
            [
                "systemctl",
                "--user",
                "show",
                unit_name,
                "--property=LoadState",
                "--value",
            ]
        )
        if not command_output_is_safe(result) or result.returncode not in {0, 4}:
            return None
        value = result.stdout.strip()
        if result.returncode == 4 and not value:
            return "not-found"
        return value if value in {"loaded", "not-found"} else None

    try:
        if load_state() != "not-found":
            raise UnitLaunchError("preflight transient unit identity is already occupied")
        started = monotonic()
        launch_attempted = True
        result = run(
            transient_service_argv(
                unit_name=unit_name,
                working_directory=working_directory,
                command=("/usr/bin/sleep", "300"),
            )
        )
        launch_latency_ms = max(0, round((monotonic() - started) * 1000))
        launched = (
            command_output_is_safe(result)
            and result.returncode == 0
            and launch_latency_ms <= latency_budget_ms
        )
        if not launched:
            raise UnitLaunchError("preflight transient unit launch failed")
        status = run(
            [
                "systemctl",
                "--user",
                "show",
                unit_name,
                "--property=LoadState",
                "--property=ActiveState",
                "--property=SubState",
                "--property=Transient",
                "--property=MainPID",
            ]
        )
        if not command_output_is_safe(status):
            raise UnitLaunchError("preflight transient unit readback failed")
        properties = parse_systemctl_properties(status.stdout)
        if (
            status.returncode != 0
            or properties.get("LoadState") != "loaded"
            or properties.get("ActiveState") != "active"
            or properties.get("SubState") != "running"
            or properties.get("Transient") != "yes"
            or not properties.get("MainPID", "").isdigit()
            or int(properties["MainPID"]) <= 0
        ):
            raise UnitLaunchError("preflight transient unit readback failed")
        started = monotonic()
        stopped = run(["systemctl", "--user", "stop", unit_name])
        reset = run(["systemctl", "--user", "reset-failed", unit_name])
        cancel_latency_ms = max(0, round((monotonic() - started) * 1000))
        unit_absent = load_state() == "not-found"
        cancelled = (
            command_output_is_safe(stopped)
            and command_output_is_safe(reset)
            and stopped.returncode == 0
            and reset.returncode in {0, 1}
            and cancel_latency_ms <= latency_budget_ms
            and unit_absent
        )
        if not cancelled:
            raise UnitLaunchError("preflight transient unit cancellation failed")
        if not unit_absent:
            raise UnitLaunchError("preflight transient unit cleanup failed")
    finally:
        if launch_attempted and not unit_absent:
            try:
                run(["systemctl", "--user", "stop", unit_name])
                run(["systemctl", "--user", "reset-failed", unit_name])
                unit_absent = load_state() == "not-found"
            except Exception:
                unit_absent = False
    payload = {
        "cancel_latency_ms": cancel_latency_ms,
        "cancelled": cancelled,
        "launch_latency_ms": launch_latency_ms,
        "launched": launched,
        "latency_budget_ms": latency_budget_ms,
        "unit_absent": unit_absent,
    }
    ready = launched and cancelled and unit_absent
    return SystemdLaunchCancelEvidence(
        ready=ready,
        launched=launched,
        cancelled=cancelled,
        unit_absent=unit_absent,
        launch_latency_ms=launch_latency_ms,
        cancel_latency_ms=cancel_latency_ms,
        latency_budget_ms=latency_budget_ms,
        evidence_digest=hashlib.sha256(
            json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest(),
    )


def _parse_unit_name(unit_name: str, *, error_type: type[RuntimeError]) -> tuple[str, int]:
    prefix = "loom-staging-rollout-"
    suffix = ".service"
    if len(unit_name) > 255 or not unit_name.startswith(prefix) or not unit_name.endswith(suffix):
        raise error_type("unit name is not an approved rollout attempt")
    identity = unit_name[len(prefix) : -len(suffix)]
    request_id, separator, attempt_text = identity.rpartition("-")
    if not separator:
        raise error_type("unit name is not an approved rollout attempt")
    try:
        validate_safe_identifier(request_id, "request_id")
    except ValueError as exc:
        raise error_type("unit name contains an invalid request id") from exc
    if not attempt_text.isascii() or not attempt_text.isdecimal() or attempt_text.startswith("0"):
        raise error_type("unit name contains an invalid attempt number")
    return request_id, int(attempt_text)


def _attempt_identity(
    config: OperatorConfig,
    envelope_path: Path,
    unit_name: str,
) -> tuple[str, int]:
    if not envelope_path.is_absolute() or ".." in envelope_path.parts:
        raise UnitLaunchError("envelope path is outside the protected request store")
    try:
        relative = envelope_path.relative_to(config.state_root)
    except ValueError as exc:
        raise UnitLaunchError("envelope path is outside the protected request store") from exc
    if len(relative.parts) != 5:
        raise UnitLaunchError("envelope path does not identify one immutable attempt")
    requests_dir, request_id, attempts_dir, attempt_text, filename = relative.parts
    if requests_dir != "requests" or attempts_dir != "attempts" or filename != "envelope.json":
        raise UnitLaunchError("envelope path does not identify one immutable attempt")
    try:
        validate_safe_identifier(request_id, "request_id")
    except ValueError as exc:
        raise UnitLaunchError("envelope path contains an invalid request id") from exc
    if not attempt_text.isascii() or not attempt_text.isdecimal() or attempt_text.startswith("0"):
        raise UnitLaunchError("envelope path contains an invalid attempt number")
    try:
        attempt_number = int(attempt_text)
    except ValueError as exc:
        raise UnitLaunchError("envelope path contains an invalid attempt number") from exc
    unit_request_id, unit_attempt_number = _parse_unit_name(
        unit_name,
        error_type=UnitLaunchError,
    )
    if (unit_request_id, unit_attempt_number) != (request_id, attempt_number):
        raise UnitLaunchError("unit name does not match the protected attempt")
    return request_id, attempt_number


def _backup_identity(
    config: OperatorConfig,
    job_path: Path,
    unit_name: str,
    *,
    error_type: type[RuntimeError] = UnitLaunchError,
) -> str:
    prefix = "loom-staging-backup-"
    suffix = ".service"
    if not unit_name.startswith(prefix) or not unit_name.endswith(suffix):
        raise error_type("unit name is not an approved backup job")
    request_id = unit_name[len(prefix) : -len(suffix)]
    try:
        validate_safe_identifier(request_id, "request_id")
    except ValueError as exc:
        raise error_type("backup unit contains an invalid request id") from exc
    if not job_path.is_absolute() or ".." in job_path.parts:
        raise error_type("backup job path is outside the protected request store")
    try:
        relative = job_path.relative_to(config.state_root)
    except ValueError as exc:
        raise error_type("backup job path is outside the protected request store") from exc
    if relative.parts != ("requests", request_id, "preflight-backup", "job.json"):
        raise error_type("backup job path does not identify one immutable job")
    return request_id


def mutation_guard_unit_name(request_id: str) -> str:
    try:
        validate_safe_identifier(request_id, "request_id")
    except ValueError as exc:
        raise UnitLaunchError("mutation guard request identity is invalid") from exc
    unit_name = f"loom-staging-mutation-guard-{request_id}.service"
    if _TRANSIENT_UNIT_RE.fullmatch(unit_name) is None:
        raise UnitLaunchError("mutation guard unit identity is invalid")
    return unit_name


def _mutation_guard_request_id(
    unit_name: str,
    *,
    error_type: type[RuntimeError],
) -> str:
    prefix = "loom-staging-mutation-guard-"
    suffix = ".service"
    if (
        len(unit_name) > 255
        or not unit_name.startswith(prefix)
        or not unit_name.endswith(suffix)
        or _TRANSIENT_UNIT_RE.fullmatch(unit_name) is None
    ):
        raise error_type("unit name is not an approved mutation guard")
    request_id = unit_name[len(prefix) : -len(suffix)]
    try:
        validate_safe_identifier(request_id, "request_id")
    except ValueError as exc:
        raise error_type("mutation guard unit contains an invalid request id") from exc
    return request_id


def _parse_nonnegative_int(value: str, property_name: str) -> int:
    if not value.isascii() or not value.isdecimal():
        raise SystemdQueryError(f"{property_name} is malformed")
    try:
        return int(value)
    except ValueError as exc:
        raise SystemdQueryError(f"{property_name} is malformed") from exc


def _parse_timestamp(value: str, property_name: str) -> str | None:
    if not value:
        return None
    if len(value) > 256 or any(ord(char) < 32 for char in value):
        raise SystemdQueryError(f"{property_name} is malformed")
    return value


def _parse_show_output(unit_name: str, stdout: str) -> SystemdUnitStatus | None:
    values: dict[str, str] = {}
    for line in stdout.splitlines():
        name, separator, value = line.partition("=")
        if not separator or name not in _SHOW_PROPERTIES or name in values:
            raise SystemdQueryError("systemd unit status output is malformed")
        values[name] = value
    if set(values) != set(_SHOW_PROPERTIES):
        raise SystemdQueryError("systemd unit status output is incomplete")
    load_state = values["LoadState"]
    if load_state == "not-found":
        if values != {
            "LoadState": "not-found",
            "ActiveState": "inactive",
            "SubState": "dead",
            "Result": "success",
            "ExecMainStatus": "0",
            "MainPID": "0",
            "ExecMainStartTimestamp": "",
            "ExecMainExitTimestamp": "",
            "ExecMainStartTimestampMonotonic": "0",
            "ExecMainExitTimestampMonotonic": "0",
        }:
            raise SystemdQueryError("LoadState contradicts systemd unit status")
        return None
    if load_state != "loaded":
        raise SystemdQueryError("LoadState is malformed")
    active_state = values["ActiveState"]
    if active_state not in _ACTIVE_STATES:
        raise SystemdQueryError("ActiveState is malformed")
    sub_state = values["SubState"]
    result = values["Result"]
    if _STATUS_TOKEN_RE.fullmatch(sub_state) is None:
        raise SystemdQueryError("SubState is malformed")
    if _RESULT_TOKEN_RE.fullmatch(result) is None:
        raise SystemdQueryError("Result is malformed")
    return SystemdUnitStatus(
        unit_name=unit_name,
        active_state=cast(ActiveState, active_state),
        sub_state=sub_state,
        result=result,
        exec_main_status=_parse_nonnegative_int(
            values["ExecMainStatus"],
            "ExecMainStatus",
        ),
        main_pid=_parse_nonnegative_int(values["MainPID"], "MainPID"),
        exec_main_start_timestamp=_parse_timestamp(
            values["ExecMainStartTimestamp"],
            "ExecMainStartTimestamp",
        ),
        exec_main_exit_timestamp=_parse_timestamp(
            values["ExecMainExitTimestamp"],
            "ExecMainExitTimestamp",
        ),
        exec_main_start_timestamp_monotonic=_parse_nonnegative_int(
            values["ExecMainStartTimestampMonotonic"],
            "ExecMainStartTimestampMonotonic",
        ),
        exec_main_exit_timestamp_monotonic=_parse_nonnegative_int(
            values["ExecMainExitTimestampMonotonic"],
            "ExecMainExitTimestampMonotonic",
        ),
    )


class _JournalLineIterator:
    def __init__(self, lines: JournalLineStream) -> None:
        self._lines = lines
        self._closed = False

    def __iter__(self) -> _JournalLineIterator:
        return self

    def __next__(self) -> str:
        if self._closed:
            raise StopIteration
        line: object | None = None
        exhausted = False
        iteration_failed = False
        try:
            line = next(self._lines)
        except StopIteration:
            exhausted = True
        except Exception:
            iteration_failed = True
        if iteration_failed:
            self._close_underlying()
            raise SystemdOperationError("rollout unit journal stream failed") from None
        if exhausted:
            if self._close_underlying():
                raise SystemdOperationError(
                    "rollout unit journal stream could not be closed"
                ) from None
            raise StopIteration
        if not isinstance(line, str):
            self._close_underlying()
            raise SystemdOperationError("rollout unit journal stream is malformed") from None
        return line

    def close(self) -> None:
        if self._close_underlying():
            raise SystemdOperationError("rollout unit journal stream could not be closed") from None

    def _close_underlying(self) -> bool:
        if self._closed:
            return False
        self._closed = True
        close_failed = False
        try:
            self._lines.close()
        except Exception:
            close_failed = True
        return close_failed


def _safe_journal_lines(lines: JournalLineStream) -> JournalLineStream:
    return _JournalLineIterator(lines)


def _buffered_journal_lines(stdout: str) -> Generator[str, None, None]:
    yield from stdout.splitlines(keepends=True)


class SystemdUserManager:
    """Build and execute only the approved rollout unit operations."""

    def __init__(
        self,
        config: OperatorConfig,
        *,
        service_uid: int,
        run: CommandRunner,
        stream: JournalStreamRunner | None = None,
        sleep: Callable[[float], None] = time.sleep,
        guard_readiness_timeout_seconds: int = _MUTATION_GUARD_READINESS_TIMEOUT_SECONDS,
        guard_generation: Callable[[], str] = _new_mutation_guard_generation,
    ) -> None:
        if not 1 <= guard_readiness_timeout_seconds <= 1_800:
            raise ValueError("mutation guard readiness timeout is invalid")
        if not callable(guard_generation):
            raise ValueError("mutation guard generation source is invalid")
        self.config = config
        self.service_uid = service_uid
        self._run = run
        self._stream = stream
        self._sleep = sleep
        self._guard_readiness_timeout_seconds = guard_readiness_timeout_seconds
        self._guard_generation = guard_generation

    def start_argv(self, envelope_path: Path, unit_name: str) -> list[str]:
        request_id, _attempt_number = _attempt_identity(
            self.config,
            envelope_path,
            unit_name,
        )
        environment = sanitized_child_environment(
            self.config,
            service_uid=self.service_uid,
        )
        python_path = self.config.runner_repo.parent / "venv" / "bin" / "python"
        argv = transient_service_argv(
            unit_name=unit_name,
            working_directory=self.config.runner_repo,
            command=(
                "/usr/bin/env",
                "-i",
                *(f"{key}={value}" for key, value in environment.items()),
                str(python_path),
                "-m",
                "loom_cli.rollout.operator.worker",
                "run-attempt",
                "--envelope",
                str(envelope_path),
            ),
        )
        guard_unit = mutation_guard_unit_name(request_id)
        argv[10:10] = [
            "--property",
            f"After={guard_unit}",
        ]
        return argv

    def start_attempt(self, envelope_path: Path, unit_name: str) -> None:
        argv = self.start_argv(envelope_path, unit_name)
        try:
            result = self._run(argv)
        except (OSError, subprocess.TimeoutExpired) as exc:
            raise UnitLaunchError("transient rollout unit could not be started") from exc
        if result.returncode != 0:
            raise UnitLaunchError("transient rollout unit launch failed")

    def start_backup_argv(self, job_path: Path, unit_name: str) -> list[str]:
        request_id = _backup_identity(self.config, job_path, unit_name)
        environment = sanitized_child_environment(
            self.config,
            service_uid=self.service_uid,
        )
        python_path = self.config.runner_repo.parent / "venv" / "bin" / "python"
        argv = transient_service_argv(
            unit_name=unit_name,
            working_directory=self.config.runner_repo,
            command=(
                "/usr/bin/env",
                "-i",
                *(f"{key}={value}" for key, value in environment.items()),
                str(python_path),
                "-m",
                "loom_cli.rollout.operator.worker",
                "run-backup",
                "--job",
                str(job_path),
            ),
        )
        guard_unit = mutation_guard_unit_name(request_id)
        argv[10:10] = [
            "--property",
            f"After={guard_unit}",
        ]
        return argv

    def start_backup(self, job_path: Path, unit_name: str) -> None:
        argv = self.start_backup_argv(job_path, unit_name)
        try:
            result = self._run(argv)
        except (OSError, subprocess.TimeoutExpired) as exc:
            raise UnitLaunchError("transient backup unit could not be started") from exc
        if result.returncode != 0:
            raise UnitLaunchError("transient backup unit launch failed")

    def _new_guard_generation(self) -> str:
        try:
            generation = self._guard_generation()
            return validate_mutation_guard_generation(generation)
        except Exception as exc:
            raise UnitLaunchError("mutation guard generation is unavailable") from exc

    def start_mutation_guard_argv(
        self,
        request_id: str,
        generation: str,
        *,
        candidate_sha: str | None = None,
        candidate_tree: str | None = None,
        runner_config_sha256: str | None = None,
        cluster_config_path: Path | None = None,
    ) -> list[str]:
        unit_name = mutation_guard_unit_name(request_id)
        try:
            validate_mutation_guard_generation(generation)
        except MutationGuardError as exc:
            raise UnitLaunchError("mutation guard generation is invalid") from exc
        candidate_identity = _guard_candidate_identity(
            candidate_sha,
            candidate_tree,
            error_type=UnitLaunchError,
        )
        authority_arguments: tuple[str, ...] = ()
        if candidate_identity is None:
            if runner_config_sha256 is not None or cluster_config_path is not None:
                raise UnitLaunchError("mutation guard candidate authority is incomplete")
        else:
            if (
                not isinstance(runner_config_sha256, str)
                or _SHA256_RE.fullmatch(runner_config_sha256) is None
                or not isinstance(cluster_config_path, Path)
                or not cluster_config_path.is_absolute()
                or ".." in cluster_config_path.parts
            ):
                raise UnitLaunchError("mutation guard candidate authority is invalid")
            authority_arguments = (
                "--candidate-sha",
                candidate_identity[0],
                "--candidate-tree",
                candidate_identity[1],
                "--runner-config-sha256",
                runner_config_sha256,
                "--cluster-config-path",
                str(cluster_config_path),
            )
        environment = sanitized_child_environment(
            self.config,
            service_uid=self.service_uid,
        )
        python_path = self.config.runner_repo.parent / "venv" / "bin" / "python"
        argv = transient_service_argv(
            unit_name=unit_name,
            working_directory=self.config.runner_repo,
            command=(
                "/usr/bin/env",
                "-i",
                *(f"{key}={value}" for key, value in environment.items()),
                str(python_path),
                "-m",
                "loom_cli.rollout.operator.staging_mutation_guard",
                "hold",
                "--request-id",
                request_id,
                "--generation",
                generation,
                *authority_arguments,
            ),
        )
        fence_candidate_arguments = (
            ()
            if candidate_identity is None
            else (
                "--candidate-sha",
                candidate_identity[0],
                "--candidate-tree",
                candidate_identity[1],
            )
        )
        fence_command = (
            "/usr/bin/env",
            "-i",
            *(f"{key}={value}" for key, value in environment.items()),
            str(python_path),
            "-m",
            "loom_cli.rollout.operator.staging_mutation_guard",
            "fence",
            "--request-id",
            request_id,
            "--generation",
            generation,
            *fence_candidate_arguments,
        )
        argv[10:10] = [
            "--property",
            "Restart=no",
            "--property",
            "KillMode=mixed",
            "--property",
            f"TimeoutStopSec={MUTATION_GUARD_SERVICE_STOP_TIMEOUT_SECONDS}s",
            "--property",
            f"RuntimeMaxSec={MUTATION_GUARD_RUNTIME_SECONDS}s",
            "--property",
            f"ExecStopPost={shlex.join(fence_command)}",
        ]
        return argv

    def _mutation_guard_owner_units(self, request_id: str) -> tuple[str, ...]:
        """Return a fully validated inventory of exact live request owners."""

        try:
            mutation_guard_unit_name(request_id)
        except UnitLaunchError as exc:
            raise SystemdQueryError("mutation guard owner identity is invalid") from exc
        backup_unit = f"loom-staging-backup-{request_id}.service"
        attempt_pattern = f"loom-staging-rollout-{request_id}-*.service"
        argv = [
            "systemctl",
            "--user",
            "list-units",
            "--all",
            "--plain",
            "--full",
            "--type=service",
            "--no-legend",
            "--no-pager",
            backup_unit,
            attempt_pattern,
        ]
        try:
            result = self._run(argv)
        except (OSError, subprocess.TimeoutExpired) as exc:
            raise SystemdQueryError("mutation guard owner query failed") from exc
        if (
            result.returncode != 0
            or not isinstance(result.stdout, str)
            or not isinstance(result.stderr, str)
            or result.stderr.strip()
            or "\x00" in result.stdout
            or "\x00" in result.stderr
            or len(result.stdout.encode()) > 1024 * 1024
            or len(result.stderr.encode()) > 1024 * 1024
        ):
            raise SystemdQueryError("mutation guard owner query failed")
        live_units: list[str] = []
        live_attempts: set[int] = set()
        seen: set[str] = set()
        for line in result.stdout.splitlines():
            fields = line.split(maxsplit=4)
            if len(fields) < 4:
                raise SystemdQueryError("mutation guard owner output is malformed")
            unit_name, load_state, active_state, sub_state = fields[:4]
            if (
                unit_name in seen
                or load_state != "loaded"
                or _STATUS_TOKEN_RE.fullmatch(active_state) is None
                or _STATUS_TOKEN_RE.fullmatch(sub_state) is None
            ):
                raise SystemdQueryError("mutation guard owner output is malformed")
            seen.add(unit_name)
            if unit_name == backup_unit:
                unit_kind = "backup"
            else:
                try:
                    unit_request_id, attempt_number = _parse_unit_name(
                        unit_name,
                        error_type=SystemdQueryError,
                    )
                except SystemdQueryError as exc:
                    raise SystemdQueryError("mutation guard owner is foreign") from exc
                if unit_request_id != request_id:
                    raise SystemdQueryError("mutation guard owner is foreign")
                unit_kind = "attempt"
            if (active_state, sub_state) in {
                ("inactive", "dead"),
                ("failed", "failed"),
            }:
                continue
            if active_state != "deactivating" and (active_state, sub_state) not in {
                ("active", "running"),
                ("activating", "start"),
            }:
                raise SystemdQueryError("mutation guard owner status is unsafe")
            if unit_kind == "backup":
                live_units.append(unit_name)
            else:
                live_attempts.add(attempt_number)
                live_units.append(unit_name)
        if len(live_attempts) > 1:
            raise SystemdQueryError("mutation guard owner attempts are ambiguous")
        return tuple(live_units)

    def mutation_guard_owner_running(self, request_id: str) -> bool:
        """Return whether one exact backup/attempt owns the request guard."""

        return bool(self._mutation_guard_owner_units(request_id))

    def fence_mutation_guard_owners(
        self,
        request_id: str,
        generation: str | None = None,
        *,
        candidate_sha: str | None = None,
        candidate_tree: str | None = None,
    ) -> None:
        """Hard-fence exact owners unless normal guard release was verified."""

        if generation is not None:
            try:
                validate_mutation_guard_generation(generation)
            except MutationGuardError as exc:
                raise SystemdOperationError("mutation guard fence generation is invalid") from exc
        candidate_identity = _guard_candidate_identity(
            candidate_sha,
            candidate_tree,
            error_type=SystemdOperationError,
        )
        evidence_path = guard_evidence_path(self.config, request_id)
        try:
            evidence = read_mutation_guard_evidence(
                evidence_path,
                service_uid=self.service_uid,
            )
        except MutationGuardError:
            evidence = None
        if (
            evidence is not None
            and generation is not None
            and evidence.request_id == request_id
            and evidence.candidate_sha
            == (
                self.config.runner_repo.parent.name
                if candidate_identity is None
                else candidate_identity[0]
            )
            and (candidate_identity is None or evidence.candidate_tree == candidate_identity[1])
            and evidence.generation == generation
            and evidence.state == "released"
        ):
            return
        live_units = self._mutation_guard_owner_units(request_id)
        fence_failed = False
        for unit_name in live_units:
            try:
                result = self._run(
                    [
                        "systemctl",
                        "--user",
                        "kill",
                        "--kill-whom=all",
                        "--signal=SIGKILL",
                        unit_name,
                    ]
                )
            except (OSError, subprocess.TimeoutExpired):
                fence_failed = True
                continue
            if result.returncode != 0:
                fence_failed = True
        if fence_failed:
            raise SystemdOperationError("mutation guard owner fence failed")

    def _retire_released_mutation_guard_evidence(
        self,
        evidence_path: Path,
        *,
        request_id: str,
        next_generation: str,
        candidate_sha: str,
        candidate_tree: str | None,
    ) -> None:
        try:
            evidence = read_mutation_guard_evidence(
                evidence_path,
                service_uid=self.service_uid,
            )
        except MutationGuardError as exc:
            raise UnitLaunchError("mutation guard evidence identity is already occupied") from exc
        if (
            evidence.request_id != request_id
            or evidence.candidate_sha != candidate_sha
            or (candidate_tree is not None and evidence.candidate_tree != candidate_tree)
            or evidence.generation == next_generation
            or evidence.state != "released"
        ):
            raise UnitLaunchError("mutation guard evidence identity is already occupied")
        try:
            directory_fd = os.open(
                evidence_path.parent,
                os.O_RDONLY
                | getattr(os, "O_CLOEXEC", 0)
                | getattr(os, "O_DIRECTORY", 0)
                | getattr(os, "O_NOFOLLOW", 0),
            )
        except OSError as exc:
            raise UnitLaunchError("mutation guard evidence authority is unavailable") from exc
        try:
            if (
                read_mutation_guard_evidence(
                    evidence_path,
                    service_uid=self.service_uid,
                )
                != evidence
            ):
                raise UnitLaunchError("mutation guard evidence changed during retirement")
            os.unlink(evidence_path.name, dir_fd=directory_fd)
            os.fsync(directory_fd)
        except UnitLaunchError:
            raise
        except (MutationGuardError, OSError) as exc:
            raise UnitLaunchError("mutation guard evidence retirement failed safely") from exc
        finally:
            os.close(directory_fd)

    def start_mutation_guard(
        self,
        request_id: str,
        *,
        candidate_sha: str | None = None,
        candidate_tree: str | None = None,
        runner_config_sha256: str | None = None,
        cluster_config_path: Path | None = None,
    ) -> MutationGuardEvidence:
        mutation_guard_unit_name(request_id)
        candidate_identity = _guard_candidate_identity(
            candidate_sha,
            candidate_tree,
            error_type=UnitLaunchError,
        )
        expected_candidate_sha = (
            self.config.runner_repo.parent.name
            if candidate_identity is None
            else candidate_identity[0]
        )
        expected_candidate_tree = None if candidate_identity is None else candidate_identity[1]
        if self.show_mutation_guard(request_id) is not None:
            raise UnitLaunchError("mutation guard unit identity is already occupied")
        generation = self._new_guard_generation()
        launch_argv = self.start_mutation_guard_argv(
            request_id,
            generation,
            candidate_sha=candidate_sha,
            candidate_tree=candidate_tree,
            runner_config_sha256=runner_config_sha256,
            cluster_config_path=cluster_config_path,
        )
        evidence_path = guard_evidence_path(self.config, request_id)
        try:
            os.lstat(evidence_path)
        except FileNotFoundError:
            pass
        except OSError as exc:
            raise UnitLaunchError("mutation guard evidence authority is unavailable") from exc
        else:
            self._retire_released_mutation_guard_evidence(
                evidence_path,
                request_id=request_id,
                next_generation=generation,
                candidate_sha=expected_candidate_sha,
                candidate_tree=expected_candidate_tree,
            )
        try:
            result = self._run(launch_argv)
        except (OSError, subprocess.TimeoutExpired) as exc:
            raise UnitLaunchError("transient mutation guard could not be started") from exc
        if result.returncode != 0:
            raise UnitLaunchError("transient mutation guard launch failed")
        failure: UnitLaunchError | None = None
        for _attempt in range(self._guard_readiness_timeout_seconds):
            status = self.show_mutation_guard(request_id)
            if status is None or not status.is_running:
                failure = UnitLaunchError("transient mutation guard exited before readiness")
                break
            try:
                os.lstat(evidence_path)
            except FileNotFoundError:
                self._sleep(1.0)
                continue
            except OSError:
                failure = UnitLaunchError("mutation guard readiness evidence is unavailable")
                break
            try:
                evidence = read_mutation_guard_evidence(
                    evidence_path,
                    service_uid=self.service_uid,
                )
            except MutationGuardError:
                failure = UnitLaunchError("mutation guard readiness evidence is invalid")
                break
            if (
                evidence.request_id != request_id
                or evidence.candidate_sha != expected_candidate_sha
                or (
                    expected_candidate_tree is not None
                    and evidence.candidate_tree != expected_candidate_tree
                )
                or evidence.generation != generation
                or evidence.guard_pid != status.main_pid
                or evidence.state != "ready"
            ):
                failure = UnitLaunchError("mutation guard readiness evidence drifted")
                break
            return evidence
        if failure is None:
            failure = UnitLaunchError("mutation guard readiness timed out")
        try:
            self.stop_mutation_guard(
                request_id,
                candidate_sha=candidate_sha,
                candidate_tree=candidate_tree,
            )
        except SystemdOperationError:
            pass
        raise failure

    def _show_validated(self, unit_name: str) -> SystemdUnitStatus | None:
        argv = [
            "systemctl",
            "--user",
            "show",
            "--no-pager",
            *(f"--property={name}" for name in _SHOW_PROPERTIES),
            unit_name,
        ]
        try:
            result = self._run(argv)
        except (OSError, subprocess.TimeoutExpired) as exc:
            raise SystemdQueryError("systemd unit status could not be queried") from exc
        if result.returncode == 4:
            return None
        if result.returncode != 0 or result.stderr.strip():
            raise SystemdQueryError("systemd unit status query failed")
        return _parse_show_output(unit_name, result.stdout)

    def show(self, unit_name: str) -> SystemdUnitStatus | None:
        _parse_unit_name(unit_name, error_type=SystemdQueryError)
        return self._show_validated(unit_name)

    def show_backup(self, job_path: Path, unit_name: str) -> SystemdUnitStatus | None:
        _backup_identity(
            self.config,
            job_path,
            unit_name,
            error_type=SystemdQueryError,
        )
        return self._show_validated(unit_name)

    def show_mutation_guard(self, request_id: str) -> SystemdUnitStatus | None:
        try:
            unit_name = mutation_guard_unit_name(request_id)
        except UnitLaunchError as exc:
            raise SystemdQueryError("mutation guard request identity is invalid") from exc
        _mutation_guard_request_id(unit_name, error_type=SystemdQueryError)
        return self._show_validated(unit_name)

    def stop_mutation_guard(
        self,
        request_id: str,
        *,
        candidate_sha: str | None = None,
        candidate_tree: str | None = None,
    ) -> MutationGuardEvidence | None:
        try:
            unit_name = mutation_guard_unit_name(request_id)
            _mutation_guard_request_id(unit_name, error_type=SystemdOperationError)
        except UnitLaunchError as exc:
            raise SystemdOperationError("mutation guard request identity is invalid") from exc
        candidate_identity = _guard_candidate_identity(
            candidate_sha,
            candidate_tree,
            error_type=SystemdOperationError,
        )
        expected_candidate_sha = (
            self.config.runner_repo.parent.name
            if candidate_identity is None
            else candidate_identity[0]
        )
        evidence_path = guard_evidence_path(self.config, request_id)
        status = self._show_validated(unit_name)
        if status is None:
            try:
                os.lstat(evidence_path)
            except FileNotFoundError:
                return None
            except OSError as exc:
                raise SystemdOperationError(
                    "mutation guard release evidence is unavailable"
                ) from exc
            try:
                evidence = read_mutation_guard_evidence(
                    evidence_path,
                    service_uid=self.service_uid,
                )
            except MutationGuardError as exc:
                raise SystemdOperationError("mutation guard release evidence is invalid") from exc
            if (
                evidence.request_id != request_id
                or evidence.candidate_sha != expected_candidate_sha
                or (
                    candidate_identity is not None
                    and evidence.candidate_tree != candidate_identity[1]
                )
                or evidence.state != "released"
            ):
                raise SystemdOperationError("absent mutation guard was not released")
            return evidence
        ready_evidence: MutationGuardEvidence | None = None
        try:
            observed_ready = read_mutation_guard_evidence(
                evidence_path,
                service_uid=self.service_uid,
            )
        except MutationGuardError:
            pass
        else:
            if (
                observed_ready.request_id == request_id
                and observed_ready.candidate_sha == expected_candidate_sha
                and (
                    candidate_identity is None
                    or observed_ready.candidate_tree == candidate_identity[1]
                )
                and observed_ready.guard_pid == status.main_pid
                and observed_ready.state == "ready"
            ):
                ready_evidence = observed_ready
        try:
            stopped = self._run(["systemctl", "--user", "stop", unit_name])
        except (OSError, subprocess.TimeoutExpired) as exc:
            raise SystemdOperationError("mutation guard stop could not be verified") from exc
        if stopped.returncode != 0:
            raise SystemdOperationError("mutation guard stop failed")
        for attempt in range(_MUTATION_GUARD_STOP_WAIT_SECONDS):
            stopped_status = self._show_validated(unit_name)
            if stopped_status is None or stopped_status.is_cleanly_inactive:
                break
            if not stopped_status.is_running:
                raise SystemdOperationError("mutation guard did not stop cleanly")
            if attempt + 1 < _MUTATION_GUARD_STOP_WAIT_SECONDS:
                self._sleep(1.0)
        else:
            raise SystemdOperationError("mutation guard unit remains present after stop")
        try:
            evidence = read_mutation_guard_evidence(
                evidence_path,
                service_uid=self.service_uid,
            )
        except MutationGuardError as exc:
            raise SystemdOperationError("mutation guard release evidence is invalid") from exc
        if (
            ready_evidence is None
            or evidence.request_id != request_id
            or evidence.candidate_sha != expected_candidate_sha
            or (candidate_identity is not None and evidence.candidate_tree != candidate_identity[1])
            or evidence.state != "released"
            or not _same_mutation_guard_acquisition(ready_evidence, evidence)
        ):
            raise SystemdOperationError("mutation guard release was not verified")
        return evidence

    def terminate(self, unit_name: str) -> None:
        _parse_unit_name(unit_name, error_type=SystemdOperationError)
        argv = [
            "systemctl",
            "--user",
            "kill",
            "--signal=SIGTERM",
            unit_name,
        ]
        try:
            result = self._run(argv)
        except OSError as exc:
            raise SystemdOperationError("rollout unit termination could not be requested") from exc
        if result.returncode != 0:
            raise SystemdOperationError("rollout unit termination request failed")

    def stream_journal(self, unit_name: str, follow: bool) -> JournalLineStream:
        if type(follow) is not bool:
            raise SystemdOperationError("journal follow must be a boolean")
        _parse_unit_name(unit_name, error_type=SystemdOperationError)
        argv = [
            "journalctl",
            "--user",
            "--unit",
            unit_name,
            "--no-pager",
            "--lines=200",
            "--output=short-iso",
        ]
        if follow:
            argv.append("--follow")
            if self._stream is None:
                raise SystemdOperationError("journal follow streaming is not configured")
            lines = None
            try:
                lines = self._stream(argv)
            except Exception:
                pass
            if lines is None:
                raise SystemdOperationError(
                    "rollout unit journal stream could not be opened"
                ) from None
            return _safe_journal_lines(lines)
        try:
            result = self._run(argv)
        except OSError as exc:
            raise SystemdOperationError("rollout unit journal could not be read") from exc
        if result.returncode != 0:
            raise SystemdOperationError("rollout unit journal command failed")
        return _buffered_journal_lines(result.stdout)


__all__ = [
    "MUTATION_GUARD_CLIENT_OPERATION_TIMEOUT_SECONDS",
    "MUTATION_GUARD_SERVICE_STOP_TIMEOUT_SECONDS",
    "MUTATION_GUARD_STOP_POST_BOUND_SECONDS",
    "ActiveState",
    "CommandResult",
    "CommandRunner",
    "JournalLineStream",
    "JournalStreamRunner",
    "SystemdOperationError",
    "SystemdQueryError",
    "SystemdUnitStatus",
    "SystemdUserManager",
    "UnitLaunchError",
    "mutation_guard_unit_name",
]
