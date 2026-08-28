"""Constrained systemd user-manager boundary for detached rollout attempts."""

from __future__ import annotations

import hashlib
import json
import os
import re
import subprocess
import time
from collections.abc import Callable, Generator, Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Literal, Protocol, cast

from loom_cli.cluster_backup_guard import DEFAULT_BACKUP_MAX_ELAPSED_SECONDS
from loom_cli.rollout.final_gate_command_runner import FINAL_GATE_MAX_ELAPSED_SECONDS

from ..systemd_readiness import parse_systemctl_properties
from .config import OperatorConfig
from .model import validate_safe_identifier
from .policy import sanitized_child_environment
from .staging_mutation_guard import (
    MutationGuardError,
    MutationGuardEvidence,
    guard_evidence_path,
    read_mutation_guard_evidence,
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
_TRANSIENT_UNIT_RE = re.compile(
    r"(?:loom-staging-backup-[a-z0-9][a-z0-9-]{7,79}"
    r"|loom-staging-mutation-guard-[a-z0-9][a-z0-9-]{7,79}"
    r"|loom-staging-rollout-[a-z0-9][a-z0-9-]{7,79}-[1-9][0-9]*"
    r"|loom-preflight-lifecycle-[0-9a-f]{16})[.]service\Z"
)
_MUTATION_GUARD_READINESS_TIMEOUT_SECONDS = 1_500
_MUTATION_GUARD_OPERATIONAL_MARGIN_SECONDS = 5 * 60 * 60
_MUTATION_GUARD_RUNTIME_SECONDS = (
    DEFAULT_BACKUP_MAX_ELAPSED_SECONDS
    + FINAL_GATE_MAX_ELAPSED_SECONDS
    + _MUTATION_GUARD_READINESS_TIMEOUT_SECONDS
    + _MUTATION_GUARD_OPERATIONAL_MARGIN_SECONDS
)
_MUTATION_GUARD_STOP_WAIT_SECONDS = 60
_SHOW_PROPERTIES = (
    "ActiveState",
    "SubState",
    "Result",
    "ExecMainStatus",
    "MainPID",
    "ExecMainStartTimestamp",
    "ExecMainExitTimestamp",
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

    @property
    def is_running(self) -> bool:
        return self.active_state in {"activating", "active", "reloading", "deactivating"}


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


def _parse_show_output(unit_name: str, stdout: str) -> SystemdUnitStatus:
    values: dict[str, str] = {}
    for line in stdout.splitlines():
        name, separator, value = line.partition("=")
        if not separator or name not in _SHOW_PROPERTIES or name in values:
            raise SystemdQueryError("systemd unit status output is malformed")
        values[name] = value
    if set(values) != set(_SHOW_PROPERTIES):
        raise SystemdQueryError("systemd unit status output is incomplete")
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
    ) -> None:
        if not 1 <= guard_readiness_timeout_seconds <= 1_800:
            raise ValueError("mutation guard readiness timeout is invalid")
        self.config = config
        self.service_uid = service_uid
        self._run = run
        self._stream = stream
        self._sleep = sleep
        self._guard_readiness_timeout_seconds = guard_readiness_timeout_seconds

    def start_argv(self, envelope_path: Path, unit_name: str) -> list[str]:
        _attempt_identity(self.config, envelope_path, unit_name)
        environment = sanitized_child_environment(
            self.config,
            service_uid=self.service_uid,
        )
        python_path = self.config.runner_repo.parent / "venv" / "bin" / "python"
        return transient_service_argv(
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

    def start_attempt(self, envelope_path: Path, unit_name: str) -> None:
        argv = self.start_argv(envelope_path, unit_name)
        try:
            result = self._run(argv)
        except (OSError, subprocess.TimeoutExpired) as exc:
            raise UnitLaunchError("transient rollout unit could not be started") from exc
        if result.returncode != 0:
            raise UnitLaunchError("transient rollout unit launch failed")

    def start_backup_argv(self, job_path: Path, unit_name: str) -> list[str]:
        _backup_identity(self.config, job_path, unit_name)
        environment = sanitized_child_environment(
            self.config,
            service_uid=self.service_uid,
        )
        python_path = self.config.runner_repo.parent / "venv" / "bin" / "python"
        return transient_service_argv(
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

    def start_backup(self, job_path: Path, unit_name: str) -> None:
        argv = self.start_backup_argv(job_path, unit_name)
        try:
            result = self._run(argv)
        except (OSError, subprocess.TimeoutExpired) as exc:
            raise UnitLaunchError("transient backup unit could not be started") from exc
        if result.returncode != 0:
            raise UnitLaunchError("transient backup unit launch failed")

    def start_mutation_guard_argv(self, request_id: str) -> list[str]:
        unit_name = mutation_guard_unit_name(request_id)
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
            ),
        )
        argv[10:10] = [
            "--property",
            "Restart=on-failure",
            "--property",
            "RestartSec=5s",
            "--property",
            "KillMode=mixed",
            "--property",
            "TimeoutStopSec=120s",
            "--property",
            f"RuntimeMaxSec={_MUTATION_GUARD_RUNTIME_SECONDS}s",
        ]
        return argv

    def start_mutation_guard(self, request_id: str) -> MutationGuardEvidence:
        mutation_guard_unit_name(request_id)
        if self.show_mutation_guard(request_id) is not None:
            raise UnitLaunchError("mutation guard unit identity is already occupied")
        evidence_path = guard_evidence_path(self.config, request_id)
        try:
            os.lstat(evidence_path)
        except FileNotFoundError:
            pass
        except OSError as exc:
            raise UnitLaunchError("mutation guard evidence authority is unavailable") from exc
        else:
            raise UnitLaunchError("mutation guard evidence identity is already occupied")
        try:
            result = self._run(self.start_mutation_guard_argv(request_id))
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
                or evidence.candidate_sha != self.config.runner_repo.parent.name
                or evidence.guard_pid != status.main_pid
                or evidence.state != "ready"
            ):
                failure = UnitLaunchError("mutation guard readiness evidence drifted")
                break
            return evidence
        if failure is None:
            failure = UnitLaunchError("mutation guard readiness timed out")
        try:
            self.stop_mutation_guard(request_id)
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
        if result.returncode != 0:
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

    def stop_mutation_guard(self, request_id: str) -> MutationGuardEvidence | None:
        try:
            unit_name = mutation_guard_unit_name(request_id)
            _mutation_guard_request_id(unit_name, error_type=SystemdOperationError)
        except UnitLaunchError as exc:
            raise SystemdOperationError("mutation guard request identity is invalid") from exc
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
                or evidence.candidate_sha != self.config.runner_repo.parent.name
                or evidence.state != "released"
            ):
                raise SystemdOperationError("absent mutation guard was not released")
            return evidence
        try:
            stopped = self._run(["systemctl", "--user", "stop", unit_name])
            reset = self._run(["systemctl", "--user", "reset-failed", unit_name])
        except (OSError, subprocess.TimeoutExpired) as exc:
            raise SystemdOperationError("mutation guard stop could not be verified") from exc
        if stopped.returncode != 0 or reset.returncode not in {0, 1}:
            raise SystemdOperationError("mutation guard stop failed")
        for attempt in range(_MUTATION_GUARD_STOP_WAIT_SECONDS):
            if self._show_validated(unit_name) is None:
                break
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
            evidence.request_id != request_id
            or evidence.candidate_sha != self.config.runner_repo.parent.name
            or evidence.state != "released"
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
