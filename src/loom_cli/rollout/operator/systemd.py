"""Constrained systemd user-manager boundary for detached rollout attempts."""

from __future__ import annotations

import re
import subprocess
from collections.abc import Callable, Generator, Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Literal, Protocol, cast

from .config import OperatorConfig
from .model import validate_safe_identifier
from .policy import sanitized_child_environment


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
    ) -> None:
        self.config = config
        self.service_uid = service_uid
        self._run = run
        self._stream = stream

    def start_argv(self, envelope_path: Path, unit_name: str) -> list[str]:
        _attempt_identity(self.config, envelope_path, unit_name)
        environment = sanitized_child_environment(
            self.config,
            service_uid=self.service_uid,
        )
        python_path = self.config.runner_repo.parent / "venv" / "bin" / "python"
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
            f"WorkingDirectory={self.config.runner_repo}",
            "/usr/bin/env",
            "-i",
            *(f"{key}={value}" for key, value in environment.items()),
            str(python_path),
            "-m",
            "loom_cli.rollout.operator.worker",
            "run-attempt",
            "--envelope",
            str(envelope_path),
        ]

    def start_attempt(self, envelope_path: Path, unit_name: str) -> None:
        argv = self.start_argv(envelope_path, unit_name)
        try:
            result = self._run(argv)
        except (OSError, subprocess.TimeoutExpired) as exc:
            raise UnitLaunchError("transient rollout unit could not be started") from exc
        if result.returncode != 0:
            raise UnitLaunchError("transient rollout unit launch failed")

    def show(self, unit_name: str) -> SystemdUnitStatus | None:
        _parse_unit_name(unit_name, error_type=SystemdQueryError)
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
]
