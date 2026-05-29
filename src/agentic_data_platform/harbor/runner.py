from __future__ import annotations

import subprocess
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from agentic_data_platform.sandbox.docker_terminal import CommandRunner, SubprocessCommandRunner


@dataclass(frozen=True)
class HarborRunSpec:
    run_id: str
    task_instance_id: str
    model_name: str
    jobs_dir: Path
    dataset_ref: str | None = None
    task_path: Path | None = None
    agent: str = "default"
    agent_import_path: Path | None = None
    sandbox: str = "docker"
    trial_name: str | None = None
    timeout_seconds: int = 3600
    extra_args: list[str] = field(default_factory=list)

    def __post_init__(self) -> None:
        _require_non_empty("run_id", self.run_id)
        _require_non_empty("task_instance_id", self.task_instance_id)
        _require_non_empty("model_name", self.model_name)
        _require_non_empty("agent", self.agent)
        _require_non_empty("sandbox", self.sandbox)

        if self.timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")
        if bool(self.dataset_ref) == bool(self.task_path):
            raise ValueError("exactly one of dataset_ref or task_path must be set")
        if self.dataset_ref is not None:
            _require_non_empty("dataset_ref", self.dataset_ref)
        if self.extra_args and any(not isinstance(arg, str) or not arg.strip() for arg in self.extra_args):
            raise ValueError("extra_args must contain non-empty strings")


@dataclass(frozen=True)
class HarborRunnerResult:
    run_id: str
    command: list[str]
    jobs_dir: Path
    started_at: datetime
    completed_at: datetime
    exit_code: int
    stdout: str
    stderr: str
    timed_out: bool = False

    @property
    def duration_seconds(self) -> float:
        return (self.completed_at - self.started_at).total_seconds()

    def to_report(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "command": list(self.command),
            "jobs_dir": self.jobs_dir.name,
            "started_at": _datetime(self.started_at),
            "completed_at": _datetime(self.completed_at),
            "duration_seconds": self.duration_seconds,
            "exit_code": self.exit_code,
            "stdout": self.stdout,
            "stderr": self.stderr,
            "timed_out": self.timed_out,
        }


class HarborRunnerBackend:
    def __init__(
        self,
        *,
        executable: str = "harbor",
        command_runner: CommandRunner | None = None,
    ) -> None:
        _require_non_empty("executable", executable)
        self.executable = executable
        self.command_runner = command_runner or SubprocessCommandRunner()

    def run(self, spec: HarborRunSpec) -> HarborRunnerResult:
        spec.jobs_dir.mkdir(parents=True, exist_ok=True)
        command = self.command_for(spec)
        started_at = datetime.now(timezone.utc)
        timed_out = False
        try:
            process = self.command_runner.run(command, timeout=spec.timeout_seconds)
            exit_code = process.returncode
            stdout = _coerce_output(process.stdout)
            stderr = _coerce_output(process.stderr)
        except subprocess.TimeoutExpired as exc:
            timed_out = True
            exit_code = 124
            stdout = _coerce_output(exc.output)
            stderr = _coerce_output(exc.stderr)
            timeout_message = f"Harbor run timed out after {spec.timeout_seconds} seconds"
            stderr = f"{stderr.rstrip()}\n{timeout_message}\n" if stderr else f"{timeout_message}\n"
        completed_at = datetime.now(timezone.utc)
        return HarborRunnerResult(
            run_id=spec.run_id,
            command=command,
            jobs_dir=spec.jobs_dir,
            started_at=started_at,
            completed_at=completed_at,
            exit_code=exit_code,
            stdout=stdout,
            stderr=stderr,
            timed_out=timed_out,
        )

    def command_for(self, spec: HarborRunSpec) -> list[str]:
        command = [self.executable, "run"]
        if spec.dataset_ref is not None:
            command.extend(["-d", spec.dataset_ref])
        elif spec.task_path is not None:
            command.extend(["-p", str(spec.task_path)])
        command.extend(
            [
                "--agent",
                spec.agent,
                "--models",
                spec.model_name,
                "--sandbox",
                spec.sandbox,
                "--jobs-dir",
                str(spec.jobs_dir),
            ]
        )
        if spec.agent_import_path is not None:
            command.extend(["--agent-import-path", str(spec.agent_import_path)])
        command.extend(spec.extra_args)
        return command


def _coerce_output(value: str | bytes | None) -> str:
    if value is None:
        return ""
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return value


def _datetime(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _require_non_empty(name: str, value: str) -> None:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} must be a non-empty string")
