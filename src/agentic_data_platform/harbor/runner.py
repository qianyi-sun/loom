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
    backend: str = "cli"
    dataset_ref: str | None = None
    task_path: Path | None = None
    agent: str = "oracle"
    agent_import_path: Path | None = None
    environment: str = "docker"
    trial_name: str | None = None
    timeout_seconds: int = 3600
    auto_confirm: bool = True
    agent_env: list[str] = field(default_factory=list)
    agent_kwargs: list[str] = field(default_factory=list)
    process_env: list[str] = field(default_factory=list)
    verifier_env: list[str] = field(default_factory=list)
    extra_args: list[str] = field(default_factory=list)

    def __post_init__(self) -> None:
        _require_non_empty("run_id", self.run_id)
        _require_non_empty("task_instance_id", self.task_instance_id)
        _require_non_empty("model_name", self.model_name)
        _require_non_empty("backend", self.backend)
        _require_non_empty("agent", self.agent)
        _require_non_empty("environment", self.environment)

        if self.backend not in {"cli", "native"}:
            raise ValueError("backend must be one of: cli, native")
        if self.timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")
        if bool(self.dataset_ref) == bool(self.task_path):
            raise ValueError("exactly one of dataset_ref or task_path must be set")
        if self.dataset_ref is not None:
            _require_non_empty("dataset_ref", self.dataset_ref)
        _validate_key_value_args(self.agent_env, field_name="agent_env")
        _validate_key_value_args(self.agent_kwargs, field_name="agent_kwargs")
        _validate_key_value_args(self.process_env, field_name="process_env")
        _validate_key_value_args(self.verifier_env, field_name="verifier_env")
        if self.extra_args and any(not isinstance(arg, str) or not arg.strip() for arg in self.extra_args):
            raise ValueError("extra_args must contain non-empty strings")


@dataclass(frozen=True)
class HarborRunnerResult:
    run_id: str
    backend: str
    command: list[str]
    jobs_dir: Path
    job_dir: Path | None
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
            "backend": self.backend,
            "command": _redacted_command(self.command),
            "jobs_dir": self.jobs_dir.name,
            "job_dir": self.job_dir.name if self.job_dir is not None else None,
            "started_at": _datetime(self.started_at),
            "completed_at": _datetime(self.completed_at),
            "duration_seconds": self.duration_seconds,
            "exit_code": self.exit_code,
            "stdout": self.stdout,
            "stderr": self.stderr,
            "timed_out": self.timed_out,
        }


class HarborCliRunnerBackend:
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
        if spec.backend != "cli":
            raise ValueError("HarborCliRunnerBackend only supports backend='cli'")
        spec.jobs_dir.mkdir(parents=True, exist_ok=True)
        existing_job_names = _harbor_job_names(spec.jobs_dir)
        command = self.command_for(spec)
        started_at = datetime.now(timezone.utc)
        timed_out = False
        try:
            process = self.command_runner.run(
                command,
                timeout=spec.timeout_seconds,
                env=_env_dict(spec.process_env),
            )
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
        job_dir = _resolve_current_job_dir(spec.jobs_dir, existing_job_names=existing_job_names)
        return HarborRunnerResult(
            run_id=spec.run_id,
            backend="cli",
            command=command,
            jobs_dir=spec.jobs_dir,
            job_dir=job_dir,
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
                "--model",
                spec.model_name,
                "--env",
                spec.environment,
                "--jobs-dir",
                str(spec.jobs_dir),
            ]
        )
        if spec.auto_confirm:
            command.append("--yes")
        if spec.agent_import_path is not None:
            command.extend(["--agent-import-path", str(spec.agent_import_path)])
        for env_value in spec.agent_env:
            command.extend(["--agent-env", env_value])
        for kwarg_value in spec.agent_kwargs:
            command.extend(["--agent-kwarg", kwarg_value])
        for env_value in spec.verifier_env:
            command.extend(["--verifier-env", env_value])
        command.extend(spec.extra_args)
        return command


HarborRunnerBackend = HarborCliRunnerBackend


def _resolve_current_job_dir(jobs_dir: Path, *, existing_job_names: set[str]) -> Path | None:
    job_dirs = _harbor_job_dirs(jobs_dir)
    if not job_dirs:
        return None

    new_job_dirs = [path for path in job_dirs if path.name not in existing_job_names]
    if len(new_job_dirs) == 1:
        return new_job_dirs[0]
    if len(new_job_dirs) > 1:
        return max(new_job_dirs, key=_path_mtime_ns)
    if len(job_dirs) == 1:
        return job_dirs[0]
    return max(job_dirs, key=_path_mtime_ns)


def _harbor_job_names(jobs_dir: Path) -> set[str]:
    return {path.name for path in _harbor_job_dirs(jobs_dir)}


def _harbor_job_dirs(jobs_dir: Path) -> list[Path]:
    if not jobs_dir.is_dir():
        return []
    return [path for path in sorted(jobs_dir.iterdir()) if _is_harbor_job_dir(path)]


def _is_harbor_job_dir(path: Path) -> bool:
    return path.is_dir() and (path / "config.json").is_file() and (path / "result.json").is_file()


def _path_mtime_ns(path: Path) -> int:
    try:
        return path.stat().st_mtime_ns
    except FileNotFoundError:
        return 0


def _coerce_output(value: str | bytes | None) -> str:
    if value is None:
        return ""
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return value


def _datetime(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _redacted_command(command: list[str]) -> list[str]:
    redacted: list[str] = []
    redact_path_next = False
    redact_env_next = False
    for value in command:
        if redact_path_next:
            redacted.append(_path_name_or_redacted(value))
            redact_path_next = False
            continue
        if redact_env_next:
            redacted.append(_redact_key_value(value))
            redact_env_next = False
            continue

        redacted.append(value)
        if value in {"-p", "--path", "--jobs-dir", "--agent-import-path", "--env-file", "-c", "--config"}:
            redact_path_next = True
        elif value in {"--agent-env", "--verifier-env", "--ae", "--ve", "--agent-kwarg", "--ak"}:
            redact_env_next = True
    return redacted


def _path_name_or_redacted(value: str) -> str:
    name = Path(value).name
    return name or "[path redacted]"


def _redact_key_value(value: str) -> str:
    key, separator, _ = value.partition("=")
    if not separator or not key.strip():
        return "[redacted]"
    return f"{key}=[redacted]"


def _require_non_empty(name: str, value: str) -> None:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} must be a non-empty string")


def _validate_key_value_args(values: list[str], *, field_name: str) -> None:
    if not isinstance(values, list) or any(
        not isinstance(item, str) or "=" not in item or not item.split("=", 1)[0].strip() for item in values
    ):
        raise ValueError(f"{field_name} must contain KEY=VALUE strings")


def _env_dict(values: list[str]) -> dict[str, str] | None:
    if not values:
        return None
    return dict(value.split("=", 1) for value in values)
