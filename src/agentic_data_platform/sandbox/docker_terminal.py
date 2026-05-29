from __future__ import annotations

import hashlib
import subprocess
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Protocol

from agentic_data_platform.domain.run_records import TerminalTurn


@dataclass(frozen=True)
class DockerTerminalSandboxConfig:
    run_id: str
    image: str
    workspace_root: Path
    host_workspace_root: Path | None = None
    container_workspace: str = "/workspace"
    cpu_limit: int | float | None = None
    memory_mb: int | None = None
    pids_limit: int | None = None
    timeout_seconds: int = 3600
    internet_access: bool = True

    def __post_init__(self) -> None:
        _require_non_empty("run_id", self.run_id)
        _require_non_empty("image", self.image)
        _require_non_empty("container_workspace", self.container_workspace)

        if self.timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")

        if self.cpu_limit is not None and self.cpu_limit <= 0:
            raise ValueError("cpu_limit must be positive when set")

        if self.memory_mb is not None and self.memory_mb <= 0:
            raise ValueError("memory_mb must be positive when set")

        if self.pids_limit is not None and self.pids_limit <= 0:
            raise ValueError("pids_limit must be positive when set")


class CommandRunner(Protocol):
    def run(self, args: list[str], *, timeout: int) -> subprocess.CompletedProcess[str]:
        ...


class SubprocessCommandRunner:
    def run(self, args: list[str], *, timeout: int) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            args,
            capture_output=True,
            text=True,
            timeout=timeout,
            check=False,
        )


@dataclass(frozen=True)
class SandboxCommandResult:
    run_id: str
    command: str
    cwd: str
    started_at: datetime
    completed_at: datetime
    exit_code: int
    stdout: str
    stderr: str
    changed_paths: list[str]
    timed_out: bool = False
    metadata: dict[str, object] = field(default_factory=dict)

    def to_terminal_turn(self, *, turn_index: int, model_call_id: str | None = None) -> TerminalTurn:
        return TerminalTurn(
            turn_index=turn_index,
            command=self.command,
            cwd=self.cwd,
            started_at=self.started_at,
            completed_at=self.completed_at,
            exit_code=self.exit_code,
            stdout=self.stdout,
            stderr=self.stderr,
            changed_paths=self.changed_paths,
            model_call_id=model_call_id,
            metadata={
                **self.metadata,
                "sandbox_run_id": self.run_id,
                "timed_out": self.timed_out,
            },
        )


@dataclass(frozen=True)
class WorkspaceFile:
    path: str
    size_bytes: int
    sha256: str


@dataclass(frozen=True)
class WorkspaceSnapshot:
    run_id: str
    workspace_path: str
    captured_at: datetime
    files: list[WorkspaceFile]


class DockerTerminalSandbox:
    def __init__(
        self,
        config: DockerTerminalSandboxConfig,
        *,
        runner: CommandRunner | None = None,
    ) -> None:
        self.config = config
        self.runner = runner or SubprocessCommandRunner()
        self.workspace_path = config.workspace_root / config.run_id
        self.host_workspace_path = (config.host_workspace_root or config.workspace_root) / config.run_id
        self.workspace_path.mkdir(parents=True, exist_ok=True)

    def execute(self, command: str, *, cwd: str | None = None, timeout_seconds: int | None = None) -> SandboxCommandResult:
        _require_non_empty("command", command)

        container_cwd = cwd or self.config.container_workspace
        timeout = timeout_seconds or self.config.timeout_seconds
        if timeout <= 0:
            raise ValueError("timeout_seconds must be positive")

        before = self._workspace_index()
        started_at = datetime.now(timezone.utc)
        docker_args = self._docker_args(command, container_cwd)

        timed_out = False
        metadata: dict[str, object] = {
            "image": self.config.image,
            "timeout_seconds": timeout,
            "internet_access": self.config.internet_access,
            "resource_limits": {
                "cpu_limit": self.config.cpu_limit,
                "memory_mb": self.config.memory_mb,
                "pids_limit": self.config.pids_limit,
            },
        }

        try:
            process = self.runner.run(docker_args, timeout=timeout)
            exit_code = process.returncode
            stdout = _coerce_output(process.stdout)
            stderr = _coerce_output(process.stderr)
        except subprocess.TimeoutExpired as exc:
            timed_out = True
            exit_code = 124
            stdout = _coerce_output(exc.output)
            stderr = _coerce_output(exc.stderr)
            timeout_message = f"Command timed out after {timeout} seconds"
            stderr = f"{stderr.rstrip()}\n{timeout_message}\n" if stderr else f"{timeout_message}\n"

        completed_at = datetime.now(timezone.utc)
        changed_paths = _changed_paths(before, self._workspace_index())

        return SandboxCommandResult(
            run_id=self.config.run_id,
            command=command,
            cwd=container_cwd,
            started_at=started_at,
            completed_at=completed_at,
            exit_code=exit_code,
            stdout=stdout,
            stderr=stderr,
            changed_paths=changed_paths,
            timed_out=timed_out,
            metadata=metadata,
        )

    def capture_workspace(self) -> WorkspaceSnapshot:
        files = [
            WorkspaceFile(path=relative_path, size_bytes=size_bytes, sha256=sha256)
            for relative_path, (size_bytes, sha256) in self._workspace_index().items()
        ]
        files.sort(key=lambda item: item.path)

        return WorkspaceSnapshot(
            run_id=self.config.run_id,
            workspace_path=str(self.workspace_path),
            captured_at=datetime.now(timezone.utc),
            files=files,
        )

    def _docker_args(self, command: str, cwd: str) -> list[str]:
        args = [
            "docker",
            "run",
            "--rm",
            "-v",
            f"{self.host_workspace_path}:{self.config.container_workspace}",
            "-w",
            cwd,
        ]

        if self.config.cpu_limit is not None:
            args.extend(["--cpus", str(self.config.cpu_limit)])

        if self.config.memory_mb is not None:
            args.extend(["--memory", f"{self.config.memory_mb}m"])

        if self.config.pids_limit is not None:
            args.extend(["--pids-limit", str(self.config.pids_limit)])

        if not self.config.internet_access:
            args.extend(["--network", "none"])

        args.extend([self.config.image, "/bin/sh", "-lc", command])
        return args

    def _workspace_index(self) -> dict[str, tuple[int, str]]:
        return _index_workspace(self.workspace_path)


def _index_workspace(workspace_path: Path) -> dict[str, tuple[int, str]]:
    indexed: dict[str, tuple[int, str]] = {}
    if not workspace_path.exists():
        return indexed

    for path in workspace_path.rglob("*"):
        if not path.is_file():
            continue

        relative_path = path.relative_to(workspace_path).as_posix()
        indexed[relative_path] = (path.stat().st_size, _sha256(path))

    return indexed


def _changed_paths(
    before: dict[str, tuple[int, str]],
    after: dict[str, tuple[int, str]],
) -> list[str]:
    paths = set(before) | set(after)
    return sorted(path for path in paths if before.get(path) != after.get(path))


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _coerce_output(value: str | bytes | None) -> str:
    if value is None:
        return ""

    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")

    return value


def _require_non_empty(name: str, value: str) -> None:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} must be a non-empty string")
