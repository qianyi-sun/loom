from __future__ import annotations

import hashlib
import json
import os
import subprocess
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Protocol

from agentic_data_platform.domain.run_records import TerminalTurn


@dataclass(frozen=True)
class DockerTerminalSandboxConfig:
    run_id: str
    image: str
    workspace_root: Path
    attempt_id: str | None = None
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
        if self.attempt_id is not None:
            _require_non_empty("attempt_id", self.attempt_id)

        if self.timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")

        if self.cpu_limit is not None and self.cpu_limit <= 0:
            raise ValueError("cpu_limit must be positive when set")

        if self.memory_mb is not None and self.memory_mb <= 0:
            raise ValueError("memory_mb must be positive when set")

        if self.pids_limit is not None and self.pids_limit <= 0:
            raise ValueError("pids_limit must be positive when set")


class CommandRunner(Protocol):
    def run(
        self,
        args: list[str],
        *,
        timeout: int,
        env: dict[str, str] | None = None,
    ) -> subprocess.CompletedProcess[str]:
        ...


class SandboxLifecycleRecorder(Protocol):
    def container_started(self, metadata: dict[str, Any]) -> None:
        ...

    def resource_sampled(self, metadata: dict[str, Any]) -> None:
        ...

    def container_completed(self, metadata: dict[str, Any]) -> None:
        ...


class SubprocessCommandRunner:
    def start(
        self,
        args: list[str],
        *,
        env: dict[str, str] | None = None,
    ) -> subprocess.Popen[str]:
        return subprocess.Popen(
            args,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            env={**os.environ, **env} if env else None,
        )

    def run(
        self,
        args: list[str],
        *,
        timeout: int,
        env: dict[str, str] | None = None,
    ) -> subprocess.CompletedProcess[str]:
        process = self.start(args, env=env)
        try:
            stdout, stderr = process.communicate(timeout=timeout)
        except subprocess.TimeoutExpired as exc:
            process.kill()
            stdout, stderr = process.communicate()
            raise subprocess.TimeoutExpired(
                cmd=args,
                timeout=timeout,
                output=stdout,
                stderr=stderr,
            ) from exc
        return subprocess.CompletedProcess(
            args=args,
            returncode=process.returncode or 0,
            stdout=stdout,
            stderr=stderr,
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


@dataclass(frozen=True)
class DockerOwnedContainerCleanupResult:
    run_id: str
    attempt_id: str | None
    container_ids: list[str]
    removed_container_ids: list[str]
    list_exit_code: int
    removal_exit_code: int | None
    stderr: str = ""


class DockerOwnedContainerCleaner:
    def __init__(
        self,
        *,
        runner: CommandRunner | None = None,
        timeout_seconds: int = 30,
    ) -> None:
        if timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")
        self.runner = runner or SubprocessCommandRunner()
        self.timeout_seconds = timeout_seconds

    def cleanup_run(
        self,
        *,
        run_id: str,
        attempt_id: str | None = None,
    ) -> DockerOwnedContainerCleanupResult:
        _require_non_empty("run_id", run_id)
        if attempt_id is not None:
            _require_non_empty("attempt_id", attempt_id)

        list_process = self.runner.run(
            _docker_ps_owned_container_args(run_id=run_id, attempt_id=attempt_id),
            timeout=self.timeout_seconds,
        )
        list_stdout = _coerce_output(list_process.stdout)
        list_stderr = _coerce_output(list_process.stderr)
        if list_process.returncode != 0:
            raise RuntimeError(f"docker owned-container list failed: {list_stderr.strip()}")

        container_ids = [line.strip() for line in list_stdout.splitlines() if line.strip()]
        if not container_ids:
            return DockerOwnedContainerCleanupResult(
                run_id=run_id,
                attempt_id=attempt_id,
                container_ids=[],
                removed_container_ids=[],
                list_exit_code=list_process.returncode,
                removal_exit_code=None,
                stderr=list_stderr,
            )

        remove_process = self.runner.run(
            ["docker", "rm", "-f", *container_ids],
            timeout=self.timeout_seconds,
        )
        remove_stdout = _coerce_output(remove_process.stdout)
        remove_stderr = _coerce_output(remove_process.stderr)
        if remove_process.returncode != 0:
            raise RuntimeError(f"docker owned-container cleanup failed: {remove_stderr.strip()}")

        removed_container_ids = [line.strip() for line in remove_stdout.splitlines() if line.strip()]
        return DockerOwnedContainerCleanupResult(
            run_id=run_id,
            attempt_id=attempt_id,
            container_ids=container_ids,
            removed_container_ids=removed_container_ids,
            list_exit_code=list_process.returncode,
            removal_exit_code=remove_process.returncode,
            stderr=remove_stderr,
        )


class DockerTerminalSandbox:
    def __init__(
        self,
        config: DockerTerminalSandboxConfig,
        *,
        runner: CommandRunner | None = None,
        lifecycle_recorder: SandboxLifecycleRecorder | None = None,
    ) -> None:
        self.config = config
        self.runner = runner or SubprocessCommandRunner()
        self.lifecycle_recorder = lifecycle_recorder
        self._command_index = 0
        self.workspace_path = config.workspace_root / config.run_id
        self.host_workspace_path = (config.host_workspace_root or config.workspace_root) / config.run_id
        self.cidfile_root = config.workspace_root / ".docker-cids" / config.run_id
        self.workspace_path.mkdir(parents=True, exist_ok=True)
        self.cidfile_root.mkdir(parents=True, exist_ok=True)

    def execute(self, command: str, *, cwd: str | None = None, timeout_seconds: int | None = None) -> SandboxCommandResult:
        _require_non_empty("command", command)

        container_cwd = cwd or self.config.container_workspace
        timeout = timeout_seconds or self.config.timeout_seconds
        if timeout <= 0:
            raise ValueError("timeout_seconds must be positive")

        sandbox_command_index = self._command_index
        self._command_index += 1
        before = self._workspace_index()
        started_at = datetime.now(timezone.utc)
        cidfile_path = self.cidfile_root / f"command-{sandbox_command_index}.cid"
        docker_args = self._docker_args(command, container_cwd, cidfile_path=cidfile_path)

        timed_out = False
        labels = _docker_owned_container_labels(
            run_id=self.config.run_id,
            attempt_id=self.config.attempt_id,
        )
        resource_limits = {
            "cpu_limit": self.config.cpu_limit,
            "memory_mb": self.config.memory_mb,
            "pids_limit": self.config.pids_limit,
        }
        metadata: dict[str, object] = {
            "image": self.config.image,
            "timeout_seconds": timeout,
            "internet_access": self.config.internet_access,
            "docker_labels": labels,
            "resource_limits": resource_limits,
            "sandbox_command_index": sandbox_command_index,
        }
        if self.lifecycle_recorder is not None:
            self.lifecycle_recorder.container_started(
                {
                    "sandbox_command_index": sandbox_command_index,
                    "sandbox_status": "running",
                    "image": self.config.image,
                    "cwd": container_cwd,
                    "timeout_seconds": timeout,
                    "internet_access": self.config.internet_access,
                    "docker_labels": labels,
                    "resource_limits": resource_limits,
                    "started_at": _datetime_json(started_at),
                }
            )

        resource_sample: dict[str, object] | None = None
        try:
            exit_code, stdout, stderr, resource_sample = self._run_docker_command_with_optional_resource_sample(
                docker_args=docker_args,
                timeout=timeout,
                cidfile_path=cidfile_path,
                sandbox_command_index=sandbox_command_index,
                resource_limits=resource_limits,
            )
        except subprocess.TimeoutExpired as exc:
            timed_out = True
            exit_code = 124
            stdout = _coerce_output(exc.output)
            stderr = _coerce_output(exc.stderr)
            timeout_message = f"Command timed out after {timeout} seconds"
            stderr = f"{stderr.rstrip()}\n{timeout_message}\n" if stderr else f"{timeout_message}\n"

        completed_at = datetime.now(timezone.utc)
        changed_paths = _changed_paths(before, self._workspace_index())
        container_id = _read_container_id(cidfile_path)
        if container_id is not None:
            metadata["container_id"] = container_id
        if resource_sample is not None:
            metadata["resource_sample"] = resource_sample
        _remove_file_if_present(cidfile_path)

        if self.lifecycle_recorder is not None:
            self.lifecycle_recorder.container_completed(
                {
                    "sandbox_command_index": sandbox_command_index,
                    "sandbox_status": "timed_out" if timed_out else "completed",
                    "image": self.config.image,
                    "container_id": container_id,
                    "exit_code": exit_code,
                    "timed_out": timed_out,
                    "changed_path_count": len(changed_paths),
                    "timeout_seconds": timeout,
                    "completed_at": _datetime_json(completed_at),
                    "duration_ms": int((completed_at - started_at).total_seconds() * 1000),
                    "resource_limits": resource_limits,
                }
            )

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

    def _run_docker_command_with_optional_resource_sample(
        self,
        *,
        docker_args: list[str],
        timeout: int,
        cidfile_path: Path,
        sandbox_command_index: int,
        resource_limits: dict[str, object],
    ) -> tuple[int, str, str, dict[str, object] | None]:
        start = getattr(self.runner, "start", None)
        if not callable(start):
            process = self.runner.run(docker_args, timeout=timeout)
            return process.returncode, _coerce_output(process.stdout), _coerce_output(process.stderr), None

        started_monotonic = time.monotonic()
        process = start(docker_args, env=None)
        container_id = _wait_for_container_id(cidfile_path, process, timeout_seconds=min(2.0, max(0.1, timeout / 10)))
        resource_sample = None
        if container_id:
            elapsed = time.monotonic() - started_monotonic
            resource_sample = self._sample_container_resources(
                container_id=container_id,
                sandbox_command_index=sandbox_command_index,
                resource_limits=resource_limits,
                timeout_seconds=max(0.1, min(1.0, timeout - elapsed)),
            )
        elapsed = time.monotonic() - started_monotonic
        remaining_timeout = max(0.1, timeout - elapsed)
        try:
            stdout, stderr = process.communicate(timeout=remaining_timeout)
        except subprocess.TimeoutExpired as exc:
            process.kill()
            stdout, stderr = process.communicate()
            raise subprocess.TimeoutExpired(
                cmd=docker_args,
                timeout=timeout,
                output=stdout,
                stderr=stderr,
            ) from exc
        return int(process.returncode or 0), _coerce_output(stdout), _coerce_output(stderr), resource_sample

    def _sample_container_resources(
        self,
        *,
        container_id: str,
        sandbox_command_index: int,
        resource_limits: dict[str, object],
        timeout_seconds: float,
    ) -> dict[str, object]:
        sampled_at = datetime.now(timezone.utc)
        base_metadata: dict[str, object] = {
            "sandbox_command_index": sandbox_command_index,
            "sandbox_status": "running",
            "container_id": container_id,
            "sampled_at": _datetime_json(sampled_at),
            "resource_limits": resource_limits,
        }
        try:
            process = self.runner.run(
                ["docker", "stats", "--no-stream", "--format", "json", container_id],
                timeout=timeout_seconds,
            )
            stdout = _coerce_output(process.stdout)
            stderr = _coerce_output(process.stderr)
            if process.returncode != 0:
                raise RuntimeError(stderr.strip() or "docker stats failed")
            sample = {
                **base_metadata,
                **_parse_docker_stats(stdout),
                "sample_status": "completed",
            }
        except Exception as exc:  # Docker stats must not change command success/failure.
            sample = {
                **base_metadata,
                "sample_status": "failed",
                "sample_error_reason": _bounded_text(str(exc), limit=240),
            }
        if self.lifecycle_recorder is not None:
            self.lifecycle_recorder.resource_sampled(sample)
        return sample

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

    def _docker_args(self, command: str, cwd: str, *, cidfile_path: Path) -> list[str]:
        args = [
            "docker",
            "run",
            "--rm",
            "--cidfile",
            str(cidfile_path),
            *[
                label_arg
                for key, value in _docker_owned_container_labels(
                    run_id=self.config.run_id,
                    attempt_id=self.config.attempt_id,
                ).items()
                for label_arg in ("--label", f"{key}={value}")
            ],
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


def docker_owned_container_labels(*, run_id: str, attempt_id: str | None = None) -> dict[str, str]:
    labels = {
        "com.agentic-data-platform.managed": "true",
        "com.agentic-data-platform.run_id": run_id,
        "com.agentic-data-platform.resource": "sandbox-container",
    }
    if attempt_id is not None:
        labels["com.agentic-data-platform.attempt_id"] = attempt_id
    return labels


def _docker_owned_container_labels(*, run_id: str, attempt_id: str | None = None) -> dict[str, str]:
    return docker_owned_container_labels(run_id=run_id, attempt_id=attempt_id)


def _docker_ps_owned_container_args(*, run_id: str, attempt_id: str | None = None) -> list[str]:
    filters = [
        "label=com.agentic-data-platform.managed=true",
        f"label=com.agentic-data-platform.run_id={run_id}",
        "label=com.agentic-data-platform.resource=sandbox-container",
    ]
    if attempt_id is not None:
        filters.append(f"label=com.agentic-data-platform.attempt_id={attempt_id}")
    args = ["docker", "ps", "-aq"]
    for item in filters:
        args.extend(["--filter", item])
    return args


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


def _read_container_id(path: Path) -> str | None:
    try:
        value = path.read_text(encoding="utf-8").strip()
    except OSError:
        return None
    return value or None


def _wait_for_container_id(path: Path, process: Any, *, timeout_seconds: float) -> str | None:
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() <= deadline:
        container_id = _read_container_id(path)
        if container_id:
            return container_id
        poll = getattr(process, "poll", None)
        if callable(poll) and poll() is not None:
            return _read_container_id(path)
        time.sleep(0.05)
    return _read_container_id(path)


def _parse_docker_stats(payload: str) -> dict[str, object]:
    line = next((item.strip() for item in payload.splitlines() if item.strip()), "")
    if not line:
        raise ValueError("docker stats returned no JSON payload")
    raw = json.loads(line)
    if not isinstance(raw, dict):
        raise ValueError("docker stats payload must be a JSON object")

    metadata: dict[str, object] = {}
    cpu_percent = _parse_percent(raw.get("CPUPerc"))
    if cpu_percent is not None:
        metadata["cpu_percent"] = cpu_percent

    memory_used, memory_limit = _parse_usage_pair(raw.get("MemUsage"))
    if memory_used is not None:
        metadata["memory_used_bytes"] = memory_used
    if memory_limit is not None:
        metadata["memory_limit_bytes"] = memory_limit

    memory_percent = _parse_percent(raw.get("MemPerc"))
    if memory_percent is not None:
        metadata["memory_percent"] = memory_percent

    net_input, net_output = _parse_usage_pair(raw.get("NetIO"))
    if net_input is not None:
        metadata["network_rx_bytes"] = net_input
    if net_output is not None:
        metadata["network_tx_bytes"] = net_output

    block_input, block_output = _parse_usage_pair(raw.get("BlockIO"))
    if block_input is not None:
        metadata["block_read_bytes"] = block_input
    if block_output is not None:
        metadata["block_write_bytes"] = block_output

    pids = _parse_int(raw.get("PIDs"))
    if pids is not None:
        metadata["pids"] = pids

    return metadata


def _parse_percent(value: object) -> float | None:
    if value is None:
        return None
    text = str(value).strip()
    if text.endswith("%"):
        text = text[:-1]
    try:
        return round(float(text), 4)
    except ValueError:
        return None


def _parse_usage_pair(value: object) -> tuple[int | None, int | None]:
    if value is None:
        return None, None
    parts = [part.strip() for part in str(value).split("/", maxsplit=1)]
    first = _parse_size_bytes(parts[0]) if parts else None
    second = _parse_size_bytes(parts[1]) if len(parts) > 1 else None
    return first, second


def _parse_size_bytes(value: object) -> int | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    number_text = ""
    unit_text = ""
    for char in text:
        if char.isdigit() or char in {".", "-"}:
            number_text += char
        elif not char.isspace():
            unit_text += char
    try:
        number = float(number_text)
    except ValueError:
        return None
    units = {
        "B": 1,
        "kB": 1000,
        "KB": 1000,
        "KiB": 1024,
        "MB": 1000**2,
        "MiB": 1024**2,
        "GB": 1000**3,
        "GiB": 1024**3,
        "TB": 1000**4,
        "TiB": 1024**4,
    }
    multiplier = units.get(unit_text or "B")
    if multiplier is None:
        return None
    return int(number * multiplier)


def _parse_int(value: object) -> int | None:
    try:
        return int(str(value).strip())
    except (TypeError, ValueError):
        return None


def _bounded_text(value: str, *, limit: int) -> str:
    if len(value) <= limit:
        return value
    return value[: limit - 3] + "..."


def _remove_file_if_present(path: Path) -> None:
    try:
        path.unlink()
    except FileNotFoundError:
        return


def _datetime_json(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _require_non_empty(name: str, value: str) -> None:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} must be a non-empty string")
