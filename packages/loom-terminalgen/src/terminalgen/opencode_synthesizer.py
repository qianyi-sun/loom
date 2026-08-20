from __future__ import annotations

import ast
import json
import subprocess
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from terminalgen.models import DatasetTask, GenerationMode, GenerationRequest


DEFAULT_MAX_ARTIFACT_BYTES = 100 * 1024 * 1024
DEFAULT_MAX_TOTAL_ARTIFACT_BYTES = 250 * 1024 * 1024
FORBIDDEN_WORKSPACE_DIR_NAMES = {
    ".pytest_cache",
    ".terminus2_tests",
    "__pycache__",
}
FORBIDDEN_WORKSPACE_FILE_NAMES = {
    "test_outputs.py",
    "terminus2_test_task.py",
}
FORBIDDEN_WORKSPACE_FILE_PREFIXES = (
    "expected",
    "_expected",
    "answer",
    "_answer",
    "solution",
    "_solution",
)
FORBIDDEN_WORKSPACE_FILE_SUBSTRINGS = (
    "ground_truth",
    "_reference_model",
)


@dataclass
class OpencodeConfig:
    model: str
    call_log_dir: Path
    staging_dir: Path
    command: str = "opencode"
    max_retries: int = 3
    timeout_sec: float = 1800.0
    max_artifact_bytes: int = DEFAULT_MAX_ARTIFACT_BYTES
    max_total_artifact_bytes: int = DEFAULT_MAX_TOTAL_ARTIFACT_BYTES
    pure: bool = True
    skip_permissions: bool = True


class OpencodeTaskSynthesizer:
    def __init__(self, config: OpencodeConfig) -> None:
        self.config = config
        self._call_index = 0
        self._lock = threading.Lock()
        self._stats = {
            "call_count": 0,
            "failed_calls": 0,
            "accepted_packages": 0,
        }

    def generate_task(
        self,
        *,
        spec: GenerationRequest,
        system_prompt: str,
        user_prompt: str,
        base_image: str,
        **_: Any,
    ) -> DatasetTask:
        last_error: Exception | None = None
        for attempt in range(1, self.config.max_retries + 1):
            call_index = self._next_call_index()
            staging = self.config.staging_dir / f"{call_index:06d}-attempt-{attempt}"
            started_at = time.perf_counter()
            command: list[str] = []
            stdout = ""
            stderr = ""
            returncode: int | None = None
            error: Exception | None = None
            try:
                staging.mkdir(parents=True, exist_ok=False)
                command = self._build_command(staging, spec, user_prompt)
                completed = subprocess.run(
                    command,
                    text=True,
                    capture_output=True,
                    timeout=self.config.timeout_sec,
                    check=False,
                )
                stdout = completed.stdout
                stderr = completed.stderr
                returncode = completed.returncode
                if completed.returncode != 0:
                    raise RuntimeError(f"opencode exited with code {completed.returncode}")
                task = self._load_task_from_staging(
                    staging,
                    require_solution=spec.generation_mode == GenerationMode.ATOMIC_TARGET,
                )
                self._record_attempt(
                    call_index=call_index,
                    attempt=attempt,
                    staging=staging,
                    command=command,
                    started_at=started_at,
                    stdout=stdout,
                    stderr=stderr,
                    returncode=returncode,
                    error=None,
                )
                with self._lock:
                    self._stats["accepted_packages"] += 1
                return task
            except Exception as exc:  # noqa: BLE001
                last_error = exc
                error = exc
                self._record_attempt(
                    call_index=call_index,
                    attempt=attempt,
                    staging=staging,
                    command=command,
                    started_at=started_at,
                    stdout=stdout,
                    stderr=stderr,
                    returncode=returncode,
                    error=error,
                )
                if attempt >= self.config.max_retries:
                    break
        assert last_error is not None
        raise last_error

    def stats_snapshot(self) -> dict[str, Any]:
        with self._lock:
            return dict(self._stats)

    def _next_call_index(self) -> int:
        with self._lock:
            self._call_index += 1
            return self._call_index

    def _build_command(self, staging: Path, spec: GenerationRequest, user_prompt: str) -> list[str]:
        command = [
            self.config.command,
            "run",
            "--dir",
            str(staging),
            "--format",
            "json",
            "--model",
            self.config.model,
            "--title",
            f"terminalgen-{spec.sample_index:06d}",
        ]
        if self.config.pure:
            command.append("--pure")
        if self.config.skip_permissions:
            command.append("--dangerously-skip-permissions")
        command.append(user_prompt)
        return command

    def _load_task_from_staging(
        self,
        staging: Path,
        *,
        require_solution: bool = False,
    ) -> DatasetTask:
        manifest_path = staging / "task.json"
        instruction_path = staging / "instruction.md"
        tests_path = staging / "tests" / "test_outputs.py"
        workspace_dir = staging / "workspace"
        solution_path = staging / "solution" / "solve.sh"

        manifest = _read_json_object(manifest_path)
        task_id = _required_str(manifest, "task_id")
        prompt = _normalize_instruction(_read_required_text(instruction_path))
        tests = _read_required_text(tests_path)
        ast.parse(tests)
        if not workspace_dir.is_dir():
            raise ValueError("workspace/ directory is required")
        self._validate_workspace_tree(workspace_dir)
        solution = None
        if solution_path.exists():
            solution = _read_required_text(solution_path)
        elif require_solution:
            raise ValueError("required file missing: solution/solve.sh")

        info = manifest.get("info")
        if info is not None and not isinstance(info, dict):
            raise ValueError("task.json info must be an object")
        test_requirements = manifest.get("test_requirements", ["pytest"])
        if not isinstance(test_requirements, list) or not all(
            isinstance(item, str) for item in test_requirements
        ):
            raise ValueError("task.json test_requirements must be a list of strings")
        sources = manifest.get("sources", [])
        if sources is not None and not isinstance(sources, list):
            raise ValueError("task.json sources must be a list")

        task = DatasetTask.model_validate(
            {
                "task_id": task_id,
                "prompt": prompt,
                "tests": tests,
                "info": info or {},
                "files": [],
                "test_requirements": test_requirements,
                "solution": solution,
                "extra": {"sources": sources or []},
            }
        )
        task.workspace_dir = workspace_dir
        return task

    def _validate_workspace_tree(self, workspace_dir: Path) -> None:
        root = workspace_dir.resolve()
        total_size = 0
        for path in workspace_dir.rglob("*"):
            if path.is_symlink():
                raise ValueError(f"workspace path cannot be a symlink: {path}")
            resolved = path.resolve()
            if not _is_relative_to(resolved, root):
                raise ValueError(f"workspace path escapes workspace root: {path}")
            relative_path = resolved.relative_to(root)
            self._validate_workspace_relative_path(relative_path, path)
            if path.is_dir():
                continue
            if not path.is_file():
                raise ValueError(f"workspace path must be a regular file or directory: {path}")
            size = path.stat().st_size
            if size > self.config.max_artifact_bytes:
                raise ValueError(f"workspace file exceeds max artifact size: {path}")
            total_size += size
            if total_size > self.config.max_total_artifact_bytes:
                raise ValueError("workspace exceeds max total artifact size")

    def _validate_workspace_relative_path(self, relative_path: Path, original_path: Path) -> None:
        parts = [part.lower() for part in relative_path.parts]
        name = parts[-1]
        if any(part in FORBIDDEN_WORKSPACE_DIR_NAMES for part in parts):
            raise ValueError(
                f"workspace/ must not contain test cache or hidden test assets: {original_path}"
            )
        if name.endswith(".pyc") or name in FORBIDDEN_WORKSPACE_FILE_NAMES:
            raise ValueError(
                f"workspace/ must not contain hidden/evaluation test files: {original_path}"
            )
        if name.startswith(FORBIDDEN_WORKSPACE_FILE_PREFIXES):
            raise ValueError(
                f"workspace/ must not contain answer or expected-output artifacts: {original_path}"
            )
        if any(fragment in name for fragment in FORBIDDEN_WORKSPACE_FILE_SUBSTRINGS):
            raise ValueError(
                f"workspace/ must not contain ground-truth or reference artifacts: {original_path}"
            )

    def _record_attempt(
        self,
        *,
        call_index: int,
        attempt: int,
        staging: Path,
        command: list[str],
        started_at: float,
        stdout: str,
        stderr: str,
        returncode: int | None,
        error: Exception | None,
    ) -> None:
        finished_at = time.perf_counter()
        with self._lock:
            self._stats["call_count"] += 1
            if error is not None:
                self._stats["failed_calls"] += 1
        self.config.call_log_dir.mkdir(parents=True, exist_ok=True)
        payload = {
            "call_index": call_index,
            "timestamp": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            "attempt": attempt,
            "model": self.config.model,
            "command": command,
            "staging_dir": str(staging),
            "duration_sec": round(finished_at - started_at, 6),
            "returncode": returncode,
            "stdout_log": f"{call_index:06d}.opencode.stdout.jsonl",
            "stderr_log": f"{call_index:06d}.opencode.stderr.log",
            "error": _serialize_error(error),
            "retry": {
                "will_retry": error is not None and attempt < self.config.max_retries,
                "max_retries": self.config.max_retries,
            },
        }
        (self.config.call_log_dir / f"{call_index:06d}.json").write_text(
            json.dumps(payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        (self.config.call_log_dir / f"{call_index:06d}.opencode.stdout.jsonl").write_text(
            stdout,
            encoding="utf-8",
        )
        (self.config.call_log_dir / f"{call_index:06d}.opencode.stderr.log").write_text(
            stderr,
            encoding="utf-8",
        )


def _read_json_object(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise ValueError(f"required file missing: {path.name}")
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"{path.name} must contain a JSON object")
    return payload


def _required_str(payload: dict[str, Any], field_name: str) -> str:
    value = payload.get(field_name)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"task.json {field_name} must be a non-empty string")
    return value.strip()


def _read_required_text(path: Path) -> str:
    if not path.exists():
        raise ValueError(f"required file missing: {path}")
    text = path.read_text(encoding="utf-8").strip()
    if not text:
        raise ValueError(f"required file is empty: {path}")
    return text


def _normalize_instruction(text: str) -> str:
    marker = "Work in /app."
    lines = text.splitlines()
    if lines and lines[0].strip() == marker:
        return text
    body = "\n".join(line for line in lines if line.strip() != marker).strip()
    return f"{marker}\n\n{body}" if body else marker


def _is_relative_to(path: Path, root: Path) -> bool:
    try:
        path.relative_to(root)
    except ValueError:
        return False
    return True


def _serialize_error(error: Exception | None) -> dict[str, str] | None:
    if error is None:
        return None
    return {"type": type(error).__name__, "message": str(error)}
