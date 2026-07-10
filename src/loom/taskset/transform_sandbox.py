"""Sandboxed transform(row) execution for TaskSet materialization (#242 sub-plan 4)."""

from __future__ import annotations

import json
import logging
import os
import resource
import subprocess
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from loom.workload_trust import INTERNAL_TRUSTED, WorkloadTrustContract

logger = logging.getLogger(__name__)

_STDERR_CAP = 2048
_HARNESS_NAME = "loom_transform_harness.py"
_USER_SCRIPT_NAME = "transform.py"
_CLONE_NEWNET = 0x40000000
_V1_INTERNAL_TRUSTED_CONTRACT = WorkloadTrustContract(
    workload_trust_mode=INTERNAL_TRUSTED,
    taskset_transforms_enabled=False,
    taskset_transform_network_isolated=False,
    untrusted_workload_isolation=False,
)


@dataclass(frozen=True)
class TransformSandboxConfig:
    enabled: bool
    network_isolated: bool
    workload_contract: WorkloadTrustContract = _V1_INTERNAL_TRUSTED_CONTRACT
    wall_timeout_sec: int = 30
    cpu_limit_sec: int = 10
    memory_limit_mb: int = 256


class TransformSandboxError(Exception):
    """Per-row transform failure surfaced to materialization error_summary."""

    def __init__(self, *, code: str, message: str, stderr: str = "") -> None:
        self.code = code
        self.message = message
        self.stderr = stderr
        super().__init__(message)


_HARNESS_SOURCE = '''\
"""Fixed harness — imports user transform.py and runs transform(row)."""
from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path


def main() -> None:
    spec = importlib.util.spec_from_file_location(
        "user_transform",
        Path(__file__).with_name("transform.py"),
    )
    if spec is None or spec.loader is None:
        raise RuntimeError("failed to load transform.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    if not hasattr(module, "transform"):
        raise AttributeError("transform.py must define transform(row)")
    transform = module.transform
    row = json.load(sys.stdin)
    if not isinstance(row, dict):
        raise TypeError(f"transform input must be dict, got {type(row).__name__}")
    out = transform(row)
    if not isinstance(out, dict):
        raise TypeError(f"transform must return dict, got {type(out).__name__}")
    json.dump(out, sys.stdout)


if __name__ == "__main__":
    main()
'''


def _truncate_stderr(stderr: bytes) -> str:
    text = stderr.decode("utf-8", errors="replace").strip()
    if len(text) > _STDERR_CAP:
        return text[: _STDERR_CAP - 3] + "..."
    return text


def _try_unshare_network() -> None:
    if not hasattr(os, "unshare"):
        return
    try:
        os.unshare(_CLONE_NEWNET)
    except OSError as exc:
        logger.warning("transform sandbox: os.unshare(CLONE_NEWNET) failed: %s", exc)


def _child_setup(
    *,
    cpu_limit_sec: int,
    memory_limit_mb: int,
    network_isolated: bool,
) -> None:
    if network_isolated:
        _try_unshare_network()
    try:
        resource.setrlimit(resource.RLIMIT_CPU, (cpu_limit_sec, cpu_limit_sec))
    except (ValueError, OSError):
        pass
    memory_bytes = memory_limit_mb * 1024 * 1024
    try:
        resource.setrlimit(resource.RLIMIT_AS, (memory_bytes, memory_bytes))
    except (ValueError, OSError, AttributeError):
        pass
    try:
        resource.setrlimit(resource.RLIMIT_NOFILE, (64, 64))
    except (ValueError, OSError):
        pass


def _effective_timeout_sec(
    *,
    config: TransformSandboxConfig,
    manifest_timeout_s: int | None,
) -> float:
    if manifest_timeout_s is None:
        return float(config.wall_timeout_sec)
    return float(min(config.wall_timeout_sec, manifest_timeout_s))


def run_transform(
    *,
    transform_script: bytes,
    row: dict[str, Any],
    config: TransformSandboxConfig,
    manifest_timeout_s: int | None,
) -> dict[str, Any]:
    """Run user transform(row) in a constrained subprocess; return transformed row."""
    timeout_sec = _effective_timeout_sec(
        config=config,
        manifest_timeout_s=manifest_timeout_s,
    )
    with tempfile.TemporaryDirectory(prefix="loom-transform-") as tmp:
        work_dir = Path(tmp)
        (work_dir / _USER_SCRIPT_NAME).write_bytes(transform_script)
        (work_dir / _HARNESS_NAME).write_text(_HARNESS_SOURCE, encoding="utf-8")
        harness_path = work_dir / _HARNESS_NAME
        try:
            completed = subprocess.run(
                [sys.executable, str(harness_path)],
                input=json.dumps(row).encode("utf-8"),
                capture_output=True,
                cwd=work_dir,
                env={},
                timeout=timeout_sec,
                check=False,
                preexec_fn=lambda: _child_setup(
                    cpu_limit_sec=config.cpu_limit_sec,
                    memory_limit_mb=config.memory_limit_mb,
                    network_isolated=config.network_isolated,
                ),
            )
        except subprocess.TimeoutExpired as exc:
            stderr = _truncate_stderr(exc.stderr or b"")
            raise TransformSandboxError(
                code="transform_limit_exceeded",
                message=f"transform exceeded wall timeout ({timeout_sec}s)",
                stderr=stderr,
            ) from exc

        stderr = _truncate_stderr(completed.stderr)
        if completed.returncode != 0:
            if completed.returncode < 0:
                raise TransformSandboxError(
                    code="transform_limit_exceeded",
                    message=f"transform killed by signal {-completed.returncode}",
                    stderr=stderr,
                )
            detail = stderr or f"exit code {completed.returncode}"
            raise TransformSandboxError(
                code="transform_error",
                message=detail,
                stderr=stderr,
            )

        try:
            parsed = json.loads(completed.stdout.decode("utf-8"))
        except json.JSONDecodeError as exc:
            raise TransformSandboxError(
                code="transform_error",
                message=f"transform stdout is not valid json: {exc}",
                stderr=stderr,
            ) from exc
        if not isinstance(parsed, dict):
            raise TransformSandboxError(
                code="transform_error",
                message=f"transform must return dict, got {type(parsed).__name__}",
                stderr=stderr,
            )
        return parsed
