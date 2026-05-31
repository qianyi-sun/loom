from __future__ import annotations

import argparse
import json
import os
import shlex
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from agentic_data_platform.providers.config import redact_sensitive_metadata

_UPSTREAM_CONFIG_ARTIFACT = "upstream-config.json"


@dataclass(frozen=True)
class WrapperPaths:
    task_manifest: Path
    workspace: Path
    output: Path
    artifacts_dir: Path
    upstream_root: Path | None
    timeout_seconds: int
    dry_run: bool


@dataclass(frozen=True)
class WrapperTaskManifest:
    run_id: str
    suite_name: str
    benchmark_version: str
    source_uri: str
    source_version: str
    task_family: str
    instance_id: str
    instruction_ref: str
    input_files: list[str]
    model: dict[str, Any]
    output_dir: str
    artifacts_dir: str


def parse_wrapper_args(argv: list[str] | None) -> WrapperPaths:
    parser = argparse.ArgumentParser(description="Run an original benchmark wrapper.")
    parser.add_argument("--task-manifest", required=True)
    parser.add_argument("--workspace", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--artifacts-dir", required=True)
    parser.add_argument("--upstream-root")
    parser.add_argument("--timeout-seconds", type=int, default=3600)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)
    return WrapperPaths(
        task_manifest=Path(args.task_manifest),
        workspace=Path(args.workspace),
        output=Path(args.output),
        artifacts_dir=Path(args.artifacts_dir),
        upstream_root=Path(args.upstream_root) if args.upstream_root else None,
        timeout_seconds=args.timeout_seconds,
        dry_run=args.dry_run,
    )


def load_task_manifest(path: Path) -> WrapperTaskManifest:
    data = json.loads(path.read_text(encoding="utf-8"))
    return WrapperTaskManifest(
        run_id=_require_string(data, "run_id"),
        suite_name=_require_string(data, "suite_name"),
        benchmark_version=_require_string(data, "benchmark_version"),
        source_uri=_require_string(data, "source_uri"),
        source_version=_require_string(data, "source_version"),
        task_family=_require_string(data, "task_family"),
        instance_id=_require_string(data, "instance_id"),
        instruction_ref=_require_string(data, "instruction_ref"),
        input_files=_require_string_list(data, "input_files"),
        model=_require_dict(data, "model"),
        output_dir=_require_string(data, "output_dir"),
        artifacts_dir=_require_string(data, "artifacts_dir"),
    )


def run_dry_wrapper(
    *,
    expected_suite: str,
    argv: list[str] | None,
    planned_command: list[str],
) -> int:
    paths = parse_wrapper_args(argv)
    manifest = load_task_manifest(paths.task_manifest)
    return run_wrapper(
        expected_suite=expected_suite,
        paths=paths,
        manifest=manifest,
        planned_command=planned_command,
    )


def run_wrapper(
    *,
    expected_suite: str,
    paths: WrapperPaths,
    manifest: WrapperTaskManifest,
    planned_command: list[str],
) -> int:
    if manifest.suite_name != expected_suite:
        raise ValueError(f"{expected_suite} wrapper received {manifest.suite_name} manifest")

    paths.workspace.mkdir(parents=True, exist_ok=True)
    paths.output.parent.mkdir(parents=True, exist_ok=True)
    paths.artifacts_dir.mkdir(parents=True, exist_ok=True)
    _write_upstream_config(paths=paths, manifest=manifest)
    _write_planned_command(paths=paths, manifest=manifest, planned_command=planned_command)

    if paths.dry_run:
        _write_result(
            paths=paths,
            manifest=manifest,
            planned_command=planned_command,
            dry_run=True,
            exit_code=0,
            stdout="",
            stderr="",
            failure_reason=None,
        )
        return 0

    if paths.upstream_root is None:
        raise ValueError("--upstream-root is required for executable wrapper runs")
    if not paths.upstream_root.exists() or not paths.upstream_root.is_dir():
        raise ValueError(f"--upstream-root must be an existing directory: {paths.upstream_root}")

    try:
        completed = subprocess.run(
            planned_command,
            cwd=paths.upstream_root,
            env=_execution_env(paths=paths, manifest=manifest),
            capture_output=True,
            text=True,
            timeout=paths.timeout_seconds,
            check=False,
        )
    except subprocess.TimeoutExpired as error:
        stdout = _process_output(error.stdout)
        stderr = _process_output(error.stderr)
        _write_process_logs(paths=paths, stdout=stdout, stderr=stderr)
        _write_result(
            paths=paths,
            manifest=manifest,
            planned_command=planned_command,
            dry_run=False,
            exit_code=124,
            stdout=stdout,
            stderr=stderr,
            failure_reason=f"upstream runner timed out after {paths.timeout_seconds} seconds",
        )
        return 124

    stdout = completed.stdout
    stderr = completed.stderr
    _write_process_logs(paths=paths, stdout=stdout, stderr=stderr)

    failure_reason = None
    if completed.returncode != 0:
        failure_reason = f"upstream runner exited with code {completed.returncode}"

    _write_result(
        paths=paths,
        manifest=manifest,
        planned_command=planned_command,
        dry_run=False,
        exit_code=completed.returncode,
        stdout=stdout,
        stderr=stderr,
        failure_reason=failure_reason,
    )
    return completed.returncode


def _write_process_logs(*, paths: WrapperPaths, stdout: str, stderr: str) -> None:
    (paths.artifacts_dir / "stdout.log").write_text(stdout, encoding="utf-8")
    (paths.artifacts_dir / "stderr.log").write_text(stderr, encoding="utf-8")


def upstream_config_path(paths: WrapperPaths) -> Path:
    return paths.artifacts_dir / _UPSTREAM_CONFIG_ARTIFACT


def _write_upstream_config(*, paths: WrapperPaths, manifest: WrapperTaskManifest) -> None:
    config = {
        "schema_version": "adp.wrapper_config.v1",
        "run_id": manifest.run_id,
        "suite_name": manifest.suite_name,
        "benchmark_version": manifest.benchmark_version,
        "source_uri": manifest.source_uri,
        "source_version": manifest.source_version,
        "task_family": manifest.task_family,
        "instance_id": manifest.instance_id,
        "instruction_ref": manifest.instruction_ref,
        "input_files": manifest.input_files,
        "model": redact_sensitive_metadata(manifest.model),
        "execution": {
            "output_dir": manifest.output_dir,
            "artifacts_dir": manifest.artifacts_dir,
        },
        "environment_contract": [
            "ADP_RUN_ID",
            "ADP_SUITE_NAME",
            "ADP_TASK_FAMILY",
            "ADP_INSTANCE_ID",
            "ADP_WORKSPACE",
            "ADP_OUTPUT_DIR",
            "ADP_ARTIFACTS_DIR",
            "ADP_MODEL_PROVIDER",
            "ADP_MODEL_NAME",
        ],
    }
    upstream_config_path(paths).write_text(
        json.dumps(config, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def _process_output(output: bytes | str | None) -> str:
    if output is None:
        return ""
    if isinstance(output, bytes):
        return output.decode("utf-8", errors="replace")
    return output


def _write_planned_command(
    *,
    paths: WrapperPaths,
    manifest: WrapperTaskManifest,
    planned_command: list[str],
) -> None:
    (paths.artifacts_dir / "planned-command.json").write_text(
        json.dumps(
            {
                "run_id": manifest.run_id,
                "suite_name": manifest.suite_name,
                "task_family": manifest.task_family,
                "instance_id": manifest.instance_id,
                "planned_command": planned_command,
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )


def _write_result(
    *,
    paths: WrapperPaths,
    manifest: WrapperTaskManifest,
    planned_command: list[str],
    dry_run: bool,
    exit_code: int,
    stdout: str,
    stderr: str,
    failure_reason: str | None,
) -> None:
    artifacts = [
        {
            "kind": "log",
            "path": "artifacts/planned-command.json",
            "media_type": "application/json",
        },
        {
            "kind": "runner_config",
            "path": f"artifacts/{_UPSTREAM_CONFIG_ARTIFACT}",
            "media_type": "application/json",
        }
    ]
    if not dry_run:
        artifacts.extend(
            [
                {
                    "kind": "log",
                    "path": "artifacts/stdout.log",
                    "media_type": "text/plain",
                },
                {
                    "kind": "log",
                    "path": "artifacts/stderr.log",
                    "media_type": "text/plain",
                },
            ]
        )

    result = {
        "status": "completed" if exit_code == 0 else "failed",
        "dry_run": dry_run,
        "exit_code": exit_code,
        "suite_name": manifest.suite_name,
        "benchmark_version": manifest.benchmark_version,
        "source_uri": manifest.source_uri,
        "source_version": manifest.source_version,
        "task_family": manifest.task_family,
        "instance_id": manifest.instance_id,
        "instruction_ref": manifest.instruction_ref,
        "input_files": manifest.input_files,
        "model": manifest.model,
        "metrics": {},
        "artifacts": artifacts,
        "planned_command": shlex.join(planned_command),
        "stdout": stdout,
        "stderr": stderr,
        "trajectory_ref": None,
        "workspace_ref": None,
        "evaluator_report_ref": None,
        "failure_reason": failure_reason,
    }
    paths.output.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _execution_env(*, paths: WrapperPaths, manifest: WrapperTaskManifest) -> dict[str, str]:
    env = dict(os.environ)
    provider = manifest.model.get("provider")
    model_name = manifest.model.get("model_name")
    env.update(
        {
            "ADP_RUN_ID": manifest.run_id,
            "ADP_SUITE_NAME": manifest.suite_name,
            "ADP_TASK_FAMILY": manifest.task_family,
            "ADP_INSTANCE_ID": manifest.instance_id,
            "ADP_WORKSPACE": str(paths.workspace),
            "ADP_OUTPUT_DIR": manifest.output_dir,
            "ADP_ARTIFACTS_DIR": str(paths.artifacts_dir),
        }
    )
    if isinstance(provider, str) and provider.strip():
        env["ADP_MODEL_PROVIDER"] = provider
    if isinstance(model_name, str) and model_name.strip():
        env["ADP_MODEL_NAME"] = model_name
    return env


def _require_string(data: dict[str, Any], key: str) -> str:
    value = data.get(key)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{key} must be a non-empty string")
    return value


def _require_string_list(data: dict[str, Any], key: str) -> list[str]:
    value = data.get(key)
    if isinstance(value, str) or not isinstance(value, list) or not value:
        raise ValueError(f"{key} must be a non-empty list of strings")
    for item in value:
        if not isinstance(item, str) or not item.strip():
            raise ValueError(f"{key} must be a non-empty list of strings")
    return value


def _require_dict(data: dict[str, Any], key: str) -> dict[str, Any]:
    value = data.get(key)
    if not isinstance(value, dict):
        raise ValueError(f"{key} must be an object")
    return value
