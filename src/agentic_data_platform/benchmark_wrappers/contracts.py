from __future__ import annotations

import argparse
import json
import shlex
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class WrapperPaths:
    task_manifest: Path
    workspace: Path
    output: Path
    artifacts_dir: Path
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
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)
    return WrapperPaths(
        task_manifest=Path(args.task_manifest),
        workspace=Path(args.workspace),
        output=Path(args.output),
        artifacts_dir=Path(args.artifacts_dir),
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
    if not paths.dry_run:
        raise ValueError("Only --dry-run wrapper execution is implemented in the MVP")

    manifest = load_task_manifest(paths.task_manifest)
    if manifest.suite_name != expected_suite:
        raise ValueError(f"{expected_suite} wrapper received {manifest.suite_name} manifest")

    paths.workspace.mkdir(parents=True, exist_ok=True)
    paths.output.parent.mkdir(parents=True, exist_ok=True)
    paths.artifacts_dir.mkdir(parents=True, exist_ok=True)

    planned_command_path = paths.artifacts_dir / "planned-command.json"
    planned_command_text = shlex.join(planned_command)
    planned_command_path.write_text(
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

    result = {
        "status": "completed",
        "dry_run": True,
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
        "artifacts": [
            {
                "kind": "log",
                "path": "artifacts/planned-command.json",
                "media_type": "application/json",
            }
        ],
        "planned_command": planned_command_text,
        "trajectory_ref": None,
        "workspace_ref": None,
        "evaluator_report_ref": None,
        "failure_reason": None,
    }
    paths.output.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return 0


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
