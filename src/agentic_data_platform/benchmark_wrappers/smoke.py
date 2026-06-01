from __future__ import annotations

import argparse
import json
import os
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from uuid import uuid4

from agentic_data_platform.benchmark_wrappers.skillflow import main as skillflow_main
from agentic_data_platform.benchmark_wrappers.skilllearnbench import main as skilllearnbench_main
from agentic_data_platform.benchmarks.fixtures import load_fixture_catalog


@dataclass(frozen=True)
class BenchmarkWrapperSmokeConfig:
    suite_name: str
    task_family: str
    instance_id: str
    workspace_root: Path
    run_id: str
    upstream_root: Path | None = None
    dry_run: bool = False
    timeout_seconds: int = 3600


def run_benchmark_wrapper_smoke(config: BenchmarkWrapperSmokeConfig) -> dict[str, Any]:
    _validate_config(config)
    run_root = config.workspace_root / config.run_id
    if run_root.exists():
        shutil.rmtree(run_root)

    input_dir = run_root / "input"
    sandbox_workspace = run_root / "workspace"
    wrapper_output_dir = run_root / "runner-output"
    artifacts_dir = run_root / "artifacts"
    input_dir.mkdir(parents=True)

    manifest_path = input_dir / "task.json"
    manifest = _task_manifest(config=config, output_dir=wrapper_output_dir)
    manifest_path.write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    output_path = run_root / "result.json"

    argv = [
        "--task-manifest",
        str(manifest_path),
        "--workspace",
        str(sandbox_workspace),
        "--output",
        str(output_path),
        "--artifacts-dir",
        str(artifacts_dir),
        "--timeout-seconds",
        str(config.timeout_seconds),
    ]
    if config.dry_run:
        argv.append("--dry-run")
    else:
        if config.upstream_root is None:
            raise ValueError("upstream_root is required unless dry_run is true")
        argv.extend(["--upstream-root", str(config.upstream_root)])

    exit_code = _wrapper_main(config.suite_name)(argv)
    result = json.loads(output_path.read_text(encoding="utf-8"))
    return {
        "run_id": config.run_id,
        "suite_name": config.suite_name,
        "task_family": config.task_family,
        "instance_id": config.instance_id,
        "status": "succeeded" if exit_code == 0 and result.get("status") == "completed" else "failed",
        "dry_run": bool(result.get("dry_run")),
        "exit_code": exit_code,
        "planned_command": result.get("planned_command", ""),
        "stdout": result.get("stdout", ""),
        "stderr": result.get("stderr", ""),
        "failure_reason": result.get("failure_reason"),
        "artifact_paths": [artifact["path"] for artifact in result.get("artifacts", []) if isinstance(artifact, dict)],
        "result_path": str(output_path),
        "artifacts_dir": str(artifacts_dir),
    }


def main(argv: list[str] | None = None) -> int:
    result = run_benchmark_wrapper_smoke(_config_from_args(argv))
    print(json.dumps(result, sort_keys=True), flush=True)
    return 0 if result["status"] == "succeeded" else 1


def _config_from_args(argv: list[str] | None = None) -> BenchmarkWrapperSmokeConfig:
    parser = argparse.ArgumentParser(description="Run a SkillFlow/SkillLearnBench wrapper smoke check.")
    parser.add_argument("--suite", default=_env("BENCHMARK_WRAPPER_SMOKE_SUITE", "SkillLearnBench"))
    parser.add_argument("--task-family", default=os.environ.get("BENCHMARK_WRAPPER_SMOKE_TASK_FAMILY", ""))
    parser.add_argument("--instance-id", default=os.environ.get("BENCHMARK_WRAPPER_SMOKE_INSTANCE_ID", ""))
    parser.add_argument(
        "--workspace-root",
        default=_env("BENCHMARK_WRAPPER_SMOKE_WORKSPACE_ROOT", ".runtime/benchmark-wrapper-smoke"),
    )
    parser.add_argument("--run-id", default=_env("BENCHMARK_WRAPPER_SMOKE_RUN_ID", f"wrapper_smoke_{uuid4().hex}"))
    parser.add_argument("--upstream-root", default=os.environ.get("BENCHMARK_WRAPPER_SMOKE_UPSTREAM_ROOT"))
    parser.add_argument(
        "--timeout-seconds",
        type=int,
        default=int(_env("BENCHMARK_WRAPPER_SMOKE_TIMEOUT_SECONDS", "3600")),
    )
    parser.add_argument("--dry-run", action="store_true", default=_env_bool("BENCHMARK_WRAPPER_SMOKE_DRY_RUN", True))
    parser.add_argument("--execute", action="store_true", help="Run against --upstream-root instead of dry-run mode.")
    args = parser.parse_args(argv)

    suite_name = _canonical_suite(args.suite)
    task_family, instance_id = _task_selection(
        suite_name=suite_name,
        task_family=args.task_family,
        instance_id=args.instance_id,
    )
    upstream_root = Path(args.upstream_root) if args.upstream_root else None
    dry_run = False if args.execute else bool(args.dry_run or upstream_root is None)
    return BenchmarkWrapperSmokeConfig(
        suite_name=suite_name,
        task_family=task_family,
        instance_id=instance_id,
        workspace_root=Path(args.workspace_root),
        run_id=args.run_id,
        upstream_root=upstream_root,
        dry_run=dry_run,
        timeout_seconds=args.timeout_seconds,
    )


def _task_manifest(*, config: BenchmarkWrapperSmokeConfig, output_dir: Path) -> dict[str, Any]:
    catalog = load_fixture_catalog(config.suite_name)
    spec = catalog.to_task_spec(task_family=config.task_family, instance_id=config.instance_id)
    return {
        "run_id": config.run_id,
        "suite_name": config.suite_name,
        "benchmark_version": catalog.benchmark_version,
        "source_uri": catalog.source_uri,
        "source_version": catalog.source_version,
        "task_family": config.task_family,
        "instance_id": config.instance_id,
        "instruction_ref": spec.metadata["instruction_ref"],
        "input_files": spec.metadata["input_files"],
        "model": {
            "provider": "smoke-api-provider",
            "model_name": "smoke-wrapper-model",
            "secret_ref": "env:MODEL_PROVIDER_API_KEY",
        },
        "output_dir": str(output_dir),
        "artifacts_dir": str(output_dir / "artifacts"),
    }


def _wrapper_main(suite_name: str):
    if suite_name == "SkillFlow":
        return skillflow_main
    if suite_name == "SkillLearnBench":
        return skilllearnbench_main
    raise ValueError(f"Unsupported suite: {suite_name}")


def _validate_config(config: BenchmarkWrapperSmokeConfig) -> None:
    _canonical_suite(config.suite_name)
    if not config.task_family.strip():
        raise ValueError("task_family must be non-empty")
    if not config.instance_id.strip():
        raise ValueError("instance_id must be non-empty")
    if config.timeout_seconds <= 0:
        raise ValueError("timeout_seconds must be positive")


def _task_selection(*, suite_name: str, task_family: str, instance_id: str) -> tuple[str, str]:
    if task_family and instance_id:
        return task_family, instance_id
    catalog = load_fixture_catalog(suite_name)
    first_instance = catalog.task_instances()[0]
    return task_family or first_instance.task_family, instance_id or first_instance.instance_id


def _canonical_suite(suite_name: str) -> str:
    normalized = suite_name.strip().lower()
    if normalized == "skillflow":
        return "SkillFlow"
    if normalized == "skilllearnbench":
        return "SkillLearnBench"
    raise ValueError(f"Unsupported suite: {suite_name}")


def _env(key: str, default: str) -> str:
    value = os.environ.get(key, default)
    return value.strip() if isinstance(value, str) and value.strip() else default


def _env_bool(key: str, default: bool) -> bool:
    value = os.environ.get(key)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


if __name__ == "__main__":
    raise SystemExit(main())
