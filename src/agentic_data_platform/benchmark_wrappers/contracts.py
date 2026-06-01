from __future__ import annotations

import argparse
import csv
import json
import mimetypes
import os
import shlex
import shutil
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from agentic_data_platform.providers.config import redact_sensitive_metadata

_UPSTREAM_CONFIG_ARTIFACT = "upstream-config.json"
_UPSTREAM_OUTPUT_DIR = "upstream-output"
_EVALUATOR_REPORT_ARTIFACT = "evaluator-report.json"


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
            upstream_artifacts=[],
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
        upstream_artifacts = _collect_upstream_output_artifacts(paths=paths, manifest=manifest)
        _write_result(
            paths=paths,
            manifest=manifest,
            planned_command=planned_command,
            dry_run=False,
            exit_code=124,
            stdout=stdout,
            stderr=stderr,
            failure_reason=f"upstream runner timed out after {paths.timeout_seconds} seconds",
            upstream_artifacts=upstream_artifacts,
        )
        return 124

    stdout = completed.stdout
    stderr = completed.stderr
    _write_process_logs(paths=paths, stdout=stdout, stderr=stderr)
    upstream_artifacts = _collect_upstream_output_artifacts(paths=paths, manifest=manifest)

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
        upstream_artifacts=upstream_artifacts,
    )
    return completed.returncode


def _write_process_logs(*, paths: WrapperPaths, stdout: str, stderr: str) -> None:
    (paths.artifacts_dir / "stdout.log").write_text(stdout, encoding="utf-8")
    (paths.artifacts_dir / "stderr.log").write_text(stderr, encoding="utf-8")


def _collect_upstream_output_artifacts(
    *,
    paths: WrapperPaths,
    manifest: WrapperTaskManifest,
) -> list[dict[str, str]]:
    output_root = Path(manifest.output_dir)
    if not output_root.exists() or not output_root.is_dir():
        return []

    artifacts_root = paths.artifacts_dir.resolve()
    upstream_artifacts_root = paths.artifacts_dir / _UPSTREAM_OUTPUT_DIR
    artifacts: list[dict[str, str]] = []
    for source_path in sorted(output_root.rglob("*")):
        if source_path.is_symlink() or not source_path.is_file():
            continue

        resolved_source = source_path.resolve()
        if resolved_source.is_relative_to(artifacts_root):
            continue

        relative_path = source_path.relative_to(output_root)
        target_path = upstream_artifacts_root / relative_path
        target_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source_path, target_path)
        artifacts.append(
            {
                "kind": "upstream_output",
                "path": f"artifacts/{_UPSTREAM_OUTPUT_DIR}/{relative_path.as_posix()}",
                "media_type": mimetypes.guess_type(target_path.name)[0] or "application/octet-stream",
            }
        )

    return artifacts


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
    upstream_artifacts: list[dict[str, str]],
) -> None:
    metrics, evaluator_report_ref, evaluator_artifacts = _write_evaluator_report(
        paths=paths,
        manifest=manifest,
        upstream_artifacts=upstream_artifacts,
    )
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

    artifacts.extend(evaluator_artifacts)
    artifacts.extend(upstream_artifacts)

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
        "metrics": metrics,
        "artifacts": artifacts,
        "planned_command": shlex.join(planned_command),
        "stdout": stdout,
        "stderr": stderr,
        "trajectory_ref": None,
        "workspace_ref": None,
        "evaluator_report_ref": evaluator_report_ref,
        "failure_reason": failure_reason,
    }
    paths.output.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _write_evaluator_report(
    *,
    paths: WrapperPaths,
    manifest: WrapperTaskManifest,
    upstream_artifacts: list[dict[str, str]],
) -> tuple[dict[str, Any], str | None, list[dict[str, str]]]:
    source_reports: list[dict[str, str]] = []
    feedback: list[str] = []
    metrics: dict[str, Any] = {}

    for artifact in upstream_artifacts:
        source_path = artifact.get("path")
        if artifact.get("kind") != "upstream_output" or not source_path:
            continue
        local_path = _local_artifact_path(paths=paths, artifact_path=source_path)
        if local_path is None or not _is_report_path(local_path):
            continue

        report_metrics, report_feedback = _parse_report_file(local_path)
        if not report_metrics and not report_feedback:
            continue

        source_reports.append(
            {
                "path": source_path,
                "media_type": artifact.get("media_type") or "application/octet-stream",
            }
        )
        feedback.extend(report_feedback)
        metrics.update(report_metrics)

    if not source_reports:
        return {}, None, []

    metrics["upstream_report_count"] = len(source_reports)
    evaluator_report_ref = f"artifacts/{_EVALUATOR_REPORT_ARTIFACT}"
    report = {
        "schema_version": "adp.wrapper_evaluator_report.v1",
        "run_id": manifest.run_id,
        "suite_name": manifest.suite_name,
        "benchmark_version": manifest.benchmark_version,
        "source_uri": manifest.source_uri,
        "source_version": manifest.source_version,
        "task_family": manifest.task_family,
        "instance_id": manifest.instance_id,
        "metrics": metrics,
        "feedback": feedback[:20],
        "source_reports": source_reports,
    }
    (paths.artifacts_dir / _EVALUATOR_REPORT_ARTIFACT).write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return metrics, evaluator_report_ref, [
        {
            "kind": "evaluator_report",
            "path": evaluator_report_ref,
            "media_type": "application/json",
        }
    ]


def _local_artifact_path(*, paths: WrapperPaths, artifact_path: str) -> Path | None:
    prefix = "artifacts/"
    if not artifact_path.startswith(prefix):
        return None
    return paths.artifacts_dir / artifact_path.removeprefix(prefix)


def _is_report_path(path: Path) -> bool:
    report_names = {"report.json", "result.json", "results.json", "summary.json", "report.csv"}
    return path.name in report_names


def _parse_report_file(path: Path) -> tuple[dict[str, Any], list[str]]:
    if path.suffix == ".json":
        return _parse_json_report(path)
    if path.suffix == ".csv":
        return _parse_csv_report(path)
    return {}, []


def _parse_json_report(path: Path) -> tuple[dict[str, Any], list[str]]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}, []

    if not isinstance(payload, dict):
        return {}, []

    metrics: dict[str, Any] = {}
    feedback = _feedback_values(payload)
    for key, value in payload.items():
        if key == "metrics" and isinstance(value, dict):
            for metric_key, metric_value in value.items():
                _store_scalar_metric(metrics, metric_key, metric_value)
            continue
        _store_scalar_metric(metrics, key, value)
    _store_harbor_job_stats(metrics=metrics, payload=payload)
    return metrics, feedback


def _parse_csv_report(path: Path) -> tuple[dict[str, Any], list[str]]:
    try:
        with path.open("r", encoding="utf-8", newline="") as handle:
            rows = list(csv.DictReader(handle))
    except OSError:
        return {}, []

    if not rows:
        return {}, []

    metrics: dict[str, Any] = {"upstream_trial_count": len(rows)}
    feedback: list[str] = []
    score_values: list[float] = []
    success_values: list[bool] = []
    for row in rows:
        for key, value in row.items():
            normalized_key = (key or "").strip().lower()
            normalized_value = (value or "").strip()
            if not normalized_value:
                continue
            if normalized_key in {"score", "reward", "accuracy"}:
                numeric_value = _numeric_value(normalized_value)
                if numeric_value is not None:
                    score_values.append(numeric_value)
            if normalized_key in {"success", "passed", "pass", "task_success"}:
                bool_value = _bool_value(normalized_value)
                if bool_value is not None:
                    success_values.append(bool_value)
            if normalized_key in {"feedback", "verbal_feedback", "comment", "message"}:
                feedback.append(normalized_value)

    if score_values:
        metrics["upstream_score_mean"] = sum(score_values) / len(score_values)
    if success_values:
        metrics["upstream_success_rate"] = sum(1 for value in success_values if value) / len(success_values)
    return metrics, feedback


def _feedback_values(payload: dict[str, Any]) -> list[str]:
    values: list[str] = []
    for key in ("feedback", "verbal_feedback", "comment", "message", "summary"):
        value = payload.get(key)
        if isinstance(value, str) and value.strip():
            values.append(value.strip())
    return values


def _store_harbor_job_stats(*, metrics: dict[str, Any], payload: dict[str, Any]) -> None:
    stats = payload.get("stats")
    if not isinstance(stats, dict):
        return

    for key in (
        "n_completed_trials",
        "n_errored_trials",
        "n_running_trials",
        "n_pending_trials",
        "n_cancelled_trials",
        "n_retries",
    ):
        if key in stats:
            _store_scalar_metric(metrics, key, stats[key])

    score_means: list[float] = []
    evals = stats.get("evals")
    if isinstance(evals, dict):
        for evaluator_result in evals.values():
            if not isinstance(evaluator_result, dict):
                continue
            if "n_errors" in evaluator_result:
                _store_scalar_metric(metrics, "n_errors", evaluator_result["n_errors"])
            metric_rows = evaluator_result.get("metrics")
            if isinstance(metric_rows, list):
                for metric_row in metric_rows:
                    if not isinstance(metric_row, dict):
                        continue
                    mean_value = _numeric_value(metric_row.get("mean"))
                    if mean_value is not None:
                        score_means.append(mean_value)

    if score_means:
        metrics["upstream_score_mean"] = sum(score_means) / len(score_means)


def _store_scalar_metric(metrics: dict[str, Any], key: str, value: Any) -> None:
    normalized_key = key.strip().lower().replace("-", "_").replace(" ", "_")
    if normalized_key in {"feedback", "verbal_feedback", "comment", "message", "summary", "status"}:
        return
    if isinstance(value, bool):
        metrics[f"upstream_{normalized_key}"] = value
        return
    numeric_value = _numeric_value(value)
    if numeric_value is not None:
        metrics[f"upstream_{normalized_key}"] = numeric_value


def _numeric_value(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int | float):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value)
        except ValueError:
            return None
    return None


def _bool_value(value: str) -> bool | None:
    normalized = value.strip().lower()
    if normalized in {"1", "true", "yes", "y", "passed", "pass", "success", "succeeded"}:
        return True
    if normalized in {"0", "false", "no", "n", "failed", "fail", "failure"}:
        return False
    return None


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
