from __future__ import annotations

import sys
import json
from pathlib import Path

from agentic_data_platform.benchmark_wrappers.contracts import (
    load_task_manifest,
    parse_wrapper_args,
    run_wrapper,
)
from agentic_data_platform.benchmark_wrappers.provider_mapping import (
    mapping_for_suite,
    runtime_environment_for_mapping,
)

_SKILLFLOW_JOB_CONFIG_ARTIFACT = "skillflow-job-config.json"


def main(argv: list[str] | None = None) -> int:
    paths = parse_wrapper_args(argv)
    manifest = load_task_manifest(paths.task_manifest)
    provider_mapping = mapping_for_suite(suite_name="SkillFlow", model=manifest.model)
    job_config_path = _write_skillflow_job_config(
        paths_artifacts_dir=paths.artifacts_dir,
        manifest_run_id=manifest.run_id,
        task_family=manifest.task_family,
        import_path=provider_mapping.skillflow_import_path,
        model_name=provider_mapping.skillflow_model_name,
    )
    extra_env, secret_values = runtime_environment_for_mapping(
        provider_mapping,
        require_secret=not paths.dry_run,
    )
    planned_command = [
        sys.executable,
        "family_job_runner.py",
        "--config",
        str(job_config_path),
        "--dataset-path",
        f"test_tasks/{manifest.task_family}",
        "--run-root-dir",
        manifest.output_dir,
        "--only-group",
        manifest.task_family,
    ]
    if paths.dry_run:
        planned_command.append("--dry-run")

    return run_wrapper(
        expected_suite="SkillFlow",
        paths=paths,
        manifest=manifest,
        planned_command=planned_command,
        provider_mapping=provider_mapping.to_safe_dict(),
        extra_env=extra_env,
        secret_values=secret_values,
        runner_config_artifacts=[
            {
                "kind": "runner_config",
                "path": f"artifacts/{_SKILLFLOW_JOB_CONFIG_ARTIFACT}",
                "media_type": "application/json",
            }
        ],
    )


def _write_skillflow_job_config(
    *,
    paths_artifacts_dir: Path,
    manifest_run_id: str,
    task_family: str,
    import_path: str | None,
    model_name: str | None,
) -> Path:
    if import_path is None or model_name is None:
        raise ValueError("unsupported_provider_mapping: SkillFlow provider mapping did not include a runner agent")
    config_path = paths_artifacts_dir / _SKILLFLOW_JOB_CONFIG_ARTIFACT
    config_path.parent.mkdir(parents=True, exist_ok=True)
    config = {
        "schema_version": "adp.skillflow_job_config.v1",
        "job_name": f"adp-{manifest_run_id}-{task_family}",
        "environment": {"type": "docker"},
        "orchestrator": {"n_concurrent_trials": 1},
        "agents": [
            {
                "import_path": import_path,
                "model_name": model_name,
                "env": {},
                "kwargs": {},
            }
        ],
        "datasets": [{"path": f"test_tasks/{task_family}", "n_tasks": 0}],
        "tasks": [],
        "metrics": [],
        "artifacts": [],
    }
    config_path.write_text(json.dumps(config, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return config_path


if __name__ == "__main__":
    raise SystemExit(main())
