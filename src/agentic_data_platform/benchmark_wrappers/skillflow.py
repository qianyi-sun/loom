from __future__ import annotations

import sys

from agentic_data_platform.benchmark_wrappers.contracts import (
    load_task_manifest,
    parse_wrapper_args,
    run_wrapper,
    upstream_config_path,
)


def main(argv: list[str] | None = None) -> int:
    paths = parse_wrapper_args(argv)
    manifest = load_task_manifest(paths.task_manifest)
    planned_command = [
        sys.executable,
        "family_job_runner.py",
        "--config",
        str(upstream_config_path(paths)),
        "--dataset-path",
        "test_tasks",
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
    )


if __name__ == "__main__":
    raise SystemExit(main())
