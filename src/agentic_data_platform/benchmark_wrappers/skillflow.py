from __future__ import annotations

from agentic_data_platform.benchmark_wrappers.contracts import (
    load_task_manifest,
    parse_wrapper_args,
    run_dry_wrapper,
)


def main(argv: list[str] | None = None) -> int:
    paths = parse_wrapper_args(argv)
    manifest = load_task_manifest(paths.task_manifest)
    planned_command = [
        "python",
        "family_job_runner.py",
        "--config",
        "configs/baseline.yaml",
        "--dataset-path",
        "test_tasks",
        "--run-root-dir",
        manifest.output_dir,
        "--only-group",
        manifest.task_family,
        "--dry-run",
    ]
    return run_dry_wrapper(
        expected_suite="SkillFlow",
        argv=argv,
        planned_command=planned_command,
    )


if __name__ == "__main__":
    raise SystemExit(main())
