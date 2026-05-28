from __future__ import annotations

import re

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
        "evaluate_skills.py",
        manifest.task_family,
        "--skill-path",
        "none",
        "--trials-dir",
        f"{manifest.output_dir}/trials",
    ]

    subtask_range = _subtask_range(manifest.instance_id)
    if subtask_range is not None:
        planned_command.extend(["--subtask-range", subtask_range])

    planned_command.append("--dry-run")
    return run_dry_wrapper(
        expected_suite="SkillLearnBench",
        argv=argv,
        planned_command=planned_command,
    )


def _subtask_range(instance_id: str) -> str | None:
    match = re.search(r"-(\d+)$", instance_id)
    if match is None:
        return None
    index = match.group(1)
    return f"{index}-{index}"


if __name__ == "__main__":
    raise SystemExit(main())
