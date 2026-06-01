from __future__ import annotations

import re
import sys

from agentic_data_platform.benchmark_wrappers.contracts import (
    load_task_manifest,
    parse_wrapper_args,
    run_wrapper,
)
from agentic_data_platform.benchmark_wrappers.provider_mapping import (
    mapping_for_suite,
    runtime_environment_for_mapping,
)


def main(argv: list[str] | None = None) -> int:
    paths = parse_wrapper_args(argv)
    manifest = load_task_manifest(paths.task_manifest)
    provider_mapping = mapping_for_suite(suite_name="SkillLearnBench", model=manifest.model)
    extra_env, secret_values = runtime_environment_for_mapping(
        provider_mapping,
        require_secret=not paths.dry_run,
    )
    planned_command = [
        sys.executable,
        "evaluate_skills.py",
        manifest.task_family,
        "--agent",
        _require_upstream_value(provider_mapping.skilllearnbench_agent, "SkillLearnBench agent"),
        "--model",
        _require_upstream_value(provider_mapping.skilllearnbench_model_name, "SkillLearnBench model"),
        "--skill-path",
        "none",
        "--trials-dir",
        f"{manifest.output_dir}/trials",
    ]

    subtask_range = _subtask_range(manifest.instance_id)
    if subtask_range is not None:
        planned_command.extend(["--subtask-range", subtask_range])

    if paths.dry_run:
        planned_command.append("--dry-run")

    return run_wrapper(
        expected_suite="SkillLearnBench",
        paths=paths,
        manifest=manifest,
        planned_command=planned_command,
        provider_mapping=provider_mapping.to_safe_dict(),
        extra_env=extra_env,
        secret_values=secret_values,
    )


def _subtask_range(instance_id: str) -> str | None:
    match = re.search(r"-(\d+)$", instance_id)
    if match is None:
        return None
    index = match.group(1)
    return f"{index}-{index}"


def _require_upstream_value(value: str | None, name: str) -> str:
    if value is None:
        raise ValueError(f"unsupported_provider_mapping: missing {name}")
    return value


if __name__ == "__main__":
    raise SystemExit(main())
