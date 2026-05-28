from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field

from agentic_data_platform.benchmarks.fixtures import (
    BenchmarkFixtureCatalog,
    BenchmarkFixtureFamily,
    BenchmarkFixtureInstance,
)

_REQUIRED_ARTIFACTS = ["trajectory", "workspace_snapshot", "evaluator_report"]


@dataclass
class _ManifestInstance:
    family: str
    instance_id: str
    root: str
    paths: list[str] = field(default_factory=list)

    @property
    def instruction_ref(self) -> str | None:
        return _first_matching(self.paths, "instruction.md")

    @property
    def task_config_ref(self) -> str | None:
        return _first_matching(self.paths, "task.toml")


def catalog_from_path_manifest(
    *,
    suite_name: str,
    source_uri: str,
    source_version: str,
    paths: list[str],
) -> BenchmarkFixtureCatalog:
    """Build a fixture catalog from generated upstream path manifests.

    This is the bridge between upstream repository/dataset tree listings and
    the platform catalog shape. It expects paths only, so tests can validate the
    import path without cloning repos or downloading benchmark assets.
    """

    if suite_name == "SkillFlow":
        return _skillflow_catalog(source_uri=source_uri, source_version=source_version, paths=paths)
    if suite_name == "SkillLearnBench":
        return _skilllearnbench_catalog(source_uri=source_uri, source_version=source_version, paths=paths)

    raise ValueError(f"Unsupported benchmark suite: {suite_name}")


def _skillflow_catalog(*, source_uri: str, source_version: str, paths: list[str]) -> BenchmarkFixtureCatalog:
    instances = _collect_instances(paths=paths, prefix="test_tasks", suite_name="SkillFlow")
    return _catalog(
        suite_name="SkillFlow",
        benchmark_version=f"hf:zhang-ziao/SkillFlow-Task@{source_version}",
        source_uri=source_uri,
        source_version=source_version,
        source_version_type="huggingface-dataset-snapshot",
        input_artifact_prefix="hf://datasets/zhang-ziao/SkillFlow-Task",
        runner_entrypoint=["python", "-m", "agentic_data_platform.benchmark_wrappers.skillflow"],
        runner_contract="skillflow-original-wrapper-v0",
        instances=instances,
    )


def _skilllearnbench_catalog(*, source_uri: str, source_version: str, paths: list[str]) -> BenchmarkFixtureCatalog:
    instances = _collect_instances(paths=paths, prefix="tasks", suite_name="SkillLearnBench")
    return _catalog(
        suite_name="SkillLearnBench",
        benchmark_version=f"git:cxcscmu/SkillLearnBench@{source_version}",
        source_uri=source_uri,
        source_version=source_version,
        source_version_type="git-commit",
        input_artifact_prefix="git://github.com/cxcscmu/SkillLearnBench",
        runner_entrypoint=["python", "-m", "agentic_data_platform.benchmark_wrappers.skilllearnbench"],
        runner_contract="skilllearnbench-original-wrapper-v0",
        instances=instances,
    )


def _catalog(
    *,
    suite_name: str,
    benchmark_version: str,
    source_uri: str,
    source_version: str,
    source_version_type: str,
    input_artifact_prefix: str,
    runner_entrypoint: list[str],
    runner_contract: str,
    instances: list[_ManifestInstance],
) -> BenchmarkFixtureCatalog:
    family_instances: dict[str, list[BenchmarkFixtureInstance]] = defaultdict(list)
    for instance in instances:
        instruction_ref = instance.instruction_ref
        task_config_ref = instance.task_config_ref
        if instruction_ref is None:
            raise ValueError(f"{suite_name}/{instance.family}/{instance.instance_id} is missing instruction.md")
        if task_config_ref is None:
            raise ValueError(f"{suite_name}/{instance.family}/{instance.instance_id} is missing task.toml")

        input_artifact_ref = f"{input_artifact_prefix}/{instance.root}"
        if source_version_type == "git-commit":
            input_artifact_ref = f"{input_artifact_ref}@{source_version}"

        family_instances[instance.family].append(
            BenchmarkFixtureInstance(
                task_family=instance.family,
                instance_id=instance.instance_id,
                instruction_ref=instruction_ref,
                input_files=sorted(instance.paths),
                input_artifact_refs=[input_artifact_ref],
                required_artifacts=_REQUIRED_ARTIFACTS,
                runner_image="python:3.12-slim",
                runner_entrypoint=runner_entrypoint,
                runner_contract=runner_contract,
                metadata={"generated_from": "upstream_path_manifest"},
            )
        )

    return BenchmarkFixtureCatalog(
        suite_name=suite_name,
        benchmark_version=benchmark_version,
        source_uri=source_uri,
        source_version=source_version,
        source_version_type=source_version_type,
        task_families=[
            BenchmarkFixtureFamily(name=family, instances=sorted(items, key=lambda item: item.instance_id))
            for family, items in sorted(family_instances.items())
        ],
        metadata={"generated_from": "upstream_path_manifest"},
    )


def _collect_instances(*, paths: list[str], prefix: str, suite_name: str) -> list[_ManifestInstance]:
    grouped: dict[tuple[str, str], _ManifestInstance] = {}
    for path in sorted(_clean_paths(paths)):
        parts = path.split("/")
        if len(parts) < 4 or parts[0] != prefix:
            continue

        family = parts[1]
        instance_id = parts[2]
        root = "/".join(parts[:3])
        key = (family, instance_id)
        if key not in grouped:
            grouped[key] = _ManifestInstance(family=family, instance_id=instance_id, root=root)
        grouped[key].paths.append(path)

    if not grouped:
        raise ValueError(f"No {suite_name} task instances found under {prefix}/")

    return list(grouped.values())


def _clean_paths(paths: list[str]) -> list[str]:
    cleaned = []
    for path in paths:
        normalized = path.strip().strip("/")
        if not normalized or normalized.endswith(".DS_Store"):
            continue
        if normalized.endswith("ALL_TASK_DIFFICULTY_RANKING.json"):
            continue
        cleaned.append(normalized)
    return cleaned


def _first_matching(paths: list[str], filename: str) -> str | None:
    for path in sorted(paths):
        if path.endswith(f"/{filename}"):
            return path
    return None
