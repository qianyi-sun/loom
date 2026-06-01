from __future__ import annotations

import json
from dataclasses import dataclass, field
from importlib.resources import files
from typing import Any

from agentic_data_platform.benchmarks.adapters import BenchmarkTaskSpec


@dataclass(frozen=True)
class BenchmarkFixtureInstance:
    task_family: str
    instance_id: str
    instruction_ref: str
    input_files: list[str]
    input_artifact_refs: list[str]
    required_artifacts: list[str]
    runner_image: str
    runner_entrypoint: list[str]
    runner_contract: str
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        _require_non_empty("task_family", self.task_family)
        _require_non_empty("instance_id", self.instance_id)
        _require_non_empty("instruction_ref", self.instruction_ref)
        _require_strings("input_files", self.input_files)
        _require_strings("input_artifact_refs", self.input_artifact_refs)
        _require_strings("required_artifacts", self.required_artifacts)
        _require_non_empty("runner_image", self.runner_image)
        _require_strings("runner_entrypoint", self.runner_entrypoint)
        _require_non_empty("runner_contract", self.runner_contract)


@dataclass(frozen=True)
class BenchmarkFixtureFamily:
    name: str
    instances: list[BenchmarkFixtureInstance]

    def __post_init__(self) -> None:
        _require_non_empty("name", self.name)
        if not self.instances:
            raise ValueError("instances must not be empty")


@dataclass(frozen=True)
class BenchmarkFixtureCatalog:
    suite_name: str
    benchmark_version: str
    source_uri: str
    source_version: str
    source_version_type: str
    task_families: list[BenchmarkFixtureFamily]
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        _require_non_empty("suite_name", self.suite_name)
        _require_non_empty("benchmark_version", self.benchmark_version)
        _require_non_empty("source_uri", self.source_uri)
        _require_non_empty("source_version", self.source_version)
        _require_non_empty("source_version_type", self.source_version_type)
        if not self.task_families:
            raise ValueError("task_families must not be empty")

    def task_instances(self) -> list[BenchmarkFixtureInstance]:
        return [instance for family in self.task_families for instance in family.instances]

    def to_task_spec(self, *, task_family: str, instance_id: str) -> BenchmarkTaskSpec:
        instance = self._find_instance(task_family=task_family, instance_id=instance_id)
        instruction = (
            f"Refer to upstream instruction at {instance.instruction_ref}. "
            "Materialize the listed input files in the sandbox workspace, run the terminal agent, "
            "and preserve the final workspace for evaluator review."
        )
        metadata = {
            **instance.metadata,
            "suite_name": self.suite_name,
            "benchmark_version": self.benchmark_version,
            "source_version": self.source_version,
            "source_version_type": self.source_version_type,
            "instruction_ref": instance.instruction_ref,
            "input_files": instance.input_files,
            "required_artifacts": instance.required_artifacts,
        }
        return BenchmarkTaskSpec(
            task_family=instance.task_family,
            instance_id=instance.instance_id,
            instruction=instruction,
            input_artifact_refs=instance.input_artifact_refs,
            runner_image=instance.runner_image,
            runner_entrypoint=instance.runner_entrypoint,
            runner_contract=instance.runner_contract,
            required_artifacts=instance.required_artifacts,
            metadata=metadata,
        )

    def _find_instance(self, *, task_family: str, instance_id: str) -> BenchmarkFixtureInstance:
        for instance in self.task_instances():
            if instance.task_family == task_family and instance.instance_id == instance_id:
                return instance

        raise ValueError(f"Unknown fixture instance: {self.suite_name}/{task_family}/{instance_id}")


def load_fixture_catalog(suite_name: str) -> BenchmarkFixtureCatalog:
    normalized = suite_name.lower()
    for catalog in load_fixture_catalogs():
        if catalog.suite_name.lower() == normalized:
            return catalog

    raise ValueError(f"Unknown fixture catalog: {suite_name}")


def load_fixture_catalogs() -> list[BenchmarkFixtureCatalog]:
    catalog_package = files("agentic_data_platform.benchmarks.catalogs")
    return [
        _catalog_from_dict(json.loads(catalog_package.joinpath("skillflow.json").read_text())),
        _catalog_from_dict(json.loads(catalog_package.joinpath("skilllearnbench.json").read_text())),
        *_harbor_fixture_catalogs(),
    ]


def _harbor_fixture_catalogs() -> list[BenchmarkFixtureCatalog]:
    from agentic_data_platform.harbor.benchmark_provider import HarborBenchmarkProvider

    return HarborBenchmarkProvider().list_catalogs()


def _catalog_from_dict(data: dict[str, Any]) -> BenchmarkFixtureCatalog:
    defaults = data.get("defaults", {})
    task_families = [
        BenchmarkFixtureFamily(
            name=family["name"],
            instances=[
                _instance_from_dict(
                    family_name=family["name"],
                    data=instance,
                    defaults=defaults,
                )
                for instance in family.get("instances", [])
            ],
        )
        for family in data.get("task_families", [])
    ]
    return BenchmarkFixtureCatalog(
        suite_name=data["suite_name"],
        benchmark_version=data["benchmark_version"],
        source_uri=data["source_uri"],
        source_version=data["source_version"],
        source_version_type=data["source_version_type"],
        task_families=task_families,
        metadata=data.get("metadata", {}),
    )


def _instance_from_dict(
    *,
    family_name: str,
    data: dict[str, Any],
    defaults: dict[str, Any],
) -> BenchmarkFixtureInstance:
    return BenchmarkFixtureInstance(
        task_family=family_name,
        instance_id=data["instance_id"],
        instruction_ref=data["instruction_ref"],
        input_files=data["input_files"],
        input_artifact_refs=data["input_artifact_refs"],
        required_artifacts=data.get("required_artifacts", defaults["required_artifacts"]),
        runner_image=data.get("runner_image", defaults["runner_image"]),
        runner_entrypoint=data.get("runner_entrypoint", defaults["runner_entrypoint"]),
        runner_contract=data.get("runner_contract", defaults["runner_contract"]),
        metadata=data.get("metadata", {}),
    )


def _require_non_empty(name: str, value: str) -> None:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} must be a non-empty string")


def _require_strings(name: str, values: list[str]) -> None:
    if isinstance(values, str) or not values:
        raise ValueError(f"{name} must be a non-empty list of strings")

    for value in values:
        _require_non_empty(name, value)
