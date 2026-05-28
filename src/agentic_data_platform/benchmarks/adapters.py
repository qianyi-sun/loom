from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from agentic_data_platform.domain.run_records import (
    BenchmarkTaskInstance,
    RunnerConfig,
    RunnerKind,
    SandboxBackend,
)


@dataclass(frozen=True)
class BenchmarkTaskSpec:
    task_family: str
    instance_id: str
    instruction: str
    input_artifact_refs: list[str]
    runner_image: str
    runner_entrypoint: list[str]
    resource_limits: dict[str, int | float] = field(
        default_factory=lambda: {"cpu": 2, "memory_gib": 8, "timeout_seconds": 3600}
    )
    internet_access: bool = True
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        _require_non_empty("task_family", self.task_family)
        _require_non_empty("instance_id", self.instance_id)
        _require_non_empty("instruction", self.instruction)
        _require_strings("input_artifact_refs", self.input_artifact_refs)
        _require_non_empty("runner_image", self.runner_image)
        _require_strings("runner_entrypoint", self.runner_entrypoint)


@dataclass(frozen=True)
class BenchmarkRegistration:
    task: BenchmarkTaskInstance
    runner: RunnerConfig


@dataclass(frozen=True)
class _SkillBenchmarkAdapter:
    suite_name: str
    benchmark_version: str
    source_uri: str

    def __post_init__(self) -> None:
        _require_non_empty("suite_name", self.suite_name)
        _require_non_empty("benchmark_version", self.benchmark_version)
        _require_non_empty("source_uri", self.source_uri)

    def register_task(self, spec: BenchmarkTaskSpec) -> BenchmarkRegistration:
        metadata = {
            **spec.metadata,
            "instruction": spec.instruction,
            "adapter": self.suite_name,
        }

        task = BenchmarkTaskInstance(
            benchmark_suite=self.suite_name,
            benchmark_version=self.benchmark_version,
            task_family=spec.task_family,
            instance_id=spec.instance_id,
            source_uri=self.source_uri,
            input_artifact_refs=spec.input_artifact_refs,
            required_artifacts=["trajectory", "workspace_snapshot", "evaluator_report"],
            metadata=metadata,
        )
        runner = RunnerConfig(
            kind=RunnerKind.ORIGINAL_BENCHMARK,
            sandbox_backend=SandboxBackend.DOCKER_TERMINAL,
            image=spec.runner_image,
            entrypoint=spec.runner_entrypoint,
            internet_access=spec.internet_access,
            resource_limits=spec.resource_limits,
            metadata={
                "adapter": self.suite_name,
                "runner_contract": "original_benchmark_wrapper",
                "task_family": spec.task_family,
                "instance_id": spec.instance_id,
            },
        )
        return BenchmarkRegistration(task=task, runner=runner)


class SkillFlowBenchmarkAdapter(_SkillBenchmarkAdapter):
    def __init__(self, *, benchmark_version: str, source_uri: str) -> None:
        super().__init__(
            suite_name="SkillFlow",
            benchmark_version=benchmark_version,
            source_uri=source_uri,
        )


class SkillLearnBenchBenchmarkAdapter(_SkillBenchmarkAdapter):
    def __init__(self, *, benchmark_version: str, source_uri: str) -> None:
        super().__init__(
            suite_name="SkillLearnBench",
            benchmark_version=benchmark_version,
            source_uri=source_uri,
        )


def _require_non_empty(name: str, value: str) -> None:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} must be a non-empty string")


def _require_strings(name: str, values: list[str]) -> None:
    if isinstance(values, str) or not values:
        raise ValueError(f"{name} must be a non-empty list of strings")

    for value in values:
        _require_non_empty(name, value)
