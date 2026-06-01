from __future__ import annotations

from dataclasses import dataclass

from agentic_data_platform.benchmarks.fixtures import (
    BenchmarkFixtureCatalog,
    BenchmarkFixtureFamily,
    BenchmarkFixtureInstance,
)
from agentic_data_platform.harbor.capabilities import probe_harbor_native_capabilities
from agentic_data_platform.harbor.task_uploads import HarborTaskArchiveValidationResult


HARBOR_RUNNER_CONTRACT = "harbor-local-docker-v0"
HARBOR_RUNNER_IMAGE = "python:3.12-slim"
HARBOR_RUNNER_ENTRYPOINT = ["harbor", "run"]
HARBOR_REQUIRED_ARTIFACTS = ["trajectory", "workspace_snapshot", "evaluator_report", "harbor_jobs_archive"]
HARBOR_ARTIFACT_CONVENTIONS = {
    "raw_jobs": "jobs/",
    "trajectory": "trajectory.json",
    "artifacts": "/logs/artifacts",
    "verifier_reward": "/logs/verifier/reward.txt or trial result.json",
}


@dataclass(frozen=True)
class HarborDatasetCatalogSpec:
    suite_name: str
    dataset_ref: str
    display_family: str
    source_version: str
    task_selector: str = "all"

    @property
    def benchmark_version(self) -> str:
        return f"harbor:{self.dataset_ref}"

    @property
    def source_uri(self) -> str:
        return f"harbor://datasets/{self.dataset_ref}"


class HarborBenchmarkProvider:
    def __init__(self, dataset_specs: list[HarborDatasetCatalogSpec] | None = None) -> None:
        self.dataset_specs = dataset_specs or [
            HarborDatasetCatalogSpec(
                suite_name="HarborTerminalBench",
                dataset_ref="terminal-bench/terminal-bench-2",
                display_family="terminal-bench-2",
                source_version="terminal-bench-2",
            )
        ]

    def list_catalogs(self) -> list[BenchmarkFixtureCatalog]:
        return [self._catalog_for_dataset(spec) for spec in self.dataset_specs]

    def catalog_for_task_upload(
        self,
        *,
        project_id: str,
        upload_id: str,
        storage_key: str,
        archive_sha256: str,
        validation: HarborTaskArchiveValidationResult,
    ) -> BenchmarkFixtureCatalog:
        _require_non_empty("project_id", project_id)
        _require_non_empty("upload_id", upload_id)
        _require_non_empty("storage_key", storage_key)
        _require_non_empty("archive_sha256", archive_sha256)
        source_uri = f"object-store://{storage_key}"
        metadata = {
            **_base_metadata(source_type="harbor_task_upload"),
            "project_id": project_id,
            "upload_id": upload_id,
            "storage_key": storage_key,
            "archive_sha256": archive_sha256,
            "declared_artifacts": list(validation.declared_artifacts),
            "environment": dict(validation.environment),
            "resource_requirements": dict(validation.resource_requirements),
            "validation_warnings": list(validation.validation_warnings),
        }
        task_metadata = {
            "source_type": "harbor_task_upload",
            "storage_key": storage_key,
            "archive_sha256": archive_sha256,
            "environment": dict(validation.environment),
            "resource_requirements": dict(validation.resource_requirements),
            "declared_artifacts": list(validation.declared_artifacts),
            "verifier_type": "harbor_verifier",
            "artifact_conventions": dict(HARBOR_ARTIFACT_CONVENTIONS),
            "harbor_run": {
                "backend": "cli",
                "task_archive_storage_key": storage_key,
                "environment": _environment_type(validation.environment),
            },
        }
        return BenchmarkFixtureCatalog(
            suite_name="HarborUploadedTask",
            benchmark_version=f"upload:{upload_id}",
            source_uri=source_uri,
            source_version=archive_sha256,
            source_version_type="sha256",
            task_families=[
                BenchmarkFixtureFamily(
                    name=validation.task_name,
                    instances=[
                        BenchmarkFixtureInstance(
                            task_family=validation.task_name,
                            instance_id=upload_id,
                            instruction_ref=f"{source_uri}#instruction.md",
                            input_files=list(validation.files),
                            input_artifact_refs=[source_uri],
                            required_artifacts=list(HARBOR_REQUIRED_ARTIFACTS),
                            runner_image=HARBOR_RUNNER_IMAGE,
                            runner_entrypoint=list(HARBOR_RUNNER_ENTRYPOINT),
                            runner_contract=HARBOR_RUNNER_CONTRACT,
                            metadata=task_metadata,
                        )
                    ],
                )
            ],
            metadata=metadata,
        )

    def _catalog_for_dataset(self, spec: HarborDatasetCatalogSpec) -> BenchmarkFixtureCatalog:
        metadata = {
            **_base_metadata(source_type="harbor_dataset"),
            "harbor_dataset_ref": spec.dataset_ref,
            "task_selector": spec.task_selector,
        }
        task_metadata = {
            "source_type": "harbor_dataset",
            "harbor_dataset_ref": spec.dataset_ref,
            "task_selector": spec.task_selector,
            "environment_types": ["docker"],
            "verifier_type": "harbor_verifier",
            "artifact_conventions": dict(HARBOR_ARTIFACT_CONVENTIONS),
            "harbor_run": {
                "backend": "cli",
                "dataset_ref": spec.dataset_ref,
                "environment": "docker",
            },
        }
        return BenchmarkFixtureCatalog(
            suite_name=spec.suite_name,
            benchmark_version=spec.benchmark_version,
            source_uri=spec.source_uri,
            source_version=spec.source_version,
            source_version_type="harbor-dataset-ref",
            task_families=[
                BenchmarkFixtureFamily(
                    name=spec.display_family,
                    instances=[
                        BenchmarkFixtureInstance(
                            task_family=spec.display_family,
                            instance_id="dataset-ref",
                            instruction_ref=spec.source_uri,
                            input_files=[spec.source_uri],
                            input_artifact_refs=[spec.source_uri],
                            required_artifacts=list(HARBOR_REQUIRED_ARTIFACTS),
                            runner_image=HARBOR_RUNNER_IMAGE,
                            runner_entrypoint=list(HARBOR_RUNNER_ENTRYPOINT),
                            runner_contract=HARBOR_RUNNER_CONTRACT,
                            metadata=task_metadata,
                        )
                    ],
                )
            ],
            metadata=metadata,
        )


def _base_metadata(*, source_type: str) -> dict[str, object]:
    capabilities = probe_harbor_native_capabilities()
    return {
        "provider": "harbor",
        "source_type": source_type,
        "environment_types": ["docker"],
        "verifier_type": "harbor_verifier",
        "artifact_conventions": dict(HARBOR_ARTIFACT_CONVENTIONS),
        "backend_modes": ["cli", "native"],
        "native_runner_available": capabilities.native_runner_available,
        "harbor_package_version": capabilities.package_version,
    }


def _environment_type(environment: dict[str, object]) -> str:
    value = environment.get("type")
    if isinstance(value, str) and value.strip():
        return value
    return "docker"


def _require_non_empty(name: str, value: str) -> None:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} must be a non-empty string")
