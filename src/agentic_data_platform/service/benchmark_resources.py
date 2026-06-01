from __future__ import annotations

from typing import Callable

from fastapi import Depends, FastAPI, HTTPException, Request
from sqlalchemy.orm import Session

from agentic_data_platform.benchmarks.fixtures import (
    BenchmarkFixtureCatalog,
    BenchmarkFixtureFamily,
    BenchmarkFixtureInstance,
)
from agentic_data_platform.persistence.repositories import BenchmarkCatalogRepository


def register_benchmark_routes(app: FastAPI, session_dependency: Callable) -> None:
    @app.get("/benchmarks", tags=["benchmarks"], responses=_example_response(_BENCHMARKS_EXAMPLE))
    def list_benchmarks(
        request: Request,
        session: Session = Depends(session_dependency),
    ) -> dict:
        repository = BenchmarkCatalogRepository(session)
        return _with_request_id(
            request,
            {"benchmarks": [_benchmark_payload(catalog) for catalog in repository.list_fixture_catalogs()]},
        )

    @app.get("/benchmarks/{suite_name}", tags=["benchmarks"], responses=_example_response(_BENCHMARK_EXAMPLE))
    def get_benchmark(
        suite_name: str,
        benchmark_version: str,
        request: Request,
        session: Session = Depends(session_dependency),
    ) -> dict:
        catalog = _get_catalog_or_404(
            BenchmarkCatalogRepository(session),
            suite_name=suite_name,
            benchmark_version=benchmark_version,
        )
        return _with_request_id(request, {"benchmark": _benchmark_payload(catalog)})

    @app.get("/task-families", tags=["benchmarks"], responses=_example_response(_TASK_FAMILIES_EXAMPLE))
    def list_task_families(
        benchmark_suite: str,
        benchmark_version: str,
        request: Request,
        session: Session = Depends(session_dependency),
    ) -> dict:
        catalog = _get_catalog_or_404(
            BenchmarkCatalogRepository(session),
            suite_name=benchmark_suite,
            benchmark_version=benchmark_version,
        )
        return _with_request_id(
            request,
            {
                "benchmark": _benchmark_payload(catalog),
                "task_families": [_family_summary_payload(family) for family in catalog.task_families],
            },
        )

    @app.get("/task-families/{task_family}", tags=["benchmarks"], responses=_example_response(_TASK_FAMILY_EXAMPLE))
    def get_task_family(
        task_family: str,
        benchmark_suite: str,
        benchmark_version: str,
        request: Request,
        session: Session = Depends(session_dependency),
    ) -> dict:
        catalog = _get_catalog_or_404(
            BenchmarkCatalogRepository(session),
            suite_name=benchmark_suite,
            benchmark_version=benchmark_version,
        )
        family = _task_family_or_404(catalog, task_family)
        return _with_request_id(
            request,
            {
                "benchmark": _benchmark_payload(catalog),
                "task_family": _family_detail_payload(family),
            },
        )

    @app.get("/tasks", tags=["benchmarks"], responses=_example_response(_TASKS_EXAMPLE))
    def list_tasks(
        benchmark_suite: str,
        benchmark_version: str,
        request: Request,
        session: Session = Depends(session_dependency),
    ) -> dict:
        catalog = _get_catalog_or_404(
            BenchmarkCatalogRepository(session),
            suite_name=benchmark_suite,
            benchmark_version=benchmark_version,
        )

        return _with_request_id(
            request,
            {
                "benchmark": _benchmark_payload(catalog),
                "tasks": [_task_payload(instance) for instance in catalog.task_instances()],
            },
        )

    @app.get("/tasks/{task_family}/{instance_id}", tags=["benchmarks"], responses=_example_response(_TASK_EXAMPLE))
    def get_task(
        task_family: str,
        instance_id: str,
        benchmark_suite: str,
        benchmark_version: str,
        request: Request,
        session: Session = Depends(session_dependency),
    ) -> dict:
        repository = BenchmarkCatalogRepository(session)
        try:
            task = repository.get_task_instance(
                suite_name=benchmark_suite,
                benchmark_version=benchmark_version,
                task_family=task_family,
                instance_id=instance_id,
            )
        except KeyError as exc:
            raise HTTPException(status_code=404, detail="Task not found") from exc

        return _with_request_id(request, {"task": _task_payload(task)})


def _with_request_id(request: Request, payload: dict) -> dict:
    request_id = getattr(request.state, "request_id", None)
    if request_id is not None:
        payload["request_id"] = request_id
    return payload


def _benchmark_payload(catalog: BenchmarkFixtureCatalog) -> dict:
    return {
        "suite_name": catalog.suite_name,
        "benchmark_version": catalog.benchmark_version,
        "source_uri": catalog.source_uri,
        "source_version": catalog.source_version,
        "source_version_type": catalog.source_version_type,
        "task_family_count": len(catalog.task_families),
        "task_instance_count": len(catalog.task_instances()),
        "metadata": dict(catalog.metadata),
    }


def _family_summary_payload(family: BenchmarkFixtureFamily) -> dict:
    return {
        "name": family.name,
        "task_instance_count": len(family.instances),
    }


def _family_detail_payload(family: BenchmarkFixtureFamily) -> dict:
    return {
        **_family_summary_payload(family),
        "tasks": [_task_payload(instance) for instance in family.instances],
    }


def _task_payload(instance: BenchmarkFixtureInstance) -> dict:
    return {
        "task_family": instance.task_family,
        "instance_id": instance.instance_id,
        "instruction_ref": instance.instruction_ref,
        "input_files": list(instance.input_files),
        "input_artifact_refs": list(instance.input_artifact_refs),
        "required_artifacts": list(instance.required_artifacts),
        "runner_image": instance.runner_image,
        "runner_entrypoint": list(instance.runner_entrypoint),
        "runner_contract": instance.runner_contract,
        "metadata": dict(instance.metadata),
    }


def _get_catalog_or_404(
    repository: BenchmarkCatalogRepository,
    *,
    suite_name: str,
    benchmark_version: str,
) -> BenchmarkFixtureCatalog:
    try:
        return repository.get_fixture_catalog(
            suite_name=suite_name,
            benchmark_version=benchmark_version,
        )
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="Benchmark not found") from exc


def _task_family_or_404(catalog: BenchmarkFixtureCatalog, task_family: str) -> BenchmarkFixtureFamily:
    for family in catalog.task_families:
        if family.name == task_family:
            return family
    raise HTTPException(status_code=404, detail="Task family not found")


def _example_response(example: dict) -> dict:
    return {200: {"content": {"application/json": {"example": example}}}}


_BENCHMARK_PAYLOAD_EXAMPLE = {
    "suite_name": "SkillFlow",
    "benchmark_version": "hf:zhang-ziao/SkillFlow-Task@ecaadb0e25d5d5cfd87bd86d81e77b4abe3a00bc",
    "source_uri": "https://huggingface.co/datasets/zhang-ziao/SkillFlow-Task",
    "source_version": "ecaadb0e25d5d5cfd87bd86d81e77b4abe3a00bc",
    "source_version_type": "huggingface-dataset-commit",
    "task_family_count": 20,
    "task_instance_count": 120,
    "metadata": {"pilot_team": "pilot-project"},
}
_TASK_PAYLOAD_EXAMPLE = {
    "task_family": "OCR-Data-Extraction",
    "instance_id": "task_family_invoice_images",
    "instruction_ref": "test_tasks/OCR-Data-Extraction/task_family_invoice_images/instruction.md",
    "input_files": ["test_tasks/OCR-Data-Extraction/task_family_invoice_images/input.pdf"],
    "input_artifact_refs": ["minio://benchmarks/skillflow/input.tar.zst"],
    "required_artifacts": ["trajectory", "workspace_snapshot", "evaluator_report"],
    "runner_image": "python:3.12-slim",
    "runner_entrypoint": ["python", "-m", "agentic_data_platform.benchmark_wrappers.skillflow"],
    "runner_contract": "skillflow-original-wrapper-v0",
    "metadata": {"difficulty": "medium"},
}
_BENCHMARKS_EXAMPLE = {"benchmarks": [_BENCHMARK_PAYLOAD_EXAMPLE], "request_id": "req_123"}
_BENCHMARK_EXAMPLE = {"benchmark": _BENCHMARK_PAYLOAD_EXAMPLE, "request_id": "req_123"}
_TASK_FAMILY_PAYLOAD_EXAMPLE = {
    "name": "OCR-Data-Extraction",
    "task_instance_count": 7,
}
_TASK_FAMILY_DETAIL_EXAMPLE = {
    **_TASK_FAMILY_PAYLOAD_EXAMPLE,
    "tasks": [_TASK_PAYLOAD_EXAMPLE],
}
_TASK_FAMILIES_EXAMPLE = {
    "benchmark": _BENCHMARK_PAYLOAD_EXAMPLE,
    "task_families": [_TASK_FAMILY_PAYLOAD_EXAMPLE],
    "request_id": "req_123",
}
_TASK_FAMILY_EXAMPLE = {
    "benchmark": _BENCHMARK_PAYLOAD_EXAMPLE,
    "task_family": _TASK_FAMILY_DETAIL_EXAMPLE,
    "request_id": "req_123",
}
_TASKS_EXAMPLE = {
    "benchmark": _BENCHMARK_PAYLOAD_EXAMPLE,
    "tasks": [_TASK_PAYLOAD_EXAMPLE],
    "request_id": "req_123",
}
_TASK_EXAMPLE = {"task": _TASK_PAYLOAD_EXAMPLE, "request_id": "req_123"}
