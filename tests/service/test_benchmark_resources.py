import unittest

from fastapi import FastAPI, Request
from fastapi.testclient import TestClient
from sqlalchemy.pool import StaticPool

from agentic_data_platform.benchmarks.fixtures import BenchmarkFixtureCatalog, load_fixture_catalog
from agentic_data_platform.harbor.benchmark_provider import HarborBenchmarkProvider
from agentic_data_platform.persistence.database import create_database_engine, session_scope
from agentic_data_platform.persistence.migrations import upgrade_database
from agentic_data_platform.persistence.repositories import BenchmarkCatalogRepository
from agentic_data_platform.service.benchmark_resources import register_benchmark_routes


class BenchmarkResourceTest(unittest.TestCase):
    def setUp(self):
        self.engine = create_database_engine(
            "sqlite+pysqlite:///:memory:",
            connect_args={"check_same_thread": False},
            poolclass=StaticPool,
        )
        upgrade_database(self.engine)
        self.catalog = load_fixture_catalog("SkillFlow")
        with session_scope(self.engine) as session:
            BenchmarkCatalogRepository(session).upsert_fixture_catalog(self.catalog)
        self.client = TestClient(_app_for_engine(self.engine))

    def tearDown(self):
        self.engine.dispose()

    def test_list_benchmarks_returns_seeded_catalog_summaries(self):
        response = self.client.get("/benchmarks", headers={"X-Request-ID": "req-benchmarks-001"})

        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            response.json(),
            {
                "benchmarks": [_benchmark_summary(self.catalog)],
                "request_id": "req-benchmarks-001",
            },
        )

    def test_get_benchmark_returns_seeded_catalog_summary(self):
        response = self.client.get(
            f"/benchmarks/{self.catalog.suite_name}",
            params={"benchmark_version": self.catalog.benchmark_version},
            headers={"X-Request-ID": "req-benchmark-001"},
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            response.json(),
            {
                "benchmark": _benchmark_summary(self.catalog),
                "request_id": "req-benchmark-001",
            },
        )

    def test_list_task_families_returns_family_summaries_for_requested_benchmark_version(self):
        response = self.client.get(
            "/task-families",
            params={
                "benchmark_suite": self.catalog.suite_name,
                "benchmark_version": self.catalog.benchmark_version,
            },
            headers={"X-Request-ID": "req-families-001"},
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            response.json(),
            {
                "benchmark": _benchmark_summary(self.catalog),
                "task_families": [_family_summary(family) for family in self.catalog.task_families],
                "request_id": "req-families-001",
            },
        )

    def test_get_task_family_returns_family_detail_for_requested_benchmark_version(self):
        family = _catalog_family(self.catalog, "OCR-Data-Extraction")

        response = self.client.get(
            "/task-families/OCR-Data-Extraction",
            params={
                "benchmark_suite": self.catalog.suite_name,
                "benchmark_version": self.catalog.benchmark_version,
            },
            headers={"X-Request-ID": "req-family-001"},
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            response.json(),
            {
                "benchmark": _benchmark_summary(self.catalog),
                "task_family": _family_detail(family),
                "request_id": "req-family-001",
            },
        )

    def test_list_tasks_returns_tasks_for_requested_benchmark_version(self):
        response = self.client.get(
            "/tasks",
            params={
                "benchmark_suite": self.catalog.suite_name,
                "benchmark_version": self.catalog.benchmark_version,
            },
            headers={"X-Request-ID": "req-tasks-001"},
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            response.json(),
            {
                "benchmark": _benchmark_summary(self.catalog),
                "tasks": [_task_payload(instance) for instance in _ordered_tasks(self.catalog)],
                "request_id": "req-tasks-001",
            },
        )

    def test_get_task_returns_single_task_for_requested_benchmark_version(self):
        response = self.client.get(
            "/tasks/OCR-Data-Extraction/task_family_invoice_images",
            params={
                "benchmark_suite": self.catalog.suite_name,
                "benchmark_version": self.catalog.benchmark_version,
            },
            headers={"X-Request-ID": "req-task-001"},
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            response.json(),
            {
                "task": _task_payload(
                    _catalog_task(
                        self.catalog,
                        task_family="OCR-Data-Extraction",
                        instance_id="task_family_invoice_images",
                    )
                ),
                "request_id": "req-task-001",
            },
        )

    def test_api_can_read_harbor_dataset_catalog_without_execution(self):
        harbor_catalog = HarborBenchmarkProvider().list_catalogs()[0]
        with session_scope(self.engine) as session:
            BenchmarkCatalogRepository(session).upsert_fixture_catalog(harbor_catalog)

        benchmark_response = self.client.get(
            f"/benchmarks/{harbor_catalog.suite_name}",
            params={"benchmark_version": harbor_catalog.benchmark_version},
            headers={"X-Request-ID": "req-harbor-benchmark-001"},
        )
        tasks_response = self.client.get(
            "/tasks",
            params={
                "benchmark_suite": harbor_catalog.suite_name,
                "benchmark_version": harbor_catalog.benchmark_version,
            },
            headers={"X-Request-ID": "req-harbor-tasks-001"},
        )

        self.assertEqual(benchmark_response.status_code, 200)
        self.assertEqual(benchmark_response.json()["benchmark"]["metadata"]["source_type"], "harbor_dataset")
        self.assertEqual(benchmark_response.json()["benchmark"]["metadata"]["environment_types"], ["docker"])
        self.assertEqual(tasks_response.status_code, 200)
        harbor_run = tasks_response.json()["tasks"][0]["metadata"]["harbor_run"]
        self.assertEqual(harbor_run["dataset_ref"], "terminal-bench@2.0")
        self.assertEqual(harbor_run["extra_args"], ["--n-tasks", "1", "--quiet"])
        self.assertEqual(tasks_response.json()["tasks"][0]["metadata"]["verifier_type"], "harbor_verifier")

    def test_missing_benchmark_maps_to_404(self):
        response = self.client.get(
            "/tasks",
            params={
                "benchmark_suite": "MissingSuite",
                "benchmark_version": "hf:missing/version@with/slash",
            },
        )

        self.assertEqual(response.status_code, 404)
        self.assertEqual(response.json(), {"detail": "Benchmark not found"})

    def test_missing_task_family_maps_to_stable_404(self):
        response = self.client.get(
            "/task-families/missing-family",
            params={
                "benchmark_suite": self.catalog.suite_name,
                "benchmark_version": self.catalog.benchmark_version,
            },
        )

        self.assertEqual(response.status_code, 404)
        self.assertEqual(response.json(), {"detail": "Task family not found"})

    def test_missing_task_maps_to_404(self):
        response = self.client.get(
            "/tasks/OCR-Data-Extraction/missing-instance",
            params={
                "benchmark_suite": self.catalog.suite_name,
                "benchmark_version": self.catalog.benchmark_version,
            },
        )

        self.assertEqual(response.status_code, 404)
        self.assertEqual(response.json(), {"detail": "Task not found"})


def _app_for_engine(engine) -> FastAPI:
    app = FastAPI()

    @app.middleware("http")
    async def attach_request_id(request: Request, call_next):
        request.state.request_id = request.headers.get("X-Request-ID", "")
        return await call_next(request)

    def session_dependency():
        with session_scope(engine) as session:
            yield session

    register_benchmark_routes(app, session_dependency)
    return app


def _benchmark_summary(catalog: BenchmarkFixtureCatalog) -> dict:
    return {
        "suite_name": catalog.suite_name,
        "benchmark_version": catalog.benchmark_version,
        "source_uri": catalog.source_uri,
        "source_version": catalog.source_version,
        "source_version_type": catalog.source_version_type,
        "task_family_count": len(catalog.task_families),
        "task_instance_count": len(catalog.task_instances()),
        "metadata": catalog.metadata,
    }


def _catalog_task(catalog: BenchmarkFixtureCatalog, *, task_family: str, instance_id: str):
    for instance in catalog.task_instances():
        if instance.task_family == task_family and instance.instance_id == instance_id:
            return instance
    raise AssertionError(f"Missing fixture task {task_family}/{instance_id}")


def _catalog_family(catalog: BenchmarkFixtureCatalog, name: str):
    for family in catalog.task_families:
        if family.name == name:
            return family
    raise AssertionError(f"Missing fixture family {name}")


def _family_summary(family) -> dict:
    return {
        "name": family.name,
        "task_instance_count": len(family.instances),
    }


def _family_detail(family) -> dict:
    return {
        **_family_summary(family),
        "tasks": [_task_payload(instance) for instance in family.instances],
    }


def _ordered_tasks(catalog: BenchmarkFixtureCatalog) -> list:
    return sorted(catalog.task_instances(), key=lambda instance: (instance.task_family, instance.instance_id))


def _task_payload(instance) -> dict:
    return {
        "task_family": instance.task_family,
        "instance_id": instance.instance_id,
        "instruction_ref": instance.instruction_ref,
        "input_files": instance.input_files,
        "input_artifact_refs": instance.input_artifact_refs,
        "required_artifacts": instance.required_artifacts,
        "runner_image": instance.runner_image,
        "runner_entrypoint": instance.runner_entrypoint,
        "runner_contract": instance.runner_contract,
        "metadata": instance.metadata,
    }
