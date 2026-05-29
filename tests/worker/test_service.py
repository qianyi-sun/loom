import tempfile
import unittest
from pathlib import Path

from fastapi.testclient import TestClient
from sqlalchemy.pool import StaticPool

from agentic_data_platform.artifacts.store import ArtifactPersistence, LocalArtifactStore
from agentic_data_platform.persistence.database import create_database_engine, session_scope
from agentic_data_platform.persistence.migrations import upgrade_database
from agentic_data_platform.persistence.repositories import IdentityRepository, ProjectRepository
from agentic_data_platform.service.app import create_app
from agentic_data_platform.service.config import ServiceSettings
from agentic_data_platform.worker.executors import FixtureTerminalBenchmarkExecutor
from agentic_data_platform.worker.service import RunWorker


class WorkerServiceTest(unittest.TestCase):
    def setUp(self):
        self.engine = create_database_engine(
            "sqlite+pysqlite:///:memory:",
            connect_args={"check_same_thread": False},
            poolclass=StaticPool,
        )
        upgrade_database(self.engine)
        with session_scope(self.engine) as session:
            IdentityRepository(session).create_team(
                team_id="pilot-project",
                name="pilot group",
            )
            IdentityRepository(session).create_user(
                user_id="[REDACTED_OWNER]",
                email="[REDACTED_OWNER]@example.com",
                display_name="[REDACTED_OWNER]",
                team_id="pilot-project",
            )
            ProjectRepository(session).create_project(
                project_id="pilot-project",
                name="pilot group",
                owner_team_id="pilot-project",
                created_by_user_id="[REDACTED_OWNER]",
            )
        self.client = TestClient(_app(self.engine))

    def tearDown(self):
        self.engine.dispose()

    def test_worker_completes_api_created_fixture_run_and_detail_exposes_outputs(self):
        create_response = self.client.post(
            "/runs",
            json=_run_create_payload("run_worker_001"),
            headers={"X-Request-ID": "req-create-worker-001"},
        )
        self.assertEqual(create_response.status_code, 201)
        self.assertEqual(create_response.json()["run"]["status"], "queued")

        with tempfile.TemporaryDirectory() as temp_dir:
            worker = RunWorker(
                engine=self.engine,
                worker_id="worker-test",
                executor=FixtureTerminalBenchmarkExecutor(
                    artifact_persistence=ArtifactPersistence(LocalArtifactStore(Path(temp_dir))),
                ),
            )

            result = worker.run_once(request_id="req-worker-once-001")

            self.assertIsNotNone(result)
            self.assertEqual(result.run_id, "run_worker_001")
            self.assertEqual(result.status, "succeeded")
            self.assertTrue(
                (Path(temp_dir) / "runs/run_worker_001/tasks/conference-expense-03/trajectory/trajectory.jsonl").exists()
            )

        detail = self.client.get("/runs/run_worker_001", headers={"X-Request-ID": "req-worker-detail-001"})

        self.assertEqual(detail.status_code, 200)
        payload = detail.json()
        self.assertEqual(payload["request_id"], "req-worker-detail-001")
        self.assertEqual(payload["run"]["status"], "succeeded")
        self.assertEqual(payload["run"]["progress"]["turn_count"], 1)
        self.assertEqual(payload["run"]["progress"]["artifact_count"], 3)
        self.assertEqual(payload["run"]["evaluator"]["score"], 0.75)
        self.assertEqual(
            [event["event_type"] for event in payload["lifecycle_events"]],
            ["run.created", "run.claimed", "run.started", "run.evaluating", "run.succeeded"],
        )
        self.assertEqual(payload["lifecycle_events"][1]["metadata"]["worker_id"], "worker-test")

    def test_worker_returns_none_when_no_queued_run_exists(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            worker = RunWorker(
                engine=self.engine,
                worker_id="worker-test",
                executor=FixtureTerminalBenchmarkExecutor(
                    artifact_persistence=ArtifactPersistence(LocalArtifactStore(Path(temp_dir))),
                ),
            )

            result = worker.run_once(request_id="req-worker-empty-001")

        self.assertIsNone(result)


def _app(engine):
    return create_app(
        ServiceSettings(
            app_name="agentic-data-platform-test",
            environment="test",
            database_url="",
            redis_url="",
            object_storage_endpoint="",
            object_storage_bucket="",
            object_storage_access_key="",
            object_storage_secret_key="",
            object_storage_region="us-east-1",
        ),
        database_engine=engine,
    )


def _run_create_payload(run_id: str) -> dict:
    return {
        "run_id": run_id,
        "project_id": "pilot-project",
        "owner_team": "pilot group",
        "created_by_user_id": "[REDACTED_OWNER]",
        "task": {
            "benchmark_suite": "SkillLearnBench",
            "benchmark_version": "git:cxcscmu/SkillLearnBench@abc123",
            "task_family": "spreadsheet-from-documents",
            "instance_id": "conference-expense-03",
            "source_uri": "https://github.com/cxcscmu/SkillLearnBench",
            "input_artifact_refs": ["s3://agentic-data-shared dev/benchmarks/skilllearnbench/input.tar.zst"],
            "required_artifacts": ["trajectory", "workspace_snapshot", "evaluator_report"],
            "metadata": {"instruction": "Read receipts and create receipts.xlsx."},
        },
        "model": {
            "provider": "mock-api",
            "model_name": "scripted-terminal-agent",
            "mode": "api",
            "prompt_template_version": "terminal-agent-v0",
        },
        "runner": {
            "kind": "original_benchmark",
            "sandbox_backend": "docker_terminal",
            "image": "python:3.12-slim",
            "entrypoint": ["python", "-m", "skilllearnbench.runner"],
            "internet_access": True,
            "resource_limits": {"cpu": 2, "memory_gib": 8, "timeout_seconds": 3600},
            "metadata": {"runner_contract": "skilllearnbench-original-wrapper-v0"},
        },
        "evaluators": [
            {
                "evaluator_id": "mock-judge-v0",
                "mode": "llm_judge",
                "judge": {
                    "provider": "mock",
                    "model_name": "deterministic-judge",
                    "rubric_version": "latent-skill-v0",
                },
            }
        ],
        "metadata": {"worker_fixture_commands": ["python solve.py"]},
    }
