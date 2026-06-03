import json
import subprocess
import tempfile
import unittest
from pathlib import Path

from fastapi.testclient import TestClient
from sqlalchemy import delete
from sqlalchemy.pool import StaticPool

from agentic_data_platform.artifacts.store import ArtifactPersistence, LocalArtifactStore
from agentic_data_platform.benchmarks.fixtures import load_fixture_catalog
from agentic_data_platform.persistence.database import create_database_engine, session_scope
from agentic_data_platform.persistence.migrations import upgrade_database
from agentic_data_platform.persistence.models import ArtifactRow, EvaluatorResultRow, RunTerminalTurnRow
from agentic_data_platform.persistence.repositories import (
    BenchmarkCatalogRepository,
    IdentityRepository,
    ProjectRepository,
    RunRepository,
)
from agentic_data_platform.service.app import create_app
from agentic_data_platform.service.config import ServiceSettings
from agentic_data_platform.worker.executors import DockerTerminalWorkerExecutor
from agentic_data_platform.worker.service import RunWorker


class DashboardContractSmokeTest(unittest.TestCase):
    def setUp(self):
        self.engine = create_database_engine(
            "sqlite+pysqlite:///:memory:",
            connect_args={"check_same_thread": False},
            poolclass=StaticPool,
        )
        upgrade_database(self.engine)
        self.catalog = load_fixture_catalog("SkillLearnBench")
        self.task = self.catalog.task_instances()[0]
        with session_scope(self.engine) as session:
            identities = IdentityRepository(session)
            identities.create_team(
                team_id="pilot-project",
                name="pilot group",
            )
            identities.create_user(
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
            BenchmarkCatalogRepository(session).upsert_fixture_catalog(self.catalog)
        self.client = TestClient(_app(self.engine), headers={"Authorization": "Bearer [REDACTED_OWNER]-token"})

    def tearDown(self):
        self.engine.dispose()

    def test_api_worker_dashboard_contract_exposes_pm_and_research_views(self):
        catalog_response = self.client.get("/benchmarks", headers={"X-Request-ID": "req-contract-catalog-001"})
        task_response = self.client.get(
            "/tasks",
            params={
                "benchmark_suite": self.catalog.suite_name,
                "benchmark_version": self.catalog.benchmark_version,
            },
            headers={"X-Request-ID": "req-contract-tasks-001"},
        )

        self.assertEqual(catalog_response.status_code, 200)
        self.assertEqual(task_response.status_code, 200)
        self.assertEqual(catalog_response.json()["benchmarks"][0]["suite_name"], "SkillLearnBench")
        self.assertIn(
            self.task.instance_id,
            {task["instance_id"] for task in task_response.json()["tasks"]},
        )

        create_response = self.client.post(
            "/runs",
            json=_run_create_payload(
                run_id="run_dashboard_contract_001",
                catalog=self.catalog,
                task=self.task,
            ),
            headers={"X-Request-ID": "req-contract-create-001"},
        )

        self.assertEqual(create_response.status_code, 201)
        self.assertEqual(create_response.json()["run"]["status"], "queued")

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            command_runner = FakeDockerCommandRunner(
                stdout="created solution/answer.txt\n",
                write_files={"solution/answer.txt": "final workspace output\n"},
            )
            worker = RunWorker(
                engine=self.engine,
                worker_id="worker-dashboard-contract",
                executor=DockerTerminalWorkerExecutor(
                    artifact_persistence=ArtifactPersistence(LocalArtifactStore(temp_path / "artifacts")),
                    workspace_root=temp_path / "workspaces",
                    command_runner=command_runner,
                ),
            )

            worker_result = worker.run_once(request_id="req-contract-worker-001")

        self.assertIsNotNone(worker_result)
        self.assertEqual(worker_result.status, "succeeded")

        detail = self.client.get(
            "/runs/run_dashboard_contract_001",
            headers={"X-Request-ID": "req-contract-detail-001"},
        )
        artifacts = self.client.get(
            "/runs/run_dashboard_contract_001/artifacts",
            headers={"X-Request-ID": "req-contract-artifacts-001"},
        )
        evaluation = self.client.get(
            "/runs/run_dashboard_contract_001/evaluation",
            headers={"X-Request-ID": "req-contract-evaluation-001"},
        )
        progress = self.client.get(
            "/dashboard/progress",
            params={"project_id": "pilot-project"},
            headers={"X-Request-ID": "req-contract-progress-001"},
        )

        self.assertEqual(detail.status_code, 200)
        self.assertEqual(artifacts.status_code, 200)
        self.assertEqual(evaluation.status_code, 200)
        self.assertEqual(progress.status_code, 200)

        detail_payload = detail.json()
        self.assertEqual(detail_payload["request_id"], "req-contract-detail-001")
        self.assertEqual(detail_payload["run"]["status"], "succeeded")
        self.assertEqual(detail_payload["run"]["progress"]["turn_count"], 1)
        self.assertEqual(detail_payload["run"]["progress"]["artifact_count"], 3)
        self.assertEqual(detail_payload["run"]["evaluator"]["status"], "completed")
        self.assertEqual(detail_payload["trajectory"][0]["command"], "python solve.py")
        self.assertEqual(detail_payload["trajectory"][0]["stdout"], "created solution/answer.txt\n")
        self.assertEqual(detail_payload["trajectory"][0]["changed_paths"], ["solution/answer.txt"])
        self.assertEqual(
            [event["event_type"] for event in detail_payload["lifecycle_events"]],
            [
                "run.created",
                "run.claimed",
                "sandbox.container_started",
                "sandbox.container_completed",
                "run.started",
                "run.evaluating",
                "evaluator.completed",
                "run.succeeded",
                "log.chunk_recorded",
                "artifact.upload_status_changed",
            ],
        )

        artifact_kinds = {artifact["kind"] for artifact in artifacts.json()["artifacts"]}
        self.assertEqual(artifact_kinds, {"trajectory", "workspace_snapshot", "evaluator_report"})
        self.assertIn(
            "Docker terminal trajectory and workspace were reviewed",
            evaluation.json()["evaluation"]["verbal_feedback_summary"],
        )

        progress_payload = progress.json()
        self.assertEqual(progress_payload["request_id"], "req-contract-progress-001")
        self.assertEqual(progress_payload["summary"]["total_runs"], 1)
        self.assertEqual(progress_payload["summary"]["runs_by_status"]["succeeded"], 1)
        self.assertEqual(progress_payload["summary"]["artifact_count"], 3)
        self.assertEqual(progress_payload["summary"]["turn_count"], 1)
        self.assertEqual(progress_payload["projects"][0]["project_id"], "pilot-project")
        self.assertEqual(progress_payload["projects"][0]["runs_by_status"]["succeeded"], 1)
        self.assertEqual(progress_payload["projects"][0]["average_evaluator_score"], 0.75)

        rendered = json.dumps(
            {
                "detail": detail_payload,
                "artifacts": artifacts.json(),
                "evaluation": evaluation.json(),
                "progress": progress_payload,
            }
        )
        self.assertNotIn("file://", rendered)
        self.assertNotIn(str(Path(tempfile.gettempdir()).resolve()), rendered)
        self.assertNotIn("X-Amz-Signature", rendered)

    def test_dashboard_progress_reads_durable_terminal_projection_without_child_hydration(self):
        create_response = self.client.post(
            "/runs",
            json=_run_create_payload(
                run_id="run_dashboard_projection_progress_001",
                catalog=self.catalog,
                task=self.task,
            ),
        )
        self.assertEqual(create_response.status_code, 201)

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            worker = RunWorker(
                engine=self.engine,
                worker_id="worker-dashboard-projection-progress",
                executor=DockerTerminalWorkerExecutor(
                    artifact_persistence=ArtifactPersistence(LocalArtifactStore(temp_path / "artifacts")),
                    workspace_root=temp_path / "workspaces",
                    command_runner=FakeDockerCommandRunner(
                        stdout="created projection-progress.txt\n",
                        write_files={"projection-progress.txt": "projection progress output\n"},
                    ),
                ),
            )

            worker_result = worker.run_once(request_id="req-projection-progress-worker-001")

        self.assertIsNotNone(worker_result)
        self.assertEqual(worker_result.status, "succeeded")

        with session_scope(self.engine) as session:
            projection = RunRepository(session).get_dashboard_projection("run_dashboard_projection_progress_001")
            self.assertEqual(projection.payload["progress"]["turn_count"], 1)
            self.assertEqual(projection.payload["progress"]["artifact_count"], 3)
            session.execute(
                delete(RunTerminalTurnRow).where(
                    RunTerminalTurnRow.run_id == "run_dashboard_projection_progress_001"
                )
            )
            session.execute(
                delete(ArtifactRow).where(ArtifactRow.run_id == "run_dashboard_projection_progress_001")
            )
            session.execute(
                delete(EvaluatorResultRow).where(
                    EvaluatorResultRow.run_id == "run_dashboard_projection_progress_001"
                )
            )

        progress = self.client.get(
            "/dashboard/progress",
            params={"project_id": "pilot-project"},
            headers={"X-Request-ID": "req-projection-progress-001"},
        )

        self.assertEqual(progress.status_code, 200)
        payload = progress.json()
        self.assertEqual(payload["request_id"], "req-projection-progress-001")
        self.assertEqual(payload["summary"]["total_runs"], 1)
        self.assertEqual(payload["summary"]["runs_by_status"]["succeeded"], 1)
        self.assertEqual(payload["summary"]["artifact_count"], 3)
        self.assertEqual(payload["summary"]["turn_count"], 1)
        self.assertEqual(payload["summary"]["evaluator_completed_count"], 1)
        self.assertEqual(payload["summary"]["average_evaluator_score"], 0.75)
        self.assertEqual(payload["projects"][0]["artifact_count"], 3)
        self.assertEqual(payload["projects"][0]["turn_count"], 1)


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
            internal_auth_tokens="[REDACTED_OWNER]=[REDACTED_OWNER]-token",
        ),
        database_engine=engine,
    )


def _run_create_payload(*, run_id: str, catalog, task) -> dict:
    return {
        "run_id": run_id,
        "project_id": "pilot-project",
        "owner_team": "pilot group",
        "created_by_user_id": "[REDACTED_OWNER]",
        "task": {
            "benchmark_suite": catalog.suite_name,
            "benchmark_version": catalog.benchmark_version,
            "task_family": task.task_family,
            "instance_id": task.instance_id,
            "source_uri": catalog.source_uri,
            "input_artifact_refs": list(task.input_artifact_refs),
            "required_artifacts": list(task.required_artifacts),
            "metadata": {
                "instruction": "Solve the SkillLearnBench task and write the final answer in the workspace.",
                "instruction_ref": task.instruction_ref,
                "input_files": list(task.input_files),
            },
        },
        "model": {
            "provider": "mock-api",
            "model_name": "scripted-terminal-agent",
            "mode": "api",
            "prompt_template_version": "terminal-agent-v0",
            "metadata": {"temperature": 0},
        },
        "runner": {
            "kind": "original_benchmark",
            "sandbox_backend": "docker_terminal",
            "image": task.runner_image,
            "entrypoint": list(task.runner_entrypoint),
            "internet_access": True,
            "resource_limits": {"cpu": 2, "memory_gib": 8, "timeout_seconds": 3600},
            "metadata": {"runner_contract": task.runner_contract},
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
        "metadata": {
            "worker_commands": [
                {
                    "command": "python solve.py",
                    "cwd": "/workspace",
                    "model_call_id": "call-dashboard-contract-1",
                }
            ]
        },
    }


class FakeDockerCommandRunner:
    def __init__(
        self,
        *,
        returncode: int = 0,
        stdout: str = "",
        stderr: str = "",
        write_files: dict[str, str] | None = None,
    ) -> None:
        self.returncode = returncode
        self.stdout = stdout
        self.stderr = stderr
        self.write_files = write_files or {}

    def run(self, args: list[str], *, timeout: int) -> subprocess.CompletedProcess[str]:
        workspace = _workspace_from_docker_args(args)
        for relative_path, content in self.write_files.items():
            target = workspace / relative_path
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(content)
        return subprocess.CompletedProcess(
            args=args,
            returncode=self.returncode,
            stdout=self.stdout,
            stderr=self.stderr,
        )


def _workspace_from_docker_args(args: list[str]) -> Path:
    volume_index = args.index("-v")
    return Path(args[volume_index + 1].split(":", maxsplit=1)[0])
