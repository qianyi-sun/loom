import json
import subprocess
import tempfile
import unittest
from pathlib import Path

import httpx
from fastapi.testclient import TestClient
from sqlalchemy.pool import StaticPool

from agentic_data_platform.artifacts.store import ArtifactPersistence, LocalArtifactStore
from agentic_data_platform.persistence.database import create_database_engine, session_scope
from agentic_data_platform.persistence.migrations import upgrade_database
from agentic_data_platform.persistence.repositories import IdentityRepository, ProjectRepository
from agentic_data_platform.providers.config import DevProviderConfigRegistry
from agentic_data_platform.service.app import create_app
from agentic_data_platform.service.config import ServiceSettings
from agentic_data_platform.worker.executors import DockerTerminalWorkerExecutor, FixtureTerminalBenchmarkExecutor
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
        self.client = TestClient(_app(self.engine), headers={"Authorization": "Bearer [REDACTED_OWNER]-token"})

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

    def test_fixture_worker_resolves_dev_provider_refs_without_exposing_secrets(self):
        payload = _run_create_payload("run_worker_provider_refs_001")
        payload["model"]["provider_config_id"] = "default-agent-model"
        payload["model"]["secret_ref"] = "env:MODEL_PROVIDER_API_KEY"
        payload["evaluators"][0]["provider_config_id"] = "default-evaluator-model"
        payload["evaluators"][0]["secret_ref"] = "env:EVALUATOR_PROVIDER_API_KEY"
        create_response = self.client.post("/runs", json=payload)
        self.assertEqual(create_response.status_code, 201)

        registry = DevProviderConfigRegistry.from_settings(
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
                model_provider_api_key="sk-model-secret",
                evaluator_provider_api_key="sk-evaluator-secret",
            )
        )
        with tempfile.TemporaryDirectory() as temp_dir:
            worker = RunWorker(
                engine=self.engine,
                worker_id="worker-test",
                executor=FixtureTerminalBenchmarkExecutor(
                    artifact_persistence=ArtifactPersistence(LocalArtifactStore(Path(temp_dir))),
                    provider_registry=registry,
                ),
            )

            result = worker.run_once(request_id="req-worker-provider-refs-001")

        self.assertIsNotNone(result)
        self.assertEqual(result.status, "succeeded")
        detail = self.client.get("/runs/run_worker_provider_refs_001")
        rendered = json.dumps(detail.json())
        self.assertNotIn("sk-model-secret", rendered)
        self.assertNotIn("sk-evaluator-secret", rendered)

    def test_worker_completes_api_created_run_through_docker_terminal_executor(self):
        payload = _run_create_payload("run_worker_docker_001")
        payload["metadata"] = {
            "worker_commands": [
                {
                    "command": "python solve.py",
                    "cwd": "/workspace",
                    "model_call_id": "call-docker-1",
                }
            ]
        }
        create_response = self.client.post(
            "/runs",
            json=payload,
            headers={"X-Request-ID": "req-create-docker-worker-001"},
        )
        self.assertEqual(create_response.status_code, 201)

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            command_runner = FakeDockerCommandRunner(
                stdout="created receipts workbook\n",
                write_files={"receipts.xlsx": "spreadsheet bytes\n"},
            )
            worker = RunWorker(
                engine=self.engine,
                worker_id="worker-docker-test",
                executor=DockerTerminalWorkerExecutor(
                    artifact_persistence=ArtifactPersistence(LocalArtifactStore(temp_path / "artifacts")),
                    workspace_root=temp_path / "workspaces",
                    command_runner=command_runner,
                ),
            )

            result = worker.run_once(request_id="req-worker-docker-once-001")

            trajectory_path = (
                temp_path
                / "artifacts/runs/run_worker_docker_001/tasks/conference-expense-03/trajectory/trajectory.jsonl"
            )
            workspace_path = (
                temp_path
                / "artifacts/runs/run_worker_docker_001/tasks/conference-expense-03/workspace/snapshot.json"
            )

            self.assertTrue(trajectory_path.exists())
            self.assertTrue(workspace_path.exists())
            trajectory_text = trajectory_path.read_text()
            workspace_text = workspace_path.read_text()

        self.assertIsNotNone(result)
        self.assertEqual(result.status, "succeeded")
        docker_args = command_runner.calls[0]["args"]
        self.assertEqual(docker_args[:3], ["docker", "run", "--rm"])
        self.assertIn("python:3.12-slim", docker_args)
        self.assertIn("python solve.py", docker_args)
        self.assertIn("call-docker-1", trajectory_text)
        self.assertIn("receipts.xlsx", workspace_text)

        detail = self.client.get("/runs/run_worker_docker_001")
        payload = detail.json()
        self.assertEqual(payload["run"]["status"], "succeeded")
        self.assertEqual(payload["run"]["progress"]["turn_count"], 1)
        self.assertEqual(payload["run"]["progress"]["artifact_count"], 3)
        self.assertEqual(payload["run"]["evaluator"]["status"], "completed")

    def test_docker_worker_uses_configured_api_model_provider_without_leaking_secret(self):
        payload = _run_create_payload("run_worker_api_provider_001")
        payload["model"]["provider"] = "openai-compatible"
        payload["model"]["model_name"] = "gpt-5-mini"
        payload["model"]["provider_config_id"] = "default-agent-model"
        payload["metadata"] = {}
        create_response = self.client.post("/runs", json=payload)
        self.assertEqual(create_response.status_code, 201)

        requests: list[httpx.Request] = []
        responses = [
            {
                "id": "chatcmpl_worker_001",
                "choices": [
                    {
                        "message": {
                            "content": (
                                '{"action":"run","command":"python solve.py",'
                                '"cwd":"/workspace"}'
                            )
                        }
                    }
                ],
            },
            {
                "id": "chatcmpl_worker_002",
                "choices": [{"message": {"content": '{"action":"finish"}'}}],
            },
        ]

        def handler(request: httpx.Request) -> httpx.Response:
            requests.append(request)
            return httpx.Response(200, json=responses.pop(0))

        registry = DevProviderConfigRegistry.from_settings(
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
                model_provider_base_url="https://models.example/v1",
                model_provider_api_key="sk-model-secret",
            )
        )
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            command_runner = FakeDockerCommandRunner(
                stdout="created receipts workbook\n",
                write_files={"receipts.xlsx": "spreadsheet bytes\n"},
            )
            worker = RunWorker(
                engine=self.engine,
                worker_id="worker-api-provider-test",
                executor=DockerTerminalWorkerExecutor(
                    artifact_persistence=ArtifactPersistence(LocalArtifactStore(temp_path / "artifacts")),
                    workspace_root=temp_path / "workspaces",
                    provider_registry=registry,
                    command_runner=command_runner,
                    model_http_client=httpx.Client(transport=httpx.MockTransport(handler)),
                ),
            )

            result = worker.run_once(request_id="req-worker-api-provider-001")

        self.assertIsNotNone(result)
        self.assertEqual(result.status, "succeeded")
        self.assertEqual(command_runner.calls[0]["args"][-1], "python solve.py")
        self.assertEqual(len(requests), 2)
        self.assertEqual(requests[0].headers["authorization"], "Bearer sk-model-secret")
        self.assertIn(
            "created receipts workbook",
            json.loads(requests[1].content)["messages"][-1]["content"],
        )

        detail = self.client.get("/runs/run_worker_api_provider_001")
        rendered = json.dumps(detail.json())
        self.assertNotIn("sk-model-secret", rendered)
        self.assertNotIn("secret_ref", rendered)
        self.assertEqual(detail.json()["run"]["progress"]["turn_count"], 1)

    def test_docker_worker_normalizes_unknown_api_provider_config(self):
        payload = _run_create_payload("run_worker_unknown_provider_config_001")
        payload["model"]["provider"] = "openai-compatible"
        payload["model"]["model_name"] = "gpt-5-mini"
        payload["model"]["provider_config_id"] = "missing-agent-model"
        payload["metadata"] = {}
        create_response = self.client.post("/runs", json=payload)
        self.assertEqual(create_response.status_code, 201)

        registry = DevProviderConfigRegistry.from_settings(
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
                model_provider_base_url="https://models.example/v1",
                model_provider_api_key="sk-model-secret",
            )
        )
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            worker = RunWorker(
                engine=self.engine,
                worker_id="worker-api-provider-test",
                executor=DockerTerminalWorkerExecutor(
                    artifact_persistence=ArtifactPersistence(LocalArtifactStore(temp_path / "artifacts")),
                    workspace_root=temp_path / "workspaces",
                    provider_registry=registry,
                    command_runner=FakeDockerCommandRunner(),
                ),
            )

            result = worker.run_once(request_id="req-worker-api-provider-unknown-config-001")

        self.assertIsNotNone(result)
        self.assertEqual(result.status, "failed")
        detail = self.client.get("/runs/run_worker_unknown_provider_config_001")
        rendered = json.dumps(detail.json())
        self.assertIn("model provider invalid_request", detail.json()["run"]["failure_reason"])
        self.assertIn("Unknown provider config", detail.json()["run"]["failure_reason"])
        self.assertEqual(detail.json()["run"]["progress"]["artifact_count"], 2)
        self.assertNotIn("sk-model-secret", rendered)

    def test_docker_worker_normalizes_missing_api_provider_base_url(self):
        payload = _run_create_payload("run_worker_missing_provider_base_url_001")
        payload["model"]["provider"] = "openai-compatible"
        payload["model"]["model_name"] = "gpt-5-mini"
        payload["model"]["provider_config_id"] = "default-agent-model"
        payload["model"]["secret_ref"] = "env:MODEL_PROVIDER_API_KEY"
        payload["metadata"] = {}
        create_response = self.client.post("/runs", json=payload)
        self.assertEqual(create_response.status_code, 201)

        registry = DevProviderConfigRegistry.from_settings(
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
                model_provider_api_key="sk-model-secret",
            )
        )
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            worker = RunWorker(
                engine=self.engine,
                worker_id="worker-api-provider-test",
                executor=DockerTerminalWorkerExecutor(
                    artifact_persistence=ArtifactPersistence(LocalArtifactStore(temp_path / "artifacts")),
                    workspace_root=temp_path / "workspaces",
                    provider_registry=registry,
                    command_runner=FakeDockerCommandRunner(),
                ),
            )

            result = worker.run_once(request_id="req-worker-api-provider-missing-base-url-001")

        self.assertIsNotNone(result)
        self.assertEqual(result.status, "failed")
        detail = self.client.get("/runs/run_worker_missing_provider_base_url_001")
        rendered = json.dumps(detail.json())
        self.assertIn("model provider invalid_request", detail.json()["run"]["failure_reason"])
        self.assertEqual(detail.json()["run"]["progress"]["artifact_count"], 2)
        self.assertNotIn("sk-model-secret", rendered)

    def test_worker_marks_docker_terminal_command_failure_with_diagnostics(self):
        payload = _run_create_payload("run_worker_docker_failed_001")
        payload["metadata"] = {"worker_commands": ["python missing.py"]}
        create_response = self.client.post("/runs", json=payload)
        self.assertEqual(create_response.status_code, 201)

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            command_runner = FakeDockerCommandRunner(
                returncode=9,
                stderr="missing.py: not found\n",
            )
            worker = RunWorker(
                engine=self.engine,
                worker_id="worker-docker-test",
                executor=DockerTerminalWorkerExecutor(
                    artifact_persistence=ArtifactPersistence(LocalArtifactStore(temp_path / "artifacts")),
                    workspace_root=temp_path / "workspaces",
                    command_runner=command_runner,
                ),
            )

            result = worker.run_once(request_id="req-worker-docker-fail-001")

            trajectory_path = (
                temp_path
                / "artifacts/runs/run_worker_docker_failed_001/tasks/conference-expense-03/trajectory/trajectory.jsonl"
            )
            workspace_path = (
                temp_path
                / "artifacts/runs/run_worker_docker_failed_001/tasks/conference-expense-03/workspace/snapshot.json"
            )

            self.assertTrue(trajectory_path.exists())
            self.assertTrue(workspace_path.exists())

        self.assertIsNotNone(result)
        self.assertEqual(result.status, "failed")

        detail = self.client.get("/runs/run_worker_docker_failed_001")
        payload = detail.json()
        self.assertEqual(payload["run"]["status"], "failed")
        self.assertIn("exit code 9", payload["run"]["failure_reason"])
        self.assertEqual(payload["run"]["progress"]["turn_count"], 1)
        self.assertEqual(payload["run"]["progress"]["artifact_count"], 2)
        self.assertEqual(
            [event["event_type"] for event in payload["lifecycle_events"]],
            ["run.created", "run.claimed", "run.started", "run.failed"],
        )

    def test_worker_executes_harbor_run_and_ingests_verifier_result(self):
        payload = _run_create_payload("run_worker_harbor_001")
        payload["runner"]["metadata"] = {"runner_contract": "harbor-local-docker-v0"}
        payload["evaluators"] = [{"evaluator_id": "harbor-verifier", "mode": "harbor_verifier"}]
        payload["metadata"] = {
            "harbor_run": {
                "dataset_ref": "terminal-bench/terminal-bench-2",
                "agent": "claude-code",
                "trial_name": "trial-hello",
                "extra_args": ["--n-tasks", "1"],
            }
        }
        create_response = self.client.post("/runs", json=payload)
        self.assertEqual(create_response.status_code, 201)

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            command_runner = FakeHarborCommandRunner()
            worker = RunWorker(
                engine=self.engine,
                worker_id="worker-harbor-test",
                executor=DockerTerminalWorkerExecutor(
                    artifact_persistence=ArtifactPersistence(LocalArtifactStore(temp_path / "artifacts")),
                    workspace_root=temp_path / "workspaces",
                    harbor_command_runner=command_runner,
                ),
            )

            result = worker.run_once(request_id="req-worker-harbor-001")

        self.assertIsNotNone(result)
        self.assertEqual(result.status, "succeeded")
        harbor_args = command_runner.calls[0]["args"]
        self.assertEqual(harbor_args[:4], ["harbor", "run", "-d", "terminal-bench/terminal-bench-2"])
        self.assertIn("--jobs-dir", harbor_args)
        self.assertIn("--env", harbor_args)
        self.assertIn("--yes", harbor_args)
        self.assertIn("--n-tasks", harbor_args)

        detail = self.client.get("/runs/run_worker_harbor_001")
        payload = detail.json()
        self.assertEqual(payload["run"]["status"], "succeeded")
        self.assertEqual(payload["run"]["evaluator"]["mode"], "harbor_verifier")
        self.assertEqual(payload["run"]["evaluator"]["score"], 1.0)
        self.assertEqual(payload["run"]["progress"]["turn_count"], 1)
        artifact_kinds = {artifact["kind"] for artifact in payload["run"]["artifacts"]}
        self.assertIn("log", artifact_kinds)
        self.assertIn("trajectory", artifact_kinds)
        self.assertIn("workspace_snapshot", artifact_kinds)
        self.assertIn("evaluator_report", artifact_kinds)
        self.assertIn("generated_file", artifact_kinds)
        self.assertEqual(
            [event["event_type"] for event in payload["lifecycle_events"]],
            ["run.created", "run.claimed", "run.started", "run.evaluating", "run.succeeded"],
        )

    def test_worker_materializes_generated_harbor_smoke_task(self):
        payload = _run_create_payload("run_worker_harbor_smoke_001")
        payload["runner"]["metadata"] = {"runner_contract": "harbor-local-docker-v0"}
        payload["evaluators"] = [{"evaluator_id": "harbor-verifier", "mode": "harbor_verifier"}]
        payload["metadata"] = {
            "harbor_run": {
                "task_template": "harbor-cli-smoke",
                "agent": "oracle",
                "model_name": "smoke/noop",
                "environment": "docker",
                "timeout_seconds": 30,
                "extra_args": ["--n-tasks", "1", "--quiet"],
            }
        }
        create_response = self.client.post("/runs", json=payload)
        self.assertEqual(create_response.status_code, 201)

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            command_runner = FakeHarborCommandRunner()
            worker = RunWorker(
                engine=self.engine,
                worker_id="worker-harbor-smoke-test",
                executor=DockerTerminalWorkerExecutor(
                    artifact_persistence=ArtifactPersistence(LocalArtifactStore(temp_path / "artifacts")),
                    workspace_root=temp_path / "workspaces",
                    harbor_command_runner=command_runner,
                ),
            )

            result = worker.run_once(request_id="req-worker-harbor-smoke-001")
            harbor_args = command_runner.calls[0]["args"]
            task_dir = Path(harbor_args[harbor_args.index("-p") + 1])

            self.assertTrue((task_dir / "task.toml").is_file())
            self.assertTrue((task_dir / "solution" / "solve.sh").is_file())

        self.assertIsNotNone(result)
        self.assertEqual(result.status, "succeeded")
        self.assertEqual(harbor_args[:3], ["harbor", "run", "-p"])
        self.assertIn("--model", harbor_args)
        self.assertEqual(harbor_args[harbor_args.index("--model") + 1], "smoke/noop")
        self.assertIn("--quiet", harbor_args)

        detail = self.client.get("/runs/run_worker_harbor_smoke_001")
        payload = detail.json()
        self.assertEqual(payload["run"]["status"], "succeeded")
        self.assertEqual(payload["run"]["evaluator"]["mode"], "harbor_verifier")
        self.assertGreaterEqual(payload["run"]["progress"]["artifact_count"], 4)

    def test_worker_normalizes_harbor_runner_failure(self):
        payload = _run_create_payload("run_worker_harbor_failed_001")
        payload["runner"]["metadata"] = {"runner_contract": "harbor-local-docker-v0"}
        payload["metadata"] = {
            "harbor_run": {
                "dataset_ref": "terminal-bench/terminal-bench-2",
                "agent": "claude-code",
            }
        }
        create_response = self.client.post("/runs", json=payload)
        self.assertEqual(create_response.status_code, 201)

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            worker = RunWorker(
                engine=self.engine,
                worker_id="worker-harbor-test",
                executor=DockerTerminalWorkerExecutor(
                    artifact_persistence=ArtifactPersistence(LocalArtifactStore(temp_path / "artifacts")),
                    workspace_root=temp_path / "workspaces",
                    harbor_command_runner=FakeHarborCommandRunner(returncode=17, stderr="harbor failed\n"),
                ),
            )

            result = worker.run_once(request_id="req-worker-harbor-fail-001")

        self.assertIsNotNone(result)
        self.assertEqual(result.status, "failed")
        detail = self.client.get("/runs/run_worker_harbor_failed_001")
        payload = detail.json()
        self.assertIn("exit code 17", payload["run"]["failure_reason"])
        self.assertEqual(payload["run"]["progress"]["artifact_count"], 1)
        self.assertEqual(
            [event["event_type"] for event in payload["lifecycle_events"]],
            ["run.created", "run.claimed", "run.started", "run.failed"],
        )

    def test_worker_executes_original_wrapper_run_and_attaches_result_artifacts(self):
        payload = _run_create_payload("run_worker_wrapper_001")
        payload["task"]["metadata"]["instruction_ref"] = "inline:task.metadata.instruction"
        payload["evaluators"] = [{"evaluator_id": "original-wrapper-verifier", "mode": "harbor_verifier"}]
        payload["metadata"] = {
            "wrapper_run": {
                "dry_run": True,
                "timeout_seconds": 45,
            }
        }
        create_response = self.client.post("/runs", json=payload)
        self.assertEqual(create_response.status_code, 201)

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            command_runner = FakeWrapperCommandRunner()
            worker = RunWorker(
                engine=self.engine,
                worker_id="worker-wrapper-test",
                executor=DockerTerminalWorkerExecutor(
                    artifact_persistence=ArtifactPersistence(LocalArtifactStore(temp_path / "artifacts")),
                    workspace_root=temp_path / "workspaces",
                    wrapper_command_runner=command_runner,
                ),
            )

            result = worker.run_once(request_id="req-worker-wrapper-001")
            wrapper_args = command_runner.calls[0]["args"]
            manifest_path = Path(_arg_value(wrapper_args, "--task-manifest"))
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))

        self.assertIsNotNone(result)
        self.assertEqual(result.status, "succeeded")
        self.assertEqual(result.turn_count, 1)
        self.assertEqual(wrapper_args[:3], ["python", "-m", "agentic_data_platform.benchmark_wrappers.skilllearnbench"])
        self.assertIn("--dry-run", wrapper_args)
        self.assertEqual(command_runner.calls[0]["timeout"], 45)
        self.assertEqual(manifest["run_id"], "run_worker_wrapper_001")
        self.assertEqual(manifest["suite_name"], "SkillLearnBench")
        self.assertEqual(manifest["task_family"], "spreadsheet-from-documents")
        self.assertEqual(manifest["model"]["provider"], "mock-api")
        self.assertEqual(manifest["instruction_ref"], "inline:task.metadata.instruction")

        detail = self.client.get("/runs/run_worker_wrapper_001")
        payload = detail.json()
        self.assertEqual(payload["run"]["status"], "succeeded")
        self.assertEqual(payload["run"]["evaluator"]["mode"], "harbor_verifier")
        self.assertEqual(payload["run"]["evaluator"]["score"], 0.91)
        self.assertIn("Wrapper accepted the generated spreadsheet", payload["run"]["evaluator"]["verbal_feedback_summary"])
        self.assertEqual(payload["run"]["progress"]["turn_count"], 1)
        artifact_kinds = {artifact["kind"] for artifact in payload["run"]["artifacts"]}
        self.assertIn("trajectory", artifact_kinds)
        self.assertIn("workspace_snapshot", artifact_kinds)
        self.assertIn("evaluator_report", artifact_kinds)
        self.assertIn("generated_file", artifact_kinds)
        self.assertIn("log", artifact_kinds)
        self.assertEqual(
            [event["event_type"] for event in payload["lifecycle_events"]],
            ["run.created", "run.claimed", "run.started", "run.evaluating", "run.succeeded"],
        )

    def test_worker_marks_original_wrapper_exit_failure(self):
        payload = _run_create_payload("run_worker_wrapper_failed_001")
        payload["metadata"] = {
            "wrapper_run": {
                "dry_run": True,
                "timeout_seconds": 45,
            }
        }
        create_response = self.client.post("/runs", json=payload)
        self.assertEqual(create_response.status_code, 201)

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            command_runner = FakeWrapperCommandRunner(
                returncode=7,
                status="failed",
                failure_reason="upstream wrapper exited with code 7",
            )
            worker = RunWorker(
                engine=self.engine,
                worker_id="worker-wrapper-test",
                executor=DockerTerminalWorkerExecutor(
                    artifact_persistence=ArtifactPersistence(LocalArtifactStore(temp_path / "artifacts")),
                    workspace_root=temp_path / "workspaces",
                    wrapper_command_runner=command_runner,
                ),
            )

            result = worker.run_once(request_id="req-worker-wrapper-failed-001")

        self.assertIsNotNone(result)
        self.assertEqual(result.status, "failed")
        self.assertEqual(result.turn_count, 1)

        detail = self.client.get("/runs/run_worker_wrapper_failed_001")
        payload = detail.json()
        self.assertEqual(payload["run"]["status"], "failed")
        self.assertIn("upstream wrapper exited with code 7", payload["run"]["failure_reason"])
        self.assertEqual(payload["run"]["progress"]["turn_count"], 1)
        artifact_kinds = {artifact["kind"] for artifact in payload["run"]["artifacts"]}
        self.assertIn("trajectory", artifact_kinds)
        self.assertIn("workspace_snapshot", artifact_kinds)
        self.assertIn("log", artifact_kinds)
        self.assertEqual(
            [event["event_type"] for event in payload["lifecycle_events"]],
            ["run.created", "run.claimed", "run.started", "run.failed"],
        )


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
            "entrypoint": ["python", "-m", "agentic_data_platform.benchmark_wrappers.skilllearnbench"],
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
        self.calls: list[dict[str, object]] = []

    def run(self, args: list[str], *, timeout: int) -> subprocess.CompletedProcess[str]:
        self.calls.append({"args": args, "timeout": timeout})
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


class FakeHarborCommandRunner:
    def __init__(self, *, returncode: int = 0, stdout: str = "harbor complete\n", stderr: str = "") -> None:
        self.returncode = returncode
        self.stdout = stdout
        self.stderr = stderr
        self.calls: list[dict[str, object]] = []

    def run(self, args: list[str], *, timeout: int) -> subprocess.CompletedProcess[str]:
        self.calls.append({"args": args, "timeout": timeout})
        jobs_dir = Path(args[args.index("--jobs-dir") + 1])
        _write_harbor_job_fixture(jobs_dir)
        return subprocess.CompletedProcess(
            args=args,
            returncode=self.returncode,
            stdout=self.stdout,
            stderr=self.stderr,
        )


class FakeWrapperCommandRunner:
    def __init__(
        self,
        *,
        returncode: int = 0,
        status: str = "completed",
        failure_reason: str | None = None,
    ) -> None:
        self.returncode = returncode
        self.status = status
        self.failure_reason = failure_reason
        self.calls: list[dict[str, object]] = []

    def run(self, args: list[str], *, timeout: int) -> subprocess.CompletedProcess[str]:
        self.calls.append({"args": args, "timeout": timeout})
        output_path = Path(_arg_value(args, "--output"))
        artifacts_dir = Path(_arg_value(args, "--artifacts-dir"))
        workspace = Path(_arg_value(args, "--workspace"))
        artifacts_dir.mkdir(parents=True, exist_ok=True)
        workspace.mkdir(parents=True, exist_ok=True)
        (workspace / "receipts.xlsx").write_text("spreadsheet bytes\n", encoding="utf-8")
        (artifacts_dir / "planned-command.json").write_text(
            json.dumps({"planned_command": ["python", "evaluate_skills.py"]}),
            encoding="utf-8",
        )
        (artifacts_dir / "stderr.log").write_text(self.failure_reason or "", encoding="utf-8")

        artifacts = [
            {
                "kind": "log",
                "path": "artifacts/planned-command.json",
                "media_type": "application/json",
            },
            {
                "kind": "log",
                "path": "artifacts/stderr.log",
                "media_type": "text/plain",
            },
        ]
        evaluator_report_ref = None
        metrics: dict[str, object] = {}
        if self.status == "completed":
            output_report = artifacts_dir / "upstream-output" / "result.json"
            output_report.parent.mkdir(parents=True, exist_ok=True)
            output_report.write_text(
                json.dumps({"score": 0.91, "feedback": "Wrapper accepted the generated spreadsheet"}),
                encoding="utf-8",
            )
            evaluator_report = artifacts_dir / "evaluator-report.json"
            evaluator_report.write_text(
                json.dumps(
                    {
                        "metrics": {"upstream_score": 0.91},
                        "feedback": ["Wrapper accepted the generated spreadsheet"],
                    }
                ),
                encoding="utf-8",
            )
            evaluator_report_ref = "artifacts/evaluator-report.json"
            metrics = {"upstream_score": 0.91}
            artifacts.extend(
                [
                    {
                        "kind": "evaluator_report",
                        "path": evaluator_report_ref,
                        "media_type": "application/json",
                    },
                    {
                        "kind": "upstream_output",
                        "path": "artifacts/upstream-output/result.json",
                        "media_type": "application/json",
                    },
                ]
            )

        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(
            json.dumps(
                {
                    "status": self.status,
                    "exit_code": self.returncode,
                    "metrics": metrics,
                    "artifacts": artifacts,
                    "evaluator_report_ref": evaluator_report_ref,
                    "failure_reason": self.failure_reason,
                }
            ),
            encoding="utf-8",
        )
        return subprocess.CompletedProcess(
            args=args,
            returncode=self.returncode,
            stdout="wrapper ok\n" if self.returncode == 0 else "",
            stderr=self.failure_reason or "",
        )


def _arg_value(args: list[str], name: str) -> str:
    return args[args.index(name) + 1]


def _write_harbor_job_fixture(root: Path) -> None:
    job_dir = root / "job-001"
    trial_dir = job_dir / "trial-hello"
    (trial_dir / "agent").mkdir(parents=True)
    (trial_dir / "verifier").mkdir()
    (trial_dir / "artifacts").mkdir()
    (job_dir / "config.json").write_text(
        json.dumps({"dataset": "terminal-bench/terminal-bench-2", "agent": "default"}),
        encoding="utf-8",
    )
    (job_dir / "result.json").write_text(json.dumps({"status": "completed", "accuracy": 1.0}), encoding="utf-8")
    (trial_dir / "config.json").write_text(
        json.dumps({"task": "hello-world", "verifier_version": "harbor-test-v1"}),
        encoding="utf-8",
    )
    (trial_dir / "result.json").write_text(json.dumps({"status": "completed"}), encoding="utf-8")
    (trial_dir / "agent" / "trajectory.json").write_text(
        json.dumps(
            [
                {
                    "command": "python solve.py",
                    "cwd": "/workspace",
                    "started_at": "2026-05-29T12:00:00Z",
                    "completed_at": "2026-05-29T12:00:01Z",
                    "exit_code": 0,
                    "stdout": "42\n",
                    "stderr": "",
                    "changed_paths": ["artifacts/answer.txt"],
                }
            ]
        ),
        encoding="utf-8",
    )
    (trial_dir / "verifier" / "reward.txt").write_text("1.0\n", encoding="utf-8")
    (trial_dir / "artifacts" / "answer.txt").write_text("42\n", encoding="utf-8")
    (trial_dir / "artifacts" / "manifest.json").write_text(
        json.dumps([{"destination": "artifacts/answer.txt", "type": "file", "status": "ok"}]),
        encoding="utf-8",
    )
