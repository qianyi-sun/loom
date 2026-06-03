import json
import subprocess
import tempfile
import unittest
import zipfile
from io import BytesIO
from pathlib import Path

import httpx
from fastapi.testclient import TestClient
from sqlalchemy.pool import StaticPool

from agentic_data_platform.artifacts.store import ArtifactPersistence, LocalArtifactStore
from agentic_data_platform.domain.artifact_metadata import ArtifactChunkKind, ArtifactUploadStatus
from agentic_data_platform.domain.run_records import EvaluatorResult, RunStatus
from agentic_data_platform.persistence.database import create_database_engine, session_scope
from agentic_data_platform.persistence.migrations import upgrade_database
from agentic_data_platform.persistence.repositories import IdentityRepository, ProjectRepository, RunRepository
from agentic_data_platform.providers.config import DevProviderConfigRegistry
from agentic_data_platform.service.app import create_app
from agentic_data_platform.service.config import ServiceSettings
from agentic_data_platform.worker.executors import DockerTerminalWorkerExecutor, FixtureTerminalBenchmarkExecutor
from agentic_data_platform.worker.service import RepositorySandboxLifecycleRecorder, RunWorker


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
            [
                "run.created",
                "run.claimed",
                "worker.heartbeat",
                "run.started",
                "run.evaluating",
                "evaluator.completed",
                "run.succeeded",
                "log.chunk_recorded",
                "artifact.upload_status_changed",
            ],
        )
        self.assertEqual(payload["lifecycle_events"][1]["metadata"]["worker_id"], "worker-test")
        heartbeat_event = next(
            event for event in payload["lifecycle_events"] if event["event_type"] == "worker.heartbeat"
        )
        self.assertEqual(heartbeat_event["from_status"], "provisioning")
        self.assertEqual(heartbeat_event["to_status"], "provisioning")
        self.assertEqual(heartbeat_event["metadata"]["worker_id"], "worker-test")
        self.assertEqual(heartbeat_event["metadata"]["execution_task_id"], "run_worker_001:attempt:1")
        self.assertEqual(heartbeat_event["metadata"]["process_status"], "heartbeating")
        self.assertEqual(heartbeat_event["metadata"]["heartbeat_status"], "running")
        self.assertIn("last_heartbeat_at", heartbeat_event["metadata"])
        evaluator_event = next(
            event for event in payload["lifecycle_events"] if event["event_type"] == "evaluator.completed"
        )
        self.assertEqual(evaluator_event["from_status"], "evaluating")
        self.assertEqual(evaluator_event["to_status"], "evaluating")
        self.assertEqual(evaluator_event["metadata"]["evaluator_id"], "mock-judge-v0")
        self.assertEqual(evaluator_event["metadata"]["mode"], "llm_judge")
        self.assertEqual(evaluator_event["metadata"]["status"], "completed")
        self.assertEqual(evaluator_event["metadata"]["score"], 0.75)
        self.assertEqual(evaluator_event["metadata"]["worker_id"], "worker-test")
        self.assertEqual(evaluator_event["metadata"]["execution_task_id"], "run_worker_001:attempt:1")
        self.assertIn("artifact_refs", evaluator_event["metadata"])
        self.assertNotIn("verbal_feedback", evaluator_event["metadata"])
        self.assertNotIn("metrics", evaluator_event["metadata"])

    def test_worker_claims_scheduler_dispatched_run(self):
        create_response = self.client.post(
            "/runs",
            json=_run_create_payload("run_worker_dispatched_001"),
            headers={"X-Request-ID": "req-create-worker-dispatched-001"},
        )
        self.assertEqual(create_response.status_code, 201)
        with session_scope(self.engine) as session:
            dispatched = RunRepository(session).dispatch_queued_runs(
                scheduler_id="scheduler-test",
                max_runs=1,
                request_id="req-dispatch-worker-001",
            )
        self.assertEqual([run.run_id for run in dispatched], ["run_worker_dispatched_001"])

        with tempfile.TemporaryDirectory() as temp_dir:
            worker = RunWorker(
                engine=self.engine,
                worker_id="worker-test",
                executor=FixtureTerminalBenchmarkExecutor(
                    artifact_persistence=ArtifactPersistence(LocalArtifactStore(Path(temp_dir))),
                ),
            )

            result = worker.run_once(request_id="req-worker-dispatched-001")

        self.assertIsNotNone(result)
        self.assertEqual(result.run_id, "run_worker_dispatched_001")
        self.assertEqual(result.status, "succeeded")
        detail = self.client.get("/runs/run_worker_dispatched_001")
        self.assertEqual(
            [event["event_type"] for event in detail.json()["lifecycle_events"]],
            [
                "run.created",
                "run.dispatched",
                "run.claimed",
                "worker.heartbeat",
                "run.started",
                "run.evaluating",
                "evaluator.completed",
                "run.succeeded",
                "log.chunk_recorded",
                "artifact.upload_status_changed",
            ],
        )
        self.assertEqual(detail.json()["lifecycle_events"][2]["from_status"], "dispatched")

    def test_worker_can_disable_legacy_queued_claim_fallback(self):
        create_response = self.client.post(
            "/runs",
            json=_run_create_payload("run_worker_requires_dispatch_001"),
        )
        self.assertEqual(create_response.status_code, 201)

        with tempfile.TemporaryDirectory() as temp_dir:
            worker = RunWorker(
                engine=self.engine,
                worker_id="worker-test",
                executor=FixtureTerminalBenchmarkExecutor(
                    artifact_persistence=ArtifactPersistence(LocalArtifactStore(Path(temp_dir))),
                ),
                allow_legacy_queue_claim=False,
            )

            result = worker.run_once(request_id="req-worker-no-legacy-001")

        self.assertIsNone(result)
        detail = self.client.get("/runs/run_worker_requires_dispatch_001")
        self.assertEqual(detail.json()["run"]["status"], "queued")

    def test_worker_records_failed_evaluator_event(self):
        create_response = self.client.post(
            "/runs",
            json=_run_create_payload("run_worker_failed_evaluator_001"),
        )
        self.assertEqual(create_response.status_code, 201)

        worker = RunWorker(
            engine=self.engine,
            worker_id="worker-test",
            executor=FailedEvaluatorExecutor(),
        )

        result = worker.run_once(request_id="req-worker-failed-evaluator-001")

        self.assertIsNotNone(result)
        self.assertEqual(result.status, "failed")
        detail = self.client.get("/runs/run_worker_failed_evaluator_001")
        payload = detail.json()
        self.assertEqual(
            [event["event_type"] for event in payload["lifecycle_events"]],
            [
                "run.created",
                "run.claimed",
                "worker.heartbeat",
                "run.started",
                "run.evaluating",
                "evaluator.failed",
                "run.failed",
            ],
        )
        evaluator_event = next(
            event for event in payload["lifecycle_events"] if event["event_type"] == "evaluator.failed"
        )
        self.assertEqual(evaluator_event["reason"], "judge service unavailable")
        self.assertEqual(evaluator_event["metadata"]["evaluator_id"], "failed-judge-v0")
        self.assertEqual(evaluator_event["metadata"]["mode"], "harbor_verifier")
        self.assertEqual(evaluator_event["metadata"]["status"], "failed")
        self.assertEqual(evaluator_event["metadata"]["failure_reason"], "judge service unavailable")
        self.assertIsNone(evaluator_event["metadata"]["score"])
        self.assertNotIn("verbal_feedback", evaluator_event["metadata"])
        self.assertNotIn("metrics", evaluator_event["metadata"])

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

    def test_worker_returns_existing_terminal_state_when_recovery_wins_race(self):
        create_response = self.client.post(
            "/runs",
            json=_run_create_payload("run_worker_recovery_race_001"),
        )
        self.assertEqual(create_response.status_code, 201)

        worker = RunWorker(
            engine=self.engine,
            worker_id="worker-race-test",
            executor=FailRunBeforeSaveExecutor(self.engine),
        )

        result = worker.run_once(request_id="req-worker-recovery-race-001")

        self.assertIsNotNone(result)
        self.assertEqual(result.run_id, "run_worker_recovery_race_001")
        self.assertEqual(result.status, "failed")

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
        event_types = [event["event_type"] for event in payload["lifecycle_events"]]
        self.assertIn("sandbox.container_started", event_types)
        self.assertIn("sandbox.container_completed", event_types)
        self.assertIn("sandbox.resource_sampled", event_types)
        started_event = next(
            event for event in payload["lifecycle_events"] if event["event_type"] == "sandbox.container_started"
        )
        completed_event = next(
            event for event in payload["lifecycle_events"] if event["event_type"] == "sandbox.container_completed"
        )
        resource_event = next(
            event for event in payload["lifecycle_events"] if event["event_type"] == "sandbox.resource_sampled"
        )
        self.assertEqual(started_event["metadata"]["worker_id"], "worker-docker-test")
        self.assertEqual(started_event["metadata"]["execution_task_id"], "run_worker_docker_001:attempt:1")
        self.assertEqual(started_event["metadata"]["sandbox_command_index"], 0)
        self.assertEqual(started_event["metadata"]["image"], "python:3.12-slim")
        self.assertEqual(started_event["metadata"]["sandbox_status"], "running")
        self.assertEqual(completed_event["metadata"]["sandbox_status"], "completed")
        self.assertEqual(completed_event["metadata"]["container_id"], "container-run-worker-docker-001")
        self.assertEqual(completed_event["metadata"]["exit_code"], 0)
        self.assertFalse(completed_event["metadata"]["timed_out"])
        self.assertEqual(completed_event["metadata"]["changed_path_count"], 1)
        self.assertEqual(resource_event["metadata"]["sample_status"], "completed")
        self.assertEqual(resource_event["metadata"]["container_id"], "container-run-worker-docker-001")
        self.assertEqual(resource_event["metadata"]["cpu_percent"], 3.5)
        self.assertEqual(resource_event["metadata"]["memory_used_bytes"], 268435456)
        self.assertEqual(resource_event["metadata"]["memory_limit_bytes"], 536870912)
        self.assertEqual(resource_event["metadata"]["pids"], 5)
        rendered_events = json.dumps([started_event, completed_event, resource_event])
        self.assertNotIn("python solve.py", rendered_events)
        self.assertNotIn("created receipts workbook", rendered_events)
        self.assertNotIn(str(temp_path), rendered_events)

    def test_docker_worker_records_stdout_and_stderr_chunks(self):
        payload = _run_create_payload("run_worker_log_chunks_001")
        payload["metadata"] = {
            "worker_commands": [
                {
                    "command": "python solve.py",
                    "cwd": "/workspace",
                    "model_call_id": "call-log-chunks-1",
                }
            ]
        }
        create_response = self.client.post("/runs", json=payload)
        self.assertEqual(create_response.status_code, 201)

        stdout_payload = "created receipts workbook\n" + ("x" * 70_000) + "\nstdout line two\n"
        stderr_payload = "solver warning\n"

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            command_runner = FakeDockerCommandRunner(
                stdout=stdout_payload,
                stderr=stderr_payload,
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

            result = worker.run_once(request_id="req-worker-log-chunks-001")

            with session_scope(self.engine) as session:
                chunks = RunRepository(session).list_artifact_chunks(run_id="run_worker_log_chunks_001")

            stdout_chunks = [chunk for chunk in chunks if chunk.chunk_kind is ArtifactChunkKind.STDOUT]
            stderr_chunks = [chunk for chunk in chunks if chunk.chunk_kind is ArtifactChunkKind.STDERR]
            self.assertEqual([chunk.chunk_sequence for chunk in stdout_chunks], [0])
            self.assertEqual([chunk.chunk_sequence for chunk in stderr_chunks], [0])
            self.assertEqual(stdout_chunks[0].metadata["turn_index"], 0)
            self.assertEqual(stderr_chunks[0].metadata["turn_index"], 0)
            self.assertEqual(stdout_chunks[0].metadata["stream"], "stdout")
            self.assertEqual(stderr_chunks[0].metadata["stream"], "stderr")
            self.assertEqual(stdout_chunks[0].size_bytes, len(stdout_payload.encode("utf-8")))
            self.assertTrue((temp_path / "artifacts" / stdout_chunks[0].storage_key).exists())
            self.assertTrue((temp_path / "artifacts" / stderr_chunks[0].storage_key).exists())
            self.assertEqual(
                (temp_path / "artifacts" / stdout_chunks[0].storage_key).read_text(),
                stdout_payload,
            )
            self.assertEqual(
                (temp_path / "artifacts" / stderr_chunks[0].storage_key).read_text(),
                stderr_payload,
            )
        self.assertIsNotNone(result)
        self.assertEqual(result.status, "succeeded")

    def test_terminal_log_chunk_upload_failure_records_failed_chunk_without_overriding_run_result(self):
        payload = _run_create_payload("run_worker_log_chunk_upload_failure_001")
        payload["metadata"] = {
            "worker_commands": [
                {
                    "command": "python solve.py",
                    "cwd": "/workspace",
                    "model_call_id": "call-log-upload-failure-1",
                }
            ]
        }
        create_response = self.client.post("/runs", json=payload)
        self.assertEqual(create_response.status_code, 201)

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            command_runner = FakeDockerCommandRunner(
                stdout="created receipts workbook\n",
                stderr="",
                write_files={"receipts.xlsx": "spreadsheet bytes\n"},
            )
            worker = RunWorker(
                engine=self.engine,
                worker_id="worker-docker-test",
                executor=DockerTerminalWorkerExecutor(
                    artifact_persistence=ArtifactPersistence(FailingTerminalLogStore(temp_path / "artifacts")),
                    workspace_root=temp_path / "workspaces",
                    command_runner=command_runner,
                ),
            )

            result = worker.run_once(request_id="req-worker-log-upload-failure-001")

            with session_scope(self.engine) as session:
                chunks = RunRepository(session).list_artifact_chunks(
                    run_id="run_worker_log_chunk_upload_failure_001"
                )

        self.assertIsNotNone(result)
        self.assertEqual(result.status, "succeeded")
        self.assertEqual(len(chunks), 1)
        chunk = chunks[0]
        self.assertIs(chunk.chunk_kind, ArtifactChunkKind.STDOUT)
        self.assertIs(chunk.upload_status, ArtifactUploadStatus.FAILED)
        self.assertIsNone(chunk.size_bytes)
        self.assertIsNone(chunk.sha256)
        self.assertIn("simulated log object store failure", chunk.upload_error_reason)
        self.assertEqual(chunk.metadata["turn_index"], 0)
        self.assertEqual(chunk.metadata["stream"], "stdout")

        detail = self.client.get("/runs/run_worker_log_chunk_upload_failure_001")
        self.assertEqual(detail.status_code, 200)
        event_types = [event["event_type"] for event in detail.json()["lifecycle_events"]]
        self.assertIn("log.chunk_recorded", event_types)
        self.assertIn("artifact.upload_status_changed", event_types)
        transition_event = next(
            event for event in detail.json()["lifecycle_events"] if event["event_type"] == "artifact.upload_status_changed"
        )
        self.assertEqual(transition_event["metadata"]["previous_upload_status"], "started")
        self.assertEqual(transition_event["metadata"]["upload_status"], "failed")

    def test_sandbox_lifecycle_recorder_ignores_stale_execution_task(self):
        run = _run_create_payload("run_sandbox_lifecycle_stale_001")
        create_response = self.client.post("/runs", json=run)
        self.assertEqual(create_response.status_code, 201)
        with session_scope(self.engine) as session:
            repository = RunRepository(session)
            repository.claim_next_queued_run(worker_id="worker-stale")
            execution_task_id = repository.current_execution_task_id("run_sandbox_lifecycle_stale_001")
            repository.cancel_run(
                "run_sandbox_lifecycle_stale_001",
                reason="replace stale sandbox attempt",
                actor_user_id="[REDACTED_OWNER]",
            )
            repository.retry_run(
                "run_sandbox_lifecycle_stale_001",
                reason="new attempt replaces stale sandbox event",
                actor_user_id="[REDACTED_OWNER]",
            )

        recorder = RepositorySandboxLifecycleRecorder(
            engine=self.engine,
            run_id="run_sandbox_lifecycle_stale_001",
            worker_id="worker-stale",
            execution_task_id=execution_task_id,
            request_id="req-stale-sandbox-event",
        )

        recorder.container_completed(
            {
                "sandbox_command_index": 0,
                "sandbox_status": "completed",
                "exit_code": 0,
            }
        )

        with session_scope(self.engine) as session:
            events = RunRepository(session).list_status_events("run_sandbox_lifecycle_stale_001")
        self.assertNotIn("sandbox.container_completed", [event.event_type for event in events])

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
                model_provider_base_url="https://api.openai.com/v1",
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
                model_provider_base_url="https://api.openai.com/v1",
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
            [
                "run.created",
                "run.claimed",
                "worker.heartbeat",
                "sandbox.container_started",
                "sandbox.resource_sampled",
                "sandbox.container_completed",
                "run.started",
                "run.failed",
                "log.chunk_recorded",
                "artifact.upload_status_changed",
            ],
        )
        sandbox_events = [
            event for event in payload["lifecycle_events"] if event["event_type"].startswith("sandbox.")
        ]
        rendered_sandbox_events = json.dumps(sandbox_events)
        self.assertNotIn("python missing.py", rendered_sandbox_events)
        self.assertNotIn("missing.py: not found", rendered_sandbox_events)

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
            [
                "run.created",
                "run.claimed",
                "worker.heartbeat",
                "run.started",
                "run.evaluating",
                "evaluator.completed",
                "run.succeeded",
                "log.chunk_recorded",
                "artifact.upload_status_changed",
            ],
        )

    def test_harbor_worker_ingests_current_retry_job_when_previous_job_remains(self):
        payload = _run_create_payload("run_worker_harbor_retry_job_001")
        payload["runner"]["metadata"] = {"runner_contract": "harbor-local-docker-v0"}
        payload["evaluators"] = [{"evaluator_id": "harbor-verifier", "mode": "harbor_verifier"}]
        payload["metadata"] = {
            "harbor_run": {
                "dataset_ref": "terminal-bench/terminal-bench-2",
                "agent": "codex",
                "trial_name": "trial-hello",
                "extra_args": ["--n-tasks", "1"],
            }
        }
        create_response = self.client.post("/runs", json=payload)
        self.assertEqual(create_response.status_code, 201)

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            command_runner = FakeHarborCommandRunner(
                job_names=["job-001", "job-002"],
                write_rewards=[False, True],
            )
            worker = RunWorker(
                engine=self.engine,
                worker_id="worker-harbor-retry-job-test",
                executor=DockerTerminalWorkerExecutor(
                    artifact_persistence=ArtifactPersistence(LocalArtifactStore(temp_path / "artifacts")),
                    workspace_root=temp_path / "workspaces",
                    harbor_command_runner=command_runner,
                ),
            )

            first = worker.run_once(request_id="req-worker-harbor-retry-job-001")
            self.assertIsNotNone(first)
            self.assertEqual(first.status, "failed")
            first_detail = self.client.get("/runs/run_worker_harbor_retry_job_001").json()["run"]
            self.assertIn("Missing Harbor verifier reward", first_detail["failure_reason"] or "")

            retry_response = self.client.post(
                "/runs/run_worker_harbor_retry_job_001/retry",
                json={"reason": "retry after verifier output fix", "actor_user_id": "[REDACTED_OWNER]"},
                headers={"X-Request-ID": "req-retry-harbor-job-001"},
            )
            self.assertEqual(retry_response.status_code, 200)

            second = worker.run_once(request_id="req-worker-harbor-retry-job-002")

        self.assertIsNotNone(second)
        self.assertEqual(second.status, "succeeded")
        detail = self.client.get("/runs/run_worker_harbor_retry_job_001")
        run_payload = detail.json()["run"]
        self.assertEqual(len(command_runner.calls), 2)
        self.assertEqual(run_payload["evaluator"]["score"], 1.0)

    def test_harbor_worker_maps_selected_model_provider_secret_to_agent_env_without_leaking_it(self):
        payload = _run_create_payload("run_worker_harbor_selected_model_001")
        payload["model"]["provider"] = "dev-api-provider"
        payload["model"]["model_name"] = "gpt-5-mini"
        payload["model"]["provider_config_id"] = "default-agent-model"
        payload["runner"]["metadata"] = {"runner_contract": "harbor-local-docker-v0"}
        payload["evaluators"] = [{"evaluator_id": "harbor-verifier", "mode": "harbor_verifier"}]
        payload["metadata"] = {
            "harbor_run": {
                "dataset_ref": "terminal-bench@2.0",
                "agent": "codex",
                "model_name": "gpt-5-mini",
                "environment": "docker",
                "agent_required_secret_refs": ["env:OPENAI_API_KEY"],
                "extra_args": ["--n-tasks", "1", "--quiet"],
            }
        }
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
                model_provider_base_url="https://api.openai.com/v1",
                model_provider_api_key="sk-model-secret",
            )
        )
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            command_runner = FakeHarborCommandRunner()
            worker = RunWorker(
                engine=self.engine,
                worker_id="worker-harbor-selected-model-test",
                executor=DockerTerminalWorkerExecutor(
                    artifact_persistence=ArtifactPersistence(LocalArtifactStore(temp_path / "artifacts")),
                    workspace_root=temp_path / "workspaces",
                    provider_registry=registry,
                    harbor_command_runner=command_runner,
                ),
            )

            result = worker.run_once(request_id="req-worker-harbor-selected-model-001")
            runner_report_text = (
                temp_path
                / "artifacts/runs/run_worker_harbor_selected_model_001/tasks/conference-expense-03/logs/harbor-runner.json"
            ).read_text(encoding="utf-8")

        self.assertIsNotNone(result)
        self.assertEqual(result.status, "succeeded")
        harbor_args = command_runner.calls[0]["args"]
        self.assertEqual(harbor_args[harbor_args.index("--model") + 1], "gpt-5-mini")
        self.assertIn("--agent-env", harbor_args)
        self.assertIn("OPENAI_API_KEY=sk-model-secret", harbor_args)
        self.assertIn("OPENAI_BASE_URL=https://api.openai.com/v1", harbor_args)

        detail = self.client.get("/runs/run_worker_harbor_selected_model_001")
        rendered = json.dumps(detail.json())
        self.assertNotIn("sk-model-secret", rendered)
        self.assertNotIn("sk-model-secret", runner_report_text)
        self.assertIn("OPENAI_API_KEY=[redacted]", runner_report_text)
        self.assertEqual(detail.json()["run"]["status"], "succeeded")

    def test_harbor_worker_infers_mainstream_agent_adapter_env(self):
        payload = _run_create_payload("run_worker_harbor_opencode_deepseek_001")
        payload["model"]["provider"] = "dev-api-provider"
        payload["model"]["model_name"] = "deepseek-v4-flash"
        payload["model"]["provider_config_id"] = "default-agent-model"
        payload["runner"]["metadata"] = {"runner_contract": "harbor-local-docker-v0"}
        payload["evaluators"] = [{"evaluator_id": "harbor-verifier", "mode": "harbor_verifier"}]
        payload["metadata"] = {
            "harbor_run": {
                "dataset_ref": "terminal-bench@2.0",
                "agent": "opencode",
                "model_name": "deepseek-v4-flash",
                "environment": "docker",
                "extra_args": ["--n-tasks", "1", "--quiet"],
            }
        }
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
                model_provider_api_key="deepseek-secret",
            )
        )
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            command_runner = FakeHarborCommandRunner()
            worker = RunWorker(
                engine=self.engine,
                worker_id="worker-harbor-opencode-adapter-test",
                executor=DockerTerminalWorkerExecutor(
                    artifact_persistence=ArtifactPersistence(LocalArtifactStore(temp_path / "artifacts")),
                    workspace_root=temp_path / "workspaces",
                    provider_registry=registry,
                    harbor_command_runner=command_runner,
                ),
            )

            result = worker.run_once(request_id="req-worker-harbor-opencode-adapter-001")
            runner_report_text = (
                temp_path
                / "artifacts/runs/run_worker_harbor_opencode_deepseek_001/tasks/conference-expense-03/logs/harbor-runner.json"
            ).read_text(encoding="utf-8")

        self.assertIsNotNone(result)
        self.assertEqual(result.status, "succeeded")
        harbor_args = command_runner.calls[0]["args"]
        harbor_env = command_runner.calls[0]["env"] or {}
        self.assertEqual(harbor_args[harbor_args.index("--model") + 1], "openai/deepseek-v4-flash")
        self.assertIn("OPENAI_API_KEY=deepseek-secret", harbor_args)
        self.assertIn("OPENAI_BASE_URL=https://models.example/v1", harbor_args)
        self.assertIn("OPENAI_API_BASE=https://models.example/v1", harbor_args)
        self.assertEqual(harbor_env["OPENAI_API_KEY"], "deepseek-secret")
        self.assertEqual(harbor_env["OPENAI_BASE_URL"], "https://models.example/v1")
        self.assertNotIn("deepseek-secret", runner_report_text)
        self.assertIn("OPENAI_API_KEY=[redacted]", runner_report_text)

    def test_harbor_worker_fails_fast_for_unadapted_external_agent(self):
        payload = _run_create_payload("run_worker_harbor_unadapted_agent_001")
        payload["model"]["provider"] = "dev-api-provider"
        payload["model"]["model_name"] = "deepseek-v4-flash"
        payload["model"]["provider_config_id"] = "default-agent-model"
        payload["runner"]["metadata"] = {"runner_contract": "harbor-local-docker-v0"}
        payload["evaluators"] = [{"evaluator_id": "harbor-verifier", "mode": "harbor_verifier"}]
        payload["metadata"] = {
            "harbor_run": {
                "dataset_ref": "terminal-bench@2.0",
                "agent": "cline-cli",
                "model_name": "deepseek-v4-flash",
                "environment": "docker",
                "extra_args": ["--n-tasks", "1", "--quiet"],
            }
        }
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
                model_provider_api_key="deepseek-secret",
            )
        )
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            command_runner = FakeHarborCommandRunner()
            worker = RunWorker(
                engine=self.engine,
                worker_id="worker-harbor-unadapted-agent-test",
                executor=DockerTerminalWorkerExecutor(
                    artifact_persistence=ArtifactPersistence(LocalArtifactStore(temp_path / "artifacts")),
                    workspace_root=temp_path / "workspaces",
                    provider_registry=registry,
                    harbor_command_runner=command_runner,
                ),
            )

            result = worker.run_once(request_id="req-worker-harbor-unadapted-agent-001")

        self.assertIsNotNone(result)
        self.assertEqual(result.status, "failed")
        self.assertEqual(command_runner.calls, [])
        detail = self.client.get("/runs/run_worker_harbor_unadapted_agent_001")
        run_payload = detail.json()["run"]
        self.assertIn("missing model-provider adapter", run_payload["failure_reason"])
        self.assertIn("missing model-provider adapter", run_payload["failure"]["message"])

    def test_harbor_worker_fails_fast_for_provider_dialect_mismatch(self):
        payload = _run_create_payload("run_worker_harbor_codex_dialect_gap_001")
        payload["model"]["provider"] = "dev-api-provider"
        payload["model"]["model_name"] = "deepseek-v4-flash"
        payload["model"]["provider_config_id"] = "default-agent-model"
        payload["runner"]["metadata"] = {"runner_contract": "harbor-local-docker-v0"}
        payload["evaluators"] = [{"evaluator_id": "harbor-verifier", "mode": "harbor_verifier"}]
        payload["metadata"] = {
            "harbor_run": {
                "dataset_ref": "terminal-bench@2.0",
                "agent": "codex",
                "model_name": "deepseek-v4-flash",
                "environment": "docker",
                "extra_args": ["--n-tasks", "1", "--quiet"],
            }
        }
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
                model_provider_api_key="deepseek-secret",
            )
        )
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            command_runner = FakeHarborCommandRunner()
            worker = RunWorker(
                engine=self.engine,
                worker_id="worker-harbor-codex-dialect-gap-test",
                executor=DockerTerminalWorkerExecutor(
                    artifact_persistence=ArtifactPersistence(LocalArtifactStore(temp_path / "artifacts")),
                    workspace_root=temp_path / "workspaces",
                    provider_registry=registry,
                    harbor_command_runner=command_runner,
                ),
            )

            result = worker.run_once(request_id="req-worker-harbor-codex-dialect-gap-001")

        self.assertIsNotNone(result)
        self.assertEqual(result.status, "failed")
        self.assertEqual(command_runner.calls, [])
        detail = self.client.get("/runs/run_worker_harbor_codex_dialect_gap_001")
        run_payload = detail.json()["run"]
        self.assertIn("openai_responses", run_payload["failure_reason"])
        self.assertIn("provider_dialect_mismatch", json.dumps(run_payload["failure"]))

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

    def test_worker_materializes_uploaded_harbor_task_archive(self):
        payload = _run_create_payload("run_worker_harbor_upload_001")
        payload["runner"]["metadata"] = {"runner_contract": "harbor-local-docker-v0"}
        payload["evaluators"] = [{"evaluator_id": "harbor-verifier", "mode": "harbor_verifier"}]

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            artifact_persistence = ArtifactPersistence(LocalArtifactStore(temp_path / "artifacts"))
            stored = artifact_persistence.store.put_bytes(
                "harbor-task-uploads/pilot-project/upload-001/task.zip",
                _uploaded_harbor_task_zip(),
                media_type="application/zip",
                metadata={"content_type": "harbor_task_archive"},
            )
            payload["metadata"] = {
                "harbor_run": {
                    "task_archive_storage_key": stored.key,
                    "agent": "oracle",
                    "model_name": "smoke/noop",
                    "environment": "docker",
                    "extra_args": ["--quiet"],
                }
            }
            create_response = self.client.post("/runs", json=payload)
            self.assertEqual(create_response.status_code, 201)

            command_runner = FakeHarborCommandRunner()
            worker = RunWorker(
                engine=self.engine,
                worker_id="worker-harbor-upload-test",
                executor=DockerTerminalWorkerExecutor(
                    artifact_persistence=artifact_persistence,
                    workspace_root=temp_path / "workspaces",
                    harbor_command_runner=command_runner,
                ),
            )

            result = worker.run_once(request_id="req-worker-harbor-upload-001")
            harbor_args = command_runner.calls[0]["args"]
            task_dir = Path(harbor_args[harbor_args.index("-p") + 1])

            self.assertTrue((task_dir / "instruction.md").is_file())
            self.assertTrue((task_dir / "task.toml").is_file())
            self.assertTrue((task_dir / "environment" / "Dockerfile").is_file())
            self.assertTrue((task_dir / "tests" / "test.sh").is_file())
            self.assertFalse((task_dir / "uploaded-harbor-task").exists())

        self.assertIsNotNone(result)
        self.assertEqual(result.status, "succeeded")
        self.assertEqual(harbor_args[:3], ["harbor", "run", "-p"])
        self.assertIn("--quiet", harbor_args)

        detail = self.client.get("/runs/run_worker_harbor_upload_001")
        payload = detail.json()
        self.assertEqual(payload["run"]["status"], "succeeded")
        self.assertEqual(payload["run"]["evaluator"]["mode"], "harbor_verifier")

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
            ["run.created", "run.claimed", "worker.heartbeat", "run.started", "run.failed"],
        )

    def test_worker_preserves_harbor_diagnostics_when_verifier_ingestion_fails(self):
        payload = _run_create_payload("run_worker_harbor_ingest_failed_001")
        payload["runner"]["metadata"] = {"runner_contract": "harbor-local-docker-v0"}
        payload["evaluators"] = [{"evaluator_id": "harbor-verifier", "mode": "harbor_verifier"}]
        payload["metadata"] = {
            "harbor_run": {
                "dataset_ref": "terminal-bench/terminal-bench-2",
                "agent": "opencode",
                "model_name": "deepseek-v4-flash",
                "trial_name": "trial-hello",
                "extra_args": ["--n-tasks", "1", "--quiet"],
            }
        }
        create_response = self.client.post("/runs", json=payload)
        self.assertEqual(create_response.status_code, 201)

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            artifact_persistence = ArtifactPersistence(LocalArtifactStore(temp_path / "artifacts"))
            self.client.app.state.artifact_store = artifact_persistence.store
            worker = RunWorker(
                engine=self.engine,
                worker_id="worker-harbor-ingestion-test",
                executor=DockerTerminalWorkerExecutor(
                    artifact_persistence=artifact_persistence,
                    workspace_root=temp_path / "workspaces",
                    harbor_command_runner=FakeHarborCommandRunner(write_reward=False),
                ),
            )

            result = worker.run_once(request_id="req-worker-harbor-ingest-fail-001")
            detail = self.client.get("/runs/run_worker_harbor_ingest_failed_001").json()
            bundle = self.client.get("/runs/run_worker_harbor_ingest_failed_001/artifact-bundle")

        self.assertIsNotNone(result)
        self.assertEqual(result.status, "failed")
        run = detail["run"]
        self.assertIn("Missing Harbor verifier reward", run["failure_reason"])
        self.assertEqual(run["failure"]["category"], "harbor_verifier_missing_reward")
        self.assertEqual(run["failure"]["source"], "harbor_ingestion")
        content_types = {artifact.get("content_type") for artifact in run["artifacts"]}
        self.assertIn("harbor_runner_report", content_types)
        self.assertIn("harbor_jobs_archive", content_types)
        self.assertIn("harbor_ingestion_diagnostics", content_types)
        self.assertGreaterEqual(run["progress"]["artifact_count"], 3)
        self.assertEqual(bundle.status_code, 200)
        with zipfile.ZipFile(BytesIO(bundle.content)) as archive:
            names = set(archive.namelist())
            rendered_bundle = "\n".join(
                archive.read(name).decode("utf-8", errors="replace")
                for name in sorted(names)
                if name.endswith((".json", ".jsonl"))
            )
        self.assertIn("artifacts/log/run_worker_harbor_ingest_failed_001-harbor-runner-report.json", names)
        self.assertIn("artifacts/log/run_worker_harbor_ingest_failed_001-job-001-harbor-jobs.tar.gz", names)
        self.assertIn("artifacts/log/run_worker_harbor_ingest_failed_001-harbor-ingestion-diagnostics.json", names)
        self.assertIn("Missing Harbor verifier reward", rendered_bundle)
        self.assertIn('"api_key": "[redacted]"', rendered_bundle)
        self.assertNotIn("deepseek-secret", rendered_bundle)

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
            [
                "run.created",
                "run.claimed",
                "worker.heartbeat",
                "run.started",
                "run.evaluating",
                "evaluator.completed",
                "run.succeeded",
                "log.chunk_recorded",
                "artifact.upload_status_changed",
            ],
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
            [
                "run.created",
                "run.claimed",
                "worker.heartbeat",
                "run.started",
                "run.failed",
                "log.chunk_recorded",
                "artifact.upload_status_changed",
            ],
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
        stats_stdout: str = (
            '{"CPUPerc":"3.50%","MemUsage":"256MiB / 512MiB",'
            '"MemPerc":"50.00%","NetIO":"0B / 0B","BlockIO":"0B / 0B","PIDs":"5"}\n'
        ),
    ) -> None:
        self.returncode = returncode
        self.stdout = stdout
        self.stderr = stderr
        self.write_files = write_files or {}
        self.stats_stdout = stats_stdout
        self.calls: list[dict[str, object]] = []

    def start(
        self,
        args: list[str],
        *,
        env: dict[str, str] | None = None,
    ):
        self.calls.append({"args": args, "env": env, "method": "start"})
        workspace = _workspace_from_docker_args(args)
        if "--cidfile" in args:
            cidfile = Path(args[args.index("--cidfile") + 1])
            cidfile.parent.mkdir(parents=True, exist_ok=True)
            cidfile.write_text("container-run-worker-docker-001\n")
        for relative_path, content in self.write_files.items():
            target = workspace / relative_path
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(content)
        return FakeDockerProcess(returncode=self.returncode, stdout=self.stdout, stderr=self.stderr)

    def run(
        self,
        args: list[str],
        *,
        timeout: int,
        env: dict[str, str] | None = None,
    ) -> subprocess.CompletedProcess[str]:
        self.calls.append({"args": args, "timeout": timeout, "env": env})
        if args[:3] == ["docker", "stats", "--no-stream"]:
            return subprocess.CompletedProcess(
                args=args,
                returncode=0,
                stdout=self.stats_stdout,
                stderr="",
            )
        workspace = _workspace_from_docker_args(args)
        if "--cidfile" in args:
            cidfile = Path(args[args.index("--cidfile") + 1])
            cidfile.parent.mkdir(parents=True, exist_ok=True)
            cidfile.write_text("container-run-worker-docker-001\n")
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


class FakeDockerProcess:
    def __init__(self, *, returncode: int, stdout: str, stderr: str) -> None:
        self._returncode = returncode
        self.returncode: int | None = None
        self.stdout = stdout
        self.stderr = stderr
        self.killed = False

    def poll(self) -> int | None:
        return self.returncode

    def communicate(self, timeout: float | None = None) -> tuple[str, str]:
        self.returncode = self._returncode
        return self.stdout, self.stderr

    def kill(self) -> None:
        self.killed = True
        self.returncode = -9


class FailingTerminalLogStore(LocalArtifactStore):
    def put_bytes(self, key, payload, *, media_type, metadata=None):
        if "/logs/stdout/" in key or "/logs/stderr/" in key:
            raise RuntimeError("simulated log object store failure")
        return super().put_bytes(key, payload, media_type=media_type, metadata=metadata)


def _workspace_from_docker_args(args: list[str]) -> Path:
    volume_index = args.index("-v")
    return Path(args[volume_index + 1].split(":", maxsplit=1)[0])


class FakeHarborCommandRunner:
    def __init__(
        self,
        *,
        returncode: int = 0,
        stdout: str = "harbor complete\n",
        stderr: str = "",
        write_reward: bool = True,
        write_rewards: list[bool] | None = None,
        job_names: list[str] | None = None,
    ) -> None:
        self.returncode = returncode
        self.stdout = stdout
        self.stderr = stderr
        self.write_reward = write_reward
        self.write_rewards = write_rewards or []
        self.job_names = job_names or []
        self.calls: list[dict[str, object]] = []

    def run(
        self,
        args: list[str],
        *,
        timeout: int,
        env: dict[str, str] | None = None,
    ) -> subprocess.CompletedProcess[str]:
        self.calls.append({"args": args, "timeout": timeout, "env": env})
        jobs_dir = Path(args[args.index("--jobs-dir") + 1])
        call_index = len(self.calls) - 1
        write_reward = self.write_rewards[call_index] if call_index < len(self.write_rewards) else self.write_reward
        job_name = self.job_names[call_index] if call_index < len(self.job_names) else "job-001"
        _write_harbor_job_fixture(jobs_dir, write_reward=write_reward, job_name=job_name)
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


class FailRunBeforeSaveExecutor:
    def __init__(self, engine):
        self.engine = engine

    def execute(self, run):
        with session_scope(self.engine) as session:
            RunRepository(session).transition_run(
                run.run_id,
                RunStatus.FAILED,
                event_type="run.recovered",
                reason="scheduler recovery won the save race",
            )
        return run


class FailedEvaluatorExecutor:
    def execute(self, run):
        if run.status is not RunStatus.PROVISIONING:
            run.transition_to(RunStatus.PROVISIONING)
        run.transition_to(RunStatus.RUNNING)
        run.transition_to(RunStatus.EVALUATING)
        run.attach_evaluator_result(
            EvaluatorResult(
                evaluator_id="failed-judge-v0",
                mode="harbor_verifier",
                status="failed",
                score=None,
                metrics={"task_success": False},
                verbal_feedback="Judge service failed before feedback.",
                judge=None,
                artifact_refs=[],
                failure_reason="judge service unavailable",
            )
        )
        run.failure_reason = "judge service unavailable"
        run.transition_to(RunStatus.FAILED)
        return run


def _arg_value(args: list[str], name: str) -> str:
    return args[args.index(name) + 1]


def _write_harbor_job_fixture(root: Path, *, write_reward: bool = True, job_name: str = "job-001") -> None:
    job_dir = root / job_name
    trial_dir = job_dir / "trial-hello"
    (trial_dir / "agent").mkdir(parents=True)
    (trial_dir / "verifier").mkdir()
    (trial_dir / "artifacts").mkdir()
    (job_dir / "config.json").write_text(
        json.dumps(
            {
                "dataset": "terminal-bench/terminal-bench-2",
                "agent": "default",
                "api_key": "deepseek-secret",
            }
        ),
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
    if write_reward:
        (trial_dir / "verifier" / "reward.txt").write_text("1.0\n", encoding="utf-8")
    (trial_dir / "artifacts" / "answer.txt").write_text("42\n", encoding="utf-8")
    (trial_dir / "artifacts" / "manifest.json").write_text(
        json.dumps([{"destination": "artifacts/answer.txt", "type": "file", "status": "ok"}]),
        encoding="utf-8",
    )


def _uploaded_harbor_task_zip() -> bytes:
    buffer = BytesIO()
    with zipfile.ZipFile(buffer, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("uploaded-harbor-task/instruction.md", "Write the answer file.\n")
        archive.writestr(
            "uploaded-harbor-task/task.toml",
            "\n".join(
                [
                    'schema_version = "1.2"',
                    'artifacts = ["/logs/artifacts/answer.txt"]',
                    "",
                    "[task]",
                    'name = "latent/uploaded-harbor-task"',
                    "",
                    "[verifier]",
                    "timeout_sec = 120.0",
                    "",
                    "[agent]",
                    "timeout_sec = 120.0",
                    "",
                    "[environment]",
                    'os = "linux"',
                    "allow_internet = true",
                    "",
                ]
            ),
        )
        archive.writestr("uploaded-harbor-task/environment/Dockerfile", "FROM ubuntu:24.04\n")
        archive.writestr(
            "uploaded-harbor-task/tests/test.sh",
            "mkdir -p /logs/verifier\nprintf '1\\n' > /logs/verifier/reward.txt\n",
        )
    return buffer.getvalue()
