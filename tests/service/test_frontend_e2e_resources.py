import io
import json
import unittest
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from tempfile import TemporaryDirectory

from fastapi.testclient import TestClient
from sqlalchemy import select
from sqlalchemy.pool import StaticPool

from agentic_data_platform.artifacts.store import LocalArtifactStore
from agentic_data_platform.domain.artifact_metadata import (
    ArtifactChunkKind,
    ArtifactChunkMetadata,
    ArtifactUploadStatus,
)
from agentic_data_platform.domain.execution_events import RunEventType
from agentic_data_platform.domain.run_records import (
    ArtifactKind,
    ArtifactRef,
    BenchmarkTaskInstance,
    EvaluatorResult,
    JudgeConfig,
    ModelConfig,
    ModelMode,
    RunnerConfig,
    RunnerKind,
    RunRecord,
    RunStatus,
    SandboxBackend,
    TerminalTurn,
)
from agentic_data_platform.persistence import (
    IdentityRepository,
    ProjectRepository,
    RunRepository,
    create_database_engine,
    session_scope,
)
from agentic_data_platform.persistence.migrations import upgrade_database
from agentic_data_platform.persistence.models import ArtifactRow
from agentic_data_platform.service.app import create_app
from agentic_data_platform.service.config import ServiceSettings


class FrontendE2EResourcesTest(unittest.TestCase):
    def setUp(self):
        self.engine = create_database_engine(
            "sqlite+pysqlite:///:memory:",
            connect_args={"check_same_thread": False},
            poolclass=StaticPool,
        )
        upgrade_database(self.engine)
        with session_scope(self.engine) as session:
            identities = IdentityRepository(session)
            identities.create_team(team_id="pilot-project", name="pilot group")
            identities.create_user(
                user_id="[REDACTED_OWNER]",
                email="[REDACTED_OWNER]@example.com",
                display_name="[REDACTED_OWNER]",
                team_id="pilot-project",
            )
            ProjectRepository(session).create_project(
                project_id="latent-skill-pilot",
                name="Latent Skill Pilot",
                owner_team_id="pilot-project",
                created_by_user_id="[REDACTED_OWNER]",
            )
            repository = RunRepository(session)
            repository.save_run(_completed_run("run_frontend_001"))
            repository._append_status_event(  # Test fixture for persisted lifecycle bundle content.
                run_id="run_frontend_001",
                attempt_id="run_frontend_001:attempt:1",
                event_type=RunEventType.CREATED,
                from_status=None,
                to_status=RunStatus.QUEUED,
                request_id="req-created-001",
            )
            repository._append_status_event(
                run_id="run_frontend_001",
                attempt_id="run_frontend_001:attempt:1",
                event_type=RunEventType.SUCCEEDED,
                from_status=RunStatus.EVALUATING,
                to_status=RunStatus.SUCCEEDED,
                request_id="req-succeeded-001",
                metadata={"worker_id": "worker-dev-1", "execution_task_id": "run_frontend_001:attempt:1"},
            )
        self.client = TestClient(_app(self.engine))

    def tearDown(self):
        self.engine.dispose()

    def test_web_login_sets_http_only_cookie_and_session_uses_cookie_without_bearer(self):
        login = self.client.post(
            "/auth/login",
            json={"username": "[REDACTED_OWNER]", "password": "[REDACTED_PASSWORD]"},
            headers={"X-Request-ID": "req-login-001"},
        )

        self.assertEqual(login.status_code, 200)
        self.assertIn("HttpOnly", login.headers["set-cookie"])
        self.assertIn("SameSite=lax", login.headers["set-cookie"])
        self.assertNotIn("[REDACTED_TOKEN]", login.text)
        self.assertEqual(login.json()["user"]["user_id"], "[REDACTED_OWNER]")
        self.assertEqual(login.json()["request_id"], "req-login-001")

        session = self.client.get("/auth/session", headers={"X-Request-ID": "req-session-001"})

        self.assertEqual(session.status_code, 200)
        self.assertEqual(
            session.json()["user"],
            {
                "user_id": "[REDACTED_OWNER]",
                "email": "[REDACTED_OWNER]@example.com",
                "display_name": "[REDACTED_OWNER]",
                "team_id": "pilot-project",
            },
        )
        self.assertEqual(session.json()["request_id"], "req-session-001")

    def test_invalid_web_login_does_not_create_session_cookie(self):
        response = self.client.post(
            "/auth/login",
            json={"username": "[REDACTED_OWNER]", "password": "wrong"},
            headers={"X-Request-ID": "req-login-bad-001"},
        )

        self.assertEqual(response.status_code, 401)
        self.assertNotIn("set-cookie", response.headers)
        self.assertEqual(response.json()["error"]["code"], "unauthorized")

    def test_model_catalog_uses_configured_static_allowlist_without_exposing_secrets(self):
        response = self.client.get(
            "/models",
            headers={"Authorization": "Bearer [REDACTED_TOKEN]", "X-Request-ID": "req-models-001"},
        )

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["request_id"], "req-models-001")
        self.assertEqual([model["model_id"] for model in payload["models"]], ["gpt-5", "gpt-5-mini"])
        self.assertEqual(payload["models"][0]["provider_config_id"], "default-agent-model")
        self.assertEqual(payload["models"][0]["source"], "static_config_fallback")
        self.assertEqual(payload["catalog"]["status"], "fallback_static_config")
        self.assertFalse(payload["models"][0]["disabled"])
        rendered = json.dumps(payload, sort_keys=True)
        self.assertNotIn("sk-model-secret", rendered)
        self.assertNotIn("secret_ref", rendered)

    def test_model_catalog_omits_provider_config_id_when_dev_fallback_has_no_secret_config(self):
        client = TestClient(
            create_app(
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
                    model_provider_models="scripted-terminal-agent",
                    internal_auth_tokens="[REDACTED_OWNER]=[REDACTED_TOKEN]",
                    web_login_credentials="[REDACTED_OWNER]=[REDACTED_PASSWORD]:[REDACTED_OWNER]",
                    web_session_secret="test-session-secret",
                ),
                database_engine=self.engine,
            )
        )

        response = client.get("/models", headers={"Authorization": "Bearer [REDACTED_TOKEN]"})

        self.assertEqual(response.status_code, 200)
        model = response.json()["models"][0]
        self.assertEqual(model["model_id"], "scripted-terminal-agent")
        self.assertNotIn("provider_config_id", model)

    def test_harness_catalog_exposes_docker_and_harbor_local_options(self):
        response = self.client.get(
            "/harnesses",
            headers={"Authorization": "Bearer [REDACTED_TOKEN]", "X-Request-ID": "req-harnesses-001"},
        )

        self.assertEqual(response.status_code, 200)
        harnesses = response.json()["harnesses"]
        self.assertEqual([harness["harness_id"] for harness in harnesses], ["docker-terminal", "harbor-local-docker"])
        self.assertEqual(harnesses[0]["sandbox_backend"], "docker_terminal")
        self.assertTrue(harnesses[1]["metadata"]["harbor_compatible"])
        self.assertEqual(harnesses[1]["metadata"]["status"], "ready")
        self.assertEqual(harnesses[1]["metadata"]["harbor_task_template"], "harbor-cli-smoke")
        self.assertEqual(response.json()["request_id"], "req-harnesses-001")

    def test_run_telemetry_is_scoped_and_reports_runtime_health_without_secret_leaks(self):
        response = self.client.get(
            "/runs/run_frontend_001/telemetry",
            headers={"Authorization": "Bearer [REDACTED_TOKEN]", "X-Request-ID": "req-telemetry-001"},
        )

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["run"]["run_id"], "run_frontend_001")
        self.assertEqual(payload["run"]["status"], "succeeded")
        self.assertEqual(payload["sandbox"]["status"], "exited")
        self.assertIn("cpu", payload["host"])
        self.assertIn("memory", payload["host"])
        self.assertIn("disk", payload["host"])
        self.assertEqual(payload["request_id"], "req-telemetry-001")
        rendered = json.dumps(payload, sort_keys=True)
        self.assertNotIn("file://", rendered)
        self.assertNotIn("[REDACTED_TOKEN]", rendered)

    def test_artifact_bundle_download_contains_sanitized_manifest_and_run_outputs(self):
        response = self.client.get(
            "/runs/run_frontend_001/artifact-bundle",
            headers={"Authorization": "Bearer [REDACTED_TOKEN]", "X-Request-ID": "req-bundle-001"},
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.headers["content-type"], "application/zip")
        self.assertIn("run_frontend_001-artifacts.zip", response.headers["content-disposition"])

        with zipfile.ZipFile(io.BytesIO(response.content)) as archive:
            names = set(archive.namelist())
            self.assertEqual(
                names,
                {
                    "manifest.json",
                    "run.json",
                    "trajectory.jsonl",
                    "evaluation.json",
                    "artifact-metadata.json",
                    "artifact-chunks.json",
                    "lifecycle-events.json",
                },
            )
            manifest = json.loads(archive.read("manifest.json"))
            lifecycle_events = json.loads(archive.read("lifecycle-events.json"))["lifecycle_events"]
            trajectory = archive.read("trajectory.jsonl").decode("utf-8")
            rendered_bundle = "\n".join(
                archive.read(name).decode("utf-8") for name in sorted(names) if name.endswith((".json", ".jsonl"))
            )

        self.assertEqual(manifest["run_id"], "run_frontend_001")
        self.assertEqual(manifest["artifact_count"], 3)
        self.assertEqual([event["seq"] for event in lifecycle_events], [1, 2])
        self.assertIn("python solve.py", trajectory)
        self.assertNotIn("file://", rendered_bundle)
        self.assertNotIn("/srv/private", rendered_bundle)
        self.assertNotIn("X-Amz-Signature", rendered_bundle)

    def test_artifact_bundle_download_includes_object_store_payloads_when_available(self):
        with TemporaryDirectory() as temp_dir:
            store = LocalArtifactStore(Path(temp_dir))
            store.put_bytes(
                "runs/run_frontend_001/tasks/task/trajectory/trajectory.jsonl",
                b'{"turn_index": 0, "stdout": "stored trajectory"}\n',
                media_type="application/x-ndjson",
            )
            store.put_bytes(
                "runs/run_frontend_001/tasks/task/workspace/snapshot.json",
                b'{"files": [{"path": "frontend-output.txt"}]}\n',
                media_type="application/json",
            )
            store.put_bytes(
                "runs/run_frontend_001/tasks/task/evaluation/report.json",
                b'{"verbal_feedback": "stored evaluator report"}\n',
                media_type="application/json",
            )
            self.client.app.state.artifact_store = store

            response = self.client.get(
                "/runs/run_frontend_001/artifact-bundle",
                headers={"Authorization": "Bearer [REDACTED_TOKEN]", "X-Request-ID": "req-bundle-payloads-001"},
            )

        self.assertEqual(response.status_code, 200)
        with zipfile.ZipFile(io.BytesIO(response.content)) as archive:
            names = set(archive.namelist())
            manifest = json.loads(archive.read("manifest.json"))
            rendered_bundle = "\n".join(
                archive.read(name).decode("utf-8") for name in sorted(names) if name.endswith((".json", ".jsonl"))
            )

        self.assertIn("artifacts/trajectory/run_frontend_001-trajectory.jsonl", names)
        self.assertIn("artifacts/workspace_snapshot/run_frontend_001-workspace.json", names)
        self.assertIn("artifacts/evaluator_report/run_frontend_001-evaluator.json", names)
        self.assertEqual(len(manifest["artifact_contents"]), 3)
        self.assertEqual(manifest["artifact_content_errors"], [])
        self.assertIn("stored trajectory", rendered_bundle)
        self.assertIn("frontend-output.txt", rendered_bundle)
        self.assertIn("stored evaluator report", rendered_bundle)
        self.assertNotIn(str(temp_dir), rendered_bundle)

    def test_artifact_bundle_download_records_missing_object_payloads(self):
        with TemporaryDirectory() as temp_dir:
            store = LocalArtifactStore(Path(temp_dir))
            store.put_bytes(
                "runs/run_frontend_001/tasks/task/trajectory/trajectory.jsonl",
                b'{"turn_index": 0, "stdout": "stored trajectory"}\n',
                media_type="application/x-ndjson",
            )
            self.client.app.state.artifact_store = store

            response = self.client.get(
                "/runs/run_frontend_001/artifact-bundle",
                headers={"Authorization": "Bearer [REDACTED_TOKEN]", "X-Request-ID": "req-bundle-missing-001"},
            )

        self.assertEqual(response.status_code, 200)
        with zipfile.ZipFile(io.BytesIO(response.content)) as archive:
            names = set(archive.namelist())
            manifest = json.loads(archive.read("manifest.json"))

        self.assertIn("artifacts/trajectory/run_frontend_001-trajectory.jsonl", names)
        self.assertEqual(len(manifest["artifact_contents"]), 1)
        self.assertEqual(len(manifest["artifact_content_errors"]), 2)
        self.assertEqual(
            {item["artifact_id"] for item in manifest["artifact_content_errors"]},
            {"run_frontend_001-workspace", "run_frontend_001-evaluator"},
        )
        self.assertTrue(
            all("not available" in item["message"] for item in manifest["artifact_content_errors"])
        )

    def test_artifact_bundle_reports_failed_upload_state_without_fetching_payload(self):
        with session_scope(self.engine) as session:
            artifact = session.scalar(
                select(ArtifactRow).where(ArtifactRow.artifact_id == "run_frontend_001-workspace")
            )
            metadata = dict(artifact.metadata_json or {})
            metadata["upload_status"] = ArtifactUploadStatus.FAILED.value
            metadata["upload_error_reason"] = "object store write failed"
            artifact.metadata_json = metadata

        with TemporaryDirectory() as temp_dir:
            store = LocalArtifactStore(Path(temp_dir))
            store.put_bytes(
                "runs/run_frontend_001/tasks/task/trajectory/trajectory.jsonl",
                b'{"turn_index": 0, "stdout": "stored trajectory"}\n',
                media_type="application/x-ndjson",
            )
            store.put_bytes(
                "runs/run_frontend_001/tasks/task/workspace/snapshot.json",
                b'{"files": [{"path": "should-not-be-downloaded.txt"}]}\n',
                media_type="application/json",
            )
            store.put_bytes(
                "runs/run_frontend_001/tasks/task/evaluation/report.json",
                b'{"verbal_feedback": "stored evaluator report"}\n',
                media_type="application/json",
            )
            self.client.app.state.artifact_store = store

            response = self.client.get(
                "/runs/run_frontend_001/artifact-bundle",
                headers={"Authorization": "Bearer [REDACTED_TOKEN]", "X-Request-ID": "req-bundle-upload-state-001"},
            )

        self.assertEqual(response.status_code, 200)
        with zipfile.ZipFile(io.BytesIO(response.content)) as archive:
            names = set(archive.namelist())
            manifest = json.loads(archive.read("manifest.json"))
            rendered_bundle = "\n".join(
                archive.read(name).decode("utf-8") for name in sorted(names) if name.endswith((".json", ".jsonl"))
            )

        self.assertNotIn("artifacts/workspace_snapshot/run_frontend_001-workspace.json", names)
        self.assertEqual(len(manifest["artifact_contents"]), 2)
        upload_errors = [
            item for item in manifest["artifact_content_errors"] if item["artifact_id"] == "run_frontend_001-workspace"
        ]
        self.assertEqual(len(upload_errors), 1)
        self.assertEqual(upload_errors[0]["upload_status"], ArtifactUploadStatus.FAILED.value)
        self.assertEqual(upload_errors[0]["upload_error_reason"], "object store write failed")
        self.assertIn("upload is failed", upload_errors[0]["message"])
        self.assertNotIn("should-not-be-downloaded.txt", rendered_bundle)

    def test_artifact_bundle_includes_completed_chunk_payloads_and_failed_chunk_state(self):
        stdout_key = (
            "runs/run_frontend_001/tasks/task/attempts/"
            "run_frontend_001-attempt-1/logs/stdout/000000.txt"
        )
        stderr_key = (
            "runs/run_frontend_001/tasks/task/attempts/"
            "run_frontend_001-attempt-1/logs/stderr/000000.txt"
        )
        with session_scope(self.engine) as session:
            repository = RunRepository(session)
            repository.record_artifact_chunk(
                ArtifactChunkMetadata(
                    run_id="run_frontend_001",
                    attempt_id="run_frontend_001:attempt:1",
                    artifact_id="run_frontend_001-trajectory",
                    chunk_kind=ArtifactChunkKind.STDOUT,
                    chunk_sequence=0,
                    storage_key=stdout_key,
                    media_type="text/plain; charset=utf-8",
                    size_bytes=len(b"full stdout chunk\n"),
                    sha256="4" * 64,
                    upload_status=ArtifactUploadStatus.COMPLETED,
                    created_at=datetime(2026, 5, 29, 12, 0, 4, tzinfo=timezone.utc),
                    metadata={"turn_index": 0, "stream": "stdout", "command": "python solve.py"},
                )
            )
            repository.record_artifact_chunk(
                ArtifactChunkMetadata(
                    run_id="run_frontend_001",
                    attempt_id="run_frontend_001:attempt:1",
                    artifact_id="run_frontend_001-trajectory",
                    chunk_kind=ArtifactChunkKind.STDERR,
                    chunk_sequence=0,
                    storage_key=stderr_key,
                    media_type="text/plain; charset=utf-8",
                    size_bytes=None,
                    sha256=None,
                    upload_status=ArtifactUploadStatus.FAILED,
                    upload_error_reason="object store write failed",
                    created_at=datetime(2026, 5, 29, 12, 0, 5, tzinfo=timezone.utc),
                    metadata={"turn_index": 0, "stream": "stderr", "command": "python solve.py"},
                )
            )

        with TemporaryDirectory() as temp_dir:
            store = LocalArtifactStore(Path(temp_dir))
            store.put_bytes(stdout_key, b"full stdout chunk\n", media_type="text/plain; charset=utf-8")
            store.put_bytes(stderr_key, b"should not be downloaded\n", media_type="text/plain; charset=utf-8")
            self.client.app.state.artifact_store = store

            response = self.client.get(
                "/runs/run_frontend_001/artifact-bundle",
                headers={"Authorization": "Bearer [REDACTED_TOKEN]", "X-Request-ID": "req-bundle-chunks-001"},
            )

        self.assertEqual(response.status_code, 200)
        with zipfile.ZipFile(io.BytesIO(response.content)) as archive:
            names = set(archive.namelist())
            manifest = json.loads(archive.read("manifest.json"))
            chunks_payload = json.loads(archive.read("artifact-chunks.json"))
            stdout_payload = archive.read(
                "artifact-chunks/stdout/run_frontend_001-trajectory-stdout-000000.txt"
            ).decode("utf-8")
            rendered_bundle = "\n".join(
                archive.read(name).decode("utf-8") for name in sorted(names) if name.endswith((".json", ".jsonl"))
            )

        self.assertEqual(manifest["artifact_chunk_count"], 2)
        self.assertEqual(manifest["artifact_chunk_payload_count"], 1)
        self.assertEqual(len(manifest["artifact_chunk_content_errors"]), 1)
        self.assertEqual(manifest["artifact_chunk_content_errors"][0]["upload_status"], "failed")
        self.assertEqual(
            manifest["artifact_chunk_content_errors"][0]["upload_error_reason"],
            "object store write failed",
        )
        self.assertEqual(len(chunks_payload["chunks"]), 2)
        self.assertIn("artifact-chunks/stdout/run_frontend_001-trajectory-stdout-000000.txt", names)
        self.assertNotIn("artifact-chunks/stderr/run_frontend_001-trajectory-stderr-000000.txt", names)
        self.assertEqual(stdout_payload, "full stdout chunk\n")
        self.assertNotIn("should not be downloaded", rendered_bundle)
        self.assertNotIn(str(temp_dir), rendered_bundle)

    def test_frontend_static_app_is_served_without_bearer_token(self):
        response = self.client.get("/app/", headers={"X-Request-ID": "req-app-001"})

        self.assertEqual(response.status_code, 200)
        self.assertIn("text/html", response.headers["content-type"])
        self.assertIn("Agentic Data Platform", response.text)
        self.assertIn("app.js", response.text)


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
            model_provider_base_url="https://models.example/v1",
            model_provider_api_key="sk-model-secret",
            model_provider_models="gpt-5,gpt-5-mini",
            evaluator_provider_base_url="https://judge.example/v1",
            evaluator_provider_api_key="sk-judge-secret",
            internal_auth_tokens="[REDACTED_OWNER]=[REDACTED_TOKEN]",
            web_login_credentials="[REDACTED_OWNER]=[REDACTED_PASSWORD]:[REDACTED_OWNER]",
            web_session_secret="test-session-secret",
        ),
        database_engine=engine,
    )


def _completed_run(run_id: str) -> RunRecord:
    run = RunRecord.create(
        run_id=run_id,
        project_id="latent-skill-pilot",
        owner_team="pilot group",
        task=BenchmarkTaskInstance(
            benchmark_suite="SkillLearnBench",
            benchmark_version="git:cxcscmu/SkillLearnBench@638284f5982f6be085a955435d2ec7a5258f5513",
            task_family="organize-messy-files",
            instance_id="organize-messy-files-1",
            source_uri="https://github.com/cxcscmu/SkillLearnBench",
            input_artifact_refs=[],
            required_artifacts=["trajectory", "workspace_snapshot", "evaluator_report"],
            metadata={"instruction": "Organize files in a terminal workspace."},
        ),
        model=ModelConfig(
            provider="dev-api-provider",
            model_name="gpt-5",
            mode=ModelMode.API,
            prompt_template_version="terminal-agent-v0",
            metadata={"provider_config_id": "default-agent-model"},
        ),
        runner=RunnerConfig(
            kind=RunnerKind.ORIGINAL_BENCHMARK,
            sandbox_backend=SandboxBackend.DOCKER_TERMINAL,
            image="python:3.12-slim",
            entrypoint=["python", "-m", "agentic_data_platform.benchmark_wrappers.skilllearnbench"],
            internet_access=True,
            resource_limits={"cpu": 1, "memory_mb": 512, "timeout_seconds": 60},
            metadata={"runner_contract": "harbor-local-docker-v0"},
        ),
        evaluator_configs=[],
        created_by_user_id="[REDACTED_OWNER]",
    )
    run.transition_to(RunStatus.PROVISIONING)
    run.transition_to(RunStatus.RUNNING)
    run.add_turn(
        TerminalTurn(
            turn_index=0,
            command="python solve.py",
            cwd="/workspace",
            started_at=datetime(2026, 5, 29, 12, 0, 0, tzinfo=timezone.utc),
            completed_at=datetime(2026, 5, 29, 12, 0, 3, tzinfo=timezone.utc),
            exit_code=0,
            stdout="created receipts.xlsx\n",
            stderr="",
            changed_paths=["receipts.xlsx"],
            model_call_id="call-1",
        )
    )
    run.attach_artifact(
        ArtifactRef(
            artifact_id=f"{run_id}-trajectory",
            kind=ArtifactKind.TRAJECTORY,
            uri="file:///srv/private/trajectory.jsonl",
            media_type="application/x-ndjson",
            sha256="1" * 64,
            size_bytes=128,
            metadata={"storage_key": "runs/run_frontend_001/tasks/task/trajectory/trajectory.jsonl"},
        )
    )
    run.attach_artifact(
        ArtifactRef(
            artifact_id=f"{run_id}-workspace",
            kind=ArtifactKind.WORKSPACE_SNAPSHOT,
            uri="s3://agentic-data-shared dev/runs/run_frontend_001/workspace.json?X-Amz-Signature=secret",
            media_type="application/json",
            sha256="2" * 64,
            size_bytes=256,
            metadata={"storage_key": "runs/run_frontend_001/tasks/task/workspace/snapshot.json"},
        )
    )
    run.transition_to(RunStatus.EVALUATING)
    result = EvaluatorResult(
        evaluator_id="mock-judge-v0",
        status="completed",
        score=0.8,
        metrics={"pass": True},
        verbal_feedback="The workspace output matches the benchmark requirements.",
        judge=JudgeConfig(provider="mock", model_name="deterministic-judge", rubric_version="frontend-e2e-v0"),
        artifact_refs=[f"{run_id}-trajectory", f"{run_id}-workspace"],
    )
    run.attach_evaluator_result(result)
    run.attach_artifact(
        ArtifactRef(
            artifact_id=f"{run_id}-evaluator",
            kind=ArtifactKind.EVALUATOR_REPORT,
            uri="s3://agentic-data-shared dev/runs/run_frontend_001/evaluation/report.json",
            media_type="application/json",
            sha256="3" * 64,
            size_bytes=512,
            metadata={"storage_key": "runs/run_frontend_001/tasks/task/evaluation/report.json"},
        )
    )
    run.transition_to(RunStatus.SUCCEEDED)
    return run
