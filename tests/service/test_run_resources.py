import json
import unittest
from datetime import datetime, timezone

from fastapi.testclient import TestClient
from sqlalchemy.pool import StaticPool

from agentic_data_platform.dashboard.projections import RunDashboardProjection
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
from agentic_data_platform.persistence.database import create_database_engine, session_scope
from agentic_data_platform.persistence.migrations import upgrade_database
from agentic_data_platform.persistence.repositories import IdentityRepository, ProjectRepository, RunRepository
from agentic_data_platform.service.app import create_app
from agentic_data_platform.service.config import ServiceSettings


class RunResourcesTest(unittest.TestCase):
    def setUp(self):
        self.engine = create_database_engine(
            "sqlite+pysqlite:///:memory:",
            connect_args={"check_same_thread": False},
            poolclass=StaticPool,
        )
        upgrade_database(self.engine)
        self.completed_run = _completed_run("run_001", project_id="pilot-project")
        self.failed_run = _failed_run("run_002", project_id="other-project")
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
            projects = ProjectRepository(session)
            projects.create_project(
                project_id="pilot-project",
                name="pilot group",
                owner_team_id="pilot-project",
            )
            projects.create_project(
                project_id="other-project",
                name="Other Project",
                owner_team_id="pilot-project",
            )
            runs = RunRepository(session)
            runs.save_run(self.completed_run)
            runs.save_run(self.failed_run)
        self.client = TestClient(_app(self.engine))

    def tearDown(self):
        self.engine.dispose()

    def test_list_runs_returns_dashboard_projections_with_request_id(self):
        response = self.client.get("/runs", headers={"X-Request-ID": "req-runs-001"})

        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            response.json(),
            {
                "runs": [
                    RunDashboardProjection.from_run(self.completed_run).to_dict(),
                    RunDashboardProjection.from_run(self.failed_run).to_dict(),
                ],
                "request_id": "req-runs-001",
            },
        )

    def test_list_runs_filters_by_project_and_status(self):
        response = self.client.get(
            "/runs?project_id=pilot-project&status=succeeded",
            headers={"X-Request-ID": "req-filtered-runs-001"},
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            response.json(),
            {
                "runs": [RunDashboardProjection.from_run(self.completed_run).to_dict()],
                "request_id": "req-filtered-runs-001",
            },
        )

    def test_get_run_returns_single_dashboard_projection(self):
        response = self.client.get("/runs/run_001", headers={"X-Request-ID": "req-run-001"})

        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            response.json(),
            {
                "run": RunDashboardProjection.from_run(self.completed_run).to_dict(),
                "lifecycle_events": [],
                "request_id": "req-run-001",
            },
        )

    def test_get_run_returns_404_for_missing_run(self):
        response = self.client.get("/runs/missing-run")

        self.assertEqual(response.status_code, 404)

    def test_list_artifacts_returns_sanitized_projection_artifacts(self):
        response = self.client.get("/runs/run_001/artifacts", headers={"X-Request-ID": "req-artifacts-001"})

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(
            payload,
            {
                "run_id": "run_001",
                "artifacts": RunDashboardProjection.from_run(self.completed_run).to_dict()["artifacts"],
                "request_id": "req-artifacts-001",
            },
        )
        rendered = json.dumps(payload)
        self.assertNotIn("file://", rendered)
        self.assertNotIn("/srv/private", rendered)
        self.assertNotIn("X-Amz-Signature", rendered)
        self.assertNotIn("secret", rendered)

    def test_get_evaluation_returns_projected_evaluator_object(self):
        response = self.client.get("/runs/run_001/evaluation", headers={"X-Request-ID": "req-eval-001"})

        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            response.json(),
            {
                "run_id": "run_001",
                "evaluation": RunDashboardProjection.from_run(self.completed_run).to_dict()["evaluator"],
                "request_id": "req-eval-001",
            },
        )

    def test_get_evaluation_returns_404_when_run_has_no_evaluator(self):
        response = self.client.get("/runs/run_002/evaluation")

        self.assertEqual(response.status_code, 404)

    def test_create_run_queues_record_and_survives_new_app_instance(self):
        response = self.client.post(
            "/runs",
            json=_run_create_payload(run_id="run_create_001"),
            headers={"X-Request-ID": "req-create-001"},
        )

        self.assertEqual(response.status_code, 201)
        payload = response.json()
        self.assertEqual(payload["request_id"], "req-create-001")
        self.assertEqual(payload["run"]["run_id"], "run_create_001")
        self.assertEqual(payload["run"]["status"], "queued")
        self.assertEqual(payload["run"]["created_by_user_id"], "[REDACTED_OWNER]")
        self.assertEqual(payload["run"]["task"]["benchmark_suite"], "SkillLearnBench")
        self.assertEqual(payload["run"]["evaluators"][0]["evaluator_id"], "llm-judge-v0")
        self.assertEqual(payload["run"]["evaluators"][0]["mode"], "llm_judge")
        self.assertEqual(payload["run"]["evaluators"][0]["judge"]["rubric_version"], "latent-skill-benchmark-2026-05-28")
        self.assertEqual(payload["lifecycle_events"][0]["event_type"], "run.created")
        self.assertEqual(payload["lifecycle_events"][0]["from_status"], None)
        self.assertEqual(payload["lifecycle_events"][0]["to_status"], "queued")

        restarted_client = TestClient(_app(self.engine))
        detail = restarted_client.get("/runs/run_create_001", headers={"X-Request-ID": "req-detail-001"})

        self.assertEqual(detail.status_code, 200)
        self.assertEqual(detail.json()["request_id"], "req-detail-001")
        self.assertEqual(detail.json()["run"]["run_id"], "run_create_001")
        self.assertEqual(detail.json()["run"]["status"], "queued")
        self.assertEqual(detail.json()["run"]["evaluators"][0]["evaluator_id"], "llm-judge-v0")
        self.assertEqual(detail.json()["lifecycle_events"][0]["event_type"], "run.created")

    def test_create_run_rejects_invalid_nested_request_with_422(self):
        payload = _run_create_payload(run_id="run_bad_payload_001")
        del payload["task"]["source_uri"]

        response = self.client.post(
            "/runs",
            json=payload,
            headers={"X-Request-ID": "req-bad-payload-001"},
        )

        self.assertEqual(response.status_code, 422)
        self.assertEqual(response.json()["error"]["code"], "validation_error")
        self.assertEqual(response.json()["error"]["request_id"], "req-bad-payload-001")

    def test_create_run_rejects_unknown_user_with_404(self):
        payload = _run_create_payload(run_id="run_unknown_user_001")
        payload["created_by_user_id"] = "missing-user"

        response = self.client.post(
            "/runs",
            json=payload,
            headers={"X-Request-ID": "req-unknown-user-001"},
        )

        self.assertEqual(response.status_code, 404)
        self.assertEqual(response.json()["error"]["code"], "not_found")
        self.assertIn("User not found", response.json()["error"]["message"])

    def test_create_run_redacts_provider_secrets_and_preserves_config_refs(self):
        payload = _run_create_payload(run_id="run_secret_boundary_001")
        payload["model"]["provider_config_id"] = "default-agent-model"
        payload["model"]["secret_ref"] = "env:MODEL_PROVIDER_API_KEY"
        payload["model"]["metadata"] = {
            "temperature": 0,
            "api_key": "sk-raw-model-secret",
            "safe_note": "dev model config",
        }
        payload["evaluators"][0]["provider_config_id"] = "default-evaluator-model"
        payload["evaluators"][0]["secret_ref"] = "env:EVALUATOR_PROVIDER_API_KEY"
        payload["evaluators"][0]["metadata"] = {
            "evaluation_mode": "llm_judge",
            "access_token": "raw-evaluator-token",
        }
        payload["evaluators"][0]["judge"]["metadata"] = {"authorization": "Bearer raw-judge-secret"}

        response = self.client.post(
            "/runs",
            json=payload,
            headers={"X-Request-ID": "req-secret-boundary-001"},
        )

        self.assertEqual(response.status_code, 201)
        rendered_response = json.dumps(response.json())
        self.assertNotIn("sk-raw-model-secret", rendered_response)
        self.assertNotIn("raw-evaluator-token", rendered_response)
        self.assertNotIn("raw-judge-secret", rendered_response)
        evaluator = response.json()["run"]["evaluators"][0]
        self.assertEqual(evaluator["metadata"]["provider_config_id"], "default-evaluator-model")
        self.assertEqual(evaluator["metadata"]["secret_ref"], "env:EVALUATOR_PROVIDER_API_KEY")
        self.assertEqual(evaluator["metadata"]["access_token"], "[redacted]")

        with session_scope(self.engine) as session:
            persisted = RunRepository(session).get_run("run_secret_boundary_001")

        rendered_persisted = json.dumps(persisted.to_dict())
        self.assertNotIn("sk-raw-model-secret", rendered_persisted)
        self.assertNotIn("raw-evaluator-token", rendered_persisted)
        self.assertNotIn("raw-judge-secret", rendered_persisted)
        self.assertEqual(persisted.model.metadata["provider_config_id"], "default-agent-model")
        self.assertEqual(persisted.model.metadata["secret_ref"], "env:MODEL_PROVIDER_API_KEY")
        self.assertEqual(persisted.model.metadata["api_key"], "[redacted]")

    def test_list_runs_filters_by_benchmark_task_creator_and_time_range(self):
        create_response = self.client.post("/runs", json=_run_create_payload(run_id="run_filter_001"))
        self.assertEqual(create_response.status_code, 201)

        response = self.client.get(
            "/runs"
            "?benchmark_suite=SkillLearnBench"
            "&task_family=spreadsheet-from-documents"
            "&task_instance_id=conference-expense-03"
            "&created_by_user_id=[REDACTED_OWNER]"
            "&created_after=2000-01-01T00:00:00Z"
            "&created_before=2100-01-01T00:00:00Z",
            headers={"X-Request-ID": "req-filter-run-001"},
        )

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["request_id"], "req-filter-run-001")
        self.assertEqual([item["run_id"] for item in payload["runs"]], ["run_filter_001"])

    def test_cancel_run_records_lifecycle_event_and_rejects_terminal_transition(self):
        create_response = self.client.post("/runs", json=_run_create_payload(run_id="run_cancel_001"))
        self.assertEqual(create_response.status_code, 201)

        response = self.client.post(
            "/runs/run_cancel_001/cancel",
            json={"reason": "user requested cancellation", "actor_user_id": "[REDACTED_OWNER]"},
            headers={"X-Request-ID": "req-cancel-001"},
        )

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["run"]["status"], "canceled")
        self.assertEqual(payload["run"]["failure_reason"], "user requested cancellation")
        self.assertEqual(payload["lifecycle_events"][-1]["event_type"], "run.canceled")
        self.assertEqual(payload["lifecycle_events"][-1]["from_status"], "queued")
        self.assertEqual(payload["lifecycle_events"][-1]["to_status"], "canceled")

        unknown_actor = self.client.post(
            "/runs/run_cancel_001/cancel",
            json={"reason": "actor typo", "actor_user_id": "missing-user"},
            headers={"X-Request-ID": "req-missing-actor-001"},
        )

        self.assertEqual(unknown_actor.status_code, 404)
        self.assertEqual(unknown_actor.json()["error"]["code"], "not_found")

        rejected = self.client.post(
            "/runs/run_cancel_001/cancel",
            json={"reason": "cancel twice"},
            headers={"X-Request-ID": "req-cancel-again-001"},
        )

        self.assertEqual(rejected.status_code, 409)
        self.assertEqual(rejected.json()["error"]["code"], "conflict")
        self.assertEqual(rejected.json()["error"]["request_id"], "req-cancel-again-001")
        self.assertIn("Invalid run status transition", rejected.json()["error"]["message"])

    def test_retry_canceled_run_requeues_same_run_with_next_attempt(self):
        self.assertEqual(
            self.client.post("/runs", json=_run_create_payload(run_id="run_retry_001")).status_code,
            201,
        )
        self.assertEqual(
            self.client.post("/runs/run_retry_001/cancel", json={"reason": "dry run stopped"}).status_code,
            200,
        )

        response = self.client.post(
            "/runs/run_retry_001/retry",
            json={"reason": "retry after config fix", "actor_user_id": "[REDACTED_OWNER]"},
            headers={"X-Request-ID": "req-retry-001"},
        )

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["run"]["status"], "queued")
        self.assertEqual(payload["run"]["failure_reason"], None)
        self.assertEqual(payload["lifecycle_events"][-1]["event_type"], "run.retried")
        self.assertEqual(payload["lifecycle_events"][-1]["from_status"], "canceled")
        self.assertEqual(payload["lifecycle_events"][-1]["to_status"], "queued")
        self.assertEqual(payload["lifecycle_events"][-1]["attempt_id"], "run_retry_001:attempt:2")

    def test_retry_rejects_non_terminal_retry_candidate_with_structured_error(self):
        create_response = self.client.post("/runs", json=_run_create_payload(run_id="run_retry_reject_001"))
        self.assertEqual(create_response.status_code, 201)

        response = self.client.post(
            "/runs/run_retry_reject_001/retry",
            json={"reason": "not terminal yet"},
            headers={"X-Request-ID": "req-retry-reject-001"},
        )

        self.assertEqual(response.status_code, 409)
        self.assertEqual(response.json()["error"]["code"], "conflict")
        self.assertIn("can only retry failed or canceled runs", response.json()["error"]["message"])

    def test_run_action_rejects_blank_reason_with_structured_422(self):
        create_response = self.client.post("/runs", json=_run_create_payload(run_id="run_blank_reason_001"))
        self.assertEqual(create_response.status_code, 201)

        response = self.client.post(
            "/runs/run_blank_reason_001/cancel",
            json={"reason": "", "actor_user_id": "[REDACTED_OWNER]"},
            headers={"X-Request-ID": "req-blank-reason-001"},
        )

        self.assertEqual(response.status_code, 422)
        self.assertEqual(response.json()["error"]["code"], "validation_error")
        self.assertEqual(response.json()["error"]["request_id"], "req-blank-reason-001")

        detail = self.client.get("/runs/run_blank_reason_001")
        self.assertEqual(detail.status_code, 200)
        self.assertEqual(detail.json()["run"]["status"], "queued")
        self.assertEqual([event["event_type"] for event in detail.json()["lifecycle_events"]], ["run.created"])


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


def _completed_run(run_id: str, *, project_id: str) -> RunRecord:
    run = _base_run(run_id, project_id=project_id)
    run.transition_to(RunStatus.PROVISIONING)
    run.transition_to(RunStatus.RUNNING)
    run.add_turn(
        TerminalTurn(
            turn_index=0,
            command="python solve.py",
            cwd="/workspace",
            started_at=datetime(2026, 5, 28, 12, 0, 0, tzinfo=timezone.utc),
            completed_at=datetime(2026, 5, 28, 12, 0, 2, tzinfo=timezone.utc),
            exit_code=0,
            stdout="created answer.xlsx\n",
            stderr="",
            changed_paths=["answer.xlsx"],
            model_call_id="call_001",
        )
    )
    run.transition_to(RunStatus.EVALUATING)
    run.attach_artifact(
        ArtifactRef(
            artifact_id=f"{run_id}-trajectory",
            kind=ArtifactKind.TRAJECTORY,
            uri=f"minio://runs/{run_id}/trajectory.jsonl",
            media_type="application/x-ndjson",
            sha256="1" * 64,
            size_bytes=512,
            metadata={"storage_key": f"runs/{run_id}/trajectory.jsonl"},
        )
    )
    run.attach_artifact(
        ArtifactRef(
            artifact_id=f"{run_id}-workspace-snapshot",
            kind=ArtifactKind.WORKSPACE_SNAPSHOT,
            uri="file:///srv/private/workspace/snapshot.json",
            media_type="application/json",
            sha256="2" * 64,
            size_bytes=2048,
            metadata={"storage_key": f"runs/{run_id}/workspace/snapshot.json"},
        )
    )
    run.attach_artifact(
        ArtifactRef(
            artifact_id=f"{run_id}-signed-report",
            kind=ArtifactKind.EVALUATOR_REPORT,
            uri=f"https://storage.example/runs/{run_id}/report.json?X-Amz-Signature=secret#fragment",
            media_type="application/json",
            sha256="3" * 64,
            size_bytes=1024,
            metadata={"storage_key": f"runs/{run_id}/evaluation/report.json"},
        )
    )
    run.attach_evaluator_result(
        EvaluatorResult(
            evaluator_id="llm-judge-v0",
            status="completed",
            score=0.91,
            metrics={"task_success": True},
            verbal_feedback="The extracted invoice workbook is correct.",
            judge=JudgeConfig(
                provider="openai",
                model_name="gpt-5",
                rubric_version="latent-skill-benchmark-2026-05-28",
            ),
            artifact_refs=[f"file:///srv/private/{run_id}/evaluation/report.json"],
        )
    )
    run.transition_to(RunStatus.SUCCEEDED)
    return run


def _failed_run(run_id: str, *, project_id: str) -> RunRecord:
    run = _base_run(run_id, project_id=project_id)
    run.transition_to(RunStatus.FAILED)
    return run


def _base_run(run_id: str, *, project_id: str) -> RunRecord:
    return RunRecord.create(
        run_id=run_id,
        project_id=project_id,
        owner_team="pilot group",
        task=BenchmarkTaskInstance(
            benchmark_suite="SkillFlow",
            benchmark_version="hf:zhang-ziao/SkillFlow-Task@2026-05-28",
            task_family="OCR-Data-Extraction",
            instance_id=f"{run_id}-invoice",
            source_uri="https://huggingface.co/datasets/zhang-ziao/SkillFlow-Task",
            input_artifact_refs=["minio://benchmarks/skillflow/input.tar.zst"],
            required_artifacts=["trajectory", "workspace_snapshot", "evaluator_report"],
            metadata={"instruction": "Extract invoice fields."},
        ),
        model=ModelConfig(
            provider="openai",
            model_name="gpt-5",
            mode=ModelMode.API,
            prompt_template_version="terminal-agent-v0",
            model_version="2026-05-28",
        ),
        runner=RunnerConfig(
            kind=RunnerKind.ORIGINAL_BENCHMARK,
            sandbox_backend=SandboxBackend.DOCKER_TERMINAL,
            image="python:3.12-slim",
            entrypoint=["python", "-m", "agentic_data_platform.benchmark_wrappers.skillflow"],
            internet_access=True,
            resource_limits={"cpu": 2, "memory_gib": 8, "timeout_seconds": 3600},
            metadata={"runner_contract": "skillflow-original-wrapper-v0"},
        ),
        metadata={"benchmark_adapter": "SkillFlow"},
    )


def _run_create_payload(*, run_id: str) -> dict:
    return {
        "run_id": run_id,
        "project_id": "pilot-project",
        "created_by_user_id": "[REDACTED_OWNER]",
        "owner_team": "pilot group",
        "task": {
            "benchmark_suite": "SkillLearnBench",
            "benchmark_version": "git:cxcscmu/SkillLearnBench@abc123",
            "task_family": "spreadsheet-from-documents",
            "instance_id": "conference-expense-03",
            "source_uri": "https://github.com/cxcscmu/SkillLearnBench",
            "input_artifact_refs": ["minio://benchmarks/skilllearnbench/conference/input.tar.zst"],
            "required_artifacts": ["trajectory", "workspace_snapshot", "evaluator_report"],
            "metadata": {"instruction": "Create a spreadsheet from receipts."},
        },
        "model": {
            "provider": "openai",
            "model_name": "gpt-5",
            "mode": "api",
            "prompt_template_version": "terminal-agent-v0",
            "model_version": "2026-05-28",
            "metadata": {"temperature": 0},
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
                "evaluator_id": "llm-judge-v0",
                "mode": "llm_judge",
                "judge": {
                    "provider": "openai",
                    "model_name": "gpt-5",
                    "rubric_version": "latent-skill-benchmark-2026-05-28",
                },
                "metadata": {"evaluation_mode": "llm_judge"},
            }
        ],
        "metadata": {"benchmark_adapter": "SkillLearnBench"},
    }
