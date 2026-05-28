import json
import unittest
from datetime import datetime, timezone

from fastapi import FastAPI, Request
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
from agentic_data_platform.service.run_resources import register_run_routes


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


def _app(engine) -> FastAPI:
    app = FastAPI()

    @app.middleware("http")
    async def request_id_middleware(request: Request, call_next):
        request.state.request_id = request.headers.get("X-Request-ID", "")
        return await call_next(request)

    def session_dependency():
        with session_scope(engine) as session:
            yield session

    register_run_routes(app, session_dependency)
    return app


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
