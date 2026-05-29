import unittest

from fastapi.testclient import TestClient
from sqlalchemy.pool import StaticPool

from agentic_data_platform.domain.run_records import (
    BenchmarkTaskInstance,
    ModelConfig,
    ModelMode,
    RunnerConfig,
    RunnerKind,
    RunRecord,
    SandboxBackend,
)
from agentic_data_platform.persistence import (
    AuditEventRepository,
    IdentityRepository,
    ProjectRepository,
    RunRepository,
    create_database_engine,
    session_scope,
)
from agentic_data_platform.persistence.migrations import upgrade_database
from agentic_data_platform.service.app import create_app
from agentic_data_platform.service.config import ServiceSettings


class ServiceAuthTest(unittest.TestCase):
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
            identities.create_team(team_id="other-team", name="Other Team")
            identities.create_user(
                user_id="[REDACTED_OWNER]",
                email="[REDACTED_OWNER]@example.com",
                display_name="[REDACTED_OWNER]",
                team_id="pilot-project",
            )
            identities.create_user(
                user_id="viewer",
                email="viewer@example.com",
                display_name="Viewer",
                team_id="pilot-project",
            )
            identities.add_member(team_id="pilot-project", user_id="viewer", role="viewer")
            identities.create_user(
                user_id="outsider",
                email="outsider@example.com",
                display_name="Outsider",
                team_id="other-team",
            )
            projects = ProjectRepository(session)
            projects.create_project(
                project_id="latent-skill-pilot",
                name="Latent Skill Pilot",
                owner_team_id="pilot-project",
                created_by_user_id="[REDACTED_OWNER]",
            )
            projects.create_project(
                project_id="other-project",
                name="Other Project",
                owner_team_id="other-team",
                created_by_user_id="outsider",
            )
            RunRepository(session).save_run(_queued_run("run_existing_001", project_id="latent-skill-pilot"))

        self.client = TestClient(_app(self.engine))

    def tearDown(self):
        self.engine.dispose()

    def test_health_and_docs_remain_public_but_api_requires_bearer_token(self):
        self.assertEqual(self.client.get("/healthz").status_code, 200)
        self.assertEqual(self.client.get("/openapi.json").status_code, 200)

        response = self.client.get("/runs", headers={"X-Request-ID": "req-auth-missing-001"})

        self.assertEqual(response.status_code, 401)
        self.assertEqual(response.headers["X-Request-ID"], "req-auth-missing-001")
        self.assertEqual(response.json()["error"]["code"], "unauthorized")

    def test_project_viewer_can_read_run_but_cannot_create_or_cancel(self):
        read_response = self.client.get(
            "/runs/run_existing_001",
            headers=_auth("viewer-token", request_id="req-viewer-read-001"),
        )

        self.assertEqual(read_response.status_code, 200)
        self.assertEqual(read_response.json()["run"]["run_id"], "run_existing_001")

        create_response = self.client.post(
            "/runs",
            json=_run_create_payload("run_viewer_create_001"),
            headers=_auth("viewer-token", request_id="req-viewer-create-001"),
        )
        cancel_response = self.client.post(
            "/runs/run_existing_001/cancel",
            json={"reason": "viewer cannot cancel"},
            headers=_auth("viewer-token", request_id="req-viewer-cancel-001"),
        )

        self.assertEqual(create_response.status_code, 403)
        self.assertEqual(create_response.json()["error"]["code"], "forbidden")
        self.assertEqual(cancel_response.status_code, 403)
        self.assertEqual(cancel_response.json()["error"]["code"], "forbidden")

    def test_unrelated_team_member_cannot_read_project_run(self):
        response = self.client.get(
            "/runs/run_existing_001",
            headers=_auth("outsider-token", request_id="req-outsider-read-001"),
        )

        self.assertEqual(response.status_code, 403)
        self.assertEqual(response.json()["error"]["request_id"], "req-outsider-read-001")

    def test_member_create_and_cancel_records_audit_events_with_authenticated_actor(self):
        create_response = self.client.post(
            "/runs",
            json=_run_create_payload("run_member_create_001", created_by_user_id=None),
            headers=_auth("[REDACTED_OWNER]-token", request_id="req-member-create-001"),
        )
        cancel_response = self.client.post(
            "/runs/run_member_create_001/cancel",
            json={"reason": "stop test run", "actor_user_id": "[REDACTED_OWNER]"},
            headers=_auth("[REDACTED_OWNER]-token", request_id="req-member-cancel-001"),
        )

        self.assertEqual(create_response.status_code, 201)
        self.assertEqual(create_response.json()["run"]["created_by_user_id"], "[REDACTED_OWNER]")
        self.assertEqual(cancel_response.status_code, 200)
        self.assertEqual(cancel_response.json()["run"]["status"], "canceled")

        with session_scope(self.engine) as session:
            events = AuditEventRepository(session).list_events(run_id="run_member_create_001")

        self.assertEqual([event.event_type for event in events], ["run.created", "run.canceled"])
        self.assertEqual([event.actor_user_id for event in events], ["[REDACTED_OWNER]", "[REDACTED_OWNER]"])
        self.assertEqual([event.request_id for event in events], ["req-member-create-001", "req-member-cancel-001"])
        self.assertEqual(events[0].payload["status"], "queued")
        self.assertEqual(events[1].payload["reason"], "stop test run")

    def test_authenticated_actor_cannot_spoof_body_user_id(self):
        payload = _run_create_payload("run_spoof_001", created_by_user_id="outsider")

        response = self.client.post(
            "/runs",
            json=payload,
            headers=_auth("[REDACTED_OWNER]-token", request_id="req-spoof-create-001"),
        )

        self.assertEqual(response.status_code, 403)
        self.assertEqual(response.json()["error"]["code"], "forbidden")

    def test_ops_metrics_requires_auth_and_exposes_queue_depth(self):
        unauthorized = self.client.get("/ops/metrics", headers={"X-Request-ID": "req-metrics-missing-001"})
        authorized = self.client.get(
            "/ops/metrics",
            headers=_auth("[REDACTED_OWNER]-token", request_id="req-metrics-001"),
        )

        self.assertEqual(unauthorized.status_code, 401)
        self.assertEqual(authorized.status_code, 200)
        payload = authorized.json()
        self.assertEqual(payload["request_id"], "req-metrics-001")
        self.assertEqual(payload["runs_by_status"]["queued"], 1)
        self.assertEqual(payload["queue_depth"], 1)


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
            internal_auth_tokens="[REDACTED_OWNER]=[REDACTED_OWNER]-token,viewer=viewer-token,outsider=outsider-token",
        ),
        database_engine=engine,
    )


def _auth(token: str, *, request_id: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}", "X-Request-ID": request_id}


def _queued_run(run_id: str, *, project_id: str) -> RunRecord:
    return RunRecord.create(
        run_id=run_id,
        project_id=project_id,
        owner_team="pilot group",
        task=BenchmarkTaskInstance(
            benchmark_suite="SkillLearnBench",
            benchmark_version="git:cxcscmu/SkillLearnBench@abc123",
            task_family="spreadsheet-from-documents",
            instance_id=f"{run_id}-instance",
            source_uri="https://github.com/cxcscmu/SkillLearnBench",
            input_artifact_refs=[],
            required_artifacts=["trajectory", "workspace_snapshot", "evaluator_report"],
            metadata={},
        ),
        model=ModelConfig(
            provider="openai",
            model_name="gpt-5",
            mode=ModelMode.API,
            prompt_template_version="terminal-agent-v0",
        ),
        runner=RunnerConfig(
            kind=RunnerKind.ORIGINAL_BENCHMARK,
            sandbox_backend=SandboxBackend.DOCKER_TERMINAL,
            image="python:3.12-slim",
            entrypoint=["python", "-m", "skilllearnbench.runner"],
            internet_access=True,
            resource_limits={"cpu": 2, "memory_gib": 8, "timeout_seconds": 3600},
            metadata={},
        ),
    )


def _run_create_payload(run_id: str, *, created_by_user_id: str | None = "[REDACTED_OWNER]") -> dict:
    payload = {
        "run_id": run_id,
        "project_id": "latent-skill-pilot",
        "owner_team": "pilot group",
        "task": {
            "benchmark_suite": "SkillLearnBench",
            "benchmark_version": "git:cxcscmu/SkillLearnBench@abc123",
            "task_family": "spreadsheet-from-documents",
            "instance_id": "conference-expense-03",
            "source_uri": "https://github.com/cxcscmu/SkillLearnBench",
            "input_artifact_refs": [],
            "required_artifacts": ["trajectory", "workspace_snapshot", "evaluator_report"],
            "metadata": {},
        },
        "model": {
            "provider": "openai",
            "model_name": "gpt-5",
            "mode": "api",
            "prompt_template_version": "terminal-agent-v0",
            "metadata": {},
        },
        "runner": {
            "kind": "original_benchmark",
            "sandbox_backend": "docker_terminal",
            "image": "python:3.12-slim",
            "entrypoint": ["python", "-m", "skilllearnbench.runner"],
            "internet_access": True,
            "resource_limits": {"cpu": 2, "memory_gib": 8, "timeout_seconds": 3600},
            "metadata": {},
        },
        "evaluators": [],
        "metadata": {},
    }
    if created_by_user_id is not None:
        payload["created_by_user_id"] = created_by_user_id
    return payload
