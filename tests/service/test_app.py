import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from fastapi.testclient import TestClient

from agentic_data_platform.persistence import (
    IdentityRepository,
    ProjectRepository,
    create_database_engine,
    session_scope,
)
from agentic_data_platform.persistence.migrations import upgrade_database
from agentic_data_platform.service.app import create_app
from agentic_data_platform.service.config import ServiceSettings, load_service_settings


class ServiceAppTest(unittest.TestCase):
    def test_loads_service_settings_from_environment(self):
        settings = load_service_settings(
            {
                "APP_NAME": "adp-test",
                "APP_ENV": "test",
                "DATABASE_URL": "postgresql://user:pass@postgres:5432/adp",
                "REDIS_URL": "redis://redis:6379/0",
                "OBJECT_STORAGE_ENDPOINT": "http://minio:9000",
                "OBJECT_STORAGE_BUCKET": "adp-bucket",
                "OBJECT_STORAGE_ACCESS_KEY": "loomdev",
                "OBJECT_STORAGE_SECRET_KEY": "miniosecret",
                "OBJECT_STORAGE_REGION": "us-west-2",
                "MODEL_PROVIDER_BASE_URL": "https://models.example/v1",
                "MODEL_PROVIDER_API_KEY": "sk-model-secret",
                "MODEL_PROVIDER_MODELS": "gpt-5,gpt-5-mini",
                "EVALUATOR_PROVIDER_BASE_URL": "https://judge.example/v1",
                "EVALUATOR_PROVIDER_API_KEY": "sk-judge-secret",
                "INTERNAL_AUTH_TOKENS": "[REDACTED_OWNER]=[REDACTED_OWNER]-token",
                "WEB_LOGIN_CREDENTIALS": "[REDACTED_OWNER]=[REDACTED_PASSWORD]:[REDACTED_OWNER]",
                "WEB_SESSION_SECRET": "dev-session-secret",
                "WEB_SESSION_TTL_SECONDS": "3600",
                "SANDBOX_WORKSPACE_ROOT": "/workspace/.runtime/sandbox-workspaces",
                "SANDBOX_HOST_WORKSPACE_ROOT": "/srv/agentic-data-platform/dev/current/.runtime/sandbox-workspaces",
                "WORKER_SUBPROCESS_ISOLATION_ENABLED": "true",
                "WORKER_SUBPROCESS_TIMEOUT_SECONDS": "1800",
                "WORKER_HEARTBEAT_INTERVAL_SECONDS": "15",
                "WORKER_CANCEL_POLL_INTERVAL_SECONDS": "0.5",
                "WORKER_LEGACY_QUEUE_CLAIM_ENABLED": "true",
                "SCHEDULER_GLOBAL_MAX_ACTIVE_RUNS": "8",
                "SCHEDULER_BACKEND_MAX_ACTIVE_RUNS": "harbor-local-docker=3,docker_terminal=2",
                "SCHEDULER_PROJECT_MAX_ACTIVE_RUNS": "pilot-project=2,foundation-model=1",
                "SCHEDULER_PROVIDER_MAX_ACTIVE_RUNS": "openai=4,anthropic=2",
                "SCHEDULER_MODEL_MAX_ACTIVE_RUNS": "gpt-5=2,claude-sonnet-4=1",
                "SCHEDULER_AGENT_MAX_ACTIVE_RUNS": "codex=2,aider=1",
                "SCHEDULER_BENCHMARK_MAX_ACTIVE_RUNS": "terminal-bench@2.0=2,skillflow@2026-06-01=1",
                "SCHEDULER_STALE_DISPATCHED_TIMEOUT_SECONDS": "420",
                "SCHEDULER_STALE_ACTIVE_HEARTBEAT_TIMEOUT_SECONDS": "900",
                "SCHEDULER_STALE_ARTIFACT_UPLOAD_TIMEOUT_SECONDS": "1800",
                "SCHEDULER_DOCKER_CLEANUP_ENABLED": "true",
                "SCHEDULER_DOCKER_CLEANUP_TIMEOUT_SECONDS": "45",
                "SCHEDULER_RECOVERY_BATCH_SIZE": "25",
            }
        )

        self.assertEqual(settings.app_name, "adp-test")
        self.assertEqual(settings.environment, "test")
        self.assertEqual(settings.database_url, "postgresql://user:pass@postgres:5432/adp")
        self.assertEqual(settings.redis_url, "redis://redis:6379/0")
        self.assertEqual(settings.object_storage_endpoint, "http://minio:9000")
        self.assertEqual(settings.object_storage_bucket, "adp-bucket")
        self.assertEqual(settings.object_storage_access_key, "loomdev")
        self.assertEqual(settings.object_storage_secret_key, "miniosecret")
        self.assertEqual(settings.object_storage_region, "us-west-2")
        self.assertEqual(settings.model_provider_base_url, "https://models.example/v1")
        self.assertEqual(settings.model_provider_api_key, "sk-model-secret")
        self.assertEqual(settings.model_provider_models, "gpt-5,gpt-5-mini")
        self.assertEqual(settings.evaluator_provider_base_url, "https://judge.example/v1")
        self.assertEqual(settings.evaluator_provider_api_key, "sk-judge-secret")
        self.assertEqual(settings.internal_auth_tokens, "[REDACTED_OWNER]=[REDACTED_OWNER]-token")
        self.assertEqual(settings.web_login_credentials, "[REDACTED_OWNER]=[REDACTED_PASSWORD]:[REDACTED_OWNER]")
        self.assertEqual(settings.web_session_secret, "dev-session-secret")
        self.assertEqual(settings.web_session_ttl_seconds, 3600)
        self.assertEqual(settings.sandbox_workspace_root, "/workspace/.runtime/sandbox-workspaces")
        self.assertEqual(
            settings.sandbox_host_workspace_root,
            "/srv/agentic-data-platform/dev/current/.runtime/sandbox-workspaces",
        )
        self.assertTrue(settings.worker_subprocess_isolation_enabled)
        self.assertEqual(settings.worker_subprocess_timeout_seconds, 1800)
        self.assertEqual(settings.worker_heartbeat_interval_seconds, 15)
        self.assertEqual(settings.worker_cancel_poll_interval_seconds, 0.5)
        self.assertTrue(settings.worker_legacy_queue_claim_enabled)
        self.assertEqual(settings.scheduler_global_max_active_runs, 8)
        self.assertEqual(
            settings.scheduler_backend_max_active_runs,
            {"harbor-local-docker": 3, "docker_terminal": 2},
        )
        self.assertEqual(
            settings.scheduler_project_max_active_runs,
            {"pilot-project": 2, "foundation-model": 1},
        )
        self.assertEqual(settings.scheduler_provider_max_active_runs, {"openai": 4, "anthropic": 2})
        self.assertEqual(settings.scheduler_model_max_active_runs, {"gpt-5": 2, "claude-sonnet-4": 1})
        self.assertEqual(settings.scheduler_agent_max_active_runs, {"codex": 2, "aider": 1})
        self.assertEqual(
            settings.scheduler_benchmark_max_active_runs,
            {"terminal-bench@2.0": 2, "skillflow@2026-06-01": 1},
        )
        self.assertEqual(settings.scheduler_stale_dispatched_timeout_seconds, 420)
        self.assertEqual(settings.scheduler_stale_active_heartbeat_timeout_seconds, 900)
        self.assertEqual(settings.scheduler_stale_artifact_upload_timeout_seconds, 1800)
        self.assertTrue(settings.scheduler_docker_cleanup_enabled)
        self.assertEqual(settings.scheduler_docker_cleanup_timeout_seconds, 45)
        self.assertEqual(settings.scheduler_recovery_batch_size, 25)

    def test_health_endpoint_returns_service_identity(self):
        client = TestClient(create_app(_settings()))

        response = client.get("/healthz", headers={"X-Request-ID": "req-health-001"})

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.headers["X-Request-ID"], "req-health-001")
        self.assertEqual(
            response.json(),
            {
                "status": "ok",
                "service": "agentic-data-platform",
                "environment": "test",
                "request_id": "req-health-001",
            },
        )

    def test_readiness_endpoint_reports_configured_dependencies(self):
        client = TestClient(create_app(_settings()))

        response = client.get("/readyz", headers={"X-Request-ID": "req-ready-001"})

        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            response.json(),
            {
                "status": "ready",
                "dependencies": {
                    "database": "configured",
                    "redis": "configured",
                    "object_storage": "configured",
                    "auth": "configured",
                },
                "request_id": "req-ready-001",
            },
        )

    def test_readiness_endpoint_fails_when_required_dependency_config_is_missing(self):
        client = TestClient(
            create_app(
                ServiceSettings(
                    app_name="agentic-data-platform",
                    environment="test",
                    database_url="",
                    redis_url="redis://redis:6379/0",
                    object_storage_endpoint="http://minio:9000",
                    object_storage_bucket="",
                    object_storage_access_key="loomdev",
                    object_storage_secret_key="loomdev",
                    object_storage_region="us-east-1",
                )
            )
        )

        response = client.get("/readyz", headers={"X-Request-ID": "req-not-ready-001"})

        self.assertEqual(response.status_code, 503)
        self.assertEqual(
            response.json(),
            {
                "status": "not_ready",
                "dependencies": {
                    "database": "missing",
                    "redis": "configured",
                    "object_storage": "missing",
                    "auth": "missing",
                },
                "request_id": "req-not-ready-001",
            },
        )

    def test_readiness_endpoint_treats_injected_database_engine_as_configured(self):
        with TemporaryDirectory() as temp_dir:
            engine = create_database_engine(f"sqlite+pysqlite:///{Path(temp_dir) / 'service.db'}")
            try:
                client = TestClient(
                    create_app(
                        ServiceSettings(
                            app_name="agentic-data-platform",
                            environment="test",
                            database_url="",
                            redis_url="redis://redis:6379/0",
                            object_storage_endpoint="http://minio:9000",
                            object_storage_bucket="adp-bucket",
                            object_storage_access_key="loomdev",
                            object_storage_secret_key="loomdev",
                            object_storage_region="us-east-1",
                            internal_auth_tokens="[REDACTED_OWNER]=[REDACTED_OWNER]-token",
                        ),
                        database_engine=engine,
                    )
                )

                response = client.get("/readyz", headers={"X-Request-ID": "req-ready-engine-001"})
            finally:
                engine.dispose()

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["dependencies"]["database"], "configured")
        self.assertEqual(response.json()["request_id"], "req-ready-engine-001")

    def test_structured_http_errors_include_request_id(self):
        client = TestClient(create_app(_settings()))

        response = client.get(
            "/missing",
            headers={"X-Request-ID": "req-missing-001", "Authorization": "Bearer [REDACTED_OWNER]-token"},
        )

        self.assertEqual(response.status_code, 404)
        self.assertEqual(response.headers["X-Request-ID"], "req-missing-001")
        self.assertEqual(
            response.json(),
            {
                "error": {
                    "code": "not_found",
                    "message": "Not Found",
                    "request_id": "req-missing-001",
                }
            },
        )

    def test_request_logging_includes_request_id_path_and_status(self):
        client = TestClient(create_app(_settings()))

        with self.assertLogs("agentic_data_platform.service", level="INFO") as logs:
            response = client.get("/healthz", headers={"X-Request-ID": "req-log-001"})

        self.assertEqual(response.status_code, 200)
        rendered = "\n".join(logs.output)
        self.assertIn("request_id=req-log-001", rendered)
        self.assertIn("path=/healthz", rendered)
        self.assertIn("status_code=200", rendered)

    def test_openapi_schema_is_available(self):
        client = TestClient(create_app(_settings()))

        response = client.get("/openapi.json")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["info"]["title"], "Agentic Data Platform API")

    def test_openapi_schema_documents_core_resource_examples(self):
        client = TestClient(create_app(_settings()))

        schema = client.get("/openapi.json").json()

        self.assertEqual(
            schema["paths"]["/projects"]["get"]["responses"]["200"]["content"]["application/json"]["example"][
                "projects"
            ][0]["project_id"],
            "latent-skill-pilot",
        )
        self.assertEqual(
            schema["paths"]["/benchmarks"]["get"]["responses"]["200"]["content"]["application/json"]["example"][
                "benchmarks"
            ][0]["suite_name"],
            "SkillFlow",
        )
        self.assertEqual(
            schema["paths"]["/runs"]["get"]["responses"]["200"]["content"]["application/json"]["example"]["runs"][0][
                "run_id"
            ],
            "run_001",
        )

    def test_create_app_wires_resource_routes_to_database_engine(self):
        with TemporaryDirectory() as temp_dir:
            engine = create_database_engine(f"sqlite+pysqlite:///{Path(temp_dir) / 'service.db'}")
            try:
                upgrade_database(engine)
                with session_scope(engine) as session:
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
                        project_id="latent-skill-pilot",
                        name="Latent Skill Pilot",
                        owner_team_id="pilot-project",
                        description="SkillFlow and SkillLearnBench pilot",
                    )

                client = TestClient(create_app(_settings(), database_engine=engine))
                response = client.get(
                    "/projects/latent-skill-pilot",
                    headers={"X-Request-ID": "req-project-001", "Authorization": "Bearer [REDACTED_OWNER]-token"},
                )
            finally:
                engine.dispose()

        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertEqual(body["request_id"], "req-project-001")
        self.assertEqual(body["project"]["project_id"], "latent-skill-pilot")
        self.assertEqual(body["project"]["owner_team_id"], "pilot-project")


def _settings() -> ServiceSettings:
    return ServiceSettings(
        app_name="agentic-data-platform",
        environment="test",
        database_url="postgresql://user:pass@postgres:5432/adp",
        redis_url="redis://redis:6379/0",
        object_storage_endpoint="http://minio:9000",
        object_storage_bucket="adp-bucket",
        object_storage_access_key="loomdev",
        object_storage_secret_key="loomdev",
        object_storage_region="us-east-1",
        internal_auth_tokens="[REDACTED_OWNER]=[REDACTED_OWNER]-token",
    )
