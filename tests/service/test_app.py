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

        response = client.get("/missing", headers={"X-Request-ID": "req-missing-001"})

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
                    ProjectRepository(session).create_project(
                        project_id="latent-skill-pilot",
                        name="Latent Skill Pilot",
                        owner_team_id="pilot-project",
                        description="SkillFlow and SkillLearnBench pilot",
                    )

                client = TestClient(create_app(_settings(), database_engine=engine))
                response = client.get(
                    "/projects/latent-skill-pilot",
                    headers={"X-Request-ID": "req-project-001"},
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
    )
