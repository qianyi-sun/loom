import unittest

from fastapi.testclient import TestClient

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
            }
        )

        self.assertEqual(settings.app_name, "adp-test")
        self.assertEqual(settings.environment, "test")
        self.assertEqual(settings.database_url, "postgresql://user:pass@postgres:5432/adp")
        self.assertEqual(settings.redis_url, "redis://redis:6379/0")
        self.assertEqual(settings.object_storage_endpoint, "http://minio:9000")
        self.assertEqual(settings.object_storage_bucket, "adp-bucket")

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


def _settings() -> ServiceSettings:
    return ServiceSettings(
        app_name="agentic-data-platform",
        environment="test",
        database_url="postgresql://user:pass@postgres:5432/adp",
        redis_url="redis://redis:6379/0",
        object_storage_endpoint="http://minio:9000",
        object_storage_bucket="adp-bucket",
    )
