import json
import tempfile
import unittest
import zipfile
from io import BytesIO
from pathlib import Path

from fastapi.testclient import TestClient
from sqlalchemy.pool import StaticPool

from agentic_data_platform.artifacts.store import LocalArtifactStore
from agentic_data_platform.persistence.database import create_database_engine, session_scope
from agentic_data_platform.persistence.migrations import upgrade_database
from agentic_data_platform.persistence.repositories import AuditEventRepository, IdentityRepository, ProjectRepository
from agentic_data_platform.service.app import create_app
from agentic_data_platform.service.config import ServiceSettings


class HarborTaskResourceTest(unittest.TestCase):
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
            identities.create_team(team_id="view-only", name="View Only")
            identities.create_user(
                user_id="[REDACTED_OWNER]",
                email="[REDACTED_OWNER]@example.com",
                display_name="[REDACTED_OWNER]",
                team_id="pilot-project",
            )
            identities.create_user(
                user_id="viewer",
                email="viewer@example.com",
                display_name="Viewer User",
                team_id="view-only",
            )
            identities.add_member(team_id="pilot-project", user_id="viewer", role="viewer")
            ProjectRepository(session).create_project(
                project_id="latent-skill-pilot",
                name="Latent Skill Pilot",
                owner_team_id="pilot-project",
                created_by_user_id="[REDACTED_OWNER]",
            )
        self.temp_dir = tempfile.TemporaryDirectory()
        app = _app(self.engine)
        app.state.artifact_store = LocalArtifactStore(Path(self.temp_dir.name) / "artifacts")
        self.client = TestClient(app, headers={"Authorization": "Bearer [REDACTED_OWNER]-token"})

    def tearDown(self):
        self.temp_dir.cleanup()
        self.engine.dispose()

    def test_uploads_valid_harbor_task_archive_and_returns_launch_metadata(self):
        archive = _harbor_task_zip()

        response = self.client.post(
            "/harbor/task-uploads",
            data={
                "project_id": "latent-skill-pilot",
                "source_uri": "https://github.com/carinrc/internal-benchmarks",
                "source_version": "git:abc123",
            },
            files={"archive": ("invoice-task.zip", archive, "application/zip")},
            headers={"X-Request-ID": "req-harbor-upload-001"},
        )

        self.assertEqual(response.status_code, 201)
        payload = response.json()
        upload = payload["task_upload"]
        self.assertEqual(payload["request_id"], "req-harbor-upload-001")
        self.assertEqual(upload["project_id"], "latent-skill-pilot")
        self.assertEqual(upload["filename"], "invoice-task.zip")
        self.assertEqual(upload["task_name"], "latent/invoice-extraction")
        self.assertEqual(upload["source_uri"], "https://github.com/carinrc/internal-benchmarks")
        self.assertEqual(upload["source_version"], "git:abc123")
        self.assertEqual(upload["validation"]["errors"], [])
        self.assertEqual(upload["validation"]["declared_artifacts"], ["/logs/artifacts/receipts.xlsx"])
        self.assertEqual(upload["launch_metadata"]["harbor_run"]["task_archive_storage_key"], upload["storage_key"])
        self.assertEqual(upload["launch_metadata"]["harbor_run"]["environment"], "docker")
        self.assertNotIn("file://", json.dumps(payload))
        self.assertTrue((Path(self.temp_dir.name) / "artifacts" / upload["storage_key"]).is_file())

        with session_scope(self.engine) as session:
            events = AuditEventRepository(session).list_events(project_id="latent-skill-pilot")
        self.assertEqual([event.event_type for event in events], ["harbor_task_upload.created"])
        self.assertEqual(events[0].subject_id, upload["task_upload_id"])
        self.assertEqual(events[0].payload["storage_key"], upload["storage_key"])

    def test_rejects_invalid_harbor_task_archive_with_actionable_errors(self):
        archive = _zip_bytes(
            {
                "task.toml": '[task]\nname = "missing/instruction"\n',
                "environment/Dockerfile": "FROM ubuntu:24.04\n",
                "tests/test.sh": "echo 1 > /logs/verifier/reward.txt\n",
            }
        )

        response = self.client.post(
            "/harbor/task-uploads",
            data={"project_id": "latent-skill-pilot"},
            files={"archive": ("bad-task.zip", archive, "application/zip")},
            headers={"X-Request-ID": "req-harbor-upload-invalid-001"},
        )

        self.assertEqual(response.status_code, 422)
        error = response.json()["error"]
        self.assertEqual(error["code"], "validation_error")
        self.assertIn("instruction.md", error["message"])
        self.assertEqual(error["request_id"], "req-harbor-upload-invalid-001")

    def test_rejects_uploads_that_exceed_configured_archive_bytes(self):
        client = TestClient(_app(self.engine, max_upload_bytes=64), headers={"Authorization": "Bearer [REDACTED_OWNER]-token"})

        response = client.post(
            "/harbor/task-uploads",
            data={"project_id": "latent-skill-pilot"},
            files={"archive": ("too-large.zip", b"x" * 65, "application/zip")},
            headers={"X-Request-ID": "req-harbor-upload-large-001"},
        )

        self.assertEqual(response.status_code, 413)
        error = response.json()["error"]
        self.assertEqual(error["code"], "payload_too_large")
        self.assertIn("64 bytes", error["message"])
        self.assertEqual(error["request_id"], "req-harbor-upload-large-001")

    def test_rejects_uploads_that_exceed_configured_zip_limits(self):
        client = TestClient(_app(self.engine, max_upload_files=3), headers={"Authorization": "Bearer [REDACTED_OWNER]-token"})

        response = client.post(
            "/harbor/task-uploads",
            data={"project_id": "latent-skill-pilot"},
            files={"archive": ("too-many-files.zip", _harbor_task_zip(), "application/zip")},
            headers={"X-Request-ID": "req-harbor-upload-files-001"},
        )

        self.assertEqual(response.status_code, 422)
        error = response.json()["error"]
        self.assertEqual(error["code"], "validation_error")
        self.assertIn("too many files", error["message"])
        self.assertEqual(error["request_id"], "req-harbor-upload-files-001")

    def test_viewer_cannot_upload_harbor_task_archive(self):
        viewer_client = TestClient(_app(self.engine), headers={"Authorization": "Bearer viewer-token"})

        response = viewer_client.post(
            "/harbor/task-uploads",
            data={"project_id": "latent-skill-pilot"},
            files={"archive": ("invoice-task.zip", _harbor_task_zip(), "application/zip")},
        )

        self.assertEqual(response.status_code, 403)


def _app(
    engine,
    *,
    max_upload_bytes: int = 10 * 1024 * 1024,
    max_upload_files: int = 256,
    max_uncompressed_bytes: int = 50 * 1024 * 1024,
):
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
            internal_auth_tokens="[REDACTED_OWNER]=[REDACTED_OWNER]-token,viewer=viewer-token",
            harbor_task_upload_max_bytes=max_upload_bytes,
            harbor_task_upload_max_files=max_upload_files,
            harbor_task_upload_max_uncompressed_bytes=max_uncompressed_bytes,
        ),
        database_engine=engine,
    )


def _harbor_task_zip() -> bytes:
    return _zip_bytes(
        {
            "invoice-task/instruction.md": "Create `/app/receipts.xlsx`.\n",
            "invoice-task/task.toml": "\n".join(
                [
                    'schema_version = "1.2"',
                    'artifacts = ["/logs/artifacts/receipts.xlsx"]',
                    "",
                    "[task]",
                    'name = "latent/invoice-extraction"',
                    "",
                    "[verifier]",
                    "timeout_sec = 120.0",
                    "",
                    "[agent]",
                    "timeout_sec = 300.0",
                    "",
                    "[environment]",
                    'os = "linux"',
                    "allow_internet = true",
                    "",
                ]
            ),
            "invoice-task/environment/Dockerfile": "FROM ubuntu:24.04\nWORKDIR /app\n",
            "invoice-task/tests/test.sh": "mkdir -p /logs/verifier\nprintf '1\\n' > /logs/verifier/reward.txt\n",
        }
    )


def _zip_bytes(files: dict[str, str]) -> bytes:
    buffer = BytesIO()
    with zipfile.ZipFile(buffer, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for path, content in files.items():
            archive.writestr(path, content)
    return buffer.getvalue()
