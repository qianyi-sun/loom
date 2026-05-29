import unittest

from fastapi import FastAPI, Request
from fastapi.testclient import TestClient
from sqlalchemy.pool import StaticPool

from agentic_data_platform.persistence.database import create_database_engine, session_scope
from agentic_data_platform.persistence.migrations import upgrade_database
from agentic_data_platform.persistence.repositories import AuditEventRepository, IdentityRepository, ProjectRepository
from agentic_data_platform.service.project_resources import register_project_routes


class ProjectResourceTest(unittest.TestCase):
    def setUp(self):
        self.engine = create_database_engine(
            "sqlite+pysqlite:///:memory:",
            connect_args={"check_same_thread": False},
            poolclass=StaticPool,
        )
        upgrade_database(self.engine)
        self._seed_records()
        self.client = TestClient(_app_for_engine(self.engine))

    def tearDown(self):
        self.engine.dispose()

    def test_lists_teams_with_request_id(self):
        response = self.client.get("/teams", headers={"X-Request-ID": "req-teams-001"})

        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            response.json(),
            {
                "teams": [
                    {
                        "team_id": "data-platform",
                        "name": "Data Platform",
                        "created_at": self.created_at["team:data-platform"],
                    },
                    {
                        "team_id": "pilot-project",
                        "name": "pilot group",
                        "created_at": self.created_at["team:pilot-project"],
                    },
                ],
                "request_id": "req-teams-001",
            },
        )

    def test_gets_team_by_id(self):
        response = self.client.get("/teams/pilot-project", headers={"X-Request-ID": "req-team-001"})

        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            response.json(),
            {
                "team": {
                    "team_id": "pilot-project",
                    "name": "pilot group",
                    "created_at": self.created_at["team:pilot-project"],
                },
                "request_id": "req-team-001",
            },
        )

    def test_missing_team_maps_to_404(self):
        response = self.client.get("/teams/missing-team")

        self.assertEqual(response.status_code, 404)
        self.assertEqual(response.json(), {"detail": "Team not found"})

    def test_lists_projects_and_filters_by_owner_team_id(self):
        response = self.client.get(
            "/projects",
            params={"owner_team_id": "pilot-project"},
            headers={"X-Request-ID": "req-projects-001"},
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            response.json(),
            {
                "projects": [
                    {
                        "project_id": "latent-skill-pilot",
                        "name": "Latent Skill Pilot",
                        "owner_team_id": "pilot-project",
                        "created_by_user_id": "[REDACTED_OWNER]",
                        "description": "SkillFlow and SkillLearnBench pilot",
                        "status": "active",
                        "created_at": self.created_at["project:latent-skill-pilot"],
                        "updated_at": self.updated_at["project:latent-skill-pilot"],
                    }
                ],
                "request_id": "req-projects-001",
            },
        )

    def test_gets_project_by_id(self):
        response = self.client.get(
            "/projects/platform-api",
            headers={"X-Request-ID": "req-project-001", "X-Test-User-ID": "devon"},
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            response.json(),
            {
                "project": {
                    "project_id": "platform-api",
                    "name": "Platform API",
                    "owner_team_id": "data-platform",
                    "created_by_user_id": "devon",
                    "description": "Service API surface",
                    "status": "planning",
                    "created_at": self.created_at["project:platform-api"],
                    "updated_at": self.updated_at["project:platform-api"],
                },
                "request_id": "req-project-001",
            },
        )

    def test_missing_project_maps_to_404(self):
        response = self.client.get("/projects/missing-project")

        self.assertEqual(response.status_code, 404)
        self.assertEqual(response.json(), {"detail": "Project not found"})

    def test_updates_project_with_patch_body(self):
        response = self.client.patch(
            "/projects/latent-skill-pilot",
            json={"name": "Latent Skill Evaluation", "status": "paused"},
            headers={"X-Request-ID": "req-update-project-001"},
        )

        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertEqual(
            body["project"],
            {
                "project_id": "latent-skill-pilot",
                "name": "Latent Skill Evaluation",
                "owner_team_id": "pilot-project",
                "created_by_user_id": "[REDACTED_OWNER]",
                "description": "SkillFlow and SkillLearnBench pilot",
                "status": "paused",
                "created_at": self.created_at["project:latent-skill-pilot"],
                "updated_at": body["project"]["updated_at"],
            },
        )
        self.assertEqual(body["request_id"], "req-update-project-001")
        self.assertTrue(body["project"]["updated_at"].endswith("Z"))

        with session_scope(self.engine) as session:
            events = AuditEventRepository(session).list_events(project_id="latent-skill-pilot")

        self.assertEqual([event.event_type for event in events], ["project.updated"])
        self.assertEqual(events[0].actor_user_id, "[REDACTED_OWNER]")
        self.assertEqual(events[0].request_id, "req-update-project-001")
        self.assertEqual(events[0].payload["status"], "paused")

    def _seed_records(self):
        self.created_at = {}
        self.updated_at = {}
        with session_scope(self.engine) as session:
            identities = IdentityRepository(session)
            projects = ProjectRepository(session)
            data_platform = identities.create_team(team_id="data-platform", name="Data Platform")
            latent_reasoning = identities.create_team(team_id="pilot-project", name="pilot group")
            identities.create_user(
                user_id="[REDACTED_OWNER]",
                email="[REDACTED_OWNER]@example.com",
                display_name="[REDACTED_OWNER]",
                team_id="pilot-project",
            )
            identities.create_user(
                user_id="devon",
                email="devon@example.com",
                display_name="Devon Park",
                team_id="data-platform",
            )
            latent_project = projects.create_project(
                project_id="latent-skill-pilot",
                name="Latent Skill Pilot",
                owner_team_id="pilot-project",
                created_by_user_id="[REDACTED_OWNER]",
                description="SkillFlow and SkillLearnBench pilot",
                status="active",
            )
            api_project = projects.create_project(
                project_id="platform-api",
                name="Platform API",
                owner_team_id="data-platform",
                created_by_user_id="devon",
                description="Service API surface",
                status="planning",
            )

        self.created_at["team:data-platform"] = _iso_z(data_platform.created_at)
        self.created_at["team:pilot-project"] = _iso_z(latent_reasoning.created_at)
        self.created_at["project:latent-skill-pilot"] = _iso_z(latent_project.created_at)
        self.updated_at["project:latent-skill-pilot"] = _iso_z(latent_project.updated_at)
        self.created_at["project:platform-api"] = _iso_z(api_project.created_at)
        self.updated_at["project:platform-api"] = _iso_z(api_project.updated_at)


def _app_for_engine(engine):
    app = FastAPI()

    @app.middleware("http")
    async def attach_request_id(request: Request, call_next):
        request.state.request_id = request.headers.get("X-Request-ID", "")
        request.state.authenticated_user_id = request.headers.get("X-Test-User-ID", "[REDACTED_OWNER]")
        return await call_next(request)

    def session_dependency():
        with session_scope(engine) as session:
            yield session

    register_project_routes(app, session_dependency)
    return app


def _iso_z(value):
    return value.isoformat().replace("+00:00", "Z")
