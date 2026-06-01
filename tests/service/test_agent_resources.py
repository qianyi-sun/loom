import json
import unittest

from fastapi.testclient import TestClient
from sqlalchemy.pool import StaticPool

from agentic_data_platform.persistence import (
    IdentityRepository,
    ProjectRepository,
    create_database_engine,
    session_scope,
)
from agentic_data_platform.persistence.migrations import upgrade_database
from agentic_data_platform.service.app import create_app
from agentic_data_platform.service.config import ServiceSettings


class AgentResourcesTest(unittest.TestCase):
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
        self.client = TestClient(_app(self.engine))

    def tearDown(self):
        self.engine.dispose()

    def test_agent_catalog_requires_auth_and_exposes_harbor_agents(self):
        unauthorized = self.client.get("/agents")
        self.assertEqual(unauthorized.status_code, 401)

        response = self.client.get(
            "/agents",
            headers={"Authorization": "Bearer [REDACTED_TOKEN]", "X-Request-ID": "req-agents-001"},
        )

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["request_id"], "req-agents-001")
        self.assertEqual(payload["errors"], [])
        agents = {agent["agent_id"]: agent for agent in payload["agents"]}
        self.assertEqual(agents["harbor:oracle"]["runner_kind"], "harbor")
        self.assertEqual(agents["harbor:oracle"]["source"], "harbor_builtin")
        self.assertEqual(agents["harbor:oracle"]["metadata"]["harbor_agent_name"], "oracle")
        self.assertEqual(agents["harbor:codex"]["required_secret_refs"], ["env:OPENAI_API_KEY"])
        self.assertTrue(agents["harbor:codex"]["supports_trajectory"])
        rendered = json.dumps(payload, sort_keys=True)
        self.assertNotIn("sk-", rendered)
        self.assertNotIn("OPENAI_API_KEY=", rendered)

    def test_agent_catalog_supports_harness_filter_and_custom_import_path(self):
        response = self.client.get(
            "/agents",
            params={
                "project_id": "latent-skill-pilot",
                "harness_id": "harbor-local-docker",
                "agent_import_path": "research_agents.skillflow:SkillFlowAgent",
            },
            headers={"Authorization": "Bearer [REDACTED_TOKEN]", "X-Request-ID": "req-agents-custom-001"},
        )

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertIn("checked_at", payload)
        agent = next(item for item in payload["agents"] if item["source"] == "harbor_custom_import")
        self.assertEqual(agent["agent_id"], "harbor-custom:research_agents.skillflow:SkillFlowAgent")
        self.assertEqual(agent["metadata"]["harbor_agent_import_path"], "research_agents.skillflow:SkillFlowAgent")
        self.assertEqual(
            agent["metadata"]["harbor_cli_args"],
            ["--agent-import-path", "research_agents.skillflow:SkillFlowAgent"],
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
            model_provider_models="gpt-5,gpt-5-mini",
            internal_auth_tokens="[REDACTED_OWNER]=[REDACTED_TOKEN]",
            web_login_credentials="[REDACTED_OWNER]=[REDACTED_PASSWORD]:[REDACTED_OWNER]",
            web_session_secret="test-session-secret",
        ),
        database_engine=engine,
    )


if __name__ == "__main__":
    unittest.main()
