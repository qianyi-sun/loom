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

    def test_agent_model_adaptation_preflight_reports_ready_env_contract_without_secrets(self):
        client = TestClient(
            _app(
                self.engine,
                model_provider_base_url="https://models.example/v1",
                model_provider_api_key="deepseek-secret",
            )
        )

        response = client.get(
            "/harbor/agent-adaptation",
            params={
                "project_id": "latent-skill-pilot",
                "harness_id": "harbor-local-docker",
                "agent_id": "harbor:opencode",
                "model_id": "deepseek-v4-flash",
                "provider_config_id": "default-agent-model",
            },
            headers={"Authorization": "Bearer [REDACTED_TOKEN]", "X-Request-ID": "req-agent-adapt-001"},
        )

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["request_id"], "req-agent-adapt-001")
        self.assertEqual(payload["status"], "ready")
        self.assertEqual(payload["adapter"]["adapter_id"], "opencode-openai-compatible")
        self.assertEqual(payload["required_secret_refs"], ["env:OPENAI_API_KEY"])
        env_sources = {item["name"]: item["source"] for item in payload["env_preview"]}
        self.assertEqual(env_sources["OPENAI_API_KEY"], "env:MODEL_PROVIDER_API_KEY")
        self.assertEqual(env_sources["OPENAI_BASE_URL"], "provider_base_url")
        self.assertEqual(env_sources["OPENAI_API_BASE"], "provider_base_url")
        rendered = json.dumps(payload, sort_keys=True)
        self.assertNotIn("deepseek-secret", rendered)

    def test_openhands_cli_preflight_reports_adapter_default_version_kwarg(self):
        client = TestClient(
            _app(
                self.engine,
                model_provider_base_url="https://models.example/v1",
                model_provider_api_key="deepseek-secret",
            )
        )

        response = client.get(
            "/harbor/agent-adaptation",
            params={
                "project_id": "latent-skill-pilot",
                "harness_id": "harbor-local-docker",
                "agent_id": "harbor:openhands",
                "model_id": "deepseek-v4-flash",
                "provider_config_id": "default-agent-model",
            },
            headers={"Authorization": "Bearer [REDACTED_TOKEN]"},
        )

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["status"], "ready")
        self.assertEqual(payload["adapter"]["adapter_id"], "openhands-openai-compatible")
        self.assertIn("version=1.6.0", payload["adapter"]["default_agent_kwargs"])
        self.assertIn({"name": "version", "source": "adapter_default"}, payload["agent_kwargs_preview"])

        sdk_response = client.get(
            "/harbor/agent-adaptation",
            params={
                "project_id": "latent-skill-pilot",
                "harness_id": "harbor-local-docker",
                "agent_id": "harbor:openhands-sdk",
                "model_id": "deepseek-v4-flash",
                "provider_config_id": "default-agent-model",
            },
            headers={"Authorization": "Bearer [REDACTED_TOKEN]"},
        )

        self.assertEqual(sdk_response.status_code, 200)
        sdk_payload = sdk_response.json()
        self.assertEqual(sdk_payload["adapter"]["adapter_id"], "openhands-sdk-openai-compatible")
        self.assertEqual(sdk_payload["agent_kwargs_preview"], [])

    def test_agent_model_adaptation_preflight_reports_missing_provider_config(self):
        response = self.client.get(
            "/harbor/agent-adaptation",
            params={
                "project_id": "latent-skill-pilot",
                "harness_id": "harbor-local-docker",
                "agent_id": "harbor:opencode",
                "model_id": "deepseek-v4-flash",
                "provider_config_id": "default-agent-model",
            },
            headers={"Authorization": "Bearer [REDACTED_TOKEN]"},
        )

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["status"], "blocked")
        self.assertEqual(payload["gaps"][0]["code"], "missing_provider_config")

    def test_agent_model_adaptation_preflight_blocks_unadapted_external_agent(self):
        client = TestClient(
            _app(
                self.engine,
                model_provider_base_url="https://models.example/v1",
                model_provider_api_key="model-secret",
            )
        )

        response = client.get(
            "/harbor/agent-adaptation",
            params={
                "project_id": "latent-skill-pilot",
                "harness_id": "harbor-local-docker",
                "agent_id": "harbor:cline-cli",
                "model_id": "deepseek-v4-flash",
                "provider_config_id": "default-agent-model",
            },
            headers={"Authorization": "Bearer [REDACTED_TOKEN]"},
        )

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["status"], "blocked")
        self.assertEqual(payload["gaps"][0]["code"], "missing_agent_model_adapter")

    def test_agent_model_adaptation_preflight_rejects_non_agent_model_provider_role(self):
        client = TestClient(
            _app(
                self.engine,
                model_provider_base_url="https://models.example/v1",
                model_provider_api_key="model-secret",
                evaluator_provider_base_url="https://judge.example/v1",
                evaluator_provider_api_key="judge-secret",
            )
        )

        response = client.get(
            "/harbor/agent-adaptation",
            params={
                "project_id": "latent-skill-pilot",
                "harness_id": "harbor-local-docker",
                "agent_id": "harbor:opencode",
                "model_id": "deepseek-v4-flash",
                "provider_config_id": "default-evaluator-model",
            },
            headers={"Authorization": "Bearer [REDACTED_TOKEN]"},
        )

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["status"], "blocked")
        self.assertEqual(payload["gaps"][0]["code"], "invalid_provider_role")

    def test_agent_model_adaptation_preflight_blocks_provider_dialect_mismatch(self):
        client = TestClient(
            _app(
                self.engine,
                model_provider_base_url="https://models.example/v1",
                model_provider_api_key="model-secret",
            )
        )

        response = client.get(
            "/harbor/agent-adaptation",
            params={
                "project_id": "latent-skill-pilot",
                "harness_id": "harbor-local-docker",
                "agent_id": "harbor:codex",
                "model_id": "deepseek-v4-flash",
                "provider_config_id": "default-agent-model",
            },
            headers={"Authorization": "Bearer [REDACTED_TOKEN]"},
        )

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["status"], "blocked")
        self.assertEqual(payload["gaps"][0]["code"], "provider_dialect_mismatch")
        self.assertIn("openai_responses", payload["gaps"][0]["message"])

    def test_agent_model_adaptation_preflight_reports_adapted_model_and_process_env(self):
        client = TestClient(
            _app(
                self.engine,
                model_provider_base_url="https://anthropic.example/v1",
                model_provider_api_key="deepseek-secret",
            )
        )

        response = client.get(
            "/harbor/agent-adaptation",
            params={
                "project_id": "latent-skill-pilot",
                "harness_id": "harbor-local-docker",
                "agent_id": "harbor:claude-code",
                "model_id": "deepseek-v4-flash",
                "provider_config_id": "default-agent-model",
            },
            headers={"Authorization": "Bearer [REDACTED_TOKEN]"},
        )

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["status"], "ready")
        self.assertEqual(payload["harbor_model_name"], "deepseek-v4-flash")
        process_env = {item["name"]: item["source"] for item in payload["process_env_preview"]}
        self.assertEqual(process_env["ANTHROPIC_API_KEY"], "env:MODEL_PROVIDER_API_KEY")
        self.assertEqual(process_env["ANTHROPIC_BASE_URL"], "provider_base_url")
        self.assertNotIn("deepseek-secret", json.dumps(payload, sort_keys=True))


def _app(
    engine,
    *,
    model_provider_base_url: str = "",
    model_provider_api_key: str = "",
    evaluator_provider_base_url: str = "",
    evaluator_provider_api_key: str = "",
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
            model_provider_base_url=model_provider_base_url,
            model_provider_api_key=model_provider_api_key,
            evaluator_provider_base_url=evaluator_provider_base_url,
            evaluator_provider_api_key=evaluator_provider_api_key,
            model_provider_models="gpt-5,gpt-5-mini",
            internal_auth_tokens="[REDACTED_OWNER]=[REDACTED_TOKEN]",
            web_login_credentials="[REDACTED_OWNER]=[REDACTED_PASSWORD]:[REDACTED_OWNER]",
            web_session_secret="test-session-secret",
        ),
        database_engine=engine,
    )


if __name__ == "__main__":
    unittest.main()
