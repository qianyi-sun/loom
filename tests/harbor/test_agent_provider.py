import json
import unittest

from agentic_data_platform.harbor.agent_provider import HarborAgentProvider


class HarborAgentProviderTest(unittest.TestCase):
    def test_lists_builtin_harbor_agents_with_execution_metadata(self):
        agents = {agent.agent_id: agent for agent in HarborAgentProvider().list_agents()}

        oracle = agents["harbor:oracle"]
        self.assertEqual(oracle.provider, "harbor")
        self.assertEqual(oracle.source, "harbor_builtin")
        self.assertEqual(oracle.execution_mode, "harbor_builtin")
        self.assertEqual(oracle.supported_harness_ids, ["harbor-local-docker"])
        self.assertEqual(oracle.supported_sandbox_backends, ["docker_terminal"])
        self.assertTrue(oracle.supports_trajectory)
        self.assertEqual(oracle.required_secret_refs, [])
        self.assertEqual(oracle.metadata["harbor_agent_name"], "oracle")
        self.assertEqual(oracle.metadata["harbor_cli_args"], ["--agent", "oracle"])

        codex = agents["harbor:codex"]
        self.assertEqual(codex.execution_mode, "external_cli")
        self.assertEqual(codex.required_secret_refs, ["env:OPENAI_API_KEY"])
        self.assertEqual(codex.metadata["model_adapter"]["adapter_id"], "codex-openai-compatible")
        self.assertIn("terminal-agent", codex.capabilities)
        self.assertIn("harbor-trial-events", codex.capabilities)

        opencode = agents["harbor:opencode"]
        self.assertEqual(opencode.required_secret_refs, ["env:OPENAI_API_KEY"])
        self.assertEqual(opencode.metadata["model_adapter"]["adapter_id"], "opencode-openai-compatible")

        claude_code = agents["harbor:claude-code"]
        self.assertEqual(claude_code.required_secret_refs, ["env:ANTHROPIC_API_KEY"])
        self.assertEqual(claude_code.metadata["model_adapter"]["adapter_id"], "anthropic-cli")

        gemini_cli = agents["harbor:gemini-cli"]
        self.assertEqual(gemini_cli.required_secret_refs, ["env:GOOGLE_API_KEY", "env:GEMINI_API_KEY"])
        self.assertEqual(gemini_cli.metadata["model_adapter"]["adapter_id"], "gemini-cli")

        rendered = json.dumps([agent.to_payload() for agent in agents.values()], sort_keys=True)
        self.assertNotIn("sk-", rendered)
        self.assertNotIn("OPENAI_API_KEY=", rendered)

    def test_builds_custom_agent_import_path_entry_without_raw_secrets(self):
        agent = HarborAgentProvider().agent_for_import_path(
            "research_agents.skillflow:SkillFlowAgent",
            display_name="Latent SkillFlow Agent",
            required_secret_refs=["env:LATENT_AGENT_API_KEY"],
        )

        self.assertEqual(agent.agent_id, "harbor-custom:research_agents.skillflow:SkillFlowAgent")
        self.assertEqual(agent.display_name, "Latent SkillFlow Agent")
        self.assertEqual(agent.provider, "harbor")
        self.assertEqual(agent.source, "harbor_custom_import")
        self.assertEqual(agent.execution_mode, "custom_import")
        self.assertEqual(agent.required_secret_refs, ["env:LATENT_AGENT_API_KEY"])
        self.assertEqual(agent.supported_harness_ids, ["harbor-local-docker"])
        self.assertEqual(agent.metadata["harbor_agent_import_path"], "research_agents.skillflow:SkillFlowAgent")
        self.assertEqual(
            agent.metadata["harbor_cli_args"],
            ["--agent-import-path", "research_agents.skillflow:SkillFlowAgent"],
        )
        rendered = json.dumps(agent.to_payload(), sort_keys=True)
        self.assertNotIn("LATENT_AGENT_API_KEY=", rendered)

    def test_rejects_malformed_custom_agent_import_paths(self):
        provider = HarborAgentProvider()

        for value in ["research_agents.skillflow", "research agents:Agent", "/tmp/agent.py:Agent", "pkg:"]:
            with self.subTest(value=value):
                with self.assertRaisesRegex(ValueError, "agent_import_path"):
                    provider.agent_for_import_path(value)


if __name__ == "__main__":
    unittest.main()
