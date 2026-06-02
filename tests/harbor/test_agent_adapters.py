import unittest

from agentic_data_platform.harbor.agent_adapters import (
    build_agent_model_env,
    build_agent_model_invocation,
    mainstream_adapter_specs,
    provider_dialect_gap,
    provider_endpoint_dialects,
)
from agentic_data_platform.providers.config import ProviderConfigRef, ProviderRole, ProviderSecret


class HarborAgentAdapterTest(unittest.TestCase):
    def test_opencode_deepseek_uses_openai_compatible_adapter_env(self):
        env_values = build_agent_model_env(
            agent_name="opencode",
            provider_ref=_provider_ref(base_url="https://models.example/v1"),
            provider_secret=ProviderSecret("deepseek-secret"),
            existing_agent_env=[],
        )

        self.assertIn("OPENAI_API_KEY=deepseek-secret", env_values)
        self.assertIn("OPENAI_BASE_URL=https://models.example/v1", env_values)
        self.assertIn("OPENAI_API_BASE=https://models.example/v1", env_values)

    def test_provider_native_agents_map_to_their_cli_secret_names(self):
        claude_env = build_agent_model_env(
            agent_name="claude-code",
            provider_ref=_provider_ref(base_url="https://anthropic.example/v1"),
            provider_secret=ProviderSecret("anthropic-secret"),
            existing_agent_env=[],
        )
        gemini_env = build_agent_model_env(
            agent_name="gemini-cli",
            provider_ref=_provider_ref(base_url="https://gemini.example/v1"),
            provider_secret=ProviderSecret("gemini-secret"),
            existing_agent_env=[],
        )

        self.assertIn("ANTHROPIC_API_KEY=anthropic-secret", claude_env)
        self.assertIn("ANTHROPIC_BASE_URL=https://anthropic.example", claude_env)
        self.assertIn("GOOGLE_API_KEY=gemini-secret", gemini_env)
        self.assertIn("GEMINI_API_KEY=gemini-secret", gemini_env)

    def test_mainstream_specs_cover_required_agent_baseline(self):
        specs = mainstream_adapter_specs()

        for agent_name in [
            "codex",
            "opencode",
            "claude-code",
            "gemini-cli",
            "qwen-coder",
            "aider",
            "openhands",
            "openhands-sdk",
            "swe-agent",
            "mini-swe-agent",
        ]:
            with self.subTest(agent_name=agent_name):
                self.assertIn(agent_name, specs)
                self.assertTrue(specs[agent_name].required_secret_refs)

        self.assertEqual(specs["codex"].endpoint_dialects, ("openai_responses",))

    def test_provider_dialect_gap_blocks_responses_only_agents_on_chat_only_provider(self):
        specs = mainstream_adapter_specs()
        chat_only_ref = _provider_ref(base_url="https://models.example/v1")
        openai_ref = _provider_ref(base_url="https://api.openai.com/v1")

        self.assertEqual(provider_endpoint_dialects(chat_only_ref), {"openai_compatible"})
        self.assertIn(
            "openai_responses",
            provider_dialect_gap(adapter=specs["codex"], provider_ref=chat_only_ref),
        )
        self.assertIsNone(provider_dialect_gap(adapter=specs["codex"], provider_ref=openai_ref))

    def test_mainstream_agent_model_pairs_build_real_invocation_contracts(self):
        mainstream_agents = [
            "codex",
            "opencode",
            "claude-code",
            "gemini-cli",
            "qwen-coder",
            "aider",
            "openhands",
            "openhands-sdk",
            "swe-agent",
            "mini-swe-agent",
            "kimi-cli",
        ]
        mainstream_models = [
            "gpt-5-mini",
            "deepseek-v4-flash",
            "claude-sonnet-4-6",
            "gemini-2.5-flash",
            "qwen3-coder-plus",
            "kimi-k2",
            "glm-4.7",
            "grok-4-fast",
            "minimax-m2",
        ]

        for agent_name in mainstream_agents:
            for model_id in mainstream_models:
                with self.subTest(agent_name=agent_name, model_id=model_id):
                    invocation = build_agent_model_invocation(
                        agent_name=agent_name,
                        model_id=model_id,
                        provider_ref=_provider_ref(base_url="https://models.example/v1"),
                        provider_secret=ProviderSecret("model-secret"),
                        existing_agent_env=[],
                    )

                    self.assertTrue(invocation.harbor_model_name)
                    self.assertTrue(invocation.agent_env)
                    self.assertTrue(invocation.process_env)

        opencode = build_agent_model_invocation(
            agent_name="opencode",
            model_id="deepseek-v4-flash",
            provider_ref=_provider_ref(base_url="https://models.example/v1"),
            provider_secret=ProviderSecret("model-secret"),
            existing_agent_env=[],
        )
        self.assertEqual(opencode.harbor_model_name, "openai/deepseek-v4-flash")
        self.assertIn("OPENAI_BASE_URL=https://models.example/v1", opencode.process_env)

        claude = build_agent_model_invocation(
            agent_name="claude-code",
            model_id="deepseek-v4-flash",
            provider_ref=_provider_ref(base_url="https://models.example/v1"),
            provider_secret=ProviderSecret("model-secret"),
            existing_agent_env=[],
        )
        self.assertEqual(claude.harbor_model_name, "deepseek-v4-flash")
        self.assertIn("ANTHROPIC_BASE_URL=https://models.example", claude.process_env)

        gemini = build_agent_model_invocation(
            agent_name="gemini-cli",
            model_id="deepseek-v4-flash",
            provider_ref=_provider_ref(base_url="https://models.example/v1"),
            provider_secret=ProviderSecret("model-secret"),
            existing_agent_env=[],
        )
        self.assertEqual(gemini.harbor_model_name, "google/deepseek-v4-flash")
        self.assertIn("GOOGLE_GEMINI_BASE_URL=https://models.example/v1beta", gemini.process_env)

    def test_openhands_cli_pins_harbor_compatible_openhands_ai_version(self):
        openhands = build_agent_model_invocation(
            agent_name="openhands",
            model_id="deepseek-v4-flash",
            provider_ref=_provider_ref(base_url="https://models.example/v1"),
            provider_secret=ProviderSecret("model-secret"),
            existing_agent_env=[],
        )
        openhands_sdk = build_agent_model_invocation(
            agent_name="openhands-sdk",
            model_id="deepseek-v4-flash",
            provider_ref=_provider_ref(base_url="https://models.example/v1"),
            provider_secret=ProviderSecret("model-secret"),
            existing_agent_env=[],
        )

        self.assertIn("version=1.6.0", openhands.agent_kwargs)
        self.assertNotIn("version=1.6.0", openhands_sdk.agent_kwargs)


def _provider_ref(*, base_url: str) -> ProviderConfigRef:
    return ProviderConfigRef(
        config_id="default-agent-model",
        role=ProviderRole.AGENT_MODEL,
        provider="dev-api-provider",
        model_name="configured-agent-model",
        base_url=base_url,
        secret_ref="env:MODEL_PROVIDER_API_KEY",
    )


if __name__ == "__main__":
    unittest.main()
