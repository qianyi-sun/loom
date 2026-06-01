import unittest
from unittest.mock import patch

from agentic_data_platform.service.config import ServiceSettings
from agentic_data_platform.service.model_resources import discover_model_catalog


class ModelResourcesTest(unittest.TestCase):
    def test_provider_discovery_is_default_when_provider_credentials_are_configured(self):
        settings = _settings(model_provider_models="")

        with patch(
            "agentic_data_platform.service.model_resources._fetch_openai_compatible_models",
            return_value=["cheap-terminal-model", "larger-terminal-model"],
        ) as fetch_models:
            payload = discover_model_catalog(settings)

        fetch_models.assert_called_once_with(
            base_url="https://models.example/v1",
            api_key="sk-model-secret",
        )
        self.assertEqual([model["model_id"] for model in payload["models"]], ["cheap-terminal-model", "larger-terminal-model"])
        self.assertEqual(payload["models"][0]["source"], "openai_compatible_discovery")
        self.assertEqual(payload["models"][0]["metadata"]["family"], "other")
        self.assertEqual(payload["models"][0]["metadata"]["endpoint_dialects"], ["openai_compatible"])
        self.assertEqual(payload["catalog"]["status"], "discovered")
        self.assertEqual(payload["errors"], [])

    def test_discovered_models_include_family_metadata_for_mainstream_models(self):
        settings = _settings(model_provider_models="")

        with patch(
            "agentic_data_platform.service.model_resources._fetch_openai_compatible_models",
            return_value=["deepseek-v4-flash", "claude-3-5-sonnet", "gemini-2.5-flash", "qwen3-coder"],
        ):
            payload = discover_model_catalog(settings)

        families = {model["model_id"]: model["metadata"]["family"] for model in payload["models"]}
        self.assertEqual(families["deepseek-v4-flash"], "deepseek")
        self.assertEqual(families["claude-3-5-sonnet"], "claude")
        self.assertEqual(families["gemini-2.5-flash"], "gemini")
        self.assertEqual(families["qwen3-coder"], "qwen")
        self.assertTrue(all(model["metadata"]["agent_capable"] for model in payload["models"]))

    def test_static_models_filter_discovered_provider_models_without_requiring_full_manual_list(self):
        settings = _settings(model_provider_models="cheap-terminal-model")

        with patch(
            "agentic_data_platform.service.model_resources._fetch_openai_compatible_models",
            return_value=["cheap-terminal-model", "larger-terminal-model"],
        ):
            payload = discover_model_catalog(settings)

        self.assertEqual([model["model_id"] for model in payload["models"]], ["cheap-terminal-model"])
        self.assertEqual(payload["models"][0]["source"], "openai_compatible_discovery_allowlist")
        self.assertEqual(payload["catalog"]["status"], "discovered_allowlisted")

    def test_static_models_are_fallback_when_provider_discovery_fails(self):
        settings = _settings(model_provider_models="cheap-terminal-model")

        with patch(
            "agentic_data_platform.service.model_resources._fetch_openai_compatible_models",
            side_effect=RuntimeError("provider unavailable"),
        ):
            payload = discover_model_catalog(settings)

        self.assertEqual([model["model_id"] for model in payload["models"]], ["cheap-terminal-model"])
        self.assertEqual(payload["models"][0]["source"], "static_config_fallback")
        self.assertFalse(payload["models"][0]["disabled"])
        self.assertEqual(payload["catalog"]["status"], "fallback_static_config")
        self.assertEqual(payload["errors"], [{"provider_config_id": "default-agent-model", "message": "provider unavailable"}])


def _settings(*, model_provider_models: str) -> ServiceSettings:
    return ServiceSettings(
        app_name="agentic-data-platform-test",
        environment="test",
        database_url="",
        redis_url="",
        object_storage_endpoint="",
        object_storage_bucket="",
        object_storage_access_key="",
        object_storage_secret_key="",
        object_storage_region="us-east-1",
        model_provider_base_url="https://models.example/v1",
        model_provider_api_key="sk-model-secret",
        model_provider_models=model_provider_models,
    )


if __name__ == "__main__":
    unittest.main()
