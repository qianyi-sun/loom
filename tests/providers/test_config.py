import json
import unittest

from agentic_data_platform.providers.config import (
    DevProviderConfigRegistry,
    ProviderConfigRef,
    ProviderRole,
    redact_sensitive_metadata,
)
from agentic_data_platform.providers.errors import ProviderErrorCode, normalize_provider_error
from agentic_data_platform.service.config import ServiceSettings


class ProviderConfigTest(unittest.TestCase):
    def test_dev_registry_exposes_safe_refs_and_resolves_env_secrets(self):
        settings = ServiceSettings(
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
            evaluator_provider_base_url="https://judge.example/v1",
            evaluator_provider_api_key="sk-evaluator-secret",
        )

        registry = DevProviderConfigRegistry.from_settings(settings)
        model_ref = registry.get("default-agent-model")
        evaluator_ref = registry.get("default-evaluator-model")

        self.assertEqual(model_ref.role, ProviderRole.AGENT_MODEL)
        self.assertEqual(model_ref.secret_ref, "env:MODEL_PROVIDER_API_KEY")
        self.assertEqual(evaluator_ref.role, ProviderRole.EVALUATOR_MODEL)
        self.assertEqual(evaluator_ref.secret_ref, "env:EVALUATOR_PROVIDER_API_KEY")
        self.assertEqual(registry.resolve_secret(model_ref.secret_ref).value, "sk-model-secret")
        self.assertEqual(registry.resolve_secret(evaluator_ref.secret_ref).redacted, "********")
        self.assertNotIn("sk-model-secret", json.dumps(model_ref.to_safe_dict()))
        self.assertNotIn("sk-evaluator-secret", json.dumps(evaluator_ref.to_safe_dict()))

    def test_provider_config_ref_rejects_raw_secret_refs(self):
        with self.assertRaisesRegex(ValueError, "secret_ref must use env:"):
            ProviderConfigRef(
                config_id="bad",
                role=ProviderRole.AGENT_MODEL,
                provider="openai",
                model_name="gpt-5",
                secret_ref="sk-raw-secret",
            )

    def test_redacts_nested_sensitive_metadata_but_preserves_secret_refs(self):
        redacted = redact_sensitive_metadata(
            {
                "temperature": 0,
                "api_key": "sk-raw",
                "nested": {"authorization": "Bearer secret", "secret_ref": "env:MODEL_PROVIDER_API_KEY"},
                "tokens": ["token-a", {"password": "pw"}],
            }
        )

        rendered = json.dumps(redacted)
        self.assertNotIn("sk-raw", rendered)
        self.assertNotIn("Bearer secret", rendered)
        self.assertNotIn("token-a", rendered)
        self.assertNotIn("pw", rendered)
        self.assertEqual(redacted["api_key"], "[redacted]")
        self.assertEqual(redacted["nested"]["secret_ref"], "env:MODEL_PROVIDER_API_KEY")
        self.assertEqual(redacted["temperature"], 0)

    def test_normalizes_provider_errors_for_api_boundaries(self):
        self.assertEqual(normalize_provider_error(Exception("status 429 rate limited")).code, ProviderErrorCode.RATE_LIMITED)
        self.assertEqual(normalize_provider_error(PermissionError("401 unauthorized")).code, ProviderErrorCode.AUTH_FAILED)
        self.assertEqual(normalize_provider_error(TimeoutError("request timed out")).code, ProviderErrorCode.TIMEOUT)
        self.assertEqual(normalize_provider_error(ValueError("bad model")).code, ProviderErrorCode.INVALID_REQUEST)
