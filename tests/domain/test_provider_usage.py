import json
import math
import unittest

from agentic_data_platform.domain.provider_usage import normalize_model_provider_usage


class ProviderUsageTest(unittest.TestCase):
    def test_normalizes_common_provider_usage_aliases_to_safe_schema(self):
        usage = normalize_model_provider_usage(
            {
                "n_input_tokens": 1200,
                "n_output_tokens": 345,
                "total_cost_usd": 0.03125,
                "latency_ms": 2500,
                "api_key": "sk-secret",
            },
            source="harbor_atif_final_metrics",
            provider="openai-compatible",
            model_name="deepseek-v4-flash",
        )

        self.assertEqual(
            usage,
            {
                "schema_version": "model-provider-usage-v1",
                "source": "harbor_atif_final_metrics",
                "provider": "openai-compatible",
                "model_name": "deepseek-v4-flash",
                "input_tokens": 1200,
                "output_tokens": 345,
                "total_tokens": 1545,
                "cost_usd": 0.03125,
                "duration_seconds": 2.5,
            },
        )
        self.assertNotIn("sk-secret", json.dumps(usage))

    def test_ignores_non_finite_negative_and_unknown_metrics(self):
        usage = normalize_model_provider_usage(
            {
                "prompt_tokens": -10,
                "completion_tokens": math.inf,
                "total_tokens": "nan",
                "cost_usd": -0.1,
                "authorization": "Bearer secret",
            },
            source="harbor_trial_final_metrics",
            provider="",
            model_name=None,
        )

        self.assertIsNone(usage)
