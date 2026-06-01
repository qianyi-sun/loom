import io
import time
import unittest
import zipfile
from types import SimpleNamespace

from agentic_data_platform.service.harbor_adapter_matrix_smoke import (
    HarborAdapterMatrixSmokeConfig,
    _run_create_payload,
    _select_models,
    _wait_for_launched_runs,
)


class HarborAdapterMatrixSmokeTest(unittest.TestCase):
    def test_select_models_picks_one_enabled_model_per_requested_family(self):
        payload = {
            "models": [
                _model("amap/maps", "other"),
                _model("deepseek-v4-flash", "deepseek"),
                _model("gpt-5-mini", "openai"),
                _model("claude-sonnet-4-6", "claude", disabled=True),
                _model("claude-sonnet-4-6-thinking", "claude"),
            ]
        }

        selected = _select_models(payload, model_ids=[], model_families=["openai", "deepseek", "claude"])

        self.assertEqual([model["model_id"] for model in selected], ["gpt-5-mini", "deepseek-v4-flash", "claude-sonnet-4-6-thinking"])

    def test_run_create_payload_uses_platform_api_model_and_harbor_agent(self):
        payload = _run_create_payload(
            HarborAdapterMatrixSmokeConfig(
                base_url="http://api:8000",
                username="[REDACTED_OWNER]",
                password="[REDACTED_PASSWORD]",
                per_run_timeout_seconds=123,
            ),
            project={"project_id": "latent-skill-pilot", "owner_team_id": "pilot-project"},
            harness={
                "harness_id": "harbor-local-docker",
                "runner_kind": "harbor",
                "sandbox_backend": "docker_terminal",
                "default_image": "python:3.12-slim",
                "internet_access": True,
                "resource_limits": {"timeout_seconds": 600},
                "metadata": {"runner_contract": "harbor-local-docker-v0"},
            },
            benchmark={"suite_name": "HarborTerminalBench", "benchmark_version": "2.0", "source_uri": "harbor://registry"},
            task={
                "task_family": "smoke",
                "instance_id": "noop",
                "metadata": {},
                "input_artifact_refs": [],
                "runner_entrypoint": ["python", "-c"],
            },
            agent_id="harbor:opencode",
            model=_model("deepseek-v4-flash", "deepseek"),
            run_id="matrix_001",
        )

        self.assertEqual(payload["model"]["model_name"], "deepseek-v4-flash")
        self.assertEqual(payload["model"]["provider_config_id"], "default-agent-model")
        harbor_run = payload["metadata"]["harbor_run"]
        self.assertEqual(harbor_run["agent"], "opencode")
        self.assertEqual(harbor_run["model_name"], "deepseek-v4-flash")
        self.assertEqual(harbor_run["task_template"], "harbor-cli-smoke")
        self.assertEqual(harbor_run["timeout_seconds"], 123)
        self.assertIn("--agent-timeout-multiplier", harbor_run["extra_args"])

    def test_wait_does_not_count_queue_time_against_per_run_timeout(self):
        client = _MatrixWaitClient(
            statuses=[
                {"run": {"run_id": "run1", "status": "queued", "progress": {"artifact_count": 0}}},
                {
                    "run": {
                        "run_id": "run1",
                        "status": "succeeded",
                        "progress": {"artifact_count": 3},
                        "evaluator": {"score": 0.0},
                    }
                },
            ]
        )

        results = _wait_for_launched_runs(
            HarborAdapterMatrixSmokeConfig(
                base_url="http://api:8000",
                username="[REDACTED_OWNER]",
                password="[REDACTED_PASSWORD]",
                per_run_timeout_seconds=1,
                timeout_seconds=30,
            ),
            client,
            pending_runs=[
                {
                    "agent_id": "harbor:codex",
                    "model_id": "gpt-5-nano",
                    "family": "openai",
                    "run_id": "run1",
                    "adapter_id": "codex-openai-compatible",
                    "launched_at": time.monotonic() - 1000,
                }
            ],
            headers={},
            sleep=lambda _: None,
        )

        self.assertEqual(results[0]["status"], "succeeded")
        self.assertEqual(results[0]["verifier_score"], 0.0)


def _model(model_id: str, family: str, *, disabled: bool = False):
    return {
        "provider": "dev-api-provider",
        "provider_config_id": "default-agent-model",
        "model_id": model_id,
        "metadata": {"family": family},
        "disabled": disabled,
    }


class _MatrixWaitClient:
    def __init__(self, *, statuses):
        self._statuses = list(statuses)

    def get(self, path, *, params=None, headers=None):
        if path == "/runs/run1":
            return SimpleNamespace(status_code=200, json=lambda: self._statuses.pop(0), text="")
        if path == "/runs/run1/artifact-bundle":
            return SimpleNamespace(status_code=200, content=_bundle_bytes(), text="")
        raise AssertionError(path)


def _bundle_bytes() -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        for name in ["manifest.json", "run.json", "trajectory.jsonl", "evaluation.json", "artifact-metadata.json"]:
            archive.writestr(name, "{}\n")
    return buffer.getvalue()


if __name__ == "__main__":
    unittest.main()
