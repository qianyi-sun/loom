import json
import shutil
import subprocess
import textwrap
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
APP_JS = ROOT / "src" / "agentic_data_platform" / "frontend" / "static" / "app.js"


class FrontendStaticLaunchTest(unittest.TestCase):
    def test_harbor_launch_payload_uses_selected_agent_model_and_task_catalog_ref(self):
        node = shutil.which("node")
        if node is None:
            self.skipTest("node is required for static frontend payload tests")

        script = _node_harness(
            """
            const payload = buildRunPayload({
              runId: "frontend_real_harbor_001",
              project: { project_id: "pilot-project", name: "pilot group", owner_team_id: "pilot-project" },
              model: {
                provider: "dev-api-provider",
                provider_config_id: "default-agent-model",
                model_id: "gpt-5-mini",
              },
              harness: {
                harness_id: "harbor-local-docker",
                runner_kind: "original_benchmark",
                sandbox_backend: "docker_terminal",
                default_image: "python:3.12-slim",
                internet_access: true,
                resource_limits: { cpu: 1, memory_mb: 512, timeout_seconds: 600 },
                metadata: {
                  harbor_compatible: true,
                  runner_contract: "harbor-local-docker-v0",
                  harbor_task_template: "harbor-cli-smoke",
                  harbor_agent: "oracle",
                  harbor_model_name: "smoke/noop",
                  harbor_environment: "docker",
                  harbor_extra_args: ["--n-tasks", "1", "--quiet"],
                },
              },
              benchmark: {
                suite_name: "Harbor:terminal-bench",
                benchmark_version: "harbor:terminal-bench@2.0",
                source_uri: "harbor://datasets/terminal-bench@2.0",
              },
              task: {
                task_family: "terminal-bench",
                instance_id: "registry-dataset",
                instruction_ref: "harbor://datasets/terminal-bench@2.0",
                input_artifact_refs: ["harbor://datasets/terminal-bench@2.0"],
                required_artifacts: ["trajectory", "workspace_snapshot", "evaluator_report", "harbor_jobs_archive"],
                metadata: {
                  instruction: "Run one Terminal-Bench 2.0 task.",
                  harbor_run: {
                    backend: "cli",
                    dataset_ref: "terminal-bench@2.0",
                    environment: "docker",
                    extra_args: ["--n-tasks", "1", "--quiet"],
                  },
                },
              },
              agent: {
                agent_id: "harbor:codex",
                display_name: "Codex",
                required_secret_refs: ["env:OPENAI_API_KEY"],
                metadata: {
                  harbor_agent_name: "codex",
                  harbor_cli_args: ["--agent", "codex"],
                },
              },
            });
            console.log(JSON.stringify(payload));
            """
        )

        result = subprocess.run([node, "-e", script], check=True, text=True, capture_output=True)
        payload = json.loads(result.stdout)

        harbor_run = payload["metadata"]["harbor_run"]
        self.assertEqual(harbor_run["dataset_ref"], "terminal-bench@2.0")
        self.assertEqual(harbor_run["agent"], "codex")
        self.assertEqual(harbor_run["model_name"], "gpt-5-mini")
        self.assertEqual(harbor_run["agent_required_secret_refs"], ["env:OPENAI_API_KEY"])
        self.assertNotIn("task_template", harbor_run)
        self.assertEqual(payload["model"]["model_name"], "gpt-5-mini")
        self.assertEqual(payload["model"]["provider_config_id"], "default-agent-model")
        self.assertEqual(payload["evaluators"], [{"evaluator_id": "harbor-verifier", "mode": "harbor_verifier"}])

    def test_harbor_smoke_payload_preserves_no_key_oracle_model(self):
        node = shutil.which("node")
        if node is None:
            self.skipTest("node is required for static frontend payload tests")

        script = _node_harness(
            """
            const payload = buildRunPayload({
              runId: "frontend_smoke_001",
              project: { project_id: "pilot-project", name: "pilot group", owner_team_id: "pilot-project" },
              model: { provider: "mock-api", model_id: "scripted-terminal-agent" },
              harness: {
                harness_id: "harbor-local-docker",
                runner_kind: "original_benchmark",
                sandbox_backend: "docker_terminal",
                default_image: "python:3.12-slim",
                internet_access: true,
                resource_limits: { cpu: 1, memory_mb: 512, timeout_seconds: 60 },
                metadata: {
                  harbor_compatible: true,
                  runner_contract: "harbor-local-docker-v0",
                  harbor_task_template: "harbor-cli-smoke",
                  harbor_agent: "oracle",
                  harbor_model_name: "smoke/noop",
                  harbor_environment: "docker",
                  harbor_extra_args: ["--n-tasks", "1", "--quiet"],
                },
              },
              benchmark: { suite_name: "SkillLearnBench", benchmark_version: "git:test", source_uri: "git://test" },
              task: {
                task_family: "organize-messy-files",
                instance_id: "organize-messy-files-1",
                instruction_ref: "tasks/organize-messy-files/instruction.md",
                metadata: { instruction: "Smoke only." },
              },
              agent: {
                agent_id: "harbor:oracle",
                display_name: "Oracle",
                required_secret_refs: [],
                metadata: { harbor_agent_name: "oracle" },
              },
            });
            console.log(JSON.stringify(payload));
            """
        )

        result = subprocess.run([node, "-e", script], check=True, text=True, capture_output=True)
        payload = json.loads(result.stdout)

        self.assertEqual(
            payload["metadata"]["harbor_run"],
            {
                "task_template": "harbor-cli-smoke",
                "agent": "oracle",
                "model_name": "smoke/noop",
                "environment": "docker",
                "timeout_seconds": 60,
                "extra_args": ["--n-tasks", "1", "--quiet"],
            },
        )

    def test_model_catalog_status_message_reports_discovery_fallback_and_error_state(self):
        node = shutil.which("node")
        if node is None:
            self.skipTest("node is required for static frontend payload tests")

        script = _node_harness(
            """
            const messages = [
              modelCatalogMessage({
                catalog: { status: "discovered", source: "openai_compatible_discovery" },
                models: [{ model_id: "cheap-terminal-model" }],
                errors: [],
              }),
              modelCatalogMessage({
                catalog: { status: "fallback_static_config", source: "static_config_fallback" },
                models: [{ model_id: "cheap-terminal-model" }],
                errors: [{ message: "provider unavailable" }],
              }),
              modelCatalogMessage({
                catalog: { status: "discovery_failed", source: "openai_compatible_discovery" },
                models: [{ model_id: "configured-agent-model", disabled: true }],
                errors: [{ message: "401 unauthorized" }],
              }),
            ];
            console.log(JSON.stringify(messages));
            """
        )

        result = subprocess.run([node, "-e", script], check=True, text=True, capture_output=True)
        messages = json.loads(result.stdout)

        self.assertEqual(messages[0], "Models discovered from provider /models.")
        self.assertEqual(messages[1], "Using static model fallback; provider discovery failed: provider unavailable")
        self.assertEqual(messages[2], "Model discovery failed: 401 unauthorized")

    def test_agent_adaptation_status_message_reports_ready_and_blocked(self):
        node = shutil.which("node")
        if node is None:
            self.skipTest("node is required for static frontend payload tests")

        script = _node_harness(
            """
            const messages = [
              adaptationMessage({
                status: "ready",
                adapter: { display_name: "OpenAI-compatible CLI adapter" },
                gaps: [],
              }),
              adaptationMessage({
                status: "blocked",
                adapter: { adapter_id: "openai-compatible-cli" },
                gaps: [{ code: "missing_provider_config", message: "A configured API model provider is required." }],
              }),
              adaptationMessage({
                status: "ready",
                adapter: null,
                gaps: [],
              }),
            ];
            console.log(JSON.stringify(messages));
            """
        )

        result = subprocess.run([node, "-e", script], check=True, text=True, capture_output=True)
        messages = json.loads(result.stdout)

        self.assertEqual(messages[0], "Adapter ready: OpenAI-compatible CLI adapter.")
        self.assertEqual(messages[1], "Adapter blocked: A configured API model provider is required.")
        self.assertEqual(messages[2], "Adapter ready: no model key required.")


def _node_harness(assertion_script: str) -> str:
    return textwrap.dedent(
        f"""
        const fs = require("fs");
        const vm = require("vm");
        const source = fs.readFileSync({json.dumps(str(APP_JS))}, "utf8");
        const context = {{
          console,
          window: {{ addEventListener: () => null }},
          document: {{
            getElementById: () => ({{
              addEventListener: () => null,
              classList: {{ add: () => null, remove: () => null }},
              disabled: false,
              innerHTML: "",
              textContent: "",
              value: "",
            }}),
            createElement: () => ({{ value: "", textContent: "" }}),
          }},
          clearInterval: () => null,
          setInterval: () => null,
          fetch: () => null,
        }};
        vm.createContext(context);
        vm.runInContext(source, context);
        vm.runInContext({json.dumps(textwrap.dedent(assertion_script))}, context);
        """
    )


if __name__ == "__main__":
    unittest.main()
