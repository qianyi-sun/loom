import json
import tempfile
import unittest
from pathlib import Path

from agentic_data_platform.benchmark_wrappers.skillflow import main as skillflow_main
from agentic_data_platform.benchmark_wrappers.skilllearnbench import main as skilllearnbench_main
from agentic_data_platform.benchmarks.fixtures import load_fixture_catalog


class DryRunWrapperTest(unittest.TestCase):
    def test_skillflow_wrapper_writes_normalized_dry_run_result(self):
        manifest = _task_manifest(
            suite_name="SkillFlow",
            task_family="OCR-Data-Extraction",
            instance_id="task_family_invoice_images",
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            manifest_path = temp_path / "task.json"
            output_path = temp_path / "result.json"
            artifacts_dir = temp_path / "artifacts"
            manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

            exit_code = skillflow_main(
                [
                    "--task-manifest",
                    str(manifest_path),
                    "--workspace",
                    str(temp_path / "workspace"),
                    "--output",
                    str(output_path),
                    "--artifacts-dir",
                    str(artifacts_dir),
                    "--dry-run",
                ]
            )

            result = json.loads(output_path.read_text(encoding="utf-8"))

        self.assertEqual(exit_code, 0)
        self.assertEqual(result["status"], "completed")
        self.assertTrue(result["dry_run"])
        self.assertEqual(result["suite_name"], "SkillFlow")
        self.assertEqual(result["task_family"], "OCR-Data-Extraction")
        self.assertEqual(result["instance_id"], "task_family_invoice_images")
        self.assertIn("family_job_runner.py", result["planned_command"])
        self.assertIn("--only-group OCR-Data-Extraction", result["planned_command"])
        self.assertIn("--dry-run", result["planned_command"])
        self.assertEqual(result["artifacts"][0]["path"], "artifacts/planned-command.json")

    def test_skillflow_wrapper_synthesizes_safe_upstream_config_from_model_manifest(self):
        manifest = _task_manifest(
            suite_name="SkillFlow",
            task_family="OCR-Data-Extraction",
            instance_id="task_family_invoice_images",
            model={
                "provider": "anthropic",
                "model_name": "claude-sonnet-4-6",
                "api_key": "sk-raw-secret",
                "secret_ref": "env:MODEL_PROVIDER_API_KEY",
                "headers": {
                    "authorization": "Bearer raw-secret",
                    "x-safe-header": "team-latent",
                },
            },
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            manifest_path = temp_path / "task.json"
            output_path = temp_path / "result.json"
            artifacts_dir = temp_path / "artifacts"
            manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

            exit_code = skillflow_main(
                [
                    "--task-manifest",
                    str(manifest_path),
                    "--workspace",
                    str(temp_path / "workspace"),
                    "--output",
                    str(output_path),
                    "--artifacts-dir",
                    str(artifacts_dir),
                    "--dry-run",
                ]
            )

            result = json.loads(output_path.read_text(encoding="utf-8"))
            config = json.loads((artifacts_dir / "upstream-config.json").read_text(encoding="utf-8"))
            skillflow_config = json.loads(
                (artifacts_dir / "skillflow-job-config.json").read_text(encoding="utf-8")
            )

        self.assertEqual(exit_code, 0)
        self.assertIn(str(artifacts_dir / "skillflow-job-config.json"), result["planned_command"])
        self.assertIn(
            {
                "kind": "runner_config",
                "path": "artifacts/upstream-config.json",
                "media_type": "application/json",
            },
            result["artifacts"],
        )
        self.assertIn(
            {
                "kind": "runner_config",
                "path": "artifacts/skillflow-job-config.json",
                "media_type": "application/json",
            },
            result["artifacts"],
        )
        self.assertEqual(config["schema_version"], "adp.wrapper_config.v1")
        self.assertEqual(config["suite_name"], "SkillFlow")
        self.assertEqual(config["model"]["provider"], "anthropic")
        self.assertEqual(config["model"]["model_name"], "claude-sonnet-4-6")
        self.assertEqual(config["model"]["api_key"], "[redacted]")
        self.assertEqual(config["model"]["secret_ref"], "env:MODEL_PROVIDER_API_KEY")
        self.assertEqual(config["model"]["headers"]["authorization"], "[redacted]")
        self.assertEqual(config["model"]["headers"]["x-safe-header"], "team-latent")
        self.assertEqual(skillflow_config["schema_version"], "adp.skillflow_job_config.v1")
        self.assertEqual(skillflow_config["job_name"], "adp-run_wrapper_001-OCR-Data-Extraction")
        self.assertEqual(
            skillflow_config["agents"][0]["import_path"],
            "libs.harbor_noinstall_agents.agents:NoInstallClaudeCode",
        )
        self.assertEqual(skillflow_config["agents"][0]["model_name"], "anthropic/claude-sonnet-4-6")
        self.assertEqual(skillflow_config["agents"][0]["env"], {})
        rendered = json.dumps({"result": result, "adp": config, "skillflow": skillflow_config})
        self.assertNotIn("sk-raw-secret", rendered)
        self.assertNotIn("Bearer raw-secret", rendered)

    def test_skilllearnbench_wrapper_writes_instance_scoped_dry_run_result(self):
        manifest = _task_manifest(
            suite_name="SkillLearnBench",
            task_family="financial-analysis",
            instance_id="financial-analysis-2",
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            manifest_path = temp_path / "task.json"
            output_path = temp_path / "result.json"
            artifacts_dir = temp_path / "artifacts"
            manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

            exit_code = skilllearnbench_main(
                [
                    "--task-manifest",
                    str(manifest_path),
                    "--workspace",
                    str(temp_path / "workspace"),
                    "--output",
                    str(output_path),
                    "--artifacts-dir",
                    str(artifacts_dir),
                    "--dry-run",
                ]
            )

            result = json.loads(output_path.read_text(encoding="utf-8"))
            config = json.loads((artifacts_dir / "upstream-config.json").read_text(encoding="utf-8"))

        self.assertEqual(exit_code, 0)
        self.assertEqual(result["status"], "completed")
        self.assertTrue(result["dry_run"])
        self.assertEqual(result["suite_name"], "SkillLearnBench")
        self.assertIn("evaluate_skills.py financial-analysis", result["planned_command"])
        self.assertIn("--subtask-range 2-2", result["planned_command"])
        self.assertIn("--skill-path none", result["planned_command"])
        self.assertEqual(result["artifacts"][0]["kind"], "log")
        self.assertIn(
            {
                "kind": "runner_config",
                "path": "artifacts/upstream-config.json",
                "media_type": "application/json",
            },
            result["artifacts"],
        )
        self.assertEqual(config["suite_name"], "SkillLearnBench")
        self.assertEqual(config["model"]["provider"], "mock-api")

    def test_skilllearnbench_wrapper_maps_platform_provider_to_agent_and_model(self):
        manifest = _task_manifest(
            suite_name="SkillLearnBench",
            task_family="financial-analysis",
            instance_id="financial-analysis-2",
            model={
                "provider": "anthropic",
                "model_name": "claude-sonnet-4-6",
                "secret_ref": "env:MODEL_PROVIDER_API_KEY",
            },
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            manifest_path = temp_path / "task.json"
            output_path = temp_path / "result.json"
            artifacts_dir = temp_path / "artifacts"
            manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

            exit_code = skilllearnbench_main(
                [
                    "--task-manifest",
                    str(manifest_path),
                    "--workspace",
                    str(temp_path / "workspace"),
                    "--output",
                    str(output_path),
                    "--artifacts-dir",
                    str(artifacts_dir),
                    "--dry-run",
                ]
            )

            result = json.loads(output_path.read_text(encoding="utf-8"))
            config = json.loads((artifacts_dir / "upstream-config.json").read_text(encoding="utf-8"))

        self.assertEqual(exit_code, 0)
        self.assertIn("--agent claude-code", result["planned_command"])
        self.assertIn("--model claude-sonnet-4-6", result["planned_command"])
        self.assertEqual(config["model"]["provider"], "anthropic")
        self.assertEqual(config["provider_mapping"]["upstream_agent"], "claude-code")
        self.assertEqual(config["provider_mapping"]["upstream_env_var"], "ANTHROPIC_API_KEY")
        self.assertEqual(config["provider_mapping"]["secret_ref"], "env:MODEL_PROVIDER_API_KEY")

    def test_wrapper_rejects_unsupported_real_provider_mapping(self):
        manifest = _task_manifest(
            suite_name="SkillLearnBench",
            task_family="financial-analysis",
            instance_id="financial-analysis-2",
            model={
                "provider": "internal-custom-provider",
                "model_name": "custom-agent-model",
                "secret_ref": "env:MODEL_PROVIDER_API_KEY",
            },
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            manifest_path = temp_path / "task.json"
            manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

            with self.assertRaisesRegex(ValueError, "unsupported_provider_mapping"):
                skilllearnbench_main(
                    [
                        "--task-manifest",
                        str(manifest_path),
                        "--workspace",
                        str(temp_path / "workspace"),
                        "--output",
                        str(temp_path / "result.json"),
                        "--artifacts-dir",
                        str(temp_path / "artifacts"),
                        "--dry-run",
                    ]
                )

    def test_wrapper_rejects_wrong_suite_manifest(self):
        manifest = _task_manifest(
            suite_name="SkillLearnBench",
            task_family="financial-analysis",
            instance_id="financial-analysis-1",
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            manifest_path = Path(temp_dir) / "task.json"
            manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

            with self.assertRaisesRegex(ValueError, "SkillFlow"):
                skillflow_main(
                    [
                        "--task-manifest",
                        str(manifest_path),
                        "--workspace",
                        str(Path(temp_dir) / "workspace"),
                        "--output",
                        str(Path(temp_dir) / "result.json"),
                        "--artifacts-dir",
                        str(Path(temp_dir) / "artifacts"),
                        "--dry-run",
                    ]
                )


def _task_manifest(
    *,
    suite_name: str,
    task_family: str,
    instance_id: str,
    model: dict[str, object] | None = None,
) -> dict[str, object]:
    catalog = load_fixture_catalog(suite_name)
    spec = catalog.to_task_spec(task_family=task_family, instance_id=instance_id)
    return {
        "run_id": "run_wrapper_001",
        "suite_name": suite_name,
        "benchmark_version": catalog.benchmark_version,
        "source_uri": catalog.source_uri,
        "source_version": catalog.source_version,
        "task_family": task_family,
        "instance_id": instance_id,
        "instruction_ref": spec.metadata["instruction_ref"],
        "input_files": spec.metadata["input_files"],
        "model": model or {
            "provider": "mock-api",
            "model_name": "scripted-terminal-agent",
        },
        "output_dir": "/output",
        "artifacts_dir": "/output/artifacts",
    }
