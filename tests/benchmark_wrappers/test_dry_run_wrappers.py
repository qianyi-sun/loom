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

        self.assertEqual(exit_code, 0)
        self.assertEqual(result["status"], "completed")
        self.assertTrue(result["dry_run"])
        self.assertEqual(result["suite_name"], "SkillLearnBench")
        self.assertIn("evaluate_skills.py financial-analysis", result["planned_command"])
        self.assertIn("--subtask-range 2-2", result["planned_command"])
        self.assertIn("--skill-path none", result["planned_command"])
        self.assertEqual(result["artifacts"][0]["kind"], "log")

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


def _task_manifest(*, suite_name: str, task_family: str, instance_id: str) -> dict[str, object]:
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
        "model": {
            "provider": "mock-api",
            "model_name": "scripted-terminal-agent",
        },
        "output_dir": "/output",
        "artifacts_dir": "/output/artifacts",
    }
