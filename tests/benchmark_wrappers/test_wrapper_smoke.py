import json
import textwrap
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from agentic_data_platform.benchmark_wrappers.smoke import (
    BenchmarkWrapperSmokeConfig,
    run_benchmark_wrapper_smoke,
)


class BenchmarkWrapperSmokeTest(unittest.TestCase):
    def test_dry_run_smoke_validates_wrapper_without_upstream_root(self):
        with TemporaryDirectory() as temp_dir:
            result = run_benchmark_wrapper_smoke(
                BenchmarkWrapperSmokeConfig(
                    suite_name="SkillLearnBench",
                    task_family="financial-analysis",
                    instance_id="financial-analysis-2",
                    workspace_root=Path(temp_dir),
                    run_id="wrapper_smoke_dry",
                    dry_run=True,
                )
            )

        self.assertEqual(result["run_id"], "wrapper_smoke_dry")
        self.assertEqual(result["suite_name"], "SkillLearnBench")
        self.assertEqual(result["status"], "succeeded")
        self.assertTrue(result["dry_run"])
        self.assertEqual(result["exit_code"], 0)
        self.assertIn("artifacts/upstream-config.json", result["artifact_paths"])
        self.assertIn("--dry-run", result["planned_command"])

    def test_executable_smoke_runs_wrapper_against_local_upstream_root(self):
        with TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            upstream_root = temp_path / "skillflow-upstream"
            _write_executable_script(
                upstream_root / "family_job_runner.py",
                """
                import argparse
                import json
                from pathlib import Path

                parser = argparse.ArgumentParser()
                parser.add_argument("--config")
                parser.add_argument("--dataset-path")
                parser.add_argument("--run-root-dir")
                parser.add_argument("--only-group")
                args = parser.parse_args()
                output = Path(args.run_root_dir)
                output.mkdir(parents=True, exist_ok=True)
                (output / "report.json").write_text(json.dumps({
                    "group": args.only_group,
                    "config": args.config,
                }), encoding="utf-8")
                print(f"smoke ran {args.only_group}")
                """,
            )

            result = run_benchmark_wrapper_smoke(
                BenchmarkWrapperSmokeConfig(
                    suite_name="SkillFlow",
                    task_family="OCR-Data-Extraction",
                    instance_id="task_family_invoice_images",
                    workspace_root=temp_path / "workspace",
                    run_id="wrapper_smoke_exec",
                    upstream_root=upstream_root,
                )
            )

            report_path = temp_path / "workspace/wrapper_smoke_exec/artifacts/upstream-output/report.json"
            report = json.loads(report_path.read_text(encoding="utf-8"))

        self.assertEqual(result["status"], "succeeded")
        self.assertFalse(result["dry_run"])
        self.assertEqual(result["exit_code"], 0)
        self.assertIn("smoke ran OCR-Data-Extraction", result["stdout"])
        self.assertIn("artifacts/upstream-output/report.json", result["artifact_paths"])
        self.assertEqual(report["group"], "OCR-Data-Extraction")
        self.assertIn("upstream-config.json", report["config"])


def _write_executable_script(path: Path, source: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(textwrap.dedent(source).strip() + "\n", encoding="utf-8")
