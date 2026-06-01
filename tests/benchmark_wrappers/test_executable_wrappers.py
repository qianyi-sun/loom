import json
import tempfile
import textwrap
import unittest
from pathlib import Path

from agentic_data_platform.benchmark_wrappers.skillflow import main as skillflow_main
from agentic_data_platform.benchmark_wrappers.skilllearnbench import main as skilllearnbench_main
from agentic_data_platform.benchmarks.fixtures import load_fixture_catalog


class ExecutableWrapperTest(unittest.TestCase):
    def test_skillflow_wrapper_executes_upstream_command_and_writes_logs(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            upstream_root = temp_path / "skillflow-upstream"
            _write_executable_script(
                upstream_root / "family_job_runner.py",
                """
                import argparse
                import json
                import os
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
                    "dataset_path": args.dataset_path,
                    "run_id": os.environ["ADP_RUN_ID"],
                }), encoding="utf-8")
                print(f"ran skillflow {args.only_group}")
                """,
            )
            manifest = _task_manifest(
                suite_name="SkillFlow",
                task_family="OCR-Data-Extraction",
                instance_id="task_family_invoice_images",
                output_dir=str(temp_path / "upstream-output"),
            )
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
                    "--upstream-root",
                    str(upstream_root),
                ]
            )

            result = json.loads(output_path.read_text(encoding="utf-8"))
            copied_report = json.loads(
                (artifacts_dir / "upstream-output/report.json").read_text(encoding="utf-8")
            )

        self.assertEqual(exit_code, 0)
        self.assertEqual(result["status"], "completed")
        self.assertFalse(result["dry_run"])
        self.assertEqual(result["exit_code"], 0)
        self.assertIn("family_job_runner.py", result["planned_command"])
        self.assertNotIn("--dry-run", result["planned_command"])
        self.assertIn("ran skillflow OCR-Data-Extraction", result["stdout"])
        self.assertEqual(result["stderr"], "")
        self.assertEqual(copied_report["dataset_path"], "test_tasks/OCR-Data-Extraction")
        artifact_paths = {artifact["path"] for artifact in result["artifacts"]}
        self.assertIn("artifacts/planned-command.json", artifact_paths)
        self.assertIn("artifacts/upstream-config.json", artifact_paths)
        self.assertIn("artifacts/stdout.log", artifact_paths)
        self.assertIn("artifacts/stderr.log", artifact_paths)
        self.assertIn("artifacts/upstream-output/report.json", artifact_paths)
        self.assertEqual(copied_report["group"], "OCR-Data-Extraction")
        self.assertIn(
            {
                "kind": "upstream_output",
                "path": "artifacts/upstream-output/report.json",
                "media_type": "application/json",
            },
            result["artifacts"],
        )

    def test_skillflow_wrapper_normalizes_json_evaluator_report(self):
        with tempfile.TemporaryDirectory() as temp_dir:
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
                    "status": "completed",
                    "score": 0.875,
                    "success": True,
                    "feedback": "all invoice cells match",
                    "metrics": {"exact_match": 0.8}
                }), encoding="utf-8")
                """,
            )
            manifest = _task_manifest(
                suite_name="SkillFlow",
                task_family="OCR-Data-Extraction",
                instance_id="task_family_invoice_images",
                output_dir=str(temp_path / "upstream-output"),
            )
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
                    "--upstream-root",
                    str(upstream_root),
                ]
            )

            result = json.loads(output_path.read_text(encoding="utf-8"))
            evaluator_report = json.loads(
                (artifacts_dir / "evaluator-report.json").read_text(encoding="utf-8")
            )

        self.assertEqual(exit_code, 0)
        self.assertEqual(result["evaluator_report_ref"], "artifacts/evaluator-report.json")
        self.assertEqual(result["metrics"]["upstream_report_count"], 1)
        self.assertEqual(result["metrics"]["upstream_score"], 0.875)
        self.assertTrue(result["metrics"]["upstream_success"])
        self.assertEqual(result["metrics"]["upstream_exact_match"], 0.8)
        self.assertIn(
            {
                "kind": "evaluator_report",
                "path": "artifacts/evaluator-report.json",
                "media_type": "application/json",
            },
            result["artifacts"],
        )
        self.assertEqual(evaluator_report["schema_version"], "adp.wrapper_evaluator_report.v1")
        self.assertEqual(evaluator_report["feedback"], ["all invoice cells match"])
        self.assertEqual(
            evaluator_report["source_reports"][0]["path"],
            "artifacts/upstream-output/report.json",
        )

    def test_skillflow_wrapper_normalizes_harbor_job_result_stats(self):
        with tempfile.TemporaryDirectory() as temp_dir:
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
                output = Path(args.run_root_dir) / "job"
                output.mkdir(parents=True, exist_ok=True)
                (output / "result.json").write_text(json.dumps({
                    "n_total_trials": 8,
                    "stats": {
                        "n_completed_trials": 8,
                        "n_errored_trials": 2,
                        "n_cancelled_trials": 0,
                        "evals": {
                            "oracle__OCR-Data-Extraction": {
                                "n_errors": 2,
                                "metrics": [{"mean": 0.75}]
                            }
                        }
                    }
                }), encoding="utf-8")
                """,
            )
            manifest = _task_manifest(
                suite_name="SkillFlow",
                task_family="OCR-Data-Extraction",
                instance_id="task_family_invoice_images",
                output_dir=str(temp_path / "upstream-output"),
            )
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
                    "--upstream-root",
                    str(upstream_root),
                ]
            )

            result = json.loads(output_path.read_text(encoding="utf-8"))

        self.assertEqual(exit_code, 0)
        self.assertEqual(result["metrics"]["upstream_n_total_trials"], 8)
        self.assertEqual(result["metrics"]["upstream_n_completed_trials"], 8)
        self.assertEqual(result["metrics"]["upstream_n_errored_trials"], 2)
        self.assertEqual(result["metrics"]["upstream_n_cancelled_trials"], 0)
        self.assertEqual(result["metrics"]["upstream_n_errors"], 2)
        self.assertEqual(result["metrics"]["upstream_score_mean"], 0.75)

    def test_skilllearnbench_wrapper_executes_instance_scoped_upstream_command(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            upstream_root = temp_path / "skilllearnbench-upstream"
            _write_executable_script(
                upstream_root / "evaluate_skills.py",
                """
                import argparse
                import os
                from pathlib import Path

                parser = argparse.ArgumentParser()
                parser.add_argument("task")
                parser.add_argument("--skill-path")
                parser.add_argument("--trials-dir")
                parser.add_argument("--subtask-range")
                args = parser.parse_args()
                Path(args.trials_dir).mkdir(parents=True, exist_ok=True)
                print(f"task={args.task} subtask={args.subtask_range} run={os.environ['ADP_RUN_ID']}")
                """,
            )
            manifest = _task_manifest(
                suite_name="SkillLearnBench",
                task_family="financial-analysis",
                instance_id="financial-analysis-2",
                output_dir=str(temp_path / "upstream-output"),
            )
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
                    "--upstream-root",
                    str(upstream_root),
                ]
            )

            result = json.loads(output_path.read_text(encoding="utf-8"))

        self.assertEqual(exit_code, 0)
        self.assertEqual(result["status"], "completed")
        self.assertIn("--subtask-range 2-2", result["planned_command"])
        self.assertIn("task=financial-analysis subtask=2-2", result["stdout"])

    def test_skilllearnbench_wrapper_normalizes_csv_evaluator_report(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            upstream_root = temp_path / "skilllearnbench-upstream"
            _write_executable_script(
                upstream_root / "evaluate_skills.py",
                """
                import argparse
                from pathlib import Path

                parser = argparse.ArgumentParser()
                parser.add_argument("task")
                parser.add_argument("--skill-path")
                parser.add_argument("--trials-dir")
                parser.add_argument("--subtask-range")
                args = parser.parse_args()
                report = Path(args.trials_dir).parent / "evaluation_reports" / "baseline" / args.task / "report.csv"
                report.parent.mkdir(parents=True, exist_ok=True)
                report.write_text(
                    "instance_id,success,score,feedback\\n"
                    "financial-analysis-1,true,1.0,ok\\n"
                    "financial-analysis-2,false,0.5,missing formula\\n",
                    encoding="utf-8",
                )
                """,
            )
            manifest = _task_manifest(
                suite_name="SkillLearnBench",
                task_family="financial-analysis",
                instance_id="financial-analysis-2",
                output_dir=str(temp_path / "upstream-output"),
            )
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
                    "--upstream-root",
                    str(upstream_root),
                ]
            )

            result = json.loads(output_path.read_text(encoding="utf-8"))
            evaluator_report = json.loads(
                (artifacts_dir / "evaluator-report.json").read_text(encoding="utf-8")
            )

        self.assertEqual(exit_code, 0)
        self.assertEqual(result["evaluator_report_ref"], "artifacts/evaluator-report.json")
        self.assertEqual(result["metrics"]["upstream_report_count"], 1)
        self.assertEqual(result["metrics"]["upstream_trial_count"], 2)
        self.assertEqual(result["metrics"]["upstream_score_mean"], 0.75)
        self.assertEqual(result["metrics"]["upstream_success_rate"], 0.5)
        self.assertEqual(evaluator_report["feedback"], ["ok", "missing formula"])
        self.assertEqual(
            evaluator_report["source_reports"][0]["path"],
            "artifacts/upstream-output/evaluation_reports/baseline/financial-analysis/report.csv",
        )

    def test_wrapper_maps_nonzero_upstream_exit_to_failed_result(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            upstream_root = temp_path / "skillflow-upstream"
            _write_executable_script(
                upstream_root / "family_job_runner.py",
                """
                import sys
                print("runner failed", file=sys.stderr)
                raise SystemExit(7)
                """,
            )
            manifest = _task_manifest(
                suite_name="SkillFlow",
                task_family="OCR-Data-Extraction",
                instance_id="task_family_invoice_images",
                output_dir=str(temp_path / "upstream-output"),
            )
            manifest_path = temp_path / "task.json"
            output_path = temp_path / "result.json"
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
                    str(temp_path / "artifacts"),
                    "--upstream-root",
                    str(upstream_root),
                ]
            )

            result = json.loads(output_path.read_text(encoding="utf-8"))

        self.assertEqual(exit_code, 7)
        self.assertEqual(result["status"], "failed")
        self.assertEqual(result["exit_code"], 7)
        self.assertIn("runner failed", result["stderr"])
        self.assertEqual(result["failure_reason"], "upstream runner exited with code 7")

    def test_wrapper_maps_upstream_timeout_to_failed_result(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            upstream_root = temp_path / "skillflow-upstream"
            _write_executable_script(
                upstream_root / "family_job_runner.py",
                """
                import time
                time.sleep(2)
                """,
            )
            manifest = _task_manifest(
                suite_name="SkillFlow",
                task_family="OCR-Data-Extraction",
                instance_id="task_family_invoice_images",
                output_dir=str(temp_path / "upstream-output"),
            )
            manifest_path = temp_path / "task.json"
            output_path = temp_path / "result.json"
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
                    str(temp_path / "artifacts"),
                    "--upstream-root",
                    str(upstream_root),
                    "--timeout-seconds",
                    "1",
                ]
            )

            result = json.loads(output_path.read_text(encoding="utf-8"))

        self.assertEqual(exit_code, 124)
        self.assertEqual(result["status"], "failed")
        self.assertEqual(result["exit_code"], 124)
        self.assertEqual(result["failure_reason"], "upstream runner timed out after 1 seconds")

    def test_executable_wrapper_requires_upstream_root(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            manifest = _task_manifest(
                suite_name="SkillFlow",
                task_family="OCR-Data-Extraction",
                instance_id="task_family_invoice_images",
                output_dir=str(temp_path / "upstream-output"),
            )
            manifest_path = temp_path / "task.json"
            manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

            with self.assertRaisesRegex(ValueError, "--upstream-root"):
                skillflow_main(
                    [
                        "--task-manifest",
                        str(manifest_path),
                        "--workspace",
                        str(temp_path / "workspace"),
                        "--output",
                        str(temp_path / "result.json"),
                        "--artifacts-dir",
                        str(temp_path / "artifacts"),
                    ]
                )


def _task_manifest(
    *,
    suite_name: str,
    task_family: str,
    instance_id: str,
    output_dir: str,
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
        "model": {
            "provider": "mock-api",
            "model_name": "scripted-terminal-agent",
        },
        "output_dir": output_dir,
        "artifacts_dir": "/output/artifacts",
    }


def _write_executable_script(path: Path, source: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(textwrap.dedent(source).strip() + "\n", encoding="utf-8")
